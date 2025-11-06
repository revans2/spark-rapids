/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.spark.rapids.benchmarks

import ai.rapids.cudf._
import ai.rapids.cudf.ast.CompiledExpression
import com.nvidia.spark.rapids.benchmarks.JoinBenchmarkRunner._
import com.nvidia.spark.rapids.jni.{HashJoin, SortMergeJoin}

/**
 * Build holder for non-conditional (key-only) joins.
 * 
 * RESOURCE OWNERSHIP CONVENTION:
 * The holder OWNS all resources passed to it.
 * 
 * - buildTable: OWNED - new Table with incremented refcounts on columns
 * - buildKeys: OWNED - new Table with incremented refcounts on key columns
 * - cached join object: OWNED - HashJoin, SortMergeJoin, etc.
 * 
 * The holder's close() method MUST close ALL resources: buildTable, buildKeys,
 * and any cached join objects. This ensures proper reference count management and
 * prevents column vector leaks.
 */
sealed trait NonConditionalBuildHolder extends AutoCloseable {
  def buildTable: Table
  def buildKeys: Table
  
  /**
   * Execute the join with the given probe keys.
   * @param probeKeys The probe-side keys
   * @return Array of GatherMap (interpretation depends on holder type)
   */
  def join(probeKeys: Table): Array[GatherMap]
}

/**
 * Build holder for mixed conditional joins (keys + AST filtering).
 * 
 * RESOURCE OWNERSHIP CONVENTION:
 * The holder OWNS all resources EXCEPT CompiledExpression (which is a reference).
 * 
 * - buildTable: OWNED - new Table with incremented refcounts on columns
 * - buildKeys: OWNED - new Table with incremented refcounts on key columns
 * - astExpression: REFERENCE - owned by JoinBenchmarkRunner, NOT closed by holder
 * - cached join object: OWNED - HashJoin, SortMergeJoin, etc.
 * 
 * The holder's close() method MUST close all owned resources: buildTable, buildKeys,
 * and any cached join objects. It must NOT close astExpression (which is a reference).
 */
sealed trait MixedConditionalBuildHolder extends AutoCloseable {
  def buildTable: Table
  def buildKeys: Table
  def astExpression: CompiledExpression
  
  /**
   * Execute the mixed join with keys and AST filtering.
   * @param probeKeys The probe-side keys
   * @param probeTable The probe-side table (for AST evaluation)
   * @return Array of GatherMap
   */
  def join(probeKeys: Table, probeTable: Table): Array[GatherMap]
}

/**
 * Non-conditional build holder for inner hash joins.
 * Uses HashJoin (can be cached if cacheJoinObject is enabled).
 */
private class InnerHashBuildHolder(
  val buildTable: Table,
  val buildKeys: Table,
  compareNullsEqual: Boolean,
  tablesWereSwapped: Boolean,
  optimizations: JoinOptimizations
) extends NonConditionalBuildHolder {
  
  private var cachedJoinObject: Option[HashJoin] = None
  private var initialized = false
  
  private def ensureInitialized(): Unit = {
    if (!initialized) {
      if (optimizations.cacheJoinObject) {
        cachedJoinObject = Some(HashJoin.create(buildKeys, compareNullsEqual))
      }
      initialized = true
    }
  }
  
  def join(probeKeys: Table): Array[GatherMap] = {
    ensureInitialized()
    
    if (cachedJoinObject.isDefined) {
      cachedJoinObject.get.innerJoin(probeKeys)
    } else {
      buildKeys.innerJoinGatherMaps(probeKeys, compareNullsEqual)
    }
  }
  
  def close(): Unit = {
    cachedJoinObject.foreach(_.close())
    buildKeys.close()
    buildTable.close()
  }
}

/**
 * Post-processing build holder for all join types.
 * Does inner join first, then applies post-processing (makeLeftOuter, makeSemi, etc.).
 * Supports both hash and sort-merge strategies.
 */
private class PostProcessingBuildHolder(
  joinType: JoinTypeSpec,
  strategy: JoinStrategySpec,
  val buildTable: Table,
  val buildKeys: Table,
  compareNullsEqual: Boolean,
  tablesWereSwapped: Boolean,
  optimizations: JoinOptimizations
) extends NonConditionalBuildHolder {
  
  private var cachedJoinObject: Option[AutoCloseable] = None
  private var initialized = false
  
  private def ensureInitialized(): Unit = {
    if (!initialized) {
      if (optimizations.cacheJoinObject) {
        cachedJoinObject = Some(strategy match {
          case HashWithPostStrategy =>
            HashJoin.create(buildKeys, compareNullsEqual)
          case SortWithPostStrategy =>
            SortMergeJoin.create(buildKeys, false /* isBuildSorted */, compareNullsEqual)
          case _ =>
            throw new IllegalArgumentException(
              s"Unsupported strategy for post-processing: $strategy")
        })
      }
      initialized = true
    }
  }
  
  def join(probeKeys: Table): Array[GatherMap] = {
    ensureInitialized()
    
    // Step 1: Do inner join
    val innerMaps = if (cachedJoinObject.isDefined) {
      cachedJoinObject.get match {
        case hj: HashJoin => hj.innerJoin(probeKeys)
        case smj: SortMergeJoin => smj.innerJoin(probeKeys, false /* isProbeSorted */)
        case _ => throw new IllegalStateException("Unexpected join object type")
      }
    } else {
      strategy match {
        case HashWithPostStrategy =>
          buildKeys.innerJoinGatherMaps(probeKeys, compareNullsEqual)
        case SortWithPostStrategy =>
          // Use JNI SortMergeJoin for consistency (not cached)
          val smj = SortMergeJoin.create(buildKeys, false /* isBuildSorted */, compareNullsEqual)
          try {
            smj.innerJoin(probeKeys, false /* isProbeSorted */)
          } finally {
            smj.close()
          }
        case _ =>
          throw new IllegalArgumentException(
            s"Unsupported strategy for post-processing: $strategy")
      }
    }
    
    // Step 2: Apply post-processing based on join type
    // For inner joins, no post-processing needed - just return the inner result
    joinType match {
      case InnerJoin =>
        innerMaps
      case _ =>
        throw new UnsupportedOperationException(
          s"Join type $joinType not yet implemented (Phase 2)")
    }
  }
  
  def close(): Unit = {
    cachedJoinObject.foreach(_.close())
    buildKeys.close()
    buildTable.close()
  }
}

/**
 * Mixed conditional build holder for inner hash joins using direct mixed API.
 * Uses mixedInnerJoinGatherMaps (keys for matching + AST for filtering).
 */
private class MixedInnerHashBuildHolder(
  val buildTable: Table,
  val buildKeys: Table,
  val astExpression: CompiledExpression,
  compareNullsEqual: Boolean,
  optimizations: JoinOptimizations
) extends MixedConditionalBuildHolder {
  
  def join(probeKeys: Table, probeTable: Table): Array[GatherMap] = {
    val nullEq = if (compareNullsEqual) NullEquality.EQUAL else NullEquality.UNEQUAL
    Table.mixedInnerJoinGatherMaps(
      /* leftKeys  = */ buildKeys,
      /* rightKeys = */ probeKeys,
      /* leftCond  = */ buildTable,
      /* rightCond = */ probeTable,
      /* condition = */ astExpression,
      /* nullEq    = */ nullEq)
  }
  
  def close(): Unit = {
    buildKeys.close()
    buildTable.close()
    // astExpression is a reference, not owned by holder
  }
}

/**
 * Mixed conditional build holder using post-processing approach.
 * Does inner join first, then applies AST filter.
 */
private class MixedPostProcessingBuildHolder(
  joinType: JoinTypeSpec,
  strategy: JoinStrategySpec,
  val buildTable: Table,
  val buildKeys: Table,
  val astExpression: CompiledExpression,
  compareNullsEqual: Boolean,
  tablesWereSwapped: Boolean,
  optimizations: JoinOptimizations
) extends MixedConditionalBuildHolder {
  
  private var cachedJoinObject: Option[AutoCloseable] = None
  private var initialized = false
  
  private def ensureInitialized(): Unit = {
    if (!initialized) {
      if (optimizations.cacheJoinObject) {
        cachedJoinObject = Some(strategy match {
          case HashWithPostStrategy =>
            HashJoin.create(buildKeys, compareNullsEqual)
          case SortWithPostStrategy =>
            SortMergeJoin.create(buildKeys, false /* isBuildSorted */, compareNullsEqual)
          case _ =>
            throw new IllegalArgumentException(s"Unsupported strategy: $strategy")
        })
      }
      initialized = true
    }
  }
  
  def join(probeKeys: Table, probeTable: Table): Array[GatherMap] = {
    ensureInitialized()
    
    // Step 1: Do inner join on keys
    val innerMaps = if (cachedJoinObject.isDefined) {
      cachedJoinObject.get match {
        case hj: HashJoin => hj.innerJoin(probeKeys)
        case smj: SortMergeJoin => smj.innerJoin(probeKeys, false /* isProbeSorted */)
        case _ => throw new IllegalStateException("Unexpected join object type")
      }
    } else {
      strategy match {
        case HashWithPostStrategy =>
          buildKeys.innerJoinGatherMaps(probeKeys, compareNullsEqual)
        case SortWithPostStrategy =>
          val smj = SortMergeJoin.create(buildKeys, false /* isBuildSorted */, compareNullsEqual)
          try {
            smj.innerJoin(probeKeys, false /* isProbeSorted */)
          } finally {
            smj.close()
          }
        case _ => throw new IllegalArgumentException(s"Unsupported strategy: $strategy")
      }
    }
    
    try {
      // Step 2: Apply AST filter to inner join results
      val filteredMaps = com.nvidia.spark.rapids.jni.JoinPrimitives.filterGatherMapsByAST(
        innerMaps(0), innerMaps(1), buildTable, probeTable, astExpression)
      
      // Step 3: Apply post-processing based on join type (Phase 2)
      joinType match {
        case InnerJoin =>
          filteredMaps
        case _ =>
          throw new UnsupportedOperationException(
            s"Join type $joinType not yet implemented (Phase 2)")
      }
    } finally {
      innerMaps.foreach(_.close())
    }
  }
  
  def close(): Unit = {
    cachedJoinObject.foreach(_.close())
    buildKeys.close()
    buildTable.close()
    // astExpression is a reference, not owned by holder
  }
}

/**
 * Internal class to manage join execution with proper build holder selection.
 */
private[benchmarks] class JoinExecutor(
  leftTable: Table,
  rightTable: Table,
  leftKeyIndices: Array[Int],
  rightKeyIndices: Array[Int],
  joinType: JoinTypeSpec,
  strategy: JoinStrategySpec,
  buildSideSpec: BuildSideSpec,
  compareNullsEqual: Boolean,
  optimizations: JoinOptimizations,
  mixedFilterLeftBuild: Option[CompiledExpression],
  mixedFilterRightBuild: Option[CompiledExpression]
) {
  
  private var cachedHolder: Option[
    Either[NonConditionalBuildHolder, MixedConditionalBuildHolder]] = None
  private var actualBuildSide: Option[BuildSideSpec] = None
  
  /**
   * Create the appropriate build holder for a specific build side.
   * 
   * Creates NEW Tables with incremented reference counts so the holder owns them.
   * This ensures clean ownership semantics: holder owns everything except CompiledExpression.
   */
  private def createBuildHolder(
      buildSide: BuildSideSpec): Either[NonConditionalBuildHolder, MixedConditionalBuildHolder] = {
    val sourceTable = if (buildSide == LeftBuild) leftTable else rightTable
    // Create new Table with incremented refcounts - holder will own this
    val buildTable = new Table(
      (0 until sourceTable.getNumberOfColumns).map(sourceTable.getColumn): _*)
    val buildKeyIndices = if (buildSide == LeftBuild) leftKeyIndices else rightKeyIndices
    // Create new Table for keys with incremented refcounts - holder will own this
    val buildKeys = new Table(buildKeyIndices.map(buildTable.getColumn): _*)
    val tablesWereSwapped = buildSideSpec match {
      case LeftBuild | RightBuild => buildSide != buildSideSpec
      case _ => false
    }
    
    val astForThisBuildSide = if (buildSide == LeftBuild) {
      mixedFilterLeftBuild
    } else {
      mixedFilterRightBuild
    }
    
    astForThisBuildSide match {
      case Some(ast) =>
        // Mixed conditional join
        val holder = (joinType, strategy) match {
          case (InnerJoin, HashJoinStrategy) =>
            new MixedInnerHashBuildHolder(buildTable, buildKeys, ast, compareNullsEqual,
              optimizations)
          
          case (InnerJoin, HashWithPostStrategy | SortWithPostStrategy) =>
            new MixedPostProcessingBuildHolder(joinType, strategy, buildTable, buildKeys,
              ast, compareNullsEqual, tablesWereSwapped, optimizations)
          
          case _ =>
            throw new IllegalArgumentException(
              s"Mixed conditional joins not supported for: $joinType with $strategy (Phase 2)")
        }
        Right(holder)
      
      case None =>
        // Non-conditional join
        val holder = (joinType, strategy) match {
          case (InnerJoin, HashJoinStrategy) =>
            new InnerHashBuildHolder(buildTable, buildKeys, compareNullsEqual,
              tablesWereSwapped, optimizations)
          
          case (InnerJoin, HashWithPostStrategy | SortWithPostStrategy) =>
            new PostProcessingBuildHolder(joinType, strategy, buildTable, buildKeys,
              compareNullsEqual, tablesWereSwapped, optimizations)
          
          case _ =>
            throw new UnsupportedOperationException(
              s"Join type $joinType not yet implemented (Phase 2)")
        }
        Left(holder)
    }
  }
  
  /**
   * Execute a single join and return gather maps.
   * Returns: (gatherMaps: Array[GatherMap], timingMs: Double)
   */
  def executeJoin(): (Array[GatherMap], Double) = {
    val holder = cachedHolder match {
      case Some(h) => h
      case None =>
        val buildSide = buildSideSpec match {
          case AutoPickSmallerIfAllowed =>
            val leftSize = leftTable.getRowCount
            val rightSize = rightTable.getRowCount
            val preferredSide = if (leftSize <= rightSize) LeftBuild else RightBuild
            
            if (optimizations.allowBuildSideSwap && canSwap(joinType, strategy)) {
              preferredSide
            } else {
              val requiredSide = defaultBuildSide(joinType, strategy)
              if (preferredSide == requiredSide || canSwap(joinType, strategy)) {
                preferredSide
              } else {
                requiredSide
              }
            }
          
          case AutoMeetJoinRequirement =>
            defaultBuildSide(joinType, strategy)
          
          case LeftBuild => LeftBuild
          case RightBuild => RightBuild
        }
        
        val h = createBuildHolder(buildSide)
        actualBuildSide = Some(buildSide)
        
        if (optimizations.cacheJoinObject) {
          cachedHolder = Some(h)
        }
        
        h
    }
    
    val buildSide = actualBuildSide.get
    val (probeTable, probeKeyIndices) = if (buildSide == LeftBuild) {
      (rightTable, rightKeyIndices)
    } else {
      (leftTable, leftKeyIndices)
    }
    
    val probeKeys = new Table(probeKeyIndices.map(probeTable.getColumn): _*)
    
    try {
      val gatherMaps = holder match {
        case Left(nonConditionalHolder) =>
          nonConditionalHolder.join(probeKeys)
        
        case Right(mixedConditionalHolder) =>
          mixedConditionalHolder.join(probeKeys, probeTable)
      }
      
      (gatherMaps, 0.0)
      
    } finally {
      probeKeys.close()  // Must close to decrement column refcounts
      
      if (!optimizations.cacheJoinObject) {
        holder match {
          case Left(h) => h.close()
          case Right(h) => h.close()
        }
        actualBuildSide = None
      }
    }
  }
  
  private def canSwap(joinType: JoinTypeSpec, strategy: JoinStrategySpec): Boolean = {
    strategy match {
      case HashWithPostStrategy | SortWithPostStrategy => true
      case _ => joinType match {
        case InnerJoin => true
        case _ => false  // Phase 2 will handle other join types
      }
    }
  }
  
  private def defaultBuildSide(
      joinType: JoinTypeSpec,
      strategy: JoinStrategySpec): BuildSideSpec = {
    joinType match {
      case InnerJoin => LeftBuild  // Arbitrary choice for inner joins
      case _ => LeftBuild  // Phase 2 will handle other join types
    }
  }
  
  def clearCache(): Unit = {
    cachedHolder.foreach {
      case Left(h) => h.close()
      case Right(h) => h.close()
    }
    cachedHolder = None
    actualBuildSide = None
  }
}

