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
import com.nvidia.spark.rapids.jni.{DistinctHashJoin, FilteredJoin, HashJoin, JoinPrimitives, KeyRemapping, SortMergeJoin}

/**
 * Join execution implementation with multiple strategies and optimizations.
 * 
 * This module provides build holders for both key-only and mixed (key+AST) joins.
 * Build holders encapsulate the join object lifecycle and optimization state,
 * enabling efficient benchmarking with various caching strategies.
 * 
 * Key design principles:
 * - Clean resource ownership: holders own all resources except CompiledExpression references
 * - Lazy initialization: caching structures created on first use
 * - Flexible strategies: supports both object-based (cacheable) and direct (one-shot) APIs
 */

/**
 * Detailed timing breakdown for join operations with key remapping.
 * All timings are in milliseconds. Only populated when key remapping is enabled.
 * 
 * @param remapStructureBuildMs Time to build the key remapping structure (dictionary + hash tables)
 * @param remapBuildKeysMs Time to remap the build table keys using the remapping structure
 * @param remapProbeKeysMs Time to remap the probe table keys using the remapping structure
 * @param createBuildObjectMs Time to create the join object (HashJoin/SortMergeJoin)
 * @param executeJoinMs Time to execute the join and produce gather maps
 */
case class DetailedTimings(
  remapStructureBuildMs: Double,
  remapBuildKeysMs: Double,
  remapProbeKeysMs: Double,
  createBuildObjectMs: Double,
  executeJoinMs: Double
)

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
  
  /**
   * Execute the join with detailed timing breakdown.
   * Only PostProcessingBuildHolder supports detailed timings.
   * @param probeKeys The probe-side keys
   * @return (gatherMaps, optionalDetailedTimings)
   */
  def joinWithDetailedTimings(probeKeys: Table): (Array[GatherMap], Option[DetailedTimings]) = {
    (join(probeKeys), None)
  }
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
 * Helper object for creating Tables with explicit ownership semantics.
 * 
 * These helpers make it clear that new Tables are being created with incremented
 * column refcounts, and the caller owns the returned Table and must close it.
 */
private object TableCopyHelper {
  /**
   * Create a new Table by copying all columns from the source table.
   * This increments the refcount on each column vector.
   * 
   * @param source The source table to copy from
   * @return A new Table that the caller OWNS and MUST close
   */
  def copyTable(source: Table): Table = {
    new Table((0 until source.getNumberOfColumns).map(source.getColumn): _*)
  }
  
  /**
   * Create a new Table containing a subset of columns from the source table.
   * This increments the refcount on the selected column vectors.
   * 
   * @param source The source table to extract columns from
   * @param columnIndices Indices of columns to include in the new table
   * @return A new Table that the caller OWNS and MUST close
   */
  def extractColumns(source: Table, columnIndices: Array[Int]): Table = {
    new Table(columnIndices.map(source.getColumn): _*)
  }
  
  /**
   * Create a new Table wrapping a single ColumnVector.
   * This increments the refcount on the column vector.
   * 
   * @param column The column to wrap
   * @return A new Table that the caller OWNS and MUST close
   */
  def wrapColumn(column: ColumnVector): Table = {
    new Table(column)
  }
}

/**
 * Helper object for key remapping operations.
 */
private object KeyRemappingHelper {
  
  /**
   * Check if any key column has a type that requires remapping for SortMergeJoin.
   * SortMergeJoin does not support STRUCT or LIST key types without remapping.
   */
  def hasUnsupportedSortMergeKeyType(keys: Table): Boolean = {
    (0 until keys.getNumberOfColumns).exists { i =>
      val dtype = keys.getColumn(i).getType
      dtype == DType.STRUCT || dtype == DType.LIST
    }
  }
  
  /**
   * Validate that key types are compatible with SortMergeJoin.
   * Throws IllegalArgumentException if struct or list keys are used without remapping.
   */
  def validateSortMergeKeyTypes(keys: Table, remappingEnabled: Boolean): Unit = {
    if (!remappingEnabled && hasUnsupportedSortMergeKeyType(keys)) {
      throw new IllegalArgumentException(
        "SortMergeJoin does not support STRUCT or LIST key types. " +
        "Enable key remapping (remapComplexKeysToInts=true) to use these key types " +
        "with SortMergeJoin.")
    }
  }
}

/**
 * Trait providing distinctness checking with optional caching.
 */
private trait DistinctnessSupport {
  protected var cachedIsDistinct: Option[Boolean] = None
  
  /**
   * Check if build keys are distinct, with optional caching.
   * Returns true if distinct join optimization can be used for this join type.
   * 
   * @param keys The keys to check for distinctness
   * @param joinType The join type (only Inner and LeftOuter support distinct optimization)
   * @param optimizations Configuration for optimizations
   * @return true if keys are distinct and distinct join can be used
   */
  protected def checkDistinctness(
      keys: Table,
      joinType: JoinTypeSpec,
      optimizations: JoinOptimizations): Boolean = {
    if (!optimizations.useDistinctJoin) {
      return false
    }
    
    // Use cached result if available
    if (optimizations.cacheDistinctFlag && cachedIsDistinct.isDefined) {
      return cachedIsDistinct.get
    }
    
    // Only Inner and LeftOuter join types support DistinctHashJoin
    val canUseDistinct = joinType match {
      case InnerJoin | LeftOuterJoin => true
      case _ => false
    }
    
    if (!canUseDistinct) {
      return false
    }
    
    // Check if keys are actually distinct
    val distinct = keys.getRowCount == keys.distinctCount()
    
    // Cache the result if caching is enabled
    if (optimizations.cacheDistinctFlag) {
      cachedIsDistinct = Some(distinct)
    }
    
    distinct
  }
}

/**
 * Trait providing common key remapping functionality for build holders.
 * Handles both cached and non-cached remapping of build and probe keys.
 */
private trait RemappingSupport {
  protected var remapStructures: Option[KeyRemapping.RemapStructures] = None
  protected var remappedBuildKeys: Option[ColumnVector] = None
  
  /**
   * Initialize remapping structures for the build keys.
   * Call this during holder initialization if remapping is enabled.
   */
  protected def initializeRemapping(buildKeys: Table, optimizations: JoinOptimizations): Unit = {
    if (optimizations.remapComplexKeysToInts && optimizations.cacheRemapping) {
      val remap = KeyRemapping.createRemapStructures(buildKeys, 
        KeyRemapping.NullEqualityMode.SPARK_EQUALITY)
      remapStructures = Some(remap)
      remappedBuildKeys = Some(KeyRemapping.applyRemapping(buildKeys, remap, true))
    }
  }
  
  /**
   * Initialize remapping structures with timing tracking.
   * Returns (remapStructureBuildMs, remapBuildKeysMs)
   */
  protected def initializeRemappingWithTiming(
      buildKeys: Table, 
      optimizations: JoinOptimizations): (Double, Double) = {
    if (optimizations.remapComplexKeysToInts) {
      val t0 = System.nanoTime()
      val remap = KeyRemapping.createRemapStructures(buildKeys, 
        KeyRemapping.NullEqualityMode.SPARK_EQUALITY)
      Cuda.DEFAULT_STREAM.sync()
      val t1 = System.nanoTime()
      
      val structureBuildMs = (t1 - t0) / 1e6
      
      if (optimizations.cacheRemapping) {
        // Cache the remapping structures and pre-remap build keys
        val remappedCol = KeyRemapping.applyRemapping(buildKeys, remap, true)
        Cuda.DEFAULT_STREAM.sync()
        val t2 = System.nanoTime()
        
        remapStructures = Some(remap)
        remappedBuildKeys = Some(remappedCol)
        
        val remapBuildMs = (t2 - t1) / 1e6
        (structureBuildMs, remapBuildMs)
      } else {
        // Non-cached: just time structure creation, will remap on-demand
        remapStructures = Some(remap)
        (structureBuildMs, 0.0)
      }
    } else {
      (0.0, 0.0)
    }
  }
  
  /**
   * Remap probe keys using cached or temporary remapping structures.
   * Returns Some(remappedColumn) if remapping is enabled, None otherwise.
   * Caller is responsible for closing the returned ColumnVector.
   */
  protected def remapProbeKeys(
      probeKeys: Table, 
      buildKeys: Table, 
      optimizations: JoinOptimizations): Option[ColumnVector] = {
    if (optimizations.remapComplexKeysToInts) {
      // Validate that cached state matches configuration
      if (optimizations.cacheRemapping && remappedBuildKeys.isEmpty) {
        throw new IllegalStateException(
          "cacheRemapping is enabled but remappedBuildKeys is not set. " +
          "initializeRemappingWithTiming must be called before remapProbeKeys.")
      }
      if (!optimizations.cacheRemapping && remappedBuildKeys.isDefined) {
        throw new IllegalStateException(
          "cacheRemapping is disabled but remappedBuildKeys is set. " +
          "This indicates a configuration/state mismatch.")
      }
      
      if (remapStructures.isDefined) {
        // Use cached remapping structures
        Some(KeyRemapping.applyRemapping(probeKeys, remapStructures.get, false))
      } else {
        throw new IllegalStateException(
          "Remapping is enabled but no remapping structures are available. " +
          "initializeRemappingWithTiming must be called before remapProbeKeys.")
      }
    } else {
      None
    }
  }
  
  /**
   * Remap probe keys with timing tracking.
   * Returns (remappedColumn, timingMs)
   */
  protected def remapProbeKeysWithTiming(
      probeKeys: Table, 
      buildKeys: Table, 
      optimizations: JoinOptimizations): (Option[ColumnVector], Double) = {
    if (optimizations.remapComplexKeysToInts) {
      // Validate that cached state matches configuration
      if (optimizations.cacheRemapping && remappedBuildKeys.isEmpty) {
        throw new IllegalStateException(
          "cacheRemapping is enabled but remappedBuildKeys is not set. " +
          "initializeRemappingWithTiming must be called before remapProbeKeysWithTiming.")
      }
      if (!optimizations.cacheRemapping && remappedBuildKeys.isDefined) {
        throw new IllegalStateException(
          "cacheRemapping is disabled but remappedBuildKeys is set. " +
          "This indicates a configuration/state mismatch.")
      }
      
      if (remapStructures.isEmpty) {
        throw new IllegalStateException(
          "Remapping is enabled but no remapping structures are available. " +
          "initializeRemappingWithTiming must be called before remapProbeKeysWithTiming.")
      }
      
      val t0 = System.nanoTime()
      val remappedCol = KeyRemapping.applyRemapping(probeKeys, remapStructures.get, false)
      Cuda.DEFAULT_STREAM.sync()
      val t1 = System.nanoTime()
      val timingMs = (t1 - t0) / 1e6
      (Some(remappedCol), timingMs)
    } else {
      (None, 0.0)
    }
  }
  
  /**
   * Get the actual build keys to use for join operations.
   * Returns a new Table (owned by caller) with either remapped or copied keys.
   * Caller MUST close the returned Table.
   */
  protected def getActualBuildKeys(
      buildKeys: Table,
      optimizations: JoinOptimizations): Table = {
    if (optimizations.remapComplexKeysToInts) {
      // Validate that cached state matches configuration
      if (optimizations.cacheRemapping && remappedBuildKeys.isEmpty) {
        throw new IllegalStateException(
          "cacheRemapping is enabled but remappedBuildKeys is not set. " +
          "initializeRemappingWithTiming must be called before getActualBuildKeys.")
      }
      if (!optimizations.cacheRemapping && remappedBuildKeys.isDefined) {
        throw new IllegalStateException(
          "cacheRemapping is disabled but remappedBuildKeys is set. " +
          "This indicates a configuration/state mismatch.")
      }
      
      if (remappedBuildKeys.isDefined) {
        // Wrap cached remapped column
        TableCopyHelper.wrapColumn(remappedBuildKeys.get)
      } else if (remapStructures.isDefined) {
        // Non-cached build keys but remapping structures exist: use the existing structure
        val remappedCol = KeyRemapping.applyRemapping(buildKeys, remapStructures.get, true)
        try {
          TableCopyHelper.wrapColumn(remappedCol)
        } finally {
          remappedCol.close()
        }
      } else {
        throw new IllegalStateException(
          "Remapping is enabled but no remapping structures are available. " +
          "initializeRemappingWithTiming must be called before getActualBuildKeys.")
      }
    } else {
      // No remapping: return a copy to maintain uniform ownership semantics
      TableCopyHelper.copyTable(buildKeys)
    }
  }
  
  /**
   * Wrap a remapped ColumnVector in a Table, or copy the original Table if no remapping.
   * Always returns a new Table that the caller OWNS and MUST close.
   * This ensures uniform ownership semantics - caller always closes the result.
   */
  protected def wrapRemappedKeys(
      remappedCol: Option[ColumnVector],
      originalKeys: Table): Table = {
    remappedCol match {
      case Some(col) => TableCopyHelper.wrapColumn(col)
      case None => TableCopyHelper.copyTable(originalKeys)
    }
  }
  
  /**
   * Get the actual probe keys to use for join operations.
   * Combines remapping and wrapping into a single operation with proper resource management.
   * Always returns a new Table that the caller OWNS and MUST close.
   * 
   * This is a convenience method that combines remapProbeKeys() and wrapRemappedKeys(),
   * handling the intermediate ColumnVector resource internally so callers don't need
   * nested try/finally blocks.
   * 
   * @param probeKeys The original probe keys
   * @param buildKeys The build keys (needed for creating remapping structures if not cached)
   * @param optimizations Configuration for optimizations
   * @return A new Table with either remapped or copied probe keys (caller must close)
   */
  protected def getActualProbeKeys(
      probeKeys: Table,
      buildKeys: Table,
      optimizations: JoinOptimizations): Table = {
    val remappedCol = remapProbeKeys(probeKeys, buildKeys, optimizations)
    try {
      wrapRemappedKeys(remappedCol, probeKeys)
    } finally {
      // Close the intermediate ColumnVector if it was created
      remappedCol.foreach(_.close())
    }
  }
  
  /**
   * Get the actual keys to use for join object creation.
   * Always returns a new Table that the caller OWNS and MUST close.
   * This ensures uniform ownership semantics - caller always closes the result.
   */
  protected def getKeysForJoin(
      buildKeys: Table,
      optimizations: JoinOptimizations): Table = {
    if (optimizations.remapComplexKeysToInts) {
      // Validate that cached state matches configuration
      if (optimizations.cacheRemapping && remappedBuildKeys.isEmpty) {
        throw new IllegalStateException(
          "cacheRemapping is enabled but remappedBuildKeys is not set. " +
          "initializeRemappingWithTiming must be called before getKeysForJoin.")
      }
      if (!optimizations.cacheRemapping && remappedBuildKeys.isDefined) {
        throw new IllegalStateException(
          "cacheRemapping is disabled but remappedBuildKeys is set. " +
          "This indicates a configuration/state mismatch.")
      }
      
      if (remappedBuildKeys.isDefined) {
        // Use cached remapped build keys
        TableCopyHelper.wrapColumn(remappedBuildKeys.get)
      } else if (remapStructures.isDefined) {
        // Remap on-demand using existing remapping structures
        val remappedCol = KeyRemapping.applyRemapping(buildKeys, remapStructures.get, true)
        try {
          TableCopyHelper.wrapColumn(remappedCol)
        } finally {
          remappedCol.close()
        }
      } else {
        throw new IllegalStateException(
          "Remapping is enabled but no remapping structures are available. " +
          "initializeRemappingWithTiming must be called before getKeysForJoin.")
      }
    } else {
      TableCopyHelper.copyTable(buildKeys)
    }
  }
  
  /**
   * Clean up remapping resources. Call from holder's close() method.
   */
  protected def closeRemappingResources(): Unit = {
    remapStructures.foreach(_.close())
    remappedBuildKeys.foreach(_.close())
  }
}

/**
 * Shared initialization support for object-based join holders that may cache join objects.
 * Initializes remapping once and, if enabled, creates the cached join object using provided keys.
 */
private trait ObjectJoinInitialization extends RemappingSupport {
  protected var initialized: Boolean = false

  /**
   * Ensure one-time initialization of remapping and optional cached join object.
   * The provided createJoin function will be invoked only when cacheJoinObject is true.
   */
  protected def maybeInit(
      buildKeys: Table,
      optimizations: JoinOptimizations)
      (createJoin: Table => Unit): Unit = {
    if (!initialized) {
      // Initialize remapping structures if configured
      initializeRemapping(buildKeys, optimizations)

      // Create cached join object if caching is enabled
      if (optimizations.cacheJoinObject) {
        val keysForJoin = getKeysForJoin(buildKeys, optimizations)
        try {
          createJoin(keysForJoin)
        } finally {
          keysForJoin.close()
        }
      }
      initialized = true
    }
  }
}

/**
 * Helper object for post-processing inner join results into other join types.
 * Consolidates the common post-processing logic used by PostProcessingBuildHolder
 * and MixedPostProcessingBuildHolder.
 */
private object PostProcessingHelper {
  /**
   * Apply post-processing to inner join gather maps based on the target join type.
   * This takes inner join results and transforms them into the desired join type
   * using JoinPrimitives operations (makeLeftOuter, makeFullOuter, makeSemi, makeAnti).
   * 
   * @param innerMaps The inner join gather maps (2 maps for inner join)
   * @param joinType The target join type to produce
   * @param buildSide Which side is the build side
   * @param buildRowCount Number of rows in the build table
   * @param probeRowCount Number of rows in the probe table
   * @return Post-processed gather maps (count depends on join type)
   * 
   * Resource ownership: Takes ownership of innerMaps and closes them. Returns new maps.
   */
  def applyPostProcessing(
      innerMaps: Array[GatherMap],
      joinType: JoinTypeSpec,
      buildSide: BuildSideSpec,
      buildRowCount: Int,
      probeRowCount: Int): Array[GatherMap] = {
    try {
      joinType match {
        case InnerJoin =>
          // No post-processing needed for inner join
          innerMaps
        
        case LeftOuterJoin =>
          // Convert inner join to left outer
          // If build is left, makeLeftOuter directly on build side
          // If build is right, swap sides to do right outer, then swap back
          val result = if (buildSide == LeftBuild) {
            JoinPrimitives.makeLeftOuter(
              innerMaps(0), innerMaps(1), buildRowCount, probeRowCount)
          } else {
            // Build is right, so we need to swap and do right outer, then swap back
            val rightOuter = JoinPrimitives.makeLeftOuter(
              innerMaps(1), innerMaps(0), probeRowCount, buildRowCount)
            Array(rightOuter(1), rightOuter(0))
          }
          innerMaps.foreach(_.close())
          result
        
        case RightOuterJoin =>
          // Convert inner join to right outer (swap left/right)
          val result = if (buildSide == RightBuild) {
            JoinPrimitives.makeLeftOuter(
              innerMaps(1), innerMaps(0), probeRowCount, buildRowCount)
          } else {
            // Build is left, so we're doing right outer from probe perspective
            val leftOuter = JoinPrimitives.makeLeftOuter(
              innerMaps(0), innerMaps(1), buildRowCount, probeRowCount)
            Array(leftOuter(1), leftOuter(0))
          }
          innerMaps.foreach(_.close())
          result
        
        case FullOuterJoin =>
          // Convert inner join to full outer
          val result = JoinPrimitives.makeFullOuter(
            innerMaps(0), innerMaps(1), buildRowCount, probeRowCount)
          innerMaps.foreach(_.close())
          result
        
        case LeftSemiJoin =>
          // Convert inner join to semi join (unique left indices from build side)
          // Semi join returns only one gather map
          val semiMap = if (buildSide == LeftBuild) {
            JoinPrimitives.makeSemi(innerMaps(0), buildRowCount)
          } else {
            // Build is right, probe is left - use probe side map
            JoinPrimitives.makeSemi(innerMaps(1), probeRowCount)
          }
          innerMaps.foreach(_.close())
          Array(semiMap)
        
        case LeftAntiJoin =>
          // Convert inner join to anti join
          // First get semi join, then anti
          val semiMap = if (buildSide == LeftBuild) {
            JoinPrimitives.makeSemi(innerMaps(0), buildRowCount)
          } else {
            JoinPrimitives.makeSemi(innerMaps(1), probeRowCount)
          }
          innerMaps.foreach(_.close())
          
          val antiMap = if (buildSide == LeftBuild) {
            JoinPrimitives.makeAnti(semiMap, buildRowCount)
          } else {
            JoinPrimitives.makeAnti(semiMap, probeRowCount)
          }
          semiMap.close()
          Array(antiMap)
      }
    } catch {
      case e: Exception =>
        innerMaps.foreach(_.close())
        throw e
    }
  }
}

/**
 * Non-conditional build holder for inner hash joins.
 * Uses HashJoin or DistinctHashJoin (can be cached if cacheJoinObject is enabled).
 * Supports distinct join optimization with cacheDistinctFlag.
 */
private class InnerHashBuildHolder(
  val buildTable: Table,
  val buildKeys: Table,
  compareNullsEqual: Boolean,
  tablesWereSwapped: Boolean,
  optimizations: JoinOptimizations
) extends NonConditionalBuildHolder with RemappingSupport with DistinctnessSupport {
  
  private var cachedJoinObject: Option[Either[HashJoin, DistinctHashJoin]] = None
  private var initialized = false
  
  private def ensureInitialized(): Unit = {
    if (!initialized) {
      // Initialize remapping if enabled
      initializeRemapping(buildKeys, optimizations)
      
      if (optimizations.cacheJoinObject) {
        // Determine which keys to use for join object creation
        val keysForJoin = getKeysForJoin(buildKeys, optimizations)
        
        try {
          // Check if build keys are distinct
          val isDistinct = checkDistinctness(keysForJoin, InnerJoin, optimizations)
          
          // Create appropriate join object based on distinctness
          cachedJoinObject = Some(if (isDistinct) {
            Right(DistinctHashJoin.create(keysForJoin, compareNullsEqual))
          } else {
            Left(HashJoin.create(keysForJoin, compareNullsEqual))
          })
        } finally {
          keysForJoin.close()
        }
      }
      initialized = true
    }
  }
  
  def join(probeKeys: Table): Array[GatherMap] = {
    ensureInitialized()
    
    val actualProbeKeys = getActualProbeKeys(probeKeys, buildKeys, optimizations)
    
    try {
      if (cachedJoinObject.isDefined) {
        cachedJoinObject.get match {
          case Left(hj) => hj.innerJoin(actualProbeKeys)
          case Right(dhj) => dhj.innerJoin(actualProbeKeys)
        }
      } else {
        // Non-cached join path
        val actualBuildKeys = getActualBuildKeys(buildKeys, optimizations)
        
        try {
          // Check if build keys are distinct
          val isDistinct = checkDistinctness(actualBuildKeys, InnerJoin, optimizations)
          
          // Create appropriate join object based on distinctness
          if (isDistinct) {
            val tempDistinctHashJoin = DistinctHashJoin.create(actualBuildKeys, compareNullsEqual)
            try {
              tempDistinctHashJoin.innerJoin(actualProbeKeys)
            } finally {
              tempDistinctHashJoin.close()
            }
          } else {
            val tempHashJoin = HashJoin.create(actualBuildKeys, compareNullsEqual)
            try {
              tempHashJoin.innerJoin(actualProbeKeys)
            } finally {
              tempHashJoin.close()
            }
          }
        } finally {
          actualBuildKeys.close()
        }
      }
    } finally {
      actualProbeKeys.close()
    }
  }
  
  override def joinWithDetailedTimings(
      probeKeys: Table): (Array[GatherMap], Option[DetailedTimings]) = {
    var remapStructureBuildMs = 0.0
    var remapBuildKeysMsTotal = 0.0
    var createBuildObjectMsTotal = 0.0
    
    if (!initialized) {
      val (structMs, buildMs) = initializeRemappingWithTiming(buildKeys, optimizations)
      remapStructureBuildMs += structMs
      remapBuildKeysMsTotal += buildMs
      
      if (optimizations.cacheJoinObject) {
        val keysForJoin = getKeysForJoin(buildKeys, optimizations)
        try {
          val createStart = System.nanoTime()
          val isDistinct = checkDistinctness(keysForJoin, InnerJoin, optimizations)
          
          cachedJoinObject = Some(if (isDistinct) {
            Right(DistinctHashJoin.create(keysForJoin, compareNullsEqual))
          } else {
            Left(HashJoin.create(keysForJoin, compareNullsEqual))
          })
          Cuda.DEFAULT_STREAM.sync()
          val createEnd = System.nanoTime()
          createBuildObjectMsTotal += (createEnd - createStart) / 1e6
        } finally {
          keysForJoin.close()
        }
      }
      initialized = true
    }
    
    val (remappedProbeCol, remapProbeKeysMs) = remapProbeKeysWithTiming(
      probeKeys, buildKeys, optimizations)
    val actualProbeKeys = wrapRemappedKeys(remappedProbeCol, probeKeys)
    
    try {
      val (gatherMaps, additionalRemapBuildMs, additionalCreateMs, executeJoinMs) =
        cachedJoinObject match {
          case Some(existingJoin) =>
            val joinStart = System.nanoTime()
            val maps = existingJoin match {
              case Left(hashJoin) => hashJoin.innerJoin(actualProbeKeys)
              case Right(distinctJoin) => distinctJoin.innerJoin(actualProbeKeys)
            }
            Cuda.DEFAULT_STREAM.sync()
            val joinEnd = System.nanoTime()
            (maps, 0.0, 0.0, (joinEnd - joinStart) / 1e6)
          
          case None =>
            val remapBuildStart = System.nanoTime()
            val actualBuildKeys = getActualBuildKeys(buildKeys, optimizations)
            Cuda.DEFAULT_STREAM.sync()
            val remapBuildEnd = System.nanoTime()
            val remapBuildMs = (remapBuildEnd - remapBuildStart) / 1e6
            
            try {
              val createStart = System.nanoTime()
              val isDistinct = checkDistinctness(actualBuildKeys, InnerJoin, optimizations)
              
              if (isDistinct) {
                val joinObj = DistinctHashJoin.create(actualBuildKeys, compareNullsEqual)
                Cuda.DEFAULT_STREAM.sync()
                val createEnd = System.nanoTime()
                val createMs = (createEnd - createStart) / 1e6
                
                try {
                  val joinStart = System.nanoTime()
                  val maps = joinObj.innerJoin(actualProbeKeys)
                  Cuda.DEFAULT_STREAM.sync()
                  val joinEnd = System.nanoTime()
                  val executeMs = (joinEnd - joinStart) / 1e6
                  (maps, remapBuildMs, createMs, executeMs)
                } finally {
                  joinObj.close()
                }
              } else {
                val joinObj = HashJoin.create(actualBuildKeys, compareNullsEqual)
                Cuda.DEFAULT_STREAM.sync()
                val createEnd = System.nanoTime()
                val createMs = (createEnd - createStart) / 1e6
                
                try {
                  val joinStart = System.nanoTime()
                  val maps = joinObj.innerJoin(actualProbeKeys)
                  Cuda.DEFAULT_STREAM.sync()
                  val joinEnd = System.nanoTime()
                  val executeMs = (joinEnd - joinStart) / 1e6
                  (maps, remapBuildMs, createMs, executeMs)
                } finally {
                  joinObj.close()
                }
              }
            } finally {
              actualBuildKeys.close()
            }
        }
      
      remapBuildKeysMsTotal += additionalRemapBuildMs
      createBuildObjectMsTotal += additionalCreateMs
      
      val timings = DetailedTimings(
        remapStructureBuildMs = remapStructureBuildMs,
        remapBuildKeysMs = remapBuildKeysMsTotal,
        remapProbeKeysMs = remapProbeKeysMs,
        createBuildObjectMs = createBuildObjectMsTotal,
        executeJoinMs = executeJoinMs
      )
      
      (gatherMaps, Some(timings))
    } finally {
      actualProbeKeys.close()
      remappedProbeCol.foreach(_.close())
    }
  }
  
  def close(): Unit = {
    cachedJoinObject.foreach {
      case Left(hj) => hj.close()
      case Right(dhj) => dhj.close()
    }
    closeRemappingResources()
    buildKeys.close()
    buildTable.close()
  }
}

/**
 * Non-conditional build holder for left outer hash joins.
 * Uses HashJoin (can be cached if cacheJoinObject is enabled).
 * Note: DistinctHashJoin.leftJoin() returns a single GatherMap (for remapping only),
 * not two maps, so it cannot be used for left outer joins. Always use HashJoin.
 */
private class LeftOuterHashBuildHolder(
  val buildTable: Table,
  val buildKeys: Table,
  compareNullsEqual: Boolean,
  tablesWereSwapped: Boolean,
  optimizations: JoinOptimizations
) extends NonConditionalBuildHolder with ObjectJoinInitialization {
  
  private var cachedJoinObject: Option[HashJoin] = None
  
  private def ensureInitialized(): Unit = {
    maybeInit(buildKeys, optimizations) { keysForJoin =>
      // Left outer join always uses HashJoin
      cachedJoinObject = Some(HashJoin.create(keysForJoin, compareNullsEqual))
    }
  }
  
  def join(probeKeys: Table): Array[GatherMap] = {
    ensureInitialized()
    
    val actualProbeKeys = getActualProbeKeys(probeKeys, buildKeys, optimizations)
    
    try {
      if (cachedJoinObject.isDefined) {
        cachedJoinObject.get.leftJoin(actualProbeKeys)
      } else {
        // Non-cached join path
        val actualBuildKeys = getActualBuildKeys(buildKeys, optimizations)
        
        try {
          val tempHashJoin = HashJoin.create(actualBuildKeys, compareNullsEqual)
          try {
            tempHashJoin.leftJoin(actualProbeKeys)
          } finally {
            tempHashJoin.close()
          }
        } finally {
          actualBuildKeys.close()
        }
      }
    } finally {
      actualProbeKeys.close()
    }
  }
  
  def close(): Unit = {
    cachedJoinObject.foreach(_.close())
    closeRemappingResources()
    buildKeys.close()
    buildTable.close()
  }
}

/**
 * Non-conditional build holder for right outer hash joins.
 * Right outer join is implemented as left outer join with swapped sides.
 * Uses HashJoin (can be cached if cacheJoinObject is enabled).
 * Note: DistinctHashJoin.leftJoin() returns a single GatherMap (for remapping only),
 * not two maps, so it cannot be used for right outer joins. Always use HashJoin.
 */
private class RightOuterHashBuildHolder(
  val buildTable: Table,
  val buildKeys: Table,
  compareNullsEqual: Boolean,
  tablesWereSwapped: Boolean,
  optimizations: JoinOptimizations
) extends NonConditionalBuildHolder with ObjectJoinInitialization {
  
  private var cachedJoinObject: Option[HashJoin] = None
  
  private def ensureInitialized(): Unit = {
    maybeInit(buildKeys, optimizations) { keysForJoin =>
      cachedJoinObject = Some(HashJoin.create(keysForJoin, compareNullsEqual))
    }
  }
  
  def join(probeKeys: Table): Array[GatherMap] = {
    ensureInitialized()
    
    val actualProbeKeys = getActualProbeKeys(probeKeys, buildKeys, optimizations)
    
    try {
      // Right outer join is implemented as left outer join with swapped sides
      val result = if (cachedJoinObject.isDefined) {
        cachedJoinObject.get.leftJoin(actualProbeKeys)
      } else {
        val actualBuildKeys = getActualBuildKeys(buildKeys, optimizations)
        
        try {
          val tempHashJoin = HashJoin.create(actualBuildKeys, compareNullsEqual)
          try {
            tempHashJoin.leftJoin(actualProbeKeys)
          } finally {
            tempHashJoin.close()
          }
        } finally {
          actualBuildKeys.close()
        }
      }
      
      // Swap the gather maps to convert left outer to right outer
      Array(result(1), result(0))
    } finally {
      actualProbeKeys.close()
    }
  }
  
  def close(): Unit = {
    cachedJoinObject.foreach(_.close())
    closeRemappingResources()
    buildKeys.close()
    buildTable.close()
  }
}

/**
 * Non-conditional build holder for full outer hash joins.
 * Uses HashJoin (can be cached if cacheJoinObject is enabled).
 * Does NOT support distinct join optimization (FullOuter doesn't support DistinctHashJoin).
 */
private class FullOuterHashBuildHolder(
  val buildTable: Table,
  val buildKeys: Table,
  compareNullsEqual: Boolean,
  tablesWereSwapped: Boolean,
  optimizations: JoinOptimizations
) extends NonConditionalBuildHolder with ObjectJoinInitialization {
  
  private var cachedJoinObject: Option[HashJoin] = None
  
  private def ensureInitialized(): Unit = {
    maybeInit(buildKeys, optimizations) { keysForJoin =>
      cachedJoinObject = Some(HashJoin.create(keysForJoin, compareNullsEqual))
    }
  }
  
  def join(probeKeys: Table): Array[GatherMap] = {
    ensureInitialized()
    
    val actualProbeKeys = getActualProbeKeys(probeKeys, buildKeys, optimizations)
    
    try {
      if (cachedJoinObject.isDefined) {
        cachedJoinObject.get.fullJoin(actualProbeKeys)
      } else {
        val actualBuildKeys = getActualBuildKeys(buildKeys, optimizations)
        
        try {
          val tempHashJoin = HashJoin.create(actualBuildKeys, compareNullsEqual)
          try {
            tempHashJoin.fullJoin(actualProbeKeys)
          } finally {
            tempHashJoin.close()
          }
        } finally {
          actualBuildKeys.close()
        }
      }
    } finally {
      actualProbeKeys.close()
    }
  }
  
  def close(): Unit = {
    cachedJoinObject.foreach(_.close())
    closeRemappingResources()
    buildKeys.close()
    buildTable.close()
  }
}

/**
 * Non-conditional build holder for semi and anti hash joins.
 * Uses FilteredJoin API for direct semi/anti join operations.
 * Supports join object caching (FilteredJoin can be built once and probed multiple times).
 * Does NOT support distinct join optimization (FilteredJoin doesn't have a distinct variant).
 */
private class SemiAntiHashBuildHolder(
  joinType: JoinTypeSpec,
  val buildTable: Table,
  val buildKeys: Table,
  compareNullsEqual: Boolean,
  optimizations: JoinOptimizations
) extends NonConditionalBuildHolder with ObjectJoinInitialization {
  
  private var cachedJoinObject: Option[FilteredJoin] = None
  
  private def ensureInitialized(): Unit = {
    maybeInit(buildKeys, optimizations) { keysForJoin =>
      cachedJoinObject = Some(FilteredJoin.create(keysForJoin, compareNullsEqual))
    }
  }
  
  def join(probeKeys: Table): Array[GatherMap] = {
    ensureInitialized()
    
    val actualProbeKeys = getActualProbeKeys(probeKeys, buildKeys, optimizations)
    
    try {
      if (cachedJoinObject.isDefined) {
        // Use cached FilteredJoin object
        joinType match {
          case LeftSemiJoin =>
            Array(cachedJoinObject.get.semiJoin(actualProbeKeys))
          
          case LeftAntiJoin =>
            Array(cachedJoinObject.get.antiJoin(actualProbeKeys))
          
          case _ =>
            throw new IllegalArgumentException(
              s"SemiAntiHashBuildHolder only supports LeftSemiJoin and " +
              s"LeftAntiJoin, got $joinType")
        }
      } else {
        // Non-cached path
        val actualBuildKeys = getActualBuildKeys(buildKeys, optimizations)
        
        try {
          val tempFilteredJoin = FilteredJoin.create(actualBuildKeys, compareNullsEqual)
          try {
            joinType match {
              case LeftSemiJoin =>
                Array(tempFilteredJoin.semiJoin(actualProbeKeys))
              
              case LeftAntiJoin =>
                Array(tempFilteredJoin.antiJoin(actualProbeKeys))
              
              case _ =>
                throw new IllegalArgumentException(
                  s"SemiAntiHashBuildHolder only supports LeftSemiJoin and " +
                  s"LeftAntiJoin, got $joinType")
            }
          } finally {
            tempFilteredJoin.close()
          }
        } finally {
          actualBuildKeys.close()
        }
      }
    } finally {
      actualProbeKeys.close()
    }
  }
  
  def close(): Unit = {
    cachedJoinObject.foreach(_.close())
    closeRemappingResources()
    buildKeys.close()
    buildTable.close()
  }
}

/**
 * Build holder for inner hash joins using Table direct APIs (no join object).
 * Uses Table.innerJoinGatherMaps() or Table.innerDistinctJoinGatherMaps().
 * Cannot cache join objects. Supports distinct join optimization with cacheDistinctFlag.
 */
private class InnerHashDirectBuildHolder(
  val buildTable: Table,
  val buildKeys: Table,
  compareNullsEqual: Boolean,
  tablesWereSwapped: Boolean,
  optimizations: JoinOptimizations
) extends NonConditionalBuildHolder with RemappingSupport with DistinctnessSupport {
  
  private var initialized = false
  
  private def ensureInitialized(): Unit = {
    if (!initialized) {
      // Initialize remapping if enabled
      initializeRemapping(buildKeys, optimizations)
      initialized = true
    }
  }
  
  def join(probeKeys: Table): Array[GatherMap] = {
    ensureInitialized()
    
    val actualBuildKeys = getActualBuildKeys(buildKeys, optimizations)
    val actualProbeKeys = getActualProbeKeys(probeKeys, buildKeys, optimizations)
    
    try {
      // Check if build keys are distinct
      val isDistinct = checkDistinctness(actualBuildKeys, InnerJoin, optimizations)
      
      // Use Table direct APIs
      if (isDistinct) {
        actualBuildKeys.innerDistinctJoinGatherMaps(actualProbeKeys, compareNullsEqual)
      } else {
        actualBuildKeys.innerJoinGatherMaps(actualProbeKeys, compareNullsEqual)
      }
    } finally {
      actualBuildKeys.close()
      actualProbeKeys.close()
    }
  }
  
  def close(): Unit = {
    closeRemappingResources()
    buildKeys.close()
    buildTable.close()
  }
}

/**
 * Build holder for left outer hash joins using Table direct APIs (no join object).
 * Uses Table.leftJoinGatherMaps().
 * Cannot cache join objects.
 */
private class LeftOuterHashDirectBuildHolder(
  val buildTable: Table,
  val buildKeys: Table,
  compareNullsEqual: Boolean,
  tablesWereSwapped: Boolean,
  optimizations: JoinOptimizations
) extends NonConditionalBuildHolder with RemappingSupport {
  
  private var initialized = false
  
  private def ensureInitialized(): Unit = {
    if (!initialized) {
      initializeRemapping(buildKeys, optimizations)
      initialized = true
    }
  }
  
  def join(probeKeys: Table): Array[GatherMap] = {
    ensureInitialized()
    
    val actualBuildKeys = getActualBuildKeys(buildKeys, optimizations)
    val actualProbeKeys = getActualProbeKeys(probeKeys, buildKeys, optimizations)
    
    try {
      actualBuildKeys.leftJoinGatherMaps(actualProbeKeys, compareNullsEqual)
    } finally {
      actualBuildKeys.close()
      actualProbeKeys.close()
    }
  }
  
  def close(): Unit = {
    closeRemappingResources()
    buildKeys.close()
    buildTable.close()
  }
}

/**
 * Build holder for right outer hash joins using Table direct APIs (no join object).
 * Right outer join is implemented as left outer join with swapped sides.
 * Uses Table.leftJoinGatherMaps().
 * Cannot cache join objects.
 */
private class RightOuterHashDirectBuildHolder(
  val buildTable: Table,
  val buildKeys: Table,
  compareNullsEqual: Boolean,
  tablesWereSwapped: Boolean,
  optimizations: JoinOptimizations
) extends NonConditionalBuildHolder with RemappingSupport {
  
  private var initialized = false
  
  private def ensureInitialized(): Unit = {
    if (!initialized) {
      initializeRemapping(buildKeys, optimizations)
      initialized = true
    }
  }
  
  def join(probeKeys: Table): Array[GatherMap] = {
    ensureInitialized()
    
    val actualBuildKeys = getActualBuildKeys(buildKeys, optimizations)
    val actualProbeKeys = getActualProbeKeys(probeKeys, buildKeys, optimizations)
    
    try {
      // Right outer join is implemented as left outer join with swapped sides
      val result = actualBuildKeys.leftJoinGatherMaps(actualProbeKeys, compareNullsEqual)
      // Swap the gather maps to convert left outer to right outer
      Array(result(1), result(0))
    } finally {
      actualBuildKeys.close()
      actualProbeKeys.close()
    }
  }
  
  def close(): Unit = {
    closeRemappingResources()
    buildKeys.close()
    buildTable.close()
  }
}

/**
 * Build holder for full outer hash joins using Table direct APIs (no join object).
 * Uses Table.fullJoinGatherMaps().
 * Cannot cache join objects.
 */
private class FullOuterHashDirectBuildHolder(
  val buildTable: Table,
  val buildKeys: Table,
  compareNullsEqual: Boolean,
  tablesWereSwapped: Boolean,
  optimizations: JoinOptimizations
) extends NonConditionalBuildHolder with RemappingSupport {
  
  private var initialized = false
  
  private def ensureInitialized(): Unit = {
    if (!initialized) {
      initializeRemapping(buildKeys, optimizations)
      initialized = true
    }
  }
  
  def join(probeKeys: Table): Array[GatherMap] = {
    ensureInitialized()
    
    val actualBuildKeys = getActualBuildKeys(buildKeys, optimizations)
    val actualProbeKeys = getActualProbeKeys(probeKeys, buildKeys, optimizations)
    
    try {
      actualBuildKeys.fullJoinGatherMaps(actualProbeKeys, compareNullsEqual)
    } finally {
      actualBuildKeys.close()
      actualProbeKeys.close()
    }
  }
  
  def close(): Unit = {
    closeRemappingResources()
    buildKeys.close()
    buildTable.close()
  }
}

/**
 * Post-processing build holder for all join types.
 * Does inner join first, then applies post-processing (makeLeftOuter, makeSemi, etc.).
 * Supports both hash and sort-merge strategies with distinct join optimization.
 */
private class PostProcessingBuildHolder(
  joinType: JoinTypeSpec,
  strategy: JoinStrategySpec,
  val buildTable: Table,
  val buildKeys: Table,
  compareNullsEqual: Boolean,
  tablesWereSwapped: Boolean,
  optimizations: JoinOptimizations,
  buildSide: BuildSideSpec,
  leftRowCount: Long,
  rightRowCount: Long
) extends NonConditionalBuildHolder with RemappingSupport with DistinctnessSupport {
  
  // Either[HashJoin | DistinctHashJoin, SortMergeJoin]
  private var cachedJoinObject:
      Option[Either[Either[HashJoin, DistinctHashJoin], SortMergeJoin]] = None
  private var initialized = false
  
  private def ensureInitialized(): Unit = {
    if (!initialized) {
      // Validate SortMergeJoin key types BEFORE any initialization
      if (strategy == SortObjectWithPostStrategy) {
        KeyRemappingHelper.validateSortMergeKeyTypes(
          buildKeys, optimizations.remapComplexKeysToInts)
      }
      
      // Initialize remapping if enabled
      initializeRemapping(buildKeys, optimizations)
      
      if (optimizations.cacheJoinObject) {
        // Determine which keys to use for join object creation
        val keysForJoin = getKeysForJoin(buildKeys, optimizations)
        
        try {
          // Check distinctness for hash joins if optimization enabled
          val isDistinct = if (strategy == HashObjectWithPostStrategy) {
            checkDistinctness(keysForJoin, joinType, optimizations)
          } else {
            false
          }
          
          cachedJoinObject = Some(strategy match {
            case HashObjectWithPostStrategy =>
              if (isDistinct) {
                Left(Right(DistinctHashJoin.create(keysForJoin, compareNullsEqual)))
              } else {
                Left(Left(HashJoin.create(keysForJoin, compareNullsEqual)))
              }
            case SortObjectWithPostStrategy =>
              Right(SortMergeJoin.create(keysForJoin, false /* isBuildSorted */, compareNullsEqual))
            case _ =>
              throw new IllegalArgumentException(
                s"Unsupported strategy for post-processing: $strategy")
          })
        } finally {
          keysForJoin.close()
        }
      }
      initialized = true
    }
  }
  
  def join(probeKeys: Table): Array[GatherMap] = {
    ensureInitialized()
    
    // Determine actual row counts based on build side
    val (buildRowCount, probeRowCount) = if (buildSide == LeftBuild) {
      (leftRowCount, rightRowCount)
    } else {
      (rightRowCount, leftRowCount)
    }
    
    val actualProbeKeys = getActualProbeKeys(probeKeys, buildKeys, optimizations)
    
    try {
      // Step 1: Do inner join
      val innerMaps = if (cachedJoinObject.isDefined) {
        cachedJoinObject.get match {
          case Left(Left(hj)) => hj.innerJoin(actualProbeKeys)
          case Left(Right(dhj)) => dhj.innerJoin(actualProbeKeys)
          case Right(smj) => smj.innerJoin(actualProbeKeys, false /* isProbeSorted */)
        }
      } else {
        // Non-cached path: create+probe+destroy join object each time
        val actualBuildKeys = getActualBuildKeys(buildKeys, optimizations)
        
        try {
          strategy match {
            case HashObjectWithPostStrategy =>
              // Check distinctness for non-cached path if optimization enabled
              val isDistinct = checkDistinctness(actualBuildKeys, joinType, optimizations)
              
              // Use HashJoin or DistinctHashJoin API (not Table one-shot)
              if (isDistinct) {
                val dhj = DistinctHashJoin.create(actualBuildKeys, compareNullsEqual)
                try {
                  dhj.innerJoin(actualProbeKeys)
                } finally {
                  dhj.close()
                }
              } else {
                val hj = HashJoin.create(actualBuildKeys, compareNullsEqual)
                try {
                  hj.innerJoin(actualProbeKeys)
                } finally {
                  hj.close()
                }
              }
              
            case HashDirectWithPostStrategy =>
              // Use Table direct APIs (no objects)
              val isDistinct = checkDistinctness(actualBuildKeys, joinType, optimizations)
              
              if (isDistinct) {
                actualBuildKeys.innerDistinctJoinGatherMaps(actualProbeKeys, compareNullsEqual)
              } else {
                actualBuildKeys.innerJoinGatherMaps(actualProbeKeys, compareNullsEqual)
              }
            
            case SortObjectWithPostStrategy =>
              // Use SortMergeJoin API (not cached)
              // Key types already validated in ensureInitialized
              val smj = SortMergeJoin.create(
                actualBuildKeys, false /* isBuildSorted */, compareNullsEqual)
              try {
                smj.innerJoin(actualProbeKeys, false /* isProbeSorted */)
              } finally {
                smj.close()
              }
            
            case SortDirectWithPostStrategy =>
              // Use JoinPrimitives sort API (no objects)
              JoinPrimitives.sortMergeInnerJoin(
                actualBuildKeys, actualProbeKeys,
                false /* isLeftSorted */, false /* isRightSorted */,
                compareNullsEqual)
              
            case _ =>
              throw new IllegalArgumentException(
                s"Unsupported strategy for post-processing: $strategy")
          }
        } finally {
          actualBuildKeys.close()
        }
      }
  
  // Step 2: Apply post-processing based on join type
  PostProcessingHelper.applyPostProcessing(
    innerMaps, joinType, buildSide, buildRowCount.toInt, probeRowCount.toInt)
    } finally {
      actualProbeKeys.close()
    }
  }
  
  override def joinWithDetailedTimings(
      probeKeys: Table): (Array[GatherMap], Option[DetailedTimings]) = {
    // Only HashObjectWithPostStrategy and SortObjectWithPostStrategy support detailed timings
    if (strategy != HashObjectWithPostStrategy && strategy != SortObjectWithPostStrategy) {
      throw new UnsupportedOperationException(
        s"Detailed timings are not supported for strategy: $strategy. " +
        s"Only HashObjectWithPostStrategy and SortObjectWithPostStrategy support detailed timings.")
    }
    
    // Track initialization timing (structure build + remap build keys)
    var remapStructureBuildMs = 0.0
    var remapBuildKeysMs = 0.0
    var createBuildObjectMs = 0.0
    
    if (!initialized) {
      // Validate SortMergeJoin key types BEFORE any initialization
      if (strategy == SortObjectWithPostStrategy) {
        KeyRemappingHelper.validateSortMergeKeyTypes(
          buildKeys, optimizations.remapComplexKeysToInts)
      }
      
      // Initialize remapping with timing
      val (structMs, buildMs) = initializeRemappingWithTiming(buildKeys, optimizations)
      remapStructureBuildMs = structMs
      remapBuildKeysMs = buildMs
      
      if (optimizations.cacheJoinObject) {
        // Time the build object creation
        val keysForJoin = getKeysForJoin(buildKeys, optimizations)
        
        try {
          val t0 = System.nanoTime()
          
          // Check distinctness for hash joins if optimization enabled
          val isDistinct = if (strategy == HashObjectWithPostStrategy) {
            checkDistinctness(keysForJoin, joinType, optimizations)
          } else {
            false
          }
          
          cachedJoinObject = Some(strategy match {
            case HashObjectWithPostStrategy =>
              if (isDistinct) {
                Left(Right(DistinctHashJoin.create(keysForJoin, compareNullsEqual)))
              } else {
                Left(Left(HashJoin.create(keysForJoin, compareNullsEqual)))
              }
            case SortObjectWithPostStrategy =>
              Right(SortMergeJoin.create(keysForJoin, false /* isBuildSorted */, compareNullsEqual))
            case _ =>
              throw new IllegalArgumentException(
                s"Unsupported strategy for post-processing: $strategy")
          })
          
          Cuda.DEFAULT_STREAM.sync()
          val t1 = System.nanoTime()
          createBuildObjectMs = (t1 - t0) / 1e6
          
        } finally {
          keysForJoin.close()
        }
      }
      initialized = true
    }
    
    // Determine actual row counts based on build side
    val (buildRowCount, probeRowCount) = if (buildSide == LeftBuild) {
      (leftRowCount, rightRowCount)
    } else {
      (rightRowCount, leftRowCount)
    }
    
    // Remap probe keys with timing
    val (remappedProbeCol, remapProbeKeysMs) = remapProbeKeysWithTiming(
      probeKeys, buildKeys, optimizations)
    val actualProbeKeys = wrapRemappedKeys(remappedProbeCol, probeKeys)
    
    try {
      val t0 = System.nanoTime()
      
      // Step 1: Do inner join
      val innerMaps = if (cachedJoinObject.isDefined) {
        cachedJoinObject.get match {
          case Left(Left(hj)) => hj.innerJoin(actualProbeKeys)
          case Left(Right(dhj)) => dhj.innerJoin(actualProbeKeys)
          case Right(smj) => smj.innerJoin(actualProbeKeys, false /* isProbeSorted */)
        }
      } else {
        // Non-cached path - need to remap build keys and create join object
        val t_remap0 = System.nanoTime()
        val actualBuildKeys = getActualBuildKeys(buildKeys, optimizations)
        Cuda.DEFAULT_STREAM.sync()
        val t_remap1 = System.nanoTime()
        
        // If remapping is enabled and not cached, this timing captures the remap operation
        if (optimizations.remapComplexKeysToInts && !optimizations.cacheRemapping) {
          remapBuildKeysMs = (t_remap1 - t_remap0) / 1e6
        }
        
        try {
          val t_build0 = System.nanoTime()
          val joinObj = strategy match {
            case HashObjectWithPostStrategy =>
              val isDistinct = checkDistinctness(actualBuildKeys, joinType, optimizations)
              if (isDistinct) {
                Left(Right(DistinctHashJoin.create(actualBuildKeys, compareNullsEqual)))
              } else {
                Left(Left(HashJoin.create(actualBuildKeys, compareNullsEqual)))
              }
            case SortObjectWithPostStrategy =>
              Right(SortMergeJoin.create(
                actualBuildKeys, false /* isBuildSorted */, compareNullsEqual))
            case _ =>
              throw new IllegalArgumentException(
                s"Unsupported strategy for post-processing: $strategy")
          }
          
          Cuda.DEFAULT_STREAM.sync()
          val t_build1 = System.nanoTime()
          createBuildObjectMs = (t_build1 - t_build0) / 1e6
          
          val maps = joinObj match {
            case Left(Left(hj)) =>
              try { hj.innerJoin(actualProbeKeys) } finally { hj.close() }
            case Left(Right(dhj)) =>
              try { dhj.innerJoin(actualProbeKeys) } finally { dhj.close() }
            case Right(smj) =>
              try {
                smj.innerJoin(actualProbeKeys, false /* isProbeSorted */)
              } finally {
                smj.close()
              }
          }
          maps
        } finally {
          actualBuildKeys.close()
        }
      }
      
      // Step 2: Apply post-processing based on join type
      val result = PostProcessingHelper.applyPostProcessing(
        innerMaps, joinType, buildSide, buildRowCount.toInt, probeRowCount.toInt)
      
      Cuda.DEFAULT_STREAM.sync()
      val t1 = System.nanoTime()
      val executeJoinMs = (t1 - t0) / 1e6
      
      val timings = DetailedTimings(
        remapStructureBuildMs = remapStructureBuildMs,
        remapBuildKeysMs = remapBuildKeysMs,
        remapProbeKeysMs = remapProbeKeysMs,
        createBuildObjectMs = createBuildObjectMs,
        executeJoinMs = executeJoinMs
      )
      
      (result, Some(timings))
      
    } finally {
      actualProbeKeys.close()
      remappedProbeCol.foreach(_.close())
    }
  }
  
  def close(): Unit = {
    cachedJoinObject.foreach {
      case Left(Left(hj)) => hj.close()
      case Left(Right(dhj)) => dhj.close()
      case Right(smj) => smj.close()
    }
    closeRemappingResources()
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
) extends MixedConditionalBuildHolder with RemappingSupport {
  
  private var initialized = false
  
  private def ensureInitialized(): Unit = {
    if (!initialized) {
      initializeRemapping(buildKeys, optimizations)
      initialized = true
    }
  }
  
  def join(probeKeys: Table, probeTable: Table): Array[GatherMap] = {
    ensureInitialized()
    
    val actualBuildKeys = getActualBuildKeys(buildKeys, optimizations)
    val actualProbeKeys = getActualProbeKeys(probeKeys, buildKeys, optimizations)
    
    try {
      val nullEq = if (compareNullsEqual) NullEquality.EQUAL else NullEquality.UNEQUAL
      Table.mixedInnerJoinGatherMaps(
        /* leftKeys  = */ actualBuildKeys,
        /* rightKeys = */ actualProbeKeys,
        /* leftCond  = */ buildTable,
        /* rightCond = */ probeTable,
        /* condition = */ astExpression,
        /* nullEq    = */ nullEq)
    } finally {
      actualBuildKeys.close()
      actualProbeKeys.close()
    }
  }
  
  def close(): Unit = {
    closeRemappingResources()
    buildKeys.close()
    buildTable.close()
    // astExpression is a reference, not owned by holder
  }
}

/**
 * Mixed conditional build holder using post-processing approach.
 * Does inner join first, then applies AST filter, then applies join type post-processing.
 * Supports all join types with distinct optimization.
 */
private class MixedPostProcessingBuildHolder(
  joinType: JoinTypeSpec,
  strategy: JoinStrategySpec,
  val buildTable: Table,
  val buildKeys: Table,
  val astExpression: CompiledExpression,
  compareNullsEqual: Boolean,
  tablesWereSwapped: Boolean,
  optimizations: JoinOptimizations,
  buildSide: BuildSideSpec,
  leftRowCount: Long,
  rightRowCount: Long
) extends MixedConditionalBuildHolder with RemappingSupport with DistinctnessSupport {
  
  // Either[HashJoin | DistinctHashJoin, SortMergeJoin]
  private var cachedJoinObject:
      Option[Either[Either[HashJoin, DistinctHashJoin], SortMergeJoin]] = None
  private var initialized = false
  
  private def ensureInitialized(): Unit = {
    if (!initialized) {
      // Validate SortMergeJoin key types BEFORE any initialization
      if (strategy == SortObjectWithPostStrategy) {
        KeyRemappingHelper.validateSortMergeKeyTypes(
          buildKeys, optimizations.remapComplexKeysToInts)
      }
      
      // Initialize remapping if enabled
      if (optimizations.remapComplexKeysToInts && optimizations.cacheRemapping) {
        val remap = KeyRemapping.createRemapStructures(buildKeys, 
          KeyRemapping.NullEqualityMode.SPARK_EQUALITY)
        remapStructures = Some(remap)
        remappedBuildKeys = Some(KeyRemapping.applyRemapping(buildKeys, remap, true))
      }
      
      // Only Object strategies can cache join objects
      val canCacheJoinObject = strategy match {
        case HashObjectWithPostStrategy | SortObjectWithPostStrategy => true
        case _ => false
      }
      
      if (optimizations.cacheJoinObject && canCacheJoinObject) {
        // Determine which keys to use for join object creation
        val keysForJoin = getKeysForJoin(buildKeys, optimizations)
        
        try {
          // Check distinctness for hash joins if optimization enabled
          val isDistinct = if (strategy == HashObjectWithPostStrategy) {
            checkDistinctness(keysForJoin, joinType, optimizations)
          } else {
            false
          }
          
          cachedJoinObject = Some(strategy match {
            case HashObjectWithPostStrategy =>
              if (isDistinct) {
                Left(Right(DistinctHashJoin.create(keysForJoin, compareNullsEqual)))
              } else {
                Left(Left(HashJoin.create(keysForJoin, compareNullsEqual)))
              }
            case SortObjectWithPostStrategy =>
              Right(SortMergeJoin.create(keysForJoin, false /* isBuildSorted */, compareNullsEqual))
            case _ =>
              throw new IllegalArgumentException(s"Unsupported strategy: $strategy")
          })
        } finally {
          keysForJoin.close()
        }
      }
      initialized = true
    }
  }
  
  def join(probeKeys: Table, probeTable: Table): Array[GatherMap] = {
    ensureInitialized()
    
    // Determine actual row counts based on build side
    val (buildRowCount, probeRowCount) = if (buildSide == LeftBuild) {
      (leftRowCount, rightRowCount)
    } else {
      (rightRowCount, leftRowCount)
    }
    
    val actualProbeKeys = getActualProbeKeys(probeKeys, buildKeys, optimizations)
    
    try {
      // Step 1: Do inner join on keys
      val innerMaps = if (cachedJoinObject.isDefined) {
        cachedJoinObject.get match {
          case Left(Left(hj)) => hj.innerJoin(actualProbeKeys)
          case Left(Right(dhj)) => dhj.innerJoin(actualProbeKeys)
          case Right(smj) => smj.innerJoin(actualProbeKeys, false /* isProbeSorted */)
        }
      } else {
        val actualBuildKeys = getActualBuildKeys(buildKeys, optimizations)
        
        try {
          strategy match {
            case HashObjectWithPostStrategy =>
              // Check distinctness for non-cached path if optimization enabled
              val isDistinct = checkDistinctness(actualBuildKeys, joinType, optimizations)
              
              if (isDistinct) {
                actualBuildKeys.innerDistinctJoinGatherMaps(actualProbeKeys, compareNullsEqual)
              } else {
                actualBuildKeys.innerJoinGatherMaps(actualProbeKeys, compareNullsEqual)
              }
            case HashDirectWithPostStrategy =>
              // Use Table direct APIs (no objects)
              val isDistinct = checkDistinctness(actualBuildKeys, joinType, optimizations)
              
              if (isDistinct) {
                actualBuildKeys.innerDistinctJoinGatherMaps(actualProbeKeys, compareNullsEqual)
              } else {
                actualBuildKeys.innerJoinGatherMaps(actualProbeKeys, compareNullsEqual)
              }
            
            case SortObjectWithPostStrategy =>
              // Validate key types for SortMergeJoin
              KeyRemappingHelper.validateSortMergeKeyTypes(
                actualBuildKeys, optimizations.remapComplexKeysToInts)
              val smj = SortMergeJoin.create(
                actualBuildKeys, false /* isBuildSorted */, compareNullsEqual)
              try {
                smj.innerJoin(actualProbeKeys, false /* isProbeSorted */)
              } finally {
                smj.close()
              }
            
            case SortDirectWithPostStrategy =>
              // Use JoinPrimitives sort API (no objects)
              JoinPrimitives.sortMergeInnerJoin(
                actualBuildKeys, actualProbeKeys,
                false /* isLeftSorted */, false /* isRightSorted */,
                compareNullsEqual)
            
            case _ => throw new IllegalArgumentException(s"Unsupported strategy: $strategy")
          }
        } finally {
          actualBuildKeys.close()
        }
      }
  
  try {
    // Step 2: Apply AST filter to inner join results
    val filteredMaps = JoinPrimitives.filterGatherMapsByAST(
      innerMaps(0), innerMaps(1), buildTable, probeTable, astExpression)
    innerMaps.foreach(_.close())
    
    // Step 3: Apply post-processing based on join type
    PostProcessingHelper.applyPostProcessing(
      filteredMaps, joinType, buildSide, buildRowCount.toInt, probeRowCount.toInt)
      } catch {
        case e: Exception =>
          innerMaps.foreach(_.close())
          throw e
      }
    } finally {
      actualProbeKeys.close()
    }
  }
  
  def close(): Unit = {
    cachedJoinObject.foreach {
      case Left(Left(hj)) => hj.close()
      case Left(Right(dhj)) => dhj.close()
      case Right(smj) => smj.close()
    }
    closeRemappingResources()
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
    // Validate that Direct strategies do not use cacheJoinObject
    strategy match {
      case HashDirectStrategy | HashDirectWithPostStrategy | SortDirectWithPostStrategy =>
        if (optimizations.cacheJoinObject) {
          throw new IllegalArgumentException(
            s"$strategy does not support cacheJoinObject optimization. " +
            s"Direct strategies use Table APIs without join objects that can be cached.")
        }
      case _ => // Object strategies can use caching
    }
    
    val sourceTable = if (buildSide == LeftBuild) leftTable else rightTable
    val buildKeyIndices = if (buildSide == LeftBuild) leftKeyIndices else rightKeyIndices
    
    // Create new Tables with incremented refcounts - holder will own these
    val buildTable = TableCopyHelper.copyTable(sourceTable)
    val buildKeys = TableCopyHelper.extractColumns(buildTable, buildKeyIndices)
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
          case (InnerJoin, HashObjectStrategy) =>
            throw new IllegalArgumentException(
              s"HashObjectStrategy does not support mixed (AST) joins. " +
              s"Use HashDirectStrategy, HashObjectWithPostStrategy, SortObjectWithPostStrategy, " +
              s"HashDirectWithPostStrategy, or SortDirectWithPostStrategy for mixed joins.")
          
          case (InnerJoin, HashDirectStrategy) =>
            new MixedInnerHashBuildHolder(buildTable, buildKeys, ast, compareNullsEqual,
              optimizations)
          
          case (_, HashObjectWithPostStrategy | SortObjectWithPostStrategy |
                   HashDirectWithPostStrategy | SortDirectWithPostStrategy) =>
            // All join types supported with post-processing (inner join + AST filter)
            new MixedPostProcessingBuildHolder(joinType, strategy, buildTable, buildKeys,
              ast, compareNullsEqual, tablesWereSwapped, optimizations, buildSide,
              leftTable.getRowCount, rightTable.getRowCount)
          
          case _ =>
            throw new IllegalArgumentException(
              s"Mixed conditional joins not supported for: $joinType with $strategy. " +
              s"HashObjectStrategy does not support mixed joins. " +
              s"Use HashDirectStrategy, HashObjectWithPostStrategy, SortObjectWithPostStrategy, " +
              s"HashDirectWithPostStrategy, or SortDirectWithPostStrategy for mixed joins.")
        }
        Right(holder)
      
      case None =>
        // Non-conditional join
        val holder = (joinType, strategy) match {
          case (InnerJoin, HashObjectStrategy) =>
            new InnerHashBuildHolder(buildTable, buildKeys, compareNullsEqual,
              tablesWereSwapped, optimizations)
          
          case (LeftOuterJoin, HashObjectStrategy) =>
            new LeftOuterHashBuildHolder(buildTable, buildKeys, compareNullsEqual,
              tablesWereSwapped, optimizations)
          
          case (RightOuterJoin, HashObjectStrategy) =>
            new RightOuterHashBuildHolder(buildTable, buildKeys, compareNullsEqual,
              tablesWereSwapped, optimizations)
          
          case (FullOuterJoin, HashObjectStrategy) =>
            new FullOuterHashBuildHolder(buildTable, buildKeys, compareNullsEqual,
              tablesWereSwapped, optimizations)
          
          case (LeftSemiJoin | LeftAntiJoin, HashObjectStrategy) =>
            new SemiAntiHashBuildHolder(joinType, buildTable, buildKeys, compareNullsEqual,
              optimizations)
          
          case (_, HashObjectWithPostStrategy | SortObjectWithPostStrategy) =>
            // All join types supported with post-processing
            new PostProcessingBuildHolder(joinType, strategy, buildTable, buildKeys,
              compareNullsEqual, tablesWereSwapped, optimizations, buildSide,
              leftTable.getRowCount, rightTable.getRowCount)
          
          case (InnerJoin, HashDirectStrategy) =>
            new InnerHashDirectBuildHolder(buildTable, buildKeys, compareNullsEqual,
              tablesWereSwapped, optimizations)
          
          case (LeftOuterJoin, HashDirectStrategy) =>
            new LeftOuterHashDirectBuildHolder(buildTable, buildKeys, compareNullsEqual,
              tablesWereSwapped, optimizations)
          
          case (RightOuterJoin, HashDirectStrategy) =>
            new RightOuterHashDirectBuildHolder(buildTable, buildKeys, compareNullsEqual,
              tablesWereSwapped, optimizations)
          
          case (FullOuterJoin, HashDirectStrategy) =>
            new FullOuterHashDirectBuildHolder(buildTable, buildKeys, compareNullsEqual,
              tablesWereSwapped, optimizations)
          
          case (LeftSemiJoin | LeftAntiJoin, HashDirectStrategy) =>
            throw new UnsupportedOperationException(
              s"HashDirectStrategy does not support $joinType. " +
              s"Use HashDirectWithPostStrategy for semi/anti joins.")
          
          case (_, HashDirectWithPostStrategy | SortDirectWithPostStrategy) =>
            // All join types supported with post-processing
            new PostProcessingBuildHolder(joinType, strategy, buildTable, buildKeys,
              compareNullsEqual, tablesWereSwapped, optimizations, buildSide,
              leftTable.getRowCount, rightTable.getRowCount)
          
          case _ =>
            throw new UnsupportedOperationException(
              s"Join type $joinType with strategy $strategy not supported")
        }
        Left(holder)
    }
  }
  
  /**
   * Execute a single join and return gather maps with optional detailed timings.
   * Returns: (gatherMaps: Array[GatherMap], optionalDetailedTimings: Option[DetailedTimings])
   */
  def executeJoinWithDetailedTimings(): (Array[GatherMap], Option[DetailedTimings]) = {
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
    
    // Create probe keys table - we own it and must close it
    val probeKeys = TableCopyHelper.extractColumns(probeTable, probeKeyIndices)
    
    try {
      val (gatherMaps, timings) = holder match {
        case Left(nonConditionalHolder) =>
          nonConditionalHolder.joinWithDetailedTimings(probeKeys)
        
        case Right(mixedConditionalHolder) =>
          (mixedConditionalHolder.join(probeKeys, probeTable), None)
      }
      
      (gatherMaps, timings)
      
    } finally {
      probeKeys.close()  // Must close to decrement column refcounts
      
      if (!optimizations.cacheJoinObject) {
        holder match {
          case Left(h) => h.close()
          case Right(h) => h.close()
        }
        // Note: Don't clear actualBuildSide here - it should persist across iterations
        // It will be cleared when clearCache() is called
      }
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
    
    // Create probe keys table - we own it and must close it
    val probeKeys = TableCopyHelper.extractColumns(probeTable, probeKeyIndices)
    
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
        // Note: Don't clear actualBuildSide here - it should persist across iterations
        // It will be cleared when clearCache() is called
      }
    }
  }
  
  private def canSwap(joinType: JoinTypeSpec, strategy: JoinStrategySpec): Boolean = {
    strategy match {
      case HashObjectWithPostStrategy | SortObjectWithPostStrategy | 
           HashDirectWithPostStrategy | SortDirectWithPostStrategy =>
        // Post-processing strategies can swap for all join types
        true
      case HashObjectStrategy | HashDirectStrategy =>
        // Direct hash join strategies can only swap for join types with symmetric semantics
        joinType match {
          case InnerJoin | FullOuterJoin =>
            // Inner and Full Outer joins are symmetric and can swap build sides
            true
          case LeftOuterJoin | RightOuterJoin | LeftSemiJoin | LeftAntiJoin =>
            // These join types have asymmetric semantics and CANNOT swap build sides
            // - LeftOuter: must preserve left side
            // - RightOuter: must preserve right side
            // - Semi/Anti: return only left side rows
            false
        }
      case _ =>
        false
    }
  }
  
  private def defaultBuildSide(
      joinType: JoinTypeSpec,
      strategy: JoinStrategySpec): BuildSideSpec = {
    joinType match {
      case InnerJoin => LeftBuild  // Arbitrary choice for inner joins
      case LeftOuterJoin => LeftBuild  // Left side must be preserved
      case RightOuterJoin => RightBuild  // Right side must be preserved
      case FullOuterJoin => LeftBuild  // Either side works, choose left
      case LeftSemiJoin => LeftBuild  // Left side is the one we return
      case LeftAntiJoin => LeftBuild  // Left side is the one we return
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
  
  def getActualBuildSide: Option[String] = {
    actualBuildSide.map {
      case LeftBuild => "Left"
      case RightBuild => "Right"
      case _ => "Unknown"
    }
  }
}

