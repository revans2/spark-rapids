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

/*
 * JOIN EXECUTOR IMPLEMENTATION NOTES:
 * 
 * SEMI/ANTI JOIN STRATEGY:
 * This implementation uses the post-processing approach for semi/anti joins rather than
 * the FilteredJoin API from spark-rapids-jni. While FilteredJoin provides direct semi/anti
 * join operations, the post-processing approach has advantages for benchmarking:
 * 
 * 1. CONSISTENT CACHING: All join types (inner, outer, semi, anti) use the same caching
 *    infrastructure (HashJoin/DistinctHashJoin/SortMergeJoin), making benchmarks comparable.
 * 
 * 2. POST-PROCESSING COST VISIBILITY: The post-processing approach (inner join + makeSemi/makeAnti)
 *    allows us to measure the cost of post-processing operations separately, providing more
 *    detailed performance insights.
 * 
 * 3. FLEXIBILITY: HashWithPostStrategy and SortWithPostStrategy work uniformly across all join
 *    types without special-casing semi/anti joins.
 * 
 * The FilteredJoin API remains available in spark-rapids-jni for production use cases where
 * direct semi/anti joins may be more efficient than post-processing.
 */

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
) extends NonConditionalBuildHolder {
  
  private var cachedJoinObject: Option[Either[HashJoin, DistinctHashJoin]] = None
  private var cachedIsDistinct: Option[Boolean] = None
  private var remapStructures: Option[KeyRemapping.RemapStructures] = None
  private var remappedBuildKeys: Option[ColumnVector] = None
  private var initialized = false
  
  private def ensureInitialized(): Unit = {
    if (!initialized) {
      // Initialize remapping if enabled (independent of join object caching)
      if (optimizations.remapComplexKeysToInts) {
        if (optimizations.cacheRemapping) {
          val remap = KeyRemapping.createRemapStructures(buildKeys)
          remapStructures = Some(remap)
          remappedBuildKeys = Some(KeyRemapping.applyRemapping(buildKeys, remap))
        }
      }
      
      if (optimizations.cacheJoinObject) {
        // Determine which keys to use for join object creation
        val keysForJoin = if (optimizations.remapComplexKeysToInts &&
            remappedBuildKeys.isDefined) {
          new Table(remappedBuildKeys.get)
        } else {
          buildKeys
        }
        
        try {
          // Check if build keys are distinct (use cached result if available)
          val isDistinct = if (optimizations.useDistinctJoin) {
            if (optimizations.cacheDistinctFlag && cachedIsDistinct.isDefined) {
              cachedIsDistinct.get
            } else {
              val distinct = keysForJoin.getRowCount == keysForJoin.distinctCount()
              if (optimizations.cacheDistinctFlag) {
                cachedIsDistinct = Some(distinct)
              }
              distinct
            }
          } else {
            false
          }
          
          // Create appropriate join object based on distinctness
          cachedJoinObject = Some(if (isDistinct) {
            Right(DistinctHashJoin.create(keysForJoin, compareNullsEqual))
          } else {
            Left(HashJoin.create(keysForJoin, compareNullsEqual))
          })
        } finally {
          if (optimizations.remapComplexKeysToInts &&
              remappedBuildKeys.isDefined) {
            keysForJoin.close()
          }
        }
      }
      initialized = true
    }
  }
  
  def join(probeKeys: Table): Array[GatherMap] = {
    ensureInitialized()
    
    // Remap probe keys if remapping is enabled
    val remappedProbeCol = if (optimizations.remapComplexKeysToInts) {
      if (remapStructures.isDefined) {
        // Use cached remapping structures
        Some(KeyRemapping.applyRemapping(probeKeys, remapStructures.get))
      } else {
        // Non-cached remapping: create structures on-the-fly
        val tempRemap = KeyRemapping.createRemapStructures(buildKeys)
        try {
          Some(KeyRemapping.applyRemapping(probeKeys, tempRemap))
        } finally {
          tempRemap.close()
        }
      }
    } else {
      None
    }
    
    try {
      val actualProbeKeys = remappedProbeCol match {
        case Some(col) => new Table(col)
        case None => probeKeys
      }
      
      try {
        if (cachedJoinObject.isDefined) {
          cachedJoinObject.get match {
            case Left(hj) => hj.innerJoin(actualProbeKeys)
            case Right(dhj) => dhj.innerJoin(actualProbeKeys)
          }
        } else {
          // Non-cached join path: determine keys and create join object
          val actualBuildKeys = if (optimizations.remapComplexKeysToInts &&
              remappedBuildKeys.isDefined) {
            new Table(remappedBuildKeys.get)
          } else if (optimizations.remapComplexKeysToInts) {
            // Remapping enabled but not cached, remap build keys now
            val tempRemap = KeyRemapping.createRemapStructures(buildKeys)
            try {
              val remappedCol = KeyRemapping.applyRemapping(buildKeys, tempRemap)
              try {
                new Table(remappedCol)
              } finally {
                remappedCol.close()
              }
            } finally {
              tempRemap.close()
            }
          } else {
            buildKeys
          }
          
          try {
            // Check if build keys are distinct (use cached result if available)
            val isDistinct = if (optimizations.useDistinctJoin) {
              if (optimizations.cacheDistinctFlag && cachedIsDistinct.isDefined) {
                cachedIsDistinct.get
              } else {
                val distinct = actualBuildKeys.getRowCount == actualBuildKeys.distinctCount()
                if (optimizations.cacheDistinctFlag) {
                  cachedIsDistinct = Some(distinct)
                }
                distinct
              }
            } else {
              false
            }
            
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
            if (optimizations.remapComplexKeysToInts &&
                actualBuildKeys != buildKeys) {
              actualBuildKeys.close()
            }
          }
        }
      } finally {
        if (actualProbeKeys != probeKeys) {
          actualProbeKeys.close()
        }
      }
    } finally {
      remappedProbeCol.foreach(_.close())
    }
  }
  
  def close(): Unit = {
    cachedJoinObject.foreach {
      case Left(hj) => hj.close()
      case Right(dhj) => dhj.close()
    }
    remapStructures.foreach(_.close())
    remappedBuildKeys.foreach(_.close())
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
) extends NonConditionalBuildHolder {
  
  private var cachedJoinObject: Option[HashJoin] = None
  private var remapStructures: Option[KeyRemapping.RemapStructures] = None
  private var remappedBuildKeys: Option[ColumnVector] = None
  private var initialized = false
  
  private def ensureInitialized(): Unit = {
    if (!initialized) {
      // Initialize remapping if enabled
      if (optimizations.remapComplexKeysToInts && optimizations.cacheRemapping) {
        val remap = KeyRemapping.createRemapStructures(buildKeys)
        remapStructures = Some(remap)
        remappedBuildKeys = Some(KeyRemapping.applyRemapping(buildKeys, remap))
      }
      
      if (optimizations.cacheJoinObject) {
        // Determine which keys to use
        val keysForJoin = if (optimizations.remapComplexKeysToInts &&
            remappedBuildKeys.isDefined) {
          new Table(remappedBuildKeys.get)
        } else {
          buildKeys
        }
        
        try {
          // Left outer join always uses HashJoin
          cachedJoinObject = Some(HashJoin.create(keysForJoin, compareNullsEqual))
        } finally {
          if (optimizations.remapComplexKeysToInts &&
              remappedBuildKeys.isDefined) {
            keysForJoin.close()
          }
        }
      }
      initialized = true
    }
  }
  
  def join(probeKeys: Table): Array[GatherMap] = {
    ensureInitialized()
    
    // Remap probe keys if needed
    val remappedProbeCol = if (optimizations.remapComplexKeysToInts) {
      if (remapStructures.isDefined) {
        Some(KeyRemapping.applyRemapping(probeKeys, remapStructures.get))
      } else {
        val tempRemap = KeyRemapping.createRemapStructures(buildKeys)
        try {
          Some(KeyRemapping.applyRemapping(probeKeys, tempRemap))
        } finally {
          tempRemap.close()
        }
      }
    } else {
      None
    }
    
    try {
      val actualProbeKeys = remappedProbeCol match {
        case Some(col) => new Table(col)
        case None => probeKeys
      }
      
      try {
        if (cachedJoinObject.isDefined) {
          cachedJoinObject.get.leftJoin(actualProbeKeys)
        } else {
          // Non-cached join path
          val actualBuildKeys = if (optimizations.remapComplexKeysToInts &&
              remappedBuildKeys.isDefined) {
            new Table(remappedBuildKeys.get)
          } else if (optimizations.remapComplexKeysToInts) {
            val tempRemap = KeyRemapping.createRemapStructures(buildKeys)
            try {
              val remappedCol = KeyRemapping.applyRemapping(buildKeys, tempRemap)
              try {
                new Table(remappedCol)
              } finally {
                remappedCol.close()
              }
            } finally {
              tempRemap.close()
            }
          } else {
            buildKeys
          }
          
          try {
            val tempHashJoin = HashJoin.create(actualBuildKeys, compareNullsEqual)
            try {
              tempHashJoin.leftJoin(actualProbeKeys)
            } finally {
              tempHashJoin.close()
            }
          } finally {
            if (optimizations.remapComplexKeysToInts &&
                actualBuildKeys != buildKeys) {
              actualBuildKeys.close()
            }
          }
        }
      } finally {
        if (actualProbeKeys != probeKeys) {
          actualProbeKeys.close()
        }
      }
    } finally {
      remappedProbeCol.foreach(_.close())
    }
  }
  
  def close(): Unit = {
    cachedJoinObject.foreach(_.close())
    remapStructures.foreach(_.close())
    remappedBuildKeys.foreach(_.close())
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
) extends NonConditionalBuildHolder {
  
  private var cachedJoinObject: Option[HashJoin] = None
  private var remapStructures: Option[KeyRemapping.RemapStructures] = None
  private var remappedBuildKeys: Option[ColumnVector] = None
  private var initialized = false
  
  private def ensureInitialized(): Unit = {
    if (!initialized) {
      // Initialize remapping if enabled
      if (optimizations.remapComplexKeysToInts && optimizations.cacheRemapping) {
        val remap = KeyRemapping.createRemapStructures(buildKeys)
        remapStructures = Some(remap)
        remappedBuildKeys = Some(KeyRemapping.applyRemapping(buildKeys, remap))
      }
      
      if (optimizations.cacheJoinObject) {
        val keysForJoin = if (optimizations.remapComplexKeysToInts &&
            remappedBuildKeys.isDefined) {
          new Table(remappedBuildKeys.get)
        } else {
          buildKeys
        }
        
        try {
          cachedJoinObject = Some(HashJoin.create(keysForJoin, compareNullsEqual))
        } finally {
          if (optimizations.remapComplexKeysToInts &&
              remappedBuildKeys.isDefined) {
            keysForJoin.close()
          }
        }
      }
      initialized = true
    }
  }
  
  def join(probeKeys: Table): Array[GatherMap] = {
    ensureInitialized()
    
    // Remap probe keys if needed
    val remappedProbeCol = if (optimizations.remapComplexKeysToInts) {
      if (remapStructures.isDefined) {
        Some(KeyRemapping.applyRemapping(probeKeys, remapStructures.get))
      } else {
        val tempRemap = KeyRemapping.createRemapStructures(buildKeys)
        try {
          Some(KeyRemapping.applyRemapping(probeKeys, tempRemap))
        } finally {
          tempRemap.close()
        }
      }
    } else {
      None
    }
    
    try {
      val actualProbeKeys = remappedProbeCol match {
        case Some(col) => new Table(col)
        case None => probeKeys
      }
      
      try {
        // Right outer join is implemented as left outer join with swapped sides
        val result = if (cachedJoinObject.isDefined) {
          cachedJoinObject.get.leftJoin(actualProbeKeys)
        } else {
          val actualBuildKeys = if (optimizations.remapComplexKeysToInts &&
              remappedBuildKeys.isDefined) {
            new Table(remappedBuildKeys.get)
          } else if (optimizations.remapComplexKeysToInts) {
            val tempRemap = KeyRemapping.createRemapStructures(buildKeys)
            try {
              val remappedCol = KeyRemapping.applyRemapping(buildKeys, tempRemap)
              try {
                new Table(remappedCol)
              } finally {
                remappedCol.close()
              }
            } finally {
              tempRemap.close()
            }
          } else {
            buildKeys
          }
          
          try {
            val tempHashJoin = HashJoin.create(actualBuildKeys, compareNullsEqual)
            try {
              tempHashJoin.leftJoin(actualProbeKeys)
            } finally {
              tempHashJoin.close()
            }
          } finally {
            if (optimizations.remapComplexKeysToInts &&
                actualBuildKeys != buildKeys) {
              actualBuildKeys.close()
            }
          }
        }
        
        // Swap the gather maps to convert left outer to right outer
        Array(result(1), result(0))
      } finally {
        if (actualProbeKeys != probeKeys) {
          actualProbeKeys.close()
        }
      }
    } finally {
      remappedProbeCol.foreach(_.close())
    }
  }
  
  def close(): Unit = {
    cachedJoinObject.foreach(_.close())
    remapStructures.foreach(_.close())
    remappedBuildKeys.foreach(_.close())
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
) extends NonConditionalBuildHolder {
  
  private var cachedJoinObject: Option[HashJoin] = None
  private var remapStructures: Option[KeyRemapping.RemapStructures] = None
  private var remappedBuildKeys: Option[ColumnVector] = None
  private var initialized = false
  
  private def ensureInitialized(): Unit = {
    if (!initialized) {
      // Initialize remapping if enabled
      if (optimizations.remapComplexKeysToInts && optimizations.cacheRemapping) {
        val remap = KeyRemapping.createRemapStructures(buildKeys)
        remapStructures = Some(remap)
        remappedBuildKeys = Some(KeyRemapping.applyRemapping(buildKeys, remap))
      }
      
      if (optimizations.cacheJoinObject) {
        val keysForJoin = if (optimizations.remapComplexKeysToInts &&
            remappedBuildKeys.isDefined) {
          new Table(remappedBuildKeys.get)
        } else {
          buildKeys
        }
        
        try {
          cachedJoinObject = Some(HashJoin.create(keysForJoin, compareNullsEqual))
        } finally {
          if (optimizations.remapComplexKeysToInts &&
              remappedBuildKeys.isDefined) {
            keysForJoin.close()
          }
        }
      }
      initialized = true
    }
  }
  
  def join(probeKeys: Table): Array[GatherMap] = {
    ensureInitialized()
    
    // Remap probe keys if needed
    val remappedProbeCol = if (optimizations.remapComplexKeysToInts) {
      if (remapStructures.isDefined) {
        Some(KeyRemapping.applyRemapping(probeKeys, remapStructures.get))
      } else {
        val tempRemap = KeyRemapping.createRemapStructures(buildKeys)
        try {
          Some(KeyRemapping.applyRemapping(probeKeys, tempRemap))
        } finally {
          tempRemap.close()
        }
      }
    } else {
      None
    }
    
    try {
      val actualProbeKeys = remappedProbeCol match {
        case Some(col) => new Table(col)
        case None => probeKeys
      }
      
      try {
        if (cachedJoinObject.isDefined) {
          cachedJoinObject.get.fullJoin(actualProbeKeys)
        } else {
          val actualBuildKeys = if (optimizations.remapComplexKeysToInts &&
              remappedBuildKeys.isDefined) {
            new Table(remappedBuildKeys.get)
          } else if (optimizations.remapComplexKeysToInts) {
            val tempRemap = KeyRemapping.createRemapStructures(buildKeys)
            try {
              val remappedCol = KeyRemapping.applyRemapping(buildKeys, tempRemap)
              try {
                new Table(remappedCol)
              } finally {
                remappedCol.close()
              }
            } finally {
              tempRemap.close()
            }
          } else {
            buildKeys
          }
          
          try {
            val tempHashJoin = HashJoin.create(actualBuildKeys, compareNullsEqual)
            try {
              tempHashJoin.fullJoin(actualProbeKeys)
            } finally {
              tempHashJoin.close()
            }
          } finally {
            if (optimizations.remapComplexKeysToInts &&
                actualBuildKeys != buildKeys) {
              actualBuildKeys.close()
            }
          }
        }
      } finally {
        if (actualProbeKeys != probeKeys) {
          actualProbeKeys.close()
        }
      }
    } finally {
      remappedProbeCol.foreach(_.close())
    }
  }
  
  def close(): Unit = {
    cachedJoinObject.foreach(_.close())
    remapStructures.foreach(_.close())
    remappedBuildKeys.foreach(_.close())
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
) extends NonConditionalBuildHolder {
  
  private var cachedJoinObject: Option[FilteredJoin] = None
  private var remapStructures: Option[KeyRemapping.RemapStructures] = None
  private var remappedBuildKeys: Option[ColumnVector] = None
  private var initialized = false
  
  private def ensureInitialized(): Unit = {
    if (!initialized) {
      // Initialize remapping if enabled
      if (optimizations.remapComplexKeysToInts && optimizations.cacheRemapping) {
        val remap = KeyRemapping.createRemapStructures(buildKeys)
        remapStructures = Some(remap)
        remappedBuildKeys = Some(KeyRemapping.applyRemapping(buildKeys, remap))
      }
      
      if (optimizations.cacheJoinObject) {
        val keysForJoin = if (optimizations.remapComplexKeysToInts &&
            remappedBuildKeys.isDefined) {
          new Table(remappedBuildKeys.get)
        } else {
          buildKeys
        }
        
        try {
          cachedJoinObject = Some(FilteredJoin.create(keysForJoin, compareNullsEqual))
        } finally {
          if (optimizations.remapComplexKeysToInts &&
              remappedBuildKeys.isDefined) {
            keysForJoin.close()
          }
        }
      }
      initialized = true
    }
  }
  
  def join(probeKeys: Table): Array[GatherMap] = {
    ensureInitialized()
    
    // Remap probe keys if needed
    val remappedProbeCol = if (optimizations.remapComplexKeysToInts) {
      if (remapStructures.isDefined) {
        Some(KeyRemapping.applyRemapping(probeKeys, remapStructures.get))
      } else {
        val tempRemap = KeyRemapping.createRemapStructures(buildKeys)
        try {
          Some(KeyRemapping.applyRemapping(probeKeys, tempRemap))
        } finally {
          tempRemap.close()
        }
      }
    } else {
      None
    }
    
    try {
      val actualProbeKeys = remappedProbeCol match {
        case Some(col) => new Table(col)
        case None => probeKeys
      }
      
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
          val actualBuildKeys = if (optimizations.remapComplexKeysToInts &&
              remappedBuildKeys.isDefined) {
            new Table(remappedBuildKeys.get)
          } else if (optimizations.remapComplexKeysToInts) {
            val tempRemap = KeyRemapping.createRemapStructures(buildKeys)
            try {
              val remappedCol = KeyRemapping.applyRemapping(buildKeys, tempRemap)
              try {
                new Table(remappedCol)
              } finally {
                remappedCol.close()
              }
            } finally {
              tempRemap.close()
            }
          } else {
            buildKeys
          }
          
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
            if (optimizations.remapComplexKeysToInts &&
                actualBuildKeys != buildKeys) {
              actualBuildKeys.close()
            }
          }
        }
      } finally {
        if (actualProbeKeys != probeKeys) {
          actualProbeKeys.close()
        }
      }
    } finally {
      remappedProbeCol.foreach(_.close())
    }
  }
  
  def close(): Unit = {
    cachedJoinObject.foreach(_.close())
    remapStructures.foreach(_.close())
    remappedBuildKeys.foreach(_.close())
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
) extends NonConditionalBuildHolder {
  
  // Either[HashJoin | DistinctHashJoin, SortMergeJoin]
  private var cachedJoinObject:
      Option[Either[Either[HashJoin, DistinctHashJoin], SortMergeJoin]] = None
  private var cachedIsDistinct: Option[Boolean] = None
  private var remapStructures: Option[KeyRemapping.RemapStructures] = None
  private var remappedBuildKeys: Option[ColumnVector] = None
  private var initialized = false
  
  private def ensureInitialized(): Unit = {
    if (!initialized) {
      // Validate SortMergeJoin key types BEFORE any initialization
      if (strategy == SortWithPostStrategy) {
        KeyRemappingHelper.validateSortMergeKeyTypes(
          buildKeys, optimizations.remapComplexKeysToInts)
      }
      
      // Initialize remapping if enabled
      if (optimizations.remapComplexKeysToInts && optimizations.cacheRemapping) {
        val remap = KeyRemapping.createRemapStructures(buildKeys)
        remapStructures = Some(remap)
        remappedBuildKeys = Some(KeyRemapping.applyRemapping(buildKeys, remap))
      }
      
      if (optimizations.cacheJoinObject) {
        // Determine which keys to use for join object creation
        val keysForJoin = if (optimizations.remapComplexKeysToInts &&
            remappedBuildKeys.isDefined) {
          new Table(remappedBuildKeys.get)
        } else {
          buildKeys
        }
        
        try {
          // Check distinctness for hash joins if optimization enabled
          val isDistinct = if (optimizations.useDistinctJoin && strategy == HashWithPostStrategy) {
            if (optimizations.cacheDistinctFlag && cachedIsDistinct.isDefined) {
              cachedIsDistinct.get
            } else {
              // Only check distinctness for join types that support DistinctHashJoin
              val canUseDistinct = joinType match {
                case InnerJoin | LeftOuterJoin => true
                case _ => false
              }
              if (canUseDistinct) {
                val distinct = keysForJoin.getRowCount == keysForJoin.distinctCount()
                if (optimizations.cacheDistinctFlag) {
                  cachedIsDistinct = Some(distinct)
                }
                distinct
              } else {
                false
              }
            }
          } else {
            false
          }
          
          cachedJoinObject = Some(strategy match {
            case HashWithPostStrategy =>
              if (isDistinct) {
                Left(Right(DistinctHashJoin.create(keysForJoin, compareNullsEqual)))
              } else {
                Left(Left(HashJoin.create(keysForJoin, compareNullsEqual)))
              }
            case SortWithPostStrategy =>
              Right(SortMergeJoin.create(keysForJoin, false /* isBuildSorted */, compareNullsEqual))
            case _ =>
              throw new IllegalArgumentException(
                s"Unsupported strategy for post-processing: $strategy")
          })
        } finally {
          if (optimizations.remapComplexKeysToInts &&
              remappedBuildKeys.isDefined) {
            keysForJoin.close()
          }
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
    
    // Remap probe keys if needed
    val remappedProbeCol = if (optimizations.remapComplexKeysToInts) {
      if (remapStructures.isDefined) {
        Some(KeyRemapping.applyRemapping(probeKeys, remapStructures.get))
      } else {
        val tempRemap = KeyRemapping.createRemapStructures(buildKeys)
        try {
          Some(KeyRemapping.applyRemapping(probeKeys, tempRemap))
        } finally {
          tempRemap.close()
        }
      }
    } else {
      None
    }
    
    try {
      val actualProbeKeys = remappedProbeCol match {
        case Some(col) => new Table(col)
        case None => probeKeys
      }
      
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
          val actualBuildKeys = if (optimizations.remapComplexKeysToInts &&
              remappedBuildKeys.isDefined) {
            new Table(remappedBuildKeys.get)
          } else if (optimizations.remapComplexKeysToInts) {
            val tempRemap = KeyRemapping.createRemapStructures(buildKeys)
            try {
              val remappedCol = KeyRemapping.applyRemapping(buildKeys, tempRemap)
              try {
                new Table(remappedCol)
              } finally {
                remappedCol.close()
              }
            } finally {
              tempRemap.close()
            }
          } else {
            buildKeys
          }
          
          try {
            strategy match {
              case HashWithPostStrategy =>
                // Check distinctness for non-cached path if optimization enabled
                val isDistinct = if (optimizations.useDistinctJoin) {
                  if (optimizations.cacheDistinctFlag && cachedIsDistinct.isDefined) {
                    cachedIsDistinct.get
                  } else {
                    val canUseDistinct = joinType match {
                      case InnerJoin | LeftOuterJoin => true
                      case _ => false
                    }
                    if (canUseDistinct) {
                      val distinct = actualBuildKeys.getRowCount == actualBuildKeys.distinctCount()
                      if (optimizations.cacheDistinctFlag) {
                        cachedIsDistinct = Some(distinct)
                      }
                      distinct
                    } else {
                      false
                    }
                  }
                } else {
                  false
                }
                
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
                
              case SortWithPostStrategy =>
                // Validate key types for SortMergeJoin
                // (in case this wasn't done in ensureInitialized)
                if (strategy == SortWithPostStrategy) {
                  KeyRemappingHelper.validateSortMergeKeyTypes(
                    actualBuildKeys, optimizations.remapComplexKeysToInts)
                }
                // Use SortMergeJoin API (not cached)
                val smj = SortMergeJoin.create(
                  actualBuildKeys, false /* isBuildSorted */, compareNullsEqual)
                try {
                  smj.innerJoin(actualProbeKeys, false /* isProbeSorted */)
                } finally {
                  smj.close()
                }
                
              case _ =>
                throw new IllegalArgumentException(
                  s"Unsupported strategy for post-processing: $strategy")
            }
          } finally {
            if (optimizations.remapComplexKeysToInts &&
                actualBuildKeys != buildKeys) {
              actualBuildKeys.close()
            }
          }
        }
    
    // Step 2: Apply post-processing based on join type
    try {
      joinType match {
        case InnerJoin =>
          // No post-processing needed for inner join
          innerMaps
        
        case LeftOuterJoin =>
          // Convert inner join to left outer
          // If build is left, swap the maps before post-processing
          val result = if (buildSide == LeftBuild) {
            JoinPrimitives.makeLeftOuter(
              innerMaps(0), innerMaps(1), buildRowCount.toInt, probeRowCount.toInt)
          } else {
            // Build is right, so we need to swap and do right outer, then swap back
            val rightOuter = JoinPrimitives.makeLeftOuter(
              innerMaps(1), innerMaps(0), probeRowCount.toInt, buildRowCount.toInt)
            Array(rightOuter(1), rightOuter(0))
          }
          innerMaps.foreach(_.close())
          result
        
        case RightOuterJoin =>
          // Convert inner join to right outer (swap left/right)
          val result = if (buildSide == RightBuild) {
            JoinPrimitives.makeLeftOuter(
              innerMaps(1), innerMaps(0), probeRowCount.toInt, buildRowCount.toInt)
          } else {
            // Build is left, so we're doing right outer from probe perspective
            val leftOuter = JoinPrimitives.makeLeftOuter(
              innerMaps(0), innerMaps(1), buildRowCount.toInt, probeRowCount.toInt)
            Array(leftOuter(1), leftOuter(0))
          }
          innerMaps.foreach(_.close())
          result
        
        case FullOuterJoin =>
          // Convert inner join to full outer
          val result = JoinPrimitives.makeFullOuter(
            innerMaps(0), innerMaps(1), buildRowCount.toInt, probeRowCount.toInt)
          innerMaps.foreach(_.close())
          result
        
        case LeftSemiJoin =>
          // Convert inner join to semi join (unique left indices from build side)
          // Semi join returns only one gather map
          val semiMap = if (buildSide == LeftBuild) {
            JoinPrimitives.makeSemi(innerMaps(0), buildRowCount.toInt)
          } else {
            // Build is right, probe is left - use probe side map
            JoinPrimitives.makeSemi(innerMaps(1), probeRowCount.toInt)
          }
          innerMaps.foreach(_.close())
          Array(semiMap)
        
        case LeftAntiJoin =>
          // Convert inner join to anti join
          // First get semi join, then anti
          val semiMap = if (buildSide == LeftBuild) {
            JoinPrimitives.makeSemi(innerMaps(0), buildRowCount.toInt)
          } else {
            JoinPrimitives.makeSemi(innerMaps(1), probeRowCount.toInt)
          }
          innerMaps.foreach(_.close())
          
          val antiMap = if (buildSide == LeftBuild) {
            JoinPrimitives.makeAnti(semiMap, buildRowCount.toInt)
          } else {
            JoinPrimitives.makeAnti(semiMap, probeRowCount.toInt)
          }
          semiMap.close()
          Array(antiMap)
      }
        } catch {
          case e: Exception =>
            innerMaps.foreach(_.close())
            throw e
        }
      } finally {
        if (actualProbeKeys != probeKeys) {
          actualProbeKeys.close()
        }
      }
    } finally {
      remappedProbeCol.foreach(_.close())
    }
  }
  
  def close(): Unit = {
    cachedJoinObject.foreach {
      case Left(Left(hj)) => hj.close()
      case Left(Right(dhj)) => dhj.close()
      case Right(smj) => smj.close()
    }
    remapStructures.foreach(_.close())
    remappedBuildKeys.foreach(_.close())
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
  
  private var remapStructures: Option[KeyRemapping.RemapStructures] = None
  private var remappedBuildKeys: Option[ColumnVector] = None
  private var initialized = false
  
  private def ensureInitialized(): Unit = {
    if (!initialized) {
      // Initialize remapping if enabled
      if (optimizations.remapComplexKeysToInts && optimizations.cacheRemapping) {
        val remap = KeyRemapping.createRemapStructures(buildKeys)
        remapStructures = Some(remap)
        remappedBuildKeys = Some(KeyRemapping.applyRemapping(buildKeys, remap))
      }
      initialized = true
    }
  }
  
  def join(probeKeys: Table, probeTable: Table): Array[GatherMap] = {
    ensureInitialized()
    
    // Remap keys if needed
    val remappedProbeCol = if (optimizations.remapComplexKeysToInts) {
      if (remapStructures.isDefined) {
        Some(KeyRemapping.applyRemapping(probeKeys, remapStructures.get))
      } else {
        val tempRemap = KeyRemapping.createRemapStructures(buildKeys)
        try {
          Some(KeyRemapping.applyRemapping(probeKeys, tempRemap))
        } finally {
          tempRemap.close()
        }
      }
    } else {
      None
    }
    
    try {
      val actualBuildKeys = if (optimizations.remapComplexKeysToInts &&
          remappedBuildKeys.isDefined) {
        new Table(remappedBuildKeys.get)
      } else if (optimizations.remapComplexKeysToInts) {
        val tempRemap = KeyRemapping.createRemapStructures(buildKeys)
        try {
          val remappedCol = KeyRemapping.applyRemapping(buildKeys, tempRemap)
          try {
            new Table(remappedCol)
          } finally {
            remappedCol.close()
          }
        } finally {
          tempRemap.close()
        }
      } else {
        buildKeys
      }
      
      val actualProbeKeys = remappedProbeCol match {
        case Some(col) => new Table(col)
        case None => probeKeys
      }
      
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
        if (optimizations.remapComplexKeysToInts &&
            actualBuildKeys != buildKeys) {
          actualBuildKeys.close()
        }
        if (actualProbeKeys != probeKeys) {
          actualProbeKeys.close()
        }
      }
    } finally {
      remappedProbeCol.foreach(_.close())
    }
  }
  
  def close(): Unit = {
    remapStructures.foreach(_.close())
    remappedBuildKeys.foreach(_.close())
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
) extends MixedConditionalBuildHolder {
  
  // Either[HashJoin | DistinctHashJoin, SortMergeJoin]
  private var cachedJoinObject:
      Option[Either[Either[HashJoin, DistinctHashJoin], SortMergeJoin]] = None
  private var cachedIsDistinct: Option[Boolean] = None
  private var remapStructures: Option[KeyRemapping.RemapStructures] = None
  private var remappedBuildKeys: Option[ColumnVector] = None
  private var initialized = false
  
  private def ensureInitialized(): Unit = {
    if (!initialized) {
      // Validate SortMergeJoin key types BEFORE any initialization
      if (strategy == SortWithPostStrategy) {
        KeyRemappingHelper.validateSortMergeKeyTypes(
          buildKeys, optimizations.remapComplexKeysToInts)
      }
      
      // Initialize remapping if enabled
      if (optimizations.remapComplexKeysToInts && optimizations.cacheRemapping) {
        val remap = KeyRemapping.createRemapStructures(buildKeys)
        remapStructures = Some(remap)
        remappedBuildKeys = Some(KeyRemapping.applyRemapping(buildKeys, remap))
      }
      
      if (optimizations.cacheJoinObject) {
        // Determine which keys to use for join object creation
        val keysForJoin = if (optimizations.remapComplexKeysToInts &&
            remappedBuildKeys.isDefined) {
          new Table(remappedBuildKeys.get)
        } else {
          buildKeys
        }
        
        try {
          // Check distinctness for hash joins if optimization enabled
          val isDistinct = if (optimizations.useDistinctJoin && strategy == HashWithPostStrategy) {
            if (optimizations.cacheDistinctFlag && cachedIsDistinct.isDefined) {
              cachedIsDistinct.get
            } else {
              val canUseDistinct = joinType match {
                case InnerJoin | LeftOuterJoin => true
                case _ => false
              }
              if (canUseDistinct) {
                val distinct = keysForJoin.getRowCount == keysForJoin.distinctCount()
                if (optimizations.cacheDistinctFlag) {
                  cachedIsDistinct = Some(distinct)
                }
                distinct
              } else {
                false
              }
            }
          } else {
            false
          }
          
          cachedJoinObject = Some(strategy match {
            case HashWithPostStrategy =>
              if (isDistinct) {
                Left(Right(DistinctHashJoin.create(keysForJoin, compareNullsEqual)))
              } else {
                Left(Left(HashJoin.create(keysForJoin, compareNullsEqual)))
              }
            case SortWithPostStrategy =>
              Right(SortMergeJoin.create(keysForJoin, false /* isBuildSorted */, compareNullsEqual))
            case _ =>
              throw new IllegalArgumentException(s"Unsupported strategy: $strategy")
          })
        } finally {
          if (optimizations.remapComplexKeysToInts &&
              remappedBuildKeys.isDefined) {
            keysForJoin.close()
          }
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
    
    // Remap probe keys if needed
    val remappedProbeCol = if (optimizations.remapComplexKeysToInts) {
      if (remapStructures.isDefined) {
        Some(KeyRemapping.applyRemapping(probeKeys, remapStructures.get))
      } else {
        val tempRemap = KeyRemapping.createRemapStructures(buildKeys)
        try {
          Some(KeyRemapping.applyRemapping(probeKeys, tempRemap))
        } finally {
          tempRemap.close()
        }
      }
    } else {
      None
    }
    
    try {
      val actualProbeKeys = remappedProbeCol match {
        case Some(col) => new Table(col)
        case None => probeKeys
      }
      
      try {
        // Step 1: Do inner join on keys
        val innerMaps = if (cachedJoinObject.isDefined) {
          cachedJoinObject.get match {
            case Left(Left(hj)) => hj.innerJoin(actualProbeKeys)
            case Left(Right(dhj)) => dhj.innerJoin(actualProbeKeys)
            case Right(smj) => smj.innerJoin(actualProbeKeys, false /* isProbeSorted */)
          }
        } else {
          val actualBuildKeys = if (optimizations.remapComplexKeysToInts &&
              remappedBuildKeys.isDefined) {
            new Table(remappedBuildKeys.get)
          } else if (optimizations.remapComplexKeysToInts) {
            val tempRemap = KeyRemapping.createRemapStructures(buildKeys)
            try {
              val remappedCol = KeyRemapping.applyRemapping(buildKeys, tempRemap)
              try {
                new Table(remappedCol)
              } finally {
                remappedCol.close()
              }
            } finally {
              tempRemap.close()
            }
          } else {
            buildKeys
          }
          
          try {
            strategy match {
              case HashWithPostStrategy =>
                // Check distinctness for non-cached path if optimization enabled
                if (optimizations.useDistinctJoin) {
                  val isDistinct = if (optimizations.cacheDistinctFlag &&
                      cachedIsDistinct.isDefined) {
                    cachedIsDistinct.get
                  } else {
                    val canUseDistinct = joinType match {
                      case InnerJoin | LeftOuterJoin => true
                      case _ => false
                    }
                    if (canUseDistinct) {
                      val distinct = actualBuildKeys.getRowCount == actualBuildKeys.distinctCount()
                      if (optimizations.cacheDistinctFlag) {
                        cachedIsDistinct = Some(distinct)
                      }
                      distinct
                    } else {
                      false
                    }
                  }
                  
                  if (isDistinct) {
                    actualBuildKeys.innerDistinctJoinGatherMaps(actualProbeKeys, compareNullsEqual)
                  } else {
                    actualBuildKeys.innerJoinGatherMaps(actualProbeKeys, compareNullsEqual)
                  }
                } else {
                  actualBuildKeys.innerJoinGatherMaps(actualProbeKeys, compareNullsEqual)
                }
              case SortWithPostStrategy =>
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
              case _ => throw new IllegalArgumentException(s"Unsupported strategy: $strategy")
            }
          } finally {
            if (optimizations.remapComplexKeysToInts &&
                actualBuildKeys != buildKeys) {
              actualBuildKeys.close()
            }
          }
        }
    
    try {
      // Step 2: Apply AST filter to inner join results
      val filteredMaps = JoinPrimitives.filterGatherMapsByAST(
        innerMaps(0), innerMaps(1), buildTable, probeTable, astExpression)
      innerMaps.foreach(_.close())
      
      // Step 3: Apply post-processing based on join type
      try {
        joinType match {
          case InnerJoin =>
            filteredMaps
          
          case LeftOuterJoin =>
            val result = if (buildSide == LeftBuild) {
              JoinPrimitives.makeLeftOuter(
                filteredMaps(0), filteredMaps(1), buildRowCount.toInt, probeRowCount.toInt)
            } else {
              val rightOuter = JoinPrimitives.makeLeftOuter(
                filteredMaps(1), filteredMaps(0), probeRowCount.toInt, buildRowCount.toInt)
              Array(rightOuter(1), rightOuter(0))
            }
            filteredMaps.foreach(_.close())
            result
          
          case RightOuterJoin =>
            val result = if (buildSide == RightBuild) {
              JoinPrimitives.makeLeftOuter(
                filteredMaps(1), filteredMaps(0), probeRowCount.toInt, buildRowCount.toInt)
            } else {
              val leftOuter = JoinPrimitives.makeLeftOuter(
                filteredMaps(0), filteredMaps(1), buildRowCount.toInt, probeRowCount.toInt)
              Array(leftOuter(1), leftOuter(0))
            }
            filteredMaps.foreach(_.close())
            result
          
          case FullOuterJoin =>
            val result = JoinPrimitives.makeFullOuter(
              filteredMaps(0), filteredMaps(1), buildRowCount.toInt, probeRowCount.toInt)
            filteredMaps.foreach(_.close())
            result
          
          case LeftSemiJoin =>
            val semiMap = if (buildSide == LeftBuild) {
              JoinPrimitives.makeSemi(filteredMaps(0), buildRowCount.toInt)
            } else {
              JoinPrimitives.makeSemi(filteredMaps(1), probeRowCount.toInt)
            }
            filteredMaps.foreach(_.close())
            Array(semiMap)
          
          case LeftAntiJoin =>
            val semiMap = if (buildSide == LeftBuild) {
              JoinPrimitives.makeSemi(filteredMaps(0), buildRowCount.toInt)
            } else {
              JoinPrimitives.makeSemi(filteredMaps(1), probeRowCount.toInt)
            }
            filteredMaps.foreach(_.close())
            
            val antiMap = if (buildSide == LeftBuild) {
              JoinPrimitives.makeAnti(semiMap, buildRowCount.toInt)
            } else {
              JoinPrimitives.makeAnti(semiMap, probeRowCount.toInt)
            }
            semiMap.close()
            Array(antiMap)
        }
      } catch {
        case e: Exception =>
          filteredMaps.foreach(_.close())
          throw e
      }
        } catch {
          case e: Exception =>
            innerMaps.foreach(_.close())
            throw e
        }
      } finally {
        if (actualProbeKeys != probeKeys) {
          actualProbeKeys.close()
        }
      }
    } finally {
      remappedProbeCol.foreach(_.close())
    }
  }
  
  def close(): Unit = {
    cachedJoinObject.foreach {
      case Left(Left(hj)) => hj.close()
      case Left(Right(dhj)) => dhj.close()
      case Right(smj) => smj.close()
    }
    remapStructures.foreach(_.close())
    remappedBuildKeys.foreach(_.close())
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
          
          case (_, HashWithPostStrategy | SortWithPostStrategy) =>
            // All join types supported with post-processing
            new MixedPostProcessingBuildHolder(joinType, strategy, buildTable, buildKeys,
              ast, compareNullsEqual, tablesWereSwapped, optimizations, buildSide,
              leftTable.getRowCount, rightTable.getRowCount)
          
          case _ =>
            throw new IllegalArgumentException(
              s"Mixed conditional joins not supported for: $joinType with $strategy. " +
              s"Use HashWithPostStrategy or SortWithPostStrategy for non-inner joins.")
        }
        Right(holder)
      
      case None =>
        // Non-conditional join
        val holder = (joinType, strategy) match {
          case (InnerJoin, HashJoinStrategy) =>
            new InnerHashBuildHolder(buildTable, buildKeys, compareNullsEqual,
              tablesWereSwapped, optimizations)
          
          case (LeftOuterJoin, HashJoinStrategy) =>
            new LeftOuterHashBuildHolder(buildTable, buildKeys, compareNullsEqual,
              tablesWereSwapped, optimizations)
          
          case (RightOuterJoin, HashJoinStrategy) =>
            new RightOuterHashBuildHolder(buildTable, buildKeys, compareNullsEqual,
              tablesWereSwapped, optimizations)
          
          case (FullOuterJoin, HashJoinStrategy) =>
            new FullOuterHashBuildHolder(buildTable, buildKeys, compareNullsEqual,
              tablesWereSwapped, optimizations)
          
          case (LeftSemiJoin | LeftAntiJoin, HashJoinStrategy) =>
            new SemiAntiHashBuildHolder(joinType, buildTable, buildKeys, compareNullsEqual,
              optimizations)
          
          case (_, HashWithPostStrategy | SortWithPostStrategy) =>
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
        // Note: Don't clear actualBuildSide here - it should persist across iterations
        // It will be cleared when clearCache() is called
      }
    }
  }
  
  private def canSwap(joinType: JoinTypeSpec, strategy: JoinStrategySpec): Boolean = {
    strategy match {
      case HashWithPostStrategy | SortWithPostStrategy =>
        // Post-processing strategies can swap for all join types
        true
      case HashJoinStrategy =>
        // Direct hash join strategy can only swap for join types with symmetric semantics
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

