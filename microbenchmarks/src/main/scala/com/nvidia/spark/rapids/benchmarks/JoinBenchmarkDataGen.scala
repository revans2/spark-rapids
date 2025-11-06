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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.tests.datagen._

object JoinBenchmarkDataGen {
  /**
   * Configuration for a single table's data generation.
   * Uses datagen's seed-based approach for deterministic data generation.
   */
  case class TableGenConfig(
    numRows: Long,
    keyColumns: Seq[KeyColumnSpec],
    payloadColumns: Seq[PayloadColumnSpec],
    outputPath: String,
    numOutputFiles: Int = 1  // For benchmarking, typically 1
  )
  
  /**
   * Specification for a join key column.
   * Uses datagen's seed range to control cardinality.
   * 
   * @param name Column name
   * @param dataType Column data type as DDL string (e.g., "int", "long", "string", "decimal(38,0)")
   * @param minSeed Minimum seed value (controls cardinality)
   * @param maxSeed Maximum seed value (maxSeed - minSeed + 1 = approx distinct values)
   * @param distribution Distribution of values (FlatDistribution, NormalDistribution, etc.)
   * @param valueRange Optional value range for the actual data values (separate from seed range)
   * @param nullProbability Probability of null values (0.0 to 1.0)
   */
  case class KeyColumnSpec(
    name: String,
    dataType: String,
    minSeed: Long,
    maxSeed: Long,
    distribution: LocationToSeedMapping = FlatDistribution(),
    valueRange: Option[(Any, Any)] = None,
    nullProbability: Double = 0.0
  )
  
  /**
   * Specification for a payload (non-key) column.
   * 
   * @param name Column name
   * @param dataType Column data type as DDL string
   * @param minSeed Minimum seed value
   * @param maxSeed Maximum seed value
   * @param distribution Distribution of values
   * @param valueRange Optional value range for the actual data values
   * @param nullProbability Probability of null values (0.0 to 1.0)
   */
  case class PayloadColumnSpec(
    name: String,
    dataType: String,
    minSeed: Long = 0,
    maxSeed: Long = 1000,
    distribution: LocationToSeedMapping = FlatDistribution(),
    valueRange: Option[(Any, Any)] = None,
    nullProbability: Double = 0.0
  )
  
  /**
   * Unified API to generate both left and right tables for a join.
   * Uses CorrelatedKeyGroup to ensure join keys match between tables.
   * 
   * @param leftConfig Configuration for left table
   * @param rightConfig Configuration for right table
   * @param keyGroupId Unique ID for the correlated key group (ensures matching keys)
   * @param spark SparkSession
   */
  def generateJoinTables(
    leftConfig: TableGenConfig,
    rightConfig: TableGenConfig,
    keyGroupId: Int,
    spark: SparkSession
  ): Unit = {
    // Validate that key columns have matching data types
    require(leftConfig.keyColumns.length == rightConfig.keyColumns.length,
      "Left and right must have same number of key columns")
    
    leftConfig.keyColumns.zip(rightConfig.keyColumns).foreach { case (lKey, rKey) =>
      require(lKey.dataType == rKey.dataType,
        s"Key column types must match: ${lKey.name}:${lKey.dataType} != " +
          s"${rKey.name}:${rKey.dataType}")
    }
    
    // Generate both tables
    generateTable(leftConfig, Some(keyGroupId), spark)
    generateTable(rightConfig, Some(keyGroupId), spark)
  }
  
  /**
   * Lower-level API to generate a single table.
   * 
   * @param config Table generation configuration
   * @param keyGroupId Optional correlated key group ID (for join benchmarks)
   * @param spark SparkSession
   */
  def generateTable(
    config: TableGenConfig,
    keyGroupId: Option[Int],
    spark: SparkSession
  ): Unit = {
    val dbgen = DBGen()
    
    // Build schema string from key and payload columns
    val allColumns = config.keyColumns.map(k => s"${k.name} ${k.dataType}") ++
                     config.payloadColumns.map(p => s"${p.name} ${p.dataType}")
    val schema = allColumns.mkString(", ")
    
    val table = dbgen.addTable("benchmark_table", schema, config.numRows)
    
    // Configure key columns
    val keyNames = config.keyColumns.map(_.name)
    keyGroupId.foreach { groupId =>
      // Use CorrelatedKeyGroup to ensure join keys are correlated
      val minSeed = config.keyColumns.map(_.minSeed).min
      val maxSeed = config.keyColumns.map(_.maxSeed).max
      val distribution = config.keyColumns.head.distribution
      table.configureKeyGroup(keyNames, CorrelatedKeyGroup(groupId, minSeed, maxSeed), distribution)
    }
    
    config.keyColumns.foreach { keySpec =>
      val column = table(keySpec.name)
      if (keyGroupId.isEmpty) {
        // Only set seed range if not using key group (key group sets it)
        column.setSeedRange(keySpec.minSeed, keySpec.maxSeed)
        column.setSeedMapping(keySpec.distribution)
      }
      column.setNullProbability(keySpec.nullProbability)
      keySpec.valueRange.foreach { case (min, max) =>
        column.setValueRange(min, max)
      }
    }
    
    // Configure payload columns
    config.payloadColumns.foreach { payloadSpec =>
      val column = table(payloadSpec.name)
      column.setSeedRange(payloadSpec.minSeed, payloadSpec.maxSeed)
      column.setSeedMapping(payloadSpec.distribution)
      column.setNullProbability(payloadSpec.nullProbability)
      payloadSpec.valueRange.foreach { case (min, max) =>
        column.setValueRange(min, max)
      }
    }
    
    // Generate DataFrame and write to Parquet
    // Use full parallelism during generation, then repartition to single file for
    // predictable benchmarking
    println(s"Generating ${config.numRows} rows to ${config.outputPath}...")
    val df = table.toDF(spark, numParts = 0)  // Use default parallelism for generation
    val repartitionedDf = if (config.numOutputFiles == 1) {
      df.repartition(1)  // Coalesce to single file for benchmarking
    } else {
      df.repartition(config.numOutputFiles)
    }
    repartitionedDf.write.mode("overwrite").parquet(config.outputPath)
    println(s"Generation complete: ${config.outputPath}")
  }
}

