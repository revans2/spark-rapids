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

// Conditional Join Benchmark Example (Mixed Keys + AST)
// This example demonstrates mixed joins with key matching + AST filtering.
//
// To run this example:
// 1. Build the microbenchmarks module:
//    cd microbenchmarks && mvn clean package -Dbuildver=353
// 2. Launch spark-shell with the JARs (note: spark-rapids-jni required for HashJoin/SortMergeJoin):
//    spark-shell --jars \
//      ../datagen/target/datagen_2.12-25.12.0-SNAPSHOT-spark353.jar,\
//      target/spark353/microbenchmarks_2.12-25.12.0-SNAPSHOT-spark353.jar,\
//      ~/.m2/repository/com/nvidia/spark-rapids-jni/25.12.0-SNAPSHOT/spark-rapids-jni-25.12.0-SNAPSHOT-cuda12.jar
// 3. Paste this script into spark-shell

import com.nvidia.spark.rapids.benchmarks.JoinBenchmarkDataGen._
import com.nvidia.spark.rapids.benchmarks.JoinBenchmarkRunner._
import org.apache.spark.sql.tests.datagen._

// ============================================================================
// CONFIGURATION - Customize these parameters
// ============================================================================

// Data generation parameters
val leftNumRows = 1000000L
val rightNumRows = 1000000L
val distinctKeys = 1000
val keyGroupId = 1

// Output paths
val leftTablePath = "/data/tmp/join_bench/left_cond"
val rightTablePath = "/data/tmp/join_bench/right_cond"

// Timestamp range for conditional filtering
val timestampMin = 0L
val timestampMax = 1000000L

// AST filter: Column indices for comparison (0=key, 1=timestamp)
val leftTimestampCol = 1
val rightTimestampCol = 1
val comparisonOp = ">"  // left.timestamp > right.timestamp

// Benchmark parameters
val benchmarkIterations = 10

// ============================================================================
// STEP 1: Generate test data with comparable columns
// ============================================================================

println("Generating test data for conditional joins...")

// Left table with integer keys and long timestamps
val leftConfig = TableGenConfig(
  numRows = leftNumRows,
  keyColumns = Seq(
    KeyColumnSpec("key1", "int", minSeed = 0, maxSeed = distinctKeys - 1)
  ),
  payloadColumns = Seq(
    PayloadColumnSpec("timestamp", "long", minSeed = 0, maxSeed = 1000000,
      valueRange = Some((timestampMin, timestampMax)))
  ),
  outputPath = leftTablePath
)

// Right table with same schema
val rightConfig = TableGenConfig(
  numRows = rightNumRows,
  keyColumns = Seq(
    KeyColumnSpec("key1", "int", minSeed = 0, maxSeed = distinctKeys - 1)
  ),
  payloadColumns = Seq(
    PayloadColumnSpec("timestamp", "long", minSeed = 0, maxSeed = 1000000,
      valueRange = Some((timestampMin, timestampMax)))
  ),
  outputPath = rightTablePath
)

generateJoinTables(leftConfig, rightConfig, keyGroupId, spark)

println("Data generation complete!")

// ============================================================================
// STEP 2: Run conditional join benchmarks
// ============================================================================

println("\n" + "="*80)
println("Running Conditional Join Benchmarks")
println("="*80 + "\n")

// Build AST for the configured comparison operation
val (normalAst, swappedAst) = ConditionalFilterSpec.buildComparison(
  leftColIdx = leftTimestampCol,
  rightColIdx = rightTimestampCol,
  op = comparisonOp
)

try {
  printTSVHeader()
  
  // Base configuration for conditional join tests
  val baseConfig = JoinBenchmarkConfig(
    testName = "base",
    leftParquetPath = leftTablePath,
    rightParquetPath = rightTablePath,
    joinType = InnerJoin,
    joinStrategy = HashJoinStrategy,
    buildSide = RightBuild,
    optimizations = JoinOptimizations(),
    conditionalFilter = Some(ConditionalFilterSpec(
      leftColumns = Seq(leftTimestampCol),
      rightColumns = Seq(rightTimestampCol),
      normalAst,
      Some(swappedAst)
    )),
    iterations = benchmarkIterations
  )
  
  // Test 1: Hash join with direct mixed API (keys + AST)
  val results1 = runBenchmark(baseConfig.copy(
    testName = "hash_mixed"
  ), spark)
  printResultsTSV(results1)
  
  // Test 2: Hash with post-processing (keys + AST filter)
  val results2 = runBenchmark(baseConfig.copy(
    testName = "hash_with_post_mixed",
    joinStrategy = HashWithPostStrategy
  ), spark)
  printResultsTSV(results2)
  
  // Test 3: Sort-merge with post-processing (keys + AST filter)
  val results3 = runBenchmark(baseConfig.copy(
    testName = "sort_with_post_mixed",
    joinStrategy = SortWithPostStrategy
  ), spark)
  printResultsTSV(results3)
  
  println("\n" + "="*80)
  println("Conditional Join Benchmark Complete!")
  println("="*80)
  
} finally {
  // Clean up AST expressions (owned by this script)
  normalAst.close()
  swappedAst.close()
}

