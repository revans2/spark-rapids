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

// Simple Inner Join Benchmark Example
// This script demonstrates the basic usage of the join micro-benchmark framework.
// 
// To run this example:
// 1. Build the microbenchmarks module:
//    cd microbenchmarks && mvn clean package -Dbuildver=353
// 2. Launch spark-shell with the JARs (note: spark-rapids-jni required for HashJoin/SortMergeJoin):
//    spark-shell \
//      --conf 'spark.rapids.sql.allowMultipleJars=ALWAYS' \
//      --jars \
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
val distinctKeys = 1000    // Number of distinct key values (maxSeed - minSeed + 1)
val keyGroupId = 1         // Ensures left and right keys are correlated

// Output paths
val leftTablePath = "/data/tmp/join_bench/left"
val rightTablePath = "/data/tmp/join_bench/right"

// Benchmark parameters
val benchmarkIterations = 10

// ============================================================================
// STEP 1: Generate test data
// ============================================================================

println("Generating test data...")

// Generate left table
val leftConfig = TableGenConfig(
  numRows = leftNumRows,
  keyColumns = Seq(
    KeyColumnSpec(
      name = "key1",
      dataType = "int",
      minSeed = 0,
      maxSeed = distinctKeys - 1,
      distribution = FlatDistribution()
    )
  ),
  payloadColumns = Seq(
    PayloadColumnSpec("payload1", "int", minSeed = 0, maxSeed = 1000),
    PayloadColumnSpec("payload2", "long", minSeed = 0, maxSeed = 1000)
  ),
  outputPath = leftTablePath
)

// Generate right table with same key distribution
val rightConfig = TableGenConfig(
  numRows = rightNumRows,
  keyColumns = Seq(
    KeyColumnSpec("key1", "int", minSeed = 0, maxSeed = distinctKeys - 1,
      distribution = FlatDistribution())
  ),
  payloadColumns = Seq(
    PayloadColumnSpec("payload1", "int")
  ),
  outputPath = rightTablePath
)

// Generate both tables with correlated keys
generateJoinTables(leftConfig, rightConfig, keyGroupId, spark)

println("Data generation complete!")

// ============================================================================
// STEP 2: Run benchmarks
// ============================================================================

println("\n" + "="*80)
println("Running Join Benchmarks")
println("="*80 + "\n")

// Print TSV header
printTSVHeader()

// Base configuration for all tests
val baseConfig = JoinBenchmarkConfig(
  testName = "base",
  leftParquetPath = leftTablePath,
  rightParquetPath = rightTablePath,
  joinType = InnerJoin,
  joinStrategy = HashObjectStrategy,
  buildSide = RightBuild,
  optimizations = JoinOptimizations(),
  conditionalFilter = None,
  iterations = benchmarkIterations,
  printHeader = false
)

// Test 1: Hash object strategy (uses HashJoin object, can cache)
val results1 = runBenchmark(baseConfig.copy(
  testName = "hash_object_inner"
), spark)
printResultsTSV(results1)

// Test 2: Hash direct strategy (uses Table APIs, no object caching)
val results2 = runBenchmark(baseConfig.copy(
  testName = "hash_direct_inner",
  joinStrategy = HashDirectStrategy
), spark)
printResultsTSV(results2)

// Test 3: Hash object with post-processing
val results3 = runBenchmark(baseConfig.copy(
  testName = "hash_object_post",
  joinStrategy = HashObjectWithPostStrategy
), spark)
printResultsTSV(results3)

// Test 4: Hash direct with post-processing
val results4 = runBenchmark(baseConfig.copy(
  testName = "hash_direct_post",
  joinStrategy = HashDirectWithPostStrategy
), spark)
printResultsTSV(results4)

// Test 5: Sort object with post-processing
val results5 = runBenchmark(baseConfig.copy(
  testName = "sort_object_post",
  joinStrategy = SortObjectWithPostStrategy
), spark)
printResultsTSV(results5)

// Test 6: Sort direct with post-processing
val results6 = runBenchmark(baseConfig.copy(
  testName = "sort_direct_post",
  joinStrategy = SortDirectWithPostStrategy
), spark)
printResultsTSV(results6)

// Test 7: Hash object with caching enabled
val results7 = runBenchmark(baseConfig.copy(
  testName = "hash_object_cached",
  optimizations = JoinOptimizations(cacheJoinObject = true)
), spark)
printResultsTSV(results7)

println("\n" + "="*80)
println("Benchmark Complete!")
println("="*80)
println("\nYou can copy the TSV output above and paste into a spreadsheet for analysis.")

