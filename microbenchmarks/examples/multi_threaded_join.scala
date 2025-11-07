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

// Multi-Threaded Join Benchmark Example
// This example demonstrates scaling behavior with multiple concurrent join threads.
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
val rightNumRows = 500000L
val distinctKeys = 1000  // High duplication: ~1000 rows per key

// Key group for correlated join keys
val keyGroupId = 1

// Output paths
val leftTablePath = "/data/tmp/join_bench/left_multithread"
val rightTablePath = "/data/tmp/join_bench/right_multithread"

// Benchmark parameters
val benchmarkIterations = 20  // Total iterations to distribute across threads
val threadCounts = Seq(1, 2, 4, 8)  // Test with different thread counts

// ============================================================================
// STEP 1: Generate test data
// ============================================================================

println("Generating test data for multi-threaded join benchmarks...")

val leftConfig = TableGenConfig(
  numRows = leftNumRows,
  keyColumns = Seq(
    KeyColumnSpec("key1", "int", minSeed = 0, maxSeed = distinctKeys - 1)
  ),
  payloadColumns = Seq(
    PayloadColumnSpec("value1", "long", minSeed = 0, maxSeed = 1000000)
  ),
  outputPath = leftTablePath
)

val rightConfig = TableGenConfig(
  numRows = rightNumRows,
  keyColumns = Seq(
    KeyColumnSpec("key1", "int", minSeed = 0, maxSeed = distinctKeys - 1)
  ),
  payloadColumns = Seq(
    PayloadColumnSpec("value2", "long", minSeed = 0, maxSeed = 1000000)
  ),
  outputPath = rightTablePath
)

generateJoinTables(leftConfig, rightConfig, keyGroupId, spark)

println("Data generation complete!")

// ============================================================================
// STEP 2: Run multi-threaded benchmarks
// ============================================================================

println("\n" + "="*80)
println("Running Multi-Threaded Join Benchmarks")
println("="*80 + "\n")

printTSVHeader()

// Base configuration
val baseConfig = JoinBenchmarkConfig(
  testName = "base",
  leftParquetPath = leftTablePath,
  rightParquetPath = rightTablePath,
  joinType = InnerJoin,
  joinStrategy = HashJoinStrategy,
  buildSide = RightBuild,
  optimizations = JoinOptimizations(),
  conditionalFilter = None,
  iterations = benchmarkIterations,
  numThreads = 1,
  printHeader = false
)

// Test 1: Single-threaded baseline (no caching)
println("\n=== Test 1: Single-threaded baseline (no caching) ===")
val results1 = runBenchmark(baseConfig.copy(
  testName = "hash_1thread_nocache"
), spark)
printResultsTSV(results1)

// Test 2: Single-threaded with caching
println("\n=== Test 2: Single-threaded with caching ===")
val results2 = runBenchmark(baseConfig.copy(
  testName = "hash_1thread_cached",
  optimizations = JoinOptimizations(cacheJoinObject = true, cacheDistinctFlag = true)
), spark)
printResultsTSV(results2)

// Test 3: Multi-threaded scaling without caching
println("\n=== Test 3: Multi-threaded scaling (no caching) ===")
threadCounts.tail.foreach { numThreads =>
  val results = runBenchmark(baseConfig.copy(
    testName = s"hash_${numThreads}threads_nocache",
    numThreads = numThreads
  ), spark)
  printResultsTSV(results)
}

// Test 4: Multi-threaded scaling with caching (per-thread caching)
println("\n=== Test 4: Multi-threaded scaling (with per-thread caching) ===")
threadCounts.tail.foreach { numThreads =>
  val results = runBenchmark(baseConfig.copy(
    testName = s"hash_${numThreads}threads_cached",
    numThreads = numThreads,
    optimizations = JoinOptimizations(cacheJoinObject = true, cacheDistinctFlag = true)
  ), spark)
  printResultsTSV(results)
}

// Test 5: Hash with post-processing strategy (multi-threaded)
println("\n=== Test 5: HashWithPost strategy (multi-threaded with caching) ===")
threadCounts.foreach { numThreads =>
  val results = runBenchmark(baseConfig.copy(
    testName = s"hash_with_post_${numThreads}threads",
    joinStrategy = HashWithPostStrategy,
    numThreads = numThreads,
    optimizations = JoinOptimizations(cacheJoinObject = true, cacheDistinctFlag = true)
  ), spark)
  printResultsTSV(results)
}

// Test 6: SortMerge strategy (multi-threaded)
println("\n=== Test 6: SortMerge strategy (multi-threaded with caching) ===")
threadCounts.foreach { numThreads =>
  val results = runBenchmark(baseConfig.copy(
    testName = s"sort_merge_${numThreads}threads",
    joinStrategy = SortWithPostStrategy,
    numThreads = numThreads,
    optimizations = JoinOptimizations(cacheJoinObject = true)
  ), spark)
  printResultsTSV(results)
}

println("\n" + "="*80)
println("Multi-Threaded Benchmark Complete!")
println("="*80)

// ============================================================================
// ANALYSIS HELPERS
// ============================================================================

println("\n=== Performance Analysis ===\n")

// Helper to analyze scaling efficiency
def analyzeScaling(singleThreadMs: Double, multiThreadMs: Double, numThreads: Int): Unit = {
  val speedup = singleThreadMs / multiThreadMs
  val efficiency = speedup / numThreads * 100
  val overhead = (multiThreadMs * numThreads - singleThreadMs) / singleThreadMs * 100
  
  println(f"Threads: $numThreads")
  println(f"  Wall clock time: $multiThreadMs%.2f ms")
  println(f"  Speedup: ${speedup}%.2fx")
  println(f"  Parallel efficiency: $efficiency%.1f%%")
  println(f"  Overhead/Interference: $overhead%.1f%%")
  println()
}

println("To analyze results:")
println("1. Compare wall clock time vs sum of iteration times (AvgTimeMs * iterations)")
println("   - Wall clock time shows concurrent execution benefit")
println("   - Sum shows total work done (should be relatively constant)")
println("2. Speedup = SingleThreadWallClock / MultiThreadWallClock")
println("3. Efficiency = Speedup / NumThreads (ideal: 100%)")
println("4. Interference = (SumOfAllThreadTimes - SingleThreadTime) / SingleThreadTime")
println("   - Low interference: threads don't slow each other down much")
println("   - High interference: GPU/memory contention between threads")
println()
println("Expected observations:")
println("- With caching: Better scaling (less work per iteration)")
println("- Without caching: More overhead (rebuild hash tables)")
println("- Efficiency decreases with more threads (GPU saturation)")
println("- Interference increases with more threads (contention)")


