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

// Comprehensive Join Types Benchmark Example
// This script demonstrates all join types with various build side options,
// including distinct join optimization.
// 
// To run this example:
// 1. Build the microbenchmarks module:
//    cd microbenchmarks && mvn clean package -Dbuildver=353
// 2. Launch spark-shell with the JARs (note: spark-rapids-jni required):
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
val distinctKeys = 1000    // Number of distinct key values
val keyGroupId = 1         // Ensures left and right keys are correlated

// Output paths
val leftTablePath = "/data/tmp/join_bench/left_all"
val rightTablePath = "/data/tmp/join_bench/right_all"

// Benchmark parameters
val benchmarkIterations = 5

// ============================================================================
// STEP 1: Generate test data with distinct keys for distinct join optimization
// ============================================================================

println("Generating test data with distinct keys...")

// Left table with distinct keys (for testing distinct join optimization)
val leftConfig = TableGenConfig(
  numRows = leftNumRows,
  keyColumns = Seq(
    KeyColumnSpec(
      name = "key1",
      dataType = "int",
      minSeed = 0,
      maxSeed = distinctKeys - 1,
      distribution = DistinctDistribution()  // Each key appears exactly once per distinct value
    )
  ),
  payloadColumns = Seq(
    PayloadColumnSpec("payload1", "int", minSeed = 0, maxSeed = 1000),
    PayloadColumnSpec("payload2", "long", minSeed = 0, maxSeed = 1000)
  ),
  outputPath = leftTablePath
)

// Right table with same key distribution (smaller for testing build side selection)
val rightConfig = TableGenConfig(
  numRows = rightNumRows,
  keyColumns = Seq(
    KeyColumnSpec("key1", "int", minSeed = 0, maxSeed = distinctKeys - 1,
      distribution = DistinctDistribution())
  ),
  payloadColumns = Seq(
    PayloadColumnSpec("payload1", "int", minSeed = 0, maxSeed = 1000)
  ),
  outputPath = rightTablePath
)

// Generate both tables with correlated keys
generateJoinTables(leftConfig, rightConfig, keyGroupId, spark)

println("Data generation complete!")
println(s"Left table: $leftNumRows rows, Right table: $rightNumRows rows")
println(s"Distinct keys: $distinctKeys (enables distinct join optimization)")

// ============================================================================
// STEP 2: Run comprehensive benchmarks for all join types
// ============================================================================

println("\n" + "="*80)
println("PART 1: CACHING PERFORMANCE DEMONSTRATION")
println("="*80 + "\n")
println("This section demonstrates the performance impact of join object caching.")
println("With more iterations, the benefit of caching becomes more pronounced.\n")

// Print TSV header
printTSVHeader()

// Base configuration
val baseConfig = JoinBenchmarkConfig(
  testName = "base",
  leftParquetPath = leftTablePath,
  rightParquetPath = rightTablePath,
  joinType = InnerJoin,
  joinStrategy = HashWithPostStrategy,
  buildSide = RightBuild,
  optimizations = JoinOptimizations(),
  conditionalFilter = None,
  iterations = benchmarkIterations,
  printHeader = false
)

// Use more iterations for caching tests to show the performance benefit clearly
val cachingIterations = 20

println("\n--- CACHING PERFORMANCE TESTS (Inner Join) ---")
println("Testing with " + cachingIterations + " iterations to demonstrate caching benefit")

// Test 0.1: Baseline - no optimizations at all
val r0_1 = runBenchmark(baseConfig.copy(
  testName = "baseline_no_opt",
  iterations = cachingIterations,
  optimizations = JoinOptimizations(
    allowBuildSideSwap = false,
    useDistinctJoin = false,
    cacheJoinObject = false,
    cacheDistinctFlag = false,
    cacheRemapping = false
  )
), spark)
printResultsTSV(r0_1)

// Test 0.2: Just useDistinctJoin (check distinctness every iteration)
val r0_2 = runBenchmark(baseConfig.copy(
  testName = "distinct_no_cache",
  iterations = cachingIterations,
  optimizations = JoinOptimizations(
    useDistinctJoin = true,
    cacheDistinctFlag = false,
    cacheJoinObject = false
  )
), spark)
printResultsTSV(r0_2)

// Test 0.3: useDistinctJoin + cacheDistinctFlag (check once, reuse result)
val r0_3 = runBenchmark(baseConfig.copy(
  testName = "distinct_cache_flag",
  iterations = cachingIterations,
  optimizations = JoinOptimizations(
    useDistinctJoin = true,
    cacheDistinctFlag = true,
    cacheJoinObject = false
  )
), spark)
printResultsTSV(r0_3)

// Test 0.4: cacheJoinObject only (build hash table once)
val r0_4 = runBenchmark(baseConfig.copy(
  testName = "cache_join_object",
  iterations = cachingIterations,
  optimizations = JoinOptimizations(
    useDistinctJoin = false,
    cacheJoinObject = true
  )
), spark)
printResultsTSV(r0_4)

// Test 0.5: useDistinctJoin + cacheJoinObject (check distinct, build once)
val r0_5 = runBenchmark(baseConfig.copy(
  testName = "distinct_cache_object",
  iterations = cachingIterations,
  optimizations = JoinOptimizations(
    useDistinctJoin = true,
    cacheDistinctFlag = false,
    cacheJoinObject = true
  )
), spark)
printResultsTSV(r0_5)

// Test 0.6: ALL CACHING OPTIMIZATIONS (best performance)
val r0_6 = runBenchmark(baseConfig.copy(
  testName = "all_caching_opts",
  iterations = cachingIterations,
  optimizations = JoinOptimizations(
    useDistinctJoin = true,
    cacheDistinctFlag = true,
    cacheJoinObject = true
  )
), spark)
printResultsTSV(r0_6)

println("\n--- CACHING PERFORMANCE TESTS (Left Outer Join) ---")

// Test 0.7: Left Outer - baseline
val r0_7 = runBenchmark(baseConfig.copy(
  testName = "left_outer_baseline",
  joinType = LeftOuterJoin,
  buildSide = LeftBuild,
  iterations = cachingIterations,
  optimizations = JoinOptimizations(
    useDistinctJoin = false,
    cacheJoinObject = false
  )
), spark)
printResultsTSV(r0_7)

// Test 0.8: Left Outer - with caching
val r0_8 = runBenchmark(baseConfig.copy(
  testName = "left_outer_cached",
  joinType = LeftOuterJoin,
  buildSide = LeftBuild,
  iterations = cachingIterations,
  optimizations = JoinOptimizations(
    useDistinctJoin = true,
    cacheDistinctFlag = true,
    cacheJoinObject = true
  )
), spark)
printResultsTSV(r0_8)

println("\n--- CACHING PERFORMANCE TESTS (Semi Join) ---")

// Test 0.9: Semi - baseline
val r0_9 = runBenchmark(baseConfig.copy(
  testName = "semi_baseline",
  joinType = LeftSemiJoin,
  buildSide = LeftBuild,
  iterations = cachingIterations,
  optimizations = JoinOptimizations(
    useDistinctJoin = false,
    cacheJoinObject = false
  )
), spark)
printResultsTSV(r0_9)

// Test 0.10: Semi - with caching
val r0_10 = runBenchmark(baseConfig.copy(
  testName = "semi_cached",
  joinType = LeftSemiJoin,
  buildSide = LeftBuild,
  iterations = cachingIterations,
  optimizations = JoinOptimizations(
    useDistinctJoin = false,  // DistinctHashJoin not applicable for semi/anti
    cacheJoinObject = true
  )
), spark)
printResultsTSV(r0_10)

println("\n--- CACHING WITH SORT-MERGE STRATEGY ---")

// Test 0.11: Sort-merge baseline
val r0_11 = runBenchmark(baseConfig.copy(
  testName = "sortmerge_baseline",
  joinStrategy = SortWithPostStrategy,
  iterations = cachingIterations,
  optimizations = JoinOptimizations(
    cacheJoinObject = false
  )
), spark)
printResultsTSV(r0_11)

// Test 0.12: Sort-merge with caching
val r0_12 = runBenchmark(baseConfig.copy(
  testName = "sortmerge_cached",
  joinStrategy = SortWithPostStrategy,
  iterations = cachingIterations,
  optimizations = JoinOptimizations(
    cacheJoinObject = true
  )
), spark)
printResultsTSV(r0_12)

println("\n" + "="*80)
println("PART 2: COMPREHENSIVE JOIN TYPE TESTS")
println("="*80 + "\n")

println("\n--- INNER JOIN TESTS ---")

// Test 1: Inner join with left build
val r1 = runBenchmark(baseConfig.copy(
  testName = "inner_left_build",
  joinType = InnerJoin,
  buildSide = LeftBuild
), spark)
printResultsTSV(r1)

// Test 2: Inner join with right build
val r2 = runBenchmark(baseConfig.copy(
  testName = "inner_right_build",
  joinType = InnerJoin,
  buildSide = RightBuild
), spark)
printResultsTSV(r2)

// Test 3: Inner join with auto build side selection (should pick smaller = right)
val r3 = runBenchmark(baseConfig.copy(
  testName = "inner_auto_build",
  joinType = InnerJoin,
  buildSide = AutoPickSmallerIfAllowed
), spark)
printResultsTSV(r3)

// Test 4: Inner join with distinct optimization
val r4 = runBenchmark(baseConfig.copy(
  testName = "inner_distinct_opt",
  joinType = InnerJoin,
  buildSide = RightBuild,
  optimizations = JoinOptimizations(useDistinctJoin = true)
), spark)
printResultsTSV(r4)

// Test 5: Inner join with distinct optimization + caching
val r5 = runBenchmark(baseConfig.copy(
  testName = "inner_distinct_cached",
  joinType = InnerJoin,
  buildSide = RightBuild,
  optimizations = JoinOptimizations(
    useDistinctJoin = true,
    cacheDistinctFlag = true,
    cacheJoinObject = true
  )
), spark)
printResultsTSV(r5)

println("\n--- LEFT OUTER JOIN TESTS ---")

// Test 6: Left outer join with left build
val r6 = runBenchmark(baseConfig.copy(
  testName = "left_outer_left_build",
  joinType = LeftOuterJoin,
  buildSide = LeftBuild
), spark)
printResultsTSV(r6)

// Test 7: Left outer join with right build (swapped)
val r7 = runBenchmark(baseConfig.copy(
  testName = "left_outer_right_build",
  joinType = LeftOuterJoin,
  buildSide = RightBuild,
  optimizations = JoinOptimizations(allowBuildSideSwap = true)
), spark)
printResultsTSV(r7)

// Test 8: Left outer join with auto build side
val r8 = runBenchmark(baseConfig.copy(
  testName = "left_outer_auto_build",
  joinType = LeftOuterJoin,
  buildSide = AutoPickSmallerIfAllowed
), spark)
printResultsTSV(r8)

// Test 9: Left outer join with distinct optimization
val r9 = runBenchmark(baseConfig.copy(
  testName = "left_outer_distinct",
  joinType = LeftOuterJoin,
  buildSide = LeftBuild,
  optimizations = JoinOptimizations(useDistinctJoin = true, cacheDistinctFlag = true)
), spark)
printResultsTSV(r9)

println("\n--- RIGHT OUTER JOIN TESTS ---")

// Test 10: Right outer join with right build
val r10 = runBenchmark(baseConfig.copy(
  testName = "right_outer_right_build",
  joinType = RightOuterJoin,
  buildSide = RightBuild
), spark)
printResultsTSV(r10)

// Test 11: Right outer join with left build (swapped)
val r11 = runBenchmark(baseConfig.copy(
  testName = "right_outer_left_build",
  joinType = RightOuterJoin,
  buildSide = LeftBuild,
  optimizations = JoinOptimizations(allowBuildSideSwap = true)
), spark)
printResultsTSV(r11)

// Test 12: Right outer join with auto build side
val r12 = runBenchmark(baseConfig.copy(
  testName = "right_outer_auto_build",
  joinType = RightOuterJoin,
  buildSide = AutoPickSmallerIfAllowed
), spark)
printResultsTSV(r12)

println("\n--- FULL OUTER JOIN TESTS ---")

// Test 13: Full outer join with left build
val r13 = runBenchmark(baseConfig.copy(
  testName = "full_outer_left_build",
  joinType = FullOuterJoin,
  buildSide = LeftBuild
), spark)
printResultsTSV(r13)

// Test 14: Full outer join with right build
val r14 = runBenchmark(baseConfig.copy(
  testName = "full_outer_right_build",
  joinType = FullOuterJoin,
  buildSide = RightBuild
), spark)
printResultsTSV(r14)

// Test 15: Full outer join with auto build side (should pick smaller)
val r15 = runBenchmark(baseConfig.copy(
  testName = "full_outer_auto_build",
  joinType = FullOuterJoin,
  buildSide = AutoPickSmallerIfAllowed
), spark)
printResultsTSV(r15)

println("\n--- LEFT SEMI JOIN TESTS ---")

// Test 16: Left semi join with left build
val r16 = runBenchmark(baseConfig.copy(
  testName = "left_semi_left_build",
  joinType = LeftSemiJoin,
  buildSide = LeftBuild
), spark)
printResultsTSV(r16)

// Test 17: Left semi join with right build (swapped)
val r17 = runBenchmark(baseConfig.copy(
  testName = "left_semi_right_build",
  joinType = LeftSemiJoin,
  buildSide = RightBuild,
  optimizations = JoinOptimizations(allowBuildSideSwap = true)
), spark)
printResultsTSV(r17)

// Test 18: Left semi join with auto build side
val r18 = runBenchmark(baseConfig.copy(
  testName = "left_semi_auto_build",
  joinType = LeftSemiJoin,
  buildSide = AutoPickSmallerIfAllowed
), spark)
printResultsTSV(r18)

println("\n--- LEFT ANTI JOIN TESTS ---")

// Test 19: Left anti join with left build
val r19 = runBenchmark(baseConfig.copy(
  testName = "left_anti_left_build",
  joinType = LeftAntiJoin,
  buildSide = LeftBuild
), spark)
printResultsTSV(r19)

// Test 20: Left anti join with right build (swapped)
val r20 = runBenchmark(baseConfig.copy(
  testName = "left_anti_right_build",
  joinType = LeftAntiJoin,
  buildSide = RightBuild,
  optimizations = JoinOptimizations(allowBuildSideSwap = true)
), spark)
printResultsTSV(r20)

// Test 21: Left anti join with auto build side
val r21 = runBenchmark(baseConfig.copy(
  testName = "left_anti_auto_build",
  joinType = LeftAntiJoin,
  buildSide = AutoPickSmallerIfAllowed
), spark)
printResultsTSV(r21)

println("\n--- DIRECT HASH JOIN STRATEGY TESTS (Inner Join Only) ---")

// Test 22: HashJoinStrategy with left build
val r22 = runBenchmark(baseConfig.copy(
  testName = "inner_hash_left_build",
  joinType = InnerJoin,
  joinStrategy = HashJoinStrategy,
  buildSide = LeftBuild
), spark)
printResultsTSV(r22)

// Test 23: HashJoinStrategy with right build
val r23 = runBenchmark(baseConfig.copy(
  testName = "inner_hash_right_build",
  joinType = InnerJoin,
  joinStrategy = HashJoinStrategy,
  buildSide = RightBuild
), spark)
printResultsTSV(r23)

// Test 24: HashJoinStrategy with auto build side (should pick smaller = right)
val r24 = runBenchmark(baseConfig.copy(
  testName = "inner_hash_auto_build",
  joinType = InnerJoin,
  joinStrategy = HashJoinStrategy,
  buildSide = AutoPickSmallerIfAllowed
), spark)
printResultsTSV(r24)

// Test 25: HashJoinStrategy with distinct optimization
val r25 = runBenchmark(baseConfig.copy(
  testName = "inner_hash_distinct",
  joinType = InnerJoin,
  joinStrategy = HashJoinStrategy,
  buildSide = RightBuild,
  optimizations = JoinOptimizations(useDistinctJoin = true, cacheDistinctFlag = true)
), spark)
printResultsTSV(r25)

println("\n--- STRATEGY COMPARISON TESTS (Inner Join) ---")

// Test 26: HashWithPostStrategy (inner join + post-processing path)
val r26 = runBenchmark(baseConfig.copy(
  testName = "inner_hash_post_strategy",
  joinType = InnerJoin,
  joinStrategy = HashWithPostStrategy,
  buildSide = RightBuild
), spark)
printResultsTSV(r26)

// Test 27: SortWithPostStrategy (sort-merge inner join + post-processing)
val r27 = runBenchmark(baseConfig.copy(
  testName = "inner_sort_post_strategy",
  joinType = InnerJoin,
  joinStrategy = SortWithPostStrategy,
  buildSide = RightBuild
), spark)
printResultsTSV(r27)

println("\n--- OPTIMIZATION COMBINATION TESTS ---")

// Test 28: All optimizations enabled
val r28 = runBenchmark(baseConfig.copy(
  testName = "inner_all_optimizations",
  joinType = InnerJoin,
  buildSide = AutoPickSmallerIfAllowed,
  optimizations = JoinOptimizations(
    allowBuildSideSwap = true,
    useDistinctJoin = true,
    cacheJoinObject = true,
    cacheDistinctFlag = true
  )
), spark)
printResultsTSV(r28)

// Test 29: Left outer with all applicable optimizations
val r29 = runBenchmark(baseConfig.copy(
  testName = "left_outer_all_optimizations",
  joinType = LeftOuterJoin,
  buildSide = AutoPickSmallerIfAllowed,
  optimizations = JoinOptimizations(
    allowBuildSideSwap = true,
    useDistinctJoin = true,
    cacheJoinObject = true,
    cacheDistinctFlag = true
  )
), spark)
printResultsTSV(r29)

println("\n" + "="*80)
println("Comprehensive Join Type Benchmark Complete!")
println("="*80)
println(s"\nTotal tests executed: 41")
println(s"\nPart 1 - Caching Performance Tests (12 tests):")
println(s"  - Demonstrates performance impact of cacheDistinctFlag and cacheJoinObject")
println(s"  - Tests with $cachingIterations iterations to show caching benefit clearly")
println(s"  - Covers: Inner, LeftOuter, Semi joins with Hash and SortMerge strategies")
println(s"\nPart 2 - Comprehensive Join Type Tests (29 tests):")
println(s"  - Join types: Inner, LeftOuter, RightOuter, FullOuter, LeftSemi, LeftAnti")
println(s"  - Build side modes: LeftBuild, RightBuild, AutoPickSmallerIfAllowed")
println(s"  - Strategies: HashJoinStrategy, HashWithPostStrategy, SortWithPostStrategy")
println(s"  - Optimizations: useDistinctJoin, cacheDistinctFlag, cacheJoinObject, allowBuildSideSwap")
println(s"\nKEY INSIGHTS TO LOOK FOR:")
println(s"  1. Compare baseline_no_opt vs all_caching_opts to see total caching benefit")
println(s"  2. Compare distinct_no_cache vs distinct_cache_flag to see cacheDistinctFlag benefit")
println(s"  3. Compare baseline_no_opt vs cache_join_object to see cacheJoinObject benefit")
println(s"  4. Look at mean time reduction across multiple iterations with caching enabled")
println("\nYou can copy the TSV output above and paste into a spreadsheet for analysis.")


