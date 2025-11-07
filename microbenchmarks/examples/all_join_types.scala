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
  joinStrategy = HashObjectWithPostStrategy,
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
  joinStrategy = SortObjectWithPostStrategy,
  iterations = cachingIterations,
  optimizations = JoinOptimizations(
    cacheJoinObject = false
  )
), spark)
printResultsTSV(r0_11)

// Test 0.12: Sort-merge with caching
val r0_12 = runBenchmark(baseConfig.copy(
  testName = "sortmerge_cached",
  joinStrategy = SortObjectWithPostStrategy,
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

println("\n--- DIRECT HASH JOIN STRATEGY TESTS (All Join Types) ---")
println("Testing new direct build holders: InnerHashBuildHolder, LeftOuterHashBuildHolder,")
println("RightOuterHashBuildHolder, FullOuterHashBuildHolder, SemiAntiHashBuildHolder")

// Test 22: HashObjectStrategy Inner - left build
val r22 = runBenchmark(baseConfig.copy(
  testName = "inner_hash_left_build",
  joinType = InnerJoin,
  joinStrategy = HashObjectStrategy,
  buildSide = LeftBuild
), spark)
printResultsTSV(r22)

// Test 23: HashObjectStrategy Inner - right build
val r23 = runBenchmark(baseConfig.copy(
  testName = "inner_hash_right_build",
  joinType = InnerJoin,
  joinStrategy = HashObjectStrategy,
  buildSide = RightBuild
), spark)
printResultsTSV(r23)

// Test 24: HashObjectStrategy Inner - auto build (can swap, picks smaller = right)
val r24 = runBenchmark(baseConfig.copy(
  testName = "inner_hash_auto_build",
  joinType = InnerJoin,
  joinStrategy = HashObjectStrategy,
  buildSide = AutoPickSmallerIfAllowed
), spark)
printResultsTSV(r24)

// Test 25: HashObjectStrategy Inner - distinct optimization
val r25 = runBenchmark(baseConfig.copy(
  testName = "inner_hash_distinct",
  joinType = InnerJoin,
  joinStrategy = HashObjectStrategy,
  buildSide = RightBuild,
  optimizations = JoinOptimizations(useDistinctJoin = true, cacheDistinctFlag = true)
), spark)
printResultsTSV(r25)

// Test 26: HashObjectStrategy Inner - with caching
val r26 = runBenchmark(baseConfig.copy(
  testName = "inner_hash_cached",
  joinType = InnerJoin,
  joinStrategy = HashObjectStrategy,
  buildSide = RightBuild,
  optimizations = JoinOptimizations(
    useDistinctJoin = true,
    cacheDistinctFlag = true,
    cacheJoinObject = true
  )
), spark)
printResultsTSV(r26)

// Test 27: HashObjectStrategy LeftOuter - left build (CANNOT swap)
val r27 = runBenchmark(baseConfig.copy(
  testName = "left_outer_hash_left",
  joinType = LeftOuterJoin,
  joinStrategy = HashObjectStrategy,
  buildSide = LeftBuild
), spark)
printResultsTSV(r27)

// Test 28: HashObjectStrategy LeftOuter - caching only (no distinct - DistinctHashJoin.leftJoin() is for remapping)
val r28 = runBenchmark(baseConfig.copy(
  testName = "left_outer_hash_cached_only",
  joinType = LeftOuterJoin,
  joinStrategy = HashObjectStrategy,
  buildSide = LeftBuild,
  optimizations = JoinOptimizations(cacheJoinObject = true)
), spark)
printResultsTSV(r28)

// Test 29: Removed - duplicate of test 28 with different name

// Test 30: HashObjectStrategy RightOuter - right build (CANNOT swap)
val r30 = runBenchmark(baseConfig.copy(
  testName = "right_outer_hash_right",
  joinType = RightOuterJoin,
  joinStrategy = HashObjectStrategy,
  buildSide = RightBuild
), spark)
printResultsTSV(r30)

// Test 31: HashObjectStrategy RightOuter - with caching (no distinct - DistinctHashJoin not supported)
val r31 = runBenchmark(baseConfig.copy(
  testName = "right_outer_hash_cached",
  joinType = RightOuterJoin,
  joinStrategy = HashObjectStrategy,
  buildSide = RightBuild,
  optimizations = JoinOptimizations(cacheJoinObject = true)
), spark)
printResultsTSV(r31)

// Test 32: Removed - was duplicate with distinct flags that don't apply

// Test 33: HashObjectStrategy FullOuter - left build
val r33 = runBenchmark(baseConfig.copy(
  testName = "full_outer_hash_left",
  joinType = FullOuterJoin,
  joinStrategy = HashObjectStrategy,
  buildSide = LeftBuild
), spark)
printResultsTSV(r33)

// Test 34: HashObjectStrategy FullOuter - right build
val r34 = runBenchmark(baseConfig.copy(
  testName = "full_outer_hash_right",
  joinType = FullOuterJoin,
  joinStrategy = HashObjectStrategy,
  buildSide = RightBuild
), spark)
printResultsTSV(r34)

// Test 35: HashObjectStrategy FullOuter - auto build (CAN swap, picks smaller = right)
val r35 = runBenchmark(baseConfig.copy(
  testName = "full_outer_hash_auto",
  joinType = FullOuterJoin,
  joinStrategy = HashObjectStrategy,
  buildSide = AutoPickSmallerIfAllowed
), spark)
printResultsTSV(r35)

// Test 36: HashObjectStrategy FullOuter - with caching
val r36 = runBenchmark(baseConfig.copy(
  testName = "full_outer_hash_cached",
  joinType = FullOuterJoin,
  joinStrategy = HashObjectStrategy,
  buildSide = RightBuild,
  optimizations = JoinOptimizations(cacheJoinObject = true)
), spark)
printResultsTSV(r36)

// Test 37: HashObjectStrategy Semi - left build (CANNOT swap)
val r37 = runBenchmark(baseConfig.copy(
  testName = "semi_hash_left",
  joinType = LeftSemiJoin,
  joinStrategy = HashObjectStrategy,
  buildSide = LeftBuild
), spark)
printResultsTSV(r37)

// Test 38: HashObjectStrategy Semi - with caching (FilteredJoin supports caching!)
val r38 = runBenchmark(baseConfig.copy(
  testName = "semi_hash_cached",
  joinType = LeftSemiJoin,
  joinStrategy = HashObjectStrategy,
  buildSide = LeftBuild,
  optimizations = JoinOptimizations(cacheJoinObject = true)
), spark)
printResultsTSV(r38)

// Test 39: HashObjectStrategy Semi - auto build (cannot swap, must use left)
val r39 = runBenchmark(baseConfig.copy(
  testName = "semi_hash_auto",
  joinType = LeftSemiJoin,
  joinStrategy = HashObjectStrategy,
  buildSide = AutoMeetJoinRequirement
), spark)
printResultsTSV(r39)

// Test 40: HashObjectStrategy Anti - left build (CANNOT swap)
val r40 = runBenchmark(baseConfig.copy(
  testName = "anti_hash_left",
  joinType = LeftAntiJoin,
  joinStrategy = HashObjectStrategy,
  buildSide = LeftBuild
), spark)
printResultsTSV(r40)

// Test 41: HashObjectStrategy Anti - with caching (FilteredJoin supports caching!)
val r41 = runBenchmark(baseConfig.copy(
  testName = "anti_hash_cached",
  joinType = LeftAntiJoin,
  joinStrategy = HashObjectStrategy,
  buildSide = LeftBuild,
  optimizations = JoinOptimizations(cacheJoinObject = true)
), spark)
printResultsTSV(r41)

// Test 42: HashObjectStrategy Anti - auto build (cannot swap, must use left)
val r42 = runBenchmark(baseConfig.copy(
  testName = "anti_hash_auto",
  joinType = LeftAntiJoin,
  joinStrategy = HashObjectStrategy,
  buildSide = AutoMeetJoinRequirement
), spark)
printResultsTSV(r42)

println("\n--- STRATEGY COMPARISON TESTS (Inner Join) ---")

// Test 43: HashObjectWithPostStrategy (inner join + post-processing path)
val r43 = runBenchmark(baseConfig.copy(
  testName = "inner_hash_post_strategy",
  joinType = InnerJoin,
  joinStrategy = HashObjectWithPostStrategy,
  buildSide = RightBuild
), spark)
printResultsTSV(r43)

// Test 44: SortObjectWithPostStrategy (sort-merge inner join + post-processing)
val r44 = runBenchmark(baseConfig.copy(
  testName = "inner_sort_post_strategy",
  joinType = InnerJoin,
  joinStrategy = SortObjectWithPostStrategy,
  buildSide = RightBuild
), spark)
printResultsTSV(r44)

println("\n--- OPTIMIZATION COMBINATION TESTS ---")

// Test 45: All optimizations enabled with HashObjectWithPostStrategy
val r45 = runBenchmark(baseConfig.copy(
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
printResultsTSV(r45)

// Test 46: Left outer with all applicable optimizations
val r46 = runBenchmark(baseConfig.copy(
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
printResultsTSV(r46)

println("\n" + "="*80)
println("Comprehensive Join Type Benchmark Complete!")
println("="*80)
println(s"\nTotal tests executed: 58")
println(s"\nPart 1 - Caching Performance Tests (12 tests):")
println(s"  - Demonstrates performance impact of cacheDistinctFlag and cacheJoinObject")
println(s"  - Tests with $cachingIterations iterations to show caching benefit clearly")
println(s"  - Covers: Inner, LeftOuter, Semi joins with Hash and SortMerge strategies")
println(s"\nPart 2 - Comprehensive Join Type Tests (46 tests):")
println(s"  - Join types: Inner, LeftOuter, RightOuter, FullOuter, LeftSemi, LeftAnti")
println(s"  - Build side modes: LeftBuild, RightBuild, AutoPickSmallerIfAllowed, AutoMeetJoinRequirement")
println(s"  - Strategies: HashObjectStrategy, HashObjectWithPostStrategy, SortObjectWithPostStrategy")
println(s"  - Optimizations: useDistinctJoin, cacheDistinctFlag, cacheJoinObject, allowBuildSideSwap")
println(s"\n**NEW: Phase 2 Direct HashObjectStrategy Tests (tests 22-42):**")
println(s"  - Tests all new direct build holders:")
println(s"    * InnerHashBuildHolder (5 tests) - supports DistinctHashJoin + caching")
println(s"    * LeftOuterHashBuildHolder (2 tests) - caching only (no DistinctHashJoin - leftJoin() is for remapping)")
println(s"    * RightOuterHashBuildHolder (2 tests) - caching only (no DistinctHashJoin)")
println(s"    * FullOuterHashBuildHolder (4 tests) - caching only (no DistinctHashJoin)")
println(s"    * SemiAntiHashBuildHolder (6 tests) - FilteredJoin supports caching! (no distinct optimization)")
println(s"  - Important: DistinctHashJoin.leftJoin() returns single GatherMap for remapping,")
println(s"               NOT two maps for left outer joins. Only InnerHashBuildHolder supports DistinctHashJoin.")
println(s"  - FilteredJoin IS a join object like HashJoin - supports create() + multiple semiJoin()/antiJoin() calls")
println(s"  - Tests join object caching for ALL join types (including Semi/Anti with FilteredJoin)")
println(s"  - Validates correct build side swapping behavior:")
println(s"    * Inner & FullOuter: CAN swap (symmetric)")
println(s"    * LeftOuter, RightOuter, Semi, Anti: CANNOT swap (asymmetric)")
println(s"\nKEY INSIGHTS TO LOOK FOR:")
println(s"  1. Compare baseline_no_opt vs all_caching_opts to see total caching benefit")
println(s"  2. Compare distinct_no_cache vs distinct_cache_flag to see cacheDistinctFlag benefit")
println(s"  3. Compare baseline_no_opt vs cache_join_object to see cacheJoinObject benefit")
println(s"  4. Look at mean time reduction across multiple iterations with caching enabled")
println(s"  5. Compare HashObjectStrategy vs HashObjectWithPostStrategy for same join types")
println(s"  6. Verify that auto build side selection works correctly for each join type")
println(s"  7. Note that LeftOuter/RightOuter with HashObjectStrategy don't benefit from DistinctHashJoin")
println(s"  8. Semi/Anti joins with FilteredJoin now support caching - compare cached vs non-cached!")
println("\nYou can copy the TSV output above and paste into a spreadsheet for analysis.")

// ============================================================================
// STEP 3: Key Remapping Tests with Different Key Types
// ============================================================================

println("\n" + "="*80)
println("PART 3: KEY REMAPPING PERFORMANCE TESTS")
println("="*80 + "\n")
println("This section tests key remapping performance with different key types.")
println("Key remapping converts complex keys to integers for faster joins.")
println("Testing with: int, long, decimal(38,0), and string keys\n")

// Function to generate data with different key types
def generateKeyTypeData(keyType: String, basePath: String): (String, String) = {
  val leftPath = s"$basePath/left_$keyType"
  val rightPath = s"$basePath/right_$keyType"
  
  println(s"Generating data with $keyType keys...")
  
  val leftCfg = TableGenConfig(
    numRows = leftNumRows,
    keyColumns = Seq(
      KeyColumnSpec(
        name = "key1",
        dataType = keyType,
        minSeed = 0,
        maxSeed = distinctKeys - 1,
        distribution = DistinctDistribution()
      )
    ),
    payloadColumns = Seq(
      PayloadColumnSpec("payload1", "int", minSeed = 0, maxSeed = 1000),
      PayloadColumnSpec("payload2", "long", minSeed = 0, maxSeed = 1000)
    ),
    outputPath = leftPath
  )
  
  val rightCfg = TableGenConfig(
    numRows = rightNumRows,
    keyColumns = Seq(
      KeyColumnSpec("key1", keyType, minSeed = 0, maxSeed = distinctKeys - 1,
        distribution = DistinctDistribution())
    ),
    payloadColumns = Seq(
      PayloadColumnSpec("payload1", "int", minSeed = 0, maxSeed = 1000)
    ),
    outputPath = rightPath
  )
  
  generateJoinTables(leftCfg, rightCfg, keyGroupId, spark)
  println(s"  Data generated: $leftPath and $rightPath")
  
  (leftPath, rightPath)
}

val keyTypesBasePath = "/data/tmp/join_bench/key_types"

// Test key types: int, long, decimal(38,0), string
val keyTypes = Seq("int", "long", "decimal(38,0)", "string")

// Generate data for each key type
val keyTypeDataPaths = keyTypes.map { keyType =>
  keyType -> generateKeyTypeData(keyType, keyTypesBasePath)
}.toMap

println("\n--- KEY REMAPPING PERFORMANCE TESTS ---")
println("Testing HashObjectWithPostStrategy and SortObjectWithPostStrategy")
println("Comparing: no remapping vs remapped (cached) vs remapped (non-cached)\n")

val remappingIterations = 10

// Test each key type with different remapping configurations
for (keyType <- keyTypes) {
  val (leftPath, rightPath) = keyTypeDataPaths(keyType)
  
  println(s"\n=== Testing $keyType keys ===")
  
  val remapBaseConfig = JoinBenchmarkConfig(
    testName = s"remap_${keyType}_base",
    leftParquetPath = leftPath,
    rightParquetPath = rightPath,
    joinType = InnerJoin,
    joinStrategy = HashObjectWithPostStrategy,
    buildSide = RightBuild,
    optimizations = JoinOptimizations(),
    conditionalFilter = None,
    iterations = remappingIterations,
    printHeader = false
  )
  
  // Test 1: HashObjectWithPostStrategy - No remapping
  val r1 = runBenchmark(remapBaseConfig.copy(
    testName = s"${keyType}_hash_no_remap",
    optimizations = JoinOptimizations(
      remapComplexKeysToInts = false,
      cacheRemapping = false,
      cacheJoinObject = false
    )
  ), spark)
  printResultsTSV(r1)
  
  // Test 2: HashObjectWithPostStrategy - Remapping with caching
  val r2 = runBenchmark(remapBaseConfig.copy(
    testName = s"${keyType}_hash_remap_cached",
    optimizations = JoinOptimizations(
      remapComplexKeysToInts = true,
      cacheRemapping = true,
      cacheJoinObject = false
    )
  ), spark)
  printResultsTSV(r2)
  
  // Test 3: HashObjectWithPostStrategy - Remapping without caching
  val r3 = runBenchmark(remapBaseConfig.copy(
    testName = s"${keyType}_hash_remap_nocache",
    optimizations = JoinOptimizations(
      remapComplexKeysToInts = true,
      cacheRemapping = false,
      cacheJoinObject = false
    )
  ), spark)
  printResultsTSV(r3)
  
  // Test 4: HashObjectWithPostStrategy - Remapping + join object caching
  val r4 = runBenchmark(remapBaseConfig.copy(
    testName = s"${keyType}_hash_both_cached",
    optimizations = JoinOptimizations(
      remapComplexKeysToInts = true,
      cacheRemapping = true,
      cacheJoinObject = true
    )
  ), spark)
  printResultsTSV(r4)
  
  // Test 5: SortObjectWithPostStrategy - No remapping
  val r5 = runBenchmark(remapBaseConfig.copy(
    testName = s"${keyType}_sort_no_remap",
    joinStrategy = SortObjectWithPostStrategy,
    optimizations = JoinOptimizations(
      remapComplexKeysToInts = false,
      cacheRemapping = false,
      cacheJoinObject = false
    )
  ), spark)
  printResultsTSV(r5)
  
  // Test 6: SortObjectWithPostStrategy - Remapping with caching
  val r6 = runBenchmark(remapBaseConfig.copy(
    testName = s"${keyType}_sort_remap_cached",
    joinStrategy = SortObjectWithPostStrategy,
    optimizations = JoinOptimizations(
      remapComplexKeysToInts = true,
      cacheRemapping = true,
      cacheJoinObject = false
    )
  ), spark)
  printResultsTSV(r6)
  
  // Test 7: SortObjectWithPostStrategy - Remapping without caching
  val r7 = runBenchmark(remapBaseConfig.copy(
    testName = s"${keyType}_sort_remap_nocache",
    joinStrategy = SortObjectWithPostStrategy,
    optimizations = JoinOptimizations(
      remapComplexKeysToInts = true,
      cacheRemapping = false,
      cacheJoinObject = false
    )
  ), spark)
  printResultsTSV(r7)
  
  // Test 8: SortObjectWithPostStrategy - Remapping + join object caching
  val r8 = runBenchmark(remapBaseConfig.copy(
    testName = s"${keyType}_sort_both_cached",
    joinStrategy = SortObjectWithPostStrategy,
    optimizations = JoinOptimizations(
      remapComplexKeysToInts = true,
      cacheRemapping = true,
      cacheJoinObject = true
    )
  ), spark)
  printResultsTSV(r8)
}

println("\n" + "="*80)
println("Key Remapping Performance Tests Complete!")
println("="*80)
println(s"\nTested key types: ${keyTypes.mkString(", ")}")
println(s"Total key remapping tests: ${keyTypes.size * 8} (8 tests per key type)")
println(s"\nFor each key type, tested:")
println(s"  1. HashObjectWithPostStrategy - no remapping (baseline)")
println(s"  2. HashObjectWithPostStrategy - remapping with caching")
println(s"  3. HashObjectWithPostStrategy - remapping without caching")
println(s"  4. HashObjectWithPostStrategy - remapping + join object caching")
println(s"  5. SortObjectWithPostStrategy - no remapping (baseline)")
println(s"  6. SortObjectWithPostStrategy - remapping with caching")
println(s"  7. SortObjectWithPostStrategy - remapping without caching")
println(s"  8. SortObjectWithPostStrategy - remapping + join object caching")
println(s"\nKEY INSIGHTS TO LOOK FOR:")
println(s"  1. Remapping overhead for different key types")
println(s"  2. Performance benefit of cacheRemapping vs non-cached remapping")
println(s"  3. String and decimal keys may benefit most from remapping")
println(s"  4. Int keys should show minimal difference (already integers)")
println(s"  5. Compare independent caching (remap only vs join object only)")
println("\nYou can copy the TSV output above and paste into a spreadsheet for analysis.")


