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

// Distinct Join Performance Analysis
// 
// This benchmark measures:
// 1. Performance benefit of DistinctHashJoin when data IS distinct
//    - Compares DistinctHashJoin vs regular HashJoin
//    - Tests with 100% distinct keys
// 
// 2. Overhead of distinctness check when data is NOT distinct
//    - Compares HashJoin with useDistinctJoin=true vs false
//    - Tests with duplicate keys (50% cardinality)
// 
// Varies across input sizes and key types to understand scaling behavior.
// 
// Runtime: ~3-5 minutes (including data generation and warmup)

import com.nvidia.spark.rapids.benchmarks.JoinBenchmarkDataGen._
import com.nvidia.spark.rapids.benchmarks.JoinBenchmarkRunner._
import org.apache.spark.sql.tests.datagen._

val baseDir = "/data/tmp/distinct_join_benchmark"

case class TestConfig(
  name: String,
  leftRows: Long,
  rightRows: Long,
  keyType: String,
  distinctKeys: Long,  // Number of distinct keys (for controlling cardinality)
  isDistinct: Boolean   // True if keys are 100% distinct
)

// ============================================================================
// PART 1: Distinct Data Tests - Measure benefit of DistinctHashJoin
// ============================================================================

val distinctTests = Seq(
  // Vary input size with INT keys (100% distinct keys)
  TestConfig("distinct_INT_10K", 10000, 10000, "int", 10000, isDistinct = true),
  TestConfig("distinct_INT_100K", 100000, 100000, "int", 100000, isDistinct = true),
  TestConfig("distinct_INT_1M", 1000000, 1000000, "int", 1000000, isDistinct = true),
  TestConfig("distinct_INT_5M", 5000000, 5000000, "int", 5000000, isDistinct = true),
  TestConfig("distinct_INT_10M", 10000000, 10000000, "int", 10000000, isDistinct = true),
  
  // Vary input size with STRING keys (100% distinct keys)
  TestConfig("distinct_STRING_10K", 10000, 10000, "string", 10000, isDistinct = true),
  TestConfig("distinct_STRING_100K", 100000, 100000, "string", 100000, isDistinct = true),
  TestConfig("distinct_STRING_1M", 1000000, 1000000, "string", 1000000, isDistinct = true),
  TestConfig("distinct_STRING_5M", 5000000, 5000000, "string", 5000000, isDistinct = true),
  TestConfig("distinct_STRING_10M", 10000000, 10000000, "string", 10000000, isDistinct = true),
  
  // LONG keys at 1M for comparison
  TestConfig("distinct_LONG_1M", 1000000, 1000000, "long", 1000000, isDistinct = true)
)

// ============================================================================
// PART 2: Non-Distinct Data Tests - Measure overhead of distinctness check
// ============================================================================

val nonDistinctTests = Seq(
  // Vary input size with INT keys (50% cardinality - lots of duplicates)
  TestConfig("nondist_INT_10K", 10000, 10000, "int", 5000, isDistinct = false),
  TestConfig("nondist_INT_100K", 100000, 100000, "int", 50000, isDistinct = false),
  TestConfig("nondist_INT_1M", 1000000, 1000000, "int", 500000, isDistinct = false),
  TestConfig("nondist_INT_5M", 5000000, 5000000, "int", 2500000, isDistinct = false),
  TestConfig("nondist_INT_10M", 10000000, 10000000, "int", 5000000, isDistinct = false),
  
  // Vary input size with STRING keys (50% cardinality - lots of duplicates)
  TestConfig("nondist_STRING_10K", 10000, 10000, "string", 5000, isDistinct = false),
  TestConfig("nondist_STRING_100K", 100000, 100000, "string", 50000, isDistinct = false),
  TestConfig("nondist_STRING_1M", 1000000, 1000000, "string", 500000, isDistinct = false),
  TestConfig("nondist_STRING_5M", 5000000, 5000000, "string", 2500000, isDistinct = false),
  TestConfig("nondist_STRING_10M", 10000000, 10000000, "string", 5000000, isDistinct = false),
  
  // LONG keys at 1M for comparison
  TestConfig("nondist_LONG_1M", 1000000, 1000000, "long", 500000, isDistinct = false)
)

println("="*80)
println("Distinct Join Performance Analysis")
println("="*80)

// ============================================================================
// Data Generation
// ============================================================================

println("\nGenerating test datasets...")
val allTests = distinctTests ++ nonDistinctTests

allTests.foreach { config =>
  val cardinalityPct = (config.distinctKeys.toDouble / config.leftRows.toDouble) * 100.0
  println(f"  ${config.name}: ${config.leftRows} rows, ${config.keyType} keys, " +
    f"${config.distinctKeys} distinct (${cardinalityPct}%.0f%% cardinality)")
  
  val leftConfig = TableGenConfig(
    numRows = config.leftRows,
    keyColumns = Seq(
      KeyColumnSpec("id", config.keyType, minSeed = 0, maxSeed = config.distinctKeys - 1)
    ),
    payloadColumns = Seq(
      PayloadColumnSpec("value", "double")
    ),
    outputPath = s"$baseDir/${config.name}/left"
  )
  
  val rightConfig = TableGenConfig(
    numRows = config.rightRows,
    keyColumns = Seq(
      KeyColumnSpec("id", config.keyType, minSeed = 0, maxSeed = config.distinctKeys - 1)
    ),
    payloadColumns = Seq(
      PayloadColumnSpec("value", "double")
    ),
    outputPath = s"$baseDir/${config.name}/right"
  )
  
  generateJoinTables(leftConfig, rightConfig, keyGroupId = 1, spark)
}

println("\nDataset generation complete!")
println("="*80)
println()

// ============================================================================
// Benchmark Functions
// ============================================================================

// Warmup function
def warmup(config: TestConfig): Unit = {
  val warmupConfig = JoinBenchmarkConfig(
    testName = "warmup",
    leftParquetPath = s"$baseDir/${config.name}/left",
    rightParquetPath = s"$baseDir/${config.name}/right",
    joinType = InnerJoin,
    joinStrategy = HashObjectStrategy,
    buildSide = AutoPickSmallerIfAllowed,
    optimizations = JoinOptimizations(
      cacheJoinObject = false,
      useDistinctJoin = false
    ),
    conditionalFilter = None,
    iterations = 1,
    numThreads = 1,
    printHeader = false
  )
  runBenchmark(warmupConfig, spark)
}

// Run distinct data tests (compare different strategies)
def runDistinctTest(config: TestConfig, iterations: Int = 60): Seq[BenchmarkResults] = {
  warmup(config)
  
  val baseConfig = JoinBenchmarkConfig(
    testName = "",
    leftParquetPath = s"$baseDir/${config.name}/left",
    rightParquetPath = s"$baseDir/${config.name}/right",
    joinType = InnerJoin,
    joinStrategy = HashObjectStrategy,
    buildSide = AutoPickSmallerIfAllowed,
    optimizations = JoinOptimizations(cacheJoinObject = false),
    conditionalFilter = None,
    iterations = iterations,
    numThreads = 1,
    printHeader = false
  )
  
  // Test 1: DistinctHashJoin with distinctCount() each iteration
  val distinctNoCache = baseConfig.copy(
    testName = s"${config.name}_DistinctNoCache",
    optimizations = JoinOptimizations(
      cacheJoinObject = false,
      useDistinctJoin = true,
      cacheDistinctFlag = false  // Don't cache, measure distinctCount() each time
    )
  )
  val distinctNoCacheResult = runBenchmark(distinctNoCache, spark)
  
  // Test 2: DistinctHashJoin with cached distinctCount()
  val distinctCached = baseConfig.copy(
    testName = s"${config.name}_DistinctCached",
    optimizations = JoinOptimizations(
      cacheJoinObject = false,
      useDistinctJoin = true,
      cacheDistinctFlag = true  // Cache distinctCount() result
    )
  )
  val distinctCachedResult = runBenchmark(distinctCached, spark)
  
  // Test 3: Regular HashJoin (optimization disabled)
  val hashConfig = baseConfig.copy(
    testName = s"${config.name}_Hash",
    optimizations = JoinOptimizations(
      cacheJoinObject = false,
      useDistinctJoin = false
    )
  )
  val hashResult = runBenchmark(hashConfig, spark)
  
  Seq(distinctNoCacheResult, distinctCachedResult, hashResult)
}

// Run non-distinct data tests (measure distinctness check overhead)
def runNonDistinctTest(config: TestConfig, iterations: Int = 50): Seq[BenchmarkResults] = {
  warmup(config)
  
  val baseConfig = JoinBenchmarkConfig(
    testName = "",
    leftParquetPath = s"$baseDir/${config.name}/left",
    rightParquetPath = s"$baseDir/${config.name}/right",
    joinType = InnerJoin,
    joinStrategy = HashObjectStrategy,
    buildSide = AutoPickSmallerIfAllowed,
    optimizations = JoinOptimizations(cacheJoinObject = false),
    conditionalFilter = None,
    iterations = iterations,
    numThreads = 1,
    printHeader = false
  )
  
  // Test 1: HashJoin with distinctCount() each iteration (will fail and use regular HashJoin)
  val withCheckNoCache = baseConfig.copy(
    testName = s"${config.name}_CheckNoCache",
    optimizations = JoinOptimizations(
      cacheJoinObject = false,
      useDistinctJoin = true,
      cacheDistinctFlag = false  // Don't cache, measure distinctCount() overhead each iteration
    )
  )
  val withCheckNoCacheResult = runBenchmark(withCheckNoCache, spark)
  
  // Test 2: HashJoin with cached distinctCount() (checks once, then uses regular HashJoin)
  val withCheckCached = baseConfig.copy(
    testName = s"${config.name}_CheckCached",
    optimizations = JoinOptimizations(
      cacheJoinObject = false,
      useDistinctJoin = true,
      cacheDistinctFlag = true  // Cache the (negative) result
    )
  )
  val withCheckCachedResult = runBenchmark(withCheckCached, spark)
  
  // Test 3: HashJoin without distinctness check
  val noCheckConfig = baseConfig.copy(
    testName = s"${config.name}_NoCheck",
    optimizations = JoinOptimizations(
      cacheJoinObject = false,
      useDistinctJoin = false
    )
  )
  val noCheckResult = runBenchmark(noCheckConfig, spark)
  
  Seq(withCheckNoCacheResult, withCheckCachedResult, noCheckResult)
}

// ============================================================================
// PART 1: Run Distinct Data Tests
// ============================================================================

println("\n" + "="*80)
println("PART 1: DISTINCT DATA - Measuring Benefit of DistinctHashJoin")
println("="*80)
println()

case class DistinctStats(
  testName: String,
  rows: Long,
  keyType: String,
  distinctNoCacheMs: Double,
  distinctCachedMs: Double,
  regularHashMs: Double,
  distinctCheckOverheadMs: Double,  // Cost of distinctCount() per iteration
  speedupVsHash: Double
)

val distinctResults = scala.collection.mutable.ArrayBuffer[DistinctStats]()

printTSVHeader()

var testCount = 0
distinctTests.foreach { config =>
  testCount += 1
  println(s"[Part 1: $testCount/${distinctTests.size}] Testing ${config.name}...")
  
  val results = runDistinctTest(config)
  results.foreach(printResultsTSV)
  
  val distinctNoCacheMs = results(0).averageMs
  val distinctCachedMs = results(1).averageMs
  val hashMs = results(2).averageMs
  
  // Cost of distinctCount() check = difference between no-cache and cached versions
  val distinctCheckOverhead = distinctNoCacheMs - distinctCachedMs
  
  // Speedup is based on cached version (pure join performance)
  val speedupVsHash = ((hashMs - distinctCachedMs) / hashMs) * 100.0
  
  distinctResults += DistinctStats(
    testName = config.name,
    rows = config.leftRows,
    keyType = config.keyType,
    distinctNoCacheMs = distinctNoCacheMs,
    distinctCachedMs = distinctCachedMs,
    regularHashMs = hashMs,
    distinctCheckOverheadMs = distinctCheckOverhead,
    speedupVsHash = speedupVsHash
  )
}

// ============================================================================
// PART 2: Run Non-Distinct Data Tests
// ============================================================================

println("\n" + "="*80)
println("PART 2: NON-DISTINCT DATA - Measuring distinctCount() Overhead")
println("="*80)
println()

case class OverheadStats(
  testName: String,
  rows: Long,
  keyType: String,
  checkNoCacheMs: Double,
  checkCachedMs: Double,
  noCheckMs: Double,
  checkOverheadPerIterMs: Double,  // Cost per iteration when not cached
  checkOverheadCachedMs: Double     // One-time cost when cached
)

val overheadResults = scala.collection.mutable.ArrayBuffer[OverheadStats]()

testCount = 0
nonDistinctTests.foreach { config =>
  testCount += 1
  println(s"[Part 2: $testCount/${nonDistinctTests.size}] Testing ${config.name}...")
  
  val results = runNonDistinctTest(config)
  results.foreach(printResultsTSV)
  
  val checkNoCacheMs = results(0).averageMs
  val checkCachedMs = results(1).averageMs
  val noCheckMs = results(2).averageMs
  
  // Per-iteration overhead when not cached
  val checkOverheadPerIter = checkNoCacheMs - noCheckMs
  
  // One-time overhead when cached (amortized over iterations)
  val checkOverheadCached = checkCachedMs - noCheckMs
  
  overheadResults += OverheadStats(
    testName = config.name,
    rows = config.leftRows,
    keyType = config.keyType,
    checkNoCacheMs = checkNoCacheMs,
    checkCachedMs = checkCachedMs,
    noCheckMs = noCheckMs,
    checkOverheadPerIterMs = checkOverheadPerIter,
    checkOverheadCachedMs = checkOverheadCached
  )
}

// ============================================================================
// Analysis and Summary
// ============================================================================

println("\n" + "="*80)
println("ANALYSIS: DISTINCT DATA BENEFITS")
println("="*80)
println()
println("TestName\tRows\tKeyType\tDistinct(NoCache)Ms\tDistinct(Cached)Ms\tRegularHashMs\tdistinctCount()Ms\tSpeedupVsHash%")
distinctResults.foreach { s =>
  println(f"${s.testName}\t${s.rows}\t${s.keyType}\t${s.distinctNoCacheMs}%.3f\t" +
    f"${s.distinctCachedMs}%.3f\t${s.regularHashMs}%.3f\t${s.distinctCheckOverheadMs}%.3f\t${s.speedupVsHash}%.2f")
}

println("\n" + "="*80)
println("ANALYSIS: distinctCount() OVERHEAD (Non-Distinct Data)")
println("="*80)
println()
println("TestName\tRows\tKeyType\tCheck(NoCache)Ms\tCheck(Cached)Ms\tNoCheckMs\tPerIterOverheadMs\tCachedOverheadMs")
overheadResults.foreach { s =>
  println(f"${s.testName}\t${s.rows}\t${s.keyType}\t${s.checkNoCacheMs}%.3f\t" +
    f"${s.checkCachedMs}%.3f\t${s.noCheckMs}%.3f\t${s.checkOverheadPerIterMs}%.3f\t${s.checkOverheadCachedMs}%.3f")
}

// Summary statistics
println("\n" + "="*80)
println("SUMMARY")
println("="*80)

// Distinct join benefits
val avgSpeedupHash = distinctResults.map(_.speedupVsHash).sum / distinctResults.size
println("\nDistinct Join Benefits (when data is distinct):")
println(f"  Average speedup vs HashJoin: ${avgSpeedupHash}%.2f%%")

val maxSpeedupHash = distinctResults.map(_.speedupVsHash).max
val minSpeedupHash = distinctResults.map(_.speedupVsHash).min
println(f"  Speedup range:                ${minSpeedupHash}%.2f%% to ${maxSpeedupHash}%.2f%%")

// distinctCount() overhead
val avgCheckOverhead = distinctResults.map(_.distinctCheckOverheadMs).sum / distinctResults.size
println("\ndistinctCount() Check Cost (per iteration):")
println(f"  Average cost: ${avgCheckOverhead}%.3f ms")

val avgOverheadPerIter = overheadResults.map(_.checkOverheadPerIterMs).sum / overheadResults.size
val avgOverheadCached = overheadResults.map(_.checkOverheadCachedMs).sum / overheadResults.size
println("\ndistinctCount() Overhead on Non-Distinct Data:")
println(f"  Per-iteration (no cache): ${avgOverheadPerIter}%.3f ms")
println(f"  One-time (cached):        ${avgOverheadCached}%.3f ms")

val maxOverhead = overheadResults.map(_.checkOverheadPerIterMs).max
val minOverhead = overheadResults.map(_.checkOverheadPerIterMs).min
println(f"  Overhead range:           ${minOverhead}%.3f ms to ${maxOverhead}%.3f ms")

// Scaling analysis by size and key type
println("\nScaling by Input Size (INT keys):")
val intSizeTests = distinctResults.filter(_.testName.startsWith("distinct_INT_")).sortBy(_.rows)
println("  Rows\t\tSpeedup vs Hash")
intSizeTests.foreach { s =>
  println(f"  ${s.rows}%,10d\t${s.speedupVsHash}%.2f%%")
}

println("\nScaling by Input Size (STRING keys):")
val stringSizeTests = distinctResults.filter(_.testName.startsWith("distinct_STRING_")).sortBy(_.rows)
println("  Rows\t\tSpeedup vs Hash")
stringSizeTests.foreach { s =>
  println(f"  ${s.rows}%,10d\t${s.speedupVsHash}%.2f%%")
}

println("\nComparison at 1M rows:")
val oneM = distinctResults.filter(_.rows == 1000000)
println("  KeyType\tSpeedup vs Hash")
oneM.foreach { s =>
  println(f"  ${s.keyType}\t\t${s.speedupVsHash}%.2f%%")
}

println("\n" + "="*80)
println("RECOMMENDATIONS")
println("="*80)
println()

if (avgSpeedupHash > 20.0) {
  println("✓ STRONG BENEFIT: DistinctHashJoin shows significant speedup (>20%)")
  println("  → Use useDistinctJoin=true when you know keys are distinct")
  println("  → Consider cacheDistinctFlag=true to avoid repeated distinctCount() calls")
} else if (avgSpeedupHash > 5.0) {
  println("≈ MODERATE BENEFIT: DistinctHashJoin shows moderate speedup (5-20%)")
  println("  → Use when performance is critical and keys are known distinct")
} else {
  println("⚠ MINIMAL BENEFIT: DistinctHashJoin shows small speedup (<5%)")
  println("  → May not be worth the distinctness check overhead")
}

if (avgCheckOverhead > 5.0) {
  println("\n✗ HIGH COST: distinctCount() check is expensive (>5ms per iteration)")
  println("  → ALWAYS use cacheDistinctFlag=true to avoid repeated checks")
  println("  → Only enable useDistinctJoin when confident keys are distinct")
} else if (avgCheckOverhead > 1.0) {
  println("\n≈ MODERATE COST: distinctCount() check has noticeable cost (1-5ms)")
  println("  → Use cacheDistinctFlag=true for multiple iterations")
  println("  → Consider trade-off vs benefit")
} else {
  println("\n✓ LOW COST: distinctCount() check is cheap (<1ms per iteration)")
  println("  → Caching less critical but still recommended")
  println("  → Safe to use even when distinctness is uncertain")
}

println("\nBenchmark complete!")

