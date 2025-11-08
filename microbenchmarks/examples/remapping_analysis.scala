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

// Key Remapping Optimization Analysis
// 
// This benchmark explores the remapComplexKeysToInts optimization which converts
// join keys to dense integers before joining. The optimization can significantly
// speed up sort-based joins by reducing comparison costs, but has overhead:
// 1. Building the dictionary (distinct key calculation)
// 2. Remapping keys before join
// 3. Memory for dictionary
//
// Key Questions:
// 1. Which data types benefit? (string/decimal help, but by how much?)
// 2. How does cardinality affect the tradeoff? (overhead vs benefit)
// 3. How does table size affect the tradeoff?
// 4. Does it help hash joins too, or only sort joins?
// 5. How much does caching the remapping structure help?
// 6. Multi-column keys: When does remapping help vs hurt?
// 7. String length: Does longer strings = more benefit?
// 8. Selectivity: Does join output size matter?
//
// Test Matrix (75 test configs, 225 benchmark runs total - 3 per config):
// - Key types: int, long, decimal(9,2), decimal(18,2), decimal(38,2), 
//              string (lengths 10, 50, 200), multi-column (2 keys)
//   NOTE: byte/short removed due to sort join gather map bug
// - Cardinalities: 1%, 5%, 10%, 50%, 100%
// - Table sizes: 1M, 5M, 10M rows
// - Join strategies: Hash, Sort (equal coverage)
// - Caching: With and without cacheRemapping
//
// Runtime: ~30-50 minutes

import com.nvidia.spark.rapids.benchmarks.JoinBenchmarkDataGen._
import com.nvidia.spark.rapids.benchmarks.JoinBenchmarkRunner._
import org.apache.spark.sql.tests.datagen._
import org.apache.spark.sql.SparkSession

val baseDir = "/data/tmp/remapping_benchmark"

case class RemapTestConfig(
  name: String,
  rows: Long,
  keyType: String,           // DDL string for key type (e.g., "int", "string")
  cardinalityPct: Double,    // Percentage of distinct keys
  joinStrategy: String,      // "hash" or "sort"
  stringLength: Int = 10,    // For string types
  numKeys: Int = 1           // Number of key columns (1=single, 2=composite)
)

// Helper to convert strategy string to enum
def strategyFromString(s: String): JoinStrategySpec = s match {
  case "hash" => HashObjectStrategy
  case "sort" => SortObjectWithPostStrategy
  case _ => throw new IllegalArgumentException(s"Unknown strategy: $s")
}

// ============================================================================
// Test Configurations
// ============================================================================

val testConfigs = Seq(
  // ========== Narrow Types - EQUAL HASH & SORT COVERAGE ==========
  // NOTE: Byte and Short tests removed due to sort join gather map bug
  
  // Int: 4 bytes -> 4 bytes = no size change
  RemapTestConfig("INT_1M_10pct_hash", 1000000, "int", 0.10, "hash"),
  RemapTestConfig("INT_1M_10pct_sort", 1000000, "int", 0.10, "sort"),
  RemapTestConfig("INT_5M_10pct_hash", 5000000, "int", 0.10, "hash"),
  RemapTestConfig("INT_5M_10pct_sort", 5000000, "int", 0.10, "sort"),
  
  // ========== Wide Types - EQUAL HASH & SORT COVERAGE ==========
  // Long: 8 bytes -> 4 bytes = 2x smaller
  RemapTestConfig("LONG_1M_10pct_hash", 1000000, "long", 0.10, "hash"),
  RemapTestConfig("LONG_1M_10pct_sort", 1000000, "long", 0.10, "sort"),
  RemapTestConfig("LONG_5M_10pct_hash", 5000000, "long", 0.10, "hash"),
  RemapTestConfig("LONG_5M_10pct_sort", 5000000, "long", 0.10, "sort"),
  RemapTestConfig("LONG_10M_10pct_hash", 10000000, "long", 0.10, "hash"),
  RemapTestConfig("LONG_10M_10pct_sort", 10000000, "long", 0.10, "sort"),
  
  // Decimal32: 4 bytes (like int)
  RemapTestConfig("DEC32_1M_10pct_hash", 1000000, "decimal(9,2)", 0.10, "hash"),
  RemapTestConfig("DEC32_1M_10pct_sort", 1000000, "decimal(9,2)", 0.10, "sort"),
  RemapTestConfig("DEC32_5M_10pct_hash", 5000000, "decimal(9,2)", 0.10, "hash"),
  RemapTestConfig("DEC32_5M_10pct_sort", 5000000, "decimal(9,2)", 0.10, "sort"),
  
  // Decimal64: 8 bytes (like long)
  RemapTestConfig("DEC64_1M_10pct_hash", 1000000, "decimal(18,2)", 0.10, "hash"),
  RemapTestConfig("DEC64_1M_10pct_sort", 1000000, "decimal(18,2)", 0.10, "sort"),
  RemapTestConfig("DEC64_5M_10pct_hash", 5000000, "decimal(18,2)", 0.10, "hash"),
  RemapTestConfig("DEC64_5M_10pct_sort", 5000000, "decimal(18,2)", 0.10, "sort"),
  
  // Decimal128: 16 bytes -> 4 bytes = 4x smaller
  RemapTestConfig("DEC128_1M_10pct_hash", 1000000, "decimal(38,2)", 0.10, "hash"),
  RemapTestConfig("DEC128_1M_10pct_sort", 1000000, "decimal(38,2)", 0.10, "sort"),
  RemapTestConfig("DEC128_5M_10pct_hash", 5000000, "decimal(38,2)", 0.10, "hash"),
  RemapTestConfig("DEC128_5M_10pct_sort", 5000000, "decimal(38,2)", 0.10, "sort"),
  RemapTestConfig("DEC128_10M_10pct_hash", 10000000, "decimal(38,2)", 0.10, "hash"),
  RemapTestConfig("DEC128_10M_10pct_sort", 10000000, "decimal(38,2)", 0.10, "sort"),
  
  // ========== Strings - EQUAL HASH & SORT COVERAGE ==========
  // Short strings (10 chars avg)
  RemapTestConfig("STR10_1M_10pct_hash", 1000000, "string", 0.10, "hash", stringLength = 10),
  RemapTestConfig("STR10_1M_10pct_sort", 1000000, "string", 0.10, "sort", stringLength = 10),
  RemapTestConfig("STR10_5M_10pct_hash", 5000000, "string", 0.10, "hash", stringLength = 10),
  RemapTestConfig("STR10_5M_10pct_sort", 5000000, "string", 0.10, "sort", stringLength = 10),
  RemapTestConfig("STR10_10M_10pct_hash", 10000000, "string", 0.10, "hash", stringLength = 10),
  RemapTestConfig("STR10_10M_10pct_sort", 10000000, "string", 0.10, "sort", stringLength = 10),
  
  // Medium strings (50 chars avg)
  RemapTestConfig("STR50_1M_10pct_hash", 1000000, "string", 0.10, "hash", stringLength = 50),
  RemapTestConfig("STR50_1M_10pct_sort", 1000000, "string", 0.10, "sort", stringLength = 50),
  RemapTestConfig("STR50_5M_10pct_hash", 5000000, "string", 0.10, "hash", stringLength = 50),
  RemapTestConfig("STR50_5M_10pct_sort", 5000000, "string", 0.10, "sort", stringLength = 50),
  RemapTestConfig("STR50_10M_10pct_hash", 10000000, "string", 0.10, "hash", stringLength = 50),
  RemapTestConfig("STR50_10M_10pct_sort", 10000000, "string", 0.10, "sort", stringLength = 50),
  
  // Long strings (200 chars avg)
  RemapTestConfig("STR200_1M_10pct_hash", 1000000, "string", 0.10, "hash", stringLength = 200),
  RemapTestConfig("STR200_1M_10pct_sort", 1000000, "string", 0.10, "sort", stringLength = 200),
  RemapTestConfig("STR200_5M_10pct_hash", 5000000, "string", 0.10, "hash", stringLength = 200),
  RemapTestConfig("STR200_5M_10pct_sort", 5000000, "string", 0.10, "sort", stringLength = 200),
  RemapTestConfig("STR200_10M_10pct_hash", 10000000, "string", 0.10, "hash", stringLength = 200),
  RemapTestConfig("STR200_10M_10pct_sort", 10000000, "string", 0.10, "sort", stringLength = 200),
  
  // ========== Cardinality Sweep - EQUAL HASH & SORT COVERAGE ==========
  // Very low cardinality (1%)
  RemapTestConfig("LONG_5M_1pct_hash", 5000000, "long", 0.01, "hash"),
  RemapTestConfig("LONG_5M_1pct_sort", 5000000, "long", 0.01, "sort"),
  RemapTestConfig("STR50_5M_1pct_hash", 5000000, "string", 0.01, "hash", stringLength = 50),
  RemapTestConfig("STR50_5M_1pct_sort", 5000000, "string", 0.01, "sort", stringLength = 50),
  
  // Low cardinality (5%)
  RemapTestConfig("LONG_5M_5pct_hash", 5000000, "long", 0.05, "hash"),
  RemapTestConfig("LONG_5M_5pct_sort", 5000000, "long", 0.05, "sort"),
  RemapTestConfig("STR50_5M_5pct_hash", 5000000, "string", 0.05, "hash", stringLength = 50),
  RemapTestConfig("STR50_5M_5pct_sort", 5000000, "string", 0.05, "sort", stringLength = 50),
  
  // Medium cardinality (50%)
  RemapTestConfig("LONG_5M_50pct_hash", 5000000, "long", 0.50, "hash"),
  RemapTestConfig("LONG_5M_50pct_sort", 5000000, "long", 0.50, "sort"),
  RemapTestConfig("STR50_5M_50pct_hash", 5000000, "string", 0.50, "hash", stringLength = 50),
  RemapTestConfig("STR50_5M_50pct_sort", 5000000, "string", 0.50, "sort", stringLength = 50),
  
  // High cardinality (100%)
  RemapTestConfig("LONG_5M_100pct_hash", 5000000, "long", 1.00, "hash"),
  RemapTestConfig("LONG_5M_100pct_sort", 5000000, "long", 1.00, "sort"),
  RemapTestConfig("STR50_5M_100pct_hash", 5000000, "string", 1.00, "hash", stringLength = 50),
  RemapTestConfig("STR50_5M_100pct_sort", 5000000, "string", 1.00, "sort", stringLength = 50),
  
  // ========== Multi-Key (Composite) Tests - EQUAL HASH & SORT COVERAGE ==========
  // 2-column INT keys
  RemapTestConfig("INT2_1M_10pct_hash", 1000000, "int", 0.10, "hash", numKeys = 2),
  RemapTestConfig("INT2_1M_10pct_sort", 1000000, "int", 0.10, "sort", numKeys = 2),
  RemapTestConfig("INT2_5M_10pct_hash", 5000000, "int", 0.10, "hash", numKeys = 2),
  RemapTestConfig("INT2_5M_10pct_sort", 5000000, "int", 0.10, "sort", numKeys = 2),
  
  // 2-column LONG keys
  RemapTestConfig("LONG2_1M_10pct_hash", 1000000, "long", 0.10, "hash", numKeys = 2),
  RemapTestConfig("LONG2_1M_10pct_sort", 1000000, "long", 0.10, "sort", numKeys = 2),
  RemapTestConfig("LONG2_5M_10pct_hash", 5000000, "long", 0.10, "hash", numKeys = 2),
  RemapTestConfig("LONG2_5M_10pct_sort", 5000000, "long", 0.10, "sort", numKeys = 2),
  
  // 2-column STRING keys (50 char)
  RemapTestConfig("STR2_1M_10pct_hash", 1000000, "string", 0.10, "hash", stringLength = 50, numKeys = 2),
  RemapTestConfig("STR2_1M_10pct_sort", 1000000, "string", 0.10, "sort", stringLength = 50, numKeys = 2),
  RemapTestConfig("STR2_5M_10pct_hash", 5000000, "string", 0.10, "hash", stringLength = 50, numKeys = 2),
  RemapTestConfig("STR2_5M_10pct_sort", 5000000, "string", 0.10, "sort", stringLength = 50, numKeys = 2),
  
  // 2-column DECIMAL128 keys
  RemapTestConfig("DEC128_2_1M_10pct_hash", 1000000, "decimal(38,2)", 0.10, "hash", numKeys = 2),
  RemapTestConfig("DEC128_2_1M_10pct_sort", 1000000, "decimal(38,2)", 0.10, "sort", numKeys = 2),
  RemapTestConfig("DEC128_2_5M_10pct_hash", 5000000, "decimal(38,2)", 0.10, "hash", numKeys = 2),
  RemapTestConfig("DEC128_2_5M_10pct_sort", 5000000, "decimal(38,2)", 0.10, "sort", numKeys = 2)
)

println("="*80)
println("Key Remapping Optimization Analysis")
println("="*80)
println(s"Total test configurations: ${testConfigs.size}")
println()

// ============================================================================
// Benchmark Functions (define before use)
// ============================================================================

// Warmup function
def warmup(config: RemapTestConfig): Unit = {
  // Build key indices based on number of keys (0-indexed)
  val keyIndices = (0 until config.numKeys).toSeq
  
  val warmupConfig = JoinBenchmarkConfig(
    testName = s"warmup_${config.name}",
    leftParquetPath = s"$baseDir/${config.name}/left",
    rightParquetPath = s"$baseDir/${config.name}/right",
    joinType = InnerJoin,
    joinStrategy = strategyFromString(config.joinStrategy),
    buildSide = LeftBuild,
    optimizations = JoinOptimizations(
      allowBuildSideSwap = false,
      remapComplexKeysToInts = false
    ),
    conditionalFilter = None,
    leftKeyIndices = keyIndices,
    rightKeyIndices = keyIndices,
    iterations = 1,
    numThreads = 1,
    printHeader = false
  )
  runBenchmark(warmupConfig, spark)
}

// Run test with and without remapping, with and without caching
def runRemapTest(config: RemapTestConfig, printHeader: Boolean, iterations: Int = 20): Seq[BenchmarkResults] = {
  warmup(config)
  
  // Build key indices based on number of keys (0-indexed)
  val keyIndices = (0 until config.numKeys).toSeq
  
  val baseConfig = JoinBenchmarkConfig(
    testName = config.name,
    leftParquetPath = s"$baseDir/${config.name}/left",
    rightParquetPath = s"$baseDir/${config.name}/right",
    joinType = InnerJoin,
    joinStrategy = strategyFromString(config.joinStrategy),
    buildSide = LeftBuild,
    optimizations = JoinOptimizations(allowBuildSideSwap = false),
    conditionalFilter = None,
    leftKeyIndices = keyIndices,
    rightKeyIndices = keyIndices,
    iterations = iterations,
    numThreads = 1,
    printHeader = printHeader
  )
  
  // Test 1: No remapping
  val noRemap = runBenchmark(
    baseConfig.copy(
      testName = s"${config.name}_no_remap",
      optimizations = baseConfig.optimizations.copy(
        remapComplexKeysToInts = false,
        cacheRemapping = false
      )
    ),
    spark
  )
  
  // Test 2: With remapping, no cache
  val withRemap = runBenchmark(
    baseConfig.copy(
      testName = s"${config.name}_with_remap",
      optimizations = baseConfig.optimizations.copy(
        remapComplexKeysToInts = true,
        cacheRemapping = false
      )
    ),
    spark
  )
  
  // Test 3: With remapping, with cache
  val withRemapCached = runBenchmark(
    baseConfig.copy(
      testName = s"${config.name}_with_remap_cached",
      optimizations = baseConfig.optimizations.copy(
        remapComplexKeysToInts = true,
        cacheRemapping = true
      )
    ),
    spark
  )
  
  Seq(noRemap, withRemap, withRemapCached)
}

// ============================================================================
// Data Generation
// ============================================================================

println("Generating test datasets...")
println()

testConfigs.foreach { config =>
  val distinctKeys = (config.rows * config.cardinalityPct).toLong
  
  println(f"  ${config.name}: ${config.rows}%,d rows, ${config.keyType}, " +
    f"${distinctKeys}%,d distinct (${config.cardinalityPct * 100}%.0f%%), ${config.joinStrategy}")
  
  // Create key columns based on type and number
  val keyColumns = (1 to config.numKeys).map { i =>
    KeyColumnSpec(s"key$i", config.keyType, minSeed = 0, maxSeed = distinctKeys - 1)
  }
  
  val leftConfig = TableGenConfig(
    numRows = config.rows,
    keyColumns = keyColumns,
    payloadColumns = Seq(
      PayloadColumnSpec("value", "double")
    ),
    outputPath = s"$baseDir/${config.name}/left"
  )
  
  val rightConfig = TableGenConfig(
    numRows = config.rows,
    keyColumns = keyColumns,
    payloadColumns = Seq(
      PayloadColumnSpec("value", "double")
    ),
    outputPath = s"$baseDir/${config.name}/right"
  )
  
  // Generate tables
  val dbgen = DBGen()
  val leftTable = dbgen.addTable("left", leftConfig.keyColumns.map(k => s"${k.name} ${k.dataType}").mkString(", ") + ", value double", config.rows)
  leftConfig.keyColumns.foreach { keySpec =>
    leftTable(keySpec.name).setSeedRange(keySpec.minSeed, keySpec.maxSeed)
    if (config.keyType == "string") {
      leftTable(keySpec.name).setLength(config.stringLength)
    }
  }
  
  val leftDf = leftTable.toDF(spark)
  leftDf.repartition(1).write.mode("overwrite").parquet(leftConfig.outputPath)
  
  val rightTable = dbgen.addTable("right", rightConfig.keyColumns.map(k => s"${k.name} ${k.dataType}").mkString(", ") + ", value double", config.rows)
  rightConfig.keyColumns.foreach { keySpec =>
    rightTable(keySpec.name).setSeedRange(keySpec.minSeed, keySpec.maxSeed)
    if (config.keyType == "string") {
      rightTable(keySpec.name).setLength(config.stringLength)
    }
  }
  
  val rightDf = rightTable.toDF(spark)
  rightDf.repartition(1).write.mode("overwrite").parquet(rightConfig.outputPath)
}

println("\nDataset generation complete!")
println("="*80)
println()

// ============================================================================
// Benchmark Execution
// ============================================================================

println("Running benchmarks...")
println()

val allResults = testConfigs.zipWithIndex.flatMap { case (config, idx) =>
  println(s"Testing: ${config.name}")
  val results = runRemapTest(config, printHeader = (idx == 0))
  results.foreach(printResultsTSV)
  results
}

case class RemapStats(
  testName: String,
  keyType: String,
  numKeys: Int,
  rows: Long,
  distinctKeys: Long,
  cardinalityPct: Double,
  joinStrategy: String,
  stringLength: Int,
  noRemapMs: Double,
  withRemapMs: Double,
  withRemapCachedMs: Double,
  remapSpeedupPct: Double,      // (noRemap - withRemap) / noRemap * 100
  cacheSpeedupPct: Double,       // (withRemap - cached) / withRemap * 100
  remapOverheadMs: Double,       // withRemap - noRemap (negative if faster)
  cacheOverheadMs: Double        // cached - noRemap
) {
  def remapHelps: Boolean = remapSpeedupPct > 0
  def cacheHelps: Boolean = cacheSpeedupPct > 0
  def keyDesc: String = if (numKeys == 1) keyType else s"${keyType}x${numKeys}"
}

val results = testConfigs.map { config =>
  val noRemap = allResults.find(_.testName == s"${config.name}_no_remap").get
  val withRemap = allResults.find(_.testName == s"${config.name}_with_remap").get
  val cached = allResults.find(_.testName == s"${config.name}_with_remap_cached").get
  
  val distinctKeys = (config.rows * config.cardinalityPct).toLong
  val remapSpeedup = ((noRemap.medianMs - withRemap.medianMs) / noRemap.medianMs) * 100.0
  val cacheSpeedup = ((withRemap.medianMs - cached.medianMs) / withRemap.medianMs) * 100.0
  
  RemapStats(
    testName = config.name,
    keyType = config.keyType,
    numKeys = config.numKeys,
    rows = config.rows,
    distinctKeys = distinctKeys,
    cardinalityPct = config.cardinalityPct,
    joinStrategy = config.joinStrategy,
    stringLength = config.stringLength,
    noRemapMs = noRemap.medianMs,
    withRemapMs = withRemap.medianMs,
    withRemapCachedMs = cached.medianMs,
    remapSpeedupPct = remapSpeedup,
    cacheSpeedupPct = cacheSpeedup,
    remapOverheadMs = withRemap.medianMs - noRemap.medianMs,
    cacheOverheadMs = cached.medianMs - noRemap.medianMs
  )
}

// ============================================================================
// Analysis
// ============================================================================

println("\n" + "="*80)
println("ANALYSIS BY KEY TYPE")
println("="*80)

val typeGroups = results.groupBy(r => (r.keyDesc, r.joinStrategy))

println("\nSort Joins:")
typeGroups.filter(_._1._2 == "sort").toSeq.sortBy(_._1._1).foreach { case ((keyDesc, _), stats) =>
  val avgSpeedup = stats.map(_.remapSpeedupPct).sum / stats.size
  val helpsCount = stats.count(_.remapHelps)
  val avgCacheSpeedup = stats.map(_.cacheSpeedupPct).sum / stats.size
  
  println(f"\n  $keyDesc:")
  println(f"    Remapping helps: $helpsCount/${stats.size} tests")
  println(f"    Avg speedup: ${avgSpeedup}%+.2f%%")
  println(f"    Avg cache benefit: ${avgCacheSpeedup}%+.2f%%")
  
  if (avgSpeedup > 5) {
    println(f"    ✓ RECOMMENDED: Remapping improves performance")
  } else if (avgSpeedup < -5) {
    println(f"    ✗ NOT RECOMMENDED: Remapping hurts performance")
  } else {
    println(f"    ~ MARGINAL: Performance impact is small")
  }
}

println("\nHash Joins:")
typeGroups.filter(_._1._2 == "hash").toSeq.sortBy(_._1._1).foreach { case ((keyDesc, _), stats) =>
  val avgSpeedup = stats.map(_.remapSpeedupPct).sum / stats.size
  val helpsCount = stats.count(_.remapHelps)
  
  println(f"\n  $keyDesc:")
  println(f"    Remapping helps: $helpsCount/${stats.size} tests")
  println(f"    Avg speedup: ${avgSpeedup}%+.2f%%")
  
  if (avgSpeedup > 5) {
    println(f"    ✓ Hash joins also benefit from remapping")
  } else if (avgSpeedup < -5) {
    println(f"    ✗ Remapping hurts hash join performance")
  } else {
    println(f"    ~ Minimal impact on hash joins")
  }
}

println("\n" + "="*80)
println("CARDINALITY IMPACT ANALYSIS")
println("="*80)

// Analyze how cardinality affects remapping benefit for long and string
val cardinalityTests = results.filter(r => 
  (r.keyType == "long" || (r.keyType == "string" && r.stringLength == 50)) && 
  r.joinStrategy == "sort" &&
  r.rows == 5000000
).sortBy(_.cardinalityPct)

if (cardinalityTests.nonEmpty) {
  println("\nRemapping speedup vs cardinality (5M rows, sort join):")
  println("  Cardinality\tDistinct\tLONG speedup%\tSTRING speedup%")
  
  cardinalityTests.groupBy(_.cardinalityPct).toSeq.sortBy(_._1).foreach { case (card, stats) =>
    val longSpeedup = stats.find(_.keyType == "long").map(_.remapSpeedupPct).getOrElse(0.0)
    val strSpeedup = stats.find(_.keyType == "string").map(_.remapSpeedupPct).getOrElse(0.0)
    val distinctKeys = stats.head.distinctKeys
    
    println(f"  ${card * 100}%3.0f%%\t\t${distinctKeys}%,8d\t${longSpeedup}%+.2f%%\t\t${strSpeedup}%+.2f%%")
  }
  
  println("\nKey Finding:")
  val lowCardBenefit = cardinalityTests.filter(_.cardinalityPct <= 0.05).map(_.remapSpeedupPct).sum / 
                       math.max(1, cardinalityTests.count(_.cardinalityPct <= 0.05))
  val highCardBenefit = cardinalityTests.filter(_.cardinalityPct >= 0.50).map(_.remapSpeedupPct).sum / 
                        math.max(1, cardinalityTests.count(_.cardinalityPct >= 0.50))
  
  if (math.abs(lowCardBenefit - highCardBenefit) > 10) {
    if (lowCardBenefit > highCardBenefit) {
      println(f"  Remapping benefits MORE at LOW cardinality (${lowCardBenefit}%.1f%% vs ${highCardBenefit}%.1f%%)")
      println(f"  → Dictionary overhead is small relative to comparison savings")
    } else {
      println(f"  Remapping benefits MORE at HIGH cardinality (${highCardBenefit}%.1f%% vs ${lowCardBenefit}%.1f%%)")
      println(f"  → More distinct keys = more comparison benefit despite larger dictionary")
    }
  } else {
    println(f"  Cardinality has MINIMAL impact on remapping benefit")
    println(f"  → Benefit is roughly constant across cardinality range")
  }
}

println("\n" + "="*80)
println("STRING LENGTH IMPACT ANALYSIS")
println("="*80)

val stringTests = results.filter(r => 
  r.keyType == "string" && 
  r.joinStrategy == "sort" &&
  r.cardinalityPct == 0.10
).groupBy(r => (r.rows, r.stringLength)).toSeq.sortBy(_._1)

if (stringTests.nonEmpty) {
  println("\nRemapping speedup vs string length (10% cardinality, sort join):")
  println("  Rows\t\tLen=10\t\tLen=50\t\tLen=200")
  
  val rowGroups = stringTests.groupBy(_._1._1).toSeq.sortBy(_._1)
  rowGroups.foreach { case (rows, tests) =>
    val len10 = tests.find(_._1._2 == 10).map(_._2.head.remapSpeedupPct).getOrElse(0.0)
    val len50 = tests.find(_._1._2 == 50).map(_._2.head.remapSpeedupPct).getOrElse(0.0)
    val len200 = tests.find(_._1._2 == 200).map(_._2.head.remapSpeedupPct).getOrElse(0.0)
    
    println(f"  ${rows}%,8d\t${len10}%+.2f%%\t\t${len50}%+.2f%%\t\t${len200}%+.2f%%")
  }
  
  println("\nKey Finding:")
  println("  Longer strings should show MORE benefit from remapping")
  println("  → String comparison cost is proportional to length")
  println("  → Integer comparison cost is constant")
}

println("\n" + "="*80)
println("SIZE SCALING ANALYSIS")
println("="*80)

// Check if benefit scales with table size
val sizeTests = results.filter(r => 
  (r.keyType == "long" || r.keyType == "decimal(38,2)") && 
  r.joinStrategy == "sort" &&
  r.cardinalityPct == 0.10
).groupBy(r => (r.keyType, r.rows)).toSeq.sortBy(_._1)

if (sizeTests.nonEmpty) {
  println("\nRemapping speedup vs table size (10% cardinality, sort join):")
  println("  KeyType\t\t1M rows\t\t5M rows\t\t10M rows")
  
  val typeGroups = sizeTests.groupBy(_._1._1).toSeq.sortBy(_._1)
  typeGroups.foreach { case (keyType, tests) =>
    val s1M = tests.find(_._1._2 == 1000000).map(_._2.head.remapSpeedupPct).getOrElse(0.0)
    val s5M = tests.find(_._1._2 == 5000000).map(_._2.head.remapSpeedupPct).getOrElse(0.0)
    val s10M = tests.find(_._1._2 == 10000000).map(_._2.head.remapSpeedupPct).getOrElse(0.0)
    
    println(f"  $keyType%-20s\t${s1M}%+.2f%%\t\t${s5M}%+.2f%%\t\t${s10M}%+.2f%%")
  }
  
  println("\nKey Finding:")
  println("  If speedup increases with size: Dictionary overhead becomes less significant")
  println("  If speedup decreases with size: Dictionary overhead dominates at scale")
  println("  If speedup is constant: Overhead and benefit scale proportionally")
}

println("\n" + "="*80)
println("CACHE EFFECTIVENESS ANALYSIS")
println("="*80)

val avgCacheBenefit = results.map(_.cacheSpeedupPct).sum / results.size
val cacheHelpsCount = results.count(_.cacheHelps)

println(f"\nAverage cache speedup: ${avgCacheBenefit}%+.2f%%")
println(f"Cache helps in: $cacheHelpsCount/${results.size} tests")

if (avgCacheBenefit > 5) {
  println("\n✓ RECOMMENDATION: Enable cacheRemapping when using remapComplexKeysToInts")
  println("  → Significant benefit from reusing remapping structure across iterations")
} else {
  println("\n~ Cache benefit is marginal")
  println("  → May not be worth the memory overhead in production")
}

// Find which types benefit most from caching
println("\nCache benefit by key type:")
results.groupBy(_.keyType).toSeq.sortBy(_._1).foreach { case (keyType, stats) =>
  val avgCache = stats.map(_.cacheSpeedupPct).sum / stats.size
  println(f"  $keyType%-20s: ${avgCache}%+.2f%%")
}

println("\n" + "="*80)
println("RECOMMENDATIONS")
println("="*80)
println()

println("Based on benchmark results:\n")

println("1. DATA TYPE RECOMMENDATIONS:")

val narrowTypes = Seq("byte", "short", "int", "decimal(9,2)")
val wideTypes = Seq("long", "decimal(18,2)", "decimal(38,2)", "string")

narrowTypes.foreach { t =>
  val typeStats = results.filter(_.keyType == t)
  if (typeStats.nonEmpty) {
    val avgSpeedup = typeStats.map(_.remapSpeedupPct).sum / typeStats.size
    if (avgSpeedup < -5) {
      println(f"   ✗ $t%-20s: Remapping HURTS (avg ${avgSpeedup}%.1f%% slower)")
    } else if (avgSpeedup > 5) {
      println(f"   ✓ $t%-20s: Remapping helps (avg ${avgSpeedup}%.1f%% faster)")
    } else {
      println(f"   ~ $t%-20s: Minimal impact (avg ${avgSpeedup}%.1f%%)")
    }
  }
}

wideTypes.foreach { t =>
  val typeStats = results.filter(_.keyType == t)
  if (typeStats.nonEmpty) {
    val avgSpeedup = typeStats.map(_.remapSpeedupPct).sum / typeStats.size
    if (avgSpeedup > 5) {
      println(f"   ✓ $t%-20s: Remapping helps (avg ${avgSpeedup}%.1f%% faster)")
    } else if (avgSpeedup < -5) {
      println(f"   ✗ $t%-20s: Remapping HURTS (avg ${avgSpeedup}%.1f%% slower)")
    } else {
      println(f"   ~ $t%-20s: Minimal impact (avg ${avgSpeedup}%.1f%%)")
    }
  }
}

println("\n2. JOIN STRATEGY:")
val sortCount = results.count(_.joinStrategy == "sort")
val sortBenefit = if (sortCount > 0) {
  results.filter(_.joinStrategy == "sort").map(_.remapSpeedupPct).sum / sortCount.toDouble
} else 0.0

val hashCount = results.count(_.joinStrategy == "hash")
val hashBenefit = if (hashCount > 0) {
  results.filter(_.joinStrategy == "hash").map(_.remapSpeedupPct).sum / hashCount.toDouble
} else 0.0

println(f"   Sort joins: avg ${sortBenefit}%+.1f%% speedup")
println(f"   Hash joins: avg ${hashBenefit}%+.1f%% speedup")

if (sortBenefit > hashBenefit + 5) {
  println("   → Remapping primarily benefits SORT joins (comparison cost reduction)")
} else if (hashBenefit > sortBenefit + 5) {
  println("   → Remapping also helps HASH joins (hash computation simplification)")
} else {
  println("   → Benefit is similar for both join strategies")
}

println("\n3. HEURISTIC SUGGESTION:")
println("   Enable remapComplexKeysToInts when:")

val shouldRemapTypes = results.filter(_.remapSpeedupPct > 5).map(_.keyType).distinct
if (shouldRemapTypes.nonEmpty) {
  println(s"   - Key type is: ${shouldRemapTypes.mkString(", ")}")
}

if (avgCacheBenefit > 5) {
  println("   - ALWAYS enable cacheRemapping for better performance")
}

println("\nBenchmark complete!")

