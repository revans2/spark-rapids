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

// Hash vs Sort Join Performance Analysis
// 
// This benchmark compares HashJoin vs SortMergeJoin performance to build a heuristic
// for deciding when to use each strategy. Key insight: it's not just about cardinality!
// 
// Tests across:
// 1. Different cardinalities (1% to 100% distinct keys)
//    - Hash joins excel with high cardinality (fewer duplicates)
//    - Sort joins excel with low cardinality (many duplicates)
//    - Fine-grained 1-10% sweep to pinpoint exact crossover point
// 
// 2. Different key distribution skew (NEW!)
//    - Uniform: All keys appear roughly same number of times
//    - Zipf (s=0.5, 1.0, 1.5): Progressively more skewed (some keys dominate)
//    - Tests if crossover point shifts with skew
//    - Theory: Hash joins suffer with skewed data (unbalanced buckets)
//             Sort joins handle skew better (duplicates are adjacent)
// 
// 3. Different key types
//    - Fixed-width types (INT, LONG) favor sort joins
//    - Variable-length types (STRING) favor hash joins
//    - Multi-column keys add complexity favors hash joins
// 
// 4. Different input sizes (1M to 25M rows)
//    - Shows how each strategy scales with data size
//
// 5. Asymmetric joins (build vs probe side analysis)
//    - Small build (1M) with large probe (10M)
//    - Large build (10M) with small probe (1M)
//    - Helps understand if cardinality matters more on build or probe side
// 
// Build side swapping is DISABLED to see true performance characteristics.
// Iterations reduced to 20 for faster runs while maintaining statistical confidence.
// 
// Runtime: ~60-120 minutes (comprehensive suite + skew tests + asymmetric joins)

import com.nvidia.spark.rapids.benchmarks.JoinBenchmarkDataGen._
import com.nvidia.spark.rapids.benchmarks.JoinBenchmarkRunner._
import org.apache.spark.sql.tests.datagen._
import org.apache.spark.sql.SparkSession

val baseDir = "/data/tmp/hash_vs_sort_benchmark"

case class TestConfig(
  name: String,
  leftRows: Long,         // Number of rows on left side
  rightRows: Long,        // Number of rows on right side (for asymmetric joins)
  keyType: String,
  numKeys: Int,           // Number of key columns
  cardinalityPct: Double, // Percentage of distinct keys (1.0 = 100%)
  skewFactor: Double = 0.0  // 0.0 = uniform, >0 = skewed (Zipf parameter)
) {
  def rows: Long = leftRows  // For backwards compatibility
  def isAsymmetric: Boolean = leftRows != rightRows
  def isSkewed: Boolean = skewFactor > 0.0
  def distributionName: String = if (skewFactor == 0.0) "uniform" else f"zipf_${skewFactor}%.1f"
}

// ============================================================================
// Skewed Distribution Helpers
// ============================================================================

/**
 * Custom LocationToSeedMapping that produces Zipf-distributed seeds.
 * 
 * Zipf distribution: P(k) ∝ 1/k^s where s is the skew parameter
 * - s=0.0: Uniform distribution (defers to FlatDistribution)
 * - s=0.5: Light skew
 * - s=1.0: Moderate skew (Zipf's law - like word frequencies)
 * - s=1.5: Heavy skew (power law)
 * 
 * Implementation: Uses MultiDistribution to approximate Zipf by dividing the
 * key space into buckets with decreasing weights.
 */
case class ZipfDistribution(
    skewFactor: Double = 1.0,
    numBuckets: Int = 20,
    colLocSeed: Long = 0,
    remapRangeFunc: Long => Long = n => n
) extends LocationToSeedMapping {

  override def withColumnConf(colConf: ColumnConf): ZipfDistribution = {
    val colLocSeed = colConf.columnLoc.hashLoc
    val remapFunc = LocationToSeedMapping.remapRangeFunc(colConf)
    
    if (skewFactor == 0.0) {
      // Uniform distribution - just use FlatDistribution behavior
      ZipfDistribution(skewFactor, numBuckets, colLocSeed, remapFunc)
    } else {
      // Build Zipf distribution using MultiDistribution
      val min = colConf.minSeed
      val max = colConf.maxSeed
      val actualBuckets = math.min(numBuckets, (max - min + 1).toInt)
      val bucketSize = (max - min + 1) / actualBuckets
      
      // Calculate Zipf weights: weight(k) ∝ 1/k^s
      val weightsAndMappings = (0 until actualBuckets).map { i =>
        val weight = 1.0 / math.pow(i + 1, skewFactor)
        val minKey = min + i * bucketSize
        val maxKey = if (i == actualBuckets - 1) max else min + (i + 1) * bucketSize - 1
        
        // Create a FlatDistribution for this bucket's range
        val bucketConf = ColumnConf(
          colConf.columnLoc,
          colConf.nullable,
          colConf.numTableRows,
          minKey,
          maxKey
        )
        (weight, FlatDistribution().withColumnConf(bucketConf))
      }
      
      val multiDist = MultiDistribution(weightsAndMappings).withColumnConf(colConf)
      // Return a ZipfDistribution that delegates to the MultiDistribution
      new ZipfDistribution(skewFactor, numBuckets, colLocSeed, remapFunc) {
        override def apply(rowLoc: RowLocation): Long = multiDist.apply(rowLoc)
      }
    }
  }

  override def apply(rowLoc: RowLocation): Long = {
    // Default implementation for uniform case
    remapRangeFunc(rowLoc.hashLoc(colLocSeed))
  }
}

// Generate skewed join tables using datagen with ZipfDistribution
def generateSkewedJoinTables(
  config: TestConfig,
  keyGroupId: Int,
  spark: SparkSession
): Unit = {
  val distinctKeys = (config.leftRows * config.cardinalityPct).toLong
  
  // Create ZipfDistribution for key columns
  val distribution = ZipfDistribution(skewFactor = config.skewFactor)
  
  // Use standard datagen with custom Zipf distribution
  val keyColumns = Seq(
    KeyColumnSpec("key1", config.keyType, minSeed = 0, maxSeed = distinctKeys - 1, distribution = distribution)
  )
  
  val leftConfig = TableGenConfig(
    numRows = config.leftRows,
    keyColumns = keyColumns,
    payloadColumns = Seq(
      PayloadColumnSpec("value", "double")
    ),
    outputPath = s"$baseDir/${config.name}/left"
  )
  
  val rightConfig = TableGenConfig(
    numRows = config.rightRows,
    keyColumns = keyColumns,
    payloadColumns = Seq(
      PayloadColumnSpec("value", "double")
    ),
    outputPath = s"$baseDir/${config.name}/right"
  )
  
  generateJoinTables(leftConfig, rightConfig, keyGroupId = keyGroupId, spark)
}

// ============================================================================
// Test Configurations
// ============================================================================

val testConfigs = Seq(
  // ========== Single INT Key ==========
  // Full cardinality sweep at 5M rows (primary key scenario)
  new TestConfig("INT_5M_1pct", 5000000, 5000000, "int", 1, 0.01),
  
  // Fine-grained sweep between 1-10% to identify exact crossover point
  new TestConfig("INT_5M_2pct", 5000000, 5000000, "int", 1, 0.02),
  new TestConfig("INT_5M_4pct", 5000000, 5000000, "int", 1, 0.04),
  new TestConfig("INT_5M_6pct", 5000000, 5000000, "int", 1, 0.06),
  new TestConfig("INT_5M_8pct", 5000000, 5000000, "int", 1, 0.08),
  new TestConfig("INT_5M_10pct", 5000000, 5000000, "int", 1, 0.10),
  
  new TestConfig("INT_5M_25pct", 5000000, 5000000, "int", 1, 0.25),
  new TestConfig("INT_5M_50pct", 5000000, 5000000, "int", 1, 0.50),
  new TestConfig("INT_5M_75pct", 5000000, 5000000, "int", 1, 0.75),
  new TestConfig("INT_5M_100pct", 5000000, 5000000, "int", 1, 1.00),
  
  // Size scaling at 100% distinct (primary key joins - common case)
  new TestConfig("INT_1M_100pct", 1000000, 1000000, "int", 1, 1.00),
  new TestConfig("INT_10M_100pct", 10000000, 10000000, "int", 1, 1.00),
  new TestConfig("INT_25M_100pct", 25000000, 25000000, "int", 1, 1.00),
  
  // Size scaling at 10% cardinality (high duplication - foreign key scenario)
  new TestConfig("INT_1M_10pct", 1000000, 1000000, "int", 1, 0.10),
  new TestConfig("INT_10M_10pct", 10000000, 10000000, "int", 1, 0.10),
  new TestConfig("INT_25M_10pct", 25000000, 25000000, "int", 1, 0.10),
  
  // ========== Single LONG Key ==========
  // Cardinality sweep at 5M rows
  new TestConfig("LONG_5M_1pct", 5000000, 5000000, "long", 1, 0.01),
  new TestConfig("LONG_5M_10pct", 5000000, 5000000, "long", 1, 0.10),
  new TestConfig("LONG_5M_25pct", 5000000, 5000000, "long", 1, 0.25),
  new TestConfig("LONG_5M_50pct", 5000000, 5000000, "long", 1, 0.50),
  new TestConfig("LONG_5M_75pct", 5000000, 5000000, "long", 1, 0.75),
  new TestConfig("LONG_5M_100pct", 5000000, 5000000, "long", 1, 1.00),
  
  // Size scaling at 100% distinct (primary key joins)
  new TestConfig("LONG_1M_100pct", 1000000, 1000000, "long", 1, 1.00),
  new TestConfig("LONG_10M_100pct", 10000000, 10000000, "long", 1, 1.00),
  new TestConfig("LONG_25M_100pct", 25000000, 25000000, "long", 1, 1.00),
  
  // Size scaling at 10% cardinality (foreign key joins)
  new TestConfig("LONG_1M_10pct", 1000000, 1000000, "long", 1, 0.10),
  new TestConfig("LONG_10M_10pct", 10000000, 10000000, "long", 1, 0.10),
  new TestConfig("LONG_25M_10pct", 25000000, 25000000, "long", 1, 0.10),
  
  // ========== Single STRING Key ==========
  // Cardinality sweep at 5M rows
  new TestConfig("STRING_5M_1pct", 5000000, 5000000, "string", 1, 0.01),
  new TestConfig("STRING_5M_10pct", 5000000, 5000000, "string", 1, 0.10),
  new TestConfig("STRING_5M_25pct", 5000000, 5000000, "string", 1, 0.25),
  new TestConfig("STRING_5M_50pct", 5000000, 5000000, "string", 1, 0.50),
  new TestConfig("STRING_5M_75pct", 5000000, 5000000, "string", 1, 0.75),
  new TestConfig("STRING_5M_100pct", 5000000, 5000000, "string", 1, 1.00),
  
  // Size scaling at 100% distinct (primary key joins)
  new TestConfig("STRING_1M_100pct", 1000000, 1000000, "string", 1, 1.00),
  new TestConfig("STRING_10M_100pct", 10000000, 10000000, "string", 1, 1.00),
  new TestConfig("STRING_25M_100pct", 25000000, 25000000, "string", 1, 1.00),
  
  // Size scaling at 10% cardinality (foreign key joins)
  new TestConfig("STRING_1M_10pct", 1000000, 1000000, "string", 1, 0.10),
  new TestConfig("STRING_10M_10pct", 10000000, 10000000, "string", 1, 0.10),
  new TestConfig("STRING_25M_10pct", 25000000, 25000000, "string", 1, 0.10),
  
  // ========== Multi-Column INT Keys ==========
  // Cardinality sweep at 5M rows
  new TestConfig("INT2_5M_1pct", 5000000, 5000000, "int", 2, 0.01),
  new TestConfig("INT2_5M_10pct", 5000000, 5000000, "int", 2, 0.10),
  new TestConfig("INT2_5M_25pct", 5000000, 5000000, "int", 2, 0.25),
  new TestConfig("INT2_5M_50pct", 5000000, 5000000, "int", 2, 0.50),
  new TestConfig("INT2_5M_75pct", 5000000, 5000000, "int", 2, 0.75),
  new TestConfig("INT2_5M_100pct", 5000000, 5000000, "int", 2, 1.00),
  
  // Size scaling at 10% cardinality (realistic for composite keys - NOT primary keys)
  new TestConfig("INT2_1M_10pct", 1000000, 1000000, "int", 2, 0.10),
  new TestConfig("INT2_10M_10pct", 10000000, 10000000, "int", 2, 0.10),
  new TestConfig("INT2_25M_10pct", 25000000, 25000000, "int", 2, 0.10),
  
  // Size scaling at 50% cardinality (medium duplication - also realistic)
  new TestConfig("INT2_1M_50pct", 1000000, 1000000, "int", 2, 0.50),
  new TestConfig("INT2_10M_50pct", 10000000, 10000000, "int", 2, 0.50),
  new TestConfig("INT2_25M_50pct", 25000000, 25000000, "int", 2, 0.50),
  
  // ========== Multi-Column STRING Keys ==========
  // Cardinality sweep at 5M rows
  new TestConfig("STRING2_5M_1pct", 5000000, 5000000, "string", 2, 0.01),
  new TestConfig("STRING2_5M_10pct", 5000000, 5000000, "string", 2, 0.10),
  new TestConfig("STRING2_5M_25pct", 5000000, 5000000, "string", 2, 0.25),
  new TestConfig("STRING2_5M_50pct", 5000000, 5000000, "string", 2, 0.50),
  new TestConfig("STRING2_5M_75pct", 5000000, 5000000, "string", 2, 0.75),
  new TestConfig("STRING2_5M_100pct", 5000000, 5000000, "string", 2, 1.00),
  
  // Size scaling at 10% cardinality (realistic for composite string keys)
  new TestConfig("STRING2_1M_10pct", 1000000, 1000000, "string", 2, 0.10),
  new TestConfig("STRING2_10M_10pct", 10000000, 10000000, "string", 2, 0.10),
  new TestConfig("STRING2_25M_10pct", 25000000, 25000000, "string", 2, 0.10),
  
  // Size scaling at 50% cardinality
  new TestConfig("STRING2_1M_50pct", 1000000, 1000000, "string", 2, 0.50),
  new TestConfig("STRING2_10M_50pct", 10000000, 10000000, "string", 2, 0.50),
  new TestConfig("STRING2_25M_50pct", 25000000, 25000000, "string", 2, 0.50),
  
  // ========== SKEWED DISTRIBUTION TESTS ==========
  // These tests explore how key distribution skew affects hash vs sort performance
  // at different cardinalities around the crossover point
  //
  // Zipf distribution: P(k) ∝ 1/k^s where s is the skew factor
  //   s=0.0: Uniform distribution (baseline)
  //   s=0.5: Light skew (common in some real-world data)
  //   s=1.0: Moderate skew (Zipf's law - word frequencies, city populations)
  //   s=1.5: Heavy skew (power law distributions)
  //   s=2.0: Extreme skew (rare in practice)
  //
  // At 2% cardinality (near crossover) with varying skew
  new TestConfig("SKEW_5M_2pct_uniform", 5000000, 5000000, "int", 1, 0.02, 0.0),
  new TestConfig("SKEW_5M_2pct_light", 5000000, 5000000, "int", 1, 0.02, 0.5),
  new TestConfig("SKEW_5M_2pct_moderate", 5000000, 5000000, "int", 1, 0.02, 1.0),
  new TestConfig("SKEW_5M_2pct_heavy", 5000000, 5000000, "int", 1, 0.02, 1.5),
  
  // At 4% cardinality with varying skew
  new TestConfig("SKEW_5M_4pct_uniform", 5000000, 5000000, "int", 1, 0.04, 0.0),
  new TestConfig("SKEW_5M_4pct_light", 5000000, 5000000, "int", 1, 0.04, 0.5),
  new TestConfig("SKEW_5M_4pct_moderate", 5000000, 5000000, "int", 1, 0.04, 1.0),
  new TestConfig("SKEW_5M_4pct_heavy", 5000000, 5000000, "int", 1, 0.04, 1.5),
  
  // At 6% cardinality with varying skew
  new TestConfig("SKEW_5M_6pct_uniform", 5000000, 5000000, "int", 1, 0.06, 0.0),
  new TestConfig("SKEW_5M_6pct_light", 5000000, 5000000, "int", 1, 0.06, 0.5),
  new TestConfig("SKEW_5M_6pct_moderate", 5000000, 5000000, "int", 1, 0.06, 1.0),
  new TestConfig("SKEW_5M_6pct_heavy", 5000000, 5000000, "int", 1, 0.06, 1.5),
  
  // At 8% cardinality with varying skew
  new TestConfig("SKEW_5M_8pct_uniform", 5000000, 5000000, "int", 1, 0.08, 0.0),
  new TestConfig("SKEW_5M_8pct_light", 5000000, 5000000, "int", 1, 0.08, 0.5),
  new TestConfig("SKEW_5M_8pct_moderate", 5000000, 5000000, "int", 1, 0.08, 1.0),
  new TestConfig("SKEW_5M_8pct_heavy", 5000000, 5000000, "int", 1, 0.08, 1.5),
  
  // At 10% cardinality with varying skew
  new TestConfig("SKEW_5M_10pct_uniform", 5000000, 5000000, "int", 1, 0.10, 0.0),
  new TestConfig("SKEW_5M_10pct_light", 5000000, 5000000, "int", 1, 0.10, 0.5),
  new TestConfig("SKEW_5M_10pct_moderate", 5000000, 5000000, "int", 1, 0.10, 1.0),
  new TestConfig("SKEW_5M_10pct_heavy", 5000000, 5000000, "int", 1, 0.10, 1.5),
  
  // ========== ASYMMETRIC JOINS - Build vs Probe Side Analysis ==========
  // These tests help understand if cardinality matters more on build or probe side
  // Format: name indicates left_right sizes and which side has low cardinality
  
  // Small build (1M), large probe (10M) - varying build side cardinality
  new TestConfig("ASYM_1M_10M_build2pct", 1000000, 10000000, "int", 1, 0.02),
  new TestConfig("ASYM_1M_10M_build4pct", 1000000, 10000000, "int", 1, 0.04),
  new TestConfig("ASYM_1M_10M_build6pct", 1000000, 10000000, "int", 1, 0.06),
  new TestConfig("ASYM_1M_10M_build8pct", 1000000, 10000000, "int", 1, 0.08),
  new TestConfig("ASYM_1M_10M_build10pct", 1000000, 10000000, "int", 1, 0.10),
  new TestConfig("ASYM_1M_10M_build50pct", 1000000, 10000000, "int", 1, 0.50),
  new TestConfig("ASYM_1M_10M_build100pct", 1000000, 10000000, "int", 1, 1.00),
  
  // Large build (10M), small probe (1M) - varying build side cardinality
  new TestConfig("ASYM_10M_1M_build2pct", 10000000, 1000000, "int", 1, 0.02),
  new TestConfig("ASYM_10M_1M_build4pct", 10000000, 1000000, "int", 1, 0.04),
  new TestConfig("ASYM_10M_1M_build6pct", 10000000, 1000000, "int", 1, 0.06),
  new TestConfig("ASYM_10M_1M_build8pct", 10000000, 1000000, "int", 1, 0.08),
  new TestConfig("ASYM_10M_1M_build10pct", 10000000, 1000000, "int", 1, 0.10),
  new TestConfig("ASYM_10M_1M_build50pct", 10000000, 1000000, "int", 1, 0.50),
  new TestConfig("ASYM_10M_1M_build100pct", 10000000, 1000000, "int", 1, 1.00)
)

println("="*80)
println("Hash vs Sort Join Performance Analysis")
println("="*80)
println(s"Total test configurations: ${testConfigs.size}")
println("Build side swapping is DISABLED for true performance comparison")
println()

// ============================================================================
// Data Generation
// ============================================================================

println("Generating test datasets...")
println("(This may take several minutes for large datasets)")
println()

testConfigs.foreach { config =>
  // For asymmetric joins, cardinality is based on the left (build) side
  val distinctKeys = (config.leftRows * config.cardinalityPct).toLong
  
  val distLabel = if (config.isSkewed) f" ${config.distributionName}" else ""
  if (config.isAsymmetric) {
    println(f"  ${config.name}: L=${config.leftRows}%,d R=${config.rightRows}%,d rows, ${config.keyType}, " +
      f"${config.numKeys} key(s), ${distinctKeys}%,d distinct (${config.cardinalityPct * 100}%.0f%% of build)$distLabel")
  } else {
    println(f"  ${config.name}: ${config.rows}%,d rows, ${config.keyType}, " +
      f"${config.numKeys} key(s), ${distinctKeys}%,d distinct (${config.cardinalityPct * 100}%.0f%%)$distLabel")
  }
  
  // Use skewed generator for skewed tests, uniform generator otherwise
  if (config.isSkewed && config.keyType == "int" && config.numKeys == 1) {
    // Skewed distribution using Zipf
    spark.time(generateSkewedJoinTables(config, keyGroupId = 1, spark))
  } else {
    // Uniform distribution using standard datagen
    val keyColumns = (1 to config.numKeys).map { i =>
      KeyColumnSpec(s"key$i", config.keyType, minSeed = 0, maxSeed = distinctKeys - 1)
    }
    
    val leftConfig = TableGenConfig(
      numRows = config.leftRows,
      keyColumns = keyColumns,
      payloadColumns = Seq(
        PayloadColumnSpec("value", "double")
      ),
      outputPath = s"$baseDir/${config.name}/left"
    )
    
    val rightConfig = TableGenConfig(
      numRows = config.rightRows,
      keyColumns = keyColumns,
      payloadColumns = Seq(
        PayloadColumnSpec("value", "double")
      ),
      outputPath = s"$baseDir/${config.name}/right"
    )
    
    spark.time(generateJoinTables(leftConfig, rightConfig, keyGroupId = 1, spark))
  }
}

println("\nDataset generation complete!")
println("="*80)
println()

// ============================================================================
// Benchmark Functions
// ============================================================================

// Warmup function
def warmup(config: TestConfig): Unit = {
  val keyIndices = (1 to config.numKeys).map(i => i - 1).toSeq
  
  val warmupConfig = JoinBenchmarkConfig(
    testName = "warmup",
    leftParquetPath = s"$baseDir/${config.name}/left",
    rightParquetPath = s"$baseDir/${config.name}/right",
    joinType = InnerJoin,
    joinStrategy = HashObjectStrategy,
    buildSide = LeftBuild,  // Fixed build side, no swapping
    optimizations = JoinOptimizations(
      allowBuildSideSwap = false,  // CRITICAL: Disable swapping
      cacheJoinObject = false
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

// Run comparison test
def runComparisonTest(config: TestConfig, iterations: Int = 20): Seq[BenchmarkResults] = {
  warmup(config)
  
  val keyIndices = (1 to config.numKeys).map(i => i - 1).toSeq
  
  val baseConfig = JoinBenchmarkConfig(
    testName = "",
    leftParquetPath = s"$baseDir/${config.name}/left",
    rightParquetPath = s"$baseDir/${config.name}/right",
    joinType = InnerJoin,
    joinStrategy = HashObjectStrategy,
    buildSide = LeftBuild,  // Fixed build side, no swapping
    optimizations = JoinOptimizations(
      allowBuildSideSwap = false,  // CRITICAL: Disable swapping
      cacheJoinObject = false
    ),
    conditionalFilter = None,
    leftKeyIndices = keyIndices,
    rightKeyIndices = keyIndices,
    iterations = iterations,
    numThreads = 1,
    printHeader = false
  )
  
  // Test 1: HashJoin
  val hashConfig = baseConfig.copy(
    testName = s"${config.name}_Hash",
    joinStrategy = HashObjectStrategy
  )
  val hashResult = runBenchmark(hashConfig, spark)
  
  // Test 2: SortMergeJoin
  val sortConfig = baseConfig.copy(
    testName = s"${config.name}_Sort",
    joinStrategy = SortObjectWithPostStrategy
  )
  val sortResult = runBenchmark(sortConfig, spark)
  
  Seq(hashResult, sortResult)
}

// ============================================================================
// Run Benchmarks
// ============================================================================

case class ComparisonStats(
  testName: String,
  rows: Long,
  keyType: String,
  numKeys: Int,
  cardinalityPct: Double,
  distinctKeys: Long,
  hashMs: Double,
  sortMs: Double,
  hashFaster: Boolean,
  speedupPct: Double  // Positive = hash faster, negative = sort faster
)

val results = scala.collection.mutable.ArrayBuffer[ComparisonStats]()

printTSVHeader()

var testCount = 0
testConfigs.foreach { config =>
  testCount += 1
  val distinctKeys = (config.rows * config.cardinalityPct).toLong
  println(s"[$testCount/${testConfigs.size}] Testing ${config.name} " +
    f"(${config.rows}%,d rows, ${config.cardinalityPct * 100}%.0f%% cardinality)...")
  
  val testResults = runComparisonTest(config)
  testResults.foreach(printResultsTSV)
  
  val hashMs = testResults(0).averageMs
  val sortMs = testResults(1).averageMs
  
  val hashFaster = hashMs < sortMs
  val speedupPct = if (hashFaster) {
    ((sortMs - hashMs) / sortMs) * 100.0
  } else {
    -((hashMs - sortMs) / hashMs) * 100.0
  }
  
  results += ComparisonStats(
    testName = config.name,
    rows = config.rows,
    keyType = config.keyType,
    numKeys = config.numKeys,
    cardinalityPct = config.cardinalityPct,
    distinctKeys = distinctKeys,
    hashMs = hashMs,
    sortMs = sortMs,
    hashFaster = hashFaster,
    speedupPct = speedupPct
  )
}

// ============================================================================
// Analysis and Summary
// ============================================================================

println("\n" + "="*80)
println("ANALYSIS: Hash vs Sort Performance")
println("="*80)
println()
println("TestName\tRows\tKeyType\tNumKeys\tCardinality%\tDistinctKeys\tHashMs\tSortMs\tFaster\tSpeedup%")
results.foreach { s =>
  val faster = if (s.hashFaster) "Hash" else "Sort"
  println(f"${s.testName}\t${s.rows}\t${s.keyType}\t${s.numKeys}\t${s.cardinalityPct * 100}%.0f\t" +
    f"${s.distinctKeys}\t${s.hashMs}%.3f\t${s.sortMs}%.3f\t$faster\t${s.speedupPct}%.2f")
}

println("\n" + "="*80)
println("SUMMARY BY KEY TYPE AND CARDINALITY")
println("="*80)

// Group by key configuration (excluding asymmetric joins)
val keyTypes = Seq(
  ("INT, 1 key", "int", 1),
  ("LONG, 1 key", "long", 1),
  ("STRING, 1 key", "string", 1),
  ("INT, 2 keys", "int", 2),
  ("STRING, 2 keys", "string", 2)
)

keyTypes.foreach { case (label, keyType, numKeys) =>
  val filtered = results.filter(r => 
    r.keyType == keyType && 
    r.numKeys == numKeys &&
    !r.testName.startsWith("ASYM_")
  )
  
  if (filtered.nonEmpty) {
    println(s"\n$label:")
    println("  Cardinality\tRows\tHashMs\tSortMs\tFaster\tSpeedup%")
    
    filtered.sortBy(r => (r.cardinalityPct, r.rows)).foreach { s =>
      val faster = if (s.hashFaster) "Hash" else "Sort"
      println(f"  ${s.cardinalityPct * 100}%3.0f%%\t\t${s.rows}%,10d\t${s.hashMs}%.2f\t${s.sortMs}%.2f\t$faster\t${s.speedupPct}%.2f")
    }
  }
}

println("\n" + "="*80)
println("CROSSOVER ANALYSIS")
println("="*80)

// Find cardinality crossover points for each key type
keyTypes.foreach { case (label, keyType, numKeys) =>
  val filtered = results.filter(r => 
    r.keyType == keyType && 
    r.numKeys == numKeys &&
    r.testName.contains("5M")  // Use 5M size tests for crossover
  ).sortBy(_.cardinalityPct)
  
  if (filtered.size >= 2) {
    println(s"\n$label (at 5M rows):")
    
    // Find where strategy preference changes
    val hashWins = filtered.filter(_.hashFaster)
    val sortWins = filtered.filter(!_.hashFaster)
    
    if (hashWins.nonEmpty && sortWins.nonEmpty) {
      val lowestHashWin = hashWins.minBy(_.cardinalityPct)
      val highestSortWin = sortWins.maxBy(_.cardinalityPct)
      
      if (lowestHashWin.cardinalityPct > highestSortWin.cardinalityPct) {
        println(f"  Sort wins: 0%% - ${highestSortWin.cardinalityPct * 100}%.0f%% cardinality")
        println(f"  Hash wins: ${lowestHashWin.cardinalityPct * 100}%.0f%% - 100%% cardinality")
        println(f"  Crossover: ~${(lowestHashWin.cardinalityPct + highestSortWin.cardinalityPct) / 2 * 100}%.0f%% cardinality")
      } else if (hashWins.size > sortWins.size) {
        println(f"  Hash consistently faster across all cardinalities")
      } else {
        println(f"  Sort consistently faster across all cardinalities")
      }
    } else if (hashWins.isEmpty) {
      println("  Sort wins across all cardinalities tested")
    } else {
      println("  Hash wins across all cardinalities tested")
    }
    
    // Show best speedups
    val bestHash = filtered.filter(_.hashFaster).sortBy(-_.speedupPct).headOption
    val bestSort = filtered.filter(!_.hashFaster).sortBy(_.speedupPct).headOption
    
    bestHash.foreach { s =>
      println(f"  Best hash speedup: ${s.speedupPct}%.2f%% at ${s.cardinalityPct * 100}%.0f%% cardinality")
    }
    bestSort.foreach { s =>
      println(f"  Best sort speedup: ${-s.speedupPct}%.2f%% at ${s.cardinalityPct * 100}%.0f%% cardinality")
    }
  }
}

println("\n" + "="*80)
println("FINE-GRAINED CARDINALITY ANALYSIS (1-10% range)")
println("="*80)
println("\nDetailed analysis to pinpoint exact crossover between sort and hash:")

// Analyze INT single key with fine granularity
val fineGrained = results.filter(r => 
  r.keyType == "int" && 
  r.numKeys == 1 &&
  r.testName.contains("5M") &&
  r.cardinalityPct <= 0.10
).sortBy(_.cardinalityPct)

if (fineGrained.nonEmpty) {
  println("\nINT single key at 5M rows (1-10% cardinality):")
  println("  Cardinality\tDistinct\tHashMs\tSortMs\tFaster\tSpeedup%\tMargin")
  
  fineGrained.foreach { s =>
    val faster = if (s.hashFaster) "Hash" else "Sort"
    val marginMs = math.abs(s.hashMs - s.sortMs)
    println(f"  ${s.cardinalityPct * 100}%3.1f%%\t\t${s.distinctKeys}%,8d\t${s.hashMs}%.2f\t${s.sortMs}%.2f\t$faster\t${s.speedupPct}%+.2f%%\t${marginMs}%.2fms")
  }
  
  // Find the crossover point
  val sortWins = fineGrained.filter(!_.hashFaster)
  val hashWins = fineGrained.filter(_.hashFaster)
  
  if (sortWins.nonEmpty && hashWins.nonEmpty) {
    val lastSortWin = sortWins.maxBy(_.cardinalityPct)
    val firstHashWin = hashWins.minBy(_.cardinalityPct)
    
    if (firstHashWin.cardinalityPct > lastSortWin.cardinalityPct) {
      println(f"\n  *** CROSSOVER RANGE: ${lastSortWin.cardinalityPct * 100}%.1f%% - ${firstHashWin.cardinalityPct * 100}%.1f%% ***")
      println(f"      Sort faster up to ~${lastSortWin.cardinalityPct * 100}%.1f%% cardinality")
      println(f"      Hash faster from ~${firstHashWin.cardinalityPct * 100}%.1f%% cardinality")
    } else {
      println("\n  Strategies are competitive in this range - no clear crossover")
    }
  } else if (sortWins.isEmpty) {
    println("\n  Hash wins across entire 1-10% range")
  } else {
    println("\n  Sort wins across entire 1-10% range")
  }
}

println("\n" + "="*80)
println("ASYMMETRIC JOIN ANALYSIS (Build vs Probe Side)")
println("="*80)
println("\nUnderstanding if cardinality matters more on build or probe side:")

val asymResults = results.filter(r => r.testName.startsWith("ASYM_"))
  .sortBy(r => (r.testName.contains("10M_1M"), r.cardinalityPct))

if (asymResults.nonEmpty) {
  // Small build, large probe
  val smallBuild = asymResults.filter(_.testName.contains("1M_10M"))
  if (smallBuild.nonEmpty) {
    println("\nSmall build (1M), Large probe (10M):")
    println("  Cardinality\tHashMs\tSortMs\tFaster\tSpeedup%")
    smallBuild.foreach { s =>
      val faster = if (s.hashFaster) "Hash" else "Sort"
      println(f"  ${s.cardinalityPct * 100}%3.0f%%\t\t${s.hashMs}%.2f\t${s.sortMs}%.2f\t$faster\t${s.speedupPct}%.2f")
    }
    
    // Find crossover for small build
    val smallBuildSortWins = smallBuild.filter(!_.hashFaster)
    val smallBuildHashWins = smallBuild.filter(_.hashFaster)
    if (smallBuildSortWins.nonEmpty && smallBuildHashWins.nonEmpty) {
      val lastSortWin = smallBuildSortWins.maxBy(_.cardinalityPct)
      val firstHashWin = smallBuildHashWins.minBy(_.cardinalityPct)
      if (firstHashWin.cardinalityPct > lastSortWin.cardinalityPct) {
        println(f"  Crossover: ${lastSortWin.cardinalityPct * 100}%.0f%% - ${firstHashWin.cardinalityPct * 100}%.0f%%")
      }
    }
  }
  
  // Large build, small probe
  val largeBuild = asymResults.filter(_.testName.contains("10M_1M"))
  if (largeBuild.nonEmpty) {
    println("\nLarge build (10M), Small probe (1M):")
    println("  Cardinality\tHashMs\tSortMs\tFaster\tSpeedup%")
    largeBuild.foreach { s =>
      val faster = if (s.hashFaster) "Hash" else "Sort"
      println(f"  ${s.cardinalityPct * 100}%3.0f%%\t\t${s.hashMs}%.2f\t${s.sortMs}%.2f\t$faster\t${s.speedupPct}%.2f")
    }
    
    // Find crossover for large build
    val largeBuildSortWins = largeBuild.filter(!_.hashFaster)
    val largeBuildHashWins = largeBuild.filter(_.hashFaster)
    if (largeBuildSortWins.nonEmpty && largeBuildHashWins.nonEmpty) {
      val lastSortWin = largeBuildSortWins.maxBy(_.cardinalityPct)
      val firstHashWin = largeBuildHashWins.minBy(_.cardinalityPct)
      if (firstHashWin.cardinalityPct > lastSortWin.cardinalityPct) {
        println(f"  Crossover: ${lastSortWin.cardinalityPct * 100}%.0f%% - ${firstHashWin.cardinalityPct * 100}%.0f%%")
      }
    }
  }
  
  // Compare crossovers
  if (smallBuild.nonEmpty && largeBuild.nonEmpty) {
    println("\nKey Insight:")
    println("  - If crossover points are similar: Build size doesn't significantly affect strategy choice")
    println("  - If crossover differs: Build size matters for choosing hash vs sort")
  }
}

println("\n" + "="*80)
println("SKEWED DISTRIBUTION ANALYSIS")
println("="*80)
println("\nHow does key distribution skew affect the hash vs sort decision?")

val skewedResults = results.filter(r => r.testName.startsWith("SKEW_"))

if (skewedResults.nonEmpty) {
  // Group by cardinality and show how skew affects performance
  val cardinalityLevels = Seq(0.02, 0.04, 0.06, 0.08, 0.10)
  
  cardinalityLevels.foreach { cardPct =>
    val cardResults = skewedResults.filter(_.cardinalityPct == cardPct).sortBy(_.testName)
    
    if (cardResults.nonEmpty) {
      println(f"\nAt ${cardPct * 100}%.0f%% Cardinality:")
      println("  Skew\t\tHashMs\tSortMs\tFaster\tSpeedup%\tHash Advantage")
      
      var uniformHashTime = 0.0
      var uniformSortTime = 0.0
      
      cardResults.foreach { s =>
        val faster = if (s.hashFaster) "Hash" else "Sort"
        val skewLabel = if (s.testName.contains("uniform")) {
          uniformHashTime = s.hashMs
          uniformSortTime = s.sortMs
          "Uniform (0.0)"
        } else if (s.testName.contains("light")) "Light (0.5)"
        else if (s.testName.contains("moderate")) "Moderate (1.0)"
        else if (s.testName.contains("heavy")) "Heavy (1.5)"
        else "Unknown"
        
        // Calculate how much hash degrades/improves vs uniform
        val hashAdvantage = if (uniformHashTime > 0 && uniformSortTime > 0) {
          val uniformGap = uniformSortTime - uniformHashTime
          val currentGap = s.sortMs - s.hashMs
          currentGap - uniformGap
        } else 0.0
        
        println(f"  $skewLabel%-15s\t${s.hashMs}%.2f\t${s.sortMs}%.2f\t$faster\t${s.speedupPct}%+.2f%%\t${hashAdvantage}%+.2fms")
      }
      
      // Analyze if crossover point changes
      val uniformWinner = cardResults.find(_.testName.contains("uniform")).map(_.hashFaster)
      val heavyWinner = cardResults.find(_.testName.contains("heavy")).map(_.hashFaster)
      
      if (uniformWinner.isDefined && heavyWinner.isDefined) {
        if (uniformWinner.get != heavyWinner.get) {
          println(f"\n  *** CROSSOVER SHIFT: Strategy preference changed from uniform to heavy skew! ***")
          if (uniformWinner.get) {
            println(f"      Hash wins with uniform distribution, but sort wins with heavy skew")
          } else {
            println(f"      Sort wins with uniform distribution, but hash wins with heavy skew")
          }
        }
      }
    }
  }
  
  // Summary: how does skew affect the overall decision?
  println("\n" + "="*80)
  println("SKEW IMPACT SUMMARY")
  println("="*80)
  println()
  
  // Count strategy changes due to skew
  var crossoverShifts = 0
  cardinalityLevels.foreach { cardPct =>
    val cardResults = skewedResults.filter(_.cardinalityPct == cardPct)
    val uniformWinner = cardResults.find(_.testName.contains("uniform")).map(_.hashFaster)
    val heavyWinner = cardResults.find(_.testName.contains("heavy")).map(_.hashFaster)
    
    if (uniformWinner.isDefined && heavyWinner.isDefined && uniformWinner.get != heavyWinner.get) {
      crossoverShifts += 1
    }
  }
  
  if (crossoverShifts > 0) {
    println(f"Strategy preference changed due to skew in $crossoverShifts/${cardinalityLevels.size} cardinality levels tested")
    println("\nKEY FINDING: Distribution skew significantly affects hash vs sort decision!")
    println("A heuristic should consider BOTH:")
    println("  1. Cardinality (distinct_keys / total_rows)")
    println("  2. Skew metric (e.g., top-10% concentration, Gini coefficient, max key frequency)")
  } else {
    println("Strategy preference remained consistent across skew levels")
    println("\nKEY FINDING: Cardinality dominates the decision; skew has limited impact")
    println("A simple cardinality threshold may be sufficient")
  }
  
  println("\nSuggested heuristic approach:")
  println("  1. Calculate cardinality = distinct_keys / total_rows")
  
  if (crossoverShifts > 0) {
    println("  2. Calculate skew metric:")
    println("     - Sample top-K keys and measure their frequency")
    println("     - If top-10% of keys account for >X% of rows, adjust threshold")
    println("  3. Decision:")
    println("     - Use SORT if: (cardinality < base_threshold) OR (skew_high AND cardinality < adjusted_threshold)")
    println("     - Use HASH otherwise")
  } else {
    println("  2. Decision:")
    println("     - Use SORT if: cardinality < threshold (~4-6% for INT keys)")
    println("     - Use HASH otherwise")
  }
} else {
  println("\nNo skewed distribution tests found")
}

println("\n" + "="*80)
println("CARDINALITY ANALYSIS (at 5M rows)")
println("="*80)

// Compare all key types at 100% cardinality (all distinct keys)
println("\nPerformance at 100% Cardinality (all distinct - primary key scenario):")
val results100pct = results.filter(r => r.rows == 5000000 && r.cardinalityPct == 1.0)
  .sortBy(r => (r.numKeys, r.keyType))

if (results100pct.nonEmpty) {
  println("\n  Key Type\t\tHashMs\tSortMs\tFaster\tSpeedup%")
  keyTypes.foreach { case (label, keyType, numKeys) =>
    results100pct.find(r => r.keyType == keyType && r.numKeys == numKeys).foreach { s =>
      val faster = if (s.hashFaster) "Hash" else "Sort"
      println(f"  ${label}%-20s\t${s.hashMs}%.2f\t${s.sortMs}%.2f\t$faster\t${s.speedupPct}%.2f")
    }
  }
  
  val hashWins100 = results100pct.count(_.hashFaster)
  println(f"\n  Summary: Hash wins ${hashWins100}/${results100pct.size} tests at 100%% cardinality")
}

// Compare all key types at 50% cardinality (medium duplication)
println("\nPerformance at 50% Cardinality (medium duplication):")
val results50pct = results.filter(r => r.rows == 5000000 && r.cardinalityPct == 0.50)
  .sortBy(r => (r.numKeys, r.keyType))

if (results50pct.nonEmpty) {
  println("\n  Key Type\t\tHashMs\tSortMs\tFaster\tSpeedup%")
  keyTypes.foreach { case (label, keyType, numKeys) =>
    results50pct.find(r => r.keyType == keyType && r.numKeys == numKeys).foreach { s =>
      val faster = if (s.hashFaster) "Hash" else "Sort"
      println(f"  ${label}%-20s\t${s.hashMs}%.2f\t${s.sortMs}%.2f\t$faster\t${s.speedupPct}%.2f")
    }
  }
  
  val hashWins50 = results50pct.count(_.hashFaster)
  println(f"\n  Summary: Hash wins ${hashWins50}/${results50pct.size} tests at 50%% cardinality")
}

println("\n" + "="*80)
println("SIZE SCALING ANALYSIS")
println("="*80)

// Analyze scaling with size at 100% cardinality (single keys only)
println("\nSize Scaling at 100% Cardinality (single keys - primary key scenario):")
val singleKeyTypes = Seq(
  ("INT", "int", 1),
  ("LONG", "long", 1),
  ("STRING", "string", 1)
)

singleKeyTypes.foreach { case (label, keyType, numKeys) =>
  val filtered = results.filter(r => 
    r.keyType == keyType && 
    r.numKeys == numKeys &&
    r.cardinalityPct == 1.0
  ).sortBy(_.rows)
  
  if (filtered.size >= 2) {
    println(s"\n  $label:")
    println("    Rows\t\tHashMs\tSortMs\tFaster\tSpeedup%")
    filtered.foreach { s =>
      val faster = if (s.hashFaster) "Hash" else "Sort"
      println(f"    ${s.rows}%,10d\t${s.hashMs}%.2f\t${s.sortMs}%.2f\t$faster\t${s.speedupPct}%.2f")
    }
  }
}

// Analyze scaling with size at 10% cardinality (all key types)
println("\nSize Scaling at 10% Cardinality (high duplication - foreign key scenario):")
keyTypes.foreach { case (label, keyType, numKeys) =>
  val filtered = results.filter(r => 
    r.keyType == keyType && 
    r.numKeys == numKeys &&
    r.cardinalityPct == 0.10
  ).sortBy(_.rows)
  
  if (filtered.size >= 2) {
    println(s"\n  $label:")
    println("    Rows\t\tHashMs\tSortMs\tFaster\tSpeedup%")
    filtered.foreach { s =>
      val faster = if (s.hashFaster) "Hash" else "Sort"
      println(f"    ${s.rows}%,10d\t${s.hashMs}%.2f\t${s.sortMs}%.2f\t$faster\t${s.speedupPct}%.2f")
    }
  }
}

// Analyze scaling at 50% cardinality for multi-column keys (more realistic scenario)
println("\nSize Scaling at 50% Cardinality (multi-column keys - medium duplication):")
val multiKeyTypes = Seq(
  ("INT, 2 keys", "int", 2),
  ("STRING, 2 keys", "string", 2)
)

multiKeyTypes.foreach { case (label, keyType, numKeys) =>
  val filtered = results.filter(r => 
    r.keyType == keyType && 
    r.numKeys == numKeys &&
    r.cardinalityPct == 0.50
  ).sortBy(_.rows)
  
  if (filtered.size >= 2) {
    println(s"\n  $label:")
    println("    Rows\t\tHashMs\tSortMs\tFaster\tSpeedup%")
    filtered.foreach { s =>
      val faster = if (s.hashFaster) "Hash" else "Sort"
      println(f"    ${s.rows}%,10d\t${s.hashMs}%.2f\t${s.sortMs}%.2f\t$faster\t${s.speedupPct}%.2f")
    }
  }
}

println("\n" + "="*80)
println("RECOMMENDATIONS")
println("="*80)
println()

// Provide strategy recommendations based on results
println("Based on the benchmark results:\n")

println("1. KEY TYPE RECOMMENDATIONS:")
println("   - Fixed-width single keys (INT, LONG):")
val intSingle = results.filter(r => r.keyType == "int" && r.numKeys == 1)
val sortWinsInt = intSingle.count(!_.hashFaster)
if (sortWinsInt > intSingle.size / 2) {
  println("     → Sort joins generally faster, especially at low cardinality")
} else {
  println("     → Hash joins generally faster, especially at high cardinality")
}

println("   - Variable-length keys (STRING):")
val stringSingle = results.filter(r => r.keyType == "string" && r.numKeys == 1)
val hashWinsString = stringSingle.count(_.hashFaster)
if (hashWinsString > stringSingle.size / 2) {
  println("     → Hash joins generally faster across cardinalities")
} else {
  println("     → Sort joins competitive, depends on cardinality")
}

println("   - Multi-column keys:")
val multiKey = results.filter(_.numKeys > 1)
val sortWinsMulti = multiKey.count(!_.hashFaster)
if (sortWinsMulti > multiKey.size / 2) {
  println("     → Sort joins show advantage with multiple keys")
} else {
  println("     → Hash joins handle multi-column keys well")
}

println("\n2. CARDINALITY RECOMMENDATIONS:")
val lowCard = results.filter(_.cardinalityPct <= 0.25)
val sortWinsLowCard = lowCard.count(!_.hashFaster)
println(f"   - Low cardinality (<= 25%%): Sort wins ${sortWinsLowCard}/${lowCard.size} tests")
if (sortWinsLowCard > lowCard.size / 2) {
  println("     → Prefer sort joins with many duplicates")
}

val highCard = results.filter(_.cardinalityPct >= 0.75)
val hashWinsHighCard = highCard.count(_.hashFaster)
println(f"   - High cardinality (>= 75%%): Hash wins ${hashWinsHighCard}/${highCard.size} tests")
if (hashWinsHighCard > highCard.size / 2) {
  println("     → Prefer hash joins with few duplicates")
}

println("\n3. SIZE SCALING:")
val large = results.filter(_.rows >= 25000000)
if (large.nonEmpty) {
  val avgSpeedup = large.map(r => if (r.hashFaster) r.speedupPct else -r.speedupPct).sum / large.size
  if (avgSpeedup > 10) {
    println(f"   - At large scale (>= 25M rows): Hash joins average ${avgSpeedup}%.1f%% faster")
  } else if (avgSpeedup < -10) {
    println(f"   - At large scale (>= 25M rows): Sort joins average ${-avgSpeedup}%.1f%% faster")
  } else {
    println("   - At large scale (>= 25M rows): Performance is comparable")
  }
}

println("\nBenchmark complete!")

