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

// Key Remapping Performance Benchmark
//
// This script generates comprehensive benchmarks specifically for analyzing
// key remapping performance in joins.
//
// PREREQUISITES:
// - You must rebuild the microbenchmarks module with the updated JoinExecutor
//   and JoinBenchmarkRunner that support detailed timing collection
// - Run: mvn clean package -pl microbenchmarks -am -DskipTests
// - The detailed timing columns will be populated when key remapping is enabled
//
// Detailed timing breakdowns collected:
// - Remapping structure build time
// - Build key remapping time
// - Probe key remapping time
// - Join object creation time
// - Join execution time
//
// Test Coverage:
// - Distributions: Uniform (Flat), Zipf (skewed), Gaussian (normal)
// - Key types: int, long, decimal(9,2), decimal(18,2), decimal(38,2),
//              string(10), string(50), string(200)
// - Composite keys: 1, 2, and 3 columns with type mixtures
// - Table sizes: Build (10K-10M), Probe (100K-50M)
// - Cardinality: 10%-100% unique keys
// - Strategies: HashObjectWithPost, SortObjectWithPost
// - Always enables build side swapping
//
// Output: TSV file with detailed timing breakdowns

import com.nvidia.spark.rapids.benchmarks.JoinBenchmarkDataGen._
import com.nvidia.spark.rapids.benchmarks.{JoinBenchmarkRunner => JBR}
import org.apache.spark.sql.tests.datagen._
import org.apache.spark.sql.SparkSession
import scala.util.Random
import java.io.{File, PrintWriter}

val baseDir = "/data/tmp/key_remapping_benchmark"
val outputTsvPath = s"$baseDir/remapping_benchmark.tsv"
val random = new Random(42)  // Fixed seed for reproducibility

// ============================================================================
// Configuration
// ============================================================================

// Benchmark parameters
val benchmarkIterations = 5

// Build table sizes (rows)
val buildSizes = Seq(
  10000L,      // 10K
  100000L,     // 100K
  1000000L,    // 1M
  10000000L    // 10M
)

// Probe table sizes (rows)
val probeSizes = Seq(
  100000L,     // 100K
  1000000L,    // 1M
  10000000L,   // 10M
  50000000L    // 50M
)

// Cardinality percentages (unique keys / total rows)
val cardinalityPcts = Seq(0.01, 0.02, 0.04, 0.10, 0.25, 0.50, 1.0)

// ============================================================================
// Data Structures
// ============================================================================

case class KeyTypeSpec(
  name: String,
  sparkType: String,
  avgBytes: Int
)

// Single column key types
val singleColumnKeyTypes = Seq(
  KeyTypeSpec("int", "int", 4),
  KeyTypeSpec("long", "long", 8),
  KeyTypeSpec("decimal_32", "decimal(9,2)", 4),
  KeyTypeSpec("decimal_64", "decimal(18,2)", 8),
  KeyTypeSpec("decimal_128", "decimal(38,2)", 16),
  KeyTypeSpec("string_10", "string", 20),
  KeyTypeSpec("string_40", "string", 50),
  KeyTypeSpec("string_200", "string", 210)
)

// 2-column composite key types (interesting combinations)
val twoColumnKeyTypes = Seq(
  ("int+string_10", Seq(KeyTypeSpec("int", "int", 4), KeyTypeSpec("string_10", "string", 20))),
  ("long+string_40", Seq(KeyTypeSpec("long", "long", 8), KeyTypeSpec("string_40", "string", 50))),
  ("decimal_32+string_10", Seq(KeyTypeSpec("decimal_32", "decimal(9,2)", 4), KeyTypeSpec("string_10", "string", 20))),
  ("decimal_64+decimal_64", Seq(KeyTypeSpec("decimal_64", "decimal(18,2)", 8), KeyTypeSpec("decimal_64", "decimal(18,2)", 8))),
  ("string_10+string_10", Seq(KeyTypeSpec("string_10", "string", 20), KeyTypeSpec("string_10", "string", 20)))
)

// 3-column composite key types
val threeColumnKeyTypes = Seq(
  ("int+long+string_10", Seq(
    KeyTypeSpec("int", "int", 4),
    KeyTypeSpec("long", "long", 8),
    KeyTypeSpec("string_10", "string", 20)
  )),
  ("string_10+string_10+string_10", Seq(
    KeyTypeSpec("string_10", "string", 20),
    KeyTypeSpec("string_10", "string", 20),
    KeyTypeSpec("string_10", "string", 20)
  )),
  ("decimal_64+string_10+int", Seq(
    KeyTypeSpec("decimal_64", "decimal(18,2)", 8),
    KeyTypeSpec("string_10", "string", 20),
    KeyTypeSpec("int", "int", 4)
  ))
)

// ============================================================================
// Zipf Distribution Implementation
// ============================================================================

/**
 * Zipf distribution implementation for skewed data generation.
 * Based on Zipf's law: frequency(k) ∝ 1/k^s where s is the skew factor.
 * 
 * Copied from ml_join_training_data.scala since it's not part of core datagen.
 */
case class ZipfDistribution(
    skewFactor: Double = 1.0,
    numBuckets: Int = 100,
    colLocSeed: Long = 0,
    remapRangeFunc: Long => Long = n => n
) extends LocationToSeedMapping {

  override def withColumnConf(colConf: ColumnConf): ZipfDistribution = {
    val colLocSeed = colConf.columnLoc.hashLoc
    val remapFunc = LocationToSeedMapping.remapRangeFunc(colConf)
    
    if (skewFactor == 0.0) {
      ZipfDistribution(skewFactor, numBuckets, colLocSeed, remapFunc)
    } else {
      val min = colConf.minSeed
      val max = colConf.maxSeed
      val actualBuckets = math.min(numBuckets, (max - min + 1).toInt)
      val bucketSize = (max - min + 1) / actualBuckets
      
      val weightsAndMappings = (0 until actualBuckets).map { i =>
        val weight = 1.0 / math.pow(i + 1, skewFactor)
        val minKey = min + i * bucketSize
        val maxKey = if (i == actualBuckets - 1) max else min + (i + 1) * bucketSize - 1
        
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
      new ZipfDistribution(skewFactor, numBuckets, colLocSeed, remapFunc) {
        override def apply(rowLoc: RowLocation): Long = multiDist.apply(rowLoc)
      }
    }
  }

  override def apply(rowLoc: RowLocation): Long = {
    remapRangeFunc(rowLoc.hashLoc(colLocSeed))
  }
}

// ============================================================================
// Distribution Specifications
// ============================================================================

case class DistributionSpec(
  name: String,
  createMapping: (Long, Long) => LocationToSeedMapping
)

val distributions = Seq(
  DistributionSpec("flat", (_, _) => FlatDistribution()),
  DistributionSpec("zipf_0.5", (min, max) => {
    ZipfDistribution(skewFactor = 0.5)
  }),
  DistributionSpec("zipf_1.0", (min, max) => {
    ZipfDistribution(skewFactor = 1.0)
  }),
  DistributionSpec("gaussian", (min, max) => {
    val mean = (min + max) / 2.0
    val range = max - min
    val stdDev = range / 6.0  // 3 stddevs covers 99.7% of range
    NormalDistribution(mean, stdDev)
  })
)

// ============================================================================
// Custom Exception for Row Count Mismatch
// ============================================================================

class RowCountMismatchException(message: String) extends RuntimeException(message)

// ============================================================================
// Test Configuration
// ============================================================================

case class TestConfig(
  name: String,
  buildRows: Long,
  probeRows: Long,
  keySpecs: Seq[KeyTypeSpec],
  keyName: String,
  cardinalityPct: Double,
  distribution: DistributionSpec,
  strategy: String
)

// ============================================================================
// Data Generation
// ============================================================================

def generateTableData(
    tableName: String,
    numRows: Long,
    keySpecs: Seq[KeyTypeSpec],
    cardinalityPct: Double,
    distribution: DistributionSpec,
    randomSeed: Long = 0
): Unit = {
  val numKeyColumns = keySpecs.length
  val distinctKeys = math.max(1, (numRows * cardinalityPct).toLong)
  
  // Build column specifications with key columns + 1 payload column
  var schema = ""
  for (i <- 0 until numKeyColumns) {
    if (i > 0) schema += ", "
    schema += s"key$i ${keySpecs(i).sparkType}"
  }
  schema += ", payload long"
  
  val dg = DBGen()
  val table = dg.addTable(tableName, schema, numRows)
  
  // Configure key columns with distribution
  for (i <- 0 until numKeyColumns) {
    val colName = s"key$i"
    val colGen = table(colName)
    
    // Disable nulls in key columns (remapping has issues with nulls)
    colGen.setNullProbability(0.0)
    
    // Set seed range based on cardinality - use SAME range for build and probe!
    // This ensures keys overlap and joins produce output
    val minSeed = i * 1000000L
    val maxSeed = minSeed + distinctKeys - 1
    colGen.setSeedRange(minSeed, maxSeed)
    
    // Set distribution
    colGen.setSeedMapping(distribution.createMapping(minSeed, maxSeed))
    
    // Configure string length if needed
    keySpecs(i).name match {
      case "string_10" => colGen.setLength(10)
      case "string_40" => colGen.setLength(40)
      case "string_200" => colGen.setLength(200)
      case _ => // Fixed width types
    }
  }
  
  // Payload column (not used in join, just for realistic table structure)
  table("payload").setSeedRange(0, 1000000)
  
  // Generate Parquet file
  val outputPath = s"$baseDir/$tableName.parquet"
  table.toDF(spark).repartition(1).write.mode("overwrite").parquet(outputPath)
  
  println(s"Generated $tableName: $numRows rows, ${keySpecs.length} key columns, " +
    s"$distinctKeys distinct keys (${cardinalityPct*100}% cardinality), " +
    s"${distribution.name} distribution")
}

// ============================================================================
// TSV Writing Helper
// ============================================================================

// Helper function to write benchmark results to TSV file
def writeResultToTSV(results: JBR.BenchmarkResults, writer: PrintWriter): Unit = {
  if (results.status == JBR.SUCCESS) {
    // Format optimizations
    val opts = results.optimizations
    val optParts = scala.collection.mutable.ArrayBuffer[String]()
    if (opts.allowBuildSideSwap) optParts += "swap"
    if (opts.remapComplexKeysToInts) optParts += "remap"
    if (opts.useDistinctJoin) optParts += "distinct"
    if (opts.cacheJoinObject) optParts += "cache"
    if (opts.cacheRemapping) optParts += "cache-remap"
    if (opts.cacheDistinctFlag) optParts += "cache-distinct"
    val optimizations = if (optParts.isEmpty) "none" else optParts.mkString(",")
    
    // Format join type
    val joinType = results.joinType match {
      case JBR.InnerJoin => "inner"
      case JBR.LeftOuterJoin => "left_outer"
      case JBR.RightOuterJoin => "right_outer"
      case JBR.FullOuterJoin => "full_outer"
      case JBR.LeftSemiJoin => "left_semi"
      case JBR.LeftAntiJoin => "left_anti"
    }
    
    // Format strategy
    val strategy = results.joinStrategy match {
      case JBR.HashObjectStrategy => "hash_object"
      case JBR.HashObjectWithPostStrategy => "hash_object_post"
      case JBR.SortObjectWithPostStrategy => "sort_object_post"
      case JBR.HashDirectStrategy => "hash_direct"
      case JBR.HashDirectWithPostStrategy => "hash_direct_post"
      case JBR.SortDirectWithPostStrategy => "sort_direct_post"
    }
    
    // Format build side config
    val buildSideConfig = results.buildSideConfig match {
      case JBR.LeftBuild => "left"
      case JBR.RightBuild => "right"
      case JBR.AutoPickSmallerIfAllowed => "auto_smaller"
      case JBR.AutoMeetJoinRequirement => "auto_required"
    }
    
    val actualBuild = results.actualBuildSide.getOrElse("N/A")
    
    // Format detailed timings if available
    val detailedTimingsStr = results.detailedTimings match {
      case Some(dt) =>
        f"\t${dt.remapStructureBuildMs}%.3f\t${dt.remapBuildKeysMs}%.3f\t" +
        f"${dt.remapProbeKeysMs}%.3f\t${dt.createBuildObjectMs}%.3f\t" +
        f"${dt.executeJoinMs}%.3f"
      case None =>
        "\t\t\t\t\t"  // Empty columns when no detailed timings
    }
    
    writer.println(s"${results.testName}\t${results.status}\t" +
      s"${results.leftRows}\t${results.rightRows}\t${results.outputRows}\t" +
      s"${results.numThreads}\t${results.iterations}\t" +
      f"${results.wallClockMs}%.2f\t${results.averageMs}%.2f\t" +
      f"${results.medianMs}%.2f\t${results.minMs}%.2f\t" +
      f"${results.maxMs}%.2f\t${results.stdDevMs}%.2f\t" +
      s"$joinType\t$strategy\t$buildSideConfig\t$actualBuild\t$optimizations" +
      detailedTimingsStr)
    
    writer.flush()  // Flush immediately so results are available
  } else {
    writer.println(s"${results.testName}\t${results.status}\t" +
      s"ERROR\t${results.errorMessage.getOrElse("Unknown error")}")
    writer.flush()
  }
}

// ============================================================================
// Benchmark Execution
// ============================================================================

def runTest(config: TestConfig, tsvWriter: PrintWriter): Unit = {
  println(s"\n========================================")
  println(s"Running: ${config.name}")
  println(s"========================================")
  
  // Generate data ONCE - will be used for both hash and sort strategies
  // Both tables use the SAME key range to ensure overlap
  generateTableData(
    "build_table",
    config.buildRows,
    config.keySpecs,
    config.cardinalityPct,
    config.distribution,
    randomSeed = 12345L  // Random seed for row sampling
  )
  
  generateTableData(
    "probe_table",
    config.probeRows,
    config.keySpecs,
    config.cardinalityPct,
    config.distribution,
    randomSeed = 67890L  // Different random seed for probe table sampling
  )
  
  // Setup benchmark configuration
  val buildPath = s"$baseDir/build_table.parquet"
  val probePath = s"$baseDir/probe_table.parquet"
  
  val numKeyColumns = config.keySpecs.length
  val leftKeyIndices = (0 until numKeyColumns).toSeq
  val rightKeyIndices = (0 until numKeyColumns).toSeq
  
  // Test both strategies with the same data
  val strategies = Seq(
    ("hash", JBR.HashObjectWithPostStrategy),
    ("sort", JBR.SortObjectWithPostStrategy)
  )
  
  strategies.foreach { case (strategyName, strategy) =>
    println("\n" + "="*80)
    println(s"SUB-TEST: ${config.name}_${strategyName}_swap (WITHOUT remapping)")
    println("="*80)
    
    // Run WITHOUT key remapping (baseline - swap only)
    val optimizationsWithoutRemap = JBR.JoinOptimizations(
      allowBuildSideSwap = true,
      remapComplexKeysToInts = false,
      useDistinctJoin = false,
      cacheJoinObject = false,  // No caching
      cacheRemapping = false,
      cacheDistinctFlag = false
    )
    
    val configWithoutRemap = JBR.JoinBenchmarkConfig(
      testName = s"${config.name}_${strategyName}_swap",
      leftParquetPath = buildPath,
      rightParquetPath = probePath,
      joinType = JBR.InnerJoin,
      joinStrategy = strategy,
      buildSide = JBR.AutoPickSmallerIfAllowed,
      optimizations = optimizationsWithoutRemap,
      conditionalFilter = None,
      leftKeyIndices = leftKeyIndices,
      rightKeyIndices = rightKeyIndices,
      iterations = benchmarkIterations,
      numThreads = 1,
      printHeader = false,
      collectDetailedTimings = true  // Collect detailed timings for both cases
    )
    
    val resultsWithoutRemap = JBR.runBenchmark(configWithoutRemap, spark)
    JBR.printResultsTSV(resultsWithoutRemap)  // Print to stdout
    writeResultToTSV(resultsWithoutRemap, tsvWriter)  // Write to file
    
    println("\n" + "="*80)
    println(s"SUB-TEST: ${config.name}_${strategyName}_swap_remap (WITH remapping)")
    println("="*80)
    
    // Run WITH key remapping (swap + remap)
    val optimizationsWithRemap = JBR.JoinOptimizations(
      allowBuildSideSwap = true,
      remapComplexKeysToInts = true,
      useDistinctJoin = false,
      cacheJoinObject = false,  // No caching
      cacheRemapping = false,
      cacheDistinctFlag = false
    )
    
    val configWithRemap = JBR.JoinBenchmarkConfig(
      testName = s"${config.name}_${strategyName}_swap_remap",
      leftParquetPath = buildPath,
      rightParquetPath = probePath,
      joinType = JBR.InnerJoin,
      joinStrategy = strategy,
      buildSide = JBR.AutoPickSmallerIfAllowed,
      optimizations = optimizationsWithRemap,
      conditionalFilter = None,
      leftKeyIndices = leftKeyIndices,
      rightKeyIndices = rightKeyIndices,
      iterations = benchmarkIterations,
      numThreads = 1,
      printHeader = false,
      collectDetailedTimings = true  // Enable detailed timings for remapping tests!
    )
    
    val resultsWithRemap = JBR.runBenchmark(configWithRemap, spark)
    JBR.printResultsTSV(resultsWithRemap)  // Print to stdout
    writeResultToTSV(resultsWithRemap, tsvWriter)  // Write to file
    
    // CRITICAL CHECK: Verify that remapping produces the same output row count
    if (resultsWithoutRemap.outputRows != resultsWithRemap.outputRows) {
      println("\n" + "="*80)
      println("ERROR: OUTPUT ROW COUNT MISMATCH DETECTED!")
      println("="*80)
      println(s"Test: ${config.name}")
      println(s"Strategy: ${strategyName}")
      println(s"Without remapping: ${resultsWithoutRemap.outputRows} rows")
      println(s"With remapping:    ${resultsWithRemap.outputRows} rows")
      println(s"Difference:        ${resultsWithRemap.outputRows - resultsWithoutRemap.outputRows} rows")
      println()
      println("Configuration details:")
      println(s"  Build rows:    ${config.buildRows}")
      println(s"  Probe rows:    ${config.probeRows}")
      println(s"  Key columns:   ${numKeyColumns}")
      println(s"  Key types:     ${config.keySpecs.map(_.name).mkString(", ")}")
      println(s"  Cardinality:   ${(config.cardinalityPct * 100).toInt}%")
      println(s"  Distribution:  ${config.distribution.name}")
      println()
      println("Data files preserved for debugging:")
      println(s"  Build table: $buildPath")
      println(s"  Probe table: $probePath")
      println()
      println("Stopping benchmark execution to allow debugging.")
      println("="*80)
      
      // Flush TSV output before exiting
      tsvWriter.flush()
      
      // Throw custom exception to stop execution - data will NOT be deleted
      throw new RowCountMismatchException(
        s"Output row count mismatch: ${strategyName} strategy produced " +
        s"${resultsWithoutRemap.outputRows} rows without remapping but " +
        s"${resultsWithRemap.outputRows} rows with remapping"
      )
    }
  }
  
  // Cleanup data files (only reached if no errors)
  new File(buildPath).delete()
  new File(probePath).delete()
  
  println(s"Completed: ${config.name}")
}

// ============================================================================
// Test Generation
// ============================================================================

def generateTestConfigs(): Seq[TestConfig] = {
  val configs = scala.collection.mutable.ArrayBuffer[TestConfig]()
  
  // Test 1: Single column keys - cover all types, distributions, sizes, cardinalities
  // Note: Each config will test BOTH hash and sort strategies, so we don't need strategy in the loop
  println("Generating single column key tests...")
  for {
    keySpec <- singleColumnKeyTypes
    buildSize <- buildSizes
    probeSize <- probeSizes.filter(_ >= buildSize)  // Probe should be >= build
    cardinality <- cardinalityPcts
    dist <- distributions
  } {
    configs += TestConfig(
      name = s"1col_${keySpec.name}_b${buildSize/1000}k_p${probeSize/1000}k_card${(cardinality*100).toInt}_${dist.name}",
      buildRows = buildSize,
      probeRows = probeSize,
      keySpecs = Seq(keySpec),
      keyName = keySpec.name,
      cardinalityPct = cardinality,
      distribution = dist,
      strategy = ""  // Will test both strategies
    )
  }
  
  // Test 2: Two column keys - test select combinations
  // Reduce test matrix to avoid explosion: sample sizes and cardinalities
  println("Generating two column key tests...")
  val sampledBuildSizes2 = Seq(100000L, 1000000L, 10000000L)
  val sampledProbeSizes2 = Seq(1000000L, 10000000L)
  val sampledCardinalities2 = Seq(0.25, 0.75, 1.0)
  val sampledDistributions2 = Seq(distributions(0), distributions(2))  // flat and zipf_1.0
  
  for {
    (keyName, keySpecs) <- twoColumnKeyTypes
    buildSize <- sampledBuildSizes2
    probeSize <- sampledProbeSizes2.filter(_ >= buildSize)
    cardinality <- sampledCardinalities2
    dist <- sampledDistributions2
  } {
    configs += TestConfig(
      name = s"2col_${keyName}_b${buildSize/1000}k_p${probeSize/1000}k_card${(cardinality*100).toInt}_${dist.name}",
      buildRows = buildSize,
      probeRows = probeSize,
      keySpecs = keySpecs,
      keyName = keyName,
      cardinalityPct = cardinality,
      distribution = dist,
      strategy = ""  // Will test both strategies
    )
  }
  
  // Test 3: Three column keys - even more selective sampling
  println("Generating three column key tests...")
  val sampledBuildSizes3 = Seq(100000L, 1000000L)
  val sampledProbeSizes3 = Seq(1000000L, 10000000L)
  val sampledCardinalities3 = Seq(0.50, 1.0)
  val sampledDistributions3 = Seq(distributions(0), distributions(3))  // flat and gaussian
  
  for {
    (keyName, keySpecs) <- threeColumnKeyTypes
    buildSize <- sampledBuildSizes3
    probeSize <- sampledProbeSizes3.filter(_ >= buildSize)
    cardinality <- sampledCardinalities3
    dist <- sampledDistributions3
  } {
    configs += TestConfig(
      name = s"3col_${keyName}_b${buildSize/1000}k_p${probeSize/1000}k_card${(cardinality*100).toInt}_${dist.name}",
      buildRows = buildSize,
      probeRows = probeSize,
      keySpecs = keySpecs,
      keyName = keyName,
      cardinalityPct = cardinality,
      distribution = dist,
      strategy = ""  // Will test both strategies
    )
  }
  
  // Filter out tests that would exceed Int.MaxValue bytes for string columns
  // This is a limitation of CUDF string columns (limit is per column, not per row)
  val INT_MAX_BYTES = Int.MaxValue.toLong
  
  def getStringColumnLength(keySpec: KeyTypeSpec): Long = {
    keySpec.name match {
      case "string_10" => 10L
      case "string_40" => 40L
      case "string_200" => 200L
      case _ => 0L
    }
  }
  
  val filteredConfigs = configs.filter { config =>
    val maxRows = math.max(config.buildRows, config.probeRows)
    
    // Check each string column individually
    val anyColumnExceedsLimit = config.keySpecs.exists { keySpec =>
      val stringLength = getStringColumnLength(keySpec)
      if (stringLength > 0) {
        val totalBytesForColumn = maxRows * stringLength
        totalBytesForColumn > INT_MAX_BYTES
      } else {
        false  // Non-string column, can't exceed limit
      }
    }
    
    !anyColumnExceedsLimit  // Keep configs where no column exceeds limit
  }
  
  val removedCount = configs.length - filteredConfigs.length
  if (removedCount > 0) {
    println(s"Filtered out $removedCount test configurations that would exceed Int.MaxValue bytes for string columns")
  }
  
  // Sort by total data size (buildRows + probeRows) in descending order
  // This ensures largest tests run first to catch OOM errors early
  val sortedConfigs = filteredConfigs.sortBy(c => -(c.buildRows + c.probeRows))
  
  println(s"Generated ${sortedConfigs.length} test configurations")
  println(s"Sorted by data size (largest first) to catch OOM errors early")
  sortedConfigs
}

// ============================================================================
// Main Execution
// ============================================================================

def runAllTests(): Unit = {
  // Create output directory
  new File(baseDir).mkdirs()

  // Generate test configurations
  val testConfigs = generateTestConfigs()

  println(s"Total tests to run: ${testConfigs.length}")
  println(s"Output will be written to: $outputTsvPath")
  println()

  // Open TSV file for writing
  val tsvWriter = new PrintWriter(new File(outputTsvPath))
  
  try {
    // Write TSV header to file
    tsvWriter.println("TestName\tStatus\tLeftRows\tRightRows\tOutputRows\t" +
      "NumThreads\tIterations\tWallClockMs\tAvgTimeMs\tMedianTimeMs\t" +
      "MinTimeMs\tMaxTimeMs\tStdDevMs\tJoinType\tStrategy\t" +
      "BuildSideConfig\tActualBuildSide\tOptimizations\t" +
      "RemapStructureBuildMs\tRemapBuildKeysMs\tRemapProbeKeysMs\t" +
      "CreateBuildObjectMs\tExecuteJoinMs")
    tsvWriter.flush()
    
    // Also print header to stdout
    JBR.printTSVHeader()

    // Run all benchmarks with progress and ETA
    val startTime = System.currentTimeMillis()
    var completedTests = 0
    val totalTests = testConfigs.length
    // Each test runs 4 benchmarks: hash_swap, hash_swap_remap, sort_swap, sort_swap_remap
    val totalBenchmarks = totalTests * 4
    
    testConfigs.zipWithIndex.foreach { case (config, idx) =>
      val currentTestNum = idx + 1
      val elapsedSec = (System.currentTimeMillis() - startTime) / 1000.0
      val avgSecPerTest = if (completedTests > 0) elapsedSec / completedTests else 0.0
      val remainingTests = totalTests - completedTests
      val etaSec = if (avgSecPerTest > 0) avgSecPerTest * remainingTests else 0.0
      val etaMin = (etaSec / 60).toInt
      val etaHours = etaMin / 60
      val etaMinRemainder = etaMin % 60
      
      val etaStr = if (completedTests == 0) {
        "Calculating..."
      } else if (etaHours > 0) {
        f"~${etaHours}h ${etaMinRemainder}%02dm"
      } else {
        f"~${etaMinRemainder}m"
      }
      
      println(s"\n[$currentTestNum/$totalTests] ${config.name}")
      println(s"Progress: ${(completedTests.toDouble / totalTests * 100).toInt}% complete, ETA: $etaStr")
      
      try {
        runTest(config, tsvWriter)
        completedTests += 1
      } catch {
        case e: RowCountMismatchException =>
          // Row count mismatch is a critical error - stop immediately
          println(s"\nCRITICAL ERROR: ${e.getMessage}")
          tsvWriter.close()
          throw e  // Re-throw to stop all execution
        case e: IllegalStateException =>
          // Iteration mismatch or other state errors are critical - stop immediately
          println(s"\nCRITICAL ERROR: ${e.getMessage}")
          tsvWriter.close()
          throw e  // Re-throw to stop all execution
        case e: Exception =>
          // Other exceptions: log and continue
          println(s"ERROR: ${e.getMessage}")
          e.printStackTrace()
          completedTests += 1  // Count as completed even on error
      }
    }

    val totalTime = (System.currentTimeMillis() - startTime) / 1000.0
    val totalMin = (totalTime / 60).toInt
    val totalHours = totalMin / 60
    val totalMinRemainder = totalMin % 60
    val timeStr = if (totalHours > 0) {
      f"${totalHours}h ${totalMinRemainder}%02dm"
    } else {
      f"${totalMinRemainder}m"
    }
    
    println(s"\n========================================")
    println(s"All benchmarks completed in $timeStr")
    println(s"Total benchmark runs: $totalBenchmarks (${totalTests} tests × 4 benchmarks each)")
    println(s"Results written to: $outputTsvPath")
    println(s"========================================")
    
  } finally {
    tsvWriter.close()
  }
}

// Execute all tests
runAllTests()

