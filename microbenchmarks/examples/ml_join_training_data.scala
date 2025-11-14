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

// ML Training Data Generation for Join Optimization Heuristics
//
// This script generates diverse join test cases to train ML models for
// predicting optimal join strategies and optimizations. It focuses on:
// 1. Sort vs Hash vs Hash Direct strategy selection
// 2. Key remapping benefits
// 3. Distinct join applicability
//
// Note: All tests use auto build side selection (allowBuildSideSwap=true) because:
// - hash_direct already optimizes build side internally in cuDF
// - hash_object benefits from automatic smaller-side selection
// - This provides apples-to-apples comparison between strategies
//
// Key Features:
// - Generates realistic data distributions (uniform, Zipf, normal)
// - Simulates primary/foreign key relationships
// - Targets GPU memory sizes (512MiB-1GiB per side)
// - Collects cheap statistics (row counts, distinct counts, types, sizes)
// - Error handling (failures don't stop execution)
// - Multiple iterations per test for statistical significance
// - Adaptive exploration: finds cutoff points and explores boundaries
// - TSV output for ML analysis
// - Disk-efficient: Generate → Test → Cleanup for each config (idempotent regeneration)
//
// Execution Strategy:
// For each test configuration:
//   1. Generate left/right join input data
//   2. Run all strategy/optimization variants
//   3. Write results to TSV (with config for regeneration)
//   4. Delete input data (can regenerate if needed - datagen is idempotent)
//   5. Move to next config
//
// This approach minimizes disk usage while maintaining full reproducibility.
//
// Runtime: Variable depending on number of tests generated (estimate 2-4 hours for comprehensive suite)

import com.nvidia.spark.rapids.benchmarks.JoinBenchmarkDataGen._
import com.nvidia.spark.rapids.benchmarks.JoinBenchmarkRunner._
import org.apache.spark.sql.tests.datagen._
import org.apache.spark.sql.SparkSession
import scala.util.Random
import java.io.{File, PrintWriter}

val baseDir = "/data/tmp/ml_join_training"
val outputTsvPath = s"$baseDir/training_data.tsv"
val random = new Random(42)  // Fixed seed for reproducibility

// ============================================================================
// Zipf Distribution Implementation (from hash_vs_sort_analysis.scala)
// ============================================================================

/**
 * Zipf distribution implementation for skewed data generation.
 * Based on Zipf's law: frequency(k) ∝ 1/k^s where s is the skew factor.
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

// ============================================================================
// Configuration
// ============================================================================

// Number of initial random tests to generate
val numInitialTests = 1000

// Number of refinement tests to generate after finding correlations
val numRefinementTests = 100

// Iterations per benchmark (for statistical significance)
val benchmarkIterations = 5

// Memory targets (in bytes) - 1MiB to 512MiB
// Larger sizes make timing differences more obvious, especially for sort joins
val minMemoryTarget = 1L * 1024 * 1024
val maxMemoryTarget = 512L * 1024 * 1024

// ============================================================================
// Data Structures
// ============================================================================

case class KeyTypeSpec(
  typeName: String,
  avgBytes: Int,  // Average bytes per key (for memory estimation)
  isVariableWidth: Boolean
)

val keyTypes = Seq(
  KeyTypeSpec("int", 4, false),
  KeyTypeSpec("long", 8, false),
  KeyTypeSpec("decimal(9,2)", 4, false),
  KeyTypeSpec("decimal(18,2)", 8, false),
  KeyTypeSpec("decimal(38,2)", 16, false),
  KeyTypeSpec("string_short", 20, true),    // ~10 char strings
  KeyTypeSpec("string_medium", 60, true),   // ~50 char strings
  KeyTypeSpec("string_long", 210, true)     // ~200 char strings
)

case class DistributionSpec(
  name: String,
  description: String
)

val distributions = Seq(
  DistributionSpec("uniform", "Uniform distribution"),
  DistributionSpec("zipf_light", "Zipf s=0.5 (light skew)"),
  DistributionSpec("zipf_moderate", "Zipf s=1.0 (moderate skew)"),
  DistributionSpec("zipf_heavy", "Zipf s=1.5 (heavy skew)")
)

case class MLTestConfig(
  name: String,
  // Left side (build side candidate)
  leftRows: Long,
  leftKeyType: KeyTypeSpec,  // Must match rightKeyType (join requirement)
  leftDistinctKeys: Long,
  leftDistribution: String,
  leftIsPrimaryKey: Boolean,  // If true, all keys are distinct
  // Right side (probe side candidate)
  rightRows: Long,
  rightKeyType: KeyTypeSpec,  // Must match leftKeyType (join requirement)
  rightDistinctKeys: Long,
  rightDistribution: String,
  rightIsPrimaryKey: Boolean,
  // Key relationship (for PK/FK simulation)
  keyOverlapPct: Double,  // % of FK keys that exist in PK
  // Composite keys (1-3 columns)
  numKeyColumns: Int,
  // Estimated memory usage
  leftMemoryMB: Double,
  rightMemoryMB: Double
)

case class TableStats(
  rows: Long,
  distinctKeys: Long,
  cardinalityPct: Double,
  keyType: String,
  avgKeyBytes: Int,
  memoryMB: Double,
  distribution: String,
  isPrimaryKey: Boolean
)

case class MLBenchmarkResult(
  testName: String,
  // Configuration
  joinStrategy: String,
  buildSide: String,
  useDistinctJoin: Boolean,
  useKeyRemapping: Boolean,
  allowBuildSideSwap: Boolean,
  // Left side stats
  leftStats: TableStats,
  // Right side stats
  rightStats: TableStats,
  // Relationship stats
  numKeyColumns: Int,
  keyOverlapPct: Double,
  // Results
  status: String,  // SUCCESS or FAILED
  medianTimeMs: Double,
  avgTimeMs: Double,
  stdDevMs: Double,
  outputRows: Long,
  actualBuildSide: String,
  iterations: Int,
  errorMessage: String = ""
)

// ============================================================================
// Helper Functions
// ============================================================================

def estimateMemoryMB(rows: Long, keyType: KeyTypeSpec, numKeys: Int): Double = {
  val bytesPerRow = keyType.avgBytes * numKeys
  (rows * bytesPerRow).toDouble / (1024 * 1024)
}

def calculateTargetRows(targetMemoryBytes: Long, keyType: KeyTypeSpec, numKeys: Int): Long = {
  val bytesPerRow = keyType.avgBytes * numKeys
  math.max(10000, targetMemoryBytes / bytesPerRow)
}

def pickRandomKeyType(): KeyTypeSpec = {
  keyTypes(random.nextInt(keyTypes.length))
}

def pickRandomDistribution(): String = {
  distributions(random.nextInt(distributions.length)).name
}

def pickCardinalityPct(isPrimaryKey: Boolean): Double = {
  if (isPrimaryKey) {
    1.0  // 100% distinct for primary keys
  } else {
    // Random cardinality from realistic range
    // Focus on low cardinalities (1-4%) where sort joins excel
    // Use weighted selection to get more low-cardinality cases
    val r = random.nextDouble()
    if (r < 0.40) {
      // 40% of cases: very low cardinality (sort-friendly)
      val lowOptions = Seq(0.005, 0.01, 0.015, 0.02, 0.025, 0.03, 0.035, 0.04)
      lowOptions(random.nextInt(lowOptions.length))
    } else if (r < 0.65) {
      // 25% of cases: low-medium cardinality
      val medLowOptions = Seq(0.05, 0.08, 0.10, 0.15)
      medLowOptions(random.nextInt(medLowOptions.length))
    } else {
      // 35% of cases: medium to high cardinality
      val highOptions = Seq(0.25, 0.50, 0.75, 1.0)
      highOptions(random.nextInt(highOptions.length))
    }
  }
}

def pickKeyOverlapPct(): Double = {
  // For FK relationships: how many FK values exist in PK
  // 0.5 = 50% orphaned FKs, 1.0 = perfect referential integrity
  val options = Seq(0.5, 0.7, 0.8, 0.9, 0.95, 1.0)
  options(random.nextInt(options.length))
}

// ============================================================================
// Test Case Generation
// ============================================================================

def generateRandomTestConfig(testId: Int): MLTestConfig = {
  // Decide if this is a PK/FK test or general test
  val isPKFK = random.nextDouble() < 0.3  // 30% PK/FK tests
  
  // Pick key type - MUST BE SAME for left and right (join requirement)
  val keyType = pickRandomKeyType()
  val leftKeyType = keyType
  val rightKeyType = keyType
  
  // Pick number of key columns
  // 70% single key, 25% 2-column composite, 5% 3-column composite
  val numKeys = {
    val r = random.nextDouble()
    if (r < 0.70) 1
    else if (r < 0.95) 2
    else 3
  }
  
  // Calculate target memory for each side
  val leftTargetMemory = minMemoryTarget + random.nextLong() % (maxMemoryTarget - minMemoryTarget)
  val rightTargetMemory = minMemoryTarget + random.nextLong() % (maxMemoryTarget - minMemoryTarget)
  
  // Calculate row counts based on memory target
  val leftRows = calculateTargetRows(leftTargetMemory, leftKeyType, numKeys)
  val rightRows = calculateTargetRows(rightTargetMemory, rightKeyType, numKeys)
  
  // Determine PK/FK configuration
  val (leftIsPK, rightIsPK, keyOverlap) = if (isPKFK) {
    // One side is PK (100% distinct), other is FK
    if (random.nextBoolean()) {
      (true, false, pickKeyOverlapPct())
    } else {
      (false, true, pickKeyOverlapPct())
    }
  } else {
    (false, false, 1.0)  // No PK/FK relationship
  }
  
  // Calculate distinct keys
  val leftCardPct = pickCardinalityPct(leftIsPK)
  val rightCardPct = pickCardinalityPct(rightIsPK)
  
  val leftDistinct = (leftRows * leftCardPct).toLong
  val rightDistinct = (rightRows * rightCardPct).toLong
  
  // Pick distributions
  val leftDist = pickRandomDistribution()
  val rightDist = pickRandomDistribution()
  
  // Calculate actual memory
  val leftMem = estimateMemoryMB(leftRows, leftKeyType, numKeys)
  val rightMem = estimateMemoryMB(rightRows, rightKeyType, numKeys)
  
  MLTestConfig(
    name = f"ml_test_${testId}%04d",
    leftRows = leftRows,
    leftKeyType = leftKeyType,
    leftDistinctKeys = leftDistinct,
    leftDistribution = leftDist,
    leftIsPrimaryKey = leftIsPK,
    rightRows = rightRows,
    rightKeyType = rightKeyType,
    rightDistinctKeys = rightDistinct,
    rightDistribution = rightDist,
    rightIsPrimaryKey = rightIsPK,
    keyOverlapPct = keyOverlap,
    numKeyColumns = numKeys,
    leftMemoryMB = leftMem,
    rightMemoryMB = rightMem
  )
}

// Generate explicit sort-friendly test cases
// Sort joins excel when build side has very low cardinality (1-4%) and larger data sizes
def generateSortFriendlyTestConfigs(startId: Int): Seq[MLTestConfig] = {
  val configs = scala.collection.mutable.ArrayBuffer[MLTestConfig]()
  var id = startId
  
  // Test various cardinalities at different data sizes
  val cardinalities = Seq(0.005, 0.01, 0.015, 0.02, 0.025, 0.03, 0.035, 0.04)  // 0.5% to 4%
  val memorySizes = Seq(50L, 100L, 200L, 400L)  // MB
  val keyTypeSubset = Seq(
    keyTypes.find(_.typeName == "long").get,
    keyTypes.find(_.typeName == "string_short").get,
    keyTypes.find(_.typeName == "decimal(18,2)").get
  )
  
  for {
    card <- cardinalities.take(4)  // Use 4 cardinalities: 0.5%, 1%, 1.5%, 2%
    memMB <- memorySizes.take(3)   // Use 3 sizes: 50MB, 100MB, 200MB
    keyType <- keyTypeSubset.take(2)  // Use 2 key types: long, string_short
  } {
    val numKeys = 1  // Single-column keys for simplicity
    val memBytes = memMB * 1024 * 1024
    
    val leftRows = calculateTargetRows(memBytes, keyType, numKeys)
    val rightRows = calculateTargetRows(memBytes * 2, keyType, numKeys)  // Right side 2x larger
    
    val leftDistinct = (leftRows * card).toLong.max(10)
    val rightDistinct = (rightRows * 0.5).toLong.max(100)  // Right side has 50% cardinality
    
    val leftMem = estimateMemoryMB(leftRows, keyType, numKeys)
    val rightMem = estimateMemoryMB(rightRows, keyType, numKeys)
    
    configs += MLTestConfig(
      name = f"ml_sort_friendly_${id}%04d",
      leftRows = leftRows,
      leftKeyType = keyType,
      leftDistinctKeys = leftDistinct,
      leftDistribution = "uniform",
      leftIsPrimaryKey = false,
      rightRows = rightRows,
      rightKeyType = keyType,
      rightDistinctKeys = rightDistinct,
      rightDistribution = "uniform",
      rightIsPrimaryKey = false,
      keyOverlapPct = 1.0,
      numKeyColumns = numKeys,
      leftMemoryMB = leftMem,
      rightMemoryMB = rightMem
    )
    
    id += 1
  }
  
  println(s"Generated ${configs.size} sort-friendly test configs (low cardinality, larger sizes)")
  configs.toSeq
}

def generateInitialTestConfigs(): Seq[MLTestConfig] = {
  // Start with explicit sort-friendly cases
  val sortFriendly = generateSortFriendlyTestConfigs(0)
  val startId = sortFriendly.size
  
  // Then add random cases
  val randomConfigs = (startId until numInitialTests).map(i => generateRandomTestConfig(i))
  
  sortFriendly ++ randomConfigs
}

// ============================================================================
// Data Generation
// ============================================================================

def createDistribution(distributionName: String): LocationToSeedMapping = {
  distributionName match {
    case "uniform" =>
      FlatDistribution()
    case "zipf_light" =>
      ZipfDistribution(skewFactor = 0.5)
    case "zipf_moderate" =>
      ZipfDistribution(skewFactor = 1.0)
    case "zipf_heavy" =>
      ZipfDistribution(skewFactor = 1.5)
    case _ =>
      println(s"Warning: Unknown distribution $distributionName, using uniform")
      FlatDistribution()
  }
}

def generateTestData(config: MLTestConfig): Unit = {
  println(f"  Generating ${config.name}:")
  println(f"    Left:  ${config.leftRows}%,d rows, ${config.leftDistinctKeys}%,d distinct " +
          f"(${config.leftKeyType.typeName}, ${config.leftDistribution})")
  println(f"    Right: ${config.rightRows}%,d rows, ${config.rightDistinctKeys}%,d distinct " +
          f"(${config.rightKeyType.typeName}, ${config.rightDistribution})")
  
  try {
    // Determine key seed ranges
    // For PK/FK relationships, we need overlapping key ranges
    val (leftMinSeed, leftMaxSeed, rightMinSeed, rightMaxSeed) = if (config.leftIsPrimaryKey || config.rightIsPrimaryKey) {
      // PK/FK case: ensure FK keys reference PK keys with specified overlap
      val pkDistinct = if (config.leftIsPrimaryKey) config.leftDistinctKeys else config.rightDistinctKeys
      val fkDistinct = if (config.leftIsPrimaryKey) config.rightDistinctKeys else config.leftDistinctKeys
      
      val overlapKeys = (pkDistinct * config.keyOverlapPct).toLong
      val orphanKeys = fkDistinct - overlapKeys
      
      if (config.leftIsPrimaryKey) {
        // Left is PK, right is FK
        (0L, pkDistinct - 1, 0L, fkDistinct - 1)
      } else {
        // Right is PK, left is FK
        (0L, fkDistinct - 1, 0L, pkDistinct - 1)
      }
    } else {
      // General case: independent key ranges
      (0L, config.leftDistinctKeys - 1, 0L, config.rightDistinctKeys - 1)
    }
    
    // Create distributions
    val leftDist = createDistribution(config.leftDistribution)
    val rightDist = createDistribution(config.rightDistribution)
    
    // Build key columns with distribution
    val baseKeyType = if (config.leftKeyType.typeName.startsWith("string")) "string" else config.leftKeyType.typeName
    val leftKeyColumns = (1 to config.numKeyColumns).map { i =>
      KeyColumnSpec(s"key$i", baseKeyType, minSeed = leftMinSeed, maxSeed = leftMaxSeed, distribution = leftDist)
    }
    val rightKeyColumns = (1 to config.numKeyColumns).map { i =>
      KeyColumnSpec(s"key$i", baseKeyType, minSeed = rightMinSeed, maxSeed = rightMaxSeed, distribution = rightDist)
    }
    
    // Set string length if needed
    val stringLength = if (config.leftKeyType.typeName.startsWith("string")) {
      Some(config.leftKeyType.typeName match {
        case "string_short" => 10
        case "string_medium" => 50
        case "string_long" => 200
      })
    } else None
    
    // Create table configs
    val leftConfig = TableGenConfig(
      numRows = config.leftRows,
      keyColumns = leftKeyColumns,
      payloadColumns = Seq(),
      outputPath = s"$baseDir/${config.name}/left"
    )
    
    val rightConfig = TableGenConfig(
      numRows = config.rightRows,
      keyColumns = rightKeyColumns,
      payloadColumns = Seq(),
      outputPath = s"$baseDir/${config.name}/right"
    )
    
    // Generate using datagen API
    generateJoinTables(leftConfig, rightConfig, keyGroupId = 0, spark)
    
    // If strings, we need to set length manually using DBGen
    if (stringLength.isDefined) {
      val len = stringLength.get
      val dbgen = DBGen()
      
      // Regenerate with proper string length
      val leftDDL = (1 to config.numKeyColumns).map(i => s"key$i string").mkString(", ")
      val leftTable = dbgen.addTable("left", leftDDL, config.leftRows)
      for (i <- 1 to config.numKeyColumns) {
        leftTable(s"key$i").setSeedRange(leftMinSeed, leftMaxSeed)
        leftTable(s"key$i").setLength(len)
      }
      val leftDf = leftTable.toDF(spark)
      leftDf.repartition(1).write.mode("overwrite").parquet(leftConfig.outputPath)
      
      val rightDDL = (1 to config.numKeyColumns).map(i => s"key$i string").mkString(", ")
      val rightTable = dbgen.addTable("right", rightDDL, config.rightRows)
      for (i <- 1 to config.numKeyColumns) {
        rightTable(s"key$i").setSeedRange(rightMinSeed, rightMaxSeed)
        rightTable(s"key$i").setLength(len)
      }
      val rightDf = rightTable.toDF(spark)
      rightDf.repartition(1).write.mode("overwrite").parquet(rightConfig.outputPath)
    }
    
  } catch {
    case e: Exception =>
      println(s"    ERROR generating data: ${e.getMessage}")
  }
}

// ============================================================================
// Statistics Collection
// ============================================================================

def collectTableStats(config: MLTestConfig, side: String): TableStats = {
  val (rows, keyType, distinctKeys, dist, isPK, memMB) = side match {
    case "left" => (config.leftRows, config.leftKeyType, config.leftDistinctKeys, 
                    config.leftDistribution, config.leftIsPrimaryKey, config.leftMemoryMB)
    case "right" => (config.rightRows, config.rightKeyType, config.rightDistinctKeys, 
                     config.rightDistribution, config.rightIsPrimaryKey, config.rightMemoryMB)
  }
  
  val cardPct = distinctKeys.toDouble / rows.toDouble
  
  TableStats(
    rows = rows,
    distinctKeys = distinctKeys,
    cardinalityPct = cardPct,
    keyType = keyType.typeName,
    avgKeyBytes = keyType.avgBytes * config.numKeyColumns,
    memoryMB = memMB,
    distribution = dist,
    isPrimaryKey = isPK
  )
}

// ============================================================================
// Benchmark Execution with Error Handling
// ============================================================================

def strategyFromString(s: String): JoinStrategySpec = s match {
  case "hash_object" => HashObjectStrategy
  case "hash_direct" => HashDirectStrategy
  case "sort" => SortObjectWithPostStrategy
  case _ => throw new IllegalArgumentException(s"Unknown strategy: $s")
}

def buildSideFromString(s: String): BuildSideSpec = s match {
  case "left" => LeftBuild
  case "right" => RightBuild
  case "auto" => AutoPickSmallerIfAllowed
  case _ => throw new IllegalArgumentException(s"Unknown build side: $s")
}

def runSingleBenchmark(
  config: MLTestConfig,
  strategy: String,
  buildSideConfig: String,
  useDistinctJoin: Boolean,
  useKeyRemapping: Boolean,
  allowBuildSideSwap: Boolean
): MLBenchmarkResult = {
  
  val testName = s"${config.name}_${strategy}_${buildSideConfig}_" +
                 s"dist${if (useDistinctJoin) "Y" else "N"}_" +
                 s"remap${if (useKeyRemapping) "Y" else "N"}_" +
                 s"swap${if (allowBuildSideSwap) "Y" else "N"}"
  
  val leftStats = collectTableStats(config, "left")
  val rightStats = collectTableStats(config, "right")
  
  try {
    val keyIndices = (0 until config.numKeyColumns).toSeq
    
    val benchConfig = JoinBenchmarkConfig(
      testName = testName,
      leftParquetPath = s"$baseDir/${config.name}/left",
      rightParquetPath = s"$baseDir/${config.name}/right",
      joinType = InnerJoin,
      joinStrategy = strategyFromString(strategy),
      buildSide = buildSideFromString(buildSideConfig),
      optimizations = JoinOptimizations(
        allowBuildSideSwap = allowBuildSideSwap,
        remapComplexKeysToInts = useKeyRemapping,
        useDistinctJoin = useDistinctJoin,
        cacheJoinObject = false,  // No caching per requirements
        cacheRemapping = false,
        cacheDistinctFlag = false
      ),
      conditionalFilter = None,
      leftKeyIndices = keyIndices,
      rightKeyIndices = keyIndices,
      iterations = benchmarkIterations,
      numThreads = 1,
      printHeader = false
    )
    
    // Warmup
    val warmupConfig = benchConfig.copy(iterations = 1)
    runBenchmark(warmupConfig, spark)
    
    // Actual benchmark
    val result = runBenchmark(benchConfig, spark)
    
    MLBenchmarkResult(
      testName = testName,
      joinStrategy = strategy,
      buildSide = buildSideConfig,
      useDistinctJoin = useDistinctJoin,
      useKeyRemapping = useKeyRemapping,
      allowBuildSideSwap = allowBuildSideSwap,
      leftStats = leftStats,
      rightStats = rightStats,
      numKeyColumns = config.numKeyColumns,
      keyOverlapPct = config.keyOverlapPct,
      status = "SUCCESS",
      medianTimeMs = result.medianMs,
      avgTimeMs = result.averageMs,
      stdDevMs = result.stdDevMs,
      outputRows = result.outputRows,
      actualBuildSide = result.actualBuildSide.getOrElse("UNKNOWN"),
      iterations = benchmarkIterations
    )
    
  } catch {
    case e: Exception =>
      println(s"    FAILED: ${e.getMessage}")
      MLBenchmarkResult(
        testName = testName,
        joinStrategy = strategy,
        buildSide = buildSideConfig,
        useDistinctJoin = useDistinctJoin,
        useKeyRemapping = useKeyRemapping,
        allowBuildSideSwap = allowBuildSideSwap,
        leftStats = leftStats,
        rightStats = rightStats,
        numKeyColumns = config.numKeyColumns,
        keyOverlapPct = config.keyOverlapPct,
        status = "FAILED",
        medianTimeMs = 0.0,
        avgTimeMs = 0.0,
        stdDevMs = 0.0,
        outputRows = 0,
        actualBuildSide = "UNKNOWN",
        iterations = 0,
        errorMessage = e.getMessage
      )
  }
}

def runAllStrategiesForConfig(config: MLTestConfig): Seq[MLBenchmarkResult] = {
  val strategies = Seq("hash_object", "hash_direct", "sort")
  val distinctJoinOptions = Seq(false, true)
  val remappingOptions = Seq(false, true)
  
  // Always use auto build side with swapping enabled for apples-to-apples comparison:
  // - hash_direct already optimizes build side internally in cuDF for inner joins
  // - hash_object benefits from picking the smaller side
  // - Removes unnecessary test matrix dimension
  val buildSide = "auto"
  val useSwap = true
  
  val results = scala.collection.mutable.ArrayBuffer[MLBenchmarkResult]()
  
  // Test all combinations
  for {
    strategy <- strategies
    useDistinct <- distinctJoinOptions
    useRemap <- remappingOptions
  } {
    // Skip redundant combinations
    if (strategy == "sort" && useDistinct) {
      // Distinct join only applies to hash joins
      // Skip this combination
    } else {
      println(s"  Running: ${strategy}, distinct=${useDistinct}, remap=${useRemap}")
      val result = runSingleBenchmark(config, strategy, buildSide, useDistinct, useRemap, useSwap)
      results += result
    }
  }
  
  results.toSeq
}

// ============================================================================
// TSV Output
// ============================================================================

def initializeTSV(path: String): Unit = {
  val file = new File(path)
  file.getParentFile.mkdirs()
  val writer = new PrintWriter(file)
  
  // Write header
  writer.println(Seq(
    "TestName",
    "Status",
    "JoinStrategy",
    "BuildSideConfig",
    "ActualBuildSide",
    "UseDistinctJoin",
    "UseKeyRemapping",
    "AllowBuildSideSwap",
    // Left side stats
    "LeftRows",
    "LeftDistinctKeys",
    "LeftCardinalityPct",
    "LeftKeyType",
    "LeftAvgKeyBytes",
    "LeftMemoryMB",
    "LeftDistribution",
    "LeftIsPrimaryKey",
    // Right side stats
    "RightRows",
    "RightDistinctKeys",
    "RightCardinalityPct",
    "RightKeyType",
    "RightAvgKeyBytes",
    "RightMemoryMB",
    "RightDistribution",
    "RightIsPrimaryKey",
    // Relationship
    "NumKeyColumns",
    "KeyOverlapPct",
    // Results
    "MedianTimeMs",
    "AvgTimeMs",
    "StdDevMs",
    "OutputRows",
    "Iterations",
    "ErrorMessage"
  ).mkString("\t"))
  
  writer.close()
}

def appendResultToTSV(path: String, result: MLBenchmarkResult): Unit = {
  val writer = new PrintWriter(new java.io.FileWriter(path, true))
  
  writer.println(Seq(
    result.testName,
    result.status,
    result.joinStrategy,
    result.buildSide,
    result.actualBuildSide,
    result.useDistinctJoin,
    result.useKeyRemapping,
    result.allowBuildSideSwap,
    // Left side
    result.leftStats.rows,
    result.leftStats.distinctKeys,
    f"${result.leftStats.cardinalityPct}%.4f",
    result.leftStats.keyType,
    result.leftStats.avgKeyBytes,
    f"${result.leftStats.memoryMB}%.2f",
    result.leftStats.distribution,
    result.leftStats.isPrimaryKey,
    // Right side
    result.rightStats.rows,
    result.rightStats.distinctKeys,
    f"${result.rightStats.cardinalityPct}%.4f",
    result.rightStats.keyType,
    result.rightStats.avgKeyBytes,
    f"${result.rightStats.memoryMB}%.2f",
    result.rightStats.distribution,
    result.rightStats.isPrimaryKey,
    // Relationship
    result.numKeyColumns,
    f"${result.keyOverlapPct}%.2f",
    // Results
    f"${result.medianTimeMs}%.4f",
    f"${result.avgTimeMs}%.4f",
    f"${result.stdDevMs}%.4f",
    result.outputRows,
    result.iterations,
    result.errorMessage.replace("\t", " ").replace("\n", " ")
  ).mkString("\t"))
  
  writer.close()
}

// ============================================================================
// Adaptive Exploration - Smart Refinement
// ============================================================================

case class MetricCorrelation(
  metricName: String,
  metricDescription: String,
  correlationCoef: Double,      // -1 to 1
  affectsStrategy: String,      // Which comparison this affects
  isPositive: Boolean          // Higher metric → better performance?
)

case class CrossoverPoint(
  metric: String,
  value: Double,
  strategy1: String,           // Better below this value
  strategy2: String,           // Better above this value
  confidence: Double,          // 0-1, how confident are we?
  sampleCount: Int            // How many samples near this point
)

case class TestGap(
  metric: String,
  value: Double,               // Missing value
  keyType: String,
  numKeys: Int,
  importance: Double           // How important to fill this gap
)

// Calculate correlation coefficient between a metric and performance difference
def calculateCorrelation(
  results: Seq[MLBenchmarkResult],
  metricExtractor: MLBenchmarkResult => Double,
  strategy1: String,
  strategy2: String
): Double = {
  // Find pairs of results with same config but different strategies
  val pairs = results.groupBy(r => (r.leftStats.rows, r.rightStats.rows, r.leftStats.distinctKeys))
    .values
    .flatMap { group =>
      val s1 = group.find(_.joinStrategy == strategy1)
      val s2 = group.find(_.joinStrategy == strategy2)
      (s1, s2) match {
        case (Some(r1), Some(r2)) =>
          Some((metricExtractor(r1), r2.medianTimeMs - r1.medianTimeMs))
        case _ => None
      }
    }
    .toSeq
  
  if (pairs.size < 3) return 0.0  // Not enough data
  
  val xs = pairs.map(_._1)
  val ys = pairs.map(_._2)
  
  val meanX = xs.sum / xs.size
  val meanY = ys.sum / ys.size
  
  val numerator = (xs zip ys).map { case (x, y) => (x - meanX) * (y - meanY) }.sum
  val denomX = math.sqrt(xs.map(x => math.pow(x - meanX, 2)).sum)
  val denomY = math.sqrt(ys.map(y => math.pow(y - meanY, 2)).sum)
  
  if (denomX == 0 || denomY == 0) 0.0
  else numerator / (denomX * denomY)
}

// Find metrics that strongly correlate with strategy performance
def findCorrelations(results: Seq[MLBenchmarkResult]): Seq[MetricCorrelation] = {
  val successfulResults = results.filter(_.status == "SUCCESS")
  if (successfulResults.size < 10) return Seq.empty
  
  val correlations = scala.collection.mutable.ArrayBuffer[MetricCorrelation]()
  
  // Metrics to check - keeping build and probe sides separate
  val metrics: Seq[(String, String, MLBenchmarkResult => Double)] = Seq(
    // Build side metrics (most important)
    ("build_cardinality", "Build side cardinality %", (r: MLBenchmarkResult) => {
      val value: Double = if (r.actualBuildSide == "Left") r.leftStats.cardinalityPct else r.rightStats.cardinalityPct
      value
    }),
    ("build_rows", "Build side row count", (r: MLBenchmarkResult) => {
      val value: Double = if (r.actualBuildSide == "Left") r.leftStats.rows.toDouble else r.rightStats.rows.toDouble
      value
    }),
    ("build_avg_key_bytes", "Build side avg key bytes", (r: MLBenchmarkResult) => {
      val value: Double = if (r.actualBuildSide == "Left") r.leftStats.avgKeyBytes else r.rightStats.avgKeyBytes
      value
    }),
    ("build_num_keys", "Build side num key columns", (r: MLBenchmarkResult) => r.numKeyColumns.toDouble),
    
    // Probe side metrics
    ("probe_cardinality", "Probe side cardinality %", (r: MLBenchmarkResult) => {
      val value: Double = if (r.actualBuildSide == "Left") r.rightStats.cardinalityPct else r.leftStats.cardinalityPct
      value
    }),
    ("probe_rows", "Probe side row count", (r: MLBenchmarkResult) => {
      val value: Double = if (r.actualBuildSide == "Left") r.rightStats.rows.toDouble else r.leftStats.rows.toDouble
      value
    }),
    ("probe_avg_key_bytes", "Probe side avg key bytes", (r: MLBenchmarkResult) => {
      val value: Double = if (r.actualBuildSide == "Left") r.rightStats.avgKeyBytes else r.leftStats.avgKeyBytes
      value
    }),
    
    // Combined metrics (for completeness)
    ("total_rows", "Total rows (both sides)", (r: MLBenchmarkResult) => 
      (r.leftStats.rows + r.rightStats.rows).toDouble),
    ("avg_key_bytes", "Average key bytes (both sides)", (r: MLBenchmarkResult) =>
      (r.leftStats.avgKeyBytes + r.rightStats.avgKeyBytes) / 2.0),
    ("row_ratio", "Row count ratio (left/right)", (r: MLBenchmarkResult) =>
      r.leftStats.rows.toDouble / math.max(1, r.rightStats.rows).toDouble)
  )
  
  // Strategy comparisons to check
  val strategyPairs = Seq(
    ("hash_object", "hash_direct"),
    ("hash_object", "sort"),
    ("hash_direct", "sort")
  )
  
  for {
    (metricName, metricDesc, extractor) <- metrics
    (s1, s2) <- strategyPairs
  } {
    val corr = calculateCorrelation(successfulResults, extractor, s1, s2)
    if (math.abs(corr) > 0.3) {  // Meaningful correlation
      correlations += MetricCorrelation(
        metricName,
        metricDesc,
        corr,
        s"$s1 vs $s2",
        corr < 0  // Negative correlation means higher metric → s1 faster
      )
    }
  }
  
  correlations.toSeq.sortBy((c: MetricCorrelation) => -math.abs(c.correlationCoef))
}

// Find crossover points where one strategy becomes better than another
def findCrossoverPoints(results: Seq[MLBenchmarkResult]): Seq[CrossoverPoint] = {
  val successfulResults = results.filter(_.status == "SUCCESS")
  if (successfulResults.size < 10) return Seq.empty
  
  val crossovers = scala.collection.mutable.ArrayBuffer[CrossoverPoint]()
  
  // Focus on build side cardinality as main crossover metric for hash vs sort
  val hashResults = successfulResults.filter(r => r.joinStrategy == "hash_object" || r.joinStrategy == "hash_direct")
  val sortResults = successfulResults.filter(_.joinStrategy == "sort")
  
  if (hashResults.nonEmpty && sortResults.nonEmpty) {
    // 1. Check cardinality crossovers
    val cardinalityBuckets = Seq(0.01, 0.02, 0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.40, 0.50, 0.75, 1.0)
    
    var previousWinner: Option[String] = None
    
    cardinalityBuckets.sliding(2).foreach { case Seq(low, high) =>
      val hashInRange = hashResults.filter { r =>
        val card = if (r.actualBuildSide == "Left") r.leftStats.cardinalityPct else r.rightStats.cardinalityPct
        card >= low && card < high
      }
      val sortInRange = sortResults.filter { r =>
        val card = if (r.actualBuildSide == "Left") r.leftStats.cardinalityPct else r.rightStats.cardinalityPct
        card >= low && card < high
      }
      
      if (hashInRange.nonEmpty && sortInRange.nonEmpty) {
        val avgHash = hashInRange.map(_.medianTimeMs).sum / hashInRange.size
        val avgSort = sortInRange.map(_.medianTimeMs).sum / sortInRange.size
        val currentWinner = if (avgHash < avgSort) "hash" else "sort"
        
        // Check if winner changed
        if (previousWinner.isDefined && previousWinner.get != currentWinner) {
          val crossoverValue = (low + high) / 2
          val sampleCount = hashInRange.size + sortInRange.size
          val confidence = math.min(1.0, sampleCount / 10.0)
          
          crossovers += CrossoverPoint(
            "build_cardinality",
            crossoverValue,
            previousWinner.get,
            currentWinner,
            confidence,
            sampleCount
          )
        }
        previousWinner = Some(currentWinner)
      }
    }
    
    // 2. Check build side row count crossovers
    val rowBuckets = Seq(1000L, 5000L, 10000L, 50000L, 100000L, 500000L, 1000000L, 5000000L, 10000000L)
    previousWinner = None
    
    rowBuckets.sliding(2).foreach { case Seq(low, high) =>
      val hashInRange = hashResults.filter { r =>
        val buildRows = if (r.actualBuildSide == "Left") r.leftStats.rows else r.rightStats.rows
        buildRows >= low && buildRows < high
      }
      val sortInRange = sortResults.filter { r =>
        val buildRows = if (r.actualBuildSide == "Left") r.leftStats.rows else r.rightStats.rows
        buildRows >= low && buildRows < high
      }
      
      if (hashInRange.size >= 3 && sortInRange.size >= 3) {  // Need more samples for row count
        val avgHash = hashInRange.map(_.medianTimeMs).sum / hashInRange.size
        val avgSort = sortInRange.map(_.medianTimeMs).sum / sortInRange.size
        val currentWinner = if (avgHash < avgSort) "hash" else "sort"
        
        if (previousWinner.isDefined && previousWinner.get != currentWinner) {
          val crossoverValue = ((low + high) / 2).toDouble
          val sampleCount = hashInRange.size + sortInRange.size
          val confidence = math.min(1.0, sampleCount / 15.0)
          
          crossovers += CrossoverPoint(
            "build_rows",
            crossoverValue,
            previousWinner.get,
            currentWinner,
            confidence,
            sampleCount
          )
        }
        previousWinner = Some(currentWinner)
      }
    }
  }
  
  crossovers.toSeq
}

// Identify gaps in test coverage that should be filled
def identifyGaps(
  results: Seq[MLBenchmarkResult],
  crossovers: Seq[CrossoverPoint]
): Seq[TestGap] = {
  val gaps = scala.collection.mutable.ArrayBuffer[TestGap]()
  
  // For each crossover, find gaps in coverage
  crossovers.foreach { crossover =>
    val nearbyRange = crossover.metric match {
      case "build_cardinality" | "probe_cardinality" =>
        (crossover.value * 0.8, crossover.value * 1.2)
      case "build_rows" | "probe_rows" =>
        (crossover.value * 0.7, crossover.value * 1.3)
      case _ =>
        (crossover.value * 0.8, crossover.value * 1.2)
    }
    
    // Check what key types and column counts we have near the crossover
    val nearbyTests = crossover.metric match {
      case "build_cardinality" =>
        results.filter { r =>
          val card = if (r.actualBuildSide == "Left") r.leftStats.cardinalityPct else r.rightStats.cardinalityPct
          card >= nearbyRange._1 && card <= nearbyRange._2
        }
      case "build_rows" =>
        results.filter { r =>
          val buildRows = if (r.actualBuildSide == "Left") r.leftStats.rows else r.rightStats.rows
          buildRows >= nearbyRange._1 && buildRows <= nearbyRange._2
        }
      case _ =>
        results.filter { r =>
          val card = if (r.actualBuildSide == "Left") r.leftStats.cardinalityPct else r.rightStats.cardinalityPct
          card >= nearbyRange._1 && card <= nearbyRange._2
        }
    }
    
    val keyTypes = nearbyTests.map(_.leftStats.keyType).distinct
    val numKeysList = nearbyTests.map(_.numKeyColumns).distinct
    
    // All key types we should test (prioritize common ones)
    val allKeyTypes = Seq("int", "long", "decimal(18,2)", "decimal(38,2)", "string_medium", "string_short")
    val allNumKeys = Seq(1, 2, 3)
    
    // Find missing combinations
    for {
      keyType <- allKeyTypes
      numKeys <- allNumKeys
      if !nearbyTests.exists(t => t.leftStats.keyType == keyType && t.numKeyColumns == numKeys)
    } {
      // Importance: high confidence + low samples = more important
      // Also boost importance for single column keys (more common)
      val baseImportance = crossover.confidence * (1.0 / math.max(1, crossover.sampleCount))
      val keyColumnBoost = if (numKeys == 1) 1.5 else if (numKeys == 2) 1.2 else 1.0
      val importance = baseImportance * keyColumnBoost
      
      gaps += TestGap(
        crossover.metric,
        crossover.value,
        keyType,
        numKeys,
        importance
      )
    }
  }
  
  gaps.toSeq.sortBy(-_.importance)
}

def analyzeResults(results: Seq[MLBenchmarkResult]): (Seq[MetricCorrelation], Seq[CrossoverPoint], Seq[TestGap]) = {
  println("\n" + "="*80)
  println("ANALYZING RESULTS FOR CORRELATIONS AND CROSSOVERS")
  println("="*80)
  
  val successfulResults = results.filter(_.status == "SUCCESS")
  
  if (successfulResults.isEmpty) {
    println("No successful results to analyze!")
    return (Seq.empty, Seq.empty, Seq.empty)
  }
  
  println(s"\nAnalyzing ${successfulResults.size} successful results...")
  
  // Find correlations
  val correlations = findCorrelations(successfulResults)
  println(s"\nFound ${correlations.size} meaningful correlations (|r| > 0.3):")
  println()
  if (correlations.nonEmpty) {
    println("  ALL CORRELATIONS (sorted by strength):")
    correlations.foreach { corr =>
      val direction = if (corr.isPositive) "+" else "-"
      println(f"    ${corr.metricDescription}%-35s vs ${corr.affectsStrategy}%-25s: ${corr.correlationCoef}%+.3f ($direction)")
    }
  } else {
    println("    No strong correlations found (all |r| < 0.3)")
  }
  
  // Find crossover points
  val crossovers = findCrossoverPoints(successfulResults)
  println()
  println(s"\nFound ${crossovers.size} crossover points:")
  if (crossovers.nonEmpty) {
    crossovers.foreach { cross =>
      println(f"  ${cross.metric} @ ${cross.value}%.3f: ${cross.strategy1} → ${cross.strategy2} " +
              f"(confidence: ${cross.confidence}%.2f, samples: ${cross.sampleCount})")
    }
  } else {
    println("  No crossover points detected")
  }
  
  // Identify gaps
  val gaps = identifyGaps(successfulResults, crossovers)
  println()
  println(s"\nIdentified ${gaps.size} coverage gaps near crossover points:")
  if (gaps.nonEmpty) {
    println()
    println("  ALL GAPS (sorted by importance):")
    gaps.foreach { gap =>
      println(f"    ${gap.metric} @ ${gap.value}%.3f: ${gap.keyType}%-20s × ${gap.numKeys} cols (importance: ${gap.importance}%.4f)")
    }
  } else {
    println("  No coverage gaps found")
  }
  println()
  
  (correlations, crossovers, gaps)
}

def generateRefinementTests(
  correlations: Seq[MetricCorrelation],
  crossovers: Seq[CrossoverPoint],
  gaps: Seq[TestGap],
  existingConfigs: Seq[MLTestConfig],
  startId: Int
): Seq[MLTestConfig] = {
  
  println("\n" + "="*80)
  println("GENERATING SMART REFINEMENT TESTS")
  println("="*80)
  
  if (crossovers.isEmpty && gaps.isEmpty) {
    println("No crossovers or gaps found, generating random refinements")
    return (0 until numRefinementTests).map(i => generateRandomTestConfig(startId + i))
  }
  
  val refinements = scala.collection.mutable.ArrayBuffer[MLTestConfig]()
  
  // Strategy 1: Fill important gaps (40% of tests)
  val gapTests = (numRefinementTests * 0.4).toInt
  println(s"\nGenerating $gapTests tests to fill coverage gaps...")
  gaps.take(gapTests).foreach { gap =>
    val config = generateRandomTestConfig(startId + refinements.size)
    
    // Adjust to target the gap based on metric type
    val adjusted = gap.metric match {
      case "build_cardinality" =>
        val targetCard = gap.value
        val newLeftDistinct = (config.leftRows * targetCard).toLong
        val newRightDistinct = (config.rightRows * targetCard).toLong
        val gapKeyType = keyTypes.find(_.typeName == gap.keyType).getOrElse(pickRandomKeyType())
        
        config.copy(
          leftKeyType = gapKeyType,
          rightKeyType = gapKeyType,
          leftDistinctKeys = newLeftDistinct,
          rightDistinctKeys = newRightDistinct,
          numKeyColumns = gap.numKeys
        )
        
      case "build_rows" =>
        val targetRows = gap.value.toLong
        val gapKeyType = keyTypes.find(_.typeName == gap.keyType).getOrElse(pickRandomKeyType())
        
        // Adjust one side to have approximately the target row count
        // Keep the other side proportional
        val ratio = config.rightRows.toDouble / math.max(1, config.leftRows).toDouble
        val newLeftRows = targetRows
        val newRightRows = (targetRows * ratio).toLong
        
        config.copy(
          leftKeyType = gapKeyType,
          rightKeyType = gapKeyType,
          leftRows = newLeftRows,
          rightRows = newRightRows,
          leftDistinctKeys = (newLeftRows * config.leftDistinctKeys.toDouble / math.max(1, config.leftRows)).toLong,
          rightDistinctKeys = (newRightRows * config.rightDistinctKeys.toDouble / math.max(1, config.rightRows)).toLong,
          numKeyColumns = gap.numKeys
        )
        
      case _ =>
        val targetCard = gap.value
        val newLeftDistinct = (config.leftRows * targetCard).toLong
        val newRightDistinct = (config.rightRows * targetCard).toLong
        val gapKeyType = keyTypes.find(_.typeName == gap.keyType).getOrElse(pickRandomKeyType())
        
        config.copy(
          leftKeyType = gapKeyType,
          rightKeyType = gapKeyType,
          leftDistinctKeys = newLeftDistinct,
          rightDistinctKeys = newRightDistinct,
          numKeyColumns = gap.numKeys
        )
    }
    
    refinements += adjusted
    println(f"  Generated test @ ${gap.metric}: ${gap.value}%.3f, ${gap.keyType}, ${gap.numKeys} cols")
  }
  
  // Strategy 2: Dense sampling around crossover points (40% of tests)
  val crossoverTests = (numRefinementTests * 0.4).toInt
  println(s"\nGenerating $crossoverTests tests around crossover points...")
  crossovers.foreach { crossover =>
    val testsPerCrossover = crossoverTests / math.max(1, crossovers.size)
    
    println(f"\n  Crossover at ${crossover.metric} = ${crossover.value}%.3f (${crossover.strategy1} → ${crossover.strategy2}):")
    
    // Generate tests densely around the crossover
    for (i <- 0 until testsPerCrossover) {
      val config = generateRandomTestConfig(startId + refinements.size)
      
      val adjusted = crossover.metric match {
        case "build_cardinality" =>
          // Vary cardinality ±20% around crossover point
          val variation = (random.nextDouble() - 0.5) * 0.4  // ±20%
          val targetCard = math.max(0.01, math.min(1.0, crossover.value + variation * crossover.value))
          
          val newLeftDistinct = (config.leftRows * targetCard).toLong
          val newRightDistinct = (config.rightRows * targetCard).toLong
          
          val result = config.copy(
            leftDistinctKeys = newLeftDistinct,
            rightDistinctKeys = newRightDistinct
          )
          
          println(f"    Test @ ${targetCard}%.3f cardinality")
          result
          
        case "build_rows" =>
          // Vary row count ±30% around crossover point
          val variation = (random.nextDouble() - 0.5) * 0.6  // ±30%
          val targetRows = math.max(1000, (crossover.value + variation * crossover.value).toLong)
          
          val ratio = config.rightRows.toDouble / math.max(1, config.leftRows).toDouble
          val newLeftRows = targetRows
          val newRightRows = (targetRows * ratio).toLong
          
          val result = config.copy(
            leftRows = newLeftRows,
            rightRows = newRightRows,
            leftDistinctKeys = (newLeftRows * config.leftDistinctKeys.toDouble / math.max(1, config.leftRows)).toLong,
            rightDistinctKeys = (newRightRows * config.rightDistinctKeys.toDouble / math.max(1, config.rightRows)).toLong
          )
          
          println(f"    Test @ ${targetRows} rows")
          result
          
        case _ =>
          val variation = (random.nextDouble() - 0.5) * 0.4
          val targetCard = math.max(0.01, math.min(1.0, crossover.value + variation * crossover.value))
          
          val newLeftDistinct = (config.leftRows * targetCard).toLong
          val newRightDistinct = (config.rightRows * targetCard).toLong
          
          val result = config.copy(
            leftDistinctKeys = newLeftDistinct,
            rightDistinctKeys = newRightDistinct
          )
          
          println(f"    Test @ ${targetCard}%.3f")
          result
      }
      
      refinements += adjusted
    }
  }
  
  // Strategy 3: Explore highly correlated metrics (20% of tests)
  val correlationTests = numRefinementTests - refinements.size
  println(s"\nGenerating $correlationTests tests exploring correlated metrics...")
  
  if (correlations.nonEmpty) {
    val topCorrelations = correlations.sortBy((c: MetricCorrelation) => -math.abs(c.correlationCoef)).take(3)
    topCorrelations.foreach { corr =>
      println(f"  Exploring ${corr.metricDescription} (corr: ${corr.correlationCoef}%+.3f)")
    }
  }
  
  // Fill remaining with random tests
  while (refinements.size < numRefinementTests) {
    refinements += generateRandomTestConfig(startId + refinements.size)
  }
  
  println(s"\nGenerated ${refinements.size} total refinement tests")
  refinements.toSeq
}

// ============================================================================
// Main Execution
// ============================================================================

println("="*80)
println("ML JOIN OPTIMIZATION TRAINING DATA GENERATION")
println("="*80)
println()
println(s"Initial tests: $numInitialTests")
println(s"Refinement tests: $numRefinementTests")
println(s"Iterations per test: $benchmarkIterations")
println(s"Memory target: ${minMemoryTarget / (1024*1024)}MB - ${maxMemoryTarget / (1024*1024)}MB")
println(s"Output TSV: $outputTsvPath")
println()

// Initialize TSV output
initializeTSV(outputTsvPath)

// ============================================================================
// Helper: Run Config with Cleanup
// ============================================================================

def runConfigWithCleanup(config: MLTestConfig, configNum: Int, totalConfigs: Int): Seq[MLBenchmarkResult] = {
  println(s"\n[${configNum}/${totalConfigs}] Processing: ${config.name}")
  val startTime = System.nanoTime()
  
  // Generate data for this config only
  println(s"  Generating data...")
  val genStartTime = System.nanoTime()
  generateTestData(config)
  val genTimeMs = (System.nanoTime() - genStartTime) / 1e6
  println(f"  Data generation took: ${genTimeMs}%.2f ms")
  
  // Run all strategy variants
  println(s"  Running benchmarks...")
  val benchStartTime = System.nanoTime()
  val results = runAllStrategiesForConfig(config)
  val benchTimeMs = (System.nanoTime() - benchStartTime) / 1e6
  
  // Write results to TSV immediately
  results.foreach(r => appendResultToTSV(outputTsvPath, r))
  
  // Print summary
  val successCount = results.count(_.status == "SUCCESS")
  val failCount = results.count(_.status == "FAILED")
  println(s"  Results: $successCount successful, $failCount failed")
  println(f"  Benchmark time: ${benchTimeMs}%.2f ms (${benchTimeMs/1000}%.2f sec)")
  
  // Cleanup: delete the data directories
  try {
    println(s"  Cleaning up data...")
    import java.nio.file.{Files, Paths, Path}
    import java.io.File
    
    // Helper to recursively delete a directory
    def deleteDirectory(directory: File): Unit = {
      if (directory.exists()) {
        val files = directory.listFiles()
        if (files != null) {
          files.foreach { file =>
            if (file.isDirectory) {
              deleteDirectory(file)
            } else {
              file.delete()
            }
          }
        }
        directory.delete()
      }
    }
    
    val leftDir = new File(s"$baseDir/${config.name}/left")
    val rightDir = new File(s"$baseDir/${config.name}/right")
    val configDir = new File(s"$baseDir/${config.name}")
    
    deleteDirectory(leftDir)
    deleteDirectory(rightDir)
    deleteDirectory(configDir) // Will remove if empty
    
    println(s"  Cleanup complete")
  } catch {
    case e: Exception =>
      println(s"  Warning: Cleanup failed: ${e.getMessage}")
      // Continue anyway - not a critical error
  }
  
  val totalTimeMs = (System.nanoTime() - startTime) / 1e6
  println(f"  Total config time: ${totalTimeMs}%.2f ms (${totalTimeMs/1000}%.2f sec)")
  
  results
}

// ============================================================================
// Phase 1: Initial Random Sampling
// ============================================================================

println("="*80)
println("PHASE 1: INITIAL RANDOM SAMPLING")
println("="*80)
println()

val initialConfigs = generateInitialTestConfigs()

println(s"Generated ${initialConfigs.size} initial test configurations")
println(s"Processing configs one at a time (generate → test → cleanup)")
println()

// Process configs one at a time
val phase1StartTime = System.nanoTime()
val initialResults = initialConfigs.zipWithIndex.flatMap { case (config, idx) =>
  val results = runConfigWithCleanup(config, idx + 1, initialConfigs.size)
  
  // Print estimated time remaining
  val elapsedSec = (System.nanoTime() - phase1StartTime) / 1e9
  val avgSecPerConfig = elapsedSec / (idx + 1)
  val remainingConfigs = initialConfigs.size - (idx + 1)
  val estimatedRemainingSec = avgSecPerConfig * remainingConfigs
  val estimatedRemainingMin = estimatedRemainingSec / 60
  
  if (remainingConfigs > 0) {
    println(f"  Estimated time remaining: ${estimatedRemainingMin}%.1f minutes ($remainingConfigs configs left)")
  }
  
  results
}

val phase1ElapsedSec = (System.nanoTime() - phase1StartTime) / 1e9
val phase1ElapsedMin = phase1ElapsedSec / 60
println(s"\nPhase 1 complete: ${initialResults.size} total benchmark runs")
val phase1Success = initialResults.count(_.status == "SUCCESS")
val phase1Failed = initialResults.count(_.status == "FAILED")
println(s"  Success: $phase1Success")
println(s"  Failed: $phase1Failed")
println(f"  Phase 1 total time: ${phase1ElapsedMin}%.2f minutes")

// ============================================================================
// Phase 2: Adaptive Refinement
// ============================================================================

{
  println("\n" + "="*80)
  println("PHASE 2: ADAPTIVE REFINEMENT")
  println("="*80)
  println()

  val (correlations, crossovers, gaps) = analyzeResults(initialResults)
  val refinementConfigs = generateRefinementTests(correlations, crossovers, gaps, initialConfigs, numInitialTests)

  println(s"\nGenerated ${refinementConfigs.size} refinement test configurations")
  println(s"Processing configs one at a time (generate → test → cleanup)")
  println()

  // Process refinement configs one at a time
  val phase2StartTime = System.nanoTime()
  val refinementResults = refinementConfigs.zipWithIndex.flatMap { case (config, idx) =>
    val results = runConfigWithCleanup(config, idx + 1, refinementConfigs.size)
    
    // Print estimated time remaining
    val elapsedSec = (System.nanoTime() - phase2StartTime) / 1e9
    val avgSecPerConfig = elapsedSec / (idx + 1)
    val remainingConfigs = refinementConfigs.size - (idx + 1)
    val estimatedRemainingSec = avgSecPerConfig * remainingConfigs
    val estimatedRemainingMin = estimatedRemainingSec / 60
    
    if (remainingConfigs > 0) {
      println(f"  Estimated time remaining: ${estimatedRemainingMin}%.1f minutes ($remainingConfigs configs left)")
    }
    
    results
  }

  val phase2ElapsedSec = (System.nanoTime() - phase2StartTime) / 1e9
  val phase2ElapsedMin = phase2ElapsedSec / 60
  println(s"\nPhase 2 complete: ${refinementResults.size} total benchmark runs")
  val phase2Success = refinementResults.count(_.status == "SUCCESS")
  val phase2Failed = refinementResults.count(_.status == "FAILED")
  println(s"  Success: $phase2Success")
  println(s"  Failed: $phase2Failed")
  println(f"  Phase 2 total time: ${phase2ElapsedMin}%.2f minutes")

  // ============================================================================
  // Final Analysis
  // ============================================================================

  println("\n" + "="*80)
  println("FINAL ANALYSIS")
  println("="*80)
  println()

  val allResults = initialResults ++ refinementResults
  val totalSuccess = allResults.count(_.status == "SUCCESS")
  val totalFailed = allResults.count(_.status == "FAILED")

  println(s"Total benchmark runs: ${allResults.size}")
  println(s"  Successful: $totalSuccess")
  println(s"  Failed: $totalFailed")
  println(s"  Success rate: ${(totalSuccess.toDouble / allResults.size * 100).toInt}%")
  println()
  
  // Detailed test statistics
  val successfulResults = allResults.filter(_.status == "SUCCESS")
  
  if (successfulResults.nonEmpty) {
    println("TEST STATISTICS:")
    println()
    
    // By key type
    println("By Key Type:")
    val byKeyType = successfulResults.groupBy(_.leftStats.keyType)
    byKeyType.toSeq.sortBy(_._1).foreach { case (keyType, tests) =>
      println(f"  $keyType%-20s: ${tests.size}%5d tests")
    }
    println()
    
    // By number of key columns
    println("By Number of Key Columns:")
    val byNumKeys = successfulResults.groupBy(_.numKeyColumns)
    byNumKeys.toSeq.sortBy(_._1).foreach { case (numKeys, tests) =>
      val label = s"$numKeys columns"
      println(f"  $label%-15s: ${tests.size}%5d tests")
    }
    println()
    
    // By join strategy
    println("By Join Strategy:")
    val byStrategy = successfulResults.groupBy(_.joinStrategy)
    byStrategy.toSeq.sortBy(_._1).foreach { case (strategy, tests) =>
      println(f"  $strategy%-20s: ${tests.size}%5d tests")
    }
    println()
    
    // By cardinality range
    println("By Cardinality (build side):")
    val cardinalityRanges = Seq(
      ("Very Low (<5%%)", 0.0, 0.05),
      ("Low (5-15%%)", 0.05, 0.15),
      ("Medium (15-40%%)", 0.15, 0.40),
      ("High (40-70%%)", 0.40, 0.70),
      ("Very High (>70%%)", 0.70, 1.0)
    )
    cardinalityRanges.foreach { case (label, min, max) =>
      val count = successfulResults.count { r =>
        val buildSideCard = if (r.actualBuildSide == "Left") r.leftStats.cardinalityPct else r.rightStats.cardinalityPct
        buildSideCard >= min && buildSideCard < max
      }
      println(f"  $label%-25s: ${count}%5d tests")
    }
    println()
    
    // By data size
    println("By Data Size (total both sides):")
    val sizeRanges = Seq(
      ("Tiny (<10 MiB)", 0.0, 10.0),
      ("Small (10-25 MiB)", 10.0, 25.0),
      ("Medium (25-50 MiB)", 25.0, 50.0),
      ("Large (50-100 MiB)", 50.0, 100.0),
      ("Very Large (>100 MiB)", 100.0, Double.MaxValue)
    )
    sizeRanges.foreach { case (label, min, max) =>
      val count = successfulResults.count { r =>
        val totalMB = r.leftStats.memoryMB + r.rightStats.memoryMB
        totalMB >= min && totalMB < max
      }
      println(f"  $label%-25s: ${count}%5d tests")
    }
    println()
    
    // Distribution statistics for strategy comparisons
    println("PERFORMANCE DISTRIBUTION STATISTICS:")
    println()
    
    println("By Join Strategy (timing distribution):")
    byStrategy.toSeq.sortBy(_._1).foreach { case (strategy, tests) =>
      val times = tests.map(_.medianTimeMs)
      if (times.nonEmpty) {
        val sorted = times.sorted
        val min = sorted.head
        val max = sorted.last
        val avg = times.sum / times.size
        val median = if (sorted.size % 2 == 0) {
          (sorted(sorted.size / 2 - 1) + sorted(sorted.size / 2)) / 2.0
        } else {
          sorted(sorted.size / 2)
        }
        val variance = times.map(t => math.pow(t - avg, 2)).sum / times.size
        val stdDev = math.sqrt(variance)
        val p25 = sorted((sorted.size * 0.25).toInt)
        val p75 = sorted((sorted.size * 0.75).toInt)
        
        println(f"  $strategy%-20s:")
        println(f"    Min: ${min}%8.2f ms  | P25: ${p25}%8.2f ms | Median: ${median}%8.2f ms")
        println(f"    P75: ${p75}%8.2f ms  | Max: ${max}%8.2f ms | StdDev: ${stdDev}%8.2f ms")
      }
    }
    println()
    
    // Compare strategy pairs with distribution stats
    println("Strategy Pair Comparisons (for same configs):")
    
    // Group by config to find paired comparisons
    val configGroups = successfulResults.groupBy(r => 
      (r.leftStats.rows, r.rightStats.rows, r.leftStats.distinctKeys, r.numKeyColumns)
    )
    
    val strategyPairs = Seq(
      ("hash_object", "hash_direct", "Hash Object vs Direct"),
      ("hash_object", "sort", "Hash Object vs Sort"),
      ("hash_direct", "sort", "Hash Direct vs Sort")
    )
    
    strategyPairs.foreach { case (s1, s2, label) =>
      val diffs = configGroups.values.flatMap { group =>
        val r1 = group.find(_.joinStrategy == s1)
        val r2 = group.find(_.joinStrategy == s2)
        (r1, r2) match {
          case (Some(a), Some(b)) => Some(a.medianTimeMs - b.medianTimeMs)
          case _ => None
        }
      }.toSeq
      
      if (diffs.nonEmpty) {
        val sorted = diffs.sorted
        val avg = diffs.sum / diffs.size
        val median = if (sorted.size % 2 == 0) {
          (sorted(sorted.size / 2 - 1) + sorted(sorted.size / 2)) / 2.0
        } else {
          sorted(sorted.size / 2)
        }
        val variance = diffs.map(d => math.pow(d - avg, 2)).sum / diffs.size
        val stdDev = math.sqrt(variance)
        
        val s1Faster = diffs.count(_ < 0)
        val s2Faster = diffs.count(_ > 0)
        val same = diffs.count(_ == 0)
        
        println(f"  $label:")
        println(f"    Avg diff: ${avg}%+8.2f ms (${s1} - ${s2})")
        println(f"    Median:   ${median}%+8.2f ms | StdDev: ${stdDev}%8.2f ms")
        println(f"    ${s1} faster: ${s1Faster}%4d | ${s2} faster: ${s2Faster}%4d | Same: ${same}%4d")
      }
    }
    println()
  }
  
  if (successfulResults.nonEmpty) {
    println("QUICK HEURISTIC INSIGHTS:")
    println()
    
    // 1. Distinct join effectiveness - split by whether build side is actually distinct
    val withDistinct = successfulResults.filter(r => r.useDistinctJoin && r.joinStrategy.contains("hash"))
    val withoutDistinct = successfulResults.filter(r => !r.useDistinctJoin && r.joinStrategy.contains("hash"))
    
    if (withDistinct.nonEmpty && withoutDistinct.nonEmpty) {
      println(f"1. DISTINCT JOIN:")
      println()
      
      // Split by whether build side is actually distinct (distinctKeys == rows)
      val actuallyDistinct = withDistinct.filter { r =>
        val buildSideIsLeft = r.actualBuildSide == "Left"
        if (buildSideIsLeft) {
          r.leftStats.distinctKeys == r.leftStats.rows
        } else {
          r.rightStats.distinctKeys == r.rightStats.rows
        }
      }
      
      val notDistinct = withDistinct.filter { r =>
        val buildSideIsLeft = r.actualBuildSide == "Left"
        if (buildSideIsLeft) {
          r.leftStats.distinctKeys != r.leftStats.rows
        } else {
          r.rightStats.distinctKeys != r.rightStats.rows
        }
      }
      
      println(f"   When build side IS distinct (${actuallyDistinct.size} tests):")
      if (actuallyDistinct.nonEmpty) {
        val avgWith = actuallyDistinct.map(_.medianTimeMs).sum / actuallyDistinct.size
        val avgWithout = withoutDistinct.map(_.medianTimeMs).sum / withoutDistinct.size
        val benefit = ((avgWithout - avgWith) / avgWithout) * 100.0
        println(f"     Avg with distinct:    ${avgWith}%.2f ms")
        println(f"     Avg without distinct: ${avgWithout}%.2f ms")
        println(f"     Benefit: ${benefit}%+.1f%%")
        if (benefit > 5) {
          println("     ✓ Distinct join HELPS when build side is actually distinct")
        } else if (benefit < -5) {
          println("     ✗ Distinct join HURTS even when build side is distinct")
        } else {
          println("     ~ Minimal impact")
        }
      }
      
      println()
      println(f"   When build side is NOT distinct (${notDistinct.size} tests):")
      if (notDistinct.nonEmpty) {
        val avgWith = notDistinct.map(_.medianTimeMs).sum / notDistinct.size
        val avgWithout = withoutDistinct.map(_.medianTimeMs).sum / withoutDistinct.size
        val overhead = ((avgWith - avgWithout) / avgWithout) * 100.0
        println(f"     Avg with distinct:    ${avgWith}%.2f ms")
        println(f"     Avg without distinct: ${avgWithout}%.2f ms")
        println(f"     Overhead: ${overhead}%+.1f%%")
        if (overhead > 5) {
          println("     ✗ Distinct join adds overhead with non-distinct build side")
        } else {
          println("     ~ Minimal overhead")
        }
      }
      
      println()
      println("   → Use distinct join ONLY when build side is truly distinct (e.g., primary key)")
      println()
    }
    
    // 2. Key remapping effectiveness
    val withRemap = successfulResults.filter(_.useKeyRemapping)
    val withoutRemap = successfulResults.filter(!_.useKeyRemapping)
    
    if (withRemap.nonEmpty && withoutRemap.nonEmpty) {
      val avgWithRemap = withRemap.map(_.medianTimeMs).sum / withRemap.size
      val avgWithoutRemap = withoutRemap.map(_.medianTimeMs).sum / withoutRemap.size
      val remapBenefit = ((avgWithoutRemap - avgWithRemap) / avgWithoutRemap) * 100.0
      
      println(f"2. KEY REMAPPING:")
      println(f"   Avg time with remapping: ${avgWithRemap}%.2f ms")
      println(f"   Avg time without: ${avgWithoutRemap}%.2f ms")
      println(f"   Benefit: ${remapBenefit}%+.1f%%")
      if (remapBenefit > 5) {
        println("   → Key remapping improves performance")
      } else if (remapBenefit < -5) {
        println("   → Key remapping hurts performance")
      } else {
        println("   → Minimal impact")
      }
      println()
    }
    
    // 3. Build side swapping
    val withSwap = successfulResults.filter(_.allowBuildSideSwap)
    val withoutSwap = successfulResults.filter(!_.allowBuildSideSwap)
    
    if (withSwap.nonEmpty && withoutSwap.nonEmpty) {
      val avgWithSwap = withSwap.map(_.medianTimeMs).sum / withSwap.size
      val avgWithoutSwap = withoutSwap.map(_.medianTimeMs).sum / withoutSwap.size
      val swapBenefit = ((avgWithoutSwap - avgWithSwap) / avgWithoutSwap) * 100.0
      
      println(f"3. BUILD SIDE SWAPPING:")
      println(f"   Avg time with swap: ${avgWithSwap}%.2f ms")
      println(f"   Avg time without: ${avgWithoutSwap}%.2f ms")
      println(f"   Benefit: ${swapBenefit}%+.1f%%")
      if (swapBenefit > 5) {
        println("   → Build side swapping improves performance")
      } else {
        println("   → Minimal or negative impact")
      }
      println()
    }
    
    // 4. Hash object vs direct
    val hashObject = successfulResults.filter(_.joinStrategy == "hash_object")
    val hashDirect = successfulResults.filter(_.joinStrategy == "hash_direct")
    
    if (hashObject.nonEmpty && hashDirect.nonEmpty) {
      val avgObject = hashObject.map(_.medianTimeMs).sum / hashObject.size
      val avgDirect = hashDirect.map(_.medianTimeMs).sum / hashDirect.size
      val overhead = ((avgObject - avgDirect) / avgDirect) * 100.0
      
      println(f"4. HASH OBJECT vs DIRECT:")
      println(f"   Avg time with object: ${avgObject}%.2f ms")
      println(f"   Avg time with direct: ${avgDirect}%.2f ms")
      println(f"   Object overhead: ${overhead}%+.1f%%")
      println()
    }
    
    // 5. Hash vs Sort
    val hashResults = successfulResults.filter(r => r.joinStrategy == "hash_object" || r.joinStrategy == "hash_direct")
    val sortResults = successfulResults.filter(_.joinStrategy == "sort")
    
    if (hashResults.nonEmpty && sortResults.nonEmpty) {
      val avgHash = hashResults.map(_.medianTimeMs).sum / hashResults.size
      val avgSort = sortResults.map(_.medianTimeMs).sum / sortResults.size
      
      println(f"5. HASH vs SORT:")
      println(f"   Avg hash time: ${avgHash}%.2f ms")
      println(f"   Avg sort time: ${avgSort}%.2f ms")
      
      // Analyze by cardinality
      val lowCardHash = hashResults.filter(_.leftStats.cardinalityPct < 0.1).map(_.medianTimeMs)
      val lowCardSort = sortResults.filter(_.leftStats.cardinalityPct < 0.1).map(_.medianTimeMs)
      val highCardHash = hashResults.filter(_.leftStats.cardinalityPct > 0.5).map(_.medianTimeMs)
      val highCardSort = sortResults.filter(_.leftStats.cardinalityPct > 0.5).map(_.medianTimeMs)
      
      if (lowCardHash.nonEmpty && lowCardSort.nonEmpty) {
        val avgLowHash = lowCardHash.sum / lowCardHash.size
        val avgLowSort = lowCardSort.sum / lowCardSort.size
        println(f"   Low cardinality (<10%%): hash=${avgLowHash}%.2f ms, sort=${avgLowSort}%.2f ms")
        if (avgLowSort < avgLowHash) {
          println(f"     → Sort is ${((avgLowHash - avgLowSort)/avgLowHash * 100).toInt}%% faster at low cardinality")
        }
      }
      
      if (highCardHash.nonEmpty && highCardSort.nonEmpty) {
        val avgHighHash = highCardHash.sum / highCardHash.size
        val avgHighSort = highCardSort.sum / highCardSort.size
        println(f"   High cardinality (>50%%): hash=${avgHighHash}%.2f ms, sort=${avgHighSort}%.2f ms")
        if (avgHighHash < avgHighSort) {
          println(f"     → Hash is ${((avgHighSort - avgHighHash)/avgHighSort * 100).toInt}%% faster at high cardinality")
        }
      }
      println()
    }
  }

  val totalElapsedMin = (phase1ElapsedMin + phase2ElapsedMin)
  println("="*80)
  println("TRAINING DATA GENERATION COMPLETE")
  println("="*80)
  println(s"Output written to: $outputTsvPath")
  println()
  println("Timing Summary:")
  println(f"  Phase 1: ${phase1ElapsedMin}%.2f minutes")
  println(f"  Phase 2: ${phase2ElapsedMin}%.2f minutes")
  println(f"  Total:   ${totalElapsedMin}%.2f minutes")
  println()
  println("Next steps:")
  println("1. Load the TSV file into your ML framework (Python, R, etc.)")
  println("2. Train models to predict:")
  println("   - Best join strategy (hash_object vs hash_direct vs sort)")
  println("   - Whether to enable key remapping")
  println("   - Whether to enable build side swapping")
  println("3. Extract decision rules/thresholds from the trained models")
  println("4. Implement the heuristics in production code")
  println()
} // End of Phase 2 + Final Analysis block

