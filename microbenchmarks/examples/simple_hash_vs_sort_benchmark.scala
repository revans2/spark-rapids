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

// Simplified Hash vs Sort Join Benchmark
//
// This benchmark focuses on finding the boundary between HashObject and SortObjectPost
// for Inner joins with the following constraints:
// - ONLY HashObject and SortObjectPost strategies
// - ONLY Inner joins
// - Swap ALWAYS enabled (allowBuildSideSwap=true)
// - Key remapping ALWAYS disabled (remapComplexKeysToInts=false)
// - Distinct join ALWAYS disabled (useDistinctJoin=false)
//
// Key Features:
// - Supports Uniform, Zipf, and Gaussian (Normal) distributions
// - Weighted toward low cardinality (<1%, <2%, <5%) where sort typically wins
// - Can read refinement config file from Python analysis script
// - Appends results to TSV for iterative refinement
// - Disk-efficient: Generate → Test → Cleanup for each config

import com.nvidia.spark.rapids.benchmarks.JoinBenchmarkDataGen._
import com.nvidia.spark.rapids.benchmarks.JoinBenchmarkRunner._
import org.apache.spark.sql.tests.datagen._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.util.{Random, Try}
import java.io.{File, PrintWriter}
import java.nio.file.{Files, Paths, StandardCopyOption}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.io.Source

val baseDir = "/data/tmp/simple_hash_vs_sort"
val outputTsvPath = s"$baseDir/benchmark_results.tsv"
val refinementConfigPath = s"$baseDir/refinement_config.json"
// Initialize random early for function definitions
// Will be reinitialized later with correct seed based on existing tests
var random = new Random(42)

// ============================================================================
// Configuration
// ============================================================================

// Number of tests to generate (if not using refinement config)
val numTests = 2000

// Iterations per benchmark (for statistical significance)
val benchmarkIterations = 1

// Memory targets (in bytes) - 1MiB to 1GiB
val minMemoryTarget = 1L * 1024 * 1024
val maxMemoryTarget = 1024L * 1024 * 1024

// ============================================================================
// Distribution Implementation
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
  KeyTypeSpec("string", 60, true)  // Variable length, will be set per test
)

val keyTypeSpecMap: Map[String, KeyTypeSpec] = keyTypes.map(kt => kt.typeName -> kt).toMap

case class DistributionSpec(
  name: String,
  description: String
)

val distributions = Seq(
  DistributionSpec("uniform", "Uniform distribution"),
  DistributionSpec("zipf", "Zipf distribution (variable skew)"),
  DistributionSpec("gaussian", "Gaussian/Normal distribution")
)

case class TestConfig(
  name: String,
  // Left side
  leftRows: Long,
  leftDistinctKeys: Long,
  leftDistribution: String,
  leftDistributionParams: Map[String, Double],  // For Gaussian: mean, stddev; For Zipf: skew
  // Right side
  rightRows: Long,
  rightDistinctKeys: Long,
  rightDistribution: String,
  rightDistributionParams: Map[String, Double],
  // Key relationship
  keyOverlapPct: Double,
  // Composite keys - left and right must match for join compatibility
  numKeyColumns: Int,
  keyTypes: Seq[String],  // Type name for each column (e.g., Seq("int", "string", "long"))
  stringLengths: Seq[Option[Int]],  // String length for each column (Some(n) for strings, None for others)
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
  distribution: String
)

case class TableCountSummary(
  rows: Long,
  distinctKeys: Long,
  avgStringLengths: Map[Int, Double]
)

case class KeyCountStats(
  maxCount: Long,
  stdDev: Double,
  p99: Double,
  p95: Double,
  p90: Double,
  p75: Double,
  p50: Double
)

case class BenchmarkResult(
  testName: String,
  // Configuration
  joinStrategy: String,
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
  errorMessage: String = "",
  buildTimeMs: Option[Double] = None,
  probeTimeMs: Option[Double] = None,
  remapStructureMs: Option[Double] = None,
  remapBuildKeysMs: Option[Double] = None,
  remapProbeKeysMs: Option[Double] = None,
  createBuildObjectMs: Option[Double] = None,
  executeJoinMs: Option[Double] = None,
  // Key count statistics (build and probe sides)
  buildKeyCountStats: Option[KeyCountStats] = None,
  probeKeyCountStats: Option[KeyCountStats] = None
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

def pickRandomKeyType(allowedTypes: Option[Seq[String]] = None): (KeyTypeSpec, Option[Int]) = {
  // Filter key types if allowedTypes is specified
  val availableTypes = allowedTypes match {
    case Some(allowed) =>
      keyTypes.filter(kt => allowed.contains(kt.typeName))
    case None =>
      keyTypes
  }
  
  val keyType = if (availableTypes.isEmpty) {
    // Fallback to all types if filtering results in empty
    keyTypes(random.nextInt(keyTypes.length))
  } else {
    availableTypes(random.nextInt(availableTypes.length))
  }
  
  // If it's a string type, pick a continuous random length from 10 to 200 chars
  val stringLength = if (keyType.isVariableWidth) {
    // Log-scale distribution: more shorter strings, fewer longer strings
    // Random from log(10) to log(200), then exponentiate
    val minLog = math.log(10.0)
    val maxLog = math.log(200.0)
    val randomLog = minLog + random.nextDouble() * (maxLog - minLog)
    Some(math.exp(randomLog).toInt)
  } else {
    None
  }
  
  (keyType, stringLength)
}

def pickMixedKeyTypes(numColumns: Int, allowedTypes: Option[Seq[String]] = None): (Seq[String], Seq[Option[Int]]) = {
  /**
   * Generate mixed key types for multi-column composite keys.
   * 
   * Strategy:
   * - Single column (numColumns=1): Always homogeneous (just pick one type)
   * - Multi-column (numColumns>1): 
   *   - 50% homogeneous (all same type)
   *   - 50% mixed (different types)
   */
  
  if (numColumns == 1) {
    // Single column - just pick one type
    val (keyType, stringLen) = pickRandomKeyType(allowedTypes)
    (Seq(keyType.typeName), Seq(stringLen))
  } else {
    // Multi-column - decide homogeneous vs mixed
    val isMixed = random.nextDouble() < 0.5
    
    if (!isMixed) {
      // Homogeneous - all columns same type
      val (keyType, stringLen) = pickRandomKeyType(allowedTypes)
      val types = Seq.fill(numColumns)(keyType.typeName)
      val lengths = Seq.fill(numColumns)(stringLen)
      (types, lengths)
    } else {
      // Mixed - each column gets its own type
      val columnsConfig = (0 until numColumns).map { _ =>
        pickRandomKeyType(allowedTypes)
      }
      val types = columnsConfig.map(_._1.typeName)
      val lengths = columnsConfig.map(_._2)
      (types, lengths)
    }
  }
}

def pickRandomDistribution(): (String, Map[String, Double]) = {
  val dist = distributions(random.nextInt(distributions.length))
  val params = dist.name match {
    case "gaussian" =>
      // Random mean (0.3 to 0.7 of distinct keys) and stddev (0.1 to 0.3)
      Map(
        "mean" -> (0.3 + random.nextDouble() * 0.4),
        "stddev" -> (0.1 + random.nextDouble() * 0.2)
      )
    case "zipf" =>
      // Random skew factor from 0.3 to 2.0
      // 0.3-0.7 = light skew, 0.8-1.2 = moderate, 1.3-2.0 = heavy
      Map("skew" -> (0.3 + random.nextDouble() * 1.7))
    case _ =>
      Map.empty[String, Double]
  }
  (dist.name, params)
}

def pickCardinalityPct(weightLow: Boolean = true): Double = {
  // Use continuous random values instead of discrete options
  // Weight toward low cardinality where sort wins
  val r = random.nextDouble()
  if (weightLow) {
    if (r < 0.30) {
      // 30% of cases: very low cardinality 0.1% to 1%
      0.001 + random.nextDouble() * 0.009
    } else if (r < 0.50) {
      // 20% of cases: low cardinality 1% to 2%
      0.01 + random.nextDouble() * 0.01
    } else if (r < 0.65) {
      // 15% of cases: low-medium cardinality 2% to 5%
      0.02 + random.nextDouble() * 0.03
    } else if (r < 0.80) {
      // 15% of cases: medium cardinality 5% to 20%
      0.05 + random.nextDouble() * 0.15
    } else {
      // 20% of cases: high cardinality 20% to 100%
      0.20 + random.nextDouble() * 0.80
    }
  } else {
    // Uniform distribution across cardinalities 0.1% to 100%
    0.001 + random.nextDouble() * 0.999
  }
}

def pickKeyOverlapPct(): Double = {
  // Continuous random from 10% to 100% overlap
  // Weight toward higher overlap (more realistic)
  val r = random.nextDouble()
  if (r < 0.3) {
    // 30%: lower overlap 10-80%
    0.1 + random.nextDouble() * 0.7
  } else {
    // 70%: higher overlap 80-100%
    0.8 + random.nextDouble() * 0.2
  }
}

// ============================================================================
// Test Case Generation
// ============================================================================

def generateRandomTestConfig(testId: Int, weightLowCardinality: Boolean = true): TestConfig = {
  // Pick number of key columns
  // 80% single key, 15% 2-column composite, 5% 3-column composite
  val numKeys = {
    val r = random.nextDouble()
    if (r < 0.80) 1
    else if (r < 0.95) 2
    else 3
  }
  
  // Pick key types (can be mixed for multi-column keys)
  // Types and string lengths must be same for left and right (join requirement)
  val (keyTypeNames, stringLengths) = pickMixedKeyTypes(numKeys)
  
  // Calculate bytes per row (sum of bytes for all key columns)
  val bytesPerRow = keyTypeNames.zip(stringLengths).map { case (typeName, stringLen) =>
    val keyTypeSpec = keyTypes.find(_.typeName == typeName).get
    stringLen.getOrElse(keyTypeSpec.avgBytes)
  }.sum.toDouble
  
  // Calculate target memory for each side
  // Use math.abs to ensure non-negative modulo result (random.nextLong() can be negative)
  val leftTargetMemory = minMemoryTarget + math.abs(random.nextLong()) % (maxMemoryTarget - minMemoryTarget)
  val rightTargetMemory = minMemoryTarget + math.abs(random.nextLong()) % (maxMemoryTarget - minMemoryTarget)
  
  // Calculate row counts based on memory target
  // Ensure we meet the minimum memory target (don't use hardcoded 10k rows)
  val minRowsForTarget = (minMemoryTarget / bytesPerRow).toLong
  val leftRows = math.max(minRowsForTarget, leftTargetMemory / bytesPerRow).toLong
  val rightRows = math.max(minRowsForTarget, rightTargetMemory / bytesPerRow).toLong
  
  // Calculate distinct keys with continuous weighting
  val leftCardPct = pickCardinalityPct(weightLowCardinality)
  val rightCardPct = pickCardinalityPct(weightLowCardinality)
  
  val leftDistinct = math.max(10, (leftRows * leftCardPct).toLong)
  val rightDistinct = math.max(10, (rightRows * rightCardPct).toLong)
  
  // Pick distributions (now includes zipf skew parameter)
  val (leftDist, leftParams) = pickRandomDistribution()
  val (rightDist, rightParams) = pickRandomDistribution()
  
  // Key overlap (continuous)
  val keyOverlap = pickKeyOverlapPct()
  
  // Calculate actual memory
  val leftMem = (leftRows * bytesPerRow) / (1024.0 * 1024.0)
  val rightMem = (rightRows * bytesPerRow) / (1024.0 * 1024.0)
  
  TestConfig(
    name = f"test_${testId}%04d",
    leftRows = leftRows,
    leftDistinctKeys = leftDistinct,
    leftDistribution = leftDist,
    leftDistributionParams = leftParams,
    rightRows = rightRows,
    rightDistinctKeys = rightDistinct,
    rightDistribution = rightDist,
    rightDistributionParams = rightParams,
    keyOverlapPct = keyOverlap,
    numKeyColumns = numKeys,
    keyTypes = keyTypeNames,
    stringLengths = stringLengths,
    leftMemoryMB = leftMem,
    rightMemoryMB = rightMem
  )
}

// ============================================================================
// Refinement Config Loading
// ============================================================================

// Define case classes FIRST before they're used
case class TargetRegion(
  buildCardinalityMin: Double,
  buildCardinalityMax: Double,
  probeCardinalityMin: Double,
  probeCardinalityMax: Double,
  buildRowsMin: Long,
  buildRowsMax: Long,
  probeRowsMin: Long,
  probeRowsMax: Long,
  keyTypes: Seq[String],
  numKeyColumns: Seq[Int],
  importance: Double
)

case class RefinementConfig(
  targetRegions: Seq[TargetRegion],
  numTestsPerRegion: Int
)

// JSON parsing using json4s (available in Spark)
def parseRefinementConfig(jsonStr: String): Option[RefinementConfig] = {
  implicit val formats: DefaultFormats.type = DefaultFormats
  
  try {
    val json = parse(jsonStr)
    
    // Extract numTestsPerRegion
    val numTests = (json \ "numTestsPerRegion").extractOpt[Int].getOrElse {
      // Fallback: use num_tests from first region if available
      ((json \ "targetRegions")(0) \ "num_tests").extractOpt[Int].getOrElse(20)
    }
    
    // Extract target regions
    val regionsJson = (json \ "targetRegions").children
    val regions = regionsJson.map { regionJson =>
      TargetRegion(
        buildCardinalityMin = (regionJson \ "buildCardinalityMin").extract[Double],
        buildCardinalityMax = (regionJson \ "buildCardinalityMax").extract[Double],
        probeCardinalityMin = (regionJson \ "probeCardinalityMin").extractOpt[Double].getOrElse(0.0),
        probeCardinalityMax = (regionJson \ "probeCardinalityMax").extractOpt[Double].getOrElse(1.0),
        buildRowsMin = (regionJson \ "buildRowsMin").extractOpt[Long].getOrElse(1000L),
        buildRowsMax = (regionJson \ "buildRowsMax").extractOpt[Long].getOrElse(10000000L),
        probeRowsMin = (regionJson \ "probeRowsMin").extractOpt[Long].getOrElse(1000L),
        probeRowsMax = (regionJson \ "probeRowsMax").extractOpt[Long].getOrElse(100000000L),
        keyTypes = (regionJson \ "keyTypes").extractOpt[List[String]].getOrElse(List("int", "long", "string")),
        numKeyColumns = (regionJson \ "numKeyColumns").extractOpt[List[Int]].getOrElse(List(1, 2, 3)),
        importance = (regionJson \ "importance").extractOpt[Double].getOrElse(1.0)
      )
    }
    
    if (regions.nonEmpty && numTests > 0) {
      Some(RefinementConfig(regions, numTests))
    } else {
      None
    }
  } catch {
    case e: Exception =>
      println(s"JSON parsing error: ${e.getMessage}")
      None
  }
}

def loadRefinementConfig(): Option[RefinementConfig] = {
  val file = new File(refinementConfigPath)
  if (!file.exists()) {
    println(s"No refinement config found at: $refinementConfigPath")
    return None
  }
  
  try {
    println(s"Loading refinement config from: $refinementConfigPath")
    val source = scala.io.Source.fromFile(file)
    val jsonStr = source.mkString
    source.close()
    
    // Use json4s to parse the JSON
    parseRefinementConfig(jsonStr) match {
      case Some(config) =>
        println(s"Successfully loaded refinement config:")
        println(s"  ${config.targetRegions.size} target regions")
        println(s"  ${config.numTestsPerRegion} tests per region")
        Some(config)
      case None =>
        println("ERROR: Could not parse refinement config JSON")
        None
    }
  } catch {
    case e: Exception =>
      println(s"ERROR loading refinement config: ${e.getMessage}")
      e.printStackTrace()
      None
  }
}

def generateTestsFromRefinementConfig(config: RefinementConfig, startId: Int): Seq[TestConfig] = {
  println(s"Generating tests from refinement config:")
  println(s"  ${config.targetRegions.size} target regions")
  println(s"  ${config.numTestsPerRegion} tests per region")
  println()
  
  val tests = scala.collection.mutable.ArrayBuffer[TestConfig]()
  var id = startId
  
  config.targetRegions.foreach { region =>
    println(f"  Region: buildCard=[${region.buildCardinalityMin}%.3f, ${region.buildCardinalityMax}%.3f], " +
            f"buildRows=[${region.buildRowsMin}%,d, ${region.buildRowsMax}%,d], importance=${region.importance}%.2f")
    
    // Generate tests for this region
    (0 until config.numTestsPerRegion).foreach { _ =>
      // Pick cardinality within region
      val buildCard = region.buildCardinalityMin + 
                      random.nextDouble() * (region.buildCardinalityMax - region.buildCardinalityMin)
      val probeCard = region.probeCardinalityMin + 
                      random.nextDouble() * (region.probeCardinalityMax - region.probeCardinalityMin)
      
      // Pick row counts within region
      val buildRows = region.buildRowsMin + 
                      (random.nextDouble() * (region.buildRowsMax - region.buildRowsMin)).toLong
      val probeRows = region.probeRowsMin + 
                      (random.nextDouble() * (region.probeRowsMax - region.probeRowsMin)).toLong
      
      // Smaller side is build, larger is probe
      val (leftRows, rightRows, leftCard, rightCard) = if (buildRows < probeRows) {
        (buildRows, probeRows, buildCard, probeCard)
      } else {
        (probeRows, buildRows, probeCard, buildCard)
      }
      
      // Pick number of key columns from allowed list
      val numKeys = region.numKeyColumns(random.nextInt(region.numKeyColumns.length))
      
      // Pick key types (can be mixed for multi-column keys)
      // Respect the keyTypes constraint from refinement config
      val allowedKeyTypes = if (region.keyTypes.nonEmpty) Some(region.keyTypes) else None
      val (keyTypeNames, stringLengths) = pickMixedKeyTypes(numKeys, allowedKeyTypes)
      
      // Calculate bytes per row (sum of bytes for all key columns)
      val bytesPerRow = keyTypeNames.zip(stringLengths).map { case (typeName, stringLen) =>
        val keyTypeSpec = keyTypes.find(_.typeName == typeName).get
        stringLen.getOrElse(keyTypeSpec.avgBytes)
      }.sum.toDouble
      
      // Calculate distinct keys
      val leftDistinct = math.max(10, (leftRows * leftCard).toLong)
      val rightDistinct = math.max(10, (rightRows * rightCard).toLong)
      
      // Pick distributions
      val (leftDist, leftParams) = pickRandomDistribution()
      val (rightDist, rightParams) = pickRandomDistribution()
      
      // Key overlap
      val keyOverlap = pickKeyOverlapPct()
      
      // Calculate memory
      val leftMem = (leftRows * bytesPerRow) / (1024.0 * 1024.0)
      val rightMem = (rightRows * bytesPerRow) / (1024.0 * 1024.0)
      
      tests += TestConfig(
        name = f"refinement_${id}%04d",
        leftRows = leftRows,
        leftDistinctKeys = leftDistinct,
        leftDistribution = leftDist,
        leftDistributionParams = leftParams,
        rightRows = rightRows,
        rightDistinctKeys = rightDistinct,
        rightDistribution = rightDist,
        rightDistributionParams = rightParams,
        keyOverlapPct = keyOverlap,
        numKeyColumns = numKeys,
        keyTypes = keyTypeNames,
        stringLengths = stringLengths,
        leftMemoryMB = leftMem,
        rightMemoryMB = rightMem
      )
      
      id += 1
    }
  }
  
  println(s"Generated ${tests.size} tests from refinement config")
  println()
  tests.toSeq
}

// ============================================================================
// Data Generation
// ============================================================================

def createDistribution(
  distributionName: String, 
  params: Map[String, Double],
  distinctKeys: Long
): LocationToSeedMapping = {
  distributionName match {
    case "uniform" =>
      FlatDistribution()
    case "zipf" =>
      // Use the skew parameter from params (continuous from 0.3 to 2.0)
      val skewFactor = params.getOrElse("skew", 1.0)
      ZipfDistribution(skewFactor = skewFactor)
    case "gaussian" =>
      // Mean and stddev are relative to the distinct key range
      // Convert them to absolute seed values
      val meanRatio = params.getOrElse("mean", 0.5)
      val stddevRatio = params.getOrElse("stddev", 0.2)
      val meanSeed = (distinctKeys * meanRatio).toLong
      val stddevSeed = (distinctKeys * stddevRatio)
      NormalDistribution(meanSeed, stddevSeed)
    case _ =>
      println(s"Warning: Unknown distribution $distributionName, using uniform")
      FlatDistribution()
  }
}

def generateTestData(config: TestConfig): Unit = {
  val keyTypesStr = config.keyTypes.mkString(", ")
  println(f"  Generating ${config.name}:")
  println(f"    Left:  ${config.leftRows}%,d rows, ${config.leftDistinctKeys}%,d distinct " +
          f"($keyTypesStr, ${config.leftDistribution})")
  println(f"    Right: ${config.rightRows}%,d rows, ${config.rightDistinctKeys}%,d distinct " +
          f"($keyTypesStr, ${config.rightDistribution})")
  
  try {
    // Determine key seed ranges
    val leftMinSeed = 0L
    val leftMaxSeed = config.leftDistinctKeys - 1
    val rightMinSeed = 0L
    val rightMaxSeed = config.rightDistinctKeys - 1
    
    // Create distributions
    val leftDist = createDistribution(config.leftDistribution, config.leftDistributionParams, config.leftDistinctKeys)
    val rightDist = createDistribution(config.rightDistribution, config.rightDistributionParams, config.rightDistinctKeys)
    
    // Use DBGen for mixed key type support
    val dbgen = DBGen()
    
    // Build DDL for left table with mixed types
    val leftDDL = (0 until config.numKeyColumns).map { i =>
      val typeName = config.keyTypes(i)
      s"key${i+1} $typeName"
    }.mkString(", ")
    
    // Build DDL for right table (same types as left)
    val rightDDL = (0 until config.numKeyColumns).map { i =>
      val typeName = config.keyTypes(i)
      s"key${i+1} $typeName"
    }.mkString(", ")
    
    // Generate left table
    val leftTable = dbgen.addTable("left", leftDDL, config.leftRows)
    
    // Build list of key column names
    val leftKeyNames = (1 to config.numKeyColumns).map(i => s"key$i").toSeq
    
    // Use CorrelatedKeyGroup for multi-key columns to prevent cartesian product explosion
    // For single keys, this still works correctly and ensures consistent behavior
    if (config.numKeyColumns > 1) {
      // Multi-key: Use CorrelatedKeyGroup so same seed generates correlated values across columns
      // This ensures distinct combinations = distinct keys (seed range), not cartesian product
      leftTable.configureKeyGroup(leftKeyNames, CorrelatedKeyGroup(1, leftMinSeed, leftMaxSeed), leftDist)
    } else {
      // Single key: Set seed range and mapping individually (works the same, but simpler)
      leftTable(leftKeyNames.head).setSeedRange(leftMinSeed, leftMaxSeed)
      leftTable(leftKeyNames.head).setSeedMapping(leftDist)
    }
    
    // Set string lengths for all key columns
    // Note: For single keys, seed range/mapping already set above.
    //       For multi-keys, CorrelatedKeyGroup handles seed range, but we can still set per-column properties like string length.
    for (i <- 0 until config.numKeyColumns) {
      val colName = s"key${i+1}"
      
      // Set string length if this column is a string
      config.stringLengths(i).foreach { len =>
        leftTable(colName).setLength(len)
      }
    }
    val leftDf = leftTable.toDF(spark)
    leftDf.repartition(1).write.mode("overwrite").parquet(s"$baseDir/${config.name}/left")
    
    // Generate right table (same approach as left)
    val rightTable = dbgen.addTable("right", rightDDL, config.rightRows)
    val rightKeyNames = (1 to config.numKeyColumns).map(i => s"key$i").toSeq
    
    if (config.numKeyColumns > 1) {
      // Multi-key: Use CorrelatedKeyGroup
      rightTable.configureKeyGroup(rightKeyNames, CorrelatedKeyGroup(1, rightMinSeed, rightMaxSeed), rightDist)
    } else {
      // Single key: Set seed range and mapping individually
      rightTable(rightKeyNames.head).setSeedRange(rightMinSeed, rightMaxSeed)
      rightTable(rightKeyNames.head).setSeedMapping(rightDist)
    }
    
    // Set string lengths for all key columns
    // Note: For single keys, seed range/mapping already set above.
    //       For multi-keys, CorrelatedKeyGroup handles seed range, but we can still set per-column properties like string length.
    for (i <- 0 until config.numKeyColumns) {
      val colName = s"key${i+1}"
      config.stringLengths(i).foreach { len =>
        rightTable(colName).setLength(len)
      }
    }
    val rightDf = rightTable.toDF(spark)
    rightDf.repartition(1).write.mode("overwrite").parquet(s"$baseDir/${config.name}/right")
    
  } catch {
    case e: Exception =>
      println(s"    ERROR generating data: ${e.getMessage}")
      e.printStackTrace()
  }
}

// ============================================================================
// Statistics Collection
// ============================================================================

def collectTableStats(config: TestConfig, side: String, summary: TableCountSummary): TableStats = {
  val dist = side match {
    case "left" => config.leftDistribution
    case "right" => config.rightDistribution
    case other => throw new IllegalArgumentException(s"Unknown side for stats collection: $other")
  }
  val rowCount = summary.rows
  val distinctKeys = summary.distinctKeys
  val cardinalityPct =
    if (rowCount == 0) 0.0 else distinctKeys.toDouble / rowCount.toDouble

  val avgKeyBytesValue = config.keyTypes.zipWithIndex.map { case (typeName, idx) =>
    val spec = keyTypeSpecMap.getOrElse(typeName, KeyTypeSpec(typeName, 0, false))
    if (spec.isVariableWidth) {
      val measured = summary.avgStringLengths.getOrElse(idx, 0.0)
      val fallbackLength = config.stringLengths(idx).map(_.toDouble).getOrElse(spec.avgBytes.toDouble)
      if (measured > 0.0) measured else fallbackLength
    } else {
      spec.avgBytes.toDouble
    }
  }.sum

  val avgKeyBytes = math.max(1, math.round(avgKeyBytesValue).toInt)
  val memoryMB =
    if (rowCount == 0) 0.0 else (rowCount.toDouble * avgKeyBytesValue) / (1024.0 * 1024.0)

  val keyTypeStr = if (config.keyTypes.size == 1) {
    config.keyTypes.head
  } else {
    config.keyTypes.mkString(",")
  }

  TableStats(
    rows = rowCount,
    distinctKeys = distinctKeys,
    cardinalityPct = cardinalityPct,
    keyType = keyTypeStr,
    avgKeyBytes = avgKeyBytes,
    memoryMB = memoryMB,
    distribution = dist
  )
}

// ============================================================================
// Key Count Statistics Calculation
// ============================================================================

def calculateKeyCountStats(config: TestConfig): (KeyCountStats, KeyCountStats, Long, TableCountSummary, TableCountSummary) = {
  /**
   * Calculate key count statistics for both left and right tables.
   * OPTIMIZED: Only 1 query per side (2 total) + 1 for join output = 3 queries total
   * Returns (leftStats, rightStats, outputRows, leftSummary, rightSummary)
   */
  
  val leftPath = s"$baseDir/${config.name}/left"
  val rightPath = s"$baseDir/${config.name}/right"
  
  val leftDf = spark.read.parquet(leftPath)
  val rightDf = spark.read.parquet(rightPath)
  val stringKeyIndices = config.keyTypes.zipWithIndex.collect {
    case (typeName, idx) if keyTypeSpecMap.get(typeName).exists(_.isVariableWidth) => idx
  }
  
  // Build key column references
  val leftKeyCols = (1 to config.numKeyColumns).map(i => col(s"key$i"))
  val rightKeyCols = (1 to config.numKeyColumns).map(i => col(s"key$i"))
  
  // Helper functions
  def getPercentile(row: org.apache.spark.sql.Row, colName: String): Double = {
    val value = row.get(row.fieldIndex(colName))
    value match {
      case l: Long => l.toDouble
      case d: Double => d
      case null => 0.0
      case _ => Option(value).map(_.toString.toDouble).getOrElse(0.0)
    }
  }
  
  def extractLong(row: org.apache.spark.sql.Row, colName: String): Long = {
    val idx = row.fieldIndex(colName)
    if (row.isNullAt(idx)) {
      0L
    } else {
      row.get(idx) match {
        case l: Long => l
        case i: Int => i.toLong
        case s: Short => s.toLong
        case b: Byte => b.toLong
        case d: Double => d.toLong
        case f: Float => f.toLong
        case bd: java.math.BigDecimal => bd.longValue()
        case bd: BigDecimal => bd.longValue()
        case value => Try(value.toString.toLong).getOrElse(0L)
      }
    }
  }

  def extractDouble(row: org.apache.spark.sql.Row, colName: String): Double = {
    val idx = row.fieldIndex(colName)
    if (row.isNullAt(idx)) 0.0
    else {
      row.get(idx) match {
        case d: Double => d
        case f: Float => f.toDouble
        case l: Long => l.toDouble
        case i: Int => i.toDouble
        case s: Short => s.toDouble
        case b: Byte => b.toDouble
        case bd: java.math.BigDecimal => bd.doubleValue()
        case bd: BigDecimal => bd.doubleValue()
        case value => Try(value.toString.toDouble).getOrElse(0.0)
      }
    }
  }

  // OPTIMIZATION: Single query per side that computes ALL stats at once
  def computeAllStats(df: org.apache.spark.sql.DataFrame, keyCols: Seq[org.apache.spark.sql.Column], countColName: String, stringIndices: Seq[Int]): (org.apache.spark.sql.Row, org.apache.spark.sql.DataFrame) = {
    // Build aggregation for groupBy: count rows per key AND avg string lengths in same pass
    val countExpr = count("*").alias(countColName)
    val stringExprs = stringIndices.map { idx =>
      avg(length(col(s"key${idx + 1}"))).alias(s"avg_len_${idx}")
    }
    
    // Group by keys and compute counts + string averages in single pass
    val keyCounts = if (stringExprs.isEmpty) {
      df.groupBy(keyCols: _*).agg(countExpr)
    } else {
      df.groupBy(keyCols: _*).agg(countExpr, stringExprs: _*)
    }
    
    // Now aggregate the grouped results to get percentiles and summary stats
    val percentileExprs = Seq(
      max(col(countColName)).alias("max_count"),
      stddev_pop(col(countColName)).alias("std_dev"),
      expr(s"percentile_approx($countColName, 0.99)").alias("p99"),
      expr(s"percentile_approx($countColName, 0.95)").alias("p95"),
      expr(s"percentile_approx($countColName, 0.90)").alias("p90"),
      expr(s"percentile_approx($countColName, 0.75)").alias("p75"),
      expr(s"percentile_approx($countColName, 0.50)").alias("p50"),
      sum(col(countColName)).alias("total_rows"),
      count(lit(1)).alias("distinct_keys")
    )
    
    // Also aggregate the string lengths (avg of avgs weighted by count)
    val stringAggExprs = stringIndices.map { idx =>
      // Weighted average: sum(avg_len * count) / sum(count)
      (sum(col(s"avg_len_${idx}") * col(countColName)) / sum(col(countColName))).alias(s"avg_len_${idx}")
    }
    
    val allExprs = percentileExprs ++ stringAggExprs
    
    // Execute single aggregation that computes everything
    val statsRow = keyCounts.agg(allExprs.head, allExprs.tail: _*).collect()(0)
    
    (statsRow, keyCounts)
  }
  
  // Execute single query per side
  val (leftStatsRow, leftKeyCounts) = computeAllStats(leftDf, leftKeyCols, "l_count", stringKeyIndices)
  val (rightStatsRow, rightKeyCounts) = computeAllStats(rightDf, rightKeyCols, "r_count", stringKeyIndices)
  
  // Extract string averages from the same row
  val leftStringAverages = stringKeyIndices.map { idx =>
    idx -> extractDouble(leftStatsRow, s"avg_len_${idx}")
  }.toMap
  
  val rightStringAverages = stringKeyIndices.map { idx =>
    idx -> extractDouble(rightStatsRow, s"avg_len_${idx}")
  }.toMap
  
  // Build summaries
  val leftSummary = TableCountSummary(
    rows = extractLong(leftStatsRow, "total_rows"),
    distinctKeys = extractLong(leftStatsRow, "distinct_keys"),
    avgStringLengths = leftStringAverages
  )
  
  val rightSummary = TableCountSummary(
    rows = extractLong(rightStatsRow, "total_rows"),
    distinctKeys = extractLong(rightStatsRow, "distinct_keys"),
    avgStringLengths = rightStringAverages
  )

  // Build key count stats
  val leftStats = KeyCountStats(
    maxCount = Option(leftStatsRow.getAs[Long]("max_count")).getOrElse(0L),
    stdDev = Option(leftStatsRow.getAs[Double]("std_dev")).getOrElse(0.0),
    p99 = getPercentile(leftStatsRow, "p99"),
    p95 = getPercentile(leftStatsRow, "p95"),
    p90 = getPercentile(leftStatsRow, "p90"),
    p75 = getPercentile(leftStatsRow, "p75"),
    p50 = getPercentile(leftStatsRow, "p50")
  )
  
  val rightStats = KeyCountStats(
    maxCount = Option(rightStatsRow.getAs[Long]("max_count")).getOrElse(0L),
    stdDev = Option(rightStatsRow.getAs[Double]("std_dev")).getOrElse(0.0),
    p99 = getPercentile(rightStatsRow, "p99"),
    p95 = getPercentile(rightStatsRow, "p95"),
    p90 = getPercentile(rightStatsRow, "p90"),
    p75 = getPercentile(rightStatsRow, "p75"),
    p50 = getPercentile(rightStatsRow, "p50")
  )
  
  // Join the two count tables and calculate output rows (3rd and final query)
  val joinConditions = (1 to config.numKeyColumns).map(i => 
    leftKeyCounts(s"key$i") === rightKeyCounts(s"key$i")
  ).reduce(_ && _)
  
  val joinedCounts = leftKeyCounts
    .join(rightKeyCounts, joinConditions, "inner")
    .select((col("l_count") * col("r_count")).alias("product"))
  
  val outputRowsResult = joinedCounts.agg(sum(col("product")).alias("sum_product")).collect()(0)
  val outputRows = extractLong(outputRowsResult, "sum_product")
  
  (leftStats, rightStats, outputRows, leftSummary, rightSummary)
}

// ============================================================================
// Join Size Validation
// ============================================================================

def validateJoinSize(outputRows: Long): (Boolean, String) = {
  /**
   * Validates that the join won't produce too many rows or run out of memory.
   * Returns (isValid, reason)
   * 
   * Uses the output rows calculated from calculateKeyCountStats.
   * Skips the test if output would exceed Int.MaxValue rows (gather map limit).
   */
  
  val maxRows = Int.MaxValue.toLong  // 2,147,483,647
  val warningThreshold = maxRows / 2  // Warn at 1B rows
  
  if (outputRows > maxRows) {
    val reason = f"Output too large: $outputRows%,d rows (> Int.MaxValue = $maxRows%,d)"
    println(s"    ✗ SKIP: $reason")
    (false, reason)
  } else if (outputRows > warningThreshold) {
    val reason = f"Large output: $outputRows%,d rows (close to limit)"
    println(s"    ⚠ WARNING: $reason (will attempt anyway)")
    (true, reason)
  } else {
    println(f"    ✓ Join output: $outputRows%,d rows (within limits)")
    (true, "OK")
  }
}

// ============================================================================
// Benchmark Execution
// ============================================================================

def runSingleBenchmark(
    config: TestConfig,
    strategy: String,
    leftStats: TableStats,
    rightStats: TableStats,
    buildKeyCountStats: Option[KeyCountStats] = None,
    probeKeyCountStats: Option[KeyCountStats] = None): BenchmarkResult = {
  val testName = s"${config.name}_${strategy}"
  
  try {
    val keyIndices = (0 until config.numKeyColumns).toSeq
    
    val joinStrategy = strategy match {
      case "hash_object" => HashObjectStrategy
      case "sort_object_post" => SortObjectWithPostStrategy
      case _ => throw new IllegalArgumentException(s"Unknown strategy: $strategy")
    }
    
    val collectTimings = strategy match {
      case "hash_object" | "sort_object_post" => true
      case _ => false
    }
    
    val benchConfig = JoinBenchmarkConfig(
      testName = testName,
      leftParquetPath = s"$baseDir/${config.name}/left",
      rightParquetPath = s"$baseDir/${config.name}/right",
      joinType = InnerJoin,
      joinStrategy = joinStrategy,
      buildSide = AutoPickSmallerIfAllowed,
      optimizations = JoinOptimizations(
        allowBuildSideSwap = true,         // ALWAYS ON
        remapComplexKeysToInts = false,    // ALWAYS OFF
        useDistinctJoin = false,           // ALWAYS OFF
        cacheJoinObject = false,
        cacheRemapping = false,
        cacheDistinctFlag = false
      ),
      conditionalFilter = None,
      leftKeyIndices = keyIndices,
      rightKeyIndices = keyIndices,
      iterations = benchmarkIterations,
      numThreads = 1,
      printHeader = false,
      collectDetailedTimings = collectTimings
    )
    
    val result = runBenchmark(benchConfig, spark)
    
    val timingBreakdown = result.detailedTimings.map { dt =>
      val buildMs = dt.remapStructureBuildMs + dt.remapBuildKeysMs + dt.createBuildObjectMs
      val probeMs = dt.remapProbeKeysMs + dt.executeJoinMs
      (buildMs, probeMs, dt)
    }
    
    BenchmarkResult(
      testName = testName,
      joinStrategy = strategy,
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
      iterations = benchmarkIterations,
      buildTimeMs = timingBreakdown.map(_._1),
      probeTimeMs = timingBreakdown.map(_._2),
      remapStructureMs = timingBreakdown.map(_._3.remapStructureBuildMs),
      remapBuildKeysMs = timingBreakdown.map(_._3.remapBuildKeysMs),
      remapProbeKeysMs = timingBreakdown.map(_._3.remapProbeKeysMs),
      createBuildObjectMs = timingBreakdown.map(_._3.createBuildObjectMs),
      executeJoinMs = timingBreakdown.map(_._3.executeJoinMs),
      buildKeyCountStats = buildKeyCountStats,
      probeKeyCountStats = probeKeyCountStats
    )
    
  } catch {
    case e: Exception =>
      println(s"    FAILED: ${e.getMessage}")
      BenchmarkResult(
        testName = testName,
        joinStrategy = strategy,
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

def runBothStrategiesForConfig(
    config: TestConfig,
    leftStats: TableStats,
    rightStats: TableStats,
    buildKeyCountStats: Option[KeyCountStats] = None,
    probeKeyCountStats: Option[KeyCountStats] = None): Seq[BenchmarkResult] = {
  val strategies = Seq("hash_object", "sort_object_post")
  val results = strategies.map { strategy =>
    println(s"  Running: ${strategy}")
    runSingleBenchmark(config, strategy, leftStats, rightStats, buildKeyCountStats, probeKeyCountStats)
  }
  results
}

// ============================================================================
// TSV Output
// ============================================================================

// Define column names first (needed by functions below)
val baseColumns = Seq(
  "TestName",
  "Status",
  "JoinStrategy",
  "ActualBuildSide",
  "LeftRows",
  "LeftDistinctKeys",
  "LeftCardinalityPct",
  "LeftKeyType",
  "LeftAvgKeyBytes",
  "LeftMemoryMB",
  "LeftDistribution",
  "RightRows",
  "RightDistinctKeys",
  "RightCardinalityPct",
  "RightKeyType",
  "RightAvgKeyBytes",
  "RightMemoryMB",
  "RightDistribution",
  "NumKeyColumns",
  "KeyOverlapPct",
  "MedianTimeMs",
  "AvgTimeMs",
  "StdDevMs",
  "OutputRows",
  "Iterations",
  "ErrorMessage"
)

val timingColumns = Seq(
  "BuildTimeMs",
  "ProbeTimeMs",
  "RemapStructureMs",
  "RemapBuildKeysMs",
  "RemapProbeKeysMs",
  "CreateBuildObjectMs",
  "ExecuteJoinMs"
)

val keyCountColumns = Seq(
  "BuildMaxKeyCount", "BuildKeyCountStdDev", "BuildKeyCountP99", "BuildKeyCountP95",
  "BuildKeyCountP90", "BuildKeyCountP75", "BuildKeyCountP50",
  "ProbeMaxKeyCount", "ProbeKeyCountStdDev", "ProbeKeyCountP99", "ProbeKeyCountP95",
  "ProbeKeyCountP90", "ProbeKeyCountP75", "ProbeKeyCountP50"
)

val legacyHeader = baseColumns.mkString("\t")
val expectedHeader = (baseColumns ++ timingColumns ++ keyCountColumns).mkString("\t")

def upgradeLegacyTSV(path: String): Unit = {
  val file = new File(path)
  if (!file.exists()) {
    return
  }
  
  val source = Source.fromFile(file)
  try {
    val linesIter = source.getLines()
    if (!linesIter.hasNext) {
      return
    }
    val header = linesIter.next()
    if (header == expectedHeader) {
      return
    }
    if (header != legacyHeader) {
      println(s"WARNING: TSV header at $path is unrecognized. Expected legacy or upgraded format.")
      return
    }
  } finally {
    source.close()
  }
  
  val backupPath = Paths.get(s"${path}.backup_legacy")
  if (!Files.exists(backupPath)) {
    Files.copy(file.toPath, backupPath, StandardCopyOption.REPLACE_EXISTING)
    println(s"Legacy TSV detected. Backup written to: ${backupPath.toString}")
  }
  
  val existingLines = Source.fromFile(backupPath.toFile)
  val tempFile = File.createTempFile("benchmark_results_upgrade", ".tsv", file.getParentFile)
  val writer = new PrintWriter(tempFile)
  try {
    val iter = existingLines.getLines()
    if (iter.hasNext) {
      iter.next() // skip old header
    }
    writer.println(expectedHeader)
    val extra = "\t" + Seq.fill(timingColumns.length + keyCountColumns.length)("").mkString("\t")
    while (iter.hasNext) {
      val line = iter.next()
      writer.println(line + extra)
    }
  } finally {
    existingLines.close()
    writer.close()
  }
  
  if (!tempFile.renameTo(file)) {
    Files.move(tempFile.toPath, file.toPath, StandardCopyOption.REPLACE_EXISTING)
  }
  println(s"Upgraded TSV header to include timing and key count columns: $path")
}

def initializeTSV(path: String): Unit = {
  val file = new File(path)
  
  upgradeLegacyTSV(path)
  
  if (file.exists()) {
    // Read file to check if it has headers and collect data
    val source = Source.fromFile(file)
    val allLines = scala.collection.mutable.ArrayBuffer[String]()
    var hasHeaders = false
    
    try {
      val linesIter = source.getLines()
      if (linesIter.hasNext) {
        val firstLine = linesIter.next()
        // Check if first line looks like headers
        hasHeaders = firstLine.contains("TestName") && 
                    firstLine.contains("Status") && 
                    firstLine.contains("JoinStrategy")
        
        if (!hasHeaders) {
          // First line is data, not headers - save it
          allLines += firstLine
          println(s"WARNING: TSV file exists but doesn't have headers. Will add headers.")
          println(s"         Existing file will be backed up and recreated with headers.")
        }
        // else: first line is headers, don't save it
        
        // Read all remaining lines as data
        while (linesIter.hasNext) {
          allLines += linesIter.next()
        }
      }
    } finally {
      source.close()
    }
    
    if (!hasHeaders) {
      // Backup existing file
      val backupPath = Paths.get(s"${path}.backup_no_headers")
      if (!Files.exists(backupPath)) {
        Files.copy(file.toPath, backupPath, StandardCopyOption.REPLACE_EXISTING)
        println(s"Backed up file without headers to: ${backupPath.toString}")
      }
      
      // Write new file with headers + existing data
      file.getParentFile.mkdirs()
      val writer = new PrintWriter(file)
      try {
        // Write header
        writer.println(expectedHeader)
        // Write existing data
        allLines.foreach(writer.println)
      } finally {
        writer.close()
      }
      println(s"Recreated TSV with headers: $path (${allLines.length} existing rows preserved)")
    } else {
      println(s"TSV already exists with headers, will append to: $path")
    }
  } else {
    // File doesn't exist - create new file with headers
    file.getParentFile.mkdirs()
    val writer = new PrintWriter(file)
    writer.println(expectedHeader)
    writer.close()
    println(s"Initialized TSV: $path")
  }
}

def formatOptionalDouble(value: Option[Double]): String = {
  value.map(v => f"$v%.4f").getOrElse("")
}

def formatKeyCountStats(stats: Option[KeyCountStats]): Seq[String] = {
  stats.map { s =>
    Seq(
      s.maxCount.toString,
      f"${s.stdDev}%.6f",
      f"${s.p99}%.6f",
      f"${s.p95}%.6f",
      f"${s.p90}%.6f",
      f"${s.p75}%.6f",
      f"${s.p50}%.6f"
    )
  }.getOrElse(Seq.fill(7)(""))
}

def appendResultToTSV(path: String, result: BenchmarkResult): Unit = {
  val file = new File(path)
  
  // Ensure file exists with headers before appending
  if (!file.exists()) {
    initializeTSV(path)
  } else {
    // Quick check: verify headers exist
    val source = Source.fromFile(file)
    try {
      val linesIter = source.getLines()
      if (linesIter.hasNext) {
        val firstLine = linesIter.next()
        val hasHeaders = firstLine.contains("TestName") && 
                        firstLine.contains("Status") && 
                        firstLine.contains("JoinStrategy")
        if (!hasHeaders) {
          println(s"WARNING: TSV file exists but doesn't have headers. Initializing now...")
          source.close()
          initializeTSV(path)
        }
      } else {
        // Empty file - needs headers
        source.close()
        initializeTSV(path)
      }
    } finally {
      source.close()
    }
  }
  
  val writer = new PrintWriter(new java.io.FileWriter(path, true))
  
  val line = (Seq(
    result.testName,
    result.status,
    result.joinStrategy,
    result.actualBuildSide,
    // Left side
    result.leftStats.rows,
    result.leftStats.distinctKeys,
    f"${result.leftStats.cardinalityPct}%.6f",
    result.leftStats.keyType,
    result.leftStats.avgKeyBytes,
    f"${result.leftStats.memoryMB}%.2f",
    result.leftStats.distribution,
    // Right side
    result.rightStats.rows,
    result.rightStats.distinctKeys,
    f"${result.rightStats.cardinalityPct}%.6f",
    result.rightStats.keyType,
    result.rightStats.avgKeyBytes,
    f"${result.rightStats.memoryMB}%.2f",
    result.rightStats.distribution,
    // Relationship
    result.numKeyColumns,
    f"${result.keyOverlapPct}%.2f",
    // Results
    f"${result.medianTimeMs}%.4f",
    f"${result.avgTimeMs}%.4f",
    f"${result.stdDevMs}%.4f",
    result.outputRows,
    result.iterations,
    result.errorMessage.replace("\t", " ").replace("\n", " "),
    formatOptionalDouble(result.buildTimeMs),
    formatOptionalDouble(result.probeTimeMs),
    formatOptionalDouble(result.remapStructureMs),
    formatOptionalDouble(result.remapBuildKeysMs),
    formatOptionalDouble(result.remapProbeKeysMs),
    formatOptionalDouble(result.createBuildObjectMs),
    formatOptionalDouble(result.executeJoinMs)
  ) ++ formatKeyCountStats(result.buildKeyCountStats) ++ formatKeyCountStats(result.probeKeyCountStats)).mkString("\t")
  
  writer.println(line)
  writer.close()
}

// ============================================================================
// Config Execution with Cleanup
// ============================================================================

def runConfigWithCleanup(config: TestConfig, configNum: Int, totalConfigs: Int): Seq[BenchmarkResult] = {
  println(s"\n[${configNum}/${totalConfigs}] Processing: ${config.name}")
  val startTime = System.nanoTime()
  
  // Generate data
  println(s"  Generating data...")
  val genStartTime = System.nanoTime()
  generateTestData(config)
  val genTimeMs = (System.nanoTime() - genStartTime) / 1e6
  println(f"  Data generation took: ${genTimeMs}%.2f ms")

  // Calculate key count statistics and output rows (optimized approach - single query)
  println(s"  Calculating key count statistics and output rows...")
  val (leftKeyStats, rightKeyStats, calculatedOutputRows, leftSummary, rightSummary) = try {
    calculateKeyCountStats(config)
  } catch {
    case e: Exception =>
      println(s"    ERROR: Failed to calculate key count stats: ${e.getMessage}")
      e.printStackTrace()
      (
        KeyCountStats(0L, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
        KeyCountStats(0L, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
        0L,
        TableCountSummary(config.leftRows, config.leftDistinctKeys, Map.empty),
        TableCountSummary(config.rightRows, config.rightDistinctKeys, Map.empty)
      )
  }
  
  println(s"  Collecting measured table statistics...")
  val leftStats = collectTableStats(config, "left", leftSummary)
  val rightStats = collectTableStats(config, "right", rightSummary)
  
  // Determine build and probe sides (smaller side is build)
  val (buildKeyStats, probeKeyStats) =
    if (leftSummary.rows <= rightSummary.rows) {
      (Some(leftKeyStats), Some(rightKeyStats))
    } else {
      (Some(rightKeyStats), Some(leftKeyStats))
    }
  
  // Validate join size before running expensive GPU benchmarks
  val (isValid, reason) = validateJoinSize(calculatedOutputRows)
  
  val results = if (!isValid) {
    // Skip benchmark, return FAILED results for both strategies
    println(s"  Skipping benchmark due to validation failure: $reason")
    
    Seq("hash_object", "sort_object_post").map { strategy =>
      BenchmarkResult(
        testName = s"${config.name}_${strategy}",
        joinStrategy = strategy,
        leftStats = leftStats,
        rightStats = rightStats,
        numKeyColumns = config.numKeyColumns,
        keyOverlapPct = config.keyOverlapPct,
        status = "FAILED",
        medianTimeMs = 0.0,
        avgTimeMs = 0.0,
        stdDevMs = 0.0,
        outputRows = calculatedOutputRows,
        actualBuildSide = "UNKNOWN",
        iterations = 0,
        errorMessage = s"SKIPPED: $reason",
        buildKeyCountStats = buildKeyStats,
        probeKeyCountStats = probeKeyStats
      )
    }
  } else {
    // Run both strategies
    println(s"  Running benchmarks...")
    val benchStartTime = System.nanoTime()
    val benchResults = runBothStrategiesForConfig(config, leftStats, rightStats, buildKeyStats, probeKeyStats)
    val benchTimeMs = (System.nanoTime() - benchStartTime) / 1e6
    println(f"  Benchmark time: ${benchTimeMs}%.2f ms")
    benchResults
  }
  
  // Write results to TSV immediately
  results.foreach(r => appendResultToTSV(outputTsvPath, r))
  
  // Print summary
  val successCount = results.count(_.status == "SUCCESS")
  val failCount = results.count(_.status == "FAILED")
  val skippedCount = results.count(_.errorMessage.startsWith("SKIPPED:"))
  if (skippedCount > 0) {
    println(s"  Results: $successCount successful, ${failCount - skippedCount} failed, $skippedCount skipped")
  } else {
    println(s"  Results: $successCount successful, $failCount failed")
  }
  
  // Cleanup: delete the data directories
  try {
    println(s"  Cleaning up data...")
    import java.io.File
    
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
    
    val configDir = new File(s"$baseDir/${config.name}")
    deleteDirectory(configDir)
    println(s"  Cleanup complete")
  } catch {
    case e: Exception =>
      println(s"  Warning: Cleanup failed: ${e.getMessage}")
  }
  
  val totalTimeMs = (System.nanoTime() - startTime) / 1e6
  println(f"  Total config time: ${totalTimeMs}%.2f ms (${totalTimeMs/1000}%.2f sec)")
  
  results
}

// ============================================================================
// Main Execution
// ============================================================================

def runMain(): Unit = {
  println("="*80)
  println("SIMPLIFIED HASH vs SORT BENCHMARK")
  println("="*80)
  println()
  println("Configuration:")
  println("  - Only HashObject vs SortObjectPost")
  println("  - Only Inner joins")
  println("  - Swap always ON, remapping always OFF, distinct join always OFF")
  println("  - Supports: Uniform, Zipf, Gaussian distributions")
  println(s"  - Output TSV: $outputTsvPath")
  println()

  // Initialize TSV output (or append if exists)
  initializeTSV(outputTsvPath)

  // Count existing tests to determine starting ID and random seed
  val existingFile = new File(outputTsvPath)
  val existingCount = if (existingFile.exists()) {
    scala.io.Source.fromFile(existingFile).getLines().size - 1  // -1 for header
  } else {
    0
  }
  val startId = existingCount / 2  // Divide by 2 since we have 2 results per config

  // Reinitialize random generator with seed based on startId
  // This ensures each iteration generates DIFFERENT tests
  // Run 1 (startId=0):  Random(42)
  // Run 2 (startId=20): Random(62)
  // Run 3 (startId=40): Random(82)
  random = new Random(42 + startId)
  println(s"Initializing random generator with seed ${42 + startId} (startId=$startId)")
  println()

  // Check for refinement config
  val refinementConfig = loadRefinementConfig()

  val configs = refinementConfig match {
    case Some(refConfig) =>
      // Generate tests from refinement config
      generateTestsFromRefinementConfig(refConfig, startId)
      
    case None =>
      // Generate random tests with weighting toward low cardinality
      println(s"No refinement config found, generating $numTests random tests")
      println("  Weighted toward low cardinality (<1%, <2%, <5%) where sort typically wins")
      println()
      (0 until numTests).map(i => generateRandomTestConfig(startId + i, weightLowCardinality = true))
  }

  println(s"Generated ${configs.size} test configurations")
  println(s"Processing configs one at a time (generate → test → cleanup)")
  println()

  // Process configs one at a time
  val startTime = System.nanoTime()
  val allResults = configs.zipWithIndex.flatMap { case (config, idx) =>
    val results = runConfigWithCleanup(config, idx + 1, configs.size)
    
    // Print estimated time remaining
    val elapsedSec = (System.nanoTime() - startTime) / 1e9
    val avgSecPerConfig = elapsedSec / (idx + 1)
    val remainingConfigs = configs.size - (idx + 1)
    val estimatedRemainingSec = avgSecPerConfig * remainingConfigs
    val estimatedRemainingMin = estimatedRemainingSec / 60
    
    if (remainingConfigs > 0) {
      println(f"  Estimated time remaining: ${estimatedRemainingMin}%.1f minutes ($remainingConfigs configs left)")
    }
    
    results
  }

  val elapsedSec = (System.nanoTime() - startTime) / 1e9
  val elapsedMin = elapsedSec / 60

  println()
  println("="*80)
  println("BENCHMARK COMPLETE")
  println("="*80)
  println(s"Total results: ${allResults.size}")
  val successCount = allResults.count(_.status == "SUCCESS")
  val failedCount = allResults.count(_.status == "FAILED")
  val skippedCount = allResults.count(_.errorMessage.startsWith("SKIPPED:"))
  val actualFailedCount = failedCount - skippedCount

  println(s"  Success: $successCount")
  if (skippedCount > 0) {
    println(s"  Failed: $actualFailedCount")
    println(s"  Skipped: $skippedCount (join output too large)")
  } else {
    println(s"  Failed: $failedCount")
  }
  println(f"  Total time: ${elapsedMin}%.2f minutes")
  println()

  if (skippedCount > 0) {
    println("Note: Some tests were skipped because the join output would exceed Int.MaxValue rows.")
    println("      This is expected with very low cardinality + large data sizes.")
    println("      The Python analysis will filter these out automatically.")
    println()
  }

  println(s"Results written to: $outputTsvPath")
  println()
  println("Next step:")
  println("  Run: python iterative_model_trainer.py")
  println("  This will analyze results, train a model, and generate refinement config if needed")
  println()
}

// Execute main function after all definitions are processed
runMain()

