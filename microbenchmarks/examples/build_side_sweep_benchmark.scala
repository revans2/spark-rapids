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

// Build Side Sweep Benchmark
//
// This benchmark sweeps through different distributions of rows between left and right
// tables to demonstrate that making the smaller table the build side is always beneficial.
//
// Test Coverage:
// - Join Types: Hash and Sort (both inner joins)
// - Build Side Configurations: Left and Right (explicit, no optimizations)
// - Total Rows: ~92,680 (fixed across all tests, limited to avoid OOM on worst case)
// - Row Distribution: Sweep from 1 row on left to 1 row on right (17 logarithmic steps)
// - Overlap: No overlap (empty result) and full overlap
// - Cardinality: 100% distinct, 50% cardinality, and 100% cardinality (1 key)
// - Key Type: Long (64-bit integer)
// - Optimizations: ALL DISABLED
//
// Note: Total rows limited so worst case (46340 left * 46340 right with 1 key)
// produces ~2.1B output rows, just under Int.MaxValue
//
// Expected Behavior:
// - When left < right, LeftBuild should be faster
// - When left > right, RightBuild should be faster
// - Some tests are skipped by default (OOM cases, known broken tests)
//
// Configuration:
// - Set runBrokenTests = true to run tests that are known to have issues
// - Known broken tests are documented in the knownBrokenTests list
// - These tests are preserved for bug reproduction but skipped by default
//
// Output: TSV file with detailed timing breakdowns for analysis

import com.nvidia.spark.rapids.benchmarks.JoinBenchmarkDataGen._
import com.nvidia.spark.rapids.benchmarks.{JoinBenchmarkRunner => JBR}
import org.apache.spark.sql.tests.datagen._
import org.apache.spark.sql.SparkSession
import java.io.{File, PrintWriter}
import scala.util.Random

val baseDir = "/data/tmp/build_side_sweep"
val outputTsvPath = s"$baseDir/build_side_sweep.tsv"
val random = new Random(42)

// ============================================================================
// Configuration
// ============================================================================

// Total rows limited to avoid OOM on worst case (1 key, full overlap)
// Worst case: ~46340 left * ~46340 right = ~2.1B output rows (just under Int.MaxValue)
val totalRows = 92680L  // Fixed total
val benchmarkIterations = 10

// Set to true to run tests that are known to have issues (for bug reproduction)
val runBrokenTests = false

// Distribution sweep: logarithmic steps from very small to very large
// These represent the number of rows on the LEFT side
val leftRowDistribution = Seq(
  1L,           // Extreme: 1 row left
  10L,
  100L,
  1000L,        // ~1%
  4634L,        // ~5%
  9268L,        // ~10%
  18536L,       // ~20%
  30893L,       // ~33%
  46340L,       // ~50% (balanced)
  61787L,       // ~67%
  74144L,       // ~80%
  83412L,       // ~90%
  88046L,       // ~95%
  91753L,       // ~99%
  92580L,       // ~99.9%
  92670L,       // ~99.99%
  92679L        // Extreme: 1 row right
)

// Cardinality scenarios
sealed trait CardinalitySpec {
  def name: String
  def description: String
  def computeCardinality(numRows: Long): Long
}

case object FullyDistinct extends CardinalitySpec {
  val name = "100pct_distinct"
  val description = "All keys are unique"
  def computeCardinality(numRows: Long): Long = numRows
}

case object HalfCardinality extends CardinalitySpec {
  val name = "50pct_cardinality"
  val description = "50% of keys are unique"
  def computeCardinality(numRows: Long): Long = Math.max(1, numRows / 2)
}

case object SingleKey extends CardinalitySpec {
  val name = "100pct_cardinality"
  val description = "Only 1 unique key (expect OOM on full overlap)"
  def computeCardinality(numRows: Long): Long = 1L
}

val cardinalityScenarios = Seq(FullyDistinct, HalfCardinality, SingleKey)

// Overlap scenarios
sealed trait OverlapSpec {
  def name: String
  def description: String
}

case object NoOverlap extends OverlapSpec {
  val name = "no_overlap"
  val description = "Disjoint key spaces (empty result)"
}

case object FullOverlap extends OverlapSpec {
  val name = "full_overlap"
  val description = "Complete key overlap"
}

val overlapScenarios = Seq(NoOverlap, FullOverlap)

// Known broken test configurations (to be skipped unless runBrokenTests = true)
// Each entry specifies a test configuration that has known issues
case class BrokenTestConfig(
  leftRows: Long,
  cardinality: String,
  overlap: String,
  strategy: String,
  buildSide: String,
  issueDescription: String
)

val knownBrokenTests: Seq[BrokenTestConfig] = Seq(
  // Add broken test configurations here as you discover them
  // Example:
  // BrokenTestConfig(
  //   leftRows = 50000L,
  //   cardinality = "100pct_distinct",
  //   overlap = "no_overlap",
  //   strategy = "Hash",
  //   buildSide = "LeftBuild",
  //   issueDescription = "CUDA illegal memory access - cuDF std::bad_alloc during data generation"
  // )
)

def isTestBroken(leftRows: Long, cardinality: CardinalitySpec, overlap: OverlapSpec, 
                 strategyName: String, buildSideName: String): Option[String] = {
  knownBrokenTests.find { broken =>
    broken.leftRows == leftRows &&
    broken.cardinality == cardinality.name &&
    broken.overlap == overlap.name &&
    broken.strategy == strategyName &&
    broken.buildSide == buildSideName
  }.map(_.issueDescription)
}

// Join strategies
val strategies = Seq(
  ("Hash", JBR.HashObjectWithPostStrategy),
  ("Sort", JBR.SortObjectWithPostStrategy)
)

// Build sides (explicit, no auto-selection)
val buildSides = Seq(
  ("LeftBuild", JBR.LeftBuild),
  ("RightBuild", JBR.RightBuild)
)

// ============================================================================
// Data Generation Functions
// ============================================================================

/**
 * Generate left and right tables for a specific test configuration.
 * Uses JoinBenchmarkDataGen API for proper data generation.
 */
def generateTestTables(
    leftRows: Long,
    rightRows: Long,
    cardinality: CardinalitySpec,
    overlap: OverlapSpec,
    dataDir: String): (String, String) = {
  
  val leftCardinality = cardinality.computeCardinality(leftRows)
  val rightCardinality = cardinality.computeCardinality(rightRows)
  
  val testDataDir = s"$dataDir/left_${leftRows}_right_${rightRows}_${cardinality.name}_${overlap.name}"
  new File(testDataDir).mkdirs()
  
  val leftPath = s"$testDataDir/left"
  val rightPath = s"$testDataDir/right"
  
  // Check if already exists
  if (new File(leftPath).exists() && new File(rightPath).exists()) {
    println(s"  Tables already exist, skipping generation")
    return (leftPath, rightPath)
  }
  
  println(s"  Generating tables: left=$leftRows rows ($leftCardinality keys), " +
    s"right=$rightRows rows ($rightCardinality keys), overlap=${overlap.name}")
  
  // Configure seed ranges based on overlap requirement
  // For no overlap: use disjoint seed ranges
  // For full overlap: use same seed range and same key group
  val (leftMinSeed, leftMaxSeed, rightMinSeed, rightMaxSeed, keyGroupId) = overlap match {
    case NoOverlap => 
      // Disjoint seed ranges: left uses 0 to leftCardinality-1, right uses leftCardinality onwards
      (0L, leftCardinality - 1, leftCardinality, leftCardinality + rightCardinality - 1, 1)
    case FullOverlap =>
      // Same seed range for both (use the smaller cardinality to ensure full overlap)
      val minCardinality = Math.min(leftCardinality, rightCardinality)
      (0L, minCardinality - 1, 0L, minCardinality - 1, 1)
    case _ => throw new IllegalArgumentException(s"Unknown overlap type: $overlap")
  }
  
  println(s"    Left seeds: $leftMinSeed to $leftMaxSeed")
  println(s"    Right seeds: $rightMinSeed to $rightMaxSeed")
  
  // Create left table configuration
  val leftConfig = TableGenConfig(
    numRows = leftRows,
    keyColumns = Seq(
      KeyColumnSpec(
        name = "join_key",
        dataType = "long",
        minSeed = leftMinSeed,
        maxSeed = leftMaxSeed,
        distribution = FlatDistribution(),
        valueRange = None,
        nullProbability = 0.0
      )
    ),
    payloadColumns = Seq.empty,  // No payload columns needed
    outputPath = leftPath,
    numOutputFiles = 1
  )
  
  // Create right table configuration
  val rightConfig = TableGenConfig(
    numRows = rightRows,
    keyColumns = Seq(
      KeyColumnSpec(
        name = "join_key",
        dataType = "long",
        minSeed = rightMinSeed,
        maxSeed = rightMaxSeed,
        distribution = FlatDistribution(),
        valueRange = None,
        nullProbability = 0.0
      )
    ),
    payloadColumns = Seq.empty,
    outputPath = rightPath,
    numOutputFiles = 1
  )
  
  // Generate both tables with the same key group (ensures same seeds produce same values)
  generateTable(leftConfig, Some(keyGroupId), spark)
  generateTable(rightConfig, Some(keyGroupId), spark)
  
  (leftPath, rightPath)
}

// ============================================================================
// Benchmark Execution
// ============================================================================

/**
 * Run a single benchmark configuration and return results.
 */
def runBenchmark(
    testName: String,
    leftPath: String,
    rightPath: String,
    strategyName: String,
    strategy: JBR.JoinStrategySpec,
    buildSideName: String,
    buildSide: JBR.BuildSideSpec,
    leftRows: Long,
    rightRows: Long): JBR.BenchmarkResults = {
  
  println(s"\n========================================")
  println(s"Running: $testName")
  println(s"Strategy: $strategyName, BuildSide: $buildSideName")
  println(s"Left: $leftRows rows, Right: $rightRows rows")
  println(s"========================================")
  
  val config = JBR.JoinBenchmarkConfig(
    testName = testName,
    leftParquetPath = leftPath,
    rightParquetPath = rightPath,
    joinType = JBR.InnerJoin,
    joinStrategy = strategy,
    buildSide = buildSide,
    optimizations = JBR.JoinOptimizations(
      allowBuildSideSwap = false,         // CRITICAL: Disable to test explicit build sides
      remapComplexKeysToInts = false,
      useDistinctJoin = false,
      cacheJoinObject = false,
      cacheRemapping = false,
      cacheDistinctFlag = false
    ),
    conditionalFilter = None,
    leftKeyIndices = Seq(0),
    rightKeyIndices = Seq(0),
    iterations = benchmarkIterations,
    numThreads = 1,
    printHeader = false,  // We'll print our own header
    collectDetailedTimings = true  // Get build/probe breakdown
  )
  
  JBR.runBenchmark(config, spark)
}

/**
 * Helper functions to format result components as strings.
 */
def formatOptimizations(opts: JBR.JoinOptimizations): String = {
  val parts = scala.collection.mutable.ArrayBuffer[String]()
  if (opts.allowBuildSideSwap) parts += "swap"
  if (opts.remapComplexKeysToInts) parts += "remap"
  if (opts.useDistinctJoin) parts += "distinct"
  if (opts.cacheJoinObject) parts += "cache"
  if (opts.cacheRemapping) parts += "cache-remap"
  if (opts.cacheDistinctFlag) parts += "cache-distinct"
  if (parts.isEmpty) "none" else parts.mkString(",")
}

def formatBuildSideConfig(buildSide: JBR.BuildSideSpec): String = buildSide match {
  case JBR.LeftBuild => "Left"
  case JBR.RightBuild => "Right"
  case JBR.AutoPickSmallerIfAllowed => "Auto(Smaller)"
  case JBR.AutoMeetJoinRequirement => "Auto(Required)"
}

def formatJoinType(joinType: JBR.JoinTypeSpec): String = joinType match {
  case JBR.InnerJoin => "Inner"
  case JBR.LeftOuterJoin => "LeftOuter"
  case JBR.RightOuterJoin => "RightOuter"
  case JBR.FullOuterJoin => "FullOuter"
  case JBR.LeftSemiJoin => "LeftSemi"
  case JBR.LeftAntiJoin => "LeftAnti"
}

def formatJoinStrategy(strategy: JBR.JoinStrategySpec): String = strategy match {
  case JBR.HashObjectStrategy => "HashObject"
  case JBR.HashObjectWithPostStrategy => "HashObjectPost"
  case JBR.SortObjectWithPostStrategy => "SortObjectPost"
  case JBR.HashDirectStrategy => "HashDirect"
  case JBR.HashDirectWithPostStrategy => "HashDirectPost"
  case JBR.SortDirectWithPostStrategy => "SortDirectPost"
}

/**
 * Format benchmark results as TSV line.
 */
def formatResultsTSV(results: JBR.BenchmarkResults): String = {
  if (results.status == JBR.SUCCESS) {
    val optimizations = formatOptimizations(results.optimizations)
    val joinType = formatJoinType(results.joinType)
    val strategy = formatJoinStrategy(results.joinStrategy)
    val buildSideConfig = formatBuildSideConfig(results.buildSideConfig)
    val actualBuild = results.actualBuildSide.getOrElse("N/A")
    
    val detailedTimingsStr = results.detailedTimings match {
      case Some(dt) =>
        s"\t${f"${dt.remapStructureBuildMs}%.3f"}\t${f"${dt.remapBuildKeysMs}%.3f"}\t" +
        s"${f"${dt.remapProbeKeysMs}%.3f"}\t${f"${dt.createBuildObjectMs}%.3f"}\t" +
        s"${f"${dt.executeJoinMs}%.3f"}"
      case None =>
        "\t\t\t\t\t"
    }
    
    s"${results.testName}\t${results.status}\t" +
      s"${results.leftRows}\t${results.rightRows}\t${results.outputRows}\t" +
      s"${results.numThreads}\t${results.iterations}\t" +
      s"${f"${results.wallClockMs}%.2f"}\t${f"${results.averageMs}%.2f"}\t" +
      s"${f"${results.medianMs}%.2f"}\t${f"${results.minMs}%.2f"}\t" +
      s"${f"${results.maxMs}%.2f"}\t${f"${results.stdDevMs}%.2f"}\t" +
      s"$joinType\t$strategy\t$buildSideConfig\t$actualBuild\t$optimizations" +
      detailedTimingsStr
  } else {
    s"${results.testName}\t${results.status}\t" +
      s"${results.errorMessage.getOrElse("Unknown error")}"
  }
}

/**
 * Format duration in human-readable form.
 */
def formatDuration(seconds: Long): String = {
  val hours = seconds / 3600
  val minutes = (seconds % 3600) / 60
  val secs = seconds % 60
  if (hours > 0) {
    f"${hours}h ${minutes}%02dm ${secs}%02ds"
  } else if (minutes > 0) {
    f"${minutes}m ${secs}%02ds"
  } else {
    f"${secs}s"
  }
}

/**
 * Delete a directory recursively.
 */
def deleteDirectory(dir: File): Unit = {
  if (dir.exists()) {
    if (dir.isDirectory) {
      dir.listFiles().foreach(deleteDirectory)
    }
    dir.delete()
  }
}

/**
 * Run full benchmark suite.
 */
def runFullBenchmark(): Unit = {
  println(s"Starting Build Side Sweep Benchmark")
  println(s"Output: $outputTsvPath")
  println(s"Total rows per test: $totalRows")
  println(s"Iterations per test: $benchmarkIterations")
  
  // Create output directory
  new File(baseDir).mkdirs()
  
  // Track data directories for cleanup
  val generatedDataDirs = scala.collection.mutable.Set[String]()
  
  // Track test timings for ETA calculation
  val testTimings = scala.collection.mutable.ArrayBuffer[Long]()
  val benchmarkStartTime = System.currentTimeMillis()
  
  // Open TSV file for writing
  val tsvWriter = new PrintWriter(new File(outputTsvPath))
  
  try {
    // Write TSV header
    tsvWriter.println("TestName\tStatus\tLeftRows\tRightRows\tOutputRows\t" +
      "NumThreads\tIterations\tWallClockMs\tAvgTimeMs\tMedianTimeMs\t" +
      "MinTimeMs\tMaxTimeMs\tStdDevMs\tJoinType\tStrategy\t" +
      "BuildSideConfig\tActualBuildSide\tOptimizations\t" +
      "RemapStructureBuildMs\tRemapBuildKeysMs\tRemapProbeKeysMs\t" +
      "CreateBuildObjectMs\tExecuteJoinMs")
    tsvWriter.flush()
    
    var testCount = 0
    val totalTests = leftRowDistribution.size * cardinalityScenarios.size * 
                     overlapScenarios.size * strategies.size * buildSides.size
    
    // Iterate through all test combinations
    for {
      leftRows <- leftRowDistribution
      cardinality <- cardinalityScenarios
      overlap <- overlapScenarios
      (strategyName, strategy) <- strategies
      (buildSideName, buildSide) <- buildSides
    } {
      val testStartTime = System.currentTimeMillis()
      testCount += 1
      val rightRows = totalRows - leftRows
      
      // Create test name
      val leftPct = (leftRows.toDouble / totalRows * 100).toInt
      val rightPct = (rightRows.toDouble / totalRows * 100).toInt
      val testName = f"L${leftPct}%03d_R${rightPct}%03d_${cardinality.name}_${overlap.name}_${strategyName}_${buildSideName}"
      
      println(s"\n\n=== TEST $testCount / $totalTests ===")
      println(s"Test: $testName")
      
      // Calculate and display ETA
      if (testCount > 1) {
        val avgTestTime = testTimings.sum.toDouble / testTimings.size
        val remainingTests = totalTests - testCount + 1
        val etaSeconds = (avgTestTime * remainingTests / 1000).toLong
        val elapsedSeconds = (System.currentTimeMillis() - benchmarkStartTime) / 1000
        println(s"Progress: ${testCount}/$totalTests | Elapsed: ${formatDuration(elapsedSeconds)} | ETA: ${formatDuration(etaSeconds)}")
      }
      
      // Check if this is a known broken test
      val brokenReason = isTestBroken(leftRows, cardinality, overlap, strategyName, buildSideName)
      val shouldSkipBrokenTest = brokenReason.isDefined && !runBrokenTests
      
      // Skip the OOM case for single key with full overlap
      // (but allow it for no overlap, which produces empty results)
      // if (cardinality == SingleKey && overlap == FullOverlap) {
      //   println(s"SKIPPING OOM-prone test: $testName")
      //   tsvWriter.println(s"$testName\tSKIPPED_OOM\t$leftRows\t$rightRows\t0\t1\t0\t0.0\t0.0\t0.0\t0.0\t0.0\t0.0\t" +
      //     s"Inner\t$strategyName\t$buildSideName\tN/A\tnone\t\t\t\t\t")
      //   tsvWriter.flush()
      // } else 
      if (shouldSkipBrokenTest) {
        println(s"SKIPPING KNOWN BROKEN TEST: $testName")
        println(s"  Reason: ${brokenReason.get}")
        println(s"  (Set runBrokenTests = true to run this test)")
        tsvWriter.println(s"$testName\tSKIPPED_BROKEN\t$leftRows\t$rightRows\t0\t1\t0\t0.0\t0.0\t0.0\t0.0\t0.0\t0.0\t" +
          s"Inner\t$strategyName\t$buildSideName\tN/A\tnone\t\t\t\t\t")
        tsvWriter.flush()
      } else {
        try {
          // Generate data and track directory
          val testDataDir = s"$baseDir/left_${leftRows}_right_${rightRows}_${cardinality.name}_${overlap.name}"
          generatedDataDirs += testDataDir
          
          val (leftPath, rightPath) = generateTestTables(
            leftRows, rightRows, cardinality, overlap, baseDir)
          
          // Run benchmark and get results
          val results = runBenchmark(
            testName, leftPath, rightPath,
            strategyName, strategy,
            buildSideName, buildSide,
            leftRows, rightRows)
          
          // Write TSV line
          tsvWriter.println(formatResultsTSV(results))
          tsvWriter.flush()
          
          // Give system time to recover between tests
          Thread.sleep(1000)
          
        } catch {
          case e: OutOfMemoryError =>
            println(s"OOM ERROR in test: $testName")
            tsvWriter.println(s"$testName\tOOM_ERROR\t$leftRows\t$rightRows\t0\t1\t0\t0.0\t0.0\t0.0\t0.0\t0.0\t0.0\t" +
              s"Inner\t$strategyName\t$buildSideName\tN/A\tnone\t\t\t\t\t")
            tsvWriter.flush()
            System.gc()  // Try to recover
            Thread.sleep(2000)
          case e: Exception =>
            println(s"ERROR in test: $testName - ${e.getMessage}")
            e.printStackTrace()
            tsvWriter.println(s"$testName\tERROR\t$leftRows\t$rightRows\t0\t1\t0\t0.0\t0.0\t0.0\t0.0\t0.0\t0.0\t" +
              s"Inner\t$strategyName\t$buildSideName\tN/A\tnone\t\t\t\t\t")
            tsvWriter.flush()
        }
      }
      
      // Track test timing
      val testDuration = System.currentTimeMillis() - testStartTime
      testTimings += testDuration
      println(s"Test duration: ${testDuration / 1000}s")
    }
    
    println(s"\n" + "="*80)
    println("All tests complete!")
    println("="*80)
    
    // Print summary statistics
    val totalElapsedSeconds = (System.currentTimeMillis() - benchmarkStartTime) / 1000
    val completedTests = testTimings.size
    val skippedTests = totalTests - completedTests
    
    println(s"\nBenchmark Summary:")
    println(s"  Total tests: $totalTests")
    println(s"  Completed: $completedTests")
    println(s"  Skipped: $skippedTests")
    println(s"  Total time: ${formatDuration(totalElapsedSeconds)}")
    if (completedTests > 0) {
      println(s"  Average test time: ${testTimings.sum / testTimings.size / 1000}s")
    }
    
    println(s"\n" + "="*80)
    println("Cleaning up input data...")
    println("="*80)
    
    // Clean up generated data directories
    generatedDataDirs.foreach { dirPath =>
      println(s"Deleting: $dirPath")
      try {
        deleteDirectory(new File(dirPath))
      } catch {
        case e: Exception =>
          println(s"Warning: Could not delete $dirPath - ${e.getMessage}")
      }
    }
    
  } finally {
    tsvWriter.close()
  }
  
  println(s"\nBenchmark complete! Results written to: $outputTsvPath")
  println(s"Input data has been cleaned up to save disk space.")
}

// ============================================================================
// Main Execution
// ============================================================================

println("=" * 80)
println("Build Side Sweep Benchmark")
println("=" * 80)
println()
println("This benchmark demonstrates the importance of choosing the smaller table")
println("as the build side in hash and sort-merge joins.")
println()
println(s"Configuration:")
println(s"  Total rows per test: $totalRows")
println(s"  Iterations: $benchmarkIterations")
println(s"  Distribution steps: ${leftRowDistribution.size}")
println(s"  Cardinality scenarios: ${cardinalityScenarios.size}")
println(s"  Overlap scenarios: ${overlapScenarios.size}")
println(s"  Join strategies: ${strategies.size}")
println(s"  Build sides: ${buildSides.size}")
println(s"  Total tests: ${leftRowDistribution.size * cardinalityScenarios.size * overlapScenarios.size * strategies.size * buildSides.size}")
println()

// Run the benchmark
runFullBenchmark()

println("\nDone! You can now analyze the results in a spreadsheet.")
println(s"TSV file: $outputTsvPath")
println()
println("Recommended analysis:")
println("1. Plot average time vs left row percentage for each strategy/build side combo")
println("2. Compare LeftBuild vs RightBuild for each left/right distribution")
println("3. Examine detailed timings to see build vs probe time breakdown")
println("4. Look for crossover point where RightBuild becomes faster than LeftBuild")

