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

// Join Type Sweep Benchmark
//
// This benchmark tests different join types across various table size distributions
// with 100% cardinality (single key) to understand join type behavior.
//
// Test Coverage:
// - Join Types: Inner, FullOuter, LeftOuter, LeftSemi, LeftAnti
// - Strategies: HashObject AND HashObjectWithPost (both tested for comparison)
// - Total Rows: ~92,680 (fixed across all tests)
// - Row Distribution: Sweep from 1 row on left to 1 row on right (17 steps)
// - Cardinality: 100% cardinality (1 key only)
// - Overlap: No overlap (empty result) and full overlap
// - Key Type: Long (64-bit integer)
// - Optimizations: ALL DISABLED
//
// Build Side Configuration:
// - HashObject:
//   * Inner, FullOuter: Test both LeftBuild and RightBuild
//   * LeftOuter, LeftSemi, LeftAnti: Use RightBuild (required for left-side joins)
// - HashObjectWithPost:
//   * Inner, FullOuter: Test both LeftBuild and RightBuild
//   * LeftOuter: Test BOTH LeftBuild and RightBuild (post-processing enables build side flexibility!)
//   * LeftSemi, LeftAnti: Use RightBuild
//
// Output: TSV file with detailed timing breakdowns

import com.nvidia.spark.rapids.benchmarks.JoinBenchmarkDataGen._
import com.nvidia.spark.rapids.benchmarks.{JoinBenchmarkRunner => JBR}
import org.apache.spark.sql.tests.datagen._
import org.apache.spark.sql.SparkSession
import java.io.{File, PrintWriter}
import scala.util.Random

val baseDir = "/data/tmp/join_type_sweep"
val outputTsvPath = s"$baseDir/join_type_sweep.tsv"
val random = new Random(42)

// ============================================================================
// Configuration
// ============================================================================

// Total rows limited to avoid OOM on worst case (1 key, full overlap)
val totalRows = 92680L
val benchmarkIterations = 10

// Distribution sweep (same as build_side_sweep)
val leftRowDistribution = Seq(
  1L, 10L, 100L, 1000L, 4634L, 9268L, 18536L, 30893L, 46340L,
  61787L, 74144L, 83412L, 88046L, 91753L, 92580L, 92670L, 92679L
)

// Overlap scenarios
sealed trait OverlapSpec {
  def name: String
  def description: String
}

case object NoOverlap extends OverlapSpec {
  val name = "no_overlap"
  val description = "Disjoint key spaces (empty or partial result)"
}

case object FullOverlap extends OverlapSpec {
  val name = "full_overlap"
  val description = "Complete key overlap"
}

val overlapScenarios = Seq(NoOverlap, FullOverlap)

// Join type configurations
case class JoinTypeConfig(
  name: String,
  joinType: JBR.JoinTypeSpec,
  strategy: JBR.JoinStrategySpec,
  testBuildSides: Seq[String],  // Which build sides to test
  description: String
)

// Create configs for both HashObject and HashObjectWithPost
val joinTypeConfigs = Seq(
  // HashObject configs (original tests)
  JoinTypeConfig(
    name = "Inner",
    joinType = JBR.InnerJoin,
    strategy = JBR.HashObjectStrategy,
    testBuildSides = Seq("LeftBuild", "RightBuild"),
    description = "Inner join - both build sides supported"
  ),
  JoinTypeConfig(
    name = "FullOuter",
    joinType = JBR.FullOuterJoin,
    strategy = JBR.HashObjectStrategy,
    testBuildSides = Seq("LeftBuild", "RightBuild"),
    description = "Full outer join - both build sides supported"
  ),
  JoinTypeConfig(
    name = "LeftOuter",
    joinType = JBR.LeftOuterJoin,
    strategy = JBR.HashObjectStrategy,
    testBuildSides = Seq("RightBuild"),
    description = "Left outer join - requires right as build side"
  ),
  JoinTypeConfig(
    name = "LeftSemi",
    joinType = JBR.LeftSemiJoin,
    strategy = JBR.HashObjectStrategy,
    testBuildSides = Seq("RightBuild"),
    description = "Left semi join - requires right as build side"
  ),
  JoinTypeConfig(
    name = "LeftAnti",
    joinType = JBR.LeftAntiJoin,
    strategy = JBR.HashObjectStrategy,
    testBuildSides = Seq("RightBuild"),
    description = "Left anti join - requires right as build side"
  ),
  
  // HashObjectWithPost configs (new tests with post-processing)
  JoinTypeConfig(
    name = "Inner",
    joinType = JBR.InnerJoin,
    strategy = JBR.HashObjectWithPostStrategy,
    testBuildSides = Seq("LeftBuild", "RightBuild"),
    description = "Inner join with post-processing - both build sides supported"
  ),
  JoinTypeConfig(
    name = "FullOuter",
    joinType = JBR.FullOuterJoin,
    strategy = JBR.HashObjectWithPostStrategy,
    testBuildSides = Seq("LeftBuild", "RightBuild"),
    description = "Full outer join with post-processing - both build sides supported"
  ),
  JoinTypeConfig(
    name = "LeftOuter",
    joinType = JBR.LeftOuterJoin,
    strategy = JBR.HashObjectWithPostStrategy,
    testBuildSides = Seq("LeftBuild", "RightBuild"),  // Both sides now possible with post-processing!
    description = "Left outer join with post-processing - both build sides now supported"
  ),
  JoinTypeConfig(
    name = "LeftSemi",
    joinType = JBR.LeftSemiJoin,
    strategy = JBR.HashObjectWithPostStrategy,
    testBuildSides = Seq("RightBuild"),
    description = "Left semi join with post-processing - requires right as build side"
  ),
  JoinTypeConfig(
    name = "LeftAnti",
    joinType = JBR.LeftAntiJoin,
    strategy = JBR.HashObjectWithPostStrategy,
    testBuildSides = Seq("RightBuild"),
    description = "Left anti join with post-processing - requires right as build side"
  )
)

// ============================================================================
// Data Generation
// ============================================================================

/**
 * Generate test tables with 100% cardinality (1 key).
 */
def generateTestTables(
    leftRows: Long,
    rightRows: Long,
    overlap: OverlapSpec,
    dataDir: String): (String, String) = {
  
  // 100% cardinality = 1 unique key
  val leftCardinality = 1L
  val rightCardinality = 1L
  
  val testDataDir = s"$dataDir/left_${leftRows}_right_${rightRows}_${overlap.name}"
  new File(testDataDir).mkdirs()
  
  val leftPath = s"$testDataDir/left"
  val rightPath = s"$testDataDir/right"
  
  // Check if already exists
  if (new File(leftPath).exists() && new File(rightPath).exists()) {
    println(s"  Tables already exist, skipping generation")
    return (leftPath, rightPath)
  }
  
  println(s"  Generating tables: left=$leftRows rows, right=$rightRows rows, overlap=${overlap.name}")
  
  // Configure seed ranges based on overlap
  val (leftMinSeed, leftMaxSeed, rightMinSeed, rightMaxSeed, keyGroupId) = overlap match {
    case NoOverlap =>
      // Disjoint seed ranges for no overlap
      (0L, 0L, 1L, 1L, 1)
    case FullOverlap =>
      // Same seed range for full overlap
      (0L, 0L, 0L, 0L, 1)
    case _ => throw new IllegalArgumentException(s"Unknown overlap type: $overlap")
  }
  
  println(s"    Left seeds: $leftMinSeed to $leftMaxSeed (cardinality=1)")
  println(s"    Right seeds: $rightMinSeed to $rightMaxSeed (cardinality=1)")
  
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
    payloadColumns = Seq.empty,
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
  
  // Generate both tables
  generateTable(leftConfig, Some(keyGroupId), spark)
  generateTable(rightConfig, Some(keyGroupId), spark)
  
  (leftPath, rightPath)
}

// ============================================================================
// Benchmark Execution
// ============================================================================

def getBuildSideSpec(buildSideName: String): JBR.BuildSideSpec = buildSideName match {
  case "LeftBuild" => JBR.LeftBuild
  case "RightBuild" => JBR.RightBuild
  case _ => throw new IllegalArgumentException(s"Unknown build side: $buildSideName")
}

/**
 * Run a single benchmark and return results.
 */
def runBenchmark(
    testName: String,
    leftPath: String,
    rightPath: String,
    joinTypeConfig: JoinTypeConfig,
    buildSideName: String,
    leftRows: Long,
    rightRows: Long): JBR.BenchmarkResults = {
  
  println(s"\n========================================")
  println(s"Running: $testName")
  println(s"Join Type: ${joinTypeConfig.name}, BuildSide: $buildSideName")
  println(s"Left: $leftRows rows, Right: $rightRows rows")
  println(s"========================================")
  
  val config = JBR.JoinBenchmarkConfig(
    testName = testName,
    leftParquetPath = leftPath,
    rightParquetPath = rightPath,
    joinType = joinTypeConfig.joinType,
    joinStrategy = joinTypeConfig.strategy,  // Use strategy from config (HashObject or HashObjectWithPost)
    buildSide = getBuildSideSpec(buildSideName),
    optimizations = JBR.JoinOptimizations(
      allowBuildSideSwap = false,
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
    printHeader = false,
    collectDetailedTimings = true
  )
  
  JBR.runBenchmark(config, spark)
}

/**
 * Format results helpers (same as build_side_sweep).
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
  println(s"Starting Join Type Sweep Benchmark")
  println(s"Output: $outputTsvPath")
  println(s"Total rows per test: $totalRows")
  println(s"Iterations per test: $benchmarkIterations")
  
  new File(baseDir).mkdirs()
  
  val generatedDataDirs = scala.collection.mutable.Set[String]()
  val testTimings = scala.collection.mutable.ArrayBuffer[Long]()
  val benchmarkStartTime = System.currentTimeMillis()
  
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
    val totalBuildSides = joinTypeConfigs.map(_.testBuildSides.size).sum
    val totalTests = leftRowDistribution.size * overlapScenarios.size * totalBuildSides
    
    // Iterate through all test combinations
    for {
      leftRows <- leftRowDistribution
      overlap <- overlapScenarios
      joinTypeConfig <- joinTypeConfigs
      buildSideName <- joinTypeConfig.testBuildSides
    } {
      val testStartTime = System.currentTimeMillis()
      testCount += 1
      val rightRows = totalRows - leftRows
      
      val leftPct = (leftRows.toDouble / totalRows * 100).toInt
      val rightPct = (rightRows.toDouble / totalRows * 100).toInt
      val strategyName = formatJoinStrategy(joinTypeConfig.strategy)
      val testName = f"L${leftPct}%03d_R${rightPct}%03d_100pct_card_${overlap.name}_${joinTypeConfig.name}_${buildSideName}_${strategyName}"
      
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
      
      try {
        // Generate data and track directory
        val testDataDir = s"$baseDir/left_${leftRows}_right_${rightRows}_${overlap.name}"
        generatedDataDirs += testDataDir
        
        val (leftPath, rightPath) = generateTestTables(leftRows, rightRows, overlap, baseDir)
        
        // Run benchmark
        val results = runBenchmark(
          testName, leftPath, rightPath,
          joinTypeConfig, buildSideName,
          leftRows, rightRows)
        
        // Write TSV line
        tsvWriter.println(formatResultsTSV(results))
        tsvWriter.flush()
        
        Thread.sleep(1000)
        
      } catch {
        case e: OutOfMemoryError =>
          println(s"OOM ERROR in test: $testName")
          val strategyName = formatJoinStrategy(joinTypeConfig.strategy)
          tsvWriter.println(s"$testName\tOOM_ERROR\t$leftRows\t$rightRows\t0\t1\t0\t0.0\t0.0\t0.0\t0.0\t0.0\t0.0\t" +
            s"${joinTypeConfig.name}\t$strategyName\t$buildSideName\tN/A\tnone\t\t\t\t\t")
          tsvWriter.flush()
          System.gc()
          Thread.sleep(2000)
        case e: Exception =>
          println(s"ERROR in test: $testName - ${e.getMessage}")
          e.printStackTrace()
          val strategyName = formatJoinStrategy(joinTypeConfig.strategy)
          tsvWriter.println(s"$testName\tERROR\t$leftRows\t$rightRows\t0\t1\t0\t0.0\t0.0\t0.0\t0.0\t0.0\t0.0\t" +
            s"${joinTypeConfig.name}\t$strategyName\t$buildSideName\tN/A\tnone\t\t\t\t\t")
          tsvWriter.flush()
      }
      
      val testDuration = System.currentTimeMillis() - testStartTime
      testTimings += testDuration
      println(s"Test duration: ${testDuration / 1000}s")
    }
    
    println(s"\n" + "="*80)
    println("All tests complete!")
    println("="*80)
    
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
println("Join Type Sweep Benchmark")
println("=" * 80)
println()
println("This benchmark tests different join types across various table size distributions")
println("with 100% cardinality (single key) to understand join type behavior.")
println()
println(s"Configuration:")
println(s"  Total rows per test: $totalRows")
println(s"  Iterations: $benchmarkIterations")
println(s"  Distribution steps: ${leftRowDistribution.size}")
println(s"  Overlap scenarios: ${overlapScenarios.size}")
println(s"  Join types: ${joinTypeConfigs.map(_.name).mkString(", ")}")
val totalBuildSides = joinTypeConfigs.map(_.testBuildSides.size).sum
val totalTests = leftRowDistribution.size * overlapScenarios.size * totalBuildSides
println(s"  Total tests: $totalTests")
println()

runFullBenchmark()

println("\nDone!")

