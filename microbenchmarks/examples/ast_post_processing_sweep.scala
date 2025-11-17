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

// AST Post-Processing Sweep Benchmark
//
// This benchmark compares HashDirectStrategy vs HashObjectWithPostStrategy
// with AST filtering across different configurations:
// - Strategies: HashDirect, HashObjectWithPost (BOTH tested)
// - Join Types: Inner, LeftOuter, RightOuter, FullOuter, LeftSemi, LeftAnti
// - AST Complexity: None (baseline), Simple, Medium, Complex
// - Table Size Distribution: Sweep from 1% left to 99% left
//
// Key Design Decisions:
// 1. Tests BOTH HashDirectStrategy AND HashObjectWithPostStrategy
// 2. Both strategies support AST filtering - we compare their performance
// 3. Enables allowBuildSideSwap = true - auto-picks optimal build side
// 4. Adds payload columns (val1, val2, val3) for AST filtering
// 5. Pre-generates AST expressions once to avoid memory leaks (816 reuses vs 816 creates)
// 6. Skips HashDirect + FullOuter (not used in production this way)
//
// This shows how each strategy scales with AST complexity.
//
// Output: TSV file with detailed timing breakdowns

import com.nvidia.spark.rapids.benchmarks.JoinBenchmarkDataGen._
import com.nvidia.spark.rapids.benchmarks.{JoinBenchmarkRunner => JBR}
import org.apache.spark.sql.tests.datagen._
import org.apache.spark.sql.SparkSession
import ai.rapids.cudf.ast._
import java.io.{File, PrintWriter}
import scala.util.Random

val baseDir = "/data/tmp/ast_post_sweep"
val outputTsvPath = s"$baseDir/ast_post_sweep.tsv"
val random = new Random(42)

// ============================================================================
// Configuration
// ============================================================================

// Total rows limited to avoid OOM
val totalRows = 92680L
val benchmarkIterations = 10

// Distribution sweep (same as join_type_sweep)
val leftRowDistribution = Seq(
  1L, 10L, 100L, 1000L, 4634L, 9268L, 18536L, 30893L, 46340L,
  61787L, 74144L, 83412L, 88046L, 91753L, 92580L, 92670L, 92679L
)

// Strategy to test
sealed trait StrategyToTest {
  def name: String
  def spec: JBR.JoinStrategySpec
}

case object HashDirectStrat extends StrategyToTest {
  val name = "HashDirect"
  val spec = JBR.HashDirectStrategy
}

case object HashObjectWithPostStrat extends StrategyToTest {
  val name = "HashObjectPost"
  val spec = JBR.HashObjectWithPostStrategy
}

val strategiesToTest = Seq(HashDirectStrat, HashObjectWithPostStrat)

// AST Complexity levels
sealed trait AstComplexity {
  def name: String
  def description: String
}

case object NoAst extends AstComplexity {
  val name = "NoAst"
  val description = "Baseline - no AST filtering"
}

case object SimpleAst extends AstComplexity {
  val name = "Simple"
  val description = "Simple: left.val1 > right.val1"
}

case object MediumAst extends AstComplexity {
  val name = "Medium"
  val description = "Medium: (left.val1 > right.val1) AND (left.val2 < right.val2 + 100)"
}

case object ComplexAst extends AstComplexity {
  val name = "Complex"
  val description = "Complex: ((left.val1 > right.val1) AND (left.val2 < right.val2 + 100)) OR (left.val3 == right.val3)"
}

val astComplexities = Seq(NoAst, SimpleAst, MediumAst, ComplexAst)

// Pre-generate all AST conditions once (to avoid leaks from repeated generation)
// We'll populate this after defining generateAstCondition
val preGeneratedAstConditions = scala.collection.mutable.Map[AstComplexity, Option[JBR.ConditionalFilterSpec]]()

// Join type configurations
case class JoinTypeConfig(
  name: String,
  joinType: JBR.JoinTypeSpec,
  testBuildSides: Seq[String],  // Which build sides to test
  description: String
)

val joinTypeConfigs = Seq(
  JoinTypeConfig(
    name = "Inner",
    joinType = JBR.InnerJoin,
    testBuildSides = Seq("Auto"),  // With allowBuildSideSwap=true, just use Auto
    description = "Inner join - auto-picks optimal build side"
  ),
  JoinTypeConfig(
    name = "LeftOuter",
    joinType = JBR.LeftOuterJoin,
    testBuildSides = Seq("Auto"),
    description = "Left outer join - auto-picks optimal build side"
  ),
  JoinTypeConfig(
    name = "RightOuter",
    joinType = JBR.RightOuterJoin,
    testBuildSides = Seq("Auto"),
    description = "Right outer join - auto-picks optimal build side"
  ),
  JoinTypeConfig(
    name = "FullOuter",
    joinType = JBR.FullOuterJoin,
    testBuildSides = Seq("Auto"),
    description = "Full outer join - auto-picks optimal build side"
  ),
  JoinTypeConfig(
    name = "LeftSemi",
    joinType = JBR.LeftSemiJoin,
    testBuildSides = Seq("Auto"),
    description = "Left semi join - auto-picks optimal build side"
  ),
  JoinTypeConfig(
    name = "LeftAnti",
    joinType = JBR.LeftAntiJoin,
    testBuildSides = Seq("Auto"),
    description = "Left anti join - auto-picks optimal build side"
  )
)

// ============================================================================
// AST Condition Generation
// ============================================================================

/**
 * Generate AST condition based on complexity.
 * Column layout: key (col 0), val1 (col 1), val2 (col 2), val3 (col 3)
 */
def generateAstCondition(complexity: AstComplexity): Option[JBR.ConditionalFilterSpec] = {
  complexity match {
    case NoAst =>
      None
      
    case SimpleAst =>
      // left.val1 > right.val1
      val normalExpr = new BinaryOperation(
        BinaryOperator.GREATER,
        new ColumnReference(1, TableReference.LEFT),  // left.val1
        new ColumnReference(1, TableReference.RIGHT)  // right.val1
      )
      val normalAst = normalExpr.compile()
      
      // Swapped version for when build side is swapped
      val swappedExpr = new BinaryOperation(
        BinaryOperator.GREATER,
        new ColumnReference(1, TableReference.RIGHT),  // left.val1 (now right)
        new ColumnReference(1, TableReference.LEFT)    // right.val1 (now left)
      )
      val swappedAst = swappedExpr.compile()
      
      Some(JBR.ConditionalFilterSpec(Seq(1), Seq(1), normalAst, Some(swappedAst)))
      
    case MediumAst =>
      // (left.val1 > right.val1) AND (left.val2 < right.val2 + 100)
      val normalExpr = new BinaryOperation(
        BinaryOperator.LOGICAL_AND,
        new BinaryOperation(
          BinaryOperator.GREATER,
          new ColumnReference(1, TableReference.LEFT),
          new ColumnReference(1, TableReference.RIGHT)
        ),
        new BinaryOperation(
          BinaryOperator.LESS,
          new ColumnReference(2, TableReference.LEFT),
          new BinaryOperation(
            BinaryOperator.ADD,
            new ColumnReference(2, TableReference.RIGHT),
            Literal.ofInt(100)
          )
        )
      )
      val normalAst = normalExpr.compile()
      
      val swappedExpr = new BinaryOperation(
        BinaryOperator.LOGICAL_AND,
        new BinaryOperation(
          BinaryOperator.GREATER,
          new ColumnReference(1, TableReference.RIGHT),
          new ColumnReference(1, TableReference.LEFT)
        ),
        new BinaryOperation(
          BinaryOperator.LESS,
          new ColumnReference(2, TableReference.RIGHT),
          new BinaryOperation(
            BinaryOperator.ADD,
            new ColumnReference(2, TableReference.LEFT),
            Literal.ofInt(100)
          )
        )
      )
      val swappedAst = swappedExpr.compile()
      
      Some(JBR.ConditionalFilterSpec(Seq(1, 2), Seq(1, 2), normalAst, Some(swappedAst)))
      
    case ComplexAst =>
      // ((left.val1 > right.val1) AND (left.val2 < right.val2 + 100)) OR (left.val3 == right.val3)
      val normalExpr = new BinaryOperation(
        BinaryOperator.LOGICAL_OR,
        new BinaryOperation(
          BinaryOperator.LOGICAL_AND,
          new BinaryOperation(
            BinaryOperator.GREATER,
            new ColumnReference(1, TableReference.LEFT),
            new ColumnReference(1, TableReference.RIGHT)
          ),
          new BinaryOperation(
            BinaryOperator.LESS,
            new ColumnReference(2, TableReference.LEFT),
            new BinaryOperation(
              BinaryOperator.ADD,
              new ColumnReference(2, TableReference.RIGHT),
              Literal.ofInt(100)
            )
          )
        ),
        new BinaryOperation(
          BinaryOperator.EQUAL,
          new ColumnReference(3, TableReference.LEFT),
          new ColumnReference(3, TableReference.RIGHT)
        )
      )
      val normalAst = normalExpr.compile()
      
      val swappedExpr = new BinaryOperation(
        BinaryOperator.LOGICAL_OR,
        new BinaryOperation(
          BinaryOperator.LOGICAL_AND,
          new BinaryOperation(
            BinaryOperator.GREATER,
            new ColumnReference(1, TableReference.RIGHT),
            new ColumnReference(1, TableReference.LEFT)
          ),
          new BinaryOperation(
            BinaryOperator.LESS,
            new ColumnReference(2, TableReference.RIGHT),
            new BinaryOperation(
              BinaryOperator.ADD,
              new ColumnReference(2, TableReference.LEFT),
              Literal.ofInt(100)
            )
          )
        ),
        new BinaryOperation(
          BinaryOperator.EQUAL,
          new ColumnReference(3, TableReference.RIGHT),
          new ColumnReference(3, TableReference.LEFT)
        )
      )
      val swappedAst = swappedExpr.compile()
      
      Some(JBR.ConditionalFilterSpec(Seq(1, 2, 3), Seq(1, 2, 3), normalAst, Some(swappedAst)))
  }
}

// Now populate the pre-generated AST conditions
println("Pre-generating AST conditions...")
astComplexities.foreach { complexity =>
  preGeneratedAstConditions(complexity) = generateAstCondition(complexity)
  println(s"  Generated: ${complexity.name}")
}
println(s"Generated ${preGeneratedAstConditions.size} AST conditions\n")

// Helper to close AST conditions at the end
def closeAllAstConditions(): Unit = {
  println("\nClosing all AST conditions...")
  preGeneratedAstConditions.values.flatten.foreach { spec =>
    spec.astExpression.close()
    spec.astExpressionSwapped.foreach(_.close())
  }
  println("All AST conditions closed.")
}

// ============================================================================
// Data Generation
// ============================================================================

/**
 * Generate test tables with 100% cardinality (1 key) and payload columns for AST.
 */
def generateTestTables(
    leftRows: Long,
    rightRows: Long,
    dataDir: String): (String, String) = {
  
  val testDataDir = s"$dataDir/left_${leftRows}_right_${rightRows}"
  new File(testDataDir).mkdirs()
  
  val leftPath = s"$testDataDir/left"
  val rightPath = s"$testDataDir/right"
  
  // Check if already exists
  if (new File(leftPath).exists() && new File(rightPath).exists()) {
    println(s"  Tables already exist, skipping generation")
    return (leftPath, rightPath)
  }
  
  println(s"  Generating tables: left=$leftRows rows, right=$rightRows rows")
  
  // Create left table with key + payload columns
  val leftConfig = TableGenConfig(
    numRows = leftRows,
    keyColumns = Seq(
      KeyColumnSpec("join_key", "long", minSeed = 0, maxSeed = 0)
    ),
    payloadColumns = Seq(
      // val1: uniformly distributed 0-1000
      PayloadColumnSpec("val1", "int", minSeed = 0, maxSeed = 1000, 
        valueRange = Some((0, 1000))),
      // val2: uniformly distributed 0-1000
      PayloadColumnSpec("val2", "int", minSeed = 0, maxSeed = 1000,
        valueRange = Some((0, 1000))),
      // val3: uniformly distributed 0-100 (for equality checks)
      PayloadColumnSpec("val3", "int", minSeed = 0, maxSeed = 100,
        valueRange = Some((0, 100)))
    ),
    outputPath = leftPath
  )
  
  // Create right table with same structure
  val rightConfig = TableGenConfig(
    numRows = rightRows,
    keyColumns = Seq(
      KeyColumnSpec("join_key", "long", minSeed = 0, maxSeed = 0)
    ),
    payloadColumns = Seq(
      PayloadColumnSpec("val1", "int", minSeed = 0, maxSeed = 1000,
        valueRange = Some((0, 1000))),
      PayloadColumnSpec("val2", "int", minSeed = 0, maxSeed = 1000,
        valueRange = Some((0, 1000))),
      PayloadColumnSpec("val3", "int", minSeed = 0, maxSeed = 100,
        valueRange = Some((0, 100)))
    ),
    outputPath = rightPath
  )
  
  // Generate both tables with same key group ID for 100% overlap
  generateTable(leftConfig, Some(1), spark)
  generateTable(rightConfig, Some(1), spark)
  
  (leftPath, rightPath)
}

// ============================================================================
// Benchmark Execution
// ============================================================================

def getBuildSideSpec(buildSideName: String): JBR.BuildSideSpec = buildSideName match {
  case "LeftBuild" => JBR.LeftBuild
  case "RightBuild" => JBR.RightBuild
  case "Auto" => JBR.AutoPickSmallerIfAllowed  // With allowBuildSideSwap=true, picks optimal
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
    strategy: StrategyToTest,
    astComplexity: AstComplexity,
    leftRows: Long,
    rightRows: Long): JBR.BenchmarkResults = {
  
  println(s"\n========================================")
  println(s"Running: $testName")
  println(s"Join Type: ${joinTypeConfig.name}, Strategy: ${strategy.name}, BuildSide: $buildSideName, AST: ${astComplexity.name}")
  println(s"Left: $leftRows rows, Right: $rightRows rows")
  println(s"========================================")
  
  // HashDirectStrategy DOES support AST!
  // Pass AST filter when we have an AST condition (both strategies support it)
  val astFilter = if (astComplexity != NoAst) {
    preGeneratedAstConditions(astComplexity)
  } else {
    None  // NoAst = no filter
  }
  
  val config = JBR.JoinBenchmarkConfig(
    testName = testName,
    leftParquetPath = leftPath,
    rightParquetPath = rightPath,
    joinType = joinTypeConfig.joinType,
    joinStrategy = strategy.spec,  // Use the actual strategy spec
    buildSide = getBuildSideSpec(buildSideName),
    optimizations = JBR.JoinOptimizations(
      allowBuildSideSwap = true,  // Let it pick the optimal build side!
      remapComplexKeysToInts = false,
      useDistinctJoin = false,
      cacheJoinObject = false,
      cacheRemapping = false,
      cacheDistinctFlag = false
    ),
    conditionalFilter = astFilter,  // Only use AST when strategy supports it!
    leftKeyIndices = Seq(0),
    rightKeyIndices = Seq(0),
    iterations = benchmarkIterations,
    numThreads = 1,
    printHeader = false,
    collectDetailedTimings = true
  )
  
  JBR.runBenchmark(config, spark)
}

// ============================================================================
// Output Formatting
// ============================================================================

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

def formatResultsTSV(results: JBR.BenchmarkResults, astComplexity: String, leftPct: Int): String = {
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
      s"$joinType\t$strategy\t$buildSideConfig\t$actualBuild\t$optimizations\t" +
      s"$astComplexity\t$leftPct" +
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

// ============================================================================
// Main Benchmark Execution
// ============================================================================

def runFullBenchmark(): Unit = {
  println(s"Starting AST Post-Processing Sweep Benchmark")
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
      "AstComplexity\tLeftPct\t" +
      "RemapStructureBuildMs\tRemapBuildKeysMs\tRemapProbeKeysMs\t" +
      "CreateBuildObjectMs\tExecuteJoinMs")
    tsvWriter.flush()
    
    var testCount = 0
    val totalBuildSides = joinTypeConfigs.map(_.testBuildSides.size).sum
    val totalTestsFormula = leftRowDistribution.size * strategiesToTest.size * astComplexities.size * totalBuildSides
    // Calculate actual tests (subtracting HashDirect + FullOuter combinations)
    val fullOuterBuildSides = joinTypeConfigs.find(_.name == "FullOuter").map(_.testBuildSides.size).getOrElse(0)
    val skippedTestsCount = leftRowDistribution.size * astComplexities.size * fullOuterBuildSides
    val totalTests = totalTestsFormula - skippedTestsCount
    
    // Iterate through all test combinations
    for {
      leftRows <- leftRowDistribution
      strategy <- strategiesToTest
      astComplexity <- astComplexities
      joinTypeConfig <- joinTypeConfigs
      buildSideName <- joinTypeConfig.testBuildSides
    } {
      // Skip HashDirectStrategy for FullOuter (not used in production this way)
      val shouldSkip = strategy == HashDirectStrat && joinTypeConfig.joinType == JBR.FullOuterJoin
      
      if (!shouldSkip) {
        val testStartTime = System.currentTimeMillis()
        testCount += 1
        val rightRows = totalRows - leftRows
        
        val leftPct = (leftRows.toDouble / totalRows * 100).toInt
        val rightPct = (rightRows.toDouble / totalRows * 100).toInt
        val testName = f"L${leftPct}%03d_${joinTypeConfig.name}_${strategy.name}_${buildSideName}_${astComplexity.name}"
        
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
        // Generate data and track directory (only once per size)
        val testDataDir = s"$baseDir/left_${leftRows}_right_${rightRows}"
        generatedDataDirs += testDataDir
        
        val (leftPath, rightPath) = generateTestTables(leftRows, rightRows, baseDir)
        
        // Run benchmark
        val results = runBenchmark(
          testName, leftPath, rightPath,
          joinTypeConfig, buildSideName, strategy, astComplexity,
          leftRows, rightRows)
        
        // Write TSV line
        tsvWriter.println(formatResultsTSV(results, astComplexity.name, leftPct))
        tsvWriter.flush()
        
        Thread.sleep(1000)
        
      } catch {
        case e: OutOfMemoryError =>
          println(s"OOM ERROR in test: $testName")
          tsvWriter.println(s"$testName\tOOM_ERROR\t$leftRows\t$rightRows\t0\t1\t0\t0.0\t0.0\t0.0\t0.0\t0.0\t0.0\t" +
            s"${joinTypeConfig.name}\tHashObjectPost\t$buildSideName\tN/A\tnone\t${astComplexity.name}\t$leftPct\t\t\t\t\t")
          tsvWriter.flush()
          System.gc()
          Thread.sleep(2000)
        case e: Exception =>
          println(s"ERROR in test: $testName - ${e.getMessage}")
          e.printStackTrace()
          tsvWriter.println(s"$testName\tERROR\t$leftRows\t$rightRows\t0\t1\t0\t0.0\t0.0\t0.0\t0.0\t0.0\t0.0\t" +
            s"${joinTypeConfig.name}\tHashObjectPost\t$buildSideName\tN/A\tnone\t${astComplexity.name}\t$leftPct\t\t\t\t\t")
          tsvWriter.flush()
      }
      
        val testDuration = System.currentTimeMillis() - testStartTime
        testTimings += testDuration
        println(s"Test duration: ${testDuration / 1000}s")
      } // end if (!shouldSkip)
    } // end for loop
    
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
println("AST Post-Processing Sweep Benchmark")
println("=" * 80)
println()
println("This benchmark compares HashDirectStrategy vs HashObjectWithPostStrategy")
println("with AST filtering to see how each strategy scales with complexity.")
println()
println(s"Configuration:")
println(s"  Total rows per test: $totalRows")
println(s"  Iterations: $benchmarkIterations")
println(s"  Distribution steps: ${leftRowDistribution.size}")
println(s"  Strategies: ${strategiesToTest.map(_.name).mkString(", ")} (both support AST)")
println(s"  AST complexities: ${astComplexities.map(_.name).mkString(", ")}")
println(s"  Join types: ${joinTypeConfigs.map(_.name).distinct.mkString(", ")}")
println(s"  Build side swapping: ENABLED (auto-picks optimal)")
println(s"  Note: Skipping HashDirect + FullOuter (not used in production)")
val totalBuildSides = joinTypeConfigs.map(_.testBuildSides.size).sum
val totalTestsFormula = leftRowDistribution.size * strategiesToTest.size * astComplexities.size * totalBuildSides
// Calculate actual tests (subtracting HashDirect + FullOuter combinations)
val fullOuterBuildSides = joinTypeConfigs.find(_.name == "FullOuter").map(_.testBuildSides.size).getOrElse(0)
val skippedTests = leftRowDistribution.size * astComplexities.size * fullOuterBuildSides
val totalTests = totalTestsFormula - skippedTests
println(s"  Total tests: $totalTests (formula: $totalTestsFormula, skipped: $skippedTests)")
println()

try {
  runFullBenchmark()
} finally {
  // Clean up AST expressions to prevent memory leaks
  // This runs even if there's an exception
  closeAllAstConditions()
}

println("\nDone!")

