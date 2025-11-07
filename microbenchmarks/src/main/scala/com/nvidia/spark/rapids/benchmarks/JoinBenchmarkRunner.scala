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

package com.nvidia.spark.rapids.benchmarks

import java.util.concurrent.{Callable, Executors, ThreadFactory, TimeUnit}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf._
import ai.rapids.cudf.ast.{BinaryOperation, BinaryOperator, ColumnReference, CompiledExpression, TableReference}

import org.apache.spark.sql.SparkSession

object JoinBenchmarkRunner {
  
  @volatile private var tsvHeaderPrinted = false
  
  /**
   * Thread factory for creating daemon threads with descriptive names.
   * This ensures threads don't block JVM shutdown and are easy to identify in debugging.
   */
  private class DaemonThreadFactory(namePrefix: String) extends ThreadFactory {
    private val threadNumber = new java.util.concurrent.atomic.AtomicInteger(1)
    
    override def newThread(r: Runnable): Thread = {
      val t = new Thread(r, s"$namePrefix-${threadNumber.getAndIncrement()}")
      t.setDaemon(true)
      t
    }
  }
  
  private def ensureTSVHeaderPrinted(printHeader: Boolean): Unit = {
    if (printHeader && !tsvHeaderPrinted) {
      printTSVHeader()
      tsvHeaderPrinted = true
    }
  }
  
  /**
   * Configuration for join benchmark execution
   */
  case class JoinBenchmarkConfig(
    testName: String,
    leftParquetPath: String,
    rightParquetPath: String,
    joinType: JoinTypeSpec,
    joinStrategy: JoinStrategySpec,
    buildSide: BuildSideSpec,
    optimizations: JoinOptimizations,
    conditionalFilter: Option[ConditionalFilterSpec],
    iterations: Int = 10,
    numThreads: Int = 1,
    enableCaching: Boolean = false,
    printHeader: Boolean = true
  )
  
  sealed trait JoinTypeSpec
  case object InnerJoin extends JoinTypeSpec
  case object LeftOuterJoin extends JoinTypeSpec
  case object RightOuterJoin extends JoinTypeSpec
  case object FullOuterJoin extends JoinTypeSpec
  case object LeftSemiJoin extends JoinTypeSpec
  case object LeftAntiJoin extends JoinTypeSpec
  
  sealed trait JoinStrategySpec
  case object HashObjectStrategy extends JoinStrategySpec
  case object HashObjectWithPostStrategy extends JoinStrategySpec
  case object SortObjectWithPostStrategy extends JoinStrategySpec
  case object HashDirectStrategy extends JoinStrategySpec
  case object HashDirectWithPostStrategy extends JoinStrategySpec
  case object SortDirectWithPostStrategy extends JoinStrategySpec
  
  sealed trait BuildSideSpec
  case object LeftBuild extends BuildSideSpec
  case object RightBuild extends BuildSideSpec
  case object AutoPickSmallerIfAllowed extends BuildSideSpec
  case object AutoMeetJoinRequirement extends BuildSideSpec
  
  case class JoinOptimizations(
    allowBuildSideSwap: Boolean = true,
    remapComplexKeysToInts: Boolean = false,
    useDistinctJoin: Boolean = false,
    cacheJoinObject: Boolean = false,
    cacheRemapping: Boolean = false,
    cacheDistinctFlag: Boolean = false
  )
  
  /**
   * Specification for conditional (AST-based) filtering in mixed joins.
   */
  case class ConditionalFilterSpec(
    leftColumns: Seq[Int],
    rightColumns: Seq[Int],
    astExpression: CompiledExpression,
    astExpressionSwapped: Option[CompiledExpression]
  )
  
  object ConditionalFilterSpec {
    /**
     * Build AST for simple comparison: leftCol [op] rightCol
     * Creates TWO AST versions to handle table swapping in join strategies.
     */
    def buildComparison(
      leftColIdx: Int,
      rightColIdx: Int,
      op: String
    ): (CompiledExpression, CompiledExpression) = {
      val binaryOp = op match {
        case ">" => BinaryOperator.GREATER
        case ">=" => BinaryOperator.GREATER_EQUAL
        case "<" => BinaryOperator.LESS
        case "<=" => BinaryOperator.LESS_EQUAL
        case "==" | "=" => BinaryOperator.EQUAL
        case "!=" => BinaryOperator.NOT_EQUAL
        case _ => throw new IllegalArgumentException(s"Unsupported operator: $op")
      }
      
      // Normal order: LEFT op RIGHT
      val normalExpr = new BinaryOperation(
        binaryOp,
        new ColumnReference(leftColIdx, TableReference.LEFT),
        new ColumnReference(rightColIdx, TableReference.RIGHT)
      )
      val normalAst = normalExpr.compile()
      
      // Swapped order: RIGHT op LEFT
      val swappedExpr = new BinaryOperation(
        binaryOp,
        new ColumnReference(leftColIdx, TableReference.RIGHT),
        new ColumnReference(rightColIdx, TableReference.LEFT)
      )
      val swappedAst = swappedExpr.compile()
      
      (normalAst, swappedAst)
    }
    
    def apply(
      leftColumns: Seq[Int],
      rightColumns: Seq[Int],
      normalAst: CompiledExpression,
      swappedAst: CompiledExpression
    ): ConditionalFilterSpec = {
      ConditionalFilterSpec(leftColumns, rightColumns, normalAst, Some(swappedAst))
    }
  }
  
  sealed trait TestStatus
  case object SUCCESS extends TestStatus
  case object OOM_ERROR extends TestStatus
  case object CONFIG_ERROR extends TestStatus
  
  case class BenchmarkResults(
    testName: String,
    status: TestStatus,
    errorMessage: Option[String],
    leftRows: Long,
    rightRows: Long,
    outputRows: Long,
    iterations: Int,
    numThreads: Int,
    timingsMs: Seq[Double],
    wallClockMs: Double,
    averageMs: Double,
    medianMs: Double,
    minMs: Double,
    maxMs: Double,
    stdDevMs: Double,
    optimizations: JoinOptimizations,
    joinType: JoinTypeSpec,
    joinStrategy: JoinStrategySpec,
    buildSideConfig: BuildSideSpec,
    actualBuildSide: Option[String]  // "Left" or "Right"
  )
  
  /**
   * Result from a single thread's benchmark execution.
   */
  private case class ThreadResult(
    timings: Seq[Double],
    outputRows: Long,
    actualBuildSide: Option[String]
  )
  
  /**
   * Run benchmark iterations on a single thread with its own executor.
   * Returns timing results for aggregation.
   */
  private def runThreadIterations(
    threadId: Int,
    iterations: Int,
    leftTable: Table,
    rightTable: Table,
    leftKeyIndices: Array[Int],
    rightKeyIndices: Array[Int],
    config: JoinBenchmarkConfig
  ): ThreadResult = {
    // Each thread creates its own executor (and thus its own build holder)
    val executor = new JoinExecutor(
      leftTable,
      rightTable,
      leftKeyIndices,
      rightKeyIndices,
      config.joinType,
      config.joinStrategy,
      config.buildSide,
      compareNullsEqual = false,
      config.optimizations,
      config.conditionalFilter.map(_.astExpression),
      config.conditionalFilter.flatMap(_.astExpressionSwapped)
    )
    
    try {
      val timings = ArrayBuffer[Double]()
      var totalOutputRows = 0L
      var actualBuildSide: Option[String] = None
      
      (1 to iterations).foreach { i =>
        val startTime = System.nanoTime()
        val (gatherMaps, _) = executor.executeJoin()
        
        // Synchronize to ensure GPU work completes
        Cuda.DEFAULT_STREAM.sync()
        val endTime = System.nanoTime()
        
        val timingMs = (endTime - startTime) / 1e6
        timings += timingMs
        
        // Track output rows and actual build side from first iteration
        if (i == 1) {
          totalOutputRows = gatherMaps(0).getRowCount
          actualBuildSide = executor.getActualBuildSide
        }
        
        // Close gather maps
        gatherMaps.foreach(_.close())
      }
      
      ThreadResult(timings.toSeq, totalOutputRows, actualBuildSide)
    } finally {
      executor.clearCache()
    }
  }
  
  /**
   * Main entry point to run a join benchmark
   */
  def runBenchmark(
    config: JoinBenchmarkConfig,
    spark: SparkSession
  ): BenchmarkResults = {
    ensureTSVHeaderPrinted(config.printHeader)
    try {
      // Load Parquet files into GPU memory
      val (leftTable, rightTable) = loadTables(config.leftParquetPath, config.rightParquetPath)
      
      try {
        // Determine key column indices
        val leftKeyIndices = Array(0)  // TODO: Make configurable
        val rightKeyIndices = Array(0)  // TODO: Make configurable
        
        val wallClockStart = System.nanoTime()
        
        // Choose single-threaded or multi-threaded execution
        val (allTimings, totalOutputRows, actualBuildSide) = if (config.numThreads == 1) {
          // Single-threaded execution (original path)
          val result = runThreadIterations(
            threadId = 0,
            iterations = config.iterations,
            leftTable = leftTable,
            rightTable = rightTable,
            leftKeyIndices = leftKeyIndices,
            rightKeyIndices = rightKeyIndices,
            config = config
          )
          (result.timings, result.outputRows, result.actualBuildSide)
          
        } else {
          // Multi-threaded execution
          val executor = Executors.newFixedThreadPool(
            config.numThreads,
            new DaemonThreadFactory(s"join-benchmark-${config.testName}")
          )
          
          try {
            // Distribute iterations across threads
            val iterationsPerThread = config.iterations / config.numThreads
            val remainingIterations = config.iterations % config.numThreads
            
            // Create tasks for each thread
            val tasks = (0 until config.numThreads).map { threadId =>
              val extraIteration = if (threadId < remainingIterations) 1 else 0
              val threadIterations = iterationsPerThread + extraIteration
              
              new Callable[ThreadResult] {
                override def call(): ThreadResult = {
                  runThreadIterations(
                    threadId = threadId,
                    iterations = threadIterations,
                    leftTable = leftTable,
                    rightTable = rightTable,
                    leftKeyIndices = leftKeyIndices,
                    rightKeyIndices = rightKeyIndices,
                    config = config
                  )
                }
              }
            }
            
            // Execute all tasks and wait for completion
            val futures = executor.invokeAll(tasks.asJava)
            
            // Aggregate results from all threads
            val results = futures.asScala.map(_.get())
            val allTimings = results.flatMap(_.timings)
            val outputRows = results.head.outputRows  // All threads should have same output rows
            val buildSide = results.head.actualBuildSide  // All threads should have same build side
            
            (allTimings, outputRows, buildSide)
            
          } finally {
            executor.shutdown()
            executor.awaitTermination(60, TimeUnit.SECONDS)
          }
        }
        
        val wallClockEnd = System.nanoTime()
        val wallClockMs = (wallClockEnd - wallClockStart) / 1e6
        
        // Calculate statistics from aggregated timings
        val sortedTimings = allTimings.sorted
        val average = allTimings.sum / allTimings.length
        val median = if (allTimings.length % 2 == 0) {
          (sortedTimings(allTimings.length / 2 - 1) + sortedTimings(allTimings.length / 2)) / 2.0
        } else {
          sortedTimings(allTimings.length / 2)
        }
        val min = sortedTimings.head
        val max = sortedTimings.last
        val variance = allTimings.map(t => math.pow(t - average, 2)).sum / allTimings.length
        val stdDev = math.sqrt(variance)
        
        BenchmarkResults(
          testName = config.testName,
          status = SUCCESS,
          errorMessage = None,
          leftRows = leftTable.getRowCount,
          rightRows = rightTable.getRowCount,
          outputRows = totalOutputRows,
          iterations = config.iterations,
          numThreads = config.numThreads,
          timingsMs = allTimings,
          wallClockMs = wallClockMs,
          averageMs = average,
          medianMs = median,
          minMs = min,
          maxMs = max,
          stdDevMs = stdDev,
          optimizations = config.optimizations,
          joinType = config.joinType,
          joinStrategy = config.joinStrategy,
          buildSideConfig = config.buildSide,
          actualBuildSide = actualBuildSide
        )
      } finally {
        leftTable.close()
        rightTable.close()
      }
    } catch {
      case e: OutOfMemoryError =>
        BenchmarkResults(
          testName = config.testName,
          status = OOM_ERROR,
          errorMessage = Some(s"Out of memory: ${e.getMessage}"),
          leftRows = 0,
          rightRows = 0,
          outputRows = 0,
          iterations = 0,
          numThreads = config.numThreads,
          timingsMs = Seq.empty,
          wallClockMs = 0.0,
          averageMs = 0.0,
          medianMs = 0.0,
          minMs = 0.0,
          maxMs = 0.0,
          stdDevMs = 0.0,
          optimizations = config.optimizations,
          joinType = config.joinType,
          joinStrategy = config.joinStrategy,
          buildSideConfig = config.buildSide,
          actualBuildSide = None
        )
      case e: IllegalArgumentException =>
        BenchmarkResults(
          testName = config.testName,
          status = CONFIG_ERROR,
          errorMessage = Some(s"Configuration error: ${e.getMessage}"),
          leftRows = 0,
          rightRows = 0,
          outputRows = 0,
          iterations = 0,
          numThreads = config.numThreads,
          timingsMs = Seq.empty,
          wallClockMs = 0.0,
          averageMs = 0.0,
          medianMs = 0.0,
          minMs = 0.0,
          maxMs = 0.0,
          stdDevMs = 0.0,
          optimizations = config.optimizations,
          joinType = config.joinType,
          joinStrategy = config.joinStrategy,
          buildSideConfig = config.buildSide,
          actualBuildSide = None
        )
      case e: Exception =>
        BenchmarkResults(
          testName = config.testName,
          status = CONFIG_ERROR,
          errorMessage = Some(s"Error: ${e.getMessage}"),
          leftRows = 0,
          rightRows = 0,
          outputRows = 0,
          iterations = 0,
          numThreads = config.numThreads,
          timingsMs = Seq.empty,
          wallClockMs = 0.0,
          averageMs = 0.0,
          medianMs = 0.0,
          minMs = 0.0,
          maxMs = 0.0,
          stdDevMs = 0.0,
          optimizations = config.optimizations,
          joinType = config.joinType,
          joinStrategy = config.joinStrategy,
          buildSideConfig = config.buildSide,
          actualBuildSide = None
        )
    }
  }
  
  /**
   * Load Parquet files into GPU tables
   */
  private def loadTables(leftPath: String, rightPath: String): (Table, Table) = {
    // Find the actual parquet file (not directory)
    val leftFile = findParquetFile(leftPath)
    val rightFile = findParquetFile(rightPath)
    
    val leftTable = Table.readParquet(leftFile)
    val rightTable = Table.readParquet(rightFile)
    
    (leftTable, rightTable)
  }
  
  private def findParquetFile(path: String): java.io.File = {
    val file = new java.io.File(path)
    if (file.isDirectory) {
      val parquetFiles = file.listFiles().filter(_.getName.endsWith(".parquet"))
      if (parquetFiles.isEmpty) {
        throw new IllegalArgumentException(s"No parquet files found in directory: $path")
      }
      if (parquetFiles.length > 1) {
        throw new IllegalArgumentException(
          s"Multiple parquet files found in directory: $path. Expected single file.")
      }
      parquetFiles.head
    } else {
      file
    }
  }
  
  /**
   * Print results in TSV format for easy copy/paste to spreadsheet
   */
  def printResultsTSV(results: BenchmarkResults): Unit = {
    if (results.status == SUCCESS) {
      val optimizations = formatOptimizations(results)
      val joinType = formatJoinType(results.joinType)
      val strategy = formatJoinStrategy(results.joinStrategy)
      val buildSideConfig = formatBuildSideConfig(results.buildSideConfig)
      val actualBuild = results.actualBuildSide.getOrElse("N/A")
      println(s"${results.testName}\t${results.status}\t" +
        s"${results.leftRows}\t${results.rightRows}\t${results.outputRows}\t" +
        s"${results.numThreads}\t${results.iterations}\t" +
        s"${f"${results.wallClockMs}%.2f"}\t${f"${results.averageMs}%.2f"}\t" +
        s"${f"${results.medianMs}%.2f"}\t${f"${results.minMs}%.2f"}\t" +
        s"${f"${results.maxMs}%.2f"}\t${f"${results.stdDevMs}%.2f"}\t" +
        s"$joinType\t$strategy\t$buildSideConfig\t$actualBuild\t$optimizations")
    } else {
      println(s"${results.testName}\t${results.status}\t" +
        s"ERROR\t${results.errorMessage.getOrElse("Unknown error")}")
    }
  }
  
  /**
   * Print TSV header
   */
  def printTSVHeader(): Unit = {
    println("TestName\tStatus\tLeftRows\tRightRows\tOutputRows\t" +
      "NumThreads\tIterations\tWallClockMs\tAvgTimeMs\tMedianTimeMs\t" +
      "MinTimeMs\tMaxTimeMs\tStdDevMs\tJoinType\tStrategy\t" +
      "BuildSideConfig\tActualBuildSide\tOptimizations")
  }
  
  private def formatOptimizations(results: BenchmarkResults): String = {
    val opts = results.optimizations
    val parts = scala.collection.mutable.ArrayBuffer[String]()
    
    if (opts.allowBuildSideSwap) parts += "swap"
    if (opts.remapComplexKeysToInts) parts += "remap"
    if (opts.useDistinctJoin) parts += "distinct"
    if (opts.cacheJoinObject) parts += "cache"
    if (opts.cacheRemapping) parts += "cache-remap"
    if (opts.cacheDistinctFlag) parts += "cache-distinct"
    
    if (parts.isEmpty) "none" else parts.mkString(",")
  }
  
  private def formatBuildSideConfig(buildSide: BuildSideSpec): String = buildSide match {
    case LeftBuild => "Left"
    case RightBuild => "Right"
    case AutoPickSmallerIfAllowed => "Auto(Smaller)"
    case AutoMeetJoinRequirement => "Auto(Required)"
  }
  
  private def formatJoinType(joinType: JoinTypeSpec): String = joinType match {
    case InnerJoin => "Inner"
    case LeftOuterJoin => "LeftOuter"
    case RightOuterJoin => "RightOuter"
    case FullOuterJoin => "FullOuter"
    case LeftSemiJoin => "LeftSemi"
    case LeftAntiJoin => "LeftAnti"
  }
  
  private def formatJoinStrategy(strategy: JoinStrategySpec): String = strategy match {
    case HashObjectStrategy => "HashObject"
    case HashObjectWithPostStrategy => "HashObjectPost"
    case SortObjectWithPostStrategy => "SortObjectPost"
    case HashDirectStrategy => "HashDirect"
    case HashDirectWithPostStrategy => "HashDirectPost"
    case SortDirectWithPostStrategy => "SortDirectPost"
  }
}

