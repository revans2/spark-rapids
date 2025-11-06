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

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf._
import ai.rapids.cudf.ast.{BinaryOperation, BinaryOperator, ColumnReference, CompiledExpression, TableReference}

import org.apache.spark.sql.SparkSession

object JoinBenchmarkRunner {
  
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
  case object HashJoinStrategy extends JoinStrategySpec
  case object HashWithPostStrategy extends JoinStrategySpec
  case object SortWithPostStrategy extends JoinStrategySpec
  
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
    stdDevMs: Double
  )
  
  /**
   * Main entry point to run a join benchmark
   */
  def runBenchmark(
    config: JoinBenchmarkConfig,
    spark: SparkSession
  ): BenchmarkResults = {
    try {
      // Load Parquet files into GPU memory
      val (leftTable, rightTable) = loadTables(config.leftParquetPath, config.rightParquetPath)
      
      try {
        // Determine key column indices (assume all leading columns before first payload column)
        // For Phase 1, we'll use a simple heuristic: read from config or use first column
        // In a real implementation, this would be specified in the config
        val leftKeyIndices = Array(0)  // TODO: Make configurable
        val rightKeyIndices = Array(0)  // TODO: Make configurable
        
        // Create join executor
        val executor = new JoinExecutor(
          leftTable,
          rightTable,
          leftKeyIndices,
          rightKeyIndices,
          config.joinType,
          config.joinStrategy,
          config.buildSide,
          compareNullsEqual = false,  // Phase 1: use standard Spark equality
          config.optimizations,
          config.conditionalFilter.map(_.astExpression),  // Left build AST
          config.conditionalFilter.flatMap(_.astExpressionSwapped)  // Right build AST
        )
        
        try {
          // Run benchmark iterations
          val timings = ArrayBuffer[Double]()
          var totalOutputRows = 0L
          val wallClockStart = System.nanoTime()
          
          (1 to config.iterations).foreach { i =>
            val startTime = System.nanoTime()
            val (gatherMaps, _) = executor.executeJoin()
            
            // Synchronize to ensure GPU work completes
            Cuda.DEFAULT_STREAM.sync()
            val endTime = System.nanoTime()
            
            val timingMs = (endTime - startTime) / 1e6
            timings += timingMs
            
            // Track output rows from first iteration
            if (i == 1) {
              totalOutputRows = gatherMaps(0).getRowCount
            }
            
            // Close gather maps
            gatherMaps.foreach(_.close())
          }
          
          val wallClockEnd = System.nanoTime()
          val wallClockMs = (wallClockEnd - wallClockStart) / 1e6
          
          // Calculate statistics
          val sortedTimings = timings.sorted
          val average = timings.sum / timings.length
          val median = if (timings.length % 2 == 0) {
            (sortedTimings(timings.length / 2 - 1) + sortedTimings(timings.length / 2)) / 2.0
          } else {
            sortedTimings(timings.length / 2)
          }
          val min = sortedTimings.head
          val max = sortedTimings.last
          val variance = timings.map(t => math.pow(t - average, 2)).sum / timings.length
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
            timingsMs = timings.toSeq,
            wallClockMs = wallClockMs,
            averageMs = average,
            medianMs = median,
            minMs = min,
            maxMs = max,
            stdDevMs = stdDev
          )
        } finally {
          executor.clearCache()
        }
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
          stdDevMs = 0.0
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
          stdDevMs = 0.0
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
          stdDevMs = 0.0
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
      println(s"${results.testName}\t${results.status}\t" +
        s"${results.leftRows}\t${results.rightRows}\t${results.outputRows}\t" +
        s"${results.numThreads}\t${results.iterations}\t" +
        s"${f"${results.wallClockMs}%.2f"}\t${f"${results.averageMs}%.2f"}\t" +
        s"${f"${results.medianMs}%.2f"}\t${f"${results.minMs}%.2f"}\t" +
        s"${f"${results.maxMs}%.2f"}\t${f"${results.stdDevMs}%.2f"}\t$optimizations")
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
      "MinTimeMs\tMaxTimeMs\tStdDevMs\tOptimizations")
  }
  
  private def formatOptimizations(results: BenchmarkResults): String = {
    // TODO: Track actual optimizations used
    "none"
  }
}

