/*
 * Object creation overhead benchmark.
 * 
 * Tests the hypothesis that join object creation overhead is relatively fixed
 * (dominated by JNI calls) versus data-dependent across multiple dimensions:
 * - Different input sizes (10K to 5M rows)
 * - Different selectivities (10%, 50%, 100%)
 * - Different key types (INT, LONG, STRING)
 * - Different join types (Inner, LeftOuter, FullOuter)
 * 
 * Runtime: ~1-2 minutes (including data generation and warmup)
 */

import com.nvidia.spark.rapids.benchmarks.JoinBenchmarkDataGen._
import com.nvidia.spark.rapids.benchmarks.JoinBenchmarkRunner._
import org.apache.spark.sql.tests.datagen._

val baseDir = "/data/tmp/join_overhead_benchmark"

case class TestConfig(
  name: String,
  leftRows: Long,
  rightRows: Long,
  keyType: String,
  keyCardinality: Long
)

// Test configurations
val testConfigs = Seq(
  // Vary input size (100% selectivity)
  TestConfig("size_10K", 10000, 10000, "int", 10000),
  TestConfig("size_100K", 100000, 100000, "int", 100000),
  TestConfig("size_1M", 1000000, 1000000, "int", 1000000),
  TestConfig("size_5M", 5000000, 5000000, "int", 5000000),
  
  // Vary selectivity (1M rows, INT keys)
  TestConfig("sel_10pct", 1000000, 1000000, "int", 10000000),    // 10% match
  TestConfig("sel_50pct", 1000000, 1000000, "int", 2000000),     // 50% match
  TestConfig("sel_100pct", 1000000, 1000000, "int", 1000000),    // 100% match
  
  // Vary key type (1M rows, 100% selectivity)
  TestConfig("key_INT", 1000000, 1000000, "int", 1000000),
  TestConfig("key_LONG", 1000000, 1000000, "long", 1000000),
  TestConfig("key_STRING", 1000000, 1000000, "string", 1000000)
)

println("="*80)
println("Object Creation Overhead Benchmark")
println("="*80)
println(s"Generating ${testConfigs.size} test datasets...")

// Generate all datasets
testConfigs.foreach { config =>
  println(s"  ${config.name}: ${config.leftRows} x ${config.rightRows} rows, ${config.keyType} keys")
  
  val leftConfig = TableGenConfig(
    numRows = config.leftRows,
    keyColumns = Seq(
      KeyColumnSpec("id", config.keyType, minSeed = 0, maxSeed = config.keyCardinality - 1)
    ),
    payloadColumns = Seq(
      PayloadColumnSpec("value", "double")
    ),
    outputPath = s"$baseDir/${config.name}/left"
  )
  
  val rightConfig = TableGenConfig(
    numRows = config.rightRows,
    keyColumns = Seq(
      KeyColumnSpec("id", config.keyType, minSeed = 0, maxSeed = config.keyCardinality - 1)
    ),
    payloadColumns = Seq(
      PayloadColumnSpec("value", "double")
    ),
    outputPath = s"$baseDir/${config.name}/right"
  )
  
  generateJoinTables(leftConfig, rightConfig, keyGroupId = 1, spark)
}

println("\nDataset generation complete!")
println("="*80)
println()

// Helper to format join type
def joinTypeName(jt: JoinTypeSpec): String = jt match {
  case InnerJoin => "Inner"
  case LeftOuterJoin => "LeftOuter"
  case RightOuterJoin => "RightOuter"
  case FullOuterJoin => "FullOuter"
  case LeftSemiJoin => "LeftSemi"
  case LeftAntiJoin => "LeftAnti"
}

// Warmup function
def warmup(config: TestConfig, joinType: JoinTypeSpec): Unit = {
  val warmupConfig = JoinBenchmarkConfig(
    testName = "warmup",
    leftParquetPath = s"$baseDir/${config.name}/left",
    rightParquetPath = s"$baseDir/${config.name}/right",
    joinType = joinType,
    joinStrategy = HashObjectStrategy,
    buildSide = AutoPickSmallerIfAllowed,
    optimizations = JoinOptimizations(cacheJoinObject = false),
    conditionalFilter = None,
    iterations = 1,  // 1 warmup iteration
    numThreads = 1,
    printHeader = false
  )
  runBenchmark(warmupConfig, spark)
}

// Run benchmarks
def runOverheadTest(config: TestConfig, joinType: JoinTypeSpec, iterations: Int = 60): (BenchmarkResults, BenchmarkResults) = {
  // Warmup before each test
  warmup(config, joinType)
  val baseConfig = JoinBenchmarkConfig(
    testName = "",
    leftParquetPath = s"$baseDir/${config.name}/left",
    rightParquetPath = s"$baseDir/${config.name}/right",
    joinType = joinType,
    joinStrategy = HashObjectStrategy,
    buildSide = AutoPickSmallerIfAllowed,
    optimizations = JoinOptimizations(cacheJoinObject = false),
    conditionalFilter = None,
    iterations = iterations,
    numThreads = 1,
    printHeader = false
  )
  
  val objectConfig = baseConfig.copy(
    testName = s"${config.name}_${joinTypeName(joinType)}_Object",
    joinStrategy = HashObjectStrategy
  )
  val objectResult = runBenchmark(objectConfig, spark)
  
  val directConfig = baseConfig.copy(
    testName = s"${config.name}_${joinTypeName(joinType)}_Direct",
    joinStrategy = HashDirectStrategy
  )
  val directResult = runBenchmark(directConfig, spark)
  
  (objectResult, directResult)
}

case class OverheadStats(
  testName: String,
  joinType: String,
  leftRows: Long,
  rightRows: Long,
  outputRows: Long,
  objectAvgMs: Double,
  directAvgMs: Double,
  overheadMs: Double,
  overheadPercent: Double
)

val joinTypes = Seq(InnerJoin, LeftOuterJoin, FullOuterJoin)
val results = scala.collection.mutable.ArrayBuffer[OverheadStats]()

printTSVHeader()

var testCount = 0
val totalTests = testConfigs.size * joinTypes.size
testConfigs.foreach { config =>
  joinTypes.foreach { joinType =>
    testCount += 1
    println(s"[$testCount/$totalTests] Testing ${config.name} with ${joinTypeName(joinType)}...")
    
    val (objectResult, directResult) = runOverheadTest(config, joinType)
    
    printResultsTSV(objectResult)
    printResultsTSV(directResult)
    
    val overhead = objectResult.averageMs - directResult.averageMs
    val overheadPct = (overhead / directResult.averageMs) * 100.0
    
    results += OverheadStats(
      testName = config.name,
      joinType = joinTypeName(joinType),
      leftRows = objectResult.leftRows,
      rightRows = objectResult.rightRows,
      outputRows = objectResult.outputRows,
      objectAvgMs = objectResult.averageMs,
      directAvgMs = directResult.averageMs,
      overheadMs = overhead,
      overheadPercent = overheadPct
    )
  }
}

println()
println("="*80)
println("OVERHEAD ANALYSIS")
println("="*80)
println()
println("TestName\tJoinType\tLeftRows\tRightRows\tOutputRows\tObjectMs\tDirectMs\tOverheadMs\tOverhead%")
results.foreach { s =>
  println(f"${s.testName}\t${s.joinType}\t${s.leftRows}\t${s.rightRows}\t${s.outputRows}\t" +
    f"${s.objectAvgMs}%.3f\t${s.directAvgMs}%.3f\t${s.overheadMs}%.3f\t${s.overheadPercent}%.2f")
}

println()
println("="*80)
println("SUMMARY")
println("="*80)

val avgOverhead = results.map(_.overheadMs).sum / results.size
val minOverhead = results.map(_.overheadMs).min
val maxOverhead = results.map(_.overheadMs).max
val stdDev = {
  val variance = results.map(s => math.pow(s.overheadMs - avgOverhead, 2)).sum / results.size
  math.sqrt(variance)
}
val cv = (stdDev / avgOverhead) * 100.0

println(f"Average overhead: ${avgOverhead}%.3f ms")
println(f"Min overhead:     ${minOverhead}%.3f ms")
println(f"Max overhead:     ${maxOverhead}%.3f ms")
println(f"StdDev:           ${stdDev}%.3f ms")
println(f"Coefficient of Variation: ${cv}%.2f%%")
println()

if (cv < 20.0) {
  println("✓ LOW VARIATION (CV < 20%): Overhead appears RELATIVELY FIXED")
  println("  → Supports hypothesis of fixed JNI/object creation overhead")
} else if (cv < 50.0) {
  println("≈ MODERATE VARIATION (CV 20-50%): Some data dependence")
  println("  → Overhead may depend on hash table size or key characteristics")
} else {
  println("✗ HIGH VARIATION (CV > 50%): Strong data dependence")
  println("  → Overhead appears to scale with data properties")
}

println()
println("Benchmark complete!")
