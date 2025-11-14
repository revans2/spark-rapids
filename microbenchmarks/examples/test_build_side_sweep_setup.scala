/*
 * Quick validation script to test that the build side sweep benchmark can load.
 * This just tests the imports and basic structure without running the full benchmark.
 */

import com.nvidia.spark.rapids.benchmarks.JoinBenchmarkDataGen._
import com.nvidia.spark.rapids.benchmarks.{JoinBenchmarkRunner => JBR}
import org.apache.spark.sql.tests.datagen._
import org.apache.spark.sql.SparkSession
import java.io.{File, FileOutputStream, PrintStream}
import scala.util.Random

println("✓ All imports loaded successfully")

// Test that we can access the necessary types
val testConfig = TableGenConfig(
  numRows = 100,
  keyColumns = Seq(
    KeyColumnSpec(
      name = "test_key",
      dataType = "long",
      minSeed = 0,
      maxSeed = 99,
      distribution = FlatDistribution()
    )
  ),
  payloadColumns = Seq.empty,
  outputPath = "/tmp/test_validation",
  numOutputFiles = 1
)

println("✓ TableGenConfig created successfully")

// Test benchmark runner types
val hashStrategy = JBR.HashObjectWithPostStrategy
val sortStrategy = JBR.SortObjectWithPostStrategy
val leftBuild = JBR.LeftBuild
val rightBuild = JBR.RightBuild

println("✓ JoinBenchmarkRunner types accessible")

println("\n✅ All validation checks passed!")
println("The build_side_sweep_benchmark.scala should work correctly.")
println("\nTo run the full benchmark, use:")
println("  :load microbenchmarks/examples/build_side_sweep_benchmark.scala")

