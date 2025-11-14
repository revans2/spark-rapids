// Quick syntax verification for key improvements
// Load this in spark-shell to verify no compilation errors

// Test 1: MetricCorrelation case class
case class MetricCorrelation(
  metricName: String,
  metricDescription: String,
  correlationCoef: Double,
  affectsStrategy: String,
  isPositive: Boolean
)

// Test 2: CrossoverPoint case class
case class CrossoverPoint(
  metric: String,
  value: Double,
  strategy1: String,
  strategy2: String,
  confidence: Double,
  sampleCount: Int
)

// Test 3: TestGap case class
case class TestGap(
  metric: String,
  value: Double,
  keyType: String,
  numKeys: Int,
  importance: Double
)

// Test 4: Correlation calculation (simplified)
def testCorrelationCalc(): Double = {
  val xs = Seq(1.0, 2.0, 3.0, 4.0, 5.0)
  val ys = Seq(2.0, 4.0, 6.0, 8.0, 10.0)
  
  val meanX = xs.sum / xs.size
  val meanY = ys.sum / ys.size
  
  val numerator = (xs zip ys).map { case (x, y) => (x - meanX) * (y - meanY) }.sum
  val denomX = math.sqrt(xs.map(x => math.pow(x - meanX, 2)).sum)
  val denomY = math.sqrt(ys.map(y => math.pow(y - meanY, 2)).sum)
  
  if (denomX == 0 || denomY == 0) 0.0
  else numerator / (denomX * denomY)
}

// Test 5: Return tuple from analysis function
def testAnalysisReturn(): (Seq[MetricCorrelation], Seq[CrossoverPoint], Seq[TestGap]) = {
  val corrs = Seq(MetricCorrelation("test", "Test metric", 0.5, "s1 vs s2", true))
  val crosses = Seq(CrossoverPoint("cardinality", 0.1, "hash", "sort", 0.8, 10))
  val gaps = Seq(TestGap("cardinality", 0.12, "long", 2, 0.5))
  (corrs, crosses, gaps)
}

// Test 6: Pattern matching on tuple
val (correlations, crossovers, gaps) = testAnalysisReturn()

// Test 7: F-string formatting
val value = 0.12345
val formatted = f"Value: ${value}%.3f"

println("✓ All syntax tests passed!")
println(s"✓ Correlation coefficient: ${testCorrelationCalc()}")
println(s"✓ Found ${correlations.size} correlations, ${crossovers.size} crossovers, ${gaps.size} gaps")
println(s"✓ $formatted")
println("\n✓ Main script should compile successfully!")

