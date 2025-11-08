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

// Post-Processing vs Combined Join API Analysis
// 
// This benchmark compares the performance of:
// 1. Inner join + post-processing with AST conditional filters
// 2. Other join types with post-processing vs direct/native implementations
//
// Key Questions:
// 1. How does AST condition complexity affect post-processing overhead?
// 2. How does output selectivity (filter passing rate) impact performance?
// 3. When is materialization cost significant vs negligible?
// 4. Which join types benefit from direct implementation vs post-processing?
// 5. How does input size and cardinality affect the tradeoff?
//
// Test Matrix:
// - **Inner Join + AST Filtering:**
//   - Inner join size scaling: cardinality 1%, 10%, 50%, 100% (large to small intermediate)
//   - Condition complexity: simple (1 pred), medium (2 preds), complex (3-4 preds with OR)
//   - Output selectivity: high (5%), medium (25%, 50%), low (75%, 90%)
//   - Input sizes: 1M, 5M rows
//   - Strategy: Hash
//   - **24 test configs, 48 benchmark runs**
//
// - **Other Join Types (Post vs Direct):**
//   - Join types: LeftOuter, RightOuter, FullOuter, LeftSemi, LeftAnti
//   - Input sizes: 1M, 5M rows
//   - Cardinalities: 10%, 50%
//   - Strategy: Hash
//   - **15 test configs, 30 benchmark runs**
//
// Runtime: ~45-90 minutes (78 configs, 156 runs total)

import com.nvidia.spark.rapids.benchmarks.JoinBenchmarkDataGen._
import com.nvidia.spark.rapids.benchmarks.JoinBenchmarkRunner._
import org.apache.spark.sql.tests.datagen._
import org.apache.spark.sql.SparkSession
import ai.rapids.cudf.ast._

val baseDir = "/data/tmp/post_processing_benchmark"

// Test configuration for AST conditional filtering
case class AstTestConfig(
  name: String,
  rows: Long,
  cardinalityPct: Double,
  conditionComplexity: String,  // "simple", "medium", "complex", "very_complex"
  outputSelectivity: Double,     // Expected % of rows that pass the filter (0.05 = 5%)
  joinStrategy: String           // "hash"
)

// Test configuration for join type comparison
case class JoinTypeTestConfig(
  name: String,
  leftRows: Long,
  rightRows: Long,
  cardinalityPct: Double,
  joinType: String,              // "left_outer", "right_outer", "full_outer", "left_semi", "left_anti"
  joinStrategy: String           // "hash"
)

// Helper to convert strategy string to enum
def strategyFromString(s: String): JoinStrategySpec = s match {
  case "hash" => HashObjectStrategy
  case _ => throw new IllegalArgumentException(s"Unknown strategy: $s")
}

def strategyWithPost(s: String): JoinStrategySpec = s match {
  case "hash" => HashObjectWithPostStrategy
  case _ => throw new IllegalArgumentException(s"Unknown strategy: $s")
}

// Helper to convert join type string to enum
def joinTypeFromString(s: String): JoinTypeSpec = s match {
  case "inner" => InnerJoin
  case "left_outer" => LeftOuterJoin
  case "right_outer" => RightOuterJoin
  case "full_outer" => FullOuterJoin
  case "left_semi" => LeftSemiJoin
  case "left_anti" => LeftAntiJoin
  case _ => throw new IllegalArgumentException(s"Unknown join type: $s")
}

// Generate AST condition based on complexity and selectivity
def generateCondition(complexity: String, selectivity: Double): ConditionalFilterSpec = {
  // Column indices: key1=0, val=1, val2=2
  // Data is uniformly distributed in range [0, 1000]
  val threshold = (selectivity * 1000).toInt
  
  complexity match {
    case "simple" =>
      // Simple: left.val > right.val AND left.val < threshold
      val normalExpr = new BinaryOperation(
        BinaryOperator.LOGICAL_AND,
        new BinaryOperation(
          BinaryOperator.GREATER,
          new ColumnReference(1, TableReference.LEFT),  // left.val
          new ColumnReference(1, TableReference.RIGHT)  // right.val
        ),
        new BinaryOperation(
          BinaryOperator.LESS,
          new ColumnReference(1, TableReference.LEFT),  // left.val
          Literal.ofInt(threshold)
        )
      )
      val normalAst = normalExpr.compile()
      
      val swappedExpr = new BinaryOperation(
        BinaryOperator.LOGICAL_AND,
        new BinaryOperation(
          BinaryOperator.GREATER,
          new ColumnReference(1, TableReference.RIGHT),  // left.val (now right)
          new ColumnReference(1, TableReference.LEFT)    // right.val (now left)
        ),
        new BinaryOperation(
          BinaryOperator.LESS,
          new ColumnReference(1, TableReference.RIGHT),  // left.val
          Literal.ofInt(threshold)
        )
      )
      val swappedAst = swappedExpr.compile()
      
      ConditionalFilterSpec(Seq(1), Seq(1), normalAst, Some(swappedAst))
      
    case "medium" =>
      // Medium: left.val > right.val AND left.val < threshold AND left.val2 > threshold2
      val threshold2 = threshold + 100
      val normalExpr = new BinaryOperation(
        BinaryOperator.LOGICAL_AND,
        new BinaryOperation(
          BinaryOperator.LOGICAL_AND,
          new BinaryOperation(
            BinaryOperator.GREATER,
            new ColumnReference(1, TableReference.LEFT),
            new ColumnReference(1, TableReference.RIGHT)
          ),
          new BinaryOperation(
            BinaryOperator.LESS,
            new ColumnReference(1, TableReference.LEFT),
            Literal.ofInt(threshold)
          )
        ),
        new BinaryOperation(
          BinaryOperator.GREATER,
          new ColumnReference(2, TableReference.LEFT),  // left.val2
          Literal.ofInt(threshold2)
        )
      )
      val normalAst = normalExpr.compile()
      
      val swappedExpr = new BinaryOperation(
        BinaryOperator.LOGICAL_AND,
        new BinaryOperation(
          BinaryOperator.LOGICAL_AND,
          new BinaryOperation(
            BinaryOperator.GREATER,
            new ColumnReference(1, TableReference.RIGHT),
            new ColumnReference(1, TableReference.LEFT)
          ),
          new BinaryOperation(
            BinaryOperator.LESS,
            new ColumnReference(1, TableReference.RIGHT),
            Literal.ofInt(threshold)
          )
        ),
        new BinaryOperation(
          BinaryOperator.GREATER,
          new ColumnReference(2, TableReference.RIGHT),
          Literal.ofInt(threshold2)
        )
      )
      val swappedAst = swappedExpr.compile()
      
      ConditionalFilterSpec(Seq(1, 2), Seq(1), normalAst, Some(swappedAst))
      
    case "complex" =>
      // Complex: (left.val > right.val AND left.val < t1) OR (left.val2 > right.val2 AND left.val2 > t2)
      val t1 = (selectivity * 500).toInt
      val t2 = 500 + (selectivity * 500).toInt
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
            new ColumnReference(1, TableReference.LEFT),
            Literal.ofInt(t1)
          )
        ),
        new BinaryOperation(
          BinaryOperator.LOGICAL_AND,
          new BinaryOperation(
            BinaryOperator.GREATER,
            new ColumnReference(2, TableReference.LEFT),
            new ColumnReference(2, TableReference.RIGHT)
          ),
          new BinaryOperation(
            BinaryOperator.GREATER,
            new ColumnReference(2, TableReference.LEFT),
            Literal.ofInt(t2)
          )
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
            new ColumnReference(1, TableReference.RIGHT),
            Literal.ofInt(t1)
          )
        ),
        new BinaryOperation(
          BinaryOperator.LOGICAL_AND,
          new BinaryOperation(
            BinaryOperator.GREATER,
            new ColumnReference(2, TableReference.RIGHT),
            new ColumnReference(2, TableReference.LEFT)
          ),
          new BinaryOperation(
            BinaryOperator.GREATER,
            new ColumnReference(2, TableReference.RIGHT),
            Literal.ofInt(t2)
          )
        )
      )
      val swappedAst = swappedExpr.compile()
      
      ConditionalFilterSpec(Seq(1, 2), Seq(1, 2), normalAst, Some(swappedAst))
      
    case "very_complex" =>
      // Very complex: ((left.val > right.val AND left.val < t1) OR (left.val2 > right.val2 AND left.val2 < t2)) AND (left.val + left.val2 > t3)
      val t1 = (selectivity * 250).toInt
      val t2 = 250 + (selectivity * 250).toInt
      val t3 = 500 + (selectivity * 250).toInt
      val normalExpr = new BinaryOperation(
        BinaryOperator.LOGICAL_AND,
        new BinaryOperation(
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
              new ColumnReference(1, TableReference.LEFT),
              Literal.ofInt(t1)
            )
          ),
          new BinaryOperation(
            BinaryOperator.LOGICAL_AND,
            new BinaryOperation(
              BinaryOperator.GREATER,
              new ColumnReference(2, TableReference.LEFT),
              new ColumnReference(2, TableReference.RIGHT)
            ),
            new BinaryOperation(
              BinaryOperator.LESS,
              new ColumnReference(2, TableReference.LEFT),
              Literal.ofInt(t2)
            )
          )
        ),
        new BinaryOperation(
          BinaryOperator.GREATER,
          new BinaryOperation(
            BinaryOperator.ADD,
            new ColumnReference(1, TableReference.LEFT),
            new ColumnReference(2, TableReference.LEFT)
          ),
          Literal.ofInt(t3)
        )
      )
      val normalAst = normalExpr.compile()
      
      val swappedExpr = new BinaryOperation(
        BinaryOperator.LOGICAL_AND,
        new BinaryOperation(
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
              new ColumnReference(1, TableReference.RIGHT),
              Literal.ofInt(t1)
            )
          ),
          new BinaryOperation(
            BinaryOperator.LOGICAL_AND,
            new BinaryOperation(
              BinaryOperator.GREATER,
              new ColumnReference(2, TableReference.RIGHT),
              new ColumnReference(2, TableReference.LEFT)
            ),
            new BinaryOperation(
              BinaryOperator.LESS,
              new ColumnReference(2, TableReference.RIGHT),
              Literal.ofInt(t2)
            )
          )
        ),
        new BinaryOperation(
          BinaryOperator.GREATER,
          new BinaryOperation(
            BinaryOperator.ADD,
            new ColumnReference(1, TableReference.RIGHT),
            new ColumnReference(2, TableReference.RIGHT)
          ),
          Literal.ofInt(t3)
        )
      )
      val swappedAst = swappedExpr.compile()
      
      ConditionalFilterSpec(Seq(1, 2), Seq(1, 2), normalAst, Some(swappedAst))
      
    case _ => throw new IllegalArgumentException(s"Unknown complexity: $complexity")
  }
}

// ============================================================================
// Test Configurations
// ============================================================================

val astTestConfigs = Seq(
  // ========== INNER JOIN OUTPUT SIZE SCALING ==========
  // Goal: See how AST filtering scales with larger intermediate results
  // Keep selectivity constant at 25%, vary cardinality to change inner join output size
  
  // High cardinality (100%) = Small inner join (~1M rows before filter)
  AstTestConfig("AST_CARD100_1M_hash", 1000000, 1.00, "simple", 0.25, "hash"),
  AstTestConfig("AST_CARD100_5M_hash", 5000000, 1.00, "simple", 0.25, "hash"),
  
  // Medium cardinality (50%) = Medium inner join (~2-5M rows before filter)
  AstTestConfig("AST_CARD50_1M_hash", 1000000, 0.50, "simple", 0.25, "hash"),
  AstTestConfig("AST_CARD50_5M_hash", 5000000, 0.50, "simple", 0.25, "hash"),
  
  // Low cardinality (10%) = Large inner join (~10-50M rows before filter)
  AstTestConfig("AST_CARD10_1M_hash", 1000000, 0.10, "simple", 0.25, "hash"),
  AstTestConfig("AST_CARD10_5M_hash", 5000000, 0.10, "simple", 0.25, "hash"),
  
  // Very low cardinality (1%) = Very large inner join (~100M+ rows before filter)
  AstTestConfig("AST_CARD1_1M_hash", 1000000, 0.01, "simple", 0.25, "hash"),
  AstTestConfig("AST_CARD1_5M_hash", 5000000, 0.01, "simple", 0.25, "hash"),
  
  // ========== Simple Condition - Varying Selectivity ==========
  // Now with fixed cardinality (50%) to isolate selectivity effect
  // 5% selectivity (high filter selectivity = small output)
  AstTestConfig("AST_SIMPLE_5pct_1M_hash", 1000000, 0.50, "simple", 0.05, "hash"),
  AstTestConfig("AST_SIMPLE_5pct_5M_hash", 5000000, 0.50, "simple", 0.05, "hash"),
  
  // 25% selectivity
  AstTestConfig("AST_SIMPLE_25pct_1M_hash", 1000000, 0.50, "simple", 0.25, "hash"),
  AstTestConfig("AST_SIMPLE_25pct_5M_hash", 5000000, 0.50, "simple", 0.25, "hash"),
  
  // 50% selectivity
  AstTestConfig("AST_SIMPLE_50pct_1M_hash", 1000000, 0.50, "simple", 0.50, "hash"),
  AstTestConfig("AST_SIMPLE_50pct_5M_hash", 5000000, 0.50, "simple", 0.50, "hash"),
  
  // 75% selectivity
  AstTestConfig("AST_SIMPLE_75pct_1M_hash", 1000000, 0.50, "simple", 0.75, "hash"),
  
  // 90% selectivity (low filter selectivity = large output)
  AstTestConfig("AST_SIMPLE_90pct_1M_hash", 1000000, 0.50, "simple", 0.90, "hash"),
  
  // ========== Varying Complexity at 25% Selectivity ==========
  // Medium complexity
  AstTestConfig("AST_MEDIUM_25pct_1M_hash", 1000000, 0.50, "medium", 0.25, "hash"),
  AstTestConfig("AST_MEDIUM_25pct_5M_hash", 5000000, 0.50, "medium", 0.25, "hash"),
  
  // Complex
  AstTestConfig("AST_COMPLEX_25pct_1M_hash", 1000000, 0.50, "complex", 0.25, "hash"),
  AstTestConfig("AST_COMPLEX_25pct_5M_hash", 5000000, 0.50, "complex", 0.25, "hash"),
  
  // Very complex
  AstTestConfig("AST_VCOMPLEX_25pct_1M_hash", 1000000, 0.50, "very_complex", 0.25, "hash"),
  AstTestConfig("AST_VCOMPLEX_25pct_5M_hash", 5000000, 0.50, "very_complex", 0.25, "hash"),
)

val joinTypeTestConfigs = Seq(
  // ========== Left Outer Join ==========
  JoinTypeTestConfig("LEFT_OUTER_1M_10pct_hash", 1000000, 1000000, 0.10, "left_outer", "hash"),
  JoinTypeTestConfig("LEFT_OUTER_1M_50pct_hash", 1000000, 1000000, 0.50, "left_outer", "hash"),
  JoinTypeTestConfig("LEFT_OUTER_1M_1pct_hash", 1000000, 1000000, 0.01, "left_outer", "hash"),
  JoinTypeTestConfig("LEFT_OUTER_1M_25pct_hash", 1000000, 1000000, 0.25, "left_outer", "hash"),
  JoinTypeTestConfig("LEFT_OUTER_5M_10pct_hash", 5000000, 5000000, 0.10, "left_outer", "hash"),
  JoinTypeTestConfig("LEFT_OUTER_5M_50pct_hash", 5000000, 5000000, 0.50, "left_outer", "hash"),
  JoinTypeTestConfig("LEFT_OUTER_5M_1pct_hash", 5000000, 5000000, 0.01, "left_outer", "hash"),
  JoinTypeTestConfig("LEFT_OUTER_L5M_R1M_10pct_hash", 5000000, 1000000, 0.10, "left_outer", "hash"),
  JoinTypeTestConfig("LEFT_OUTER_L1M_R5M_10pct_hash", 1000000, 5000000, 0.10, "left_outer", "hash"),
  JoinTypeTestConfig("LEFT_OUTER_L5M_R1M_50pct_hash", 5000000, 1000000, 0.50, "left_outer", "hash"),
  JoinTypeTestConfig("LEFT_OUTER_L1M_R5M_50pct_hash", 1000000, 5000000, 0.50, "left_outer", "hash"),
  
  // ========== Right Outer Join ==========
  JoinTypeTestConfig("RIGHT_OUTER_1M_10pct_hash", 1000000, 1000000, 0.10, "right_outer", "hash"),
  JoinTypeTestConfig("RIGHT_OUTER_1M_50pct_hash", 1000000, 1000000, 0.50, "right_outer", "hash"),
  JoinTypeTestConfig("RIGHT_OUTER_5M_10pct_hash", 5000000, 5000000, 0.10, "right_outer", "hash"),
  JoinTypeTestConfig("RIGHT_OUTER_L5M_R1M_10pct_hash", 5000000, 1000000, 0.10, "right_outer", "hash"),
  JoinTypeTestConfig("RIGHT_OUTER_L1M_R5M_10pct_hash", 1000000, 5000000, 0.10, "right_outer", "hash"),
  JoinTypeTestConfig("RIGHT_OUTER_L5M_R1M_50pct_hash", 5000000, 1000000, 0.50, "right_outer", "hash"),
  JoinTypeTestConfig("RIGHT_OUTER_L1M_R5M_50pct_hash", 1000000, 5000000, 0.50, "right_outer", "hash"),
  
  // ========== Full Outer Join ==========
  JoinTypeTestConfig("FULL_OUTER_1M_10pct_hash", 1000000, 1000000, 0.10, "full_outer", "hash"),
  JoinTypeTestConfig("FULL_OUTER_1M_50pct_hash", 1000000, 1000000, 0.50, "full_outer", "hash"),
  JoinTypeTestConfig("FULL_OUTER_5M_10pct_hash", 5000000, 5000000, 0.10, "full_outer", "hash"),
  JoinTypeTestConfig("FULL_OUTER_5M_50pct_hash", 5000000, 5000000, 0.50, "full_outer", "hash"),
  JoinTypeTestConfig("FULL_OUTER_L5M_R1M_10pct_hash", 5000000, 1000000, 0.10, "full_outer", "hash"),
  JoinTypeTestConfig("FULL_OUTER_L1M_R5M_10pct_hash", 1000000, 5000000, 0.10, "full_outer", "hash"),
  JoinTypeTestConfig("FULL_OUTER_L5M_R1M_50pct_hash", 5000000, 1000000, 0.50, "full_outer", "hash"),
  JoinTypeTestConfig("FULL_OUTER_L1M_R5M_50pct_hash", 1000000, 5000000, 0.50, "full_outer", "hash"),
  
  // ========== Left Semi Join ==========
  JoinTypeTestConfig("LEFT_SEMI_1M_10pct_hash", 1000000, 1000000, 0.10, "left_semi", "hash"),
  JoinTypeTestConfig("LEFT_SEMI_1M_50pct_hash", 1000000, 1000000, 0.50, "left_semi", "hash"),
  JoinTypeTestConfig("LEFT_SEMI_5M_10pct_hash", 5000000, 5000000, 0.10, "left_semi", "hash"),
  JoinTypeTestConfig("LEFT_SEMI_5M_50pct_hash", 5000000, 5000000, 0.50, "left_semi", "hash"),
  JoinTypeTestConfig("LEFT_SEMI_L1M_R5M_10pct_hash", 1000000, 5000000, 0.10, "left_semi", "hash"),
  JoinTypeTestConfig("LEFT_SEMI_L5M_R1M_10pct_hash", 5000000, 1000000, 0.10, "left_semi", "hash"),
  JoinTypeTestConfig("LEFT_SEMI_L1M_R5M_50pct_hash", 1000000, 5000000, 0.50, "left_semi", "hash"),
  JoinTypeTestConfig("LEFT_SEMI_L5M_R1M_50pct_hash", 5000000, 1000000, 0.50, "left_semi", "hash"),
   
  // ========== Left Anti Join ==========
  JoinTypeTestConfig("LEFT_ANTI_1M_10pct_hash", 1000000, 1000000, 0.10, "left_anti", "hash"),
  JoinTypeTestConfig("LEFT_ANTI_1M_50pct_hash", 1000000, 1000000, 0.50, "left_anti", "hash"),
  JoinTypeTestConfig("LEFT_ANTI_5M_10pct_hash", 5000000, 5000000, 0.10, "left_anti", "hash"),
  JoinTypeTestConfig("LEFT_ANTI_5M_50pct_hash", 5000000, 5000000, 0.50, "left_anti", "hash"),
  JoinTypeTestConfig("LEFT_ANTI_L1M_R5M_10pct_hash", 1000000, 5000000, 0.10, "left_anti", "hash"),
  JoinTypeTestConfig("LEFT_ANTI_L5M_R1M_10pct_hash", 5000000, 1000000, 0.10, "left_anti", "hash"),
  JoinTypeTestConfig("LEFT_ANTI_L1M_R5M_50pct_hash", 1000000, 5000000, 0.50, "left_anti", "hash"),
  JoinTypeTestConfig("LEFT_ANTI_L5M_R1M_50pct_hash", 5000000, 1000000, 0.50, "left_anti", "hash")
)

println("="*80)
println("Post-Processing vs Combined Join API Analysis")
println("="*80)
println(s"Total AST test configurations: ${astTestConfigs.size}")
val totalRuns = (astTestConfigs.size * 2) + (joinTypeTestConfigs.size * 2)
println(s"Total join type test configurations: ${joinTypeTestConfigs.size}")
println(s"Total benchmark runs: ${totalRuns}")
println()

// ============================================================================
// Data Generation
// ============================================================================

println("Generating test datasets...")
println()

// Generate data for AST tests
astTestConfigs.map(c => (c.name, c.rows, c.cardinalityPct)).distinct.foreach { case (name, rows, cardPct) =>
  val distinctKeys = (rows * cardPct).toLong
  
  println(f"  $name: ${rows}%,d rows, ${distinctKeys}%,d distinct (${cardPct * 100}%.0f%%)")
  
  val leftConfig = TableGenConfig(
    numRows = rows,
    keyColumns = Seq(KeyColumnSpec("key1", "int", minSeed = 0, maxSeed = distinctKeys - 1)),
    payloadColumns = Seq(
      PayloadColumnSpec("val", "int"),
      PayloadColumnSpec("val2", "int")
    ),
    outputPath = s"$baseDir/${name}/left"
  )
  
  val rightConfig = TableGenConfig(
    numRows = rows,
    keyColumns = Seq(KeyColumnSpec("key1", "int", minSeed = 0, maxSeed = distinctKeys - 1)),
    payloadColumns = Seq(
      PayloadColumnSpec("val", "int"),
      PayloadColumnSpec("val2", "int")
    ),
    outputPath = s"$baseDir/${name}/right"
  )
  
  // Generate tables with value columns in range [0, 1000]
  val dbgen = DBGen()
  val leftTable = dbgen.addTable("left", "key1 int, val int, val2 int", rows)
  leftTable("key1").setSeedRange(0, distinctKeys - 1)
  leftTable("val").setSeedRange(0, 1000)
  leftTable("val2").setSeedRange(0, 1000)
  
  val leftDf = leftTable.toDF(spark)
  leftDf.repartition(1).write.mode("overwrite").parquet(leftConfig.outputPath)
  
  val rightTable = dbgen.addTable("right", "key1 int, val int, val2 int", rows)
  rightTable("key1").setSeedRange(0, distinctKeys - 1)
  rightTable("val").setSeedRange(0, 1000)
  rightTable("val2").setSeedRange(0, 1000)
  
  val rightDf = rightTable.toDF(spark)
  rightDf.repartition(1).write.mode("overwrite").parquet(rightConfig.outputPath)
}

// Generate data for join type tests
joinTypeTestConfigs.map(c => (c.name, c.leftRows, c.rightRows, c.cardinalityPct)).distinct.foreach { 
  case (name, leftRows, rightRows, cardPct) =>
    val distinctKeys = (math.min(leftRows, rightRows) * cardPct).toLong
    
    println(f"  $name: left=${leftRows}%,d, right=${rightRows}%,d, ${distinctKeys}%,d distinct (${cardPct * 100}%.0f%%)")
    
    val leftConfig = TableGenConfig(
      numRows = leftRows,
      keyColumns = Seq(KeyColumnSpec("key1", "int", minSeed = 0, maxSeed = distinctKeys - 1)),
      payloadColumns = Seq(PayloadColumnSpec("value", "double")),
      outputPath = s"$baseDir/${name}/left"
    )
    
    val rightConfig = TableGenConfig(
      numRows = rightRows,
      keyColumns = Seq(KeyColumnSpec("key1", "int", minSeed = 0, maxSeed = distinctKeys - 1)),
      payloadColumns = Seq(PayloadColumnSpec("value", "double")),
      outputPath = s"$baseDir/${name}/right"
    )
    
    val dbgen = DBGen()
    val leftTable = dbgen.addTable("left", "key1 int, value double", leftRows)
    leftTable("key1").setSeedRange(0, distinctKeys - 1)
    
    val leftDf = leftTable.toDF(spark)
    leftDf.repartition(1).write.mode("overwrite").parquet(leftConfig.outputPath)
    
    val rightTable = dbgen.addTable("right", "key1 int, value double", rightRows)
    rightTable("key1").setSeedRange(0, distinctKeys - 1)
    
    val rightDf = rightTable.toDF(spark)
    rightDf.repartition(1).write.mode("overwrite").parquet(rightConfig.outputPath)
}

println("\nDataset generation complete!")
println("="*80)
println()

// ============================================================================
// Benchmark Functions
// ============================================================================

// Pre-generate all AST conditions (will be closed at the end)
println("Pre-generating AST conditions...")
val astConditions = scala.collection.mutable.Map[String, ConditionalFilterSpec]()
astTestConfigs.foreach { config =>
  val key = s"${config.conditionComplexity}_${config.outputSelectivity}"
  if (!astConditions.contains(key)) {
    astConditions(key) = generateCondition(config.conditionComplexity, config.outputSelectivity)
  }
}
println(s"Generated ${astConditions.size} unique AST conditions")
println()

// Warmup for AST tests
def warmupAst(config: AstTestConfig, condition: ConditionalFilterSpec): Unit = {
  val warmupConfig = JoinBenchmarkConfig(
    testName = s"warmup_${config.name}",
    leftParquetPath = s"$baseDir/${config.name}/left",
    rightParquetPath = s"$baseDir/${config.name}/right",
    joinType = InnerJoin,
    joinStrategy = strategyWithPost(config.joinStrategy),
    buildSide = LeftBuild,
    optimizations = JoinOptimizations(allowBuildSideSwap = false),
    conditionalFilter = Some(condition),
    leftKeyIndices = Seq(0),
    rightKeyIndices = Seq(0),
    iterations = 1,
    numThreads = 1,
    printHeader = false
  )
  runBenchmark(warmupConfig, spark)
}

// Run AST test: inner + post with AST vs inner only (no condition)
def runAstTest(config: AstTestConfig, condition: ConditionalFilterSpec, printHeader: Boolean, iterations: Int = 20): Seq[BenchmarkResults] = {
  warmupAst(config, condition)
  
  // Test 1: Inner + Post with AST condition
  val withPost = runBenchmark(
    JoinBenchmarkConfig(
      testName = s"${config.name}_with_post",
      leftParquetPath = s"$baseDir/${config.name}/left",
      rightParquetPath = s"$baseDir/${config.name}/right",
      joinType = InnerJoin,
      joinStrategy = strategyWithPost(config.joinStrategy),
      buildSide = LeftBuild,
      optimizations = JoinOptimizations(allowBuildSideSwap = false),
      conditionalFilter = Some(condition),
      leftKeyIndices = Seq(0),
      rightKeyIndices = Seq(0),
      iterations = iterations,
      numThreads = 1,
      printHeader = printHeader
    ),
    spark
  )
  
  // Test 2: Inner only (baseline - no condition)
  val innerOnly = runBenchmark(
    JoinBenchmarkConfig(
      testName = s"${config.name}_inner_only",
      leftParquetPath = s"$baseDir/${config.name}/left",
      rightParquetPath = s"$baseDir/${config.name}/right",
      joinType = InnerJoin,
      joinStrategy = strategyFromString(config.joinStrategy),
      buildSide = LeftBuild,
      optimizations = JoinOptimizations(allowBuildSideSwap = false),
      conditionalFilter = None,
      leftKeyIndices = Seq(0),
      rightKeyIndices = Seq(0),
      iterations = iterations,
      numThreads = 1,
      printHeader = false
    ),
    spark
  )
  
  Seq(withPost, innerOnly)
}

// Warmup for join type tests
def warmupJoinType(config: JoinTypeTestConfig): Unit = {
  val warmupConfig = JoinBenchmarkConfig(
    testName = s"warmup_${config.name}",
    leftParquetPath = s"$baseDir/${config.name}/left",
    rightParquetPath = s"$baseDir/${config.name}/right",
    joinType = joinTypeFromString(config.joinType),
    joinStrategy = strategyWithPost(config.joinStrategy),
    buildSide = LeftBuild,
    optimizations = JoinOptimizations(allowBuildSideSwap = false),
    conditionalFilter = None,
    leftKeyIndices = Seq(0),
    rightKeyIndices = Seq(0),
    iterations = 1,
    numThreads = 1,
    printHeader = false
  )
  runBenchmark(warmupConfig, spark)
}

// Baseline (direct) strategy selector for join-type tests.
// Only 'hash' has a true direct implementation to compare against.
def baselineStrategyForJoinType(s: String): JoinStrategySpec = s match {
  case "hash" => HashObjectStrategy
  case other => throw new IllegalArgumentException(s"No direct (non-post) baseline for strategy: $other")
}

// Run join type test: post-processing vs direct implementation
def runJoinTypeTest(config: JoinTypeTestConfig, printHeader: Boolean, iterations: Int = 20): Seq[BenchmarkResults] = {
  warmupJoinType(config)
  
  val jType = joinTypeFromString(config.joinType)
  
  // Test 1: With post-processing strategy
  val withPost = runBenchmark(
    JoinBenchmarkConfig(
      testName = s"${config.name}_with_post",
      leftParquetPath = s"$baseDir/${config.name}/left",
      rightParquetPath = s"$baseDir/${config.name}/right",
      joinType = jType,
      joinStrategy = strategyWithPost(config.joinStrategy),
      buildSide = LeftBuild,
      optimizations = JoinOptimizations(allowBuildSideSwap = false),
      conditionalFilter = None,
      leftKeyIndices = Seq(0),
      rightKeyIndices = Seq(0),
      iterations = iterations,
      numThreads = 1,
      printHeader = printHeader
    ),
    spark
  )
  
  // Test 2: Direct implementation (no post-processing)
  val direct = runBenchmark(
    JoinBenchmarkConfig(
      testName = s"${config.name}_direct",
      leftParquetPath = s"$baseDir/${config.name}/left",
      rightParquetPath = s"$baseDir/${config.name}/right",
      joinType = jType,
      joinStrategy = baselineStrategyForJoinType(config.joinStrategy),
      buildSide = LeftBuild,
      optimizations = JoinOptimizations(allowBuildSideSwap = false),
      conditionalFilter = None,
      leftKeyIndices = Seq(0),
      rightKeyIndices = Seq(0),
      iterations = iterations,
      numThreads = 1,
      printHeader = false
    ),
    spark
  )
  
  Seq(withPost, direct)
}

// ============================================================================
// Benchmark Execution
// ============================================================================

println("Running benchmarks...")
println()

// Print TSV header
println("TestName\tStatus\tLeftRows\tRightRows\tOutputRows\tNumThreads\tIterations\tWallClockMs\tAvgTimeMs\tMedianTimeMs\tMinTimeMs\tMaxTimeMs\tStdDevMs\tJoinType\tStrategy\tBuildSideConfig\tActualBuildSide\tOptimizations")

println("\n" + "="*80)
println("PART 1: INNER JOIN + AST POST-FILTERING")
println("="*80)
println()

val astResults = astTestConfigs.zipWithIndex.flatMap { case (config, idx) =>
  println(s"Testing: ${config.name}")
  val key = s"${config.conditionComplexity}_${config.outputSelectivity}"
  val condition = astConditions(key)
  val results = runAstTest(config, condition, printHeader = false)
  results.foreach(printResultsTSV)
  results
}

println("\n" + "="*80)
println("PART 2: JOIN TYPE POST-PROCESSING VS DIRECT")
println("="*80)
println()

val joinTypeResults = joinTypeTestConfigs.zipWithIndex.flatMap { case (config, idx) =>
  println(s"Testing: ${config.name}")
  val results = runJoinTypeTest(config, printHeader = false)
  results.foreach(printResultsTSV)
  results
}

// ============================================================================
// Analysis
// ============================================================================

case class AstStats(
  testName: String,
  rows: Long,
  cardinalityPct: Double,
  complexity: String,
  selectivity: Double,
  joinStrategy: String,
  withPostMs: Double,
  innerOnlyMs: Double,
  overheadMs: Double,
  overheadPct: Double
) {
  def hasSignificantOverhead: Boolean = overheadPct > 5.0
}

case class JoinTypeStats(
  testName: String,
  joinType: String,
  leftRows: Long,
  rightRows: Long,
  cardinalityPct: Double,
  joinStrategy: String,
  withPostMs: Double,
  directMs: Double,
  overheadMs: Double,
  overheadPct: Double
) {
  def postIsFaster: Boolean = overheadPct < 0
  def hasSignificantOverhead: Boolean = overheadPct > 5.0
}

val astStats = astTestConfigs.map { config =>
  val withPost = astResults.find(_.testName == s"${config.name}_with_post").get
  val innerOnly = astResults.find(_.testName == s"${config.name}_inner_only").get
  
  val overhead = withPost.medianMs - innerOnly.medianMs
  val overheadPct = (overhead / innerOnly.medianMs) * 100.0
  
  AstStats(
    testName = config.name,
    rows = config.rows,
    cardinalityPct = config.cardinalityPct,
    complexity = config.conditionComplexity,
    selectivity = config.outputSelectivity,
    joinStrategy = config.joinStrategy,
    withPostMs = withPost.medianMs,
    innerOnlyMs = innerOnly.medianMs,
    overheadMs = overhead,
    overheadPct = overheadPct
  )
}

val joinTypeStats = joinTypeTestConfigs.map { config =>
  val withPost = joinTypeResults.find(_.testName == s"${config.name}_with_post").get
  val direct = joinTypeResults.find(_.testName == s"${config.name}_direct").get
  
  val overhead = withPost.medianMs - direct.medianMs
  val overheadPct = (overhead / direct.medianMs) * 100.0
  
  JoinTypeStats(
    testName = config.name,
    joinType = config.joinType,
    leftRows = config.leftRows,
    rightRows = config.rightRows,
    cardinalityPct = config.cardinalityPct,
    joinStrategy = config.joinStrategy,
    withPostMs = withPost.medianMs,
    directMs = direct.medianMs,
    overheadMs = overhead,
    overheadPct = overheadPct
  )
}

println("\n" + "="*80)
println("ANALYSIS: AST POST-FILTERING OVERHEAD")
println("="*80)

println("\n1. OVERHEAD BY INNER JOIN OUTPUT SIZE (Cardinality Scaling):")
println("   (Fixed 25% selectivity, varying cardinality to change intermediate result size)")

// Group by cardinality to see scaling
val cardinalityScaling: Seq[((Long, Double), Seq[AstStats])] = {
  astStats.filter(s => s.selectivity == 0.25 && s.complexity == "simple")
    .groupBy(s => (s.rows, s.cardinalityPct))
    .toSeq.sortBy(t => (t._1._1, -t._1._2))
}

cardinalityScaling.foreach { entry =>
  val rows = entry._1._1
  val card = entry._1._2
  val stats = entry._2
  
  val avgOverhead = stats.map(_.overheadPct).sum / stats.size.toDouble
  
  // Estimate inner join size
  val estimatedInnerSize = if (card == 1.0) rows else if (card == 0.5) rows * 2 else if (card == 0.1) rows * 10 else rows * 100
  
  println(f"\n  ${rows}%,d input rows, ${card * 100}%.0f%% cardinality (~${estimatedInnerSize}%,d inner join rows):")
  println(f"    Avg overhead: ${avgOverhead}%+.2f%%")
}

// Check if overhead scales linearly with inner join size
val lowCardOverhead = {
  val filtered = cardinalityScaling.filter(entry => entry._1._2 <= 0.1)
  val allOverheads = filtered.flatMap(_._2.map(_.overheadPct))
  if (allOverheads.isEmpty) 0.0 else allOverheads.sum / allOverheads.size.toDouble
}
val highCardOverhead = {
  val filtered = cardinalityScaling.filter(entry => entry._1._2 >= 0.5)
  val allOverheads = filtered.flatMap(_._2.map(_.overheadPct))
  if (allOverheads.isEmpty) 0.0 else allOverheads.sum / allOverheads.size.toDouble
}

println(f"\nKey Finding:")
println(f"  Low cardinality (≤10%% = large inner join): avg ${lowCardOverhead}%+.2f%% overhead")
println(f"  High cardinality (≥50%% = small inner join): avg ${highCardOverhead}%+.2f%% overhead")

if (math.abs(lowCardOverhead - highCardOverhead) > 20) {
  println("  → AST filtering overhead SCALES SIGNIFICANTLY with inner join output size")
  println("  → Materialization cost dominates for large intermediate results")
} else if (math.abs(lowCardOverhead - highCardOverhead) > 10) {
  println("  → AST filtering overhead scales moderately with inner join output size")
} else {
  println("  → AST filtering overhead is relatively CONSTANT regardless of inner join size")
  println("  → Filtering cost dominates over materialization cost")
}

println("\n2. OVERHEAD BY SELECTIVITY (Simple Condition, Fixed 50% Cardinality):")
val simpleBySelectivity = astStats.filter(s => s.complexity == "simple" && s.selectivity != 0.25).groupBy(_.selectivity).toSeq.sortBy(_._1)
simpleBySelectivity.foreach { case (sel, stats) =>
  val avgOverhead = stats.map(_.overheadPct).sum / stats.size.toDouble
  
  println(f"\n  Selectivity ${sel * 100}%.0f%% (${(sel * 100).toInt}%% of inner join passes filter):")
  println(f"    Avg overhead: ${avgOverhead}%+.2f%%")
  
  if (avgOverhead < 5) {
    println(f"    ✓ Low overhead - post-filtering is efficient")
  } else if (avgOverhead < 15) {
    println(f"    ~ Moderate overhead - acceptable for complex filters")
  } else {
    println(f"    ✗ High overhead - consider filter pushdown or different approach")
  }
}

println("\n3. OVERHEAD BY COMPLEXITY (25% Selectivity, 50% Cardinality):")
val complexityTests = astStats.filter(s => s.selectivity == 0.25 && s.rows == 5000000 && s.complexity != "simple")
val byComplexity = complexityTests.groupBy(_.complexity).toSeq.sortBy(_._1)
byComplexity.foreach { case (complexity, stats) =>
  val avgOverhead = stats.map(_.overheadPct).sum / stats.size.toDouble
  
  println(f"\n  $complexity:")
  println(f"    Avg overhead: ${avgOverhead}%+.2f%%")
  
  stats.foreach { s =>
    println(f"      ${s.overheadPct}%+.2f%% (${s.withPostMs}%.2f ms vs ${s.innerOnlyMs}%.2f ms)")
  }
}

println("\n4. KEY INSIGHTS:")
val highSelectivityOverhead = {
  val filtered = astStats.filter(_.selectivity <= 0.25)
  if (filtered.isEmpty) 0.0 else filtered.map(_.overheadPct).sum / filtered.size.toDouble
}
val lowSelectivityOverhead = {
  val filtered = astStats.filter(_.selectivity >= 0.75)
  if (filtered.isEmpty) 0.0 else filtered.map(_.overheadPct).sum / filtered.size.toDouble
}

println(f"   High selectivity (≤25%% pass): avg ${highSelectivityOverhead}%+.2f%% overhead")
println(f"   Low selectivity (≥75%% pass): avg ${lowSelectivityOverhead}%+.2f%% overhead")

if (math.abs(highSelectivityOverhead - lowSelectivityOverhead) > 10) {
  println("   → Selectivity has SIGNIFICANT impact on post-filtering overhead")
  if (highSelectivityOverhead < lowSelectivityOverhead) {
    println("   → Materialization cost is noticeable when many rows pass the filter")
  }
} else {
  println("   → Selectivity has MINIMAL impact - overhead is roughly constant")
}

println("\n" + "="*80)
println("ANALYSIS: JOIN TYPE POST-PROCESSING OVERHEAD")
println("="*80)

val byJoinType = joinTypeStats.groupBy(_.joinType).toSeq.sortBy(_._1)

byJoinType.foreach { case (jType, stats) =>
  val avgOverhead = stats.map(_.overheadPct).sum / stats.size.toDouble
  val hasNegative = stats.exists(_.postIsFaster)
  
  println(f"\n$jType:")
  println(f"  Avg overhead: ${avgOverhead}%+.2f%%")
  println(f"  Post faster in: ${stats.count(_.postIsFaster)}/${stats.size} tests")
  
  if (avgOverhead < -5) {
    println(f"  ✓ Post-processing is FASTER - use it!")
  } else if (avgOverhead < 5) {
    println(f"  ~ Negligible difference - either approach is fine")
  } else {
    println(f"  ✗ Direct implementation is FASTER - prefer it when available")
  }
  
  // Show details
  stats.foreach { s =>
    println(f"    ${s.testName}: ${s.overheadPct}%+.2f%% (${s.withPostMs}%.2f ms vs ${s.directMs}%.2f ms)")
    
    // Derived estimates under uniform key distribution
    val D = math.max(1L, (math.min(s.leftRows, s.rightRows) * s.cardinalityPct).toLong)
    val fanout = s.rightRows.toDouble / D.toDouble  // expected right matches per left row
    val innerRowsEst = (s.leftRows.toDouble * s.rightRows.toDouble) / D.toDouble
    val pLeftMatch = 1.0 - math.pow(1.0 - 1.0 / D.toDouble, s.rightRows.toDouble)
    val pRightMatch = 1.0 - math.pow(1.0 - 1.0 / D.toDouble, s.leftRows.toDouble)
    val unmatchedLeftEst = s.leftRows.toDouble * (1.0 - pLeftMatch)
    val unmatchedRightEst = s.rightRows.toDouble * (1.0 - pRightMatch)
    
    jType match {
      case "left_outer" =>
        println(f"      estUnmatchedLeft=${unmatchedLeftEst}%.0f, fanout=${fanout}%.2f, estInner=${innerRowsEst}%.0f")
      case "right_outer" =>
        println(f"      estUnmatchedRight=${unmatchedRightEst}%.0f, fanout=${fanout}%.2f, estInner=${innerRowsEst}%.0f")
      case "full_outer" =>
        println(f"      estUnmatchedLeft=${unmatchedLeftEst}%.0f, estUnmatchedRight=${unmatchedRightEst}%.0f, estInner=${innerRowsEst}%.0f")
      case "left_semi" =>
        println(f"      pLeftMatch=${pLeftMatch * 100}%.1f%%, fanout=${fanout}%.2f, estInner=${innerRowsEst}%.0f")
      case "left_anti" =>
        println(f"      pLeftNoMatch=${(1.0 - pLeftMatch) * 100}%.1f%%, fanout=${fanout}%.2f, estInner=${innerRowsEst}%.0f")
      case _ =>
    }
  }
}

println("\n" + "="*80)
println("JOIN TYPE CORRELATION TSV")
println("="*80)
println("TestName\tJoinType\tLeftRows\tRightRows\tCardinality%\tWithPostMs\tDirectMs\tOverhead%\tEstInner\tFanout\tpLeftMatch%\tpRightMatch%\tEstUnmatchedLeft\tEstUnmatchedRight")
joinTypeStats.foreach { s =>
  val D = math.max(1L, (math.min(s.leftRows, s.rightRows) * s.cardinalityPct).toLong)
  val fanout = s.rightRows.toDouble / D.toDouble
  val innerRowsEst = (s.leftRows.toDouble * s.rightRows.toDouble) / D.toDouble
  val pLeftMatch = 1.0 - math.pow(1.0 - 1.0 / D.toDouble, s.rightRows.toDouble)
  val pRightMatch = 1.0 - math.pow(1.0 - 1.0 / D.toDouble, s.leftRows.toDouble)
  val unmatchedLeftEst = s.leftRows.toDouble * (1.0 - pLeftMatch)
  val unmatchedRightEst = s.rightRows.toDouble * (1.0 - pRightMatch)
  println(f"${s.testName}\t${s.joinType}\t${s.leftRows}\t${s.rightRows}\t${s.cardinalityPct * 100}%.0f\t${s.withPostMs}%.2f\t${s.directMs}%.2f\t${s.overheadPct}%.2f\t${innerRowsEst}%.0f\t${fanout}%.2f\t${pLeftMatch * 100}%.2f\t${pRightMatch * 100}%.2f\t${unmatchedLeftEst}%.0f\t${unmatchedRightEst}%.0f")
}

println("\n" + "="*80)
println("RECOMMENDATIONS")
println("="*80)

println("\n1. AST POST-FILTERING:")
val avgAstOverhead = astStats.map(_.overheadPct).sum / astStats.size.toDouble
println(f"   Average overhead: ${avgAstOverhead}%+.2f%%")

if (avgAstOverhead < 10) {
  println("   ✓ AST post-filtering is generally efficient")
  println("   → Use it for complex conditions that can't be pushed down")
} else {
  println("   ⚠ AST post-filtering has measurable overhead")
  println("   → Prefer filter pushdown when possible")
  println("   → Most efficient when selectivity is high (small output)")
}

println("\n2. JOIN TYPE POST-PROCESSING:")
byJoinType.foreach { case (jType, stats) =>
  val avgOh = stats.map(_.overheadPct).sum / stats.size.toDouble
  if (avgOh < 5) {
    println(f"   ✓ $jType: Post-processing is acceptable (${avgOh}%+.1f%% overhead)")
  } else {
    println(f"   ⚠ $jType: Prefer direct implementation (${avgOh}%+.1f%% overhead)")
  }
}

println("\nBenchmark complete!")

// ============================================================================
// Cleanup: Close all AST expressions
// ============================================================================

println("\nCleaning up AST expressions...")
astConditions.values.foreach { condition =>
  try {
    condition.astExpression.close()
    condition.astExpressionSwapped.foreach(_.close())
  } catch {
    case e: Exception => println(s"Warning: Error closing AST expression: ${e.getMessage}")
  }
}
println(s"Closed ${astConditions.size} AST conditions")

