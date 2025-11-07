# Join Micro-Benchmarks

A comprehensive micro-benchmark framework for measuring GPU join performance at the lowest level.

## Building

### Prerequisites
1. Build the datagen module first (required dependency):
```bash
cd ../datagen
mvn clean install -Dbuildver=353  # Use your Spark version (e.g., 330, 353, 400)
```

### Build Microbenchmarks
```bash
cd microbenchmarks
mvn clean package -Dbuildver=353
```

This will create:
- `target/spark353/microbenchmarks_2.12-25.12.0-SNAPSHOT-spark353.jar`

## Running Examples

### Launch spark-shell with JARs

**Important:** You need three JARs:
1. datagen JAR (for data generation)
2. microbenchmarks JAR (the benchmark framework)
3. spark-rapids-jni JAR (for HashJoin/SortMergeJoin implementations)

```bash
spark-shell \
  --conf 'spark.rapids.sql.allowMultipleJars=ALWAYS' \
  --jars ../datagen/target/datagen_2.12-25.12.0-SNAPSHOT-spark353.jar,\
target/spark353/microbenchmarks_2.12-25.12.0-SNAPSHOT-spark353.jar,\
~/.m2/repository/com/nvidia/spark-rapids-jni/25.12.0-SNAPSHOT/spark-rapids-jni-25.12.0-SNAPSHOT-cuda12.jar
```

**Note:** Adjust the spark-rapids-jni version and CUDA version as needed for your environment.

### Run Simple Inner Join Example

From within spark-shell, paste the contents of:
```bash
:load examples/simple_inner_join.scala
```

Or manually paste the code from `examples/simple_inner_join.scala`.

### Run Conditional Join Example

```bash
:load examples/conditional_join.scala
```

### Run All Join Types Example

```bash
:load examples/all_join_types.scala
```

This comprehensive example includes extensive test coverage:
- All 6 join types (Inner, LeftOuter, RightOuter, FullOuter, LeftSemi, LeftAnti)
- Multiple build side configurations
- All join strategies (Object and Direct variants)
- Various optimization combinations

### Run Object Creation Overhead Benchmark

Measures the overhead of join object creation (JNI calls) versus direct API calls:

```bash
:load examples/object_creation_overhead.scala
```

Tests across different data sizes (10K-5M rows), key types (INT, LONG, STRING), selectivities (10%-100%), and join types to determine if overhead is fixed or data-dependent. Runtime: approximately 1-2 minutes including data generation and warmup.

### Run Distinct Join Analysis Benchmark

Analyzes the performance benefits and costs of the distinct join optimization:

```bash
:load examples/distinct_join_analysis.scala
```

**Part 1:** Measures performance benefit when data IS distinct
- Compares DistinctHashJoin vs regular HashJoin
- Shows speedup percentages for distinct data

**Part 2:** Measures distinctCount() check overhead when data is NOT distinct
- Compares HashJoin with useDistinctJoin=true vs false
- Quantifies cost of the distinctness check

Tests across different data sizes (10K-10M rows) for both INT and STRING keys to understand how distinctness checking scales with data size and key complexity. Runtime: approximately 3-5 minutes including data generation and warmup.

### Run Hash vs Sort Join Analysis

Compares HashJoin vs SortMergeJoin performance to build a heuristic for deciding when to use each strategy:

```bash
:load examples/hash_vs_sort_analysis.scala
```

**Key Questions Answered:**
- How does build-side cardinality (% distinct keys) affect hash vs sort performance?
- What is the exact crossover point? (Fine-grained 1-10% sweep with 2%, 4%, 6%, 8% tests)
- **Does key distribution skew affect the decision?** (NEW!)
  - Tests uniform vs Zipf-distributed keys at different skew levels
  - Theory: Hash joins may suffer with skewed data (unbalanced buckets), sort joins handle skew better
- Which key types favor hash joins vs sort joins?
- How do hash and sort joins scale with data size?
- Does cardinality matter more on the build side or probe side? (Asymmetric join tests)

**Test Coverage:**
- **Symmetric joins:** 1M to 25M rows
  - Cardinality: Full sweep (1%, 2%, 4%, 6%, 8%, 10%, 25%, 50%, 75%, 100%)
  - Key types: INT (single/multi), LONG (single), STRING (single/multi)
- **Skewed distribution tests:** 5M rows at crossover cardinalities (2%, 4%, 6%, 8%, 10%)
  - Uniform (baseline), Zipf s=0.5 (light skew), s=1.0 (moderate), s=1.5 (heavy)
  - Identifies if crossover point shifts with distribution skew
- **Asymmetric joins:** Build vs probe side analysis
  - Small build (1M) with large probe (10M)
  - Large build (10M) with small probe (1M)
  - Cardinality sweep: 2%, 4%, 6%, 8%, 10%, 50%, 100%
- Build side swapping DISABLED for true performance comparison
- Iterations: 20 per test (faster runtime with good statistical confidence)

**Output includes heuristic recommendations:**
- If skew matters: 2D decision (cardinality + skew metric)
- If skew doesn't matter: Simple cardinality threshold
- Specific threshold values based on benchmark results

Runtime: approximately 60-120 minutes (comprehensive suite with skew + asymmetric tests).

## Implemented Features

### Core Functionality
- ✅ Data generation using datagen APIs (all types supported via DDL strings)
- ✅ Multi-column composite keys
- ✅ All join types:
  - Inner, LeftOuter, RightOuter, FullOuter, LeftSemi, LeftAnti
- ✅ Multiple join strategies:
  - Object strategies: `HashObjectStrategy`, `HashObjectWithPostStrategy`, `SortObjectWithPostStrategy`
  - Direct strategies: `HashDirectStrategy`, `HashDirectWithPostStrategy`, `SortDirectWithPostStrategy`
- ✅ All build side selection modes:
  - `LeftBuild` / `RightBuild` - Explicit build side
  - `AutoPickSmallerIfAllowed` - Pick smaller side when allowed
  - `AutoMeetJoinRequirement` - Always use required side
- ✅ Build holder pattern with caching support
- ✅ AST-based conditional joins (keys + filtering)
- ✅ Multi-threaded execution with per-thread build holders
- ✅ TSV output for spreadsheet analysis

### Optimizations
- ✅ **Key remapping** - Remaps complex types (String, Decimal, composite) to dense integers
- ✅ **Distinct join optimization** - Uses `DistinctHashJoin` when build side has distinct keys
- ✅ **Distinct flag caching** - Avoids repeated `distinctCount()` checks
- ✅ **Join object caching** - Reuses hash tables across iterations
- ✅ **Build side swapping** - Automatically picks smaller side when allowed
- ✅ **Remapping structure caching** - Reuses remapping structures across iterations

## Configuration Reference (JoinBenchmarkConfig)

Key fields for configuring a benchmark run:

- testName: Label for the test row in TSV output
- leftParquetPath / rightParquetPath: Paths to the single-file Parquet inputs
- joinType: One of Inner, LeftOuter, RightOuter, FullOuter, LeftSemi, LeftAnti
- joinStrategy: One of HashObject, HashObjectPost, SortObjectPost, HashDirect, HashDirectPost, SortDirectPost
- buildSide: Left, Right, Auto(Smaller), Auto(Required)
- optimizations:
  - allowBuildSideSwap: Enable build-side swap when allowed by join type/strategy
  - remapComplexKeysToInts: Remap complex keys (e.g., string/decimal/composite) to dense ints
  - useDistinctJoin: Use DistinctHashJoin when build-side keys are distinct (inner joins)
  - cacheJoinObject: Cache HashJoin/SortMergeJoin/FilteredJoin objects across iterations
  - cacheRemapping: Cache key remapping structures across iterations
  - cacheDistinctFlag: Cache the distinctness check result across iterations
- leftKeyIndices / rightKeyIndices: Key column indices (default: Seq(0))
- iterations: Iterations per thread
- numThreads: Number of concurrent threads
- printHeader: Whether to print the TSV header

Example: multi-column keys and custom key indices

```scala
val cfg = JoinBenchmarkConfig(
  testName = "inner_multi_key",
  leftParquetPath = "/data/left.parquet",
  rightParquetPath = "/data/right.parquet",
  joinType = InnerJoin,
  joinStrategy = HashObjectWithPostStrategy,
  buildSide = AutoPickSmallerIfAllowed,
  optimizations = JoinOptimizations(
    allowBuildSideSwap = true,
    useDistinctJoin = true,
    cacheJoinObject = true,
    cacheDistinctFlag = true
  ),
  leftKeyIndices = Seq(0, 2),   // composite key: columns 0 and 2 on left
  rightKeyIndices = Seq(0, 2),  // composite key: columns 0 and 2 on right
  iterations = 20,
  numThreads = 1,
  printHeader = true
)
```

## Example Output

The benchmarks output TSV format that can be pasted into a spreadsheet. Header and a sample row are shown below (fields match the implementation in `printTSVHeader` and `printResultsTSV`):

```
TestName	Status	LeftRows	RightRows	OutputRows	NumThreads	Iterations	WallClockMs	AvgTimeMs	MedianTimeMs	MinTimeMs	MaxTimeMs	StdDevMs	JoinType	Strategy	BuildSideConfig	ActualBuildSide	Optimizations
hash_inner_1M_1M	SUCCESS	1000000	1000000	1000000	1	10	152.45	15.12	14.98	14.23	17.45	0.89	Inner	HashObject	Right	Right	none
```

## Architecture

The framework follows a three-layer architecture:

1. **Data Generation Layer** (`JoinBenchmarkDataGen`)
   - Uses datagen APIs for reproducible data
   - Generates correlated left/right tables
   - Writes to single Parquet files

2. **Benchmark Execution Layer** (`JoinBenchmarkRunner`)
   - Loads Parquet into GPU memory
   - Runs multiple iterations
   - Collects timing statistics
   - Outputs TSV results

3. **Low-Level Join Layer** (`JoinExecutor` + Build Holders)
   - Build holder pattern for caching
   - Supports key-only and mixed (key+AST) joins
   - Strategy-specific implementations

## Future Enhancements

Potential areas for future development:
- Automated heuristics for strategy selection based on workload characteristics
- Additional performance visualizations and analysis tools
- Support for additional join types or custom join operations
