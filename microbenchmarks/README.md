# Join Micro-Benchmarks

Phase 1 & 2 implementation of the join micro-benchmark framework for measuring GPU join performance at the lowest level.

## Building

### Prerequisites
1. Build the datagen module first (required dependency):
```bash
cd ../datagen
mvn clean install -Dbuildver=353  # Use your Spark version (330, 353, 400, etc.)
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

### Run All Join Types Example (Phase 2 - Comprehensive Tests)

```bash
:load examples/all_join_types.scala
```

This comprehensive example runs 29 test cases covering:
- All 6 join types (Inner, LeftOuter, RightOuter, FullOuter, LeftSemi, LeftAnti)
- Multiple build side configurations
- All 3 join strategies
- Various optimization combinations

## Implemented Features (Phase 1 & 2)

### Core Functionality
- ✅ Data generation using datagen APIs (Int, Long key types)
- ✅ Multi-column composite keys
- ✅ **All join types supported:**
  - Inner, LeftOuter, RightOuter, FullOuter, LeftSemi, LeftAnti
- ✅ Three join strategies:
  - `HashJoinStrategy` - Direct hash join (inner join only)
  - `HashWithPostStrategy` - Hash join with post-processing (all join types)
  - `SortWithPostStrategy` - Sort-merge join with post-processing (all join types)
- ✅ All build side selection modes:
  - `LeftBuild` / `RightBuild` - Explicit build side
  - `AutoPickSmallerIfAllowed` - Pick smaller side when allowed
  - `AutoMeetJoinRequirement` - Always use required side
- ✅ Build holder pattern with caching support
- ✅ AST-based conditional joins (keys + filtering)
- ✅ TSV output for spreadsheet analysis
- ✅ Single-threaded execution (Phase 4 will add multi-threading)

### Optimizations
- ✅ **Distinct join optimization** - Uses `DistinctHashJoin` when build side has distinct keys
- ✅ **Distinct flag caching** - Avoids repeated `distinctCount()` checks
- ✅ **Join object caching** - Reuses hash tables across iterations
- ✅ **Build side swapping** - Automatically picks smaller side when allowed

### Not Yet Implemented (Future Phases)
- ❌ Key remapping optimization - Phase 5
- ❌ Multi-threaded execution - Phase 4
- ❌ String/Decimal key types - Phase 4

## Example Output

The benchmarks output TSV format that can be pasted into a spreadsheet:

```
TestName	Status	LeftRows	RightRows	OutputRows	NumThreads	Iterations	WallClockMs	AvgTimeMs	MedianTimeMs	MinTimeMs	MaxTimeMs	StdDevMs	Optimizations
hash_inner_1M_1M	SUCCESS	1000000	1000000	1000000	1	10	152.45	15.12	14.98	14.23	17.45	0.89	none
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

## Next Steps

Phase 3 (Join Object APIs - Completed):
- HashJoin, DistinctHashJoin, SortMergeJoin, FilteredJoin APIs

Phase 4 will add:
- Multi-threaded execution support
- Advanced strategies and threading
- String/Decimal key types

Phase 5 will add:
- Key remapping optimization for complex types

See `JOIN_MICROBENCHMARK_PLAN.md` for the complete roadmap.

