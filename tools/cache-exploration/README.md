# Arrow / Vortex cache codec experiment

This standalone Java 17 harness checks Vortex cache-codec paths with CPU or GPU
population and consumption. CPU population writes an Arrow Java host batch directly
through C Data to Vortex. CPU consumption reads Arrow batches directly. GPU endpoints
use the public cuDF CPU Arrow IPC bridge, including transfer/conversion costs.
It does not register a Spark cache serializer or change production code.

The comparison leg is **cuDF Parquet with Snappy**, an 8 MiB row-group target and
row-group statistics. It is a codec proxy, **not branch PCBS end-to-end**: it omits
PCBS cache-record framing, spillable staging, reader bundling and Spark storage.
Do not report these timings as application-level cache performance.

## Prerequisites

Build the matching Vortex Java classes and optimized JNI library. The Vortex
Gradle build defaults to a debug native library; use a verified release library
for timings. Export the unshaded Vortex Java runtime classpath to a text file.
For this sandbox the build worker exported
`/data/tmp/vortex-java-runtime-classpath.txt`.

The checked-out Vortex writer has a small Arrow C Data ownership defect: its builder
borrows the exported schema in native code, then closes only the struct without
calling its release callback. The allocator guard exposed the leak. For this
experiment, apply `vortex-writer-schema-release.patch` to an isolated build copy;
the original /data/vortex checkout is unchanged. The harness also explicitly
releases the borrowed schema for each writeBatch call.

For a full isolated Vortex checkout, apply with:

```text
git -C /path/to/isolated-vortex apply /home/roberte/src/rapids-plugin-4-spark/tools/cache-exploration/vortex-writer-schema-release.patch
```

For this session's Java-only build overlay, the equivalent application is
`patch -p2 -d /data/tmp/vortex-java-build -i /home/roberte/src/rapids-plugin-4-spark/tools/cache-exploration/vortex-writer-schema-release.patch`.
The overlay already contains this repair. Rebuild its Java classes with
`env VORTEX_SKIP_MAKE_TEST_FILES=true gradle --wrapper ./gradlew :vortex-jni:classes`
from /data/tmp/vortex-java-build. The environment setting suppresses rebuilding
the native test library because the optimized library is built and staged separately;
it does not suppress Java tests.

The installed `cudf-spark-jni` jar supplies the cuDF classes and native library.
The harness uses Arrow Java 19 from the Vortex classpath; do not append Spark's
older Arrow jars. Coexistence with Spark's classloader remains separate work.

Run through MCP docker_exec, as required for all project operations:

```text
python tools/cache-exploration/run.py \
  --vortex-classpath /data/tmp/vortex-java-runtime-classpath.txt \
  --cudf-jar /home/roberte/.m2/repository/com/nvidia/cudf-spark-jni/26.12.0-SNAPSHOT/cudf-spark-jni-26.12.0-SNAPSHOT-cuda12.jar \
  --native-lib /data/vortex/target/release/libvortex_jni.so \
  --fixture strings-low --rows 65536 --warmups 2 --repeats 5 \
  --mode gpu-gpu --codec vortex
```

Use `--mode cpu-cpu`, `cpu-gpu`, `gpu-cpu` or `gpu-gpu`; the first endpoint
populates and the second consumes. `cpu-cpu` executes with
`CUDA_VISIBLE_DEVICES=` and never calls cuDF or CUDA, so it works with GPU execution
disabled. The cuDF jar is still present for compilation and shared class signatures.
The other modes initialize CUDA before measured work.

Select `--codec vortex` for isolated Vortex runs, `--codec parquet-proxy` for
the GPU-only codec proxy, or `--codec both` to alternate both in one GPU-GPU process.
Defaults are `both` for GPU-GPU and `vortex` otherwise. Parquet proxy CPU/mixed
paths are unsupported; these are separate from the actual PCBS harness.

For a small CPU-only correctness run, append
`--fixture strings-low --rows 1024 --warmups 0 --repeats 1 --mode cpu-cpu --codec vortex`.
For same-byte CPU/GPU interoperability checks, select `cpu-gpu`, `gpu-cpu`
or `gpu-gpu` and append `--verify-cross-read`. It validates the exact same encoded
unit through the other reader outside the selected timing. It is rejected for
CPU-CPU, which must remain CPU-only. Omit this flag for process-memory runs:
the extra reader would change the measured footprint.

Append `--java-command-file /data/tmp/vortex-command.json` to compile and stage the
library, then write the exact Java execution argv as JSON and exit. CPU-only commands
include an `env CUDA_VISIBLE_DEVICES=` prefix. This allows process sampling to run
the JVM without including javac. For example:

```text
python tools/cache-exploration/measure_process.py \
  --command-file /data/tmp/vortex-command.json \
  --output-dir /data/tmp/vortex-process-sample --heap --gpu
```

The runner compiles to an external scratch directory, stages the supplied native
library as the classpath resource Vortex's NativeLoader loads, and prints its SHA256
to stderr. Use `--compile-only` to check compilation before the library is built.
It does not change repository build files or native loader implementation.

## Correctness and timing

Fixtures are deterministic:

- `fixed`: signed int64 IDs plus nullable int32 keys.
- `strings-low`: the same columns plus nullable strings from 32 groups.
- `strings-wide`: the same columns plus nullable varying strings derived from row IDs.

Every decoded value, null, row count and row order is checked at the selected
consumer (Arrow host vectors for CPU, copied host values after import for GPU).
CPU and GPU population use identical deterministic fixture values and logical
schema. Vortex scans explicitly request ordered output. For GPU consumption,
Vortex Arrow Utf8View output is written directly to IPC: the tested cuDF runtime
supports importing it, so no extra string normalization is performed. GPU paths
use an IPC bridge, not a direct native cuDF C Data bridge.

Each run prints individual samples as CSV. Warmups run the same checks but do not
print samples; codec order alternates when both are selected. Data generation and
CUDA initialization are excluded. GPU device synchronization precedes completion
of phases that perform GPU work. Host/GPU correctness checks are excluded from
measured decode time. Optional cross-reader checks are also excluded from timings,
but remain part of process lifetime and memory if requested.

Columns use nanoseconds:

| Column | Meaning |
| --- | --- |
| `export_ns` | GPU population's cuDF whole-input CPU Arrow conversion, IPC serialization and host-to-Java copying; zero for CPU population and Parquet. |
| `encode_ns` | CPU population: C Data export, Vortex write/finalization. GPU population also parses the exported IPC first. Parquet proxy: GPU encode and host-to-Java copying. |
| `decode_ns` | Full selected decode path excluding validation: Vortex open/scan and Arrow materialization, plus IPC/GPU import for GPU consumption, including cleanup. GPU Parquet read/cleanup for the proxy. |
| `gpu_import_ns` | Subset of Vortex decode spent constructing/reading the cuDF IPC reader and synchronizing GPU completion. |
| `roundtrip_ns` | `export_ns + encode_ns + decode_ns`; data generation and correctness checks excluded. |
| `logical_bytes` | Canonical Arrow-layout estimate defined below, identical across CPU/GPU modes and independent of device size. |
| `input_gpu_bytes` | cuDF input table's reported device memory size; zero for CPU population, including CPU-GPU. |
| `encoded_bytes` | Completed stored Vortex file or Parquet file length. |
| `encoded_bytes_per_row` | Stored encoded bytes divided by logical rows represented. |
| `logical_to_encoded` | Canonical logical-byte estimate divided by stored encoded bytes. |
| `cross_read_checked` | Whether this exact encoded unit also passed the optional other-reader check. |

The canonical Arrow-layout estimate counts 8-byte ID and 4-byte key slots per row,
including null key slots, plus one `ceil(rows/8)` validity bitmap for every declared
nullable schema column. String fixtures add 4-byte offsets for `rows+1` positions
and UTF-8 bytes for nonnull strings. This deliberately ignores allocator padding
and representation-specific compression. It is neither a Spark size estimate nor
a measured resident allocation. The actual PCBS harness uses the same definition.

A source host batch or GPU table is reused across warmups and samples. Each encoded
unit is finalized independently. Multiple Vortex scan batches are consumed and
checked one at a time, without concatenation. CPU population does not serialize
its Arrow batch to IPC before Vortex; CPU consumption does not create GPU tables.

## Resource limits and remaining work

Rows are explicitly limited, repeats/warmups are finite, and growable IPC/encoded
sinks fail if `--cap-bytes` is exceeded (default 128 MiB). Growth can temporarily
retain both old and new arrays. The canonical logical-byte estimate is checked
before fixture allocation, while physical GPU input bytes are checked afterward;
this harness does not implement an upstream GPU input splitter. Run a row-size
sweep to select small independent units.

Capped Java sinks do not establish a bound on Arrow or Rust working memory.
Vortex DataSource/Scan/Session cleanup relies on cleaners, so finite iterations
matter and retained allocations require separate measurement. The harness
checks that Arrow Java's live allocation returns to the reusable host-fixture
baseline after every codec unit; root allocator closure enforces final zero after
the fixture closes. That does not account for Rust/native memory or JVM heap.

Not yet covered: Spark integration, released PCBS behavior, constrained-heap
pressure, spill/cancellation, native RSS/GPU peak-memory sampling, projection,
decimal/timestamp/nested schemas, and large parent GPU batches. Do not infer
default-Spark OOM resilience from this experiment.

## Local Spark measurements

The separate [Spark prototype](spark/README.md) and `spark_cache_bench.py` now cover
actual Spark default/PCBS/Vortex cache paths. Launch them with `run_spark.py` after
building the prototype. The Vortex prototype is local Spark3.5.5-only and currently
supports int/long/string. Its CPU row route differs from vectorized default/PCBS
consumption; do not present its timings as the format's intrinsic limit.

The launcher uses1GiB Java heap,4GiB RAPIDS-managed host cap and2task slots by default.
The managed cap excludes Arrow/Rust. A sampled4GiB process-tree RSS guard and timeout
stop conservative experiments; no machine-exhaustion search is authorized. Startup,
reference and optional payload-audit work are included in whole-job memory samples.
Payload audit runs AFTER timing/storage snapshots because it can recompute blocks.

```text
python tools/cache-exploration/run_spark.py --format pcbs --write-mode gpu \\
  --read-modes cpu,gpu --fixture fixed-wide --task-size-mib 256 \\
  --tasks 2 --partitions 4 --warmups 1 --repeats 2 --payload-audit \\
  --output-dir /data/tmp/pcbs-256m-per-task
```

Approved scale choices are256,512,1024,2048MiB logical data per task, using exactly
two waves. The32-field fixed-wide fixture has a256B/row standardized estimate and
streamed generation. It mixes monotonic, low-cardinality and hashed longs with nulls;
it is different from the small string fixture. Start at256MiB and inspect footprint
before advancing. A task may produce multiple GPU batches and many small cache records.
Report only actual producer batch counters exposed by the plan, not inferred counts.

Raw outputs separate population nanoseconds and every read sample/warmup, actual
CPU/GPU cache scanning, encoded payload, BlockManager residency/spill, logical rows
resident, driver heap and RMM counters. Final reports must distinguish read-only
reuse from population+read. Label sums of phase times as computed totals. No pressure
resilience or production compatibility claim follows from the first bounded runs.
