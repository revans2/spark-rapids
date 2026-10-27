# Arrow / Vortex cache codec experiment

This standalone Java 17 harness checks the proposed cuDF GPU -> CPU Arrow IPC ->
Vortex bytes -> CPU Arrow IPC -> cuDF GPU path. It does not register a Spark cache
serializer or change production code.

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
  --fixture strings-low --rows 65536 --warmups 2 --repeats 5
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

Every decoded value, null, row count and row order is checked after importing to
the GPU. Vortex scans explicitly request ordered output. Vortex Arrow Utf8View
output is written directly to IPC: the tested cuDF runtime supports importing it,
so no extra string normalization is performed. This is an IPC bridge, not a direct
native C Data bridge.

Each run prints individual samples as CSV. Warmups run the same checks but do not
print samples; codec order alternates to reduce systematic order bias. GPU device
synchronization precedes timing completion. Correctness copies from GPU to CPU
and value comparisons are excluded from measured decode time.

Columns use nanoseconds:

| Column | Meaning |
| --- | --- |
| `export_ns` | cuDF whole-input CPU Arrow conversion, IPC serialization and host-to-Java copying; zero for Parquet. |
| `encode_ns` | Arrow Java IPC reading, C Data export, Vortex write and finalization; Parquet GPU encode and host-to-Java copying for the proxy. |
| `decode_ns` | Full decode path excluding validation: Vortex open/scan, Arrow IPC, GPU import and cleanup; GPU Parquet read for the proxy. |
| `gpu_import_ns` | Subset of Vortex decode spent constructing/reading the cuDF IPC reader and synchronizing GPU completion. |
| `roundtrip_ns` | `export_ns + encode_ns + decode_ns`; data generation and correctness checks excluded. |
| `input_gpu_bytes` | cuDF input table's reported device memory size. |
| `encoded_bytes` | Completed Vortex file or Parquet file length. |

A source table is reused across warmups and samples. Each encoded unit is finalized
independently. Multiple Vortex scan batches are imported and checked one at a time,
without GPU concatenation. This avoids introducing an extra concatenation cost.

## Resource limits and remaining work

Rows are explicitly limited, repeats/warmups are finite, and growable IPC/encoded
sinks fail if `--cap-bytes` is exceeded (default 128 MiB). Growth can temporarily
retain both old and new arrays. Input generation allocates the selected fixture
before checking its GPU byte size; this harness does not implement an upstream
GPU input splitter. Run a row-size sweep to select small independent units.

Capped Java sinks do not establish a bound on Arrow or Rust working memory.
Vortex DataSource/Scan/Session cleanup relies on cleaners, so finite iterations
matter and retained allocations require separate measurement. The harness
asserts that Arrow Java's allocator has no live allocations after the run, but
that does not account for Rust/native memory or JVM heap.

Not yet covered: Spark integration, released PCBS behavior, constrained-heap
pressure, spill/cancellation, native RSS/GPU peak-memory sampling, projection,
decimal/timestamp/nested schemas, and large parent GPU batches. Do not infer
default-Spark OOM resilience from this experiment.
