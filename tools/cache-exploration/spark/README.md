# Local Spark Vortex cache prototype

This experiment compiles only for the mounted Spark 3.5.5 / Scala 2.12 runtime.
It preserves production source and build profiles. It is not a supported serializer.

`SparkVortexSerializer` subclasses the public PCBS wrapper solely because the
existing GPU scan checks and casts that concrete class. Every serializer method,
including the GPU entry point, is overridden. No PCBS encode/decode path is used.
The configured experimental class is
`org.apache.spark.sql.cacheexploration.SparkVortexSerializer`.

The supported schema is int/long/string (including nulls). Other schemas fail with
an explicit unsupported-combination message; no silent PCBS fallback is provided.
CPU consumption uses the required InternalRow path, not CPU vectorized output.
Spark default and PCBS can use vectorized CPU scans. Timing differences therefore
compare these current implementations and output routes; they do not establish an
intrinsic performance ceiling for the Vortex format.
Both CPU and GPU consumers read the same stored Vortex files. Projection uses
attribute identity to select/reorder Vortex fields, preserving duplicate/name
independence. Count-only consumption uses record row-count metadata.

## Dependency isolation

Spark uses Arrow 12 / Netty 4.1; Vortex uses Arrow 19 / Netty 4.2. A per-JVM loader
with the Java platform loader as its parent owns Vortex's complete unshaded
dependency closure. A reflective facade exchanges IPC byte arrays, primitive
arguments, and an opaque decoder object; no Spark/cuDF/Scala/Arrow object is
passed across the dependency boundary.

CPU population uses Spark's own Arrow writer. CPU decode normalizes Utf8View to
ordinary Utf8 inside the isolated loader before returning IPC to Spark Arrow 12.
GPU population/consumption uses cuDF's public Arrow IPC APIs; GPU decode accepts
Vortex view strings directly. These paths include IPC copying and are not a direct
native C Data bridge.

The child classpath is captured in the generated jar as local absolute paths.
This first build supports local mode only; it does not distribute those artifacts
to remote executors. Vortex's schema-release overlay patch from the parent
experiment must already be applied/recompiled.

## Build and correctness probe

Run every project operation through MCP docker_exec.

```text
python tools/cache-exploration/spark/build.py \
  --vortex-classpath /data/tmp/vortex-java-runtime-classpath.txt \
  --native-lib /data/vortex/target/release/libvortex_jni.so \
  --cudf-jar /home/roberte/.m2/repository/com/nvidia/cudf-spark-jni/26.12.0-SNAPSHOT/cudf-spark-jni-26.12.0-SNAPSHOT-cuda12.jar \
  --build-dir /data/tmp/vortex-spark-prototype-rebuild
```

Current source uses the original 8 MiB estimated record target. The measured build
remains unchanged under `/data/tmp/vortex-spark-prototype`; use a separate output
directory for rebuilds. An isolated 1 MiB-target trial under
`/data/tmp/vortex-spark-prototype-1m` hit the earlier constrained 4 GiB RSS guard;
its source is archived there. It is not the primary benchmark build, and the
guard stop is not evidence of host OOM or a normal-host memory failure.
The build writes the
experiment jar and `plugin-jars.txt`, the comma-separated native/private/raw
plugin dependencies for Spark submission. Use the current raw Spark355 module
jars, including UDF and Delta probe modules needed by plugin startup. Artifact
provenance must be recorded for measurement runs; matching filenames alone do
not prove those supplemental modules are current.

Submit the jar with Spark's spark-submit, the dependency list passed through
`--jars`, `--class org.apache.spark.sql.cacheexploration.SparkCacheProbe`, and
`--driver-java-options --add-opens=java.base/java.nio=ALL-UNNAMED`.
Set SPARK_LOCAL_IP=127.0.0.1 in this sandbox. Probe arguments are:

```text
vortex cpu strings-low 1024 cpu
vortex gpu strings-low 1024 cpu,gpu
vortex cpu strings-wide 1024 gpu,cpu
```

The CPU-only first case does not load the RAPIDS Spark plugin and can run with
CUDA_VISIBLE_DEVICES empty. Mixed cases load the plugin, disable SQL GPU execution
for CPU population/consumption, and re-enable it for GPU consumption of the same
cache. The probe checks every value/null/row order, full reads and reordered narrow
projections, and asserts that GPU consumption contains an actual
GpuInMemoryTableScan. It is a correctness probe, not a performance benchmark.

## Memory limitations

CPU row-to-Arrow input is generated lazily using Spark's estimated byte budget.
GPU input batches are retained as spillable parents; each slice is produced lazily
using an 8 MiB byte estimate and at most 100,000 rows, then checked before export
against a 64 MiB hard per-unit cap. GPU allocations use the existing retry/ARM
patterns. Each encoded file and decoded IPC sink is capped at 64 MiB; these are
experimental constants, not production configuration or total memory budgets.

A very wide row can exceed the target; a row/slice exceeding the hard cap fails
explicitly. CPU inputs and sink growth can allocate before checks or overlap old
and new buffers. Decoder IPC batches are consumed one at a time. Task listeners
and exhaustion close Arrow readers and spillable parents. The isolated decoder
owns raw Vortex session/source/scan/expression handles and frees them explicitly,
then closes its caller-owned byte source. That source synchronizes reads with close
and drops its encoded-array reference, so canceled native tasks cannot pin evicted
Java cache arrays. CPU decode uses one task-owned reader cursor and clears consumed
IPC/reader references rather than retaining per-record task listeners.

With Vortex's default zero background workers, canceled Rust tasks may defer final
native destruction until another blocking Vortex call drives the runtime. No worker
threads are added to hide that behavior, and native working memory has no demonstrated
task budget. No pressure/OOM-resilience claim follows from the correctness probe.
VortexLifecycleProbe checks that encoded arrays become collectible after exhaustion,
partial/unread close and constructor failure, including idempotent close. It forces GC
only for ownership verification; it is not a memory/performance benchmark.

## Optional payload audit

`org.apache.spark.sql.cacheexploration.CachePayloadAudit.summarize(DataFrame)`
returns JSON scalar summaries per cached partition: record count, logical row
count and actual encoded array lengths. It recognizes Spark default buffers,
PCBS buffers and experimental Vortex bytes.

This executes an RDD action and can reread/recompute blocks or change residency.
Run it only after timing/storage snapshots, and exclude it from capacity/memory
measurements. Spark's sizeInBytesStats is not a uniform encoded-byte measurement;
BlockManager memory is a separate footprint.
