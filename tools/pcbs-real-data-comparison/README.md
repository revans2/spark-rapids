# Real-row CPU / PCBS codec comparison

Unreleased diagnostic harness; no production defaults or APIs change. Source ready
for review. Stock-only compilation, canonical/CPU correctness smokes, both
common GPU correctness smokes and the separate GPU disk/wire gate passed.
The exact96 GiB canonical CPU materialization is in progress; full cache
benchmarks remain unrun. All project operations use MCP docker_exec tools.

This is different from the 384-Long synthetic padded serializer fixture. Full
input has **805,306,368 actual rows × 16 non-null DOUBLE values = 96 GiB** of
logical values. Each of 16 cache partitions owns 50,331,648 rows (6 GiB). File
and cache formats add overhead and compression; neither is forced to 96 GiB.

`canonical` generates the deterministic salted xxhash64/mask53/DOUBLE expression
used in the earlier CPU/GPU generator oracle, using CPU Spark once. It writes
16 Parquet files, checks their footer row counts and schema, and computes the
all-column SQL aggregate oracle. The manifest freezes full-file SHA256, footer
SHA256, file sizes, modification times and CPU sums. Each benchmark hashes the
same files before launching its JVM. This warms the OS page cache: results are
not cold-disk claims. Manifest/oracle creation and hashing are outside fill/read
timing. Do not replace canonical files or compare runs from different manifests.

Each file is read as a separate coalesce(1) child, then 16 children are unioned.
This preserves one file/cache partition despite default Parquet scan splitting.
There is no shuffle repartition. Actual cached RDD partition count and each
partition's fully consumed row count must agree with the manifest.

CPU uses stock Spark3.5.5, no RAPIDS jar/plugin/overlay, default JavaSerializer,
DefaultCachedBatchSerializer, 10,000-row batches and compression enabled. GPU
uses the separate research PCBS provider and per-cache-codec Spark overlay;
it is not an old Java-PCBS control. Target remains 1 GiB, device pool15 GiB,
GPU concurrency2, host40 GiB/pinned2 GiB. Split and combined modes are separate
artifacts, not new public configurations. Provider:
`org.apache.spark.sql.pcbsrealdata.ResearchParquetCachedBatchSerializer`.
`PcbsCodecSnapshot.snapshotJson` is accessed by reflection only in GPU mode.
The common harness compiles against stock Spark jars only.

Both modes use local[8], Xms/Xmx48g, ParallelGC4 and MEMORY_AND_DISK. Memory
fraction, unroll checks and Java stream reset remain defaults. No forced GC.
All16 cache partitions populate concurrently through the actual cache RDD,
returning small summaries. Fill timing includes canonical-file input, cache
encoding and persistence. It is not comparable to historical sequential fill.
One warmup and three measured fresh all-column sum + count queries follow.
Every SQL scan must output all16 columns and the full row count. Verification
compares every sum against the shared CPU oracle with relative tolerance1e-10;
this aggregate is not a full row-by-row oracle for the large dataset.

CPU encoded bytes are the sum of DefaultCachedBatch.buffers lengths. Separately
record CachedBatch.sizeInBytes (statistics; not compressed bytes), record counts,
maximum record size/rows, BM memory/disk residency after fill/every read,
sampled heap occupancy, cumulative GC deltas and raw plans/events. GPU uses the
actual shim-loaded object's buffer accessor; codec and decode counters are
attempt totals and may include retries. Standard coalescer metrics are logged.
Whole-process RSS samples include JVM and native memory, not isolated native
retention. Warmup is excluded from read median. OOM/error results remain in
result.json; they are not cache-correctness passes. Successful blocking
unpersist must remove the actual cached RDD's storage entry.

## Commands (review before execution)

No command auto-launches a matrix. Every destination must be new. `build`
uses only the installed Spark3.5.5 assembly jars and a separate output jar.
Use the build action first:

```sh
python3 run_bench.py build --out /data/pcbs-oom/real-data/build-small
python3 run_bench.py canonical --out /data/pcbs-oom/real-data/canonical-small-run \
  --bench /data/pcbs-oom/real-data/build-small/real-cache-bench.jar \
  --canonical /data/pcbs-oom/real-data/canonical-small --rows-per-partition 60000
python3 run_bench.py cpu --out /data/pcbs-oom/real-data/cpu-small \
  --bench /data/pcbs-oom/real-data/build-small/real-cache-bench.jar \
  --canonical /data/pcbs-oom/real-data/canonical-small --rows-per-partition 60000 \
  --manifest /data/pcbs-oom/real-data/canonical-small-run/canonical-manifest.json --smoke
```

GPU commands additionally require `--spark-overlay /absolute/overlay.jar` and
one `--gpu-jar` for the selected split-only/combine plugin artifact and one for
the provider jar. They cannot run without those explicit arguments. Root must
review artifact identity, native/device gates and standard coalescer behavior
before allowing GPU cases. A tiny fixture may not exercise disk/codec IO or
32 MiB splitting; its result is a correctness gate, not the full-pressure pass.
Use `--smoke` for an explicit SMOKE_PASS (no performance acceptance). A separate
`--smoke --disk-roundtrip` selects DISK_ONLY solely for the codec roundtrip gate;
it never changes the full comparison's MEMORY_AND_DISK. Full GPU PASS requires
codec IO, exact per-read decoded row deltas, and actual record/decoded sizes
<=34 MiB for this fixed schema (32 MiB budget plus a documented2 MiB allowance).
Combined full PASS also requires actual maximum output900 MiB..1 GiB+34 MiB,
reduced batch count and direct post-scan standard-coalescer evidence. This is
measured fixture acceptance, not a universal record size guarantee. Failures
must be reported without tuning those guards. Maxima are cumulative attempts;
mean bytes use per-read byte/batch deltas. Extra retry rows are reported as a
mismatch needing inspection, never silently accepted.

Only after the small gates and source review may root authorize the full
canonical materialization, then CPU/default, GPU32split and GPU32combine. Omit
`--rows-per-partition` for the exact96 GiB input. Unsplit references and further
record-size matrices are outside this initial plan. Completed small evidence:
`/data/pcbs-oom/pcbs-real-data-comparison/build-small`, `canonical-small-run2`
and `cpu-small`; CPU SMOKE_PASS has960,000real rows,96default batches,
122,892,288encoded buffer bytes, four full16-column cache scans and successful
blocking cleanup. The first canonical launch failed because spark-submit
rejects /dev/null as a properties file; its log is retained in canonical-small-run.
The runner now creates an empty regular properties file. `gpu32split-small`
and `gpu32combine-small` both SMOKE_PASS on the identical960,000 canonical
rows:16real Parquet cache records,122,927,776encoded bytes, four full GPU cache
scans/GpuHashAggregate reads with all CPU-oracle sums matching. Their small
MEMORY_AND_DISK data remains memory-resident, so codec IO counters are zero;
SMOKE_PASS deliberately does not claim disk/codec pressure. Both cached plans
show16 pre-cache GpuCoalesceBatches targetsize1073741824 nodes.

The separate `gpu-small-gate2/{only,combine}` DISK_ONLY gate uses128 MiB of
known-value rows, validates every Parquet payload byte through a codec roundtrip
and actual shim-class identity, then verifies all16 sums on GPU. Five decoded
records combine into one134,217,728-byte device batch in combined mode. Its
compressed payload bytes are not the canonical random-input size. The first
GPU gate's classloader failure is retained; corrected GPU driver/executor
classpath is overlay:provider:selectedPlugin, with those jars also supplied
through --jars. CPU continues to have neither overlay nor plugin.

Canonical full data is currently being written once by stock CPU under
`canonical96-run1`/`canonical96`; no full cache benchmark has run. Preserve old
fixtures and their negative outcomes.
