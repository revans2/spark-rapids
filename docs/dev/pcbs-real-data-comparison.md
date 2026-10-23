# PCBS record splitting, read coalescing and stock CPU caching

Status: IN PROGRESS. Research fixtures only; no production configuration or
cache contract changes. Existing Spark/PCBS behavior is released and unchanged.
These fixtures and generated overlays are unreleased. All project operations
use MCP docker_exec tools. Architect acceptance is pending.

The question is whether small cache records can reduce heap pressure while
restoring large GPU batches for downstream processing. The earlier
[PCBS-only split sweep](pcbs-split-budget-sweep.md) did not coalesce immediately
after the cache scan. Its fill action also populated partitions sequentially.
Those timings cannot be treated as controls for this parallel-fill comparison.

The initial matrix is stock CPU caching, PCBS capped at approximately 32 MiB,
and that same PCBS cap with standard GPU coalescing immediately after the
cache scan. The GPU cases use the research per-cache codec from the
[BlockManager codec spike](pcbs-blockmanager-codec-spike.md). They therefore
measure the combined codec/splitting approach, not Java-serialized PCBS.
The GPU target remains 1 GiB. No unsplit reference or further size sweep is
part of this first matrix.

The canonical input is generated once on CPU and shared as immutable Parquet
files. Sixteen DOUBLE columns contain deterministic salted xxhash64 values.
The full case has 805,306,368 rows: exactly 96 GiB of logical values, with
16 cache partitions and 6 GiB per partition. This is not an estimate of
compressed Parquet bytes or JVM/device allocation. Both cache formats report
actual encoded buffer lengths separately. CPU compression remains enabled
with the default 10,000-row batches. CPU runs load stock Spark alone.

Each file is read through a separate coalesce(1) child before unioning all
16 children. This preserves 16 cache partitions without a shuffle, despite
Parquet's normal file splitting. GPU CoalesceExec requests the existing batch
coalescer; the actual cached plan must confirm the 1 GiB target before caching.
CPU and GPU read exactly the same source files, checked by full SHA256 hashes,
footer row counts and schema. All 16 sums and the count are checked against
the CPU canonical aggregate oracle. Large-data aggregate agreement is not a
row-by-row comparison.

Each case uses a fresh JVM: local[8], configured heap 48 GiB, ParallelGC with
four GC threads, and MEMORY_AND_DISK. GPU concurrency is two, device pool is
fixed at 15 GiB, host off-heap limit is 40 GiB and pinned pool is 2 GiB. Memory
fraction, unroll check period and Java stream reset remain defaults. Spark's
own OFF_HEAP storage is not used. Configured heap and actual usable capacity
are recorded separately.

Fill consumes all cached RDD partitions in parallel, returning only summaries.
It includes reading the canonical files and building/persisting cache entries.
One warmup and three fresh all-column sum/count reads follow. Full file hashing
before each JVM warms the OS page cache; these are not cold-disk measurements.
Heap samples measure occupancy, not live payload. Raw GC logs provide collection
counts, time and observed post-full-GC occupancy. BlockManager memory/disk
residency and GPU decode/coalescer batch counts and bytes are also recorded.

Implementation reuses the existing PCBS split loop and
GpuCompressionAwareCoalesceIterator. Generated-only patches lower the private
PCBS GPU record budget and make the cache scan request standard coalescing in
one artifact. Observers preserve ownership using closeOnExcept. The per-cache
codec streams the existing Parquet byte array with a small header and reads
back the actual shim-loaded record class. Payloads still live on the heap;
this does not add a native or shadow cache store. The common benchmark compiles
against stock Spark jars only.

## Correctness gates

Stock CPU small input and an independent root rerun passed: 960,000 real rows,
16 partitions, 96 default cache records, four full-column scans and blocking
unpersist. Actual encoded buffers total 122,892,288 bytes. These are smoke
results, not performance results at the target pressure.

The separate 128 MiB GPU DISK_ONLY gate passed for split-only and split/coalesce.
Five real PCBS records were serialized and decoded with exact shim class,
row count, length and byte-for-byte payload equality. Their known-value input
compressed to 66,710,566 bytes. GPU decoding produced 134,217,728 device bytes;
coalescing produced one 134,217,728-byte batch instead of five small batches.
The plan places GpuCoalesceBatches(targetsize(1073741824)) directly between
GpuInMemoryTableScan and GpuHashAggregate. All 16 sums and the count matched
exact expected values. This checks correctness at 128 MiB, not near-1-GiB
performance or MEMORY_AND_DISK pressure.

Evidence root: `/data/pcbs-oom/pcbs-real-data-comparison/`.
Frozen plugin/codec artifacts and generated source hashes are in
`gpu-overlay-build2/manifest.json`; raw GPU gate results are in
`gpu-small-gate2/{only,combine}/`. The first prep attempt hit Python 3.6's
unsupported text keyword; the first GPU gate exposed a fixture classpath
error. Both failed attempts remain available. Their fixes change only research
preparation/launching, with no production change or weakened checks.

Full-data results and independent verification are pending. Expected heap OOM
will be reported as a negative outcome. Non-fixture correctness failures or
required production changes must be investigated and escalated before proceeding.
