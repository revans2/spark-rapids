# PCBS record splitting, read coalescing and stock CPU caching

Status: READY FOR REVIEW. Six full runs and independent raw-evidence audits
passed. Research fixtures only; no production configuration or
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
Primary outcomes are maximum heap usage and fill/read wall times. Matching CPU
GC behavior is not an experimental goal. Heap samples measure occupancy, not
live payload. Raw GC logs provide collection
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

## First full matrix

All three root-run fresh JVMs passed the full runtime verifier, exact row count
and canonical sums, retained all 16 cache blocks, and completed blocking
unpersist. No OOM, failed action or timeout occurred. The immutable canonical
manifest SHA256 is `e5484fe01a568ba9708479d6bf311fa0dd82173595a451097c9a8713a902858b`.
Actual source Parquet bytes are 103,135,540,973. Source generation took 88.02 s
and its CPU oracle took 23.47 s; neither is cache-fill time.

| Mode | Fill wall s | Three read walls s | Read median s | Sampled fill heap peak GiB | Sampled read heap peak GiB |
|---|---:|---|---:|---:|---:|
| CPU/default | 114.32 | 18.91, 18.64, 18.62 | 18.64 | 41.78 | 42.73 |
| GPU/PCBS codec, split only | 138.37 | 29.45, 26.84, 26.12 | 26.84 | 41.63 | 42.01 |
| GPU/PCBS codec, split and coalesce | 113.52 | 27.22, 26.96, 25.55 | 26.96 | 41.70 | 36.22 |

Heap is observed occupancy, including garbage. The 20 ms sampler can miss a
burst: for example, CPU fill's largest logged pre-GC occupancy was 41.91 GiB,
slightly above its 41.78 GiB sample. Logged pre-GC observations will accompany
final heap maxima; they are not a proof of an absolute allocation bound.

CPU produced 80,544 default records containing 103,089,524,736 encoded buffer
bytes, maximum 1,280,128 bytes per record. Both GPU modes produced exactly
3,184 records containing 103,105,342,960 encoded bytes, maximum 33,395,036
bytes. Each GPU read decoded 3,184 batches with maximum device size 33,908,864
bytes. Coalescing reduced this to 112 batches (seven per cache partition),
maximum 1,039,425,024 bytes (991.27 MiB), mean 877.71 MiB including tails.
The plan places the standard 1 GiB-target coalescer directly above the cache
scan and below the GPU aggregate.

Decoded GPU device buffers totaled 104,691,630,080 bytes (97.5 GiB), while
combined buffers totaled 103,079,215,104 bytes (96 GiB). Logical rows and values
are unchanged. A validity-buffer/allocation difference is a plausible cause;
this experiment has not independently attributed the difference to cuDF code.
These are measured device bytes, separate from logical values and Parquet bytes.

BlockManager residency remained stable through all four reads. CPU reported
25,658,721,280 memory bytes and 77,384,389,608 disk bytes; both GPU modes
reported 25,776,390,048 memory bytes and 77,329,035,876 disk bytes. These are
Spark's recorded sizes, not additional isolated live-heap measurements.

In this first matrix coalescing restored large GPU batches with essentially
unchanged median read time. Fill peaks dominated the combined case. Its lower
fill wall time cannot be attributed to read coalescing: both GPU artifacts have
the same fill path. The unchanged reverse-order repeat below checks this variation.

## Verified comparison across two fresh JVMs per case

Root ran CPU, split, then split/coalesce. The independent worker repeated the
same commands in reverse order, changing only output directories. All six
runs passed with the same canonical hashes, exact rows and aggregate oracle;
there were no failed tasks, OOMs or timeouts. Global GPU target remained 1 GiB,
with private PCBS pre-splitting only. No control was tuned to obtain a pass or
match GC behavior. Full audits ran after timing ended, avoiding competing I/O.

The following heap maxima take the larger of the 20 ms samples and logged
pre-GC occupancy during each timed phase. Read peaks include warmup; read wall
medians exclude warmup. These are observed peaks, not guaranteed upper bounds
or isolated payload/live-set measurements. Two JVMs give observed ranges, not
a statistical confidence interval.

| Mode | Fill wall range s | Read median range s | Largest observed fill heap GiB | Largest observed read heap GiB | Largest overall observed heap GiB |
|---|---:|---:|---:|---:|---:|
| CPU/default | 99.30–114.32 | 18.64–19.20 | 42.19 | 42.73 | 42.73 |
| GPU/PCBS codec, split only | 110.20–138.37 | 26.84–28.20 | 41.63 | 42.01 | 42.01 |
| GPU/PCBS codec, split/coalesce | 113.52–138.55 | 24.96–26.96 | 41.74 | 42.10 | 42.10 |

The reverse-order individual observations are:

| Mode | Fill wall s | Three measured read walls s | Observed fill/read heap peaks GiB |
|---|---:|---|---|
| GPU split/coalesce | 138.55 | 26.26, 24.96, 24.38 | 39.92 / 42.10 |
| GPU split | 110.20 | 28.20, 29.00, 27.95 | 41.47 / 36.84 |
| CPU/default | 99.30 | 19.28, 19.20, 19.16 | 42.19 / 41.74 |

The lower read-phase peak switched between GPU modes across the two orders.
It is not reproducible evidence that coalescing reduces heap. Overall maximum
observed heap stayed near 42–43 GiB in all modes. Fill rankings also reversed,
so the fill difference is run variation, not a coalescer benefit. Coalescing
consistently restored 112 large GPU batches per read instead of 3,184 small
ones, with the same measured 991.27 MiB maximum. Read times were similar or
modestly lower with coalescing in these observations. CPU/default was faster
for this all-column sum/count query. These measurements do not isolate the
codec's benefit from the PCBS format and splitting changes, nor predict a
customer query with different downstream processing or establish a safe cap
for smaller heaps and higher parallelism.

All repeats matched their corresponding first-run encoded bytes, record
counts, measured GPU batch sizes and stable BlockManager residency. CPU kept
normal 10,000-row batches (maximum encoded record 1.22 MiB); it had no RAPIDS or
research-overlay jars and default Java serialization. Its input bytes and
partition layout were identical to the GPU cases.

Root independently audited all six cases against raw markers, source/build
manifests, runtime artifact hashes, actual stock/overlay class origins, full
canonical hashes, GC logs, Spark event logs, complete cache retention and
successful unpersist. Every audit passed. GPU resources were released and no
Spark JVM remained after timing. Evidence under the shared root:

- `root-full-audits.json`: six independent raw audits with hashes.
- `independently-verified-comparison.json`: phase maxima and paired ranges.
- `root-matrix-process.json`, `worker-reverse-matrix-process.json`: serial
  process outcomes and exact command files.
- `{root,worker}-{cpu96,gpu32split96,gpu32combine96}/`: raw logs, event logs,
  GC logs, memory samples, launch controls, process and verifier results.
- `summarize_full_audits.py`: reviewed derivation of the comparison JSON.

Research is READY FOR REVIEW. Architect acceptance and any production design
or implementation remain separate.
