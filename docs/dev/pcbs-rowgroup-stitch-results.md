# PCBS row-group prototype results

Status: READY FOR REVIEW for the bounded large-data proof, 2026-10-23.
Production readiness and architect acceptance remain pending.

Both modes passed the canonical 96 GiB workload with default Java reset 100 and
MemoryStore unroll 16: 16 partitions, 6 GiB per partition, eight task slots,
48 GiB configured heap (46 GiB Runtime.maxMemory), and two concurrent GPU tasks.
The GPU pool remained 15 GiB; RAPIDS host limit 40 GiB, pinned pool 2 GiB and memory fraction 0.6
were unchanged. Storage was MEMORY_AND_DISK.

The actual payload path uses memory buffers. The GPU writer encodes each large
input once; the splitter copies encoded row groups into small standalone Parquet
records; Spark stores/spills those records normally; the reader stages them into
RAPIDS spillable host buffers and decodes several in one cuDF call. Independent
Parquet framing does not require a filesystem file. Normal Spark/RAPIDS spilling
can still use disk. No production Spark/plugin source or default was changed.

Each mode wrote 112 large inputs into the same 12,400 records, 7.08–7.94 MiB
each. Every read decoded those records in 112 calls, with a largest output of
about 1 GiB. The exact 805,306,368 rows and all 16 sum oracles passed within the
existing floating-point tolerance. All 100 Spark
tasks in each JVM succeeded on their first attempt; no re-encoding occurred.
Codec disk I/O was exercised: 9,300 records / 77,344,541,220 payload bytes per read.

Java filled in 155.28 s and had a 36.79 s median measured read; codec filled in 136.23 s
and read in 21.34 s. Codec had higher observed peak heap: 42.35 GiB versus 39.63 GiB.
These are one fresh JVM per mode with three measured reads after warmup.
They establish a successful bounded proof and a promising timing difference;
they do not establish repeatability, required live heap, or the cause of the
timing/occupancy difference. No GC behavior was forced to match.

The nullable dictionary/numeric in-memory correctness fixture also passed NONE
and SNAPPY, including projection and distinct dictionary/data offsets. On 262,144
identical rows, changing writer defaults to 65,536 rows/8 MiB produced five groups
instead of one. Writer encoded size increased by 0.049% (NONE) and 0.096% (SNAPPY);
standalone-file footer framing adds 579/584 bytes respectively. This small fixture
does not predict the customer's string compression ratio.

Evidence is under /data/pcbs-oom/pcbs-rowgroup-stitch-spike/:
full-java001/, full-codec001/, root-rechecked-results.json,
independent-full-java-audit.json, independent-full-codec-audit.json,
independent-full-paired-layout-audit.json, root-native-gate003/,
root-writer-gate001/ and comparison001/. All operations used MCP docker_exec.

The stored-record shape changes Spark's memory windows: 16 roughly 8 MiB records
are about 128 MiB between MemoryStore checks, while the old seven-record partition
reached its completion check only after roughly 6 GiB. Java's 100-record reset
window likewise refers to roughly 800 MiB of these small records instead of
retaining the old whole partition. These are approximate payload windows, not
heap-size guarantees. The codec bypasses Java's object-handle retention, while
retaining MemoryStore's default checks. Combined decoding keeps the GPU batches
large. Profiling is still needed to explain the measured timing difference.

Root-consumer-cleanup001 also passes the actual generated consumer under stock
Spark TaskContext task completion: consume one row group with more remaining,
observe live writer buffers, complete the task, observe zero callback references,
and close again safely. The independent copied record survives source cleanup.

Open verification: repeated fresh-JVM performance runs, profiling to distinguish
CPU serialization/copying, GC and I/O, wider schemas and failure/concurrency
stress.
The prototype uses inferred pinned RMM/CCCL headers, with exact-runtime linkage
and small shared-RMM/ABI gates; this is not a general binary compatibility claim.

## Comparison with historical references

One row per method/settings cohort. Seconds, MiB and GiB are labeled below.

| Method / mode | JVMs / outcome | Storage | Compression | Reset / unroll | Producer / record target MiB | Actual records MiB | Fill s | Warmup s | Read median/JVM s | Peak fill / read heap GiB | Parquet / logical bytes |
|---|---|---|---|---|---|---|---|---|---|---|---|
| row-group / java | 1 / 1 PASS | MEMORY_AND_DISK | SNAPPY | 100 / 16 | 1024 / 8.0 | 7.08–7.94 | 155.28 | 38.82 | 36.79 | 36.14 / 39.63 | 1.000454 |
| row-group / codec | 1 / 1 PASS | MEMORY_AND_DISK | SNAPPY | 100 / 16 | 1024 / 8.0 | 7.08–7.94 | 136.23 | 42.83 | 21.34 | 41.10 / 42.35 | 1.000454 |
| historical / java | 3 / 3 HEAP_OOM | MEMORY_AND_DISK | SNAPPY | 100 / 16 | 1024 / 1024 | 769.64–895.94 | 148.42–166.63 | — | — | 42.28–44.52 / 45.24–45.24* | 1.000205 |
| historical / java | 3 / 3 PASS | MEMORY_AND_DISK | SNAPPY | 1 / 1 | 1024 / 1024 | 769.64–895.94 | 135.89–151.73 | 38.39–48.59 | 37.59–38.49 | 40.51–41.35 / 45.20–45.25 | 1.000205 |
| historical / codec | 3 / 3 PASS | MEMORY_AND_DISK | SNAPPY | 1 / 1 | 1024 / 1024 | 769.64–895.94 | 132.74–159.68 | 41.30–42.64 | 37.58–46.16 | 41.87–43.73 / 45.12–45.25 | 1.000205 |
| historical / codec | 3 / 3 HEAP_OOM | MEMORY_AND_DISK | SNAPPY | 100 / 16 | 1024 / 1024 | 769.64–895.94 | 145.19–166.96 | — | — | 42.80–44.55 / 45.09–45.24* | 1.000205 |

*OOM read occupancy is a partial observation. Failed read durations are retained in JSON and are not reported as successful warmups or measured reads.


- All included cases populated the same 96 GiB logical values in 16 partitions, with local[8], 48 GiB configured heap, ParallelGC four workers, 15 GiB GPU pool/concurrency two, host limit 40 GiB/pinned pool 2 GiB.
- The consumer is the same simple count plus sums over all 16 columns; this does not measure substantial downstream GPU work or a CPU-cache alternative.
- Java means ordinary Java object serialization of cached records; codec means the research custom cache-byte path. Spark BlockManager/MemoryStore remain stock in both.
- Historical runs have 1 GiB producer and cache-record targets. Row-group runs encode each large producer batch once, store smaller standalone row-group files, and combine files before GPU decoding.
- Historical private PCBS targets budget estimated GPU device bytes before Parquet encoding. New row-group targets are writer uncompressed-data byte and row targets. Neither is a hard encoded-size bound. Actual record ranges are encoded standalone Parquet file sizes, excluding Java stream framing. The combined GPU decode target remains 1 GiB.
- Compression ratio means Parquet payload bytes divided by logical value bytes; it includes file/footer overhead and is not the pure compression-codec ratio.
- Read timings exclude warmup. Grouped read ranges are the minimum/maximum of per-JVM three-read medians; individual observations remain in JSON.
- Peak heap is observed occupancy (including garbage), not live heap or minimum required memory. Read peaks include warmup; OOM peaks describe a failed, incomplete read and are not workload bounds.
- The table reports fresh JVM counts. The initial row-group pair has one JVM per mode; the prescribed historical reference set has three per setting. New timings are preliminary.
- Encoding layout, decode path and reset/unroll settings intentionally differ between some rows; those comparisons do not isolate a single cause. Only the two new modes share the same row-group artifacts.
- This reporter consumes existing verified results and bounded runtime config prefixes; it does not replace independent raw-result verification.
