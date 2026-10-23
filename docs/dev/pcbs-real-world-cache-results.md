# Real-world PCBS cache measurements

Status: IN PROGRESS. The first twenty-four full cases are READY FOR REVIEW; remaining
memory/storage cases and architect acceptance are pending. Follow the
[experiment plan](pcbs-real-world-cache-experiments.md). Existing behavior is
RELEASED; all new fixtures and formats remain UNRELEASED. All operations, including
worker audits, use MCP docker_exec. Do not launch another benchmark alongside the
active matrix supervisor.

## First disk-only references

Each row is one fresh JVM, local[8], 16 fixed canonical partitions, configured
48 GiB heap (recorded Runtime maximum 46 GiB), default reset 100 / unroll 16. GPU
cases keep the 1 GiB input/decode target, 15 GiB device pool, concurrency 2 and
40 GiB RAPIDS host limit. Timestamp inference selects Timestamp with session UTC
for every case and CPU oracle. No forced GC or data-volume changes.

GHArchive has 32,078,549 rows and 91.095 GiB of measured raw values; TLC has
239,470,448 rows and 31.108 GiB. Raw values mean nonnull scalar widths plus actual
UTF8 octets, excluding offsets, validity masks and padding. They are not footer
uncompressed-page sizes or JVM footprint estimates. CPU column encodings differ
from Parquet SNAPPY; Java serialization itself does not provide compression.

All eight cases PASS exact row counts, two salted all-column hash fingerprints,
null counts, value-byte totals, expected execution paths, successful first-attempt
Spark tasks, physical block accounting and blocking cleanup. Hashes are evidence,
not a mathematical proof of equality. Fill includes Spark's normal readback after
a successful DISK_ONLY put. Read is the median of three reads after warmup and
includes the all-column verification reduction.

| Dataset | Cache path | Disk GiB | Raw/disk factor | Fill s | Read s | Fill heap GiB | Read heap GiB |
|---|---|---:|---:|---:|---:|---:|---:|
| GHArchive | Existing PCBS + Java | 23.512 | 3.874 | 48.511 | 14.554 | 14.750 | 21.065 |
| GHArchive | Row groups + Java | 23.336 | 3.904 | 46.276 | 13.635 | 15.889 | 15.875 |
| GHArchive | Row groups + codec | 23.336 | 3.904 | 38.165 | 7.295 | 16.025 | 16.047 |
| GHArchive | Default CPU cache + Java | 87.110 | 1.046 | 92.672 | 36.288 | 44.714 | 41.948 |
| TLC | Existing PCBS + Java | 5.755 | 5.405 | 18.354 | 5.113 | 12.052 | 15.922 |
| TLC | Row groups + Java | 6.265 | 4.965 | 18.637 | 5.782 | 12.059 | 15.913 |
| TLC | Row groups + codec | 6.265 | 4.965 | 16.174 | 4.388 | 12.059 | 16.023 |
| TLC | Default CPU cache + Java | 24.272 | 1.282 | 116.068 | 38.776 | 25.550 | 26.326 |

Heap means observed occupancy, augmented with GC-log observations; it includes
collectible garbage and is not the live set or minimum required heap. Codec did
not reduce observed peak heap versus row-group Java at this heap size. No claim
about reduced-heap success follows yet. Timings have one JVM per method and a
fixed order, not repeated balanced fresh-JVM trials.

## What is and is not controlled

All three GPU modes have identical producer batch counts, rows, device-byte totals
and min/max sizes within each dataset. GHArchive produces 128 batches; TLC 52.
The row-group Java and codec arms also have identical actual decoded layouts:
GHArchive 96 batches per read, TLC 80. Parquet payload sizes differ by 548 bytes for
GHArchive and 14,868 bytes for TLC out of billions of bytes; byte-for-byte
writer determinism is not claimed.

Existing PCBS versus row groups changes decoded batching. GHArchive goes from 128
to 96 batches, with the largest row-group output about 1.10 GiB. TLC goes from 52
to 80, with the largest row-group output about 0.56 GiB. The 1 GiB decoded-size
estimate is a hint: it underestimates this string-heavy GHArchive output and is
conservative for TLC. These differences can affect performance and must not be
hidden behind a common configured target. Improving that estimate is future work;
this sweep does not retune it between cases.

Row-group storage produces 11,722 GHArchive records (maximum 3.90 MiB) and 5,346 TLC
records (maximum 1.49 MiB), versus 128 and 52 existing records respectively. It
preserves GHArchive compression here but increases TLC payload by 8.86%. Both
Parquet layouts remain much smaller than the default CPU format on these inputs.
This result is dataset-specific, not a universal compression guarantee.

Default CPU caching stops at 10,000 rows or a soft 4 MiB uncompressed column-stat
budget, checked before appending the next whole row; it chooses column compression
after batching. GHArchive reaches the byte budget and produces 23,527 records;
TLC reaches the row cap and produces 23,955. The final encoded record size need not
be 4 MiB. This is a format/compute reference, not an isolated serializer comparison.

## Evidence behind the codec comparison

The row-group Java and codec GPU layouts match, and their serialized sizes are
effectively equal. GHArchive measured-read GC is 11–53 ms for row-group Java and
8–27 ms for codec, too little to explain the roughly 6 s difference. Sampled read
intervals report zero physical read_bytes delta while returning 21.4–25.1 GB through
rchar; phase-boundary gaps remain unmeasured, so whole-phase zero I/O is not proved.

Local JDK 17 bytecode shows ObjectInputStream reading byte arrays through an extra
copy loop with at most 1,024-byte pieces. The research codec allocates its final
heap array and requests bulk reads with DataInputStream.readFully. These DISK_ONLY
reads use FileInputStream and BufferedInputStream, not Spark's 4 MiB serialized
buffer chunks. The extra looping/copying is a concrete hypothesis for lower CPU
cost; no profile has attributed the measured speedup to it. Both paths still
allocate payload arrays on the heap.

Evidence root: /data/pcbs-oom/pcbs-real-world-cache/. Full sparse table, separate
payload/file bytes, warmup and individual reads, exact source hashes and paths:
first8-comparison001.md/.csv/.json. Independent raw checks:
first8-independent-audit001.json, matrix001/independent-first8-audit.json and
matrix001-operational-audit/first8-summary.json. Mechanism notes:
java-codec-read-mechanism.md and cpu-default-batch-policy.md. Root inspected the
reporter and decisive Spark/JDK/codec code, reran its synthetic checks, and checked
the published table against case results; workers recomputed raw phases, GC peaks,
partition buffers, disk files, task events and content fingerprints independently.

## Memory-and-disk references at 48 GiB

Cases 008–015 also all PASS. All six GPU cache cases and the TLC default CPU
cache remain fully deserialized in RAM at every checkpoint. The GHArchive CPU
cache has five memory-resident and fourteen disk-backed blocks, with three
blocks in both; its 76.038 GiB of disk files are a subset, not a whole-cache
compression measurement.

| Dataset | Cache path | Fill s | Read s | Fill heap GiB | Read heap GiB | Residency |
|---|---|---:|---:|---:|---:|---|
| GHArchive | Existing PCBS + Java | 33.487 | 6.100 | 24.278 | 25.324 | All RAM |
| GHArchive | Row groups + Java | 32.799 | 5.623 | 23.717 | 27.378 | All RAM |
| GHArchive | Row groups + codec | 33.046 | 5.642 | 23.623 | 27.332 | All RAM |
| GHArchive | Default CPU cache + Java | 101.561 | 15.090 | 43.566 | 42.303 | Mixed |
| TLC | Existing PCBS + Java | 12.962 | 3.517 | 11.057 | 12.059 | All RAM |
| TLC | Row groups + Java | 14.367 | 3.864 | 12.060 | 11.255 | All RAM |
| TLC | Row groups + codec | 14.129 | 3.827 | 12.060 | 11.249 | All RAM |
| TLC | Default CPU cache + Java | 120.186 | 39.263 | 45.988 | 39.286 | All RAM |

Both codec cases perform zero codec reads and writes: these RAM hits bypass
wire serialization. The near-equal row-group Java/codec timings therefore do
not measure a serialization difference. The same actual producer and decoded
GPU fingerprints match their disk-only controls. Peak heap can rise because
the cache itself now remains on heap; it still does not establish a minimum
required heap.

MemoryStore accounting is an estimate, separate from measured payload bytes.
For example, the GHArchive row-group cache is charged about 22.032 GiB while
its arrays total 23.336 GiB. The cause of that difference is not established;
the experiment records both rather than treating the estimate as actual
residency footprint.

The full single table and raw checks now also exist as
first16-comparison001.md/.csv/.json and first16-independent-audit001.json.
The raw/content audit is matrix001/independent-next8-audit.json; operational
evidence is matrix001-operational-audit/second8-summary.json. Root checked
the table against case results; workers independently recomputed raw phases,
GC/sample peaks, buffer/file arrays, codec counters and task/content evidence.

## Disk-only at 16 GiB

Cases 016–023 all PASS with the same full datasets and controls. Configured heap
is 16 GiB, recorded Runtime maximum 15.333 GiB. Every checkpoint has all sixteen
cache blocks on disk and none in Spark MemoryStore. OS page-cache residency is
separate.

| Dataset | Cache path | Fill s | Read s | Fill heap GiB | Read heap GiB |
|---|---|---:|---:|---:|---:|
| GHArchive | Existing PCBS + Java | 49.415 | 16.232 | 13.652 | 13.185 |
| GHArchive | Row groups + Java | 45.375 | 13.452 | 5.176 | 5.644 |
| GHArchive | Row groups + codec | 36.474 | 7.455 | 5.365 | 5.507 |
| GHArchive | Default CPU cache + Java | 99.323 | 19.368 | 14.813 | 13.521 |
| TLC | Existing PCBS + Java | 17.546 | 5.112 | 5.023 | 7.457 |
| TLC | Row groups + Java | 17.422 | 5.856 | 5.374 | 5.403 |
| TLC | Row groups + codec | 15.093 | 4.407 | 5.342 | 5.372 |
| TLC | Default CPU cache + Java | 121.614 | 40.329 | 15.164 | 13.637 |

Row-group Java and codec retain their actual producer/decoded GPU layouts from
48 GiB and continue to agree with one another. Their GHArchive read times remain
near the larger-heap measurements while observed heap occupancy falls sharply.
This reinforces that occupancy at 48 GiB was not a minimum heap requirement.
Existing PCBS also passes these disk-only cases; no OOM threshold has been shown.
The default CPU GHArchive read is faster at the smaller heap, while TLC is slightly
slower; these single-JVM results do not establish a causal explanation.

The complete first24-comparison001.md/.csv/.json and
first24-independent-audit001.json preserve the combined table. Raw content/layout
checks are matrix001/independent-third8-audit.json and operational checks are
matrix001-operational-audit/third8-summary.json. Root independently reparsed raw
phase times, partition rows/buffer totals and completion markers for all 24 cases:
root-phase-recheck-024.json. All reported disk compression factors remain close
to the 48 GiB references; small writer-size variation is preserved in the table.

## Remaining measurements

The active 48-case schedule is matrix001/schedule.json, supervisor PID 365757;
status is matrix001/progress.json. All first 24 cases pass; cases 024–031
now test MEMORY_AND_DISK at 16 GiB, followed by 032–047 at 8 GiB.
Dataset rows, partitioning, targets and other controls stay fixed. Record actual
residency and whether the codec wire path is exercised; RAM hits can bypass it.
A partial disk subset has no whole-cache disk compression ratio. Genuine heap or
GC-limit OOM is a negative result; unexpected errors park the supervisor. CPU PCBS
adaptation/profiling and heavier downstream workloads remain deferred by the user.
