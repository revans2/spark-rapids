# Real-world PCBS cache measurements

Status: IN PROGRESS. The first eight full cases are READY FOR REVIEW; remaining
memory/storage cases and architect acceptance are pending. Follow the
[experiment plan](pcbs-real-world-cache-experiments.md). Existing behavior is
RELEASED; all new fixtures and formats remain UNRELEASED. All operations, including
worker audits, use MCP docker_exec. Do not launch another benchmark alongside the
active matrix supervisor.

## First disk-only references

Each row is one fresh JVM, local[8], 16 fixed canonical partitions, configured
48 GiB heap (recorded Runtime maximum 46 GiB), default reset100/unroll16. GPU
cases keep the 1 GiB input/decode target, 15 GiB device pool, concurrency2 and
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
and min/max sizes within each dataset. GHArchive produces128 batches; TLC52.
The row-group Java and codec arms also have identical actual decoded layouts:
GHArchive96 batches per read, TLC80. Parquet payload sizes differ by548 bytes for
GHArchive and14,868 bytes for TLC out of tens/billions of bytes; byte-for-byte
writer determinism is not claimed.

Existing PCBS versus row groups changes decoded batching. GHArchive goes from128
to96 batches, with the largest row-group output about1.10 GiB. TLC goes from52
to80, with the largest row-group output about0.56 GiB. The 1 GiB decoded-size
estimate is a hint: it underestimates this string-heavy GHArchive output and is
conservative for TLC. These differences can affect performance and must not be
hidden behind a common configured target. Improving that estimate is future work;
this sweep does not retune it between cases.

Row-group storage produces11,722 GHArchive records (maximum3.90 MiB) and5,346 TLC
records (maximum1.49 MiB), versus128 and52 existing records respectively. It
preserves GHArchive compression here but increases TLC payload by8.86%. Both
Parquet layouts remain much smaller than the default CPU format on these inputs.
This result is dataset-specific, not a universal compression guarantee.

Default CPU caching stops at10,000 rows or a soft4 MiB uncompressed column-stat
budget, checked before appending the next whole row; it chooses column compression
after batching. GHArchive reaches the byte budget and produces23,527 records;
TLC reaches the row cap and produces23,955. The final encoded record size need not
be4 MiB. This is a format/compute reference, not an isolated serializer comparison.

## Evidence behind the codec comparison

The row-group Java and codec GPU layouts match, and their serialized sizes are
effectively equal. GHArchive measured-read GC is11–53 ms for row-group Java and
8–27 ms for codec, too little to explain the roughly6 s difference. Sampled read
intervals report zero physical read_bytes delta while returning21.4–25.1 GB through
rchar; phase-boundary gaps remain unmeasured, so whole-phase zero I/O is not proved.

Local JDK17 bytecode shows ObjectInputStream reading byte arrays through an extra
copy loop with at most1,024-byte pieces. The research codec allocates its final
heap array and requests bulk reads with DataInputStream.readFully. These DISK_ONLY
reads use FileInputStream and BufferedInputStream, not Spark's4 MiB serialized
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

## Remaining measurements

The active 48-case schedule is matrix001/schedule.json, supervisor PID365757;
status is matrix001/progress.json. Cases008–015 test MEMORY_AND_DISK at48 GiB,
then cases016–031 repeat both storage levels at16 GiB and032–047 at8 GiB.
Dataset rows, partitioning, targets and other controls stay fixed. Record actual
residency and whether the codec wire path is exercised; RAM hits can bypass it.
A partial disk subset has no whole-cache disk compression ratio. Genuine heap or
GC-limit OOM is a negative result; unexpected errors park the supervisor. CPU PCBS
adaptation/profiling and heavier downstream workloads remain deferred by the user.
