# First Spark comparison: measured phases and computed combinations

Source: six `/data/tmp/cache-first-comparison/<format>-<producer>/result.json` files.
All cases use the original **strings-wide** fixture: 524,288 rows and two source
partitions; these are separate from the newer fixed-wide/task-size experiments.

Times below are milliseconds. Population is the single measured cache materialization.
**First** is the first read for that requested consumer mode and projection
(the recorded warmup, iteration -1). **Warmed** is the median of the two following
measured reads (iterations 0 and 1).

Neither first-read column is a globally cold/JVM-cold result. CPU consumers run
before GPU consumers on the same cache; narrow reads also follow full reads.
Default's requested GPU mode still uses a CPU cache reader; PCBS and Vortex use
direct GPU cache readers. GPU-enabled downstream aggregation remains part of the
read-only query timing. Vortex CPU output is row-based in this prototype.

The final two columns are **computed sums of measured phases**, not independently
measured end-to-end runs. They model one population plus the indicated read; prior
queries and initialization differ, so the sums must retain this qualification.

## Full read (all cached columns)

| Format / producer | Requested consumer (cache reader) | Population | First read | Warmed read median | Computed population + first | Computed population + warmed |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Default / CPU | CPU (CPU) | 775.594 | 257.674 | 185.129 | 1033.268 | 960.723 |
| Default / CPU | GPU (CPU) | 775.594 | 766.248 | 258.774 | 1541.842 | 1034.368 |
| Default / GPU | CPU (CPU) | 2245.839 | 196.732 | 146.216 | 2442.571 | 2392.055 |
| Default / GPU | GPU (CPU) | 2245.839 | 324.365 | 250.506 | 2570.204 | 2496.345 |
| PCBS / CPU | CPU (CPU) | 1157.194 | 266.009 | 224.079 | 1423.203 | 1381.273 |
| PCBS / CPU | GPU (GPU) | 1157.194 | 663.607 | 204.775 | 1820.801 | 1361.968 |
| PCBS / GPU | CPU (CPU) | 976.781 | 621.971 | 246.615 | 1598.752 | 1223.396 |
| PCBS / GPU | GPU (GPU) | 976.781 | 302.184 | 198.712 | 1278.964 | 1175.492 |
| Vortex / CPU | CPU (CPU) | 1221.914 | 387.805 | 248.621 | 1609.718 | 1470.535 |
| Vortex / CPU | GPU (GPU) | 1221.914 | 1362.942 | 815.468 | 2584.856 | 2037.382 |
| Vortex / GPU | CPU (CPU) | 1318.547 | 486.564 | 236.199 | 1805.110 | 1554.746 |
| Vortex / GPU | GPU (GPU) | 1318.547 | 896.763 | 871.027 | 2215.310 | 2189.574 |

## Narrow projection (ID only)

| Format / producer | Requested consumer (cache reader) | Population | First read | Warmed read median | Computed population + first | Computed population + warmed |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Default / CPU | CPU (CPU) | 775.594 | 127.687 | 80.392 | 903.281 | 855.986 |
| Default / CPU | GPU (CPU) | 775.594 | 178.628 | 134.645 | 954.222 | 910.239 |
| Default / GPU | CPU (CPU) | 2245.839 | 99.634 | 63.480 | 2345.473 | 2309.319 |
| Default / GPU | GPU (CPU) | 2245.839 | 159.639 | 111.325 | 2405.478 | 2357.164 |
| PCBS / CPU | CPU (CPU) | 1157.194 | 159.429 | 125.438 | 1316.622 | 1282.632 |
| PCBS / CPU | GPU (GPU) | 1157.194 | 133.736 | 127.193 | 1290.930 | 1284.387 |
| PCBS / GPU | CPU (CPU) | 976.781 | 178.262 | 137.217 | 1155.043 | 1113.997 |
| PCBS / GPU | GPU (GPU) | 976.781 | 125.424 | 133.467 | 1102.205 | 1110.248 |
| Vortex / CPU | CPU (CPU) | 1221.914 | 117.708 | 81.638 | 1339.622 | 1303.552 |
| Vortex / CPU | GPU (GPU) | 1221.914 | 144.403 | 126.606 | 1366.317 | 1348.520 |
| Vortex / GPU | CPU (CPU) | 1318.547 | 142.399 | 86.632 | 1460.946 | 1405.178 |
| Vortex / GPU | GPU (GPU) | 1318.547 | 148.126 | 117.781 | 1466.673 | 1436.327 |

Verification: independently parsed all six source JSON files, checked the shared
fixture/row/partition settings, all 72 recorded read entries, CPU-before-GPU order,
one first/warmup plus two measured samples for every consumer/projection cell,
and every reader backend against its recorded executed plan. Computed medians and additions from original
nanoseconds before rounding. No benchmarks, builds, implementation edits, or commits
were performed; all file access used MCP docker_exec.


## Fixed-wide: 256 MiB logical per task

This is a different workload from the original strings-wide comparison above:
32 fixed-width fields, 4,194,304 rows, four partitions of 1,048,576 rows each.
The canonical estimate is **256 MiB per task, 1 GiB total**, with two concurrent
tasks and two waves. Each job uses a 1 GiB JVM heap and a 4 GiB RAPIDS-managed host
limit; that managed limit does not cover Arrow/Rust memory. These are preliminary
results: one cold-cache population sample and two measured reuse samples after one
warmup, under the stated safety envelope.

Both CPU- and GPU-producer cohorts completed for PCBS and Vortex. **Default failed on the first
GPU-enabled full read** after population and six successful CPU read samples.
The separate Default CPU-producer case failed at the same phase. Both logs show
`OutOfMemoryError: Java heap space`, including
`DefaultCachedBatchSerializer.createAndDecompressColumn` /
`OnHeapColumnVector`. Neither failed case produced a GPU-enabled read timing or
a post-run payload audit; those cells are failures/unavailable, not zero-duration
results. GPU-enabled narrow reads were not reached. The cache decoder remains
CPU for Default; a GPU-enabled consumer does not imply GPU cache decoding.

All six producer/format cases at this task size are now recorded, including the
Default failures. Larger CPU-producer cases remain unmeasured here. No result in
this section establishes behavior at the larger requested task sizes.

### Storage and process footprint

Resident and disk bytes below are sums of the **same local block inventory**
captured after population. Logical residency counts each partition once; disk-only
logical data excludes partitions also present in memory. Payload bytes are actual
cached-array lengths from the optional post-run audit, which can change residency
and is separate from these population snapshots. Whole-job sampled RSS includes
startup, reference actions and any completed audits; failed jobs stopped earlier.
It is not a pure encoder/decoder working-memory measurement.

| Format / producer | Run status | Audited payload B | Resident storage B | Disk storage B | Logical resident | Logical disk-only | Sampled whole-job peak RSS B |
| --- | --- | ---: | ---: | ---: | --- | --- | ---: |
| Default / CPU | FAIL | Not obtained | 437,697,968 | 0 | 1024 MiB (4 partitions) | 0 MiB | 2,521,595,904 |
| Default / GPU | FAIL | Not obtained | 437,697,968 | 0 | 1024 MiB (4 partitions) | 0 MiB | 2,683,441,152 |
| PCBS / CPU | PASS | 735,262,340 | 183,817,312 | 382,166,705 | 256 MiB (1 partition) | 768 MiB | 2,611,281,920 |
| PCBS / GPU | PASS | 513,454,363 | 385,095,928 | 90,851,217 | 768 MiB (3 partitions) | 256 MiB | 2,537,283,584 |
| Vortex / CPU | PASS | 489,848,544 | 244,925,792 | 223,294,310 | 512 MiB (2 partitions) | 512 MiB | 2,668,290,048 |
| Vortex / GPU | PASS | 489,897,376 | 367,427,984 | 223,125,668 | 768 MiB (3 partitions) | 256 MiB | 2,756,464,640 |

Vortex/GPU's raw `getRDDStorageInfo` aggregate reported **244,961,864 resident B**,
while its local block statuses sum to **367,427,984 B** across three resident
partitions, representing **768 MiB logical data**. The table uses the latter,
consistent with its logical-residency fields. Original raw files are unchanged;
the lead-verified summary preserves the old counter separately. Vortex partition 2
has both memory and disk copies: its **223,125,668 disk B** span two partitions,
but only partition 3 contributes **256 MiB disk-only logical data**. Adding all
disk-represented logical data to resident logical data would double-count that copy.
PCBS/GPU likewise represents 768 MiB in memory and 256 MiB disk-only, with
385,095,928 resident B and 90,851,217 disk B. Default retained all four partitions
before its read failure. There is no audited Default payload size to substitute
for its storage footprint. The GPU-producer post-run audits returned 140 PCBS
records and 128 Vortex records across all four partitions, not just the resident
partitions.

For CPU producers, PCBS retained **256 MiB logical data** in 183,817,312 memory B
and 768 MiB disk-only logical data in 382,166,705 disk B. Vortex retained **512 MiB
logical data** in 244,925,792 memory B and 512 MiB disk-only in 223,294,310 disk B.
Thus the higher CPU-produced Vortex resident byte count also represents twice as
many resident logical rows in this observation. Both CPU-produced formats audited
136 records across all four partitions. Their after-population and after-read totals
are identical; all fourteen recorded population/read snapshots for each case agree
on these byte and logical totals. This does not make the sequential observations
atomic or establish a general retention-capacity limit. These resident subsets are
single fresh-job outcomes under fixed budgets and concurrent unrolling, not proven
maximum format capacities. Spark stores RDD partitions atomically: smaller
serializer records can bound working allocations without making fractions of an
oversized partition independently resident. The observed partition counts do not
establish capacity superiority for either format or producer mode.

### Population and read-only reuse

All times are milliseconds. First reads and warmed medians use the same definitions
as above: first-for-mode/projection is the recorded warmup; the median uses the two
following samples. CPU reads precede GPU reads on the same cache, so first GPU is
not globally cold. Full reads hash all thirty added fields and check null counts;
narrow reads use ID only. PCBS and Vortex GPU consumers use direct GPU cache scans.
Vortex's current CPU row-output route and separate GPU Arrow IPC units are prototype
implementation choices, not intrinsic format-performance ceilings.

| Format / producer | Population ms | Population outcome |
| --- | ---: | --- |
| Default / CPU | 21521.448 | Completed; later GPU-enabled full read failed |
| Default / GPU | 24564.578 | Completed; later GPU-enabled full read failed |
| PCBS / CPU | 14060.908 | Completed; all recorded reads passed |
| PCBS / GPU | 2689.563 | Completed; all recorded reads passed |
| Vortex / CPU | 7170.255 | Completed; all recorded reads passed |
| Vortex / GPU | 6946.398 | Completed; all recorded reads passed |

#### Full all-column read

| Format / producer | Consumer (cache reader) | First read ms | Warmed median ms | Computed population + first ms | Computed population + warmed ms |
| --- | --- | ---: | ---: | ---: | ---: |
| Default / CPU | CPU (CPU) | 1684.012 | 1421.167 | 23205.460 | 22942.615 |
| Default / CPU | GPU-enabled (CPU cache decoder) | FAIL: Java heap | Unavailable | Unavailable | Unavailable |
| Default / GPU | CPU (CPU) | 1740.541 | 1340.473 | 26305.120 | 25905.052 |
| Default / GPU | GPU-enabled (CPU cache decoder) | FAIL: Java heap | Unavailable | Unavailable | Unavailable |
| PCBS / CPU | CPU (CPU) | 5695.013 | 5644.279 | 19755.921 | 19705.187 |
| PCBS / CPU | GPU (GPU) | 1327.179 | 696.969 | 15388.086 | 14757.876 |
| PCBS / GPU | CPU (CPU) | 6045.854 | 5092.274 | 8735.416 | 7781.837 |
| PCBS / GPU | GPU (GPU) | 638.823 | 420.687 | 3328.386 | 3110.250 |
| Vortex / CPU | CPU (CPU) | 2783.802 | 2465.181 | 9954.056 | 9635.435 |
| Vortex / CPU | GPU (GPU) | 12116.471 | 10715.521 | 19286.726 | 17885.776 |
| Vortex / GPU | CPU (CPU) | 3094.962 | 2736.383 | 10041.360 | 9682.781 |
| Vortex / GPU | GPU (GPU) | 10539.385 | 10903.511 | 17485.783 | 17849.909 |

#### Narrow ID-only read

| Format / producer | Consumer (cache reader) | First read ms | Warmed median ms | Computed population + first ms | Computed population + warmed ms |
| --- | --- | ---: | ---: | ---: | ---: |
| Default / CPU | CPU (CPU) | 181.940 | 82.148 | 21703.388 | 21603.597 |
| Default / CPU | GPU-enabled (CPU cache decoder) | Not reached | Unavailable | Unavailable | Unavailable |
| Default / GPU | CPU (CPU) | 227.744 | 91.204 | 24792.322 | 24655.782 |
| Default / GPU | GPU-enabled (CPU cache decoder) | Not reached | Unavailable | Unavailable | Unavailable |
| PCBS / CPU | CPU (CPU) | 4390.764 | 4703.388 | 18451.672 | 18764.295 |
| PCBS / CPU | GPU (GPU) | 656.194 | 596.965 | 14717.102 | 14657.873 |
| PCBS / GPU | CPU (CPU) | 4001.435 | 4028.651 | 6690.998 | 6718.213 |
| PCBS / GPU | GPU (GPU) | 273.622 | 266.955 | 2963.185 | 2956.518 |
| Vortex / CPU | CPU (CPU) | 319.195 | 300.762 | 7489.450 | 7471.017 |
| Vortex / CPU | GPU (GPU) | 864.633 | 764.256 | 8034.887 | 7934.511 |
| Vortex / GPU | CPU (CPU) | 355.903 | 295.154 | 7302.301 | 7241.552 |
| Vortex / GPU | GPU (GPU) | 843.722 | 780.033 | 7790.120 | 7726.431 |

Combined columns are **computed additions of measured phases**, not separately
measured end-to-end jobs. Default's computed CPU combinations describe its completed
CPU reads only; they do not turn the failed overall run into a pass.

Evidence: all six `/data/tmp/cache-scale/256m/<format>-<producer>/` directories,
each containing unchanged `result.json`, `lead-verified-summary.json`, `memory.json`
and logs. Numerical verification independently recomputed medians, phase additions,
audited payload/record totals and block-based resident/disk/logical totals from raw
results, checked their lead summaries and actual reader plans, and inspected both
Default Java-heap failure logs. Source-only peer review of the subsequent
`storage_info` repair found no blocker: primary counters now use one sequential
local-block observation set, with the separate potentially lagging RDD report
retained as evidence. No build or runtime job was run for this report.
