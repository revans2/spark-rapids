# Constrained-memory cache experiments: 1 GiB JVM heap

These are historical constrained-budget results, not the primary normal-host
comparison. The user selected a [16 GiB-heap baseline](vortex-cache-results.md).
In particular, a 4 GiB sampled RSS guard can interrupt legitimate host-memory use;
a guard stop is not a host OOM or proof of a format memory defect.

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


## Fixed-wide: 512 MiB logical per task, GPU producers

PCBS and Vortex both **PASS** at 8,388,608 rows: four partitions of 2,097,152
rows, **512 MiB canonical logical data per task and 2 GiB total**. This uses the
same fixed-wide fixture, two concurrent tasks/two waves, 1 GiB JVM heap and 4 GiB
RAPIDS-managed host limit as the preceding cohort. The host limit excludes Arrow
and Rust memory. All full/narrow CPU/GPU reads passed their expected aggregates
and actual reader-plan checks, without CPU expression bridges.

These remain preliminary single fresh-job population observations with two
measured reuse samples after one warmup per mode/projection. CPU producers above
256 MiB/task remain unmeasured. Larger Default GPU-enabled cases remain parked
after the recorded 256 MiB failures; this does not establish failure for larger
CPU-only Default execution.

### Storage and process footprint

| Format / producer | Audited payload B | Resident storage B | Disk storage B | Logical resident | Logical disk-only | Sampled whole-job peak RSS B |
| --- | ---: | ---: | ---: | --- | --- | ---: |
| PCBS / GPU | 1,026,901,869 | 256,727,704 | 545,052,644 | 512 MiB (1 partition) | 1536 MiB (3 partitions) | 2,849,349,632 |
| Vortex / GPU | 979,946,776 | 244,969,488 | 668,576,200 | 512 MiB (1 partition) | 1536 MiB (3 partitions) | 2,988,404,736 |

Both cases have identical after-population and after-read totals; all fourteen
recorded storage snapshots per case agree. PCBS retained partition 2 in memory;
Vortex retained partition 1. Each had the other three partitions only on disk,
with **no memory/disk overlap** in these observations. All four logical partitions
are represented once across those subsets. Primary physical bytes are sums of
the same sequential local-block inventory as the logical subsets, not a mixture
with separately reported RDD counters.

The post-run audits counted **280 PCBS records** and **256 Vortex records**, each
covering all 8,388,608 rows. Audited payload lengths are separate from BlockManager
storage bytes and disk serialization/compression. The optional audit follows the
storage/timing observations and can reread or recompute data and alter residency.

These resident subsets are outcomes of single jobs under the fixed budgets and
concurrent unrolling, not proven maximum capacities. Spark stores partitions
atomically; smaller serializer records bound working allocations without making
fractions of an oversized partition independently resident. Whole-job sampled RSS
includes startup, reference actions and audit; it may miss brief peaks and does
not isolate encoder/decoder or native working memory.

### Population and read-only reuse

All times are milliseconds. First means the recorded warmup for that
mode/projection; warmed means the median of the following two samples. CPU reads
precede GPU reads on the same cache, so first GPU is not globally cold. Full reads
consume all fields; narrow reads consume ID only. CPU and GPU consumers use the
corresponding actual cache readers. Vortex's CPU row-output route and separate GPU
Arrow IPC units describe the current prototype, not intrinsic format ceilings.

| Format / producer | Population ms |
| --- | ---: |
| PCBS / GPU | 4847.272 |
| Vortex / GPU | 13329.875 |

#### Full all-column read

| Format / producer | Consumer (cache reader) | First read ms | Warmed median ms | Computed population + first ms | Computed population + warmed ms |
| --- | --- | ---: | ---: | ---: | ---: |
| PCBS / GPU | CPU (CPU) | 11307.121 | 10642.250 | 16154.393 | 15489.522 |
| PCBS / GPU | GPU (GPU) | 963.589 | 748.136 | 5810.861 | 5595.409 |
| Vortex / GPU | CPU (CPU) | 6359.496 | 5829.208 | 19689.371 | 19159.083 |
| Vortex / GPU | GPU (GPU) | 22139.698 | 22329.740 | 35469.573 | 35659.616 |

#### Narrow ID-only read

| Format / producer | Consumer (cache reader) | First read ms | Warmed median ms | Computed population + first ms | Computed population + warmed ms |
| --- | --- | ---: | ---: | ---: | ---: |
| PCBS / GPU | CPU (CPU) | 8415.872 | 8246.604 | 13263.144 | 13093.876 |
| PCBS / GPU | GPU (GPU) | 509.853 | 523.121 | 5357.126 | 5370.393 |
| Vortex / GPU | CPU (CPU) | 636.384 | 679.993 | 13966.259 | 14009.868 |
| Vortex / GPU | GPU (GPU) | 1482.869 | 1569.608 | 14812.744 | 14899.483 |

Combined columns are computed additions of measured phases, not independently
measured end-to-end jobs. No CPU-producer or Default result at this task size is
implied by these two GPU-producer cases.

Evidence: unchanged raw results, lead summaries and memory records under
`/data/tmp/cache-scale/512m/{pcbs-gpu,vortex-gpu}/`. Report verification reproduced
all aggregate and actual-reader checks, fourteen storage snapshots per case,
audited row/record/payload totals, phase medians and sums, and sampled RSS. No build
or runtime job was run for this report.


## Separate Default CPU-only control: 256 MiB logical per task

A fresh **CPU-only** Default run **PASS** at the same 4,194,304-row fixed-wide
input, 256 MiB/task and 1 GiB total. The RAPIDS plugin was loaded, with GPU SQL
disabled for population and every read; this control did not attempt a GPU-enabled
consumer. It is separate from both earlier Default runs that failed on their first
GPU-enabled full read. Their unavailable payloads and failed read cells above
remain unchanged.

All six CPU full/narrow read samples passed their exact expected aggregates and
CPU reader-plan checks. The eight population/read storage observations agree:
**437,697,968 resident storage B** represent all four partitions/**1 GiB logical
data**, with **0 disk B**. The post-run audit counted **435,890,015 payload B**
and **420 records**, covering all rows. Sampled whole-job peak RSS was
**1,973,252,096 B**; it includes startup, reference actions and audit and is not
isolated decoder/native working memory.

Default's audited payload is smaller than both PCBS and Vortex payloads measured
at this same numeric fixture and task size. This is workload-specific evidence:
Vortex is not uniformly smaller across formats. Payload size and BlockManager
storage footprint remain distinct measurements.

Population took **21087.453 ms**. The read-only phases and computed
additions below use first-for-projection warmup and the following two-sample
median; the additions are not independently measured end-to-end jobs.

| CPU projection | First read ms | Warmed median ms | Computed population + first ms | Computed population + warmed ms |
| --- | ---: | ---: | ---: | ---: |
| Full all-column | 1607.114 | 1351.400 | 22694.567 | 22438.853 |
| Narrow ID-only | 164.875 | 102.831 | 21252.328 | 21190.284 |

Evidence: unchanged `result.json`, `lead-verified-summary.json` and `memory.json`
under `/data/tmp/cache-scale/256m/default-cpu-only/`. Independent report checks
reproduced aggregates, CPU plans, storage sums, audited totals, medians, phase sums
and RSS. This control establishes neither a GPU-enabled pass nor behavior at
larger CPU-only task sizes. No build or runtime job was run for this report.


## Fixed-wide: 1 GiB logical per task, original 8 MiB record target

This GPU-producer cohort uses 16,777,216 fixed-wide rows, four partitions,
**1 GiB canonical logical data per task and 4 GiB total**. Two concurrent tasks,
two waves, the 1 GiB JVM heap and the 4 GiB RAPIDS-managed host limit are unchanged.
The Vortex artifact is the original **8 MiB estimated record-target** build under
`/data/tmp/vortex-spark-prototype/`; all preceding measured Vortex cases also
used that build. This section does not describe the separate 1 MiB variant.

**PCBS PASS** includes all full/narrow CPU/GPU aggregates, actual reader checks
and audit. **Vortex FAIL** occurred on its **first CPU full read**, after population
completed in **27066.909 ms**. The log reports `OutOfMemoryError: Java heap space`
at `VortexIpcCodec.Sink.reserve` / `writeIpc` / `Decoder.next` / `cpuRows`.
No read sample completed and no payload audit ran. GPU and narrow reads were not
reached. This is a bounded JVM-heap failure, not evidence of a host OOM; the lower
failed-job RSS is not an efficiency advantage.

### Storage and process footprint

| Format / GPU producer | Run status | Audited payload B | Resident storage B | Disk storage B | Logical resident | Logical disk-only | Sampled whole-job peak RSS B |
| --- | --- | ---: | ---: | ---: | --- | --- | ---: |
| PCBS | PASS | 2,053,789,695 | 0 | 1,453,409,220 | 0 GiB | 4 GiB (4 partitions) | 3,429,646,336 |
| Vortex, 8 MiB target | FAIL | Not obtained | 0 | 1,783,189,437 | 0 GiB | 4 GiB (4 partitions) | 2,379,546,624 |

Both population snapshots are disk-only, without memory/disk overlap. PCBS's
fourteen population/read storage observations have unchanged totals; its post-run
audit counted 560 records and all rows. Vortex has only the pre-failure population
storage observation. Physical totals sum local blocks; payload and disk storage
are separate measurements. Disk-only BlockManager status does **not** establish
cold physical disk I/O or exclude filesystem/page-cache hits.

RSS is sampled over each whole process/job, including startup, reference actions
and any completed audits; it may miss brief peaks. The failed job stopped earlier.
Neither RSS nor the RAPIDS-managed host limit measures or bounds all Arrow/Rust
working memory. These observed disk-only outcomes do not prove maximum capacity.

### Population and read-only reuse

| Format / GPU producer | Population ms | Population outcome |
| --- | ---: | --- |
| PCBS | 7070.628 | Completed; all recorded reads passed |
| Vortex, 8 MiB target | 27066.909 | Completed; first CPU full read failed |

First-for-mode/projection is the recorded warmup; warmed is the median of the
following two samples. CPU precedes GPU on the same cache, so first GPU is not
globally cold. PCBS consumers use the matching actual CPU/GPU cache reader.

#### Full all-column read

| Format / GPU producer | Consumer (cache reader) | First read ms | Warmed median ms | Computed population + first ms | Computed population + warmed ms |
| --- | --- | ---: | ---: | ---: | ---: |
| PCBS | CPU (CPU) | 22027.611 | 21635.687 | 29098.239 | 28706.315 |
| PCBS | GPU (GPU) | 2005.040 | 1371.336 | 9075.668 | 8441.964 |
| Vortex, 8 MiB target | CPU (CPU) | FAIL: Java heap | Unavailable | Unavailable | Unavailable |
| Vortex, 8 MiB target | GPU | Not reached | Unavailable | Unavailable | Unavailable |

#### Narrow ID-only read

| Format / GPU producer | Consumer (cache reader) | First read ms | Warmed median ms | Computed population + first ms | Computed population + warmed ms |
| --- | --- | ---: | ---: | ---: | ---: |
| PCBS | CPU (CPU) | 17174.024 | 16920.899 | 24244.652 | 23991.527 |
| PCBS | GPU (GPU) | 927.788 | 910.511 | 7998.416 | 7981.139 |
| Vortex, 8 MiB target | CPU (CPU) | Not reached | Unavailable | Unavailable | Unavailable |
| Vortex, 8 MiB target | GPU | Not reached | Unavailable | Unavailable | Unavailable |

Combined columns are computed phase additions, not independently measured
end-to-end jobs. No failed or unreached read is represented as a zero timing.

### Failure evidence and bounded mitigation experiment

Source inspection identifies plausible concurrent retention mechanisms, not a
proved allocation census. Spark's Java serializer resets its object stream after
100 outer records; these plain cache-record case classes strongly own their
encoded arrays. Deserialization can therefore retain prior records through stream
handles. This representation also applies to PCBS; it does not by itself explain
the differing outcomes.

For deserialized memory-and-disk storage, Spark also tries to unroll disk records
back into memory. Failed attempts return a partially unrolled prefix whose vector
retains the prefix until it is exhausted. Immediately before the Vortex failure,
logs report estimated prefixes of **412.7 MiB** and **62.1 MiB** for the two active
partitions. These are estimates and may reference the same encoded arrays as stream
handles; they must not be added as independent/disjoint heap totals. IPC sink growth
also overlaps old/new arrays. The failing allocation site does not establish which
retention mechanism dominated or that a single record exceeded its hard cap.
The corrected CPU cursor clears exhausted reader/IPC references, and no growing
per-record listener/IPC list was found in this source review.

Decisive sources are Spark 3.5.5 `JavaSerializer.scala`,
`BlockManager.maybeCacheDiskValuesInMemory`, and
`MemoryStore.PartiallyUnrolledIterator`, plus this prototype's
`cpuRows`, `decodedIpc` and `VortexIpcCodec.Source`.
A separate **1 MiB estimated record-target** variant has been built under
`/data/tmp/vortex-spark-prototype-1m/` to reduce per-record temporary/retention
bursts. That trial was stopped by the constrained RSS guard, as detailed below.
It changes no heap/cap/production configuration and establishes no universal OOM
fix or normal-host memory failure. The original artifact tree and logs remain
unchanged.

Evidence: `/data/tmp/cache-scale/1024m/{pcbs-gpu,vortex-gpu}/` raw results,
summaries, memory records and logs. Report checks independently reproduced PCBS
aggregates, reader plans, storage, audit, timings and computed sums, and Vortex's
completed population/disk totals, absent reads/audit and recorded failure stack.
No build or runtime job was run for this report append.

## Isolated 1 MiB-target trial: constrained RSS guard stop

The GPU-producer/CPU-consumer 1 GiB/task trial used the separate application
`cdb79de3114e7cf1864e020b06831e90d21a4820f03229220cbe8363a5695ab4`.
It kept the 1 GiB heap, 4 GiB RAPIDS-managed cap and 4 GiB sampled RSS guard.
At 123.549 seconds, sampled RSS was **4,473,876,480 B**, exceeding the
**4,294,967,296 B** threshold; the launcher recorded RSS_GUARD and terminated the
owned process group. Lead and worker independently confirmed process cleanup.

No result.json survived the termination, so completed population/read timings,
read correctness, payload and precise active read iteration are **unverified**.
The sample and logs establish a reactive guard stop, not a host OOM, a native leak
or an allocation census. RSS includes heap and other process memory. The plugin
cap excludes Arrow/Rust, and the guard was restrictive relative to the combination
of allowed JVM and managed-host memory. No limits were raised to rerun this case.

Evidence: /data/tmp/cache-scale-1m/1024m/vortex-gpu-cpu-only/ command.json,
memory.json, memory-samples.jsonl and logs. The trial source is archived under
/data/tmp/vortex-spark-prototype-1m/SparkVortexSerializer.scala.
The source target was restored to the original 8 MiB baseline; no failed
mitigation was promoted to the primary implementation. The user then selected
16 GiB JVM heap for the independent normal-host comparison. The outcome here
does not predict that new baseline.
