# Spark cache comparison: 16 GiB JVM heap

Status: READY FOR REVIEW. All 24 primary cases passed: 3 formats × 4 task sizes ×
2 population modes (GPU-enabled and CPU-only). GPU-input jobs measured CPU and GPU queries;
CPU-only jobs kept GPU SQL disabled throughout. The user selected a 16 GiB JVM heap.
Earlier 1 GiB-heap runs are [constrained-memory experiments](vortex-cache-constrained-results.md),
not the normal-host baseline. No production serializer changes are proposed.

[Follow-up profiling](vortex-cache-profiling.md) reproduced the slow Vortex GPU
reads and found substantial pinned-host allocation/free and host copy API costs
inside Arrow import. GPU copy execution was short. Treat these GPU read numbers
as measurements of the current prototype bridge, not isolated Vortex decoding or
PCIe transfer throughput. [Bounded bridge controls](vortex-cache-bridge-results.md)
now demonstrate a substantial improvement with a shared pinned pool, plus a
numeric direct-upload alternative. The completed [2 GiB-pool matrix](vortex-cache-pinned2g-results.md)
and [bounded copy improvement](vortex-cache-copy-results.md) are separate follow-ups
and do not overwrite these original numbers.

Each primary run uses two local task slots, four source/cache partitions, default
DataFrame memory-and-disk storage, a 4 GiB RAPIDS-managed host-memory cap and a
32 GiB sampled process-tree RSS early-stop guard. Arrow/Rust allocations are outside
the plugin cap. The guard is reactive and can miss brief peaks; it is not an OS limit.
The sandbox has about 188 GiB host RAM; roughly 175 GiB was available before this cohort.
No machine-exhaustion testing is authorized.

The fixed-wide fixture has 32 numeric fields with nulls, including monotonic,
low-cardinality and hash-derived values. Its standardized logical-layout estimate
is 256 bytes per row. Requested task sizes are 256 MiB, 512 MiB, 1 GiB and 2 GiB,
with four partitions supplying enough work for two waves across two slots.
Logical input size, encoded payload, local BlockManager memory/disk bytes and
represented logical data are separate measurements. For deserialized cache blocks,
resident storage bytes use Spark's SizeEstimator accounting, not an exact heap census;
they can be lower than the audited payload lengths. Disk bytes are separate again.

Vortex uses the original 8 MiB estimated outer record target and its default
adaptive encoding selection. The Java/JNI writer does not expose Rust's alternate
encoding strategies; no forced-encoding comparison has been performed. The isolated
1 MiB-record trial hit its 4 GiB RSS guard under the earlier constrained envelope;
that stop does not establish a memory problem on this host.

Population includes generating the deterministic projected input, encoding,
transfers and cache materialization; it is not isolated codec throughput.
Population, first-for-mode/projection reads and warmed read medians are reported
separately. Warmed medians use two samples after one warmup. CPU reads precede GPU
reads on the same stored data; first GPU is not a globally cold read. Full reads
consume all fields; narrow reads select ID. Population-plus-read columns are
explicitly computed sums, not independently measured end-to-end jobs.

Producer/consumer mode names refer to Spark execution, not necessarily codec hardware.
Default encodes and decodes its cache on CPU, including for GPU-enabled queries.
PCBS's GPU path uses cuDF Parquet encoding/decoding on GPU. Vortex encoding and
decoding remain on CPU; its GPU path transfers through host Arrow and returns
GPU batches for downstream GPU work. A GPU cache-reader label identifies the
Spark scan node/output, not a claim that Vortex decompression runs on GPU.
This prototype opens a cuDF IPC reader for each decoded IPC batch; that bridge's
cost has not been isolated from Vortex decoding. CPU row-output and GPU IPC paths
are implementation choices, not intrinsic format speed limits.
Whole-job RSS includes startup, reference actions and the optional post-run audit;
it does not isolate temporary encoding or decoding memory. Observed residency is
not a proven maximum capacity, and OS file caching can affect disk-backed reads.

## Results at a glance

All formats retained the full logical input at every primary size, including
**8 GiB total at 2 GiB/task**, with no disk-backed cache blocks at the recorded
storage snapshots. This does not exclude transient managed/GPU spill activity.
Each population number is one fresh-cache observation; read-only figures are two-sample warmed medians.
These are preliminary measurements of the current paths, not optimized format limits.

The tables below summarize the largest case, in **seconds**. Combined totals
are **computed population + warmed read**, not independently measured end-to-end jobs.
First-read and narrow-projection results remain in the detailed tables.

### CPU-only SQL execution

| Format | Population s | Read-only s | Computed population + read s |
| --- | ---: | ---: | ---: |
| Default | 151.765 | 10.830 | 162.595 |
| PCBS | 88.446 | 41.660 | 130.106 |
| Vortex | 43.818 | 24.100 | 67.917 |

### GPU-enabled input and GPU-enabled query

| Format | Population s | Read-only s | Computed population + read s |
| --- | ---: | ---: | ---: |
| Default | 321.273 | 7.646 | 328.919 |
| PCBS | 9.739 | 1.578 | 11.317 |
| Vortex | 41.934 | 178.681 | 220.615 |

GPU-enabled describes Spark execution: Vortex still encodes/decodes on CPU
through host Arrow, Default uses CPU cache coding, and PCBS uses cuDF GPU
Parquet coding. PCBS CPU uses UNCOMPRESSED while its GPU path uses Snappy;
the CPU/GPU comparison is not compression-matched.

### Encoded payload at the largest size

| Format | CPU-produced payload GiB | GPU-produced payload GiB |
| --- | ---: | ---: |
| Default | 3.248 | 3.248 |
| PCBS | 5.478 | 3.826 |
| Vortex | 3.649 | 3.651 |

Every row above represents the same 8 GiB logical data. Default is smallest on
this numeric fixture; earlier string results have a different ranking.
Peak RSS is not the retained-cache size or a live native-memory census.

PCBS has the strongest measured GPU path. Vortex is promising for CPU-only
population and reuse against the current PCBS CPU path; Default reads fastest
on CPU once populated. Reuse count matters: a simple timing model is
population + N x warmed read, with startup/first-read and memory caveats.
The current Vortex GPU IPC route needs profiling before a format-level decision.

[Machine-readable measurements](vortex-cache-results.csv) contain 72 rows, one per
producer/consumer/projection combination. Population, payload and storage fields
repeat for the same cache; do not sum those repeated fields across queries.
Times in the CSV are original nanoseconds; combined columns are explicitly computed.
The 24 underlying result JSON files and artifact/source manifests remain under
`/data/tmp/cache-main-16g/<task-size>m/<format>-<gpu|cpu-only>/`.

## Encoding policies compared

These runs compare the current default implementations, not compression-matched
codec configurations:

| Path | Policy used |
| --- | --- |
| Default, either input mode | Spark default column-cache compression |
| PCBS CPU writer | Explicit Parquet UNCOMPRESSED codec; Parquet value encodings/dictionaries still apply |
| PCBS GPU writer | cuDF Parquet SNAPPY when cache compression is enabled, as in these runs |
| Vortex, either input mode | Default adaptive Vortex encoding selection on CPU |

The [PCBS writer source](/home/roberte/src/rapids-plugin-4-spark/sql-plugin/src/main/scala/com/nvidia/spark/rapids/parquet/ParquetCachedBatchSerializer.scala:426)
selects SNAPPY/NONE in getParquetWriterOptions, while its CPU getRecordWriter
explicitly requests UNCOMPRESSED. The CPU configuration starts with
new Configuration(false); normal Hadoop codec overrides are not inherited.
The uncompressed CPU path also exists on main; this experiment did not introduce
or change it. No compression-matched CPU Parquet control has been run.

CPU and GPU writers also delegate encoding decisions to different implementations.
No actual per-column footer/page audit has been done, so the measured size gap
cannot be assigned specifically to Snappy or dictionary choices. Vortex's alternate
Rust strategies are not exposed by the Java/JNI API and remain untested. These
limitations matter before treating observed storage differences as format-intrinsic.

## Fixed-wide: 256 MiB logical per task, GPU producers

All three formats **PASS** at 4,194,304 rows, four partitions of 1,048,576 rows,
**256 MiB logical per task and 1 GiB total**. GPU production and CPU/GPU
consumption use the same stored data within each fresh job. Every full/narrow
read passed the expected aggregates and actual cache-reader plan checks.
These are preliminary results from one population sample and two measured reads
after one warmup for each mode/projection.

CPU-only producer results are reported separately below. These GPU-produced
measurements do not cover GPU consumption of CPU-produced caches.
The constrained-budget failures remain separate historical evidence; Default's
earlier GPU-enabled read failure did not recur in this primary case.

### Storage and process footprint

| Format / GPU producer | Audited payload B | Records | Resident storage B | Logical resident | Disk storage B | Sampled whole-job peak RSS B |
| --- | ---: | ---: | ---: | --- | ---: | ---: |
| Default | 435,890,015 | 420 | 437,697,968 | 1 GiB (4 partitions) | 0 | 5,122,056,192 |
| PCBS | 513,454,990 | 140 | 513,461,696 | 1 GiB (4 partitions) | 0 | 3,980,845,056 |
| Vortex | 489,897,376 | 128 | 489,903,072 | 1 GiB (4 partitions) | 0 | 5,558,865,920 |

All fourteen population/read storage observations per format have unchanged totals:
all rows are memory-resident, with no disk-only logical data or memory/disk overlap.
Primary bytes and logical subsets come from the same sequential local-block
inventory. The optional post-run audit counted every row; its actual payload
lengths are distinct from BlockManager footprint and serializer size statistics.

On this numeric fixture, Default has the smallest audited payload, followed by
Vortex and PCBS. This is a measured workload result, not a general compression
ranking. Whole-job sampled RSS includes startup, reference actions and audit;
it may miss brief peaks and does not isolate temporary encoding/decoding or
Arrow/Rust working memory. Residency here establishes that this input fit, not
a maximum retention capacity.

### Population

| Format / GPU producer | Population ms |
| --- | ---: |
| Default | 23012.288 |
| PCBS | 2502.820 |
| Vortex | 6918.857 |

### Read-only reuse and computed phase combinations

First is the recorded warmup for that consumer mode and projection; warmed is
the median of the following two samples. CPU consumers precede GPU consumers,
and narrow reads follow full reads, so first GPU is not globally cold.
Default's GPU-enabled timing includes its CPU cache decoder and subsequent GPU
work. PCBS and Vortex GPU consumers use direct GPU cache scans. Full queries
consume/hash all fields; narrow queries use ID only.

#### Full all-column read

| Format / GPU producer | Consumer (cache reader) | First read ms | Warmed median ms | Computed population + first ms | Computed population + warmed ms |
| --- | --- | ---: | ---: | ---: | ---: |
| Default | CPU (CPU) | 1681.215 | 1524.406 | 24693.503 | 24536.694 |
| Default | GPU-enabled (CPU cache decoder) | 1349.526 | 1170.159 | 24361.814 | 24182.447 |
| PCBS | CPU (CPU) | 6177.436 | 5576.536 | 8680.256 | 8079.355 |
| PCBS | GPU (GPU) | 653.951 | 454.920 | 3156.771 | 2957.740 |
| Vortex | CPU (CPU) | 3378.307 | 2675.958 | 10297.164 | 9594.814 |
| Vortex | GPU (GPU) | 13426.724 | 13852.579 | 20345.580 | 20771.436 |

#### Narrow ID-only read

| Format / GPU producer | Consumer (cache reader) | First read ms | Warmed median ms | Computed population + first ms | Computed population + warmed ms |
| --- | --- | ---: | ---: | ---: | ---: |
| Default | CPU (CPU) | 181.171 | 80.459 | 23193.459 | 23092.747 |
| Default | GPU-enabled (CPU cache decoder) | 300.994 | 216.442 | 23313.282 | 23228.730 |
| PCBS | CPU (CPU) | 4433.774 | 4334.172 | 6936.593 | 6836.992 |
| PCBS | GPU (GPU) | 291.815 | 248.663 | 2794.634 | 2751.482 |
| Vortex | CPU (CPU) | 303.042 | 321.267 | 7221.899 | 7240.123 |
| Vortex | GPU (GPU) | 1010.138 | 892.792 | 7928.995 | 7811.649 |

Combined columns are additions of measured phases, not independently measured
end-to-end runs. The different CPU output routes and GPU Arrow IPC bridge are part
of these prototype timings; these results do not establish intrinsic format limits.
All Vortex samples use the original 8 MiB outer target and default adaptive
encoding selection, without forced alternate encodings.

Evidence: `/data/tmp/cache-main-16g/256m/{default-gpu,pcbs-gpu,vortex-gpu}/`
unchanged raw results, lead summaries, command manifests and memory records.
Independent report verification reproduced the explicit 16 GiB heap / 32 GiB
sampled guard / 4 GiB managed cap, exact aggregates and actual readers, fourteen
local-block snapshots per format, audit totals, medians, computed sums and RSS.
No build or runtime job was run for this report.

## Fixed-wide: 512 MiB logical per task, GPU producers

All three GPU-producer cases **PASS** with **512 MiB logical per task**
and **2048 MiB total**, four partitions and two task slots/two waves.
The primary settings remain a 16 GiB heap, 4 GiB RAPIDS-managed host cap and
32 GiB reactive RSS guard, using the original 8 MiB Vortex outer target.
Every full/narrow CPU/GPU read passed expected aggregates and actual-reader checks.
CPU-only producer results are reported separately below. Population is one fresh-job sample;
warmed read medians use two samples after one warmup, so results are preliminary.

### Storage and process footprint

| Format / GPU producer | Audited payload B | Records | Resident storage B | Logical resident MiB | Disk storage B | Sampled whole-job peak RSS B |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Default | 871,780,099 | 840 | 875,395,032 | 2048 | 0 | 7,943,970,816 |
| PCBS | 1,026,907,683 | 280 | 1,026,921,048 | 2048 | 0 | 3,675,885,568 |
| Vortex | 979,946,776 | 256 | 979,958,104 | 2048 | 0 | 7,785,480,192 |

All fourteen storage observations per format have unchanged totals: every row
is resident, with no disk-only data or overlap. Storage-accounted bytes and logical
subsets use the same local block inventory. The post-run audit covers all rows;
its payload lengths are separate from BlockManager accounting. RSS includes
startup, reference actions and audit, may miss peaks and is not live-native or
temporary-memory accounting. This observed fit does not prove maximum capacity.

### Population

| Format / GPU producer | Population ms |
| --- | ---: |
| Default | 42646.828 |
| PCBS | 3381.433 |
| Vortex | 12008.372 |

### Read-only reuse and computed phase combinations

First means the recorded warmup for the mode/projection; warmed is the median
of the two following samples. CPU precedes GPU on the same cache, so first GPU
is not globally cold. Default uses a CPU cache decoder before GPU conversion
and aggregation; PCBS/Vortex use GPU cache scans for GPU-enabled queries.

#### Full all-column read

| Format / GPU producer | Consumer (cache reader) | First read ms | Warmed median ms | Computed population + first ms | Computed population + warmed ms |
| --- | --- | ---: | ---: | ---: | ---: |
| Default | CPU (CPU) | 3014.337 | 2779.819 | 45661.165 | 45426.647 |
| Default | GPU-enabled (CPU cache decoder) | 2455.764 | 2006.628 | 45102.593 | 44653.456 |
| PCBS | CPU (CPU) | 11065.666 | 10525.747 | 14447.099 | 13907.179 |
| PCBS | GPU (GPU) | 890.735 | 523.472 | 4272.168 | 3904.905 |
| Vortex | CPU (CPU) | 6249.544 | 5744.754 | 18257.916 | 17753.126 |
| Vortex | GPU (GPU) | 30229.741 | 29643.115 | 42238.113 | 41651.488 |

#### Narrow ID-only read

| Format / GPU producer | Consumer (cache reader) | First read ms | Warmed median ms | Computed population + first ms | Computed population + warmed ms |
| --- | --- | ---: | ---: | ---: | ---: |
| Default | CPU (CPU) | 229.196 | 126.646 | 42876.024 | 42773.474 |
| Default | GPU-enabled (CPU cache decoder) | 287.666 | 216.692 | 42934.494 | 42863.520 |
| PCBS | CPU (CPU) | 8029.005 | 7964.310 | 11410.437 | 11345.743 |
| PCBS | GPU (GPU) | 287.694 | 256.238 | 3669.127 | 3637.671 |
| Vortex | CPU (CPU) | 540.606 | 733.012 | 12548.978 | 12741.384 |
| Vortex | GPU (GPU) | 1792.264 | 1678.262 | 13800.636 | 13686.634 |

Combined columns are computed additions of measured phases, not independently
measured end-to-end jobs. Full queries consume all fields; narrow queries select ID.
Vortex uses default adaptive encoding choices only. The CPU row-output and GPU
IPC routes are prototype implementation choices, not intrinsic format limits.

Evidence: /data/tmp/cache-main-16g/512m/{default-gpu,pcbs-gpu,vortex-gpu}/
raw results, command/provenance snapshots, lead summaries, memory records and logs.
The lead independently verified budgets, all14 storage snapshots, aggregate and
reader checks, audit counts and sampled RSS before report generation. Numerical
phase tables are reproduced from original nanoseconds and independently checked.

## Fixed-wide: 1 GiB logical per task, GPU producers

All three GPU-producer cases **PASS** with **1024 MiB logical per task**
and **4096 MiB total**, four partitions and two task slots/two waves.
The primary settings remain a 16 GiB heap, 4 GiB RAPIDS-managed host cap and
32 GiB reactive RSS guard, using the original 8 MiB Vortex outer target.
Every full/narrow CPU/GPU read passed expected aggregates and actual-reader checks.
CPU-only producer results are reported separately below. Population is one fresh-job sample;
warmed read medians use two samples after one warmup, so results are preliminary.

### Storage and process footprint

| Format / GPU producer | Audited payload B | Records | Resident storage B | Logical resident MiB | Disk storage B | Sampled whole-job peak RSS B |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Default | 1,743,560,044 | 1680 | 1,662,574,592 | 4096 | 0 | 15,233,388,544 |
| PCBS | 2,053,799,024 | 560 | 2,053,825,704 | 4096 | 0 | 10,749,976,576 |
| Vortex | 1,959,914,440 | 512 | 1,959,937,032 | 4096 | 0 | 9,508,372,480 |

All fourteen storage observations per format have unchanged totals: every row
is resident, with no disk-only data or overlap. Storage-accounted bytes and logical
subsets use the same local block inventory. The post-run audit covers all rows;
its payload lengths are separate from BlockManager accounting. RSS includes
startup, reference actions and audit, may miss peaks and is not live-native or
temporary-memory accounting. This observed fit does not prove maximum capacity.

### Population

| Format / GPU producer | Population ms |
| --- | ---: |
| Default | 85826.450 |
| PCBS | 5759.752 |
| Vortex | 20788.708 |

### Read-only reuse and computed phase combinations

First means the recorded warmup for the mode/projection; warmed is the median
of the two following samples. CPU precedes GPU on the same cache, so first GPU
is not globally cold. Default uses a CPU cache decoder before GPU conversion
and aggregation; PCBS/Vortex use GPU cache scans for GPU-enabled queries.

#### Full all-column read

| Format / GPU producer | Consumer (cache reader) | First read ms | Warmed median ms | Computed population + first ms | Computed population + warmed ms |
| --- | --- | ---: | ---: | ---: | ---: |
| Default | CPU (CPU) | 5560.953 | 5381.009 | 91387.403 | 91207.459 |
| Default | GPU-enabled (CPU cache decoder) | 4368.756 | 3979.743 | 90195.205 | 89806.192 |
| PCBS | CPU (CPU) | 21954.334 | 20290.702 | 27714.086 | 26050.455 |
| PCBS | GPU (GPU) | 1745.185 | 1043.448 | 7504.938 | 6803.200 |
| Vortex | CPU (CPU) | 12273.539 | 11997.896 | 33062.247 | 32786.605 |
| Vortex | GPU (GPU) | 70528.447 | 71252.518 | 91317.155 | 92041.227 |

#### Narrow ID-only read

| Format / GPU producer | Consumer (cache reader) | First read ms | Warmed median ms | Computed population + first ms | Computed population + warmed ms |
| --- | --- | ---: | ---: | ---: | ---: |
| Default | CPU (CPU) | 279.652 | 167.120 | 86106.102 | 85993.569 |
| Default | GPU-enabled (CPU cache decoder) | 419.565 | 396.185 | 86246.015 | 86222.634 |
| PCBS | CPU (CPU) | 15684.423 | 15733.824 | 21444.175 | 21493.576 |
| PCBS | GPU (GPU) | 455.833 | 523.178 | 6215.586 | 6282.930 |
| Vortex | CPU (CPU) | 861.450 | 948.139 | 21650.159 | 21736.847 |
| Vortex | GPU (GPU) | 3895.371 | 3399.091 | 24684.080 | 24187.800 |

Combined columns are computed additions of measured phases, not independently
measured end-to-end jobs. Full queries consume all fields; narrow queries select ID.
Vortex uses default adaptive encoding choices only. The CPU row-output and GPU
IPC routes are prototype implementation choices, not intrinsic format limits.

Evidence: /data/tmp/cache-main-16g/1024m/{default-gpu,pcbs-gpu,vortex-gpu}/
raw results, command/provenance snapshots, lead summaries, memory records and logs.
The lead independently verified budgets, all14 storage snapshots, aggregate and
reader checks, audit counts and sampled RSS before report generation. Numerical
phase tables are reproduced from original nanoseconds and independently checked.

## Fixed-wide: 2 GiB logical per task, GPU producers

All three GPU-producer cases **PASS** with **2048 MiB logical per task**
and **8192 MiB total**, four partitions and two task slots/two waves.
The primary settings remain a 16 GiB heap, 4 GiB RAPIDS-managed host cap and
32 GiB reactive RSS guard, using the original 8 MiB Vortex outer target.
Every full/narrow CPU/GPU read passed expected aggregates and actual-reader checks.
CPU-only producer results are reported separately below. Population is one fresh-job sample;
warmed read medians use two samples after one warmup, so results are preliminary.

### Storage and process footprint

| Format / GPU producer | Audited payload B | Records | Resident storage B | Logical resident MiB | Disk storage B | Sampled whole-job peak RSS B |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Default | 3,487,118,351 | 3356 | 3,335,300,728 | 8192 | 0 | 19,117,694,976 |
| PCBS | 4,107,649,355 | 1124 | 4,107,702,824 | 8192 | 0 | 9,637,453,824 |
| Vortex | 3,919,838,784 | 1024 | 3,919,883,904 | 8192 | 0 | 14,475,481,088 |

All fourteen storage observations per format have unchanged totals: every row
is resident, with no disk-only data or overlap. Storage-accounted bytes and logical
subsets use the same local block inventory. The post-run audit covers all rows;
its payload lengths are separate from BlockManager accounting. RSS includes
startup, reference actions and audit, may miss peaks and is not live-native or
temporary-memory accounting. This observed fit does not prove maximum capacity.

### Population

| Format / GPU producer | Population ms |
| --- | ---: |
| Default | 321273.290 |
| PCBS | 9738.901 |
| Vortex | 41934.176 |

At this size, Default logged roughly 2 GiB host allocation requests while more
than 2 GiB was already tracked, exceeding the fixed 4 GiB managed-host cap.
Its population time includes that backpressure; it is not pure encoding throughput.
A larger managed-host-cap control has not been run. This observation is specific
to this input plan, concurrency and memory budget.

### Read-only reuse and computed phase combinations

First means the recorded warmup for the mode/projection; warmed is the median
of the two following samples. CPU precedes GPU on the same cache, so first GPU
is not globally cold. Default uses a CPU cache decoder before GPU conversion
and aggregation; PCBS/Vortex use GPU cache scans for GPU-enabled queries.

#### Full all-column read

| Format / GPU producer | Consumer (cache reader) | First read ms | Warmed median ms | Computed population + first ms | Computed population + warmed ms |
| --- | --- | ---: | ---: | ---: | ---: |
| Default | CPU (CPU) | 10549.895 | 10094.548 | 331823.186 | 331367.839 |
| Default | GPU-enabled (CPU cache decoder) | 8314.751 | 7645.673 | 329588.041 | 328918.963 |
| PCBS | CPU (CPU) | 43258.512 | 42574.593 | 52997.414 | 52313.494 |
| PCBS | GPU (GPU) | 2028.247 | 1578.360 | 11767.148 | 11317.261 |
| Vortex | CPU (CPU) | 27505.363 | 26639.627 | 69439.539 | 68573.803 |
| Vortex | GPU (GPU) | 167244.530 | 178680.817 | 209178.707 | 220614.993 |

#### Narrow ID-only read

| Format / GPU producer | Consumer (cache reader) | First read ms | Warmed median ms | Computed population + first ms | Computed population + warmed ms |
| --- | --- | ---: | ---: | ---: | ---: |
| Default | CPU (CPU) | 524.236 | 272.122 | 321797.526 | 321545.413 |
| Default | GPU-enabled (CPU cache decoder) | 795.157 | 538.411 | 322068.448 | 321811.702 |
| PCBS | CPU (CPU) | 33077.265 | 33031.808 | 42816.166 | 42770.709 |
| PCBS | GPU (GPU) | 717.382 | 765.077 | 10456.283 | 10503.979 |
| Vortex | CPU (CPU) | 2096.451 | 1848.183 | 44030.627 | 43782.359 |
| Vortex | GPU (GPU) | 7745.977 | 7130.814 | 49680.153 | 49064.990 |

Combined columns are computed additions of measured phases, not independently
measured end-to-end jobs. Full queries consume all fields; narrow queries select ID.
Vortex uses default adaptive encoding choices only. The CPU row-output and GPU
IPC routes are prototype implementation choices, not intrinsic format limits.

Evidence: /data/tmp/cache-main-16g/2048m/{default-gpu,pcbs-gpu,vortex-gpu}/
raw results, command/provenance snapshots, lead summaries, memory records and logs.
The lead independently verified budgets, all14 storage snapshots, aggregate and
reader checks, audit counts and sampled RSS before report generation. Numerical
phase tables are reproduced from original nanoseconds and independently checked.

## Fixed-wide: 256 MiB logical per task, CPU-only SQL execution

All three **CPU-produced / CPU-consumed** cases PASS at 256 MiB
logical data per task, 1024 MiB total, four partitions and two local
task slots/two waves. GPU SQL execution is disabled throughout population and
all timed queries. The RAPIDS plugin remains loaded for the configured-off
scenario; this is not a claim that the process has no GPU initialization.
Resources are unchanged: 16 GiB JVM heap, 4 GiB RAPIDS-managed host cap and
32 GiB sampled RSS guard. Vortex uses the original 8 MiB outer target.
Population has one sample; each full/narrow read has one warmup and two samples.

### Storage and process footprint

| Format / CPU producer | Audited payload B | Records | Resident storage B | Logical resident MiB | Disk storage B | Sampled whole-job peak RSS B |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Default | 435,890,015 | 420 | 437,697,968 | 1024 | 0 | 2,120,806,400 |
| PCBS | 735,262,340 | 136 | 735,268,848 | 1024 | 0 | 5,190,844,416 |
| Vortex | 489,848,544 | 136 | 489,854,608 | 1024 | 0 | 4,223,963,136 |

All eight storage observations per case agree: every row is resident and disk
storage is zero. Resident storage bytes are Spark accounting estimates; payload
lengths are actual audited arrays, and RSS is a sampled whole-job footprint.
The audit follows timing/storage observations and covers all rows. These are
observed fits, not maximum capacities or isolated working-memory measurements.

### Population

| Format / CPU producer | Population ms |
| --- | ---: |
| Default | 20019.692 |
| PCBS | 12673.226 |
| Vortex | 6278.025 |

### CPU read-only reuse and computed phase combinations

First is the warmup for that projection; warmed is the median of two following
samples. Full queries consume all fields; narrow queries select ID only.

#### Full all-column read

| Format / CPU producer | First read ms | Warmed median ms | Computed population + first ms | Computed population + warmed ms |
| --- | ---: | ---: | ---: | ---: |
| Default | 1705.744 | 1469.287 | 21725.436 | 21488.979 |
| PCBS | 5925.487 | 5561.717 | 18598.714 | 18234.943 |
| Vortex | 2909.689 | 2714.931 | 9187.714 | 8992.956 |

#### Narrow ID-only read

| Format / CPU producer | First read ms | Warmed median ms | Computed population + first ms | Computed population + warmed ms |
| --- | ---: | ---: | ---: | ---: |
| Default | 193.505 | 81.404 | 20213.197 | 20101.096 |
| PCBS | 4237.744 | 4211.434 | 16910.971 | 16884.661 |
| Vortex | 266.162 | 274.580 | 6544.187 | 6552.605 |

Combined columns are computed additions of measured phases, not independently
measured end-to-end runs. These cases do not measure GPU consumption of
CPU-produced caches. PCBS CPU uses the UNCOMPRESSED codec while its GPU path
uses Snappy by default. Policies are not compression-matched; Parquet value
encodings can still reduce data without an external compression codec.

Evidence: /data/tmp/cache-main-16g/256m/{default-cpu-only,pcbs-cpu-only,vortex-cpu-only}/
raw results, source/artifact manifests, lead summaries, progress and memory logs.
All aggregates, CPU producer/reader plans, budgets, storage snapshots and audited
counts were independently checked before these preliminary tables were generated.

## Fixed-wide: 512 MiB logical per task, CPU-only SQL execution

All three **CPU-produced / CPU-consumed** cases PASS at 512 MiB
logical data per task, 2048 MiB total, four partitions and two local
task slots/two waves. GPU SQL execution is disabled throughout population and
all timed queries. The RAPIDS plugin remains loaded for the configured-off
scenario; this is not a claim that the process has no GPU initialization.
Resources are unchanged: 16 GiB JVM heap, 4 GiB RAPIDS-managed host cap and
32 GiB sampled RSS guard. Vortex uses the original 8 MiB outer target.
Population has one sample; each full/narrow read has one warmup and two samples.

### Storage and process footprint

| Format / CPU producer | Audited payload B | Records | Resident storage B | Logical resident MiB | Disk storage B | Sampled whole-job peak RSS B |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Default | 871,780,099 | 840 | 875,395,032 | 2048 | 0 | 3,100,147,712 |
| PCBS | 1,470,493,190 | 268 | 1,470,505,992 | 2048 | 0 | 6,050,312,192 |
| Vortex | 979,576,576 | 268 | 979,588,464 | 2048 | 0 | 5,709,697,024 |

All eight storage observations per case agree: every row is resident and disk
storage is zero. Resident storage bytes are Spark accounting estimates; payload
lengths are actual audited arrays, and RSS is a sampled whole-job footprint.
The audit follows timing/storage observations and covers all rows. These are
observed fits, not maximum capacities or isolated working-memory measurements.

### Population

| Format / CPU producer | Population ms |
| --- | ---: |
| Default | 38603.799 |
| PCBS | 23495.837 |
| Vortex | 12091.614 |

### CPU read-only reuse and computed phase combinations

First is the warmup for that projection; warmed is the median of two following
samples. Full queries consume all fields; narrow queries select ID only.

#### Full all-column read

| Format / CPU producer | First read ms | Warmed median ms | Computed population + first ms | Computed population + warmed ms |
| --- | ---: | ---: | ---: | ---: |
| Default | 2946.366 | 2857.102 | 41550.165 | 41460.900 |
| PCBS | 10932.754 | 10611.852 | 34428.590 | 34107.689 |
| Vortex | 5359.904 | 5183.774 | 17451.519 | 17275.388 |

#### Narrow ID-only read

| Format / CPU producer | First read ms | Warmed median ms | Computed population + first ms | Computed population + warmed ms |
| --- | ---: | ---: | ---: | ---: |
| Default | 221.135 | 138.149 | 38824.933 | 38741.947 |
| PCBS | 8318.834 | 8242.842 | 31814.671 | 31738.679 |
| Vortex | 610.700 | 431.667 | 12702.314 | 12523.281 |

Combined columns are computed additions of measured phases, not independently
measured end-to-end runs. These cases do not measure GPU consumption of
CPU-produced caches. PCBS CPU uses the UNCOMPRESSED codec while its GPU path
uses Snappy by default. Policies are not compression-matched; Parquet value
encodings can still reduce data without an external compression codec.

Evidence: /data/tmp/cache-main-16g/512m/{default-cpu-only,pcbs-cpu-only,vortex-cpu-only}/
raw results, source/artifact manifests, lead summaries, progress and memory logs.
All aggregates, CPU producer/reader plans, budgets, storage snapshots and audited
counts were independently checked before these preliminary tables were generated.

## Fixed-wide: 1 GiB logical per task, CPU-only SQL execution

All three **CPU-produced / CPU-consumed** cases PASS at 1024 MiB
logical data per task, 4096 MiB total, four partitions and two local
task slots/two waves. GPU SQL execution is disabled throughout population and
all timed queries. The RAPIDS plugin remains loaded for the configured-off
scenario; this is not a claim that the process has no GPU initialization.
Resources are unchanged: 16 GiB JVM heap, 4 GiB RAPIDS-managed host cap and
32 GiB sampled RSS guard. Vortex uses the original 8 MiB outer target.
Population has one sample; each full/narrow read has one warmup and two samples.

### Storage and process footprint

| Format / CPU producer | Audited payload B | Records | Resident storage B | Logical resident MiB | Disk storage B | Sampled whole-job peak RSS B |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Default | 1,743,560,044 | 1680 | 1,662,574,592 | 4096 | 0 | 4,000,350,208 |
| PCBS | 2,940,954,398 | 532 | 2,940,979,728 | 4096 | 0 | 9,390,419,968 |
| Vortex | 1,958,941,632 | 532 | 1,958,965,136 | 4096 | 0 | 7,809,339,392 |

All eight storage observations per case agree: every row is resident and disk
storage is zero. Resident storage bytes are Spark accounting estimates; payload
lengths are actual audited arrays, and RSS is a sampled whole-job footprint.
The audit follows timing/storage observations and covers all rows. These are
observed fits, not maximum capacities or isolated working-memory measurements.

### Population

| Format / CPU producer | Population ms |
| --- | ---: |
| Default | 82457.703 |
| PCBS | 46421.181 |
| Vortex | 22510.843 |

### CPU read-only reuse and computed phase combinations

First is the warmup for that projection; warmed is the median of two following
samples. Full queries consume all fields; narrow queries select ID only.

#### Full all-column read

| Format / CPU producer | First read ms | Warmed median ms | Computed population + first ms | Computed population + warmed ms |
| --- | ---: | ---: | ---: | ---: |
| Default | 5809.504 | 5485.507 | 88267.206 | 87943.210 |
| PCBS | 21028.167 | 20762.743 | 67449.349 | 67183.925 |
| Vortex | 11741.666 | 11287.980 | 34252.509 | 33798.823 |

#### Narrow ID-only read

| Format / CPU producer | First read ms | Warmed median ms | Computed population + first ms | Computed population + warmed ms |
| --- | ---: | ---: | ---: | ---: |
| Default | 237.173 | 200.698 | 82694.876 | 82658.401 |
| PCBS | 15939.415 | 15881.603 | 62360.597 | 62302.785 |
| Vortex | 796.104 | 937.641 | 23306.947 | 23448.484 |

Combined columns are computed additions of measured phases, not independently
measured end-to-end runs. These cases do not measure GPU consumption of
CPU-produced caches. PCBS CPU uses the UNCOMPRESSED codec while its GPU path
uses Snappy by default. Policies are not compression-matched; Parquet value
encodings can still reduce data without an external compression codec.

Evidence: /data/tmp/cache-main-16g/1024m/{default-cpu-only,pcbs-cpu-only,vortex-cpu-only}/
raw results, source/artifact manifests, lead summaries, progress and memory logs.
All aggregates, CPU producer/reader plans, budgets, storage snapshots and audited
counts were independently checked before these preliminary tables were generated.

## Fixed-wide: 2 GiB logical per task, CPU-only SQL execution

All three **CPU-produced / CPU-consumed** cases PASS at 2048 MiB
logical data per task, 8192 MiB total, four partitions and two local
task slots/two waves. GPU SQL execution is disabled throughout population and
all timed queries. The RAPIDS plugin remains loaded for the configured-off
scenario; this is not a claim that the process has no GPU initialization.
Resources are unchanged: 16 GiB JVM heap, 4 GiB RAPIDS-managed host cap and
32 GiB sampled RSS guard. Vortex uses the original 8 MiB outer target.
Population has one sample; each full/narrow read has one warmup and two samples.

### Storage and process footprint

| Format / CPU producer | Audited payload B | Records | Resident storage B | Logical resident MiB | Disk storage B | Sampled whole-job peak RSS B |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Default | 3,487,118,351 | 3356 | 3,335,300,728 | 8192 | 0 | 7,251,562,496 |
| PCBS | 5,881,877,158 | 1060 | 5,881,927,680 | 8192 | 0 | 15,266,975,744 |
| Vortex | 3,917,795,072 | 1060 | 3,917,841,808 | 8192 | 0 | 17,076,637,696 |

All eight storage observations per case agree: every row is resident and disk
storage is zero. Resident storage bytes are Spark accounting estimates; payload
lengths are actual audited arrays, and RSS is a sampled whole-job footprint.
The audit follows timing/storage observations and covers all rows. These are
observed fits, not maximum capacities or isolated working-memory measurements.

### Population

| Format / CPU producer | Population ms |
| --- | ---: |
| Default | 151765.038 |
| PCBS | 88446.376 |
| Vortex | 43817.920 |

### CPU read-only reuse and computed phase combinations

First is the warmup for that projection; warmed is the median of two following
samples. Full queries consume all fields; narrow queries select ID only.

#### Full all-column read

| Format / CPU producer | First read ms | Warmed median ms | Computed population + first ms | Computed population + warmed ms |
| --- | ---: | ---: | ---: | ---: |
| Default | 11468.064 | 10830.367 | 163233.102 | 162595.405 |
| PCBS | 42025.038 | 41659.611 | 130471.414 | 130105.987 |
| Vortex | 26132.747 | 24099.541 | 69950.667 | 67917.461 |

#### Narrow ID-only read

| Format / CPU producer | First read ms | Warmed median ms | Computed population + first ms | Computed population + warmed ms |
| --- | ---: | ---: | ---: | ---: |
| Default | 425.130 | 350.299 | 152190.168 | 152115.337 |
| PCBS | 31581.409 | 31727.154 | 120027.785 | 120173.530 |
| Vortex | 2840.643 | 2992.852 | 46658.563 | 46810.772 |

Combined columns are computed additions of measured phases, not independently
measured end-to-end runs. These cases do not measure GPU consumption of
CPU-produced caches. PCBS CPU uses the UNCOMPRESSED codec while its GPU path
uses Snappy by default. Policies are not compression-matched; Parquet value
encodings can still reduce data without an external compression codec.

Evidence: /data/tmp/cache-main-16g/2048m/{default-cpu-only,pcbs-cpu-only,vortex-cpu-only}/
raw results, source/artifact manifests, lead summaries, progress and memory logs.
All aggregates, CPU producer/reader plans, budgets, storage snapshots and audited
counts were independently checked before these preliminary tables were generated.

## Interpretation limits and next evidence

This tools-only prototype is local Spark 3.5.5 / Scala 2.12 and currently supports
int/long/string. Decimal, timestamp, nested-schema and unsupported-type fallback
coverage for Vortex is incomplete; it is not a production drop-in replacement.

Main CPU-produced caches were consumed only on CPU. GPU consumption of those
caches has preliminary mixed-mode correctness evidence in the earlier series,
but no primary 16 GiB performance matrix. No forced Vortex encoding policies,
compression-matched CPU Parquet control or per-column encoding audit was run.

The GPU Arrow bridge opens a cuDF reader for each decoded IPC batch. Its time
has not been separated from Vortex decoding; actual IPC/output-batch counts were
not captured. Logical task size is not measured GPU batch size. The synthetic
wide projection and 4 GiB managed host cap affect transfer/unroll behavior; Default's
largest population includes logged cap backpressure. A larger-cap control remains
unmeasured. Do not attribute these implementation/budget effects to format limits.

All 24 primary cases fit the tested envelope. This does not establish maximum
capacity, arbitrary-schema safety, long-lived eviction/repopulation behavior or
a total Arrow/Rust allocation budget. The earlier 1 GiB heap / 4 GiB RSS experiments
remain separately labeled in the constrained appendix. Their failures do not
predict the user-selected normal-host baseline.

Before an adoption decision, the most useful next controls are compression-matched
CPU Parquet and an Arrow bridge profile/streaming comparison. They would distinguish
codec choices from integration costs without treating the constrained-memory trial
as a format defect. No production source, public configuration or release contract
was changed by this exploration.
