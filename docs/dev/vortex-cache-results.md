# Spark cache comparison: 16 GiB JVM heap

Status: IN PROGRESS. The user selected a 16 GiB JVM heap for the primary comparison.
Earlier 1 GiB-heap runs are [constrained-memory experiments](vortex-cache-constrained-results.md),
not the normal-host baseline. No production serializer changes are proposed.

Each primary run uses two local task slots, four source/cache partitions, default
DataFrame memory-and-disk storage, a 4 GiB RAPIDS-managed host-memory cap and a
32 GiB sampled process-tree RSS early-stop guard. Arrow/Rust allocations are outside
the plugin cap. The guard is reactive and can miss brief peaks; it is not an OS limit.
The sandbox has about 188 GiB host RAM; roughly 175 GiB was available before this cohort.
No machine-exhaustion testing is authorized.

The fixed-wide fixture has 32 nullable numeric fields, including monotonic,
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

Population, first-for-mode/projection reads and warmed read medians will be reported
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

## Fixed-wide: 256 MiB logical per task, GPU producers

All three formats **PASS** at 4,194,304 rows, four partitions of 1,048,576 rows,
**256 MiB logical per task and 1 GiB total**. GPU production and CPU/GPU
consumption use the same stored data within each fresh job. Every full/narrow
read passed the expected aggregates and actual cache-reader plan checks.
These are preliminary results from one population sample and two measured reads
after one warmup for each mode/projection.

CPU-producer cases with the 16 GiB heap are pending; later sections cover larger GPU-producer cases.
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
CPU producers at this size are unmeasured. Population is one fresh-job sample;
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
CPU producers at this size are unmeasured. Population is one fresh-job sample;
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
