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
represented logical data are separate measurements.

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

Default's GPU-enabled query uses a CPU cache decoder followed by GPU work.
PCBS and Vortex can use direct GPU cache scans. Vortex's current CPU row-output
and GPU Arrow IPC paths are prototype choices, not intrinsic format speed limits.
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

CPU-producer cases with the 16 GiB heap and larger primary task sizes are pending.
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
