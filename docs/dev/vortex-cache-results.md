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

The first primary cohort is 256 MiB per task, GPU production with CPU and GPU
consumers. Values will be added only after independent verification.
