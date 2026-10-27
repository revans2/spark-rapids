# Vortex Arrow bridge experiments

Status: READY FOR REVIEW for the bounded numeric implementation and controls.
This follows the [profiling investigation](vortex-cache-profiling.md).
The [original 24-case comparison](vortex-cache-results.md) and its CSV remain intact.
No production serializer, allocator defaults or supported configuration changed.

The 256 MiB shared pinned pool removed the expensive raw pin/unpin calls from
Arrow import. Direct buffer upload also substantially reduced full GPU read
time with the pinned pool left at zero. Neither result establishes a general
out-of-box Vortex integration.

## Matched unprofiled GPU controls

Each case uses GPU population and GPU consumption of 1 GiB logical fixed-wide
data: 256 MiB per task, four partitions, two task slots/two waves. The JVM heap
is 16 GiB, RAPIDS-managed host cap 4 GiB, sampled process-tree RSS stop guard
32 GiB. Default DataFrame memory-and-disk storage, one warmup and two timed reads
per projection. Each case is one fresh JVM/cache, not independent repetitions
of the entire experiment. Arrow/Rust allocations remain outside the plugin cap.

All times below are seconds. Population-plus-read is a computed sum of measured
phases, not a separately timed combined job. Full reads consume every field;
narrow reads select ID. These controls read GPU only; unlike the original
matrix, CPU reads do not precede GPU reads.

| GPU read route / pinned pool | Population | First full read | Warm full median | Population + warm full | Warm ID-only median |
| --- | ---: | ---: | ---: | ---: | ---: |
| Original IPC / 0 | 7.134 | 12.000 | 13.241 | 20.375 | 0.813 |
| Original IPC / 256 MiB | 5.701 | 1.665 | 1.339 | 7.039 | 0.277 |
| Direct buffers / 0 | 7.065 | 3.305 | 2.506 | 9.571 | 0.764 |

The two full-read samples were respectively 13.176/13.306, 1.464/1.214 and
3.230/1.782 seconds. In particular, the direct-buffer samples vary substantially;
two samples do not establish a stable throughput distribution or a precise
speedup. The direct variant is slower than pooled IPC in this control.
[Machine-readable results](vortex-cache-bridge-results.csv) preserve the raw samples.

## Size, capacity and memory

Every case audited exactly **489,897,376 encoded bytes in 128 cache records**,
representing 4,194,304 rows / 1,073,741,824 logical bytes. All four partitions
were resident, with disk bytes zero at every recorded storage snapshot.
Spark's resident storage estimate was 489,903,072 bytes in all three cases.
The direct route therefore changed consumption, not cache encoding or retained
logical capacity.

| GPU read route / pinned pool | Sampled whole-job peak RSS (bytes) |
| --- | ---: |
| Original IPC / 0 | 4,379,815,936 |
| Original IPC / 256 MiB | 6,110,429,184 |
| Direct buffers / 0 | 4,336,750,592 |

RSS includes startup, reference actions, population, reads, post-timing payload
audit and heap garbage. It is not live native working memory or a per-phase peak.
The pooled job's larger RSS cannot be explained by holding more logical cache
data in this experiment; all cases retained the same data. It also must not be
attributed entirely to the 256 MiB pool from these snapshots alone.
The guard did not fire; no machine-exhaustion experiment was performed.

## Pooling evidence

The first 64 MiB pool diagnostic passed correctness but logged 16,321 pool
exhaustion messages. It retained 7,936 raw pinned allocations and frees inside
Arrow import and full read latency remained about 14 seconds. This is evidence
of an inadequate pool for that run, not a general negative result for pooling.

The 256 MiB diagnostic initialized 256 MiB pinned, 3,810 MiB non-pinned and
30 MiB untracked overhead inside the same configured 4 GiB budget. It logged no
pool exhaustion. Across 512 from_arrow NVTX ranges, raw cudaHostAlloc and
cudaFreeHost counts fell to zero. Summed range time fell from 39.822 seconds in
the original zero-pool profile to 0.905 seconds. All range/API sums span concurrent
task threads and multiple reads, and must not be interpreted as one query's wall
time. The import still performed null-count kernels and copies; pooling removed
their raw pinned-allocation overhead rather than eliminating all import work.

These runtime measurements establish effective pool use beyond merely setting
a configuration flag. They do not establish a universal pool size, a hard native
memory limit or permission to change released allocator defaults.

## Direct-buffer profiling

A final small Nsight/JFR diagnostic reproduced a 3.208-second full read and
0.647-second ID-only read. Independent lead queries found zero from_arrow ranges
and zero count_set_bits kernels, compared with 512 ranges and 7,936 such kernels
in the original IPC diagnostic. Whole-job raw pinned allocations/frees fell from
16,104 each to 8,168 each; the remaining calls are not attributed to decoding by
these whole-job statistics. GPU H2D copy execution totaled 0.123 seconds.
The direct route bypasses the targeted importer work, but the remaining host
copy/setup cost and sample variability still matter. These profiled times do
not replace the unprofiled controls.

## Implementation and verification

SparkVortexDirectBufferSerializer is an alternate exploration class. It inherits
the encoder and CPU path, but its GPU read accepts only cached int/long schemas.
The isolated Arrow 19 decoder returns borrowed JDK ByteBuffers plus widths,
null counts and row count. The parent copies into owned host buffers, pads masks
for cuDF, and uses HostColumnVector.copyToDevice with the exact null count.
Host buffers use the default allocator installed by HostAlloc. Uploads complete
before decoder advancement; GPU retries retain the same Arrow root and reset
buffer positions. ARM handles staged buffers, GPU columns and table references.
The original IPC path reuses the same lazy root-reading logic.

Independent lead verification reproduced all aggregates, actual GPU scan plans,
artifact hashes, budgets, encoded sizes and resident snapshots from saved results.
The lead also independently queried both pool traces and inspected the decisive
codec, allocator and upload code. Focused peer review found no blocking issues.

The separate build compiled for Spark 3.5.5 / Scala 2.12. Seven lifecycle cases
passed, including borrowed-buffer exhaustion, partial and unread close, plus
invalid-input cleanup. Two 513-row edge probes passed for CPU and GPU population,
each consumed on CPU and GPU with all values/nulls/order checked, including
all-null/all-valid fields and reversed/ID-only projections. The new build's
original IPC path also passed a string/null regression probe on CPU and GPU.
No runtime OOM or injected retry test was attempted.

Genuine nonzero Arrow slice offsets, duplicate projections, injected retries and
task cancellation remain unverified. The direct variant deliberately rejects
string-containing GPU caches even for ID-only projection; strings, nested types,
decimals, booleans and general layout conversion are not implemented there.
The existing prototype's broader limitations still apply. No larger matrix was
rerun, and no full CPU performance comparison of the new route is claimed.

## Reproduction artifacts

The original application SHA256 is
78e04232b44e4a0569eb024ec0d911649719c560808c9bc457bceabe1f5f8824.
The direct build under /data/tmp/vortex-spark-direct-buffer has application SHA256
860a81612d8777afed716ce63a3ba52ecb24c3dedb2f7e7a2db9d000ae5c4225.
Native Vortex and runtime dependencies are unchanged.

Unprofiled jobs: /data/tmp/cache-control-vortex-pool0,
 /data/tmp/cache-control-vortex-pool256 and /data/tmp/cache-control-vortex-direct0.
Each contains command/provenance snapshots, result.json, memory samples, logs,
payload audit and lead-verified-summary.json. Pool profiles are under
/data/tmp/cache-profile-vortex-pool64 and /data/tmp/cache-profile-vortex-pool256;
original zero-pool profile is /data/tmp/cache-profile-vortex-256.
Direct profile: /data/tmp/cache-profile-vortex-direct0, including its own
lead-independent-verification.json. Correctness artifacts:
/data/tmp/cache-direct-validation.
Lead independent control verification: /data/tmp/bridge-controls-lead-verified.json.

The launcher exposes only experimental route/pool choices; see the
[prototype README](../../tools/cache-exploration/spark/README.md).
Every operation used MCP docker_exec. Implementation is ready for architect
review; only the architect marks it DONE.
