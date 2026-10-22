# PCBS-only cached-record split comparison

READY FOR REVIEW — architect acceptance pending

Source changes are isolated on branch `pcbs-cache-size-experiment` in `/data/pcbs-oom/pcbs-split-experiment`.

Only GPU PCBS split budget changes in the isolated prototype; global GPU batch target remains 1 GiB. Global-target controls and prototype use default unroll period 16/reset 100, memory fraction 0.6.

| Change | Target MiB | PASS/JVMs | Fill seconds (range) | Scan seconds (range) | Fill/read GC % | Max post-full-GC GiB | Records/partition | Max encoded record MiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| global target | 16 | 3/3 | 271.17 (270.67–271.58) | 44.99 (42.76–45.47) | 2.51/9.27 | 31.13 | 402 | 15.924 |
| global target | 32 | 3/3 | 264.04 (254.37–264.05) | 42.89 (41.76–43.49) | 3.72/15.04 | 35.01 | 198 | 31.846 |
| PCBS only | 16 | 3/3 | 273.72 (273.55–276.41) | 45.20 (44.62–45.46) | 2.60/8.80 | 31.14 | 390 | 15.924 |
| PCBS only | 32 | 3/3 | 262.73 (262.62–263.16) | 42.96 (41.91–43.35) | 4.22/16.03 | 35.15 | 198 | 31.846 |
| historical checks | 1024 | 3/3 | 249.37 (247.45–251.25) | 37.46 (37.38–38.71) | 5.73/24.70 | 41.87 | 6 | 1019.037 |

For each size, PCBS-only splitting and lowering the global target have overlapping read-time ranges; fill medians differ by less than 1%. The larger cache records trade lower elapsed time for higher GC time and post-full-GC occupancy. No production policy is selected.

Fill is the median across completed fresh JVMs, including a successful fill followed by a read OOM. Read is the median of per-JVM medians, each calculated from three timed scans; ranges are JVM-median ranges. One full warmup precedes those three scans. Read GC is the median per-JVM timed-scan GC fraction. Failed JVMs remain in pass counts and have no comparable read timing. Passing JVM timings are conditional on success.

Historical checks is a qualified reference: both unroll/reset change to 1/1, so differences from that row do not isolate batch sizing or prototype overhead. Global-target controls and PCBS-only cases ran in separate groups, not interleaved randomized trials. Three JVMs do not establish production reliability or OOM probability.

The [global-target sweep](pcbs-batch-size-sweep.md) also retains the default-settings 64 and 512 MiB heap-OOM controls. Those sizes are not rerun or scheduled as PCBS-only prototypes here.

## Workload and limits

Spark 3.5.5/Scala 2.12.18/Java 17.0.20.1 on one RTX A6000; local[8], shared driver/executor 48 GiB heap, ParallelGC, GPU concurrency/max 2, fixed 15 GiB GPU pool, host off-heap limit 40 GiB, pinned pool 2 GiB. Customer GPUs are T4s; this is a synthetic local-mode comparison.

801,279,744 rows of sixteen seeded Double columns (128 value bytes/row), sixteen cache partitions from ninety-six sources. Approximately 95.54 GiB encoded cache, about 6 GiB/partition. MEMORY_AND_DISK deserialized caching; natural GC. Fill is sequential by partition and includes generation, encoding, storage and job overhead; it is not parallel customer cache-build throughput. Full reads sum every column, with no filter. Repeated reads can use warm OS page cache, and cache memory/disk residency is preserved in case CSVs.

Split layouts are verified from actual row/byte arrays; equal record counts do not establish identical boundaries. The inspected 32 MiB partition has 198 records in both approaches: PCBS-only has 192 full 260,833-row records and six 8-row tails, whereas the global-target control has 186 full records and larger Range-derived tails. Changing the global target also changes Range/source batch boundaries. Data shape and seeded distribution match; bit-identical random values across global settings are not claimed.

Small cache records still decode one Parquet file at a time. Keeping the global target at 1 GiB does not guarantee large GPU batches after the scan. In the inspected cap16/trial0 SQL plan, the cache scan feeds the partial GpuHashAggregate directly: there is no coalescer between them. The 1 GiB GpuShuffleCoalesce appears after the partial-aggregate exchange and does not combine cached input before aggregation. The scan reports zero output batches despite the verified full row count; that metric is not used as a batch count. The existing encoder eagerly retains all output records for an input batch; the row entry can materialize an entire partition before splitting. Neither ownership nor materialization behavior is fixed by this experiment. The existing average-row estimate and metadata allowance do not impose a strict encoded-byte bound for arbitrary schemas or oversized values. Post-full-GC numbers are whole-heap occupancy, not dominator-retained size; sampled occupancy includes garbage.

## Provenance and focused validation

Baseline plugin source: 3cdf711ded8e8b72d0f3ee92866401df5d9d2a06.
Baseline artifact SHA-256: `f3bc760d51c1c8596ac9108242f2c9366f570d7b5612a1239aba88e73ee25af5`.
Unchanged benchmark source SHA-256: `70b05672362009d39ae32cddf2897227c9451a32847ab3ddd9331fe9f0f207b2`; jar: `395f1a2601d2cac253ff46aaa2d40b2a5222360fbc57298a3a8d903df6137e35`.

Cap 16 MiB source revision: `ba6d57a9ab5e349fe53a5089f4c3449268d713b6`; source SHA-256: `86e2deb05a39aa2581c8b2ad6cc0caf1f6cc149e0a4f07f4142688d74e3d3bb1`.
Artifact: `/data/pcbs-oom/pcbs-only-sweep/rapids-cap16-cuda12.jar`; SHA-256: `ba863f88d8470866bf1d55038e1d941faa5fe565b60d7bf3aae83066d0c2d83d`.
CachedBatchWriterSuite: 17 passed, no failures/errors/skips.
Known-value row and GPU-columnar writer entries: round-trip PASS with global target 1 GiB; PCBS_ENTRY_SUCCESS entry=row rows=1200000 records=3 globalTarget=1073741824 cap=16777216; PCBS_ENTRY_SUCCESS entry=columnar rows=1200000 records=3 globalTarget=1073741824 cap=16777216.

Cap 32 MiB source revision: `b65451d39134eb9f6475c5ae16fb437534d7b760`; source SHA-256: `b3c11561bbe40bb8fd78f64e308cb864d1fe1eda09214776d777b07495cb5c32`.
Artifact: `/data/pcbs-oom/pcbs-only-sweep/rapids-cap32-cuda12.jar`; SHA-256: `ab3167db471c90a97068ee3ca1923d028e941e74edfe324a75c99de2ca0efaf7`.
CachedBatchWriterSuite: 17 passed, no failures/errors/skips.
Known-value row and GPU-columnar writer entries: round-trip PASS with global target 1 GiB; PCBS_ENTRY_SUCCESS entry=row rows=1200000 records=2 globalTarget=1073741824 cap=33554432; PCBS_ENTRY_SUCCESS entry=columnar rows=1200000 records=2 globalTarget=1073741824 cap=33554432.

The cap adapts the existing GPU-only budget helper with the same 0.5% metadata allowance and original safety ceiling. CPU writer, API, cached representation, global configuration and resource lifecycle are unchanged. Existing sizing is released behavior; this isolated altered cache layout is experimental. No production default policy or fix is selected.

Inputs: audited `cases.json` and `summary.json` in batch-size-sweep and pcbs-only-sweep, with historical cases in unroll-sweep. The lead and an independent worker verified raw logs, effective Spark/JVM configuration, source/artifact hashes, exact record rows and bytes, full-scan counts, storage checkpoints, exit status and GC calculations. Both cap variants passed the focused suite and entry-point checks. Full unit/integration suites and production workload validation are not claimed. All project operations used MCP docker_exec.

[Comparison CSV](pcbs-split-budget-sweep.csv) · [Per-JVM cases](pcbs-split-budget-cases.csv).
