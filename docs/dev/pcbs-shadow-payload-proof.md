# Local spillable PCBS payload proof

Status: READY FOR REVIEW for the first proof. Direct host-buffer GPU path and
stress tests are in progress; no production design is selected.

The disposable registry prototype passed the original large workload with the
default unroll period (16) and Java object-stream reset interval (100). Spark
retained small references while the registry retained the Parquet payloads in
spillable native buffers and disk files. This proves the local retention idea
for this workload, not distributed ownership, deletion or failure recovery.

## First proof: array-copy path

Source commit `98e5bcc897` is isolated in
`/data/pcbs-oom/pcbs-shadow-experiment`. Original 1 GiB batch sizing is unchanged;
no 16/32 MiB cache split cap is present. Each descriptor contains only row count,
payload ID and encoded payload length. SQL cache size/row accumulators preserve
the original values. BlockManager accounts for the small physical descriptors.

Spark 3.5.5, Scala 2.12, Java 17, local[8], configured 48 GiB heap, ParallelGC;
GPU concurrency/max 2, GPU pool 15 GiB, host limit enabled at 40 GiB, pinned pool
2 GiB, memory fraction 0.6. Sixteen cache partitions, each 50,079,984 rows in
six records, sixteen Double columns, 96 source partitions. Every encoded record
was exactly 1,068,538,171 bytes; total encoded data was 102,579,664,416 bytes
(95.535 GiB). Total rows per full read: 801,279,744.

| Measurement | First fresh JVM |
|---|---:|
| Sequential cache population | 246.403 s |
| Warmup full SQL read, excluded from median | 81.244 s |
| Three measured full SQL reads | 74.095 / 73.939 / 76.581 s |
| Median measured read | 74.095 s |
| GC time / measured read elapsed time | 3.90% |
| Maximum sampled heap occupancy | 35.15 GiB |
| Maximum sampled whole-process RSS | 76.66 GiB |
| Observed swap | 0 |

All four SQL reads checked every input column through finite, stable aggregate
sums and exact scan row counts. The JVM exited normally, with no heap OOM.
Population and all four read checkpoints retained exactly 96 registry entries
and the same total payload bytes. After population, 39 payloads were resident
in host memory and 57 had disk handles; after reads, 37 were resident and 59
had disk handles. Resident payload bytes stayed below the configured host limit.
Spark's entire cached reference set occupied 4,992 bytes in memory, with no
Spark cache disk bytes. Shadow disk handles are separate from Spark disk bytes.

Existing task metrics recorded GPU admission high-watermarks of 1 during the
sequential fill and 2 in cache-read stages. This measures semaphore admission,
not simultaneous kernels. The first proof still creates temporary Java payload
arrays on writing and reading. Its log contains 735 recoverable native pool
capacity messages at the 2 GiB pinned limit; no tuning was applied to hide them.
Attribution of their performance cost has not been established. Peak sampled
heap is occupancy, not a live-set measurement. RSS sampling began during
population and therefore does not cover initial startup/early population.

The previous original 1 GiB/default-check workload failed. A previous passing
1 GiB control using unroll/reset 1/1 had median fill/read times of
249.370/37.460 s across three fresh JVMs. The current array-copy registry is
slower to read in this first JVM. These variants change ownership, cache tiers
and copy paths as well as checks; they do not isolate one setting's overhead.
The next GPU path will remove the intermediate Java payload arrays with all
resource limits preserved. Neither this timing nor a later local result is a
customer MERGE performance prediction.

## Serialization and correctness checks

A separate fresh-JVM check tested both row and GPU-columnar writer entries.
Each encoded 1.2 million ordered four-Long rows into a 19,203,662-byte payload
and a 127-byte serialized reference. Java serialization and Spark DISK_ONLY
reload preserved the ID, row count and payload length. Parquet header/footer
and every decoded value/order passed. A second fresh-JVM smoke verified the
non-materializing registry/spill diagnostics used by the large benchmark.
CPU operation without the GPU plugin is outside this disposable prototype's
scope; the existing CPU reader still reconstructs a Java array.

## Evidence and review

Evidence root: `/data/pcbs-oom/pcbs-shadow-sweep`.

- `build-result.json`, immutable `rapids-shadow-array-cuda12.jar` and frozen sources.
- `entry-shadow-array-result.json`, `entry-shadow-diagnostics-result.json` and raw logs.
- `shadow-array-manifest.json`, per-case command, raw log, GC log and event log.
- `shadow-array-results.jsonl`, GPU samples and explicitly partial RSS samples.
- `stage1-independent-verification.json` and `verify_stage1.py`: root independently
  checked raw population shapes, exact rows, five storage checkpoints, normal exit,
  source/artifact hashes and task-local GPU concurrency accumulator updates.

The benchmark is a versioned copy of `tools/CacheReloadBench.scala`, with
non-materializing registry/store diagnostics after measured phases. The original
benchmark and baseline plugin artifact remain unchanged. Source/jar hashes and
the diagnostic diff are retained. The first compile attempt used a nonexistent
Spark jars directory; its error log is preserved, the classpath was corrected
and compilation succeeded before launching any large job.

A peer independently reviewed the prototype source and compiled descriptor
fields. No in-scope defect was found. All project operations used MCP docker_exec.
No broad unit-suite pass is claimed: packaging skipped tests, and the scoped
checks deliberately use a fresh JVM rather than unsupported multi-session or
plugin-disabled behavior.

## Remaining tests and production limits

The user authorized a direct native GPU writer/reader after this first pass,
then a heap-size boundary and independent 2 GiB, automatic GPU concurrency and
16-thread cases before combinations. Keep tracked host allocation bounded at
40 GiB with disk spill, pinned 2 GiB and GPU 15 GiB. At 16 threads use 32 cache
partitions for two waves. To create actual roughly 2 GiB records, generate three
larger source partitions per cache partition instead of six smaller sources;
keep the same approximately 6 GiB per cached partition and report that change.

The prototype intentionally never deletes published payloads. IDs are valid
only within one JVM, which exits after each test. No remote fetch, actual RDD
owner association, replication, retirement or missing-payload recovery is
implemented. See the separate metadata and lifecycle research. Any Spark
modification remains an explicitly labeled last resort; none is needed or
proposed for this local proof.
