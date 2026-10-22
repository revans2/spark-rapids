# Local spillable PCBS payload proof

Status: READY FOR REVIEW for the local registry, native GPU cache path,
reduced-heap controls and stress tests. No production design is selected.

The disposable registry prototype passed the original large workload with the
default unroll period (16) and Java object-stream reset interval (100). Spark
retained small references while the registry retained the Parquet payloads in
spillable native buffers and disk files. This proves the local retention idea
for this workload, not distributed ownership, deletion or failure recovery.

The strongest completed case cached 191.08 GiB as 96 approximately 2 GiB
payloads with a configured 512 MiB heap and local[16]. All four complete reads
passed; automatic GPU admission reached 3. Default unroll/reset stayed 16/100,
with host40/pinned2/GPU15 GiB limits. Peak sampled heap was 239 MiB and process
RSS 39.66 GiB. This eliminates batch-sized PCBS Java arrays; bounded spill
codec/staging arrays remain, so literal zero heap traffic is not established.

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

## Native PCBS control: same generator and data layout

The native cache-path source `d247fe7a6e` passed the same large workload in
one fresh JVM with all settings above unchanged. Consumer callbacks now copy
valid native ranges into a spillable host buffer; the GPU reader uses cuDF's
HostMemoryBuffer overload. The descriptor still has three primitive fields.

| Measurement | Native PCBS control |
|---|---:|
| Sequential cache population | 214.977 s |
| Warmup full SQL read | 21.591 s |
| Three measured full SQL reads | 20.929 / 20.632 / 20.116 s |
| Median measured read | 20.632 s |
| GC time during measured reads | 0 ms |
| Maximum sampled heap occupancy | 16.02 GiB |
| Maximum sampled whole-process RSS | 56.71 GiB |

All rows, aggregate checks, six-record partition shapes and 95.535 GiB total
encoded bytes passed. Array put/get calls and bytes at the PCBS registry were
zero at all five checkpoints. Native puts were exactly 96 records; native read
attempts were exactly 384, corresponding to four complete reads. Spark retained
4,992 bytes of references. Host/disk handles were 38/58 after fill and 37/59 after
reads. The JVM exited normally. Root reran an independently prepared analyzer
against raw events, including native counter validation, with no audit issues.

This is **not an end-to-end zero-heap payload path**. Source audit found:

- GpuRand generates primitive doubles on the CPU into native HostColumnVector
  storage before copying to the GPU. GpuRange itself uses a native sequence.
- SpillOutputChannel uses reusable 8 KiB Java staging arrays; default compressed
  spill additionally uses reusable LZ4 block arrays (32 KiB block setting).
- Eliminating the channel staging alone would not remove codec heap traffic.
  A raw-channel experiment requires different compression settings and explicit
  encryption checks; neither was changed for these comparisons.
- Registry counters prove the PCBS payload-array paths were avoided, not that
  all components avoided heap. Significant temporary allocation remains during
  population. An untimed generation-only JFR probe subsequently attributed most
  weighted allocation samples to boxed loop indices in GpuRand (see below).
  Heap peaks are occupancy, not retained payload size.

These bounded spill buffers differ from batch-sized retained arrays. Reducing
only heap to 16 GiB also passed: fill 215.200 s, median read 20.356 s, read GC
0.0098%, maximum sampled heap occupancy 5.38 GiB and RSS 45.95 GiB. Row layout,
payload totals, native routing, aggregates and JVM exit passed independent checks.

The untimed JFR probe generated and aggregated six source partitions without
caching. All 1,239 Integer samples had the same allocation chain through
Integer.valueOf, Scala Range.foreach and GpuRand.doColumnarEval. Their sample
weights were 12.094 GiB of 13.144 GiB total. These weights are estimates, not an
exact allocation census or attribution of all cache-fill allocation. No task-
reported Spark or RAPIDS spill occurred in this probe; background activity is
not excluded by task metrics.

## GPU-generated workload and stress sweep

A separate fixture uses salted xxhash64, a 53-bit mask, cast and division to
produce sixteen Double columns. Its oracle verified 160,000 CPU/GPU values by
raw bits, GPU-only computational expressions and 128 native bytes per row.
The dataset and generation cost differ from rand, requiring a new baseline.

| Heap / GPU mode | Fill | Median full read | Read GC | Peak sampled heap | Peak RSS |
|---|---:|---:|---:|---:|---:|
| 48 GiB / capped 2 | 125.243 s | 20.486 s | 0 ms | 4.75 GiB | 42.21 GiB |
| 16 GiB / capped 2 | 127.860 s | 20.533 s | 0 ms | 2.58 GiB | 42.18 GiB |
| 48 GiB / automatic | 126.816 s | 13.013 s | 0 ms | 4.54 GiB | 41.31 GiB |
| 512 MiB / capped 2 | 126.489 s | 20.453 s | 0.0404% | 0.194 GiB | 40.82 GiB |

This baseline passed exact row/shape/aggregate checks and normal exit. It held
96 payloads totaling 102,584,813,088 encoded bytes, with 37 host/59 disk handles
after capped reads. Automatic mode passed the same payload/shape checks with
32 host/64 disk handles after its first measured read. Both ordinary and internal
plugin-prefixed concurrency overrides were absent from effective configuration;
read-stage GPU admission high-watermarks were 7 versus 2 when capped. This is
semaphore admission, not simultaneous kernels. PCBS registry array put/get
counters remained zero. All fixed limits
and default checks remain unchanged. The heap, automatic-concurrency, 16-thread
and actual 2 GiB-record controls have passed. The combined stress also passed
as recorded below. Each case uses one fresh JVM and three measured reads after
warmup; these are local synthetic timings, not a customer MERGE prediction.

512 MiB is the lowest heap tested, not a proven exact minimum. The intermediate
8/4/2/1 GiB sizes were skipped after 16 GiB passed. Reduced heap does not reduce
the configured native pool limits; RSS remains approximately 41 GiB.

The first 2 GiB-target run exited normally but failed the required three-record
shape: it still emitted six approximately 1 GiB records. GpuProject pre-splits
at GPU pool/8 (1.875 GiB here), independently of PCBS. This outcome is retained
as an invalid stress fixture. A second fixture added the existing test-only
3 GiB projection threshold, but also failed shape: direct RDD fill skipped SQL
configuration propagation, so tasks still used the default. Both normal-exit
failures remain in the evidence. V5 now sets/restores only that task-local key,
checks its actual getter in every population task, and requires measured encoded
records >95% of the 2 GiB target. The device pool and host limits remain fixed;
this extra fixture dimension must accompany any timing comparison.

The revised 2 GiB-record case PASSED all checks and normal JVM exit. Each of
48 records measured 2,137,181,644 encoded bytes (about 1.99 GiB), totaling
102,584,718,912 bytes. Fill was 161.977 s; measured reads were
34.361/34.748/33.942 s (median 34.361 s), read GC 0 ms, sampled heap peak
5.34 GiB/RSS 41.85 GiB. Native array counters stayed zero, and all 16 actual
executor threshold checks passed. Root independently ran the strict large-record
and raw-event checks, with no audit issues. This case is slower than the 1 GiB
control in this fixture; the projection threshold/source boundary adaptation
must remain visible in the comparison.

Process IO sampling began late, so coverage is explicitly partial. During
95.0% and 97.2% sampled subwindows of the last two measured reads, logical read
counters advanced by approximately 60–61 GiB while kernel-accounted storage
read bytes stayed zero. This does not measure exact whole-phase or payload-only
IO; it does not support cold storage reads as the cause of this case's slowdown.

The independent 16-thread case also PASSED with 32 cache partitions and twice
the data (205,169,626,176 encoded bytes): fill 300.317 s, warmup 104.013 s,
measured reads 94.637/95.151/94.997 s, read GC 0 ms, sampled heap peak 7.74 GiB
and RSS 43.86 GiB. Spark retained 9,984 bytes of references; 192 payloads remained
present, with 36 host/156 disk handles after reads. Native bulk and independent
correctness checks passed. Stock executor gauge observed 16 active task threads
in every read while GPU admission stayed at 2. The first sampler URL redirected;
its failed coverage and later shutdown errors are retained, not counted as zero
threads. Corrected samples cover all reads.

This larger case is slower per byte: warmup GPU utilization averaged 9.6% versus
40% in the smaller baseline. Spill read/materialization summed task time was
182.749 s versus 29.728 s. That timer includes open/read/decompression/copy;
physical disk/cache-miss attribution is not established. Only one additional
approximately 1 GiB spill write and exact registry read counts were observed,
so repeated payload rewrite/retry is not supported as the explanation.

The combined local[16]/32-partition/2 GiB-target/automatic-GPU/512 MiB case
PASSED, including normal JVM exit and independent strict record/config checks.
It retained 96 payloads of 2,137,181,644 bytes each (205,169,437,824 bytes total),
with 15 host/81 disk handles after reads. Spark references occupied 4,096 bytes.
All 32 task threshold markers were 3 GiB; array put/get counters remained zero,
and native reads were exactly 96/192/288/384 across the four full reads.

| Additional stress | Encoded data | Fill | Median read | Read GC | Peak sampled heap / RSS |
|---|---:|---:|---:|---:|---:|
| 1 GiB, 16 threads, 48 GiB heap, GPU capped 2 | 191.08 GiB | 300.317 s | 94.997 s | 0 ms | 7.74 / 43.86 GiB |
| 2 GiB, 8 threads, 48 GiB heap, GPU capped 2 | 95.54 GiB | 161.977 s | 34.361 s | 0 ms | 5.34 / 41.85 GiB |
| 2 GiB, 16 threads, 512 MiB heap, automatic GPU | 191.08 GiB | 358.876 s | 106.227 s | 47 ms (0.0147%) | 0.234 / 39.66 GiB |

Combined warmup was 115.575 s; measured reads were 106.227/105.117/109.340 s.
No ordinary/internal concurrency overrides were present. GPU admission MAX was
3 and sampled active executor threads reached 16 in every read; neither metric
is simultaneous kernel/CPU execution. Process IO subwindows covered 98.8–98.9%
of measured reads and recorded 155.5–160.0 GiB kernel-accounted backing reads.
This supports substantial storage IO, without claiming exact whole-phase or
payload-only totals. Recoverable pinned-pool capacity/fallback messages remain
in the logs; successful exit does not mean those messages were absent.

Seven accepted GPU-generator cases, timing/memory/row-byte measurements and
source/artifact hashes are in [pcbs-shadow-performance.csv](pcbs-shadow-performance.csv).
The full scratch aggregate retains both invalid-shape fixtures separately.
Root rehashed eight frozen source/artifact checks after the final JVM exited;
all matched, including the unchanged original benchmark and baseline plugin.
Root also reconstructed all seven CSV rows from raw phase/partition/task events;
the audit passed. Final focused evidence and report reviews found no material
issue. Local results remain READY FOR REVIEW pending architect acceptance.

Evidence: `/data/pcbs-oom/pcbs-gpu-generator`, including the oracle, frozen v4/v5
source/jar provenance and per-case results/raw logs/independent analysis. The
allocation probe is under `/data/pcbs-oom/generation-allocation-probe`.
Literal zero-heap compressed spill remains an unresolved implementation/design
question, rather than a completed claim.

Native evidence: `/data/pcbs-oom/pcbs-shadow-native`, including frozen build and
artifact hashes, `entry-native-result.json`, v3 benchmark diff/provenance,
`cases/native-g1-slots8-capped2-heap49152m/{result,independent-analysis}.json`,
raw logs, event metrics and process-memory samples. Full source audit notes are
under `/data/pcbs-oom/ownership-research/design/` and the generator notes under
`/data/pcbs-oom/pcbs-shadow-sweep/`.

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

## Tested boundaries and production limits

The native cache path, 512 MiB heap, independent 2 GiB/automatic-GPU/16-thread
controls and combined stress passed. The exact minimum heap and a failing heap
boundary were not determined; intermediate heaps and the combined 48 GiB variant
were not run. The host limit stayed enabled at 40 GiB, pinned2/GPU15 GiB, memory
fraction0.6, unroll16/reset100. At 16 threads, 32 partitions provide two waves.
Actual 2 GiB records require three source partitions per cache partition plus
existing test-only spark.rapids.sql.test.overrides.splitUntilSize=3221225472,
explicitly propagated to task-local properties for direct fill. This is a
fixture adaptation, not a proposed production configuration.

The prototype intentionally never deletes published payloads. IDs are valid
only within one JVM, which exits after each test. No remote fetch, actual RDD
owner association, replication, retirement or missing-payload recovery is
implemented. See the separate metadata and lifecycle research. Any Spark
modification remains an explicitly labeled last resort; none is needed or
proposed for this local proof.
