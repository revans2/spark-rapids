# PCBS row-group production measurements

Status: IN PROGRESS. All fifteen synthetic JVM runs and all sixteen production
public-data cases are independently verified and READY FOR REVIEW. Decoded batch-
layout diagnostics and the new byte-only target sweep remain pending. These
measurements use the production logical-buffer reader, separately from the older
research multi-file prototype.

## Repeated synthetic comparison

15 fresh JVMs: default100/16 and baseline known1/1 remain separate. Each JVM read median uses three reads after one warmup; aggregate ranges span fresh JVMs. OOMs have no successful-read median. Heap/RSS are observed maxima, not live-set requirements. No profiler or data/control change;96GiB values,16 partitions,8 task slots,48GiB heap.

| Profile | Storage | Arm | PASS/OOM | Fill median [range] s | Read median [range] s | Read JVMs |
|---|---|---|---|---:|---:|---:|
| default | DISK_ONLY | baseline | 3/0 | 171.507 [167.364, 186.661] | 49.030 [48.677, 53.764] | 3 |
| default | DISK_ONLY | patched | 3/0 | 164.026 [149.797, 186.312] | 46.849 [44.860, 70.240] | 3 |
| default | MEMORY_AND_DISK | baseline | 0/3 | 156.450 [150.251, 164.011] |  | 0 |
| default | MEMORY_AND_DISK | patched | 3/0 | 157.327 [143.255, 181.988] | 41.013 [37.065, 45.764] | 3 |
| known-1-1-workaround | MEMORY_AND_DISK | baseline | 3/0 | 147.397 [135.590, 161.678] | 38.943 [38.414, 39.926] | 3 |

Per-JVM measurements (full IO, GC, residency/layout and strict provenance in JSON/CSV):

| Profile | Storage | Arm | Repeat | Outcome | Fill s | Warmup s | Read s | Heap GiB | RSS GiB | Records | Max record MiB |
|---|---|---|---:|---|---:|---:|---:|---:|---:|---:|---:|
| default | DISK_ONLY | baseline | 1 | PASS | 186.661 | 53.323 | 53.764 | 44.286 | 51.663 | 112 | 895.936 |
| default | DISK_ONLY | baseline | 2 | PASS | 171.507 | 58.918 | 49.030 | 43.753 | 50.180 | 112 | 895.936 |
| default | DISK_ONLY | baseline | 3 | PASS | 167.364 | 67.633 | 48.677 | 44.515 | 51.840 | 112 | 895.936 |
| default | DISK_ONLY | patched | 1 | PASS | 164.026 | 79.399 | 70.240 | 18.054 | 23.109 | 12400 | 7.938 |
| default | DISK_ONLY | patched | 2 | PASS | 186.312 | 47.659 | 46.849 | 15.655 | 21.659 | 12400 | 7.938 |
| default | DISK_ONLY | patched | 3 | PASS | 149.797 | 47.204 | 44.860 | 18.470 | 24.033 | 12400 | 7.938 |
| default | MEMORY_AND_DISK | baseline | 1 | HEAP_OOM | 164.011 |  |  | 45.248 | 51.390 | 112 | 895.936 |
| default | MEMORY_AND_DISK | baseline | 2 | HEAP_OOM | 150.251 |  |  | 45.235 | 51.407 | 112 | 895.936 |
| default | MEMORY_AND_DISK | baseline | 3 | HEAP_OOM | 156.450 |  |  | 45.241 | 51.416 | 112 | 895.936 |
| default | MEMORY_AND_DISK | patched | 1 | PASS | 181.988 | 60.319 | 45.764 | 38.696 | 53.007 | 12400 | 7.938 |
| default | MEMORY_AND_DISK | patched | 2 | PASS | 157.327 | 42.927 | 41.013 | 38.106 | 52.682 | 12400 | 7.938 |
| default | MEMORY_AND_DISK | patched | 3 | PASS | 143.255 | 52.757 | 37.065 | 38.172 | 52.957 | 12400 | 7.938 |
| known-1-1-workaround | MEMORY_AND_DISK | baseline | 1 | PASS | 161.678 | 42.058 | 39.926 | 45.241 | 51.432 | 112 | 895.936 |
| known-1-1-workaround | MEMORY_AND_DISK | baseline | 2 | PASS | 147.397 | 39.372 | 38.943 | 45.220 | 51.424 | 112 | 895.936 |
| known-1-1-workaround | MEMORY_AND_DISK | baseline | 3 | PASS | 135.590 | 51.802 | 38.414 | 45.239 | 51.437 | 112 | 895.936 |

Statistics report n explicitly; fill may exist in a subsequently failed JVM. No partial-OOM read time is imputed. Missing metrics remain unknown, never zero. Observed heap includes warmup. GC totals cover only reported phases. Per-phase sampled IO deltas have boundary gaps and differ from exact file sizes. Canonical storage figures are asynchronous RDDStorageInfo estimates, not physical block files. Counterbalanced order does not control page-cache state; no causal conclusion follows. The retained originals are unchanged; only strict audited outcome/issues normalization is used.



All arms above use stock Java serialization. The patched artifact is c385525a51,
with 8 MiB uncompressed row groups and the earlier 65,536-row limit. These are
not measurements of the new default-row-limit candidate or the 16/32 MiB targets.

At MEMORY_AND_DISK the patch passes at defaults in all three JVMs. Relative to
the baseline 1/1 workaround, its median fill is about 7% slower and read median
about 5% slower. Its observed heap peaks are lower, but RSS is slightly higher:
52.68–53.01 GiB versus 51.42–51.44 GiB. These are whole-run observed peaks, not
live-set requirements. The default baseline completes fill but fails readback in
all three JVMs; its fill timings remain valid completed-phase measurements.

The initial disk read slowdown does not hold across repeats. Patched read medians
range from 44.86 to 70.24 seconds. The initial patched reads sampled about 99 GB
of physical IO each; the second pair sampled zero during the measured intervals
in both arms. Host memory is 188.52 GiB, actual input files total 96.052 GiB and
encoded cache payload is about 96.04 GiB. Their sum exceeds host capacity before
JVM/native/other memory, but does not establish simultaneous residency. Later
samples show file refaults and cache turnover despite nearly unchanged aggregate
Cached bytes. Page-cache state remains a material confound; these medians do not
establish a universal disk speedup or regression.

The task metric gpuTime is host elapsed time while holding the GPU semaphore,
not CUDA kernel time. Root checked GpuTaskMetrics and GpuSemaphore definitions:
PCBS acquires before reader.next(), which can stage/copy/read host data under the
lease. GPU wait/holding totals overlap across tasks and cannot alone establish
GPU compute saturation. Retry/spill updates are absent in the eight inspected
completed cases; missing updates remain unknown, not proven zero.

Root independently replayed all original verifiers and the exact approved
Java-options correction: 12 PASS / three genuine HEAP_OOM, identical metrics,
16 actual input files and 180 raw evidence files. Initial and final content
checks each verified 106,043,786,691 bytes; replay took 140.391 seconds. CSV/JSON/
Markdown exports: ../canonical-repeat-queue/report001/summary.{csv,json,md}.
Full replay: ../canonical-repeat-queue/reverified001/result.json; independent
closure/metric checks: ../support-audit/root-repeat-final-checks001.json.
OS analysis was rerun after sampler closure with an unchanged source hash;
its original helper retains its provisional label. These checks do not supply
missing OS history for the first pair or a causal GPU profile.

## First matched cases (historical single-JVM comparison)

| Implementation | Storage | Fill (s) | Warmup (s) | Measured-read median (s) | Observed heap peak (GiB) | Result |
|---|---|---:|---:|---:|---:|---|
| Existing PCBS | MEMORY_AND_DISK | 164.01 | OOM | — | 45.25 | HEAP_OOM |
| Row-group PCBS patch | MEMORY_AND_DISK | 181.99 | 60.32 | 45.76 | 38.70 | PASS |
| Existing PCBS | DISK_ONLY | 186.66 | 53.32 | 53.76 | 44.29 | PASS |
| Row-group PCBS patch | DISK_ONLY | 164.03 | 79.40 | 70.24 | 18.05 | PASS |

One fresh JVM per row in this initial comparison. The patched measured reads were 45.764, 47.327 and
40.242 seconds; these are three reads in one JVM, not three independent repeats.
All four patched read row counts and sixteen column sums match the frozen oracle
within the existing 1e-10 relative tolerance. No patched Spark task failed or required another task attempt.
The baseline failed its first read after 16.218 seconds in
ObjectInputStream.readArray → JavaDeserializationStream → MemoryStore.putIterator
→ BlockManager.maybeCacheDiskValuesInMemory. That failure duration is not a
successful read timing. Its fill completed normally.

The patch's first fill is about 11% slower. The baseline has no successful read
median at these defaults, so this pair does not establish a read-speed ratio.
The disk-only pair passed all four read oracles without failed tasks. Baseline
measured reads were 63.050, 53.764 and 51.773 seconds; patched reads were 70.326,
70.240 and 70.191 seconds. Patched fill was about 12% faster, measured-read median
about 31% slower, and observed peak heap about 59% lower in this one pair. The
read regression needs investigation and independent repeats before attributing
it to an implementation bottleneck or making a general performance claim.
Patched measured reads each performed about 99 GB of physical reads, versus
43.6 GB, 164 KB and zero for baseline. Logical reads were about 102 GB in both.
Patched per-read global GC time was 0.37–0.51 seconds versus 5.76–8.55 seconds
for baseline. These counters do not identify which files caused the physical
reads, and do not separate format overhead from page-cache or spill behavior.
Patched read RSS peaks were also lower (22.4–23.1 GiB versus 50.1–51.7 GiB),
so the samples do not support increased process RSS as the explanation. System
page-cache/pressure history was not recorded in these first four cases.

Observed heap includes sampled occupancy and logged pre-GC occupancy across fill,
warmup and reads. It includes garbage; it is not a live-set or minimum-heap measure.
Mixed storage residency differs between runs and cannot isolate serializer costs.

## Controls and actual stored layout

All four cases use exactly the same immutable 96 GiB of double values: 805,306,368 rows,
sixteen columns and sixteen 6 GiB partitions. There are eight local task slots,
a 48 GiB initial/maximum heap, Parallel GC with four GC threads, the 1 GiB GPU batch
target, a 15 GiB managed GPU pool, two concurrent GPU tasks, a 40 GiB managed host
limit and a 2 GiB pinned pool. Java stream reset remains 100, MemoryStore unroll
check period remains 16, memory fraction remains 0.6, RDD compression is disabled,
and cache Parquet compression is SNAPPY. No forced GC or control tuning was added.

Hardware is the sandbox RTX A6000. Its managed pool is capped at 15 GiB; its
compute performance is not representative of a customer T4. No filesystem cache
flush is performed, and warmup is excluded from the read median. Physical reads
still vary between measured phases; warmup does not establish a fully warm page
cache. Per-phase physical/logical I/O counters are retained. Input checksums run
before timed phases. Fill traverses cached
records and can include disk readback or cache repopulation; that definition is
identical in both arms.

The encoder layout also changes between implementations:

| Writer setting | Existing PCBS | Row-group patch |
|---|---:|---:|
| Row-group uncompressed-byte target | 128 MiB | 8 MiB |
| Row-group row limit | 1,000,000 | 65,536 |

Existing PCBS leaves these options unset in the cuDF Java builder. The exact
frozen Java Builder class initializes 128 MiB / 1,000,000 rows, and JNI explicitly
passes those values to native cuDF. Native C++ defaults differ: unlimited
row-group bytes and 1,000,000 rows. The byte target is based on uncompressed
row-group data, not the compressed Parquet file length or GPU batch allocation.
The byte target does not impose a hard compressed-file-size limit. A whole
existing cache record can contain several row groups and is much larger than one row group.

Thus the patch does not merely split the original baseline file after writing.
It changes row-group layout before encoding, then copies encoded column chunks
into independent files without decoding/reencoding them. Current comparisons
combine encoder-layout changes, splitting, staging and combined/chunked reading;
they do not isolate post-processing overhead. Dictionary/page encoding and
compression can change even though the input values and Parquet codec match.
Exact default evidence, independently inspected by root without loading a JVM:
support-audit/java-writer-defaults-root001.json. Matching native defaults and
JNI setters are in the retained cuDF source under the evidence root's sibling
pcbs-rowgroup-stitch-spike/native-prerequisites/cudf-source.

| Implementation | Encoded Parquet bytes | Stored records | Smallest record (bytes) | Largest record (bytes) |
|---|---:|---:|---:|---:|
| Existing PCBS | 103,100,327,376 | 112 | 807,025,755 | 939,457,451 |
| Row-group PCBS patch | 103,126,054,960 | 12,400 | 7,428,469 | 8,323,781 |

Actual baseline layout is seven records per partition, not six: 1 GiB is the
configured GPU target. The patch stores 775 row-group records per partition
without reducing that GPU target. These byte counts sum actual Parquet array
lengths, including their file framing; they exclude Java serialization framing.
They are not physical cache-file sizes or GPU residency. The 8 MiB writer target
is soft, and this workload does not establish a bound for large rows or strings.
Actual decoded GPU batch sizes still need the separate layout diagnostic.

## Production public-data measurements

All sixteen cases completed: fifteen PASS, and existing PCBS filled its GHArchive
cache at 8 GiB before a genuine Java heap OOM during its first read. The patch
passed that case. Both implementations retain ordinary Spark Java serialization;
this is the production row-group patch, not the research codec.

Controls match the synthetic cases except for the listed heap and dataset:
eight local slots, sixteen immutable input partitions, the same 1 GiB GPU target,
15 GiB managed GPU pool, two concurrent GPU tasks, 40 GiB managed host limit,
2 GiB pinned pool, default reset100/unroll16, memory fraction0.6 and SNAPPY.
Timestamp inference uses inferTimestampNTZ=false and UTC in both arms and their
CPU-derived oracles. GHArchive contains 32,078,549 rows / 91.095 GiB semantic
values; TLC contains 239,470,448 rows / 31.108 GiB. Semantic sizes exclude offsets,
null masks and padding and do not measure GPU allocations.

Each cell below is existing PCBS → patched PCBS. Read is the median of three
within-JVM reads after a separate warmup; each arm has one fresh JVM.

| Dataset | Heap GiB | Storage | Fill (s) | Read (s) | Observed heap GiB |
|---|---:|---|---:|---:|---:|
| GHArchive | 48 | DISK_ONLY | 48.72 → 46.77 | 15.33 → 15.05 | 20.98 → 16.00 |
| GHArchive | 48 | MEMORY_AND_DISK | 32.93 → 32.62 | 6.07 → 7.16 | 25.33 → 27.66 |
| GHArchive | 8 | DISK_ONLY | 51.76 → 44.10 | 18.07 → 15.76 | 7.44 → 2.98 |
| GHArchive | 8 | MEMORY_AND_DISK | 50.30 → 47.47 | OOM → 16.41 | 7.44 → 6.42 |
| TLC | 48 | DISK_ONLY | 18.23 → 18.54 | 4.96 → 6.20 | 16.00 → 15.99 |
| TLC | 48 | MEMORY_AND_DISK | 12.71 → 13.89 | 3.55 → 4.64 | 12.06 → 12.70 |
| TLC | 8 | DISK_ONLY | 17.25 → 16.50 | 5.20 → 6.26 | 7.04 → 2.79 |
| TLC | 8 | MEMORY_AND_DISK | 14.37 → 14.66 | 3.89 → 5.52 | 7.31 → 5.74 |

The patch avoids the observed OOM and reduces heap pressure in the small-heap
disk cases. The benefit is conditional: TLC reads are 20–42% slower; GHArchive
at 48 GiB with MEMORY_AND_DISK reads 18% slower with 9% higher observed heap.
TLC's corresponding heap peak is 5% higher. Fill differences range from 15%
faster to 9% slower. Observed occupancy includes garbage and does not establish
the minimum heap or a live-set requirement.

| Dataset | Parquet payload GiB, existing → patch | Payload / semantic bytes | Records | Maximum record MiB |
|---|---:|---:|---:|---:|
| GHArchive | 23.512 → 23.336 | 25.81% → 25.62% | 128 → 11,722 | 258.1 → 3.90 |
| TLC | 5.755 → 6.265 | 18.50% → 20.14% | 52 → 5,346 | about 153.2 → 1.49 |

TLC's payload is about 8.9% larger. These are complete Parquet array lengths,
including framing. Captured Java-serialized block-file lengths are reported
separately in the full table, only when all sixteen partition files exist at one
checkpoint. Partial mixed residency cannot provide a whole-cache file ratio.
Spark MemoryStore estimates are also distinct: GHArchive's patched 48 GiB
mixed case reports about 22.032 GiB while arrays total 23.336 GiB. Changed Parquet
layout, encoding and framing affect size; this is not a comparison of generic
Java compression algorithms.

The runs do not isolate the cause of read regressions. More records introduce
metadata and input-segment work, while compression, residency and filesystem
cache also differ. Warmup does not equalize the page cache. No forced GC, cache
flush or memory-fraction tuning was added. Earlier CPU-default and custom-codec
measurements remain separate historical fixture/runtime cohorts. CPU oracles
here are correctness controls, not CPU cache-performance measurements.

The full table, including warmup, RSS, record ranges, payload ratios and actual
storage estimates, is retained at world-queue/complete001.{md,csv,json} under the
evidence root. Root independently checked every scalar against raw phase,
record, GC and CPU-oracle markers in support-audit/world-complete-raw001.json.
Root then reran all sixteen unchanged original verifiers, obtaining exact
retained-result equality, and hashed all frozen artifacts, 32 actual input
files and 192 raw-evidence files. An independent final content rehash also
passed: world-queue/reverified001/result.json. No OOM timing was converted to
a successful read median. These are single-JVM measurements, not independent
run-to-run confidence intervals.

## Evidence and verification

Production sources are the implementation in commit c385525a51; subsequent
integration-test and documentation commits do not change that implementation.
Baseline source is 60c28e44f0. Both use the same frozen Spark 3.5.5, JNI/private
runtime and benchmark harness. Spark 3.5.5 identifies this measurement runtime;
the patch changes RAPIDS only and must preserve all supported Spark contracts.
See the [compatibility audit](pcbs-rowgroup-compatibility.md).

Evidence root: `/data/pcbs-oom/pcbs-rowgroup-production/`.

- `runtime-matched001/artifacts.json`: pinned runtime and source/class-byte audits.
- `production-runs/canonical-{baseline,patched}-memory-disk001/`: original commands,
  logs, process reports, phase metrics, event logs and original result JSON.
- `production-runs/canonical-{baseline,patched}-memory-disk001-verified001.json`:
  strict separate verification envelopes; root executed both successfully.
- `production-runs/canonical-first-pair001.json`: explicit audited memory pair.
- `production-runs/canonical-{baseline,patched}-disk001/`: original disk-only cases.
- `canonical-queue/remaining-default002/case{0,1}-java-options-audit.json`: strict
  disk-only verification envelopes, both PASS with unchanged metrics.
- `production-runs/canonical-first-four001.json`: explicit four-case comparison.
- `support-audit/first-pair-{scalar,gc-oracle}-audit.json`: independent agent
  verification of raw phase values, heap peaks and all four patched read oracles.
- `support-audit/disk-pair-{scalar-gc-oracle,phase-bottlenecks,phase-bottlenecks-brief}001.json`:
  independent raw disk-pair verification and I/O/CPU/GC evidence.

The original verifier reports ERROR solely because stock SparkContext prefixes
standard Java module/IPv6 options to the requested driver-options string. Those
original results and all frozen runners remain unchanged. The separately reviewed
verification sidecar first reproduces the original result, proves the exact known
prefix and actual JVM arguments, and delegates to the same original verifier with
only that one copied metadata field normalized. All metrics remain identical;
only classification/issues change. The baseline recheck is HEAP_OOM and the
patched recheck is PASS. This correction changes no benchmark execution or data.
