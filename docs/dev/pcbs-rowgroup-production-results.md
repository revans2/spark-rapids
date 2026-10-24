# PCBS row-group production measurements

Status: IN PROGRESS. All fifteen repeated synthetic JVM runs, sixteen production
public-data cases and the separate twelve-case byte-target screen are independently
verified and READY FOR REVIEW. All six GH8GiB byte-target cases and the decoded
batch-layout diagnostic pair are also independently verified. Root selected the
8 MiB/default-row policy as the production starting point, committed in fa29f4d90c;
final-source correctness and compatibility remain open. The separate TLC diagnostic
pair is closed and its analysis is READY FOR REVIEW, not causal performance proof.
These measurements use the production logical-buffer reader, separately from the
older research multi-file prototype. The separate 20-case outer-compression
comparison is also complete, independently replayed and READY FOR REVIEW.

## Why stored record size matters

Spark's default safeguards count records rather than bytes. In the inspected
3.5.5 MemoryStore loop, unroll period16 checks after records1,17,33 and so on,
after the object has already been materialized. The check belongs to one
putIterator operation; it is not a global allocation limit. Java reset100 counts
top-level writes in one serialization stream. The reader consumes those reset
markers later; neither setting bounds a nested array allocation in advance.
The unrolled prefix and Java handles can reference the same arrays, so their
sizes must not simply be added.

For illustration, six distinct 1 GiB arrays per partition can reach 48 GiB of
payload across eight tasks before considering other heap users. There is no
periodic reset within those six records, and no second periodic unroll check
before the final reservation. This is a possible-overlap example, not the
measured canonical layout or a universal heap formula. If actual arrays are
8 MiB instead, sixteen new arrays represent 128 MiB and a hundred-array interval
800 MiB. Actual encoded array length is what matters here, not the uncompressed
row-group target or decoded GPU batch size.

Smaller records make those count-based delays less costly in bytes, while adding
footer, object and transfer work. Reassembling them for GPU decoding separates
stored record size from the GPU processing target. Outer Spark compression changes
serialized storage bytes; decompression still restores the same array length
before PCBS decoding. It can change IO and CPU cost without shrinking the
materialized Parquet arrays. This source explanation is not a fitted memory model
or a measured bottleneck claim. Decisive source and full qualifications:
../support-audit/record-budget-explanation001.md. Root independently read the
MemoryStore, JavaSerializer, SerializerManager and NextIterator paths.

## Completed outer Spark compression comparison

All 20 cases PASS and are READY FOR REVIEW. Root replayed every original verifier
with exact retained-result equality and all four CPU-oracle read checks. The
closed replay covers 32 immutable input files and 240 raw evidence files, with
20,533,013,314 content bytes checked initially and rehashed at the end.
Evidence: [accepted replay](/data/pcbs-oom/pcbs-rowgroup-production/compression-queue/reverified001/result.json).

Each case is a fresh JVM over the full GHArchive dataset (32,078,549 rows) or TLC
dataset (239,470,448 rows), using sixteen fixed partitions, a 48 GiB heap and
DISK_ONLY. Baseline is original PCBS `60c28e44f0`; patched is selected byte8/default
rows `d9ba8787f6`, whose unchanged final-source behavior is recorded separately
in the source-equivalence receipt. Runtime policy remains `fa29f4d90c`; the
adjacent-view prototype is excluded. All arms retain local eight slots, a 1 GiB
GPU batch target, 15 GiB GPU pool, concurrent GPU tasks two, host limit 40 GiB,
pinned pool 2 GiB, reset100/unroll16 and memory fraction 0.6. Inner PCBS Parquet
compression remains SNAPPY.

OFF disables RDD-cache compression only and explicitly retains the global LZ4
codec. The other profiles enable RDD compression and select the named global
codec. That codec also applies to task broadcasts, SQL result collection and
applicable shuffle/spill. Timings therefore compare full-query settings rather
than isolated cache-codec costs; the file ratios describe captured cache files
specifically. No executable control or default changes follow from this screen.

Times below are seconds. Median read excludes the separate warmup and uses three
within-JVM reads. Heap is maximum observed phase occupancy, including garbage,
not a live set or minimum heap. GiB uses 2^30 bytes. Payload sums complete encoded
Parquet arrays; file size sums the complete Java-serialized cache files for all
sixteen partitions. File/payload includes Java framing and outer compression.

| Dataset | Arm | Outer profile | Fill s | Warmup s | Median read s | Heap GiB | Payload GiB | Full file GiB | File/payload |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| GHArchive | baseline | OFF | 48.721 | 16.694 | 15.645 | 19.984 | 23.512 | 23.512 | 1.0000002 |
| GHArchive | patched | OFF | 46.500 | 16.064 | 15.407 | 16.311 | 23.336 | 23.336 | 1.0000099 |
| GHArchive | patched | LZ4 | 52.193 | 14.781 | 13.808 | 16.157 | 23.336 | 23.015 | 0.9862549 |
| GHArchive | baseline | LZ4 | 54.949 | 14.567 | 14.131 | 38.505 | 23.512 | 23.177 | 0.9857279 |
| GHArchive | baseline | ZSTD | 70.013 | 20.885 | 19.373 | 43.734 | 23.512 | 20.085 | 0.8542123 |
| GHArchive | patched | ZSTD | 67.309 | 20.021 | 19.337 | 16.651 | 23.336 | 20.039 | 0.8587306 |
| GHArchive | patched | SNAPPY | 43.066 | 12.693 | 11.390 | 15.841 | 23.336 | 23.301 | 0.9984988 |
| GHArchive | baseline | SNAPPY | 44.904 | 12.610 | 12.305 | 32.658 | 23.512 | 23.475 | 0.9983974 |
| GHArchive | baseline | LZF | 149.014 | 18.053 | 17.386 | 21.741 | 23.512 | 23.095 | 0.9822343 |
| GHArchive | patched | LZF | 128.443 | 18.966 | 18.253 | 16.167 | 23.336 | 22.921 | 0.9822236 |
| TLC | baseline | OFF | 18.625 | 5.908 | 4.905 | 15.685 | 5.755 | 5.755 | 1.0000004 |
| TLC | patched | OFF | 18.137 | 7.771 | 6.468 | 15.950 | 6.265 | 6.265 | 1.0000168 |
| TLC | patched | LZ4 | 19.218 | 7.165 | 6.085 | 15.943 | 6.265 | 6.221 | 0.9930387 |
| TLC | baseline | LZ4 | 18.907 | 5.316 | 4.759 | 15.957 | 5.755 | 5.736 | 0.9965493 |
| TLC | baseline | ZSTD | 20.868 | 5.913 | 5.036 | 15.547 | 5.755 | 5.413 | 0.9406200 |
| TLC | patched | ZSTD | 21.089 | 7.751 | 7.188 | 15.947 | 6.265 | 6.034 | 0.9631437 |
| TLC | patched | SNAPPY | 16.864 | 6.492 | 5.631 | 15.960 | 6.265 | 6.261 | 0.9994265 |
| TLC | baseline | SNAPPY | 17.389 | 5.361 | 4.383 | 15.914 | 5.755 | 5.754 | 0.9997811 |
| TLC | baseline | LZF | 40.378 | 5.133 | 4.442 | 15.363 | 5.755 | 5.723 | 0.9943702 |
| TLC | patched | LZF | 42.479 | 6.925 | 5.687 | 15.998 | 6.265 | 6.240 | 0.9959904 |

Full precision and all three individual reads are retained in
[CSV](/data/pcbs-oom/pcbs-rowgroup-production/compression-queue/report001/summary.csv)
and [JSON](/data/pcbs-oom/pcbs-rowgroup-production/compression-queue/report001/summary.json).
The JSON also retains per-phase IO windows, GC observations, storage checkpoints,
record counts and exact source/artifact identities.

SNAPPY versus OFF reduced the observed read median by 21.35% for GH baseline,
26.07% for GH patched, 10.64% for TLC baseline and 12.94% for TLC patched. Fill was
6.64–7.84% lower across those four comparisons. Yet SNAPPY cache files were only
0.022–0.160% smaller than their own Parquet payloads. These measurements do not
establish that cache-file reduction caused the timing change. All measured
OFF/SNAPPY read sample windows report zero physical `read_bytes`; those windows
cover only part of each phase and are distinct from logical `rchar`.

The same-codec comparison still retains the TLC regression: under SNAPPY,
patched reads took 5.631 s versus baseline 4.383 s (28.48% longer), while patched
GH reads took 11.390 s versus baseline 12.305 s (7.43% lower). TLC patched payload
was 8.85% larger than baseline under that profile.

ZSTD produced the smallest cache files, about 14.1–14.6% below the Parquet payload
on GH and 3.7–5.9% below it on TLC, but its fill and read medians were higher than
OFF in every arm. LZF fill took 128.443–149.014 s on GH and 40.378–42.479 s on TLC,
versus OFF 46.500–48.721 s and 18.137–18.625 s respectively. Smaller files alone
did not predict lower elapsed time in this screen.

There is one JVM per cell, not independent repeated runs; the three reads within
a JVM do not estimate between-run variability. Run order, page-cache/system
history, physical IO coverage and global codec consumers remain confounds.
Existing task spill/retry counters and operator metrics can help separate
follow-up hypotheses, but were not used here to assign a bottleneck. No causal
claim, universal winner, new default or compression recommendation is established.

## Verified byte-only target screen

All twelve cases PASS with no failed Spark tasks. The eight canonical cases use
the same 96 GiB of semantic values (805,306,368 rows, sixteen columns and sixteen
partitions); the four TLC cases retain all 239,470,448 rows and sixteen immutable
input files, totaling 33,401,767,650 semantic value bytes. Every case uses a
48 GiB heap and the unchanged query/resource controls: local eight slots, GPU
target 1 GiB, managed pool 15 GiB, concurrent GPU tasks two, managed host 40 GiB,
pinned pool 2 GiB, memory fraction 0.6 and Java reset100/unroll16. Inner Parquet
compression is SNAPPY and outer Spark RDD compression is disabled.

The old control is the frozen c385 patch with an 8 MiB uncompressed-byte target
and a 65,536-row override; it is not the original unsplit PCBS baseline. The new
byte8/16/32 candidates retain cuDF's default 1,000,000-row cap. Their byte targets
are soft uncompressed row-group targets, not compressed record sizes or GPU
allocation limits. Exact source identities are c385525a51 (old control),
d9ba8787f6 (byte8), cebb9206d5 (byte16) and abec8b6da5 (byte32).

One fresh JVM per row; warmup is separate and the median uses the three listed
within-JVM reads. Times are seconds and heap is observed phase occupancy in GiB,
including garbage, not a live set or minimum requirement. Payload/value is
encoded Parquet-array bytes divided by semantic value bytes.

| Workload | Storage | Arm | Fill s | Warmup s | Read1 s | Read2 s | Read3 s | Median s | Heap GiB | Records | Payload/value |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Canonical | DISK_ONLY | old8/65k | 151.324 | 82.475 | 72.578 | 72.285 | 72.612 | 72.578 | 16.722 | 12,400 | 1.000454 |
| Canonical | DISK_ONLY | byte8/default rows | 149.122 | 81.608 | 72.074 | 71.345 | 72.135 | 72.074 | 16.187 | 12,400 | 1.000454 |
| Canonical | DISK_ONLY | byte16/default rows | 156.266 | 80.002 | 71.460 | 71.582 | 71.332 | 71.460 | 20.125 | 6,256 | 1.000320 |
| Canonical | DISK_ONLY | byte32/default rows | 154.756 | 83.946 | 65.727 | 50.624 | 50.362 | 50.624 | 39.504 | 3,184 | 1.000251 |
| Canonical | MEMORY_AND_DISK | old8/65k | 148.849 | 49.633 | 38.428 | 39.171 | 38.033 | 38.428 | 40.988 | 12,400 | 1.000454 |
| Canonical | MEMORY_AND_DISK | byte8/default rows | 161.659 | 41.779 | 38.043 | 39.793 | 38.262 | 38.262 | 40.106 | 12,400 | 1.000454 |
| Canonical | MEMORY_AND_DISK | byte16/default rows | 143.309 | 54.847 | 39.228 | 39.571 | 41.553 | 39.571 | 39.213 | 6,256 | 1.000320 |
| Canonical | MEMORY_AND_DISK | byte32/default rows | 164.830 | 41.463 | 41.463 | 38.679 | 40.973 | 40.973 | 43.610 | 3,184 | 1.000251 |
| TLC | DISK_ONLY | old8/65k | 18.320 | 7.637 | 5.850 | 6.663 | 5.892 | 5.892 | 15.903 | 5,346 | 0.201396 |
| TLC | DISK_ONLY | byte8/default rows | 18.091 | 7.808 | 5.686 | 6.673 | 6.630 | 6.630 | 15.947 | 5,346 | 0.201399 |
| TLC | DISK_ONLY | byte16/default rows | 16.985 | 6.612 | 5.633 | 5.726 | 6.100 | 5.726 | 15.859 | 2,561 | 0.190333 |
| TLC | DISK_ONLY | byte32/default rows | 16.993 | 6.269 | 5.543 | 5.432 | 5.558 | 5.543 | 15.640 | 1,244 | 0.185474 |

The canonical old8 and byte8 arms have identical encoded payload totals and record
counts in this screen. Byte16/32 reduce record count, but the byte32 DISK timing
cannot isolate a target-size benefit: its measured physical read samples are
69,986,586,624 bytes, zero and zero, versus roughly 100 GB on each measured read
in the preceding three DISK cases. Those are sampled process IO intervals, not
per-file residency or complete physical IO attribution. Most mixed-storage
measured reads also sampled zero physical read bytes; byte32's first sampled
about 1.62 GB. The closed system/cgroup sampler shows observations over these
intervals, not a causal explanation or equivalent page-cache state. It missed
the first fill and began partway through its warmup.

TLC byte16/32 have fewer records and smaller encoded payload ratios than byte8.
The inner codec stays SNAPPY, but row-group layout can change dictionary/page
encoding and compression. These runs combine that layout change with storage
and read overhead; they do not isolate Java serialization or prove that fewer
records alone improve performance. Actual whole Java-cache file lengths, where
all sixteen positive block files were captured, are separate from these payload
ratios. Canonical RDDStorageInfo memory/disk values remain estimates, not physical
file lengths or a known RAM-resident payload subset.

The six GH8GiB follow-up cases below are now independently verified. Root has
selected 8 MiB/default rows as the conservative production starting point, not a
universal performance winner. A single JVM per cell does not provide
independent-run confidence; physical IO/order/system history remain confounds.
Heavy downstream GPU work and native off-heap storage are not requirements of
this screen. No Spark-core change, Spark OFF_HEAP setting,
memory-fraction tuning or public configuration was added.

Evidence: ../byte-target-screen/report001/summary.{json,csv,md} retains full
precision, sampled RSS/heap, record ranges, exact payloads and storage estimates.
Root independently replayed all original canonical/world verifiers and the exact
approved canonical Java-options sidecars, requiring identical complete metrics
and row/sum or four-hash/null/value/schema CPU oracles. Initial and final content
checks each covered 114,309,769,065 bytes, including 32 actual input files and
144 raw-evidence files: ../byte-target-screen/reverified001/result.json.
Original canonical prefix-only ERROR envelopes remain unchanged; accepted PASS
classification comes from the strict separate correction, not a rerun or changed
metrics. ../byte-target-screen/render-check001.json confirms all twelve rows,
all 28 CSV fields and all 15 Markdown columns against that replay.
Final OS analysis: ../support-audit/root-byte-screen-os001.json.
These verified measurements are READY FOR REVIEW; selecting the initial policy
does not complete final-source correctness or supported-version acceptance.

## Verified full GHArchive at 8 GiB heap

All six cases PASS. Root independently replayed every unchanged original world
verifier, required exact retained-result equality and CPU-oracle checks, and
verified inputs/artifacts/raw evidence with initial and final real content hashes.
This follow-up keeps all 32,078,549 GHArchive rows, 97,812,713,394 semantic value
bytes and sixteen immutable input partitions. The full-row query, local eight
slots, 1 GiB GPU target, 15 GiB pool, concurrency two, host40/pinned2 GiB,
default reset100/unroll16, memory fraction0.6, inner SNAPPY and outer compression
OFF remain unchanged. The heap is 8 GiB; this is a separate cohort from the
48 GiB table above.

| Storage | Arm | Fill s | Warmup s | Read1 s | Read2 s | Read3 s | Median s | Heap GiB | Records | Payload/value |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| DISK_ONLY | byte8/default rows | 43.331 | 15.660 | 15.229 | 15.341 | 15.296 | 15.296 | 3.002 | 11,722 | 0.256166 |
| MEMORY_AND_DISK | byte8/default rows | 46.242 | 16.031 | 15.296 | 15.099 | 14.936 | 15.099 | 6.584 | 11,722 | 0.256166 |
| DISK_ONLY | byte16/default rows | 43.570 | 15.990 | 14.890 | 15.175 | 14.389 | 14.890 | 3.978 | 6,602 | 0.254405 |
| MEMORY_AND_DISK | byte16/default rows | 46.122 | 16.773 | 16.239 | 15.570 | 16.012 | 16.012 | 6.271 | 6,602 | 0.254404 |
| DISK_ONLY | byte32/default rows | 45.270 | 16.251 | 15.055 | 15.686 | 15.092 | 15.092 | 6.745 | 4,160 | 0.253064 |
| MEMORY_AND_DISK | byte32/default rows | 43.976 | 16.306 | 15.356 | 14.356 | 15.382 | 15.356 | 7.418 | 4,160 | 0.253065 |

Warmup is excluded from each three-read median. Heap is observed phase occupancy,
not live heap or a minimum requirement; RSS remains sampled in the full exports.
Encoded payloads vary slightly between storage cases even with identical values.
Payload/value describes complete encoded Parquet arrays, not physical IO or
whole Java-cache files. For all three mixed-storage cases the whole-cache
serialized-file total and its ratio are NULL because all sixteen positive disk
block files were not captured together. This is unknown full-cache file size,
not zero bytes or missing payload. Retained per-block residency, Spark memory/
disk estimates and actual file bytes remain separate evidence.

Root selects 8 MiB uncompressed row-group bytes with the default cuDF
1,000,000-row cap as the conservative production starting point. In these
single-JVM DISK cases, read medians remain near 15 seconds while byte32's observed
heap peak is 6.745 GiB versus byte8's 3.002 GiB. The canonical DISK screen also
observed 39.504 GiB for byte32 versus 16.187 GiB for byte8, although its read
comparison is physically IO-confounded. TLC's improved payload ratios at16/32
are real measured layout differences; they do not establish a universal optimum.
Neither a confidence interval nor a causal performance or heap-safety guarantee
follows from one JVM per cell.

The benchmark source identities remain d9ba8787f6, cebb9206d5 and abec8b6da5;
the selected 8 MiB policy is committed in fa29f4d90c without relabeling any
measured artifact as that commit. Remaining supported-version gates are in
progress. The separate TLC diagnostic pair below is closed and analyzed; its
profiled wall times are not primary performance evidence. The twenty-case outer Spark-compression comparison is
complete and independently replayed, as reported above. Native off-heap storage
remains optional, with no Spark-core or Spark OFF_HEAP change.

Evidence: ../gh8-byte-target-screen/reverified001/summary.{json,csv,md}. The JSON
retains the exact six-case original results, CPU-oracle proof, source identities,
per-phase memory/IO, per-block residency and before/final content-hash receipt.
These measurements are READY FOR REVIEW, separately from the older48-case
prototype and earlier CPU/custom-codec cohorts.

## TLC diagnostic transfer profile

Both separately profiled JVMs completed the full TLC query with exact original
world-verifier PASS replay, and both Nsight collectors and SQLite exports
completed successfully. This is the original unsplit PCBS baseline from
60c28e44f0 versus the measured byte8/default-row artifact d9ba8787f6, not the
c385 row-group control or the isolated coalescing prototype. They retain all
239,470,448 TLC rows, sixteen input files and the original all-column checksum
query at DISK_ONLY/48 GiB, local8, GPU target1 GiB/pool15 GiB/concurrency2,
host40/pinned2 GiB, default reset100/unroll16/fraction0.6 and inner SNAPPY/outer OFF.

The table shows exported pinned-source Host-to-Device transfer-event rows for
read3. Direction/source/destination labels come from the actual export enum tables.

| Actual artifact | Target JVM PID | Event rows | Bytes | Summed transfer duration (ns) |
|---|---:|---:|---:|---:|
| Original unsplit PCBS baseline | 579202 | 67,096 | 6,387,132,221 | 332,931,228 |
| byte8/default-row patch d9ba | 580827 | 196,954 | 7,135,673,140 | 523,016,220 |

Counts and bytes are identical across read0 warmup and reads1–3 within each arm;
their durations vary, and the durations above belong only to read3. Fill has
37,066 pinned Host-to-Device event rows in each arm. These are event rows, not
counts of physical DMA operations, CUDA API calls or logical host-buffer views.
The exported copyCount field is retained raw and unweighted; its batched-operation
meaning remains unresolved. No byte or duration total is multiplied by it.

Summed event durations can overlap; they are not read wall time, GPU utilization
or on-core CPU time. The analysis also retains runtime-name totals with non-null
callchain rows excluded, kernel-name totals, and explicit boundary-straddling and
outside-phase-start counts. Attribution uses event start in millisecond-anchored
phase windows, so boundary and overlapping-work limitations remain. CPU stack
and context-switch sampling were disabled. Profiled wall times are excluded from
the primary performance tables; one capture per arm, different encoded payloads,
physical IO/page-cache state and profiling overhead prevent a causal slowdown
claim. More transfer-event rows motivate investigation, not proof of the cause.

Evidence: ../selected-world-profiling/supervisor001 and
../selected-world-profiling/analysis001.json. The final analysis is
READY FOR REVIEW. Root checked the actual schema, session normalization and
sampled JVM identities, and independently matched the read3 pinned-transfer SQL
totals above. Initial and final content checks each verified 542,273,856 bytes
of analysis/trace/log/result/plan evidence. Both captures retain successful
collector/audit/export process closure and exact original benchmark-result
equality; no benchmark control or production runtime was changed by analysis.

The resulting adjacent-view experiment is isolated at 8d14ea52cd in
../contiguous-view-worktree. It preserves stored records and payload bytes while
merging only ordered physically contiguous extents within the same materialized
parent. Its 3.5.5 build passed all 55 Scala and nine Java tests. Root independently
replayed the reports and checked 3,110 frozen entries, 3,095 tracked regular
files, all 36 implementation-family class bytes and 237 private class bytes.
This is a verified isolated build, not all-version or final-source acceptance.
Main fa29f4d90c retains the selected 8 MiB policy and its separate 53-plus-nine
compatibility gate. The prototype is not promoted to main or substituted into
the earlier timing/profile cohorts. Its three-case bracket is below. The separate
outer-compression comparison is complete; remaining selected-source compatibility
gates are in progress.

## Adjacent-view prototype: matched TLC bracket

The isolated prototype8d14 was measured between two fresh-JVM current-policy
controls (d9ba), using the same full239,470,448-row TLC data, sixteen files and
query. All three retain DISK_ONLY/48 GiB/local8, GPU target1 GiB/pool15 GiB/
concurrency2, host40/pinned2 GiB, default reset100/unroll16/fraction0.6,
innerSNAPPY/outerOFF. No stored-record size, format or data-volume control changes
between these arms; adjacent-view coalescing changes internal host views only.

All three cases PASS. Times are seconds; heap is observed occupancy in GiB and
RSS is sampled GiB. Warmup is excluded from the three-read median.

| Artifact / order | Storage | Fill s | Warmup s | Read1 s | Read2 s | Read3 s | Median s | Heap GiB | RSS GiB | Encoded payload bytes | Records |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Current byte8 d9ba / before | DISK_ONLY | 17.963 | 6.924 | 5.771 | 7.027 | 6.604 | 6.604 | 15.957 | 19.138 | 6,727,149,448 | 5,346 |
| Adjacent-view8d14 | DISK_ONLY | 17.966 | 7.425 | 6.129 | 6.936 | 6.548 | 6.548 | 15.919 | 19.111 | 6,727,013,536 | 5,346 |
| Current byte8 d9ba / after | DISK_ONLY | 17.867 | 7.103 | 5.965 | 7.256 | 6.369 | 6.369 | 15.964 | 19.065 | 6,727,054,205 | 5,346 |

There is no clear gain: the prototype median lies between the before/after
controls. One prototype JVM and two bracketing controls do not establish
independent-run confidence or a causal result. Sampled physical read-byte deltas
are zero in measured read intervals for all three cases; sampling gaps and
unmeasured host/native work remain, and zero process IO is not file-residency
proof. Small encoded-payload differences are retained rather than relabeled as
exact byte identity. Occupancy is not live heap or minimum required memory.

Root independently replayed all three original world verifiers with entire
retained-result equality and the original four-hash/null/value/schema CPU
oracles; initial and final real content checks cover frozen inputs/artifacts and
raw evidence. Evidence: ../contiguous-view-screen/reverified001/summary.{json,csv,md}.
Per-phase IO, block residency, semantic sizes, exact file lengths and sources
remain in JSON; no old cohort or profiled timing is pooled here.

The single optimized capture in
../selected-world-profiling/contiguous-diagnostic001 is complete and analyzed
READY FOR REVIEW. It uses the same verified8d14 artifact and fullTLC/DISK48
controls. The table compares read3 with the earlier d9ba byte8 capture, not with
the original unsplit PCBS baseline. API columns count exported cudaMemcpyAsync
runtime events after the existing non-null-callchain exclusion.

| Actual artifact | Pinned H2D event rows | H2D bytes | Summed transfer ns | cudaMemcpyAsync events | Summed API elapsed ns |
|---|---:|---:|---:|---:|---:|
| Current byte8 d9ba, PID580827 | 196,954 | 7,135,673,140 | 523,016,220 | 245,724 | 920,050,434 |
| Adjacent-view8d14, PID593922 | 73,996 | 7,135,801,653 | 363,811,389 | 122,766 | 539,169,655 |

The optimized trace contains fewer actual transfer and runtime-API event rows,
rather than only fewer Java/native host-view objects. This does not equate
transfer rows with DMA operations or assign each event to a view. Bytes are
retained as measured, not forced equal; copyCount remains raw and unweighted.
Transfer and API elapsed sums can overlap with each other and other work, and
neither is wall time, utilization or on-core CPU time. The unprofiled bracket
above still shows no clear wall-time gain; a count reduction alone does not
establish that copies caused the regression or that the prototype is beneficial.

Root executed the separate read-only analyzer after independent source review,
checked exact process/sampled PID/source/result closure and reused the reviewed
epoch/schema/phase queries. Root independently matched read3 raw SQL totals.
Initial and final real content checks each verified 1,130,626,586 bytes.
Receipt: ../selected-world-profiling/contiguous-analysis001.json, retaining
exact units, raw copyCount fields, boundary/outside rows and proof hashes.
Profiled wall times remain diagnostic-only and are not pooled into the bracket.

## Host-side read work observed by JFR

Two separately labelled full-TLC diagnostics compare original PCBS with the
measured byte8/default-row artifact d9ba8787f6, using stock Java serialization,
DISK_ONLY and the same 48 GiB/resource/query controls. Both complete original
benchmark results and all four read oracles were replayed exactly, with sixteen
inputs and 9,590,376,664 content bytes checked initially and finally. Evidence:
`../jfr-host-profile/reverified001/result.json`. These are diagnostic JVMs,
separate from primary timings and the unpromoted contiguous-view prototype.

The corrected worker classification in `interpretation002.json` recognizes both
observed Spark worker-name forms; interpretation001 must not be used for this
cross-arm comparison. Timestamp-attributed worker allocation samples estimate:

| Read | Original PCBS: estimated allocated bytes | Byte8 d9: estimated allocated bytes |
|---|---:|---:|
| 1 | 6,454,458,144 | 8,011,877,520 |
| 2 | 6,346,770,952 | 7,961,649,504 |
| 3 | 6,434,683,176 | 7,900,105,208 |

That is approximately 5.91–6.01 GiB/read versus 7.36–7.46 GiB/read of represented
allocation, not live heap, required heap or exact allocated bytes. Baseline
recording began about thirteen seconds into fill; fill is excluded from this
comparison. Both recordings extend across the read windows, but selected-event
endpoints and absence of exported DataLoss events do not prove continuous
sampling or complete stacks.

Both arms still show the Java array-deserialization path. In read3, recorded
native-method stacks include
`FileInputStream.available0 → FileInputStream.available → BufferedInputStream.read
→ ObjectInputStream$PeekInputStream.read → BlockDataInputStream.readFully
→ ObjectInputStream.readArray` (leaf-to-caller direction, intermediate frames
omitted). The native leaf appears in five baseline and six byte8 worker samples;
those are samples, not syscall counts or time measurements. Byte8 also has
allocation/execution stacks through `ParquetCachedBatchFiles.describe`,
`logicalFile` and Parquet/Thrift footer reading/writing. For example, read3
allocation weights include 675,224,368 bytes on the describe path and
671,217,520 on the logicalFile path. These inclusive paths overlap and must not
be added. They identify extra host metadata/wrapper work as a candidate cost;
they do not assign a fraction of the wall-time regression to it.

The separate four-case GH task report records read3 task-thread CPU sums of
31.79/30.57 seconds with outer compression OFF (baseline/byte8), versus
25.18/23.16 seconds with SNAPPY. These are sums across seventeen tasks, not
phase wall time or all process CPU. The global compression codec also affects
task broadcasts, result collection and applicable shuffle/spill; OFF retains
global LZ4 while disabling RDD-cache compression. Therefore this comparison
does not isolate cache decompression or explain the change by file size alone.
RAPIDS spill/retry fields are absent for all seventeen read3 task reports in
these four cases: their values remain unknown, not measured zeros.

Samples are sparse and often truncated: read3 baseline execution has 12 samples
(10 truncated), and byte8 has 33 (30 truncated); byte8 allocation has 232 samples
(231 truncated). Native-method samples can include native waits and are not
native C++ backtraces. Allocation weights can represent allocations preceding
the event timestamp, including across phase boundaries. One diagnostic JVM per
arm and profiler perturbation preclude a causal or confidence claim. This is
evidence of observed paths and estimated allocation work, not exact CPU
percentages, GPU utilization or a demonstrated optimization.

Evidence:
`../jfr-host-profile/interpretation002.json` (initial/final content checks each
992,931,558 bytes);
`../support-audit/jfr-tlc-overhead-explanation001.md`;
`../support-audit/gh-compression-tasks001.json`.

The prototype remains isolated/unpromoted. Main fa29 and the reported original
runtime remain unchanged. The separate outer-compression comparison is complete;
remaining main-policy53-plus-nine supported-version gates remain open.

## Returned GPU batch sizes

A separate full-size diagnostic pair confirms small cached records still return
large GPU batches. Both paths produce 112 batches (seven per partition), with
805,306,368 rows and all 16 columns. The actual scan plans contain no separate
full-row coalescer; the patched reader itself combines stored records.

| Path | Batches | Represented buffer min bytes | Median bytes | Max bytes |
|---|---:|---:|---:|---:|
| Baseline | 112 | 819,467,520 | 953,942,144 | 953,942,144 |
| Row-group patch c385 | 112 | 109,667,584 | 1,072,241,792 | 1,072,241,792 |

The patched path has a smaller final batch per partition. These sizes represent
returned device buffers, including column overhead; they are not live residency,
allocation peaks or encoded storage sizes. Separate diagnostic actions add
instrumentation, so their timings are excluded from the performance comparison.
Root independently replayed both complete strict verifiers, checked actual task
identities, artifact origins and exact row/column contracts, required exact
retained-result equality, and verified final quiescence. Evidence:
../layout-queue/pair001 and ../support-audit/root-layout-pair001.json.

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

The earlier matched, repeated synthetic and sixteen-case public-data cohorts use
the implementation in commit c385525a51; subsequent integration-test and
documentation commits do not change that implementation. The separate byte-target
screen uses the four source identities listed in its section above.
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
