# GPU contiguous packing and nvCOMP exploration

Status: IN PROGRESS — predicate pruning authorized; column-pruning timing checkpoint parked at the permitted 2GiB heap blocker.
Source/provenance repairs and the corrected 256MiB/task cohort are READY FOR REVIEW.
Current-source CPU/GPU profiling is verified and READY FOR REVIEW below.
User-approved decoded batching and its impact reruns are READY FOR REVIEW below.
Configured-target batching and the 32-case size matrix are READY FOR REVIEW:
31 cases completed, uncompressed 2GiB/task failed in JVM heap population.
Direct multi-boundary GPU packing and its host-CPU/kernel profiles are verified
and READY FOR REVIEW below. CPU codec prerequisites are READY FOR REVIEW;
CPU row/packed cache integration and first 256MiB/task comparison are
READY FOR REVIEW below. CPU scaling at512/1024/2048MiB/task is verified and READY FOR REVIEW.
Column-pruning/predicate research and the isolated column-slice feasibility proof
are READY FOR REVIEW. No Spark serializer pruning implementation has landed.
Large-string performance and active GPU spill/retry validation remain pending.

IMPORTANT BASELINE CORRECTION: PCBS numbers before the resumed current-source
cohort section describe the older hash-pinned prebuilt jar, not this branch's
ParquetCachedBatchReader. The new cohort below explicitly fixes that mismatch.

Previous scope: REAL SPARK PROTOTYPE. User explicitly moved from the
codec/host round-trip microbenchmark to a real Spark cache serializer. CPU encode
and decode fallback MUST THROW for the new prototype (explicit user instruction).
The microbenchmark phase below is historical evidence, not Spark cache timings.
The original first step was GPU codec and compressed host round trips
before a full Spark cache serializer. The historical GPU-only scope below has
now been extended by the user to CPU versions; implementation status is at the end. GPU-only capabilities of current installed nvCOMP are the
subject; the redesigned Cascaded / future CPU support is WIP and unmeasured.
Results must be presented inline to the user, not replaced by links to reports.

## Authorized scope and resources

Use branch pcbs-rowgroup-production, starting b819090a37. The user authorized
returning to this branch after lending the checkout to another task. Preserve
unrelated untracked files, other branches and previous benchmark artifacts.
Use MCP docker_exec for EVERY project operation, worker handoff, resumption and
progress checkpoint. Each worker must confirm continued use. No host shell or
filesystem substitutes. Keep GPU jobs serial across agents; within each experiment
two task slots and four work items provide two waves.

Explicitly approved: JVM heap 16 GiB, RAPIDS-managed host off-heap budget 32 GiB
INCLUDING a 2 GiB pinned pool, separate sampled whole-process RSS stop guard
64 GiB. RSS is not an off-heap allocation budget or hard OS limit. The historical
4 GiB managed cap and 32 GiB RSS guard are obsolete for this experiment. Record
actual initialization/counters as well as command flags. Do not exhaust the box.
Park guard/OOM/resource failures without relaxing caps or silently changing scope.

First compare uncompressed contiguous packing, nvCOMP LZ4, Zstd and available
legacy Cascaded. Include flat packed-buffer Cascaded and a labeled type-aware
variant, preserving integer widths and accounting separately for masks/offsets/
byte data/padding. Use identical deterministic GPU inputs for every codec;
representative wide numeric, low-cardinality strings and varying strings.
Requested task-scale envelope is 256/512/1024/2048 MiB logical input, two slots/
four work items. Separate physical packed bytes from canonical logical bytes.
Use small correctness prerequisites before dependent larger/concurrent runs.
User explicitly requires enough independent blocks to occupy the GPU for LZ4/Zstd,
analogous to Parquet pages. The cuDF batched APIs already partition packed buffers
and submit all chunks in one native batch. Expose block size and actual block count
per native call; start with 64/256 KiB blocks (about 4096/1024 blocks per 256 MiB).
Do not conflate codec blocks with independently decodable row/cache batches.
Measure the block-size tradeoff rather than drawing conclusions from one block.
Do not add native GPU Vortex decoding, CPU codec support or production configs.

## Observable acceptance

- Round trips preserve every value/null/row/order and reconstruct cuDF tables
  from opaque packed layout metadata. Small cases include tails, all-null,
  strings and sliced views. Full byte verification can establish lossless packed
  payload reconstruction; table shape/types/values must also be checked.
- Report GPU fixture generation separately/outside timed encoding. Separately
  measure pack/split, compression including allocation/stitch overhead, D2H,
  retained host storage, H2D, decompression and unpack/reconstruction. Report
  codec-only and actual timed host round-trip encode/decode totals without
  substituting one for the other. Synchronization scope must be explicit.
- Report first/warmed samples and repeated timings, rows/logical bytes, packed
  bytes, actual compressed payload plus all metadata/framing, ratio to packed
  bytes and ratio to logical bytes, retained bytes and temporary allocations.
  Used output bytes and reserved capacity are separate.
- Verify heap/managed host/pinned/GPU memory and sampled RSS. Record whether
  validation/reference work contributes to whole-job peaks; never describe RSS
  as live codec scratch. Reuse count and incompressible expansion remain visible.
- Previous PCBS/Vortex Spark timings are context only: a standalone codec
  benchmark is not an actual Spark cache serializer comparison.
- Lead reads decisive code and independently verifies delegated implementation
  and raw outputs. Focused peer review precedes delivery. Commit completed tasks
  with tracker evidence; only architect marks DONE. Mark verified work READY FOR REVIEW.

## Release and compatibility classification

RELEASED: cuDF public packing/nvCOMP interfaces and plugin LZ4/Zstd table codecs,
shuffle IDs/layouts/configuration. Evidence: TableCompressionCodec,
NvcompLZ4CompressionCodec and NvcompZSTDCompressionCodec exist on local main;
main history includes 3956615705 for these paths. Preserve all production callers,
public APIs, IDs and stored-format contracts. No production source change planned.

UNRELEASED: new standalone benchmark, Cascaded bridge and experiment-only
descriptors. No published artifact or documented production configuration exposes
these additions. Change their interfaces directly for the simplest correct design;
no compatibility aliases/migrations. Prior Vortex prototype and branch PCBS
additions retain the classification in vortex-cache-exploration.md.

## Decisive discovery and prerequisites

Lead read TableCompressionCodec and both LZ4/Zstd codecs: existing shuffle path
compresses ContiguousTable GPU buffers and keeps opaque table metadata, then
decompresses exact-size buffers for Table.fromPackedTable. Reuse this mechanism
rather than replacing released serializers. Batched compressor consumes input
references, stitches outputs with chunk-size metadata and synchronously fetches
compressed lengths; account for that work. ChunkedPack streams a whole packed
table through a reusable bounce buffer but reconstruction requires its layout.

Installed cudf-spark-jni 26.12 runtime Java signatures differ from mounted cuDF
source: BatchedCompressor/Decompressor scratch-size callback has FOUR long
arguments (including stream). Inspect exact runtime javap before coding.
Runtime Java provides LZ4/Zstd but no Cascaded wrapper. Native Cascaded SDK/library
5.0.0.6 exists under /home/roberte/miniconda3/envs/spark-accel; use SDK headers/libs
only, not its Java/Python toolchain. Current native Cascaded is not the WIP V2.
Do not infer performance or compatibility for the redesign.

Library requires aligned pointers and valid compressed sizes/statuses. Check
alignment after output stitching; do not feed misaligned or type-width-invalid
segments. Cascaded chunk size <=16 MiB. Buffer ownership and native error cleanup
must remain correct on every failed stage. Source/runtime versions and native
dependencies must be recorded.

## Verified prerequisites — READY FOR REVIEW

Lead independently built the isolated Cascaded bridge with g++17/Werror and
javac/Xlint, then ran all 16 GPU byte-exact cases, including signed integer
extremes, real compression, odd tails and multiple blocks. A mixed INT32 case
produced compressed sizes [7948, 7792, 7660, 20], exercising two misaligned
starts. The final smoke explicitly asserts bounce coverage and checks zero live
RMM bytes. Rebuilt artifacts/provenance: /data/tmp/nvcomp-cache-probe/bridge-01.
Lead also compiled and ran GPU fixtures: fixed-wide/strings-low/strings-wide,
513-row known values/null patterns, exact string logical-size accounting,
pack/unpack/byte checks and all-null/all-valid/sliced tails all PASS.

Focused independent worker review checked exact runtime signatures, native SDK
alignment/type limits, status/length validation, exception synchronization and
non-owning packed-buffer views. No blocking native/fixture finding remains.
The reviewed decoder reserves worst-case RMM bounce capacity for ALL blocks;
this can approach another packed-input-sized allocation even with no copies.
Callback times include host descriptor copies/synchronization/status validation,
not pure codec kernel time. No native cudaMalloc is introduced.

## Harness progress — IN PROGRESS

A first flat-codec 513-row two-slot/four-record host round trip passed all four
codecs, original packed-byte prerequisites, regenerated full-table validation
and zero final RMM/HostAlloc live bytes. Exact initialized resources in
/data/tmp/nvcomp-cache-probe/smoke-flat-03/results.json are heap16/pinned2/
nonpinned30/managed32/device-pool-max24 GiB; all tiny retained payloads actually
used pinned storage. Classes: full-04. Compilation needed the existing raw
plugin dependency aggregator on the standalone classpath; production unchanged.
The first two tiny harness launches failed on this missing class before any
timing results and were corrected, without changing caps.

The lead is verifying the guarded runner; review found stale-artifact checking
and unexpected-launcher-error process cleanup gaps, both being repaired before
scaled runs. Image python3 is 3.6; use available python3.9 for the new runner.
Typed Cascaded integration follows this flat prerequisite. Large performance
matrix, type-aware host round trips and final review remain unverified. No
performance conclusion is supported by the tiny smoke.

Two 256 MiB/task numeric flat-codec PILOTS passed at 64/256 KiB blocks with
full-05 artifacts; directories pilot-flat-256-64k and pilot-flat-256-256k.
The lead's /data/tmp/nvcomp-cache-probe/summarize.py independently checks resource
values, all four records, framing-byte counts, retained-byte sums, stage/total
bounds, correctness flags and final allocation zero before computing medians.
At 64 KiB there were 4094 blocks/call; 256 KiB yielded 1024. These are not final
performance evidence: lead found untimed sibling generation can overlap encoding,
and sibling validation can overlap decoding. Worker is introducing two-item wave
barriers so both generations finish before either encode, and both timed decodes
finish before validation. Only two GPU source/result tables need coexist. This
corrects measurement isolation while retaining two slots/four items/two waves.
Do not silently mix pilot numbers with final staged cohorts.

Guard fixes now independently pass CPU-only injected failure tests, including
RSS crossing without real pressure, sampler exception, stale sources rejected
before launch, process-group termination and a SIGTERM-ignoring descendant.
Explicit ordered Spark jars plus source/compiled artifact/native SDK/runtime
hashes are checked. Independent final launcher review found no remaining material
finding after those fixes. Guarded runner: READY FOR REVIEW. Type-aware staged
harness and final performance matrix remain IN PROGRESS. Native/fixtures
prerequisite commit: 56450b834d.

## Staged harness — READY FOR REVIEW; larger matrix IN PROGRESS

Lead read and built the completed harness/descriptor code; independent reviews
covered ownership, serialized descriptor decoding, homogeneous grouped batching,
DMA exception synchronization, phase barriers and scoped RMM peak accounting.
All five codec variants passed 513-row original-packed-byte prerequisites for
three fixtures at both block sizes. Full-table reference validation is separate
and regenerated packed padding is deliberately not used as an equality oracle.

Initial mixed-width Cascaded batch decoding failed status12 even with independent
output allocations, although every segment decoded individually. A separate tiny
SDK probe passed homogeneous batches; mixed batches failed and a later permutation
triggered GPU illegal-address status, ending that diagnostic process. No OOM.
The diagnostic should have stopped after its first negative case. Headers do not
document a uniform-width restriction; record this as observed SDK5.0.0.6 behavior,
not a published contract. Minimal repair batches decode by width like encode,
with many blocks per call. Fresh-JVM grouped round trips pass. No further mixed
probes are planned. Layout/metadata and approved resource budgets are unchanged.

Lead fixed a fixture fairness issue: fixed-width ifElse null slots had unspecified
physical bytes that affected compressed size through allocator history. The new
view copies deterministic original data with the generated validity mask. Strings
keep ifElse's required empty-null layout; an initial generic view failed cuDF's
non-empty-null assertion and was corrected. A 65536-row independent GPU byte
comparison after perturbing allocator history passed data (including null slots),
offsets and live validity bits for all three fixtures. Packed padding identity is
not claimed. Edge checks independently passed all-null, sliced and empty strings
through all five codecs and both block sizes, including host round trips, exact
original packed bytes, table values and zero final RMM bytes.

First deterministic cohort artifacts: staged-typed-05 (superseded below). Six 256 MiB/task fixture/block cases
(matrix-{fixed,low,wide}-256-{64k,256k}) all PASS, each with one recorded warmup
and two measured repeats, four work items and two slots. Independent raw JSON
checks reproduce framing/retained-byte sums, actual pinning, all values, zero final
allocations, budget limits, and timestamp inequalities proving phase barriers.
The uncompressed baseline repeats across block sizes as a consistency control.
Do not mix earlier pilot/staged-typed-03 results with this deterministic cohort.
Source generation and validation peaks are distinguished from encode/decode GPU
live peaks using scoped RMM counters reset only at completed phase barriers.

512 and 1024 MiB/task are now also complete: all 12 additional fixture/block
cases PASS under staged-typed-05, with the same independent JSON phase, size,
allocation and budget checks. Total completed: 18 of 24 cases / 90 of 120 codec
configurations. No host/GPU OOM or RSS guard failure in the final cohort. At 1 GiB,
whole-run sampled RSS remains <=5.52 GiB and tracked live GPU peak <=11.52 GiB.
Some records now use pageable memory; report actual pinned fractions, not just
pool size. Whole-run RSS includes every codec and validation and cannot be
attributed to one codec. GPU phase peaks are separately attributable.

## Large-case park and bounded repair — IN PROGRESS

The first 2048 MiB fixed-wide/64 KiB case stopped at the lead-selected 24 GiB
GPU pool cap. A 3.632184 GiB BatchedCompressor workspace allocation failed;
the partial stdout contains completed earlier codec samples but the overall case
did not PASS. Whole-process RSS peaked at 8.47 GiB, below the 64 GiB guard;
32 GiB managed host did not exhaust. Pinned-pool 2 GiB allocation failures in
the same log are caught pageable fallbacks, distinct from fatal GPU workspace.
The GPU process exited and nvidia-smi confirmed idle. All remaining large cases
were stopped immediately; no cap was relaxed.

The original single whole-task native submission was insufficiently bounded.
Adapt the unreleased harness, preserving the agreed architecture and ALL caps:
split packed byte/type regions into <=256 MiB uncompressed segments and group
native submissions by width AND total uncompressed bytes <=256 MiB. Keep many
blocks per full call (4096 at 64 KiB,1024 at 256 KiB), never per-block calls.
Codec expansion is allowed and measured; do not invent a hard compressed-size
256 MiB rejection. Layout metadata still reconstructs one packed GPU allocation.
Flat codecs now also retain smaller segment payloads, making pinned allocation
granularity explicit. Untimed fixture/reference generation is serialized within
waves to avoid diagnostic temporaries themselves exhausting the GPU; timed
encode/decode remain two concurrent tasks/four work items/two waves.

This changes submission/payload granularity and the untimed workload. Rerun ALL
24 cases as a new consistently labeled cohort; do not mix timings/pinning from
staged-typed-05 with the bounded cohort. Worker owns PackedCodecBench.java and
PackedSegments.java during repair, MCP-only, no worker GPU jobs. Root independently
reviews/builds/smokes and verifies all new outputs before accepting the repair.
Native library, production serializers/configs and the approved memory settings
are unchanged. Source/header compatibility remains as classified above.

A focused 256 MiB Nsight run of the earlier immutable cohort completed:
profile-fixed-256-64k.nsys-rep / .sqlite. It is diagnostic only; profiler timings
are not primary benchmark samples. Lead is inspecting CUDA API/kernel/NVTX data.

Inspect failures before dependent work; no cap relaxation or host-OOM pressure
experiments. Report final results inline with encode, decode, stored size/ratios,
block-size tradeoffs and memory caveats. Full Spark serializer integration and
matched PCBS timings remain outside this first experiment.

## User-directed transition to real Spark — IN PROGRESS

User: "If we think we have a proof that this is going to work, can we move to
the more realistic work?" Then: "the CPU fallback should just be throw an
exception. We are not going to try and support it right now."
Remaining microbenchmark sweep STOPPED. Root terminated the orchestration and
confirmed no matching benchmark processes / no GPU compute processes remained.
Do not restart the full microbenchmark matrix without a new reason.

Bounded repair source reviewed independently and lead-verified through all 12
256/512 MiB fixture/block cases, all five codecs, original packed-byte small
prerequisites, full table reference checks, actual submission byte/rounded-block
limits, true two-task timer overlap, serial untimed phases, memory budgets,
framing/retained bytes and final zero live allocations. Artifacts bounded-02,
bounded-matrix-{fixed,low,wide}-{256,512}-{64k,256k}. These are COMPONENT timings,
not Spark population/read timings. Large bounded 1/2 GiB component cases were
not completed and remain unverified. Source READY FOR REVIEW; architect accepts.

Realistic prototype scope:
- Local Spark 3.5.5 / Scala 2.12, actual CachedBatchSerializer + real cache RDD,
  GPU encode/read plans, Spark storage accounting and unpersist. Reuse the
  existing Vortex experiment's concrete PCBS-wrapper subtype integration; do
  not modify the released production scan gate or PCBS behavior.
- New prototype CPU row encode, CPU row decode, CPU columnar decode, and CPU
  input batches throw clear unsupported-operation exceptions. Unsupported schema
  combinations fail explicitly. No CPU implementation/fallback is being built.
- Ordinary Serializable heap-byte-array cache records let Spark own retention,
  storage accounting, eviction and disk serialization, matching the existing
  experiment/PCBS model. Native HostMemoryBuffers are temporary managed transfers,
  not hidden retained cache allocations. Bound record sizes and incremental
  production to avoid partition-sized heap buffers.
- Small row packs (initial target about8 MiB) batched for GPU encode/decode to
  <=256 MiB actual AND chunk-rounded input per native call; enough independent
  blocks, homogeneous Cascaded widths. Preserve metadata/projection/row order.
- Same user-approved heap16/managed host32 inclpin2/RSS64 GiB; verify actual
  plugin initialization rather than inherit obsolete4GiB/32GiB runner defaults.
  No more codec microbenchmark matrices. Small correctness prerequisites, then
  matched real Spark population, full/projection cached reads, encoded payload,
  resident/disk storage and memory against PCBS/default Spark as applicable.
- GPU task semaphore, retry, resource ownership and cancellation cleanup follow
  existing code. Lead reads decisive implementation and independently validates;
  focused worker review and signed commits plus tracker updates required.

Release classification: all new Spark prototype classes/records/build/benchmark
interfaces are UNRELEASED, isolated tools, no compatibility aliases. Released
PCBS wrapper, GPU scan gate, plugin memory configuration and Spark contracts are
preserved. Existing Vortex integration is reused as a pattern, not replaced.
The shim skill was read for this boundary; tools-only Spark3.5.5 prototype
changes no production shared shim caller or source-selection/build matrix.
Reclassify/invoke full preflight if production/shared shim sources must change.

Current bounded handoffs, all MCP docker_exec ONLY:
bridge_harness: new multi-table PackedTableCodec GPU core/shared bounded plan.
vortex_native_build: new SparkNvcompSerializer.scala, every CPU path throws.
bridge_review: read-only decisive Spark lifecycle/budget discovery and final review.
Root: build/real Spark benchmark/guard, independent verification and tracker.

## Real Spark implementation checkpoint

Implementation READY FOR REVIEW; benchmark matrix IN PROGRESS. Architect alone
accepts. All operations and worker reviews continue through MCP docker_exec only.
Root independently read the multi-table codec, serializer, probe and launcher;
workers reviewed substantive sources and root's timing/accounting/build changes.
Production sources, configuration, POMs and shims remain untouched.

The isolated tools implement GPU packing, bounded multi-table codec submissions,
heap-byte-array CachedBatch records, temporary managed host transfers, retry/task
cleanup and real Spark cache reads. CPU row encode, row decode, columnar decode
and CPU input batches explicitly throw in this new UNRELEASED serializer.
Released PCBS/default behavior is preserved. Prototype schema is int/long/string;
zero-column input and a row exceeding the 8 MiB pack bound are unsupported.
Projection currently decompresses the full packed table before selecting columns.

Lead verification:
- build-07 compiled Java/JNI and Spark3.5.5/Scala2.12 sources. Root ran real Spark
  probes for typed Cascaded, LZ4, Zstd and uncompressed: exact edge values/nulls,
  projected/reordered/duplicate fields, count-only empty selection, DISK_ONLY
  cached-record byte serialization, CPU row encode/decode and CPU columnar
  decode rejection, actual GPU-off cached-read rejection, early-stop TaskEnd
  cleanup. GPU/managed host allocations returned to the existing 128 MiB
  plugin bounce-buffer baselines, not global zero.
- Direct CPU columnar INPUT rejection has been inspected in source but its
  runtime test remains unverified. Cascaded byte-only Spark runtime is pending.
- Peer review found stale-build verification missed added Scala files. Root
  fixed exact Scala source inventory checking; reviewer inspected the fix.
  Fresh build-08 passed; root reproduced provenance acceptance and rejection
  of an unrecorded Scala source, compiled the shared-planner microbenchmark
  caller (CPU-only), and ran PackedTableCodec CPU submission-plan checks.
  No benchmark source additions occurred during build-07 runs; those artifacts
  remain valid. No microbenchmark performance sweep was restarted.

Matched actual Spark fixed-wide fixture: 4 x 256 MiB logical partitions,
local[2], two waves, 1 GiB total. Whole-job seconds, one cold population,
one warmup then median of two read samples. Full aggregate touches every column;
projection reads id only. No write+read total substituted for cached-read time.

| Format | Population s | Full cached read s | Id-only read s | Encoded MiB | Resident MiB | Peak tree RSS GiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Default Spark | 20.464 | 0.795 | 0.129 | 415.697 | 417.421 | 5.834 |
| PCBS | 1.050 | 0.263 | 0.160 | 487.518 | 487.518 | 3.920 |
| Contiguous uncompressed | 1.519 | 0.632 | 0.278 | 1024.319 | 1024.329 | 4.975 |
| Contiguous LZ4 | 1.540 | 0.537 | 0.202 | 509.380 | 509.391 | 3.996 |
| Contiguous Zstd | 1.694 | 0.450 | 0.213 | 385.422 | 385.433 | 4.101 |
| Contiguous typed Cascaded | 1.807 | 0.729 | 0.414 | 390.614 | 390.625 | 4.224 |

Artifacts: /data/tmp/nvcomp-spark-probe/{default,pcbs,uncompressed,lz4,zstd,cascaded}-fixed-256.
Root reproduced JSON summaries and checked actual reader labels/plans: default
CPU cache decode -> HostColumnarToGpu; others GpuInMemoryTableScan. Default's
producer converts GPU source data to rows for its CPU serializer. Thus default
is a GPU-enabled-job baseline, not a GPU codec. All four partitions resident,
zero disk bytes, no guard stop. nvCOMP encoded-group counters unchanged through
reads (no population recomputation). Payload audit ran AFTER timing and storage
snapshots; default payload excludes its separate statistics objects, while
nvCOMP includes layout/metadata/framing. Resident bytes include Spark object
accounting. RSS covers the process tree and reference/validation work, not codec
scratch alone. This is preliminary, one fixture/scale and limited repetitions.

Approved configured heap16/host32 inclpin2/RSS64 GiB retained, GPU pool24 GiB.
Actual managed HostAlloc data limit is 32 GiB minus default two-task30 MiB
overhead, pinned pool2 GiB included. No caps relaxed, no host OOM stress tests.

Remaining: runtime CPU columnar-input rejection, byte-Cascaded if retained,
512/1024/2048 MiB per-task real Spark cases, representative string workloads,
and repeatability/profiling of material differences. No production-readiness,
Cascaded V2, CPU codec performance, or full multi-profile compatibility claim.

## Snappy, ANS and actual Spark GPU activity breakdown

User explicitly authorized adding Snappy and ANS and measuring movement versus
kernel runtime. Implementation and this bounded 256 MiB/task measurement batch
READY FOR REVIEW; larger fixture/size matrix remains IN PROGRESS. All work and
worker checkpoints used MCP docker_exec only; root exclusively ran GPU jobs,
sequentially. New adapters/flags/analyzer remain UNRELEASED tools-only; no
production behavior/config/shims changed and prototype CPU paths still throw.

PCBS GPU writer getParquetWriterOptions selects SNAPPY when useCompression is
true (ParquetCachedBatchSerializer.scala). Spark SQLConf COMPRESS_CACHED maps to
spark.sql.inMemoryColumnarStorage.compressed, default true; the new benchmark
records its effective value, true in every run. This is independent of the
ordinary Parquet file compression configuration.

Changes: ByteCodec + native adapter use installed nvCOMP5.0.0.6 Snappy and default
generic-byte rANS, shared cuDF chunk framing, status/length/alignment validation,
RMM-owned scratch and bounded submissions. Spark classes/wire IDs and isolated
build/provenance extended. Optional --profile captures CUDA/NVTX; start/end
NvtxUniqueRange markers encompass population and every full/id-only collect,
excluding reference generation and post-run audit. Timers exclude marker calls.
profile.py filters phase timestamps to the marker JVM across all worker threads,
clips intervals, and reports bytes, sum/union times, overlap, runtime API calls
and top kernel names. It does not use thread-based NVTX GPU launch projection.

Independent verification:
- Root read complete Java/native adapter and integration diffs. Peer review
  matched SDK ABI/options and found a host descriptor vector exception lifetime
  issue. Fixed drain-guard ordering, reread and peer-reviewed; no fault injection
  claim. Root GPU byte tests passed both codecs across ten odd-tail/random/
  compressible/multi-block cases, plus forced offset1 frame decode; final RMM0.
- Root real Spark Snappy/ANS probes passed exact values/nulls, projection,
  reorder/duplicate fields, empty selected schema, DISK_ONLY byte records,
  CPU row encode/decode/columnar decode and actual GPU-off read rejection,
  early-stop TaskEnd cleanup back to existing plugin allocation baselines.
  First Snappy probe dispatch lacked the new codec cases; fixed two-case probe
  routing and rebuilt before accepting results (failed artifact probe-snappy
  preserved). CPU columnar INPUT runtime rejection remains unverified.
- Fresh build-11 produced all accepted Spark runs. Native compile -Werror,
  Java/Scala compile and CPU planner passed. Synthetic analyzer fixture passed
  concurrent kernel overlap, clipped copies, boundary-byte caveat, JVM/thread
  filtering. Reviewer independently reproduced PCBS raw SQLite sums/unions,
  copy bytes and phase PID; marker windows differed from collect by0.1-0.2ms.
- Root reproduced every result/analysis and checked PASS, rows/aggregates,
  actual GPU readers, resident4partitions/no disk, unchanged encode counters
  through reads, actual memory limits and zero boundary-crossing GPU events.
- Final log review found four duplicate jar registration errors before timing.
  Raw and dist/deps copies had identical SHA256. Build now deduplicates identical
  basenames preserving raw priority and rejects conflicting bytes. Peer-reviewed;
  build-12 provenance passed, every application class byte-identical to build-11,
  dependency hashes unchanged, CPU planner passed, final1024row ANS Spark smoke
  passed with no duplicate registration errors. Existing measurements remain
  valid. No production build artifacts modified.

Unprofiled matched additions, same workload/settings as prior table (seconds):
| Format | Population | Full read | Id-only read | Encoded MiB | Resident MiB | Peak RSS GiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Packed Snappy | 1.456 | 0.517 | 0.216 | 520.304 | 520.315 | 4.058 |
| Packed generic-byte ANS | 1.329 | 0.454 | 0.200 | 572.975 | 572.986 | 4.346 |

Artifacts /data/tmp/nvcomp-spark-probe/{snappy,ans}-fixed-256 and
probe-{snappy,ans}-11. Cold population1sample; warm read median2 after1warmup.
No CPU codec measurements: unsupported by explicit scope.

Separate diagnostic profiles: /data/tmp/nvcomp-spark-probe/profile-{pcbs,
uncompressed,lz4,zstd,cascaded,snappy,ans}-256/{profile.nsys-rep,profile.sqlite,
analysis.json,result.json}. The tables below use GPU interval UNION milliseconds;
reads average the two measured samples after1warmup. Overlap means columns MUST
NOT be added as a wall-time decomposition. All kernels include source generation
during population and query aggregation during reads, not codec-only time.
Profile wall time is perturbed and never replaces the unprofiled performance
table. Physical payload sizes vary slightly across these separate runs.

| Format | Population wall ms | Kernels ms | H2D ms | D2H ms | D2D ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| PCBS | 1129.640 | 150.566 | 2.112 | 22.798 | 0.006 |
| Uncompressed | 1974.109 | 82.967 | 7.231 | 58.856 | 14.767 |
| LZ4 | 1869.289 | 266.534 | 7.043 | 35.600 | 44.688 |
| Zstd | 1999.632 | 400.798 | 7.305 | 31.948 | 45.957 |
| Typed Cascaded | 2131.960 | 94.705 | 7.343 | 49.700 | 78.214 |
| Snappy | 1977.637 | 210.678 | 7.139 | 36.583 | 38.446 |
| ANS | 1708.827 | 88.994 | 7.760 | 40.966 | 39.796 |

| Format | Full-read wall ms | Kernels ms | H2D ms | D2H ms | D2D ms | GPU-active union ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| PCBS | 291.099 | 26.402 | 21.335 | 0.681 | 0.037 | 48.064 |
| Uncompressed | 753.063 | 95.205 | 61.321 | 8.827 | 4.309 | 165.270 |
| LZ4 | 708.715 | 120.338 | 32.719 | 8.646 | 0.709 | 158.834 |
| Zstd | 711.275 | 140.536 | 26.178 | 8.761 | 0.702 | 173.129 |
| Typed Cascaded | 984.342 | 100.697 | 36.895 | 9.005 | 21.789 | 165.126 |
| Snappy | 690.861 | 106.331 | 33.448 | 8.730 | 0.718 | 146.784 |
| ANS | 780.407 | 98.540 | 36.833 | 8.796 | 18.824 | 159.959 |

Interpretation/limits: unprofiled rankings remain primary; tracing adds material
overhead, especially in paths with many small API calls. GPU-inactive remainder
is CPU work/scheduling/GC/waits/profiler overhead, NOT measured CPU execution.
CPU sampling unavailable in container (perf paranoid4); ordinary CPU heap memcpy
is not measured by CUDA DMA activity. No inference of an exact unprofiled CPU
fraction. PCBS id-only H2D16.404MiB versus packed Snappy521.793MiB /
ANS572.806MiB / Zstd384.485MiB: prototype decompresses full table before selecting.
ANS fullread0 adds12,337 non-PTDS cudaMemcpyAsync D2D calls/444258370B/19.005ms
summed GPU time, consistent with bridge alignment bounce; no call stacks prove
exclusive attribution versus SDK internals. No speculative optimization applied.

Memory approval unchanged heap16/host32 includingpin2/RSS64/GPU24GiB.
Profiles peak5.25-6.01GiB process-tree RSS; no guard stops, OOM, cap relaxation
or disk spill. Matrix512/1024/2048MiB/task and broader strings still pending.

## CPU profiling checkpoint — parked for repo handoff

User requested a stopping point to use the repo. No further work is authorized
until the user returns it. All GPU/benchmark processes finished; root checked
nvidia-smi (no compute processes) and pgrep for benchmark/SparkSubmit/nsys (none).
Workers are completed/idle. All project operations, including worker handoffs
and checks, used MCP docker_exec ONLY; preserve that requirement on resumption.

CPU profiler implementation and bounded diagnostics READY FOR REVIEW, not DONE.
New tools are UNRELEASED, no compatibility aliases needed. Production plugin,
PCBS/default and shim sources/configuration remain unchanged. GPU-only serializer
CPU fallback still throws. These CPU profiles measure host work in GPU execution,
not CPU encode/decode implementations.

Added CpuPhaseProfiler.java, CpuProfileReader.java, cpu_profile.py and optional
--cpu-profile runner integration. Reuses the plugin-packaged async-profiler3.0
loader: ctimer CPU sampling1ms plus wall5ms, native frame pointers, per-action JFR.
This works with perf paranoid4; earlier sampling restriction applied to Nsight/
perf, not every CPU profiling method. Plugin per-stage polling default5seconds
would miss subsecond phases, so tools invoke the same loader at action boundaries.
ThreadMX/process CPU counters plus GC/JIT counters complement samples. Reader
uses [start,end) epoch bounds and separates CPU STATE_DEFAULT from wall
RUNNABLE/SLEEPING (root/worker inspected AP3 source and smoke recording). Counts
are samples, never nominal interval multiplied into CPU milliseconds.

Verification:
- Native/Java/Scala isolated builds passed. Root read decisive helper/reader/
  analyzer and harness code. Focused worker source reviews and independent
  final-PCBS counter/window arithmetic passed. Missing CPU counters remain null.
- CPU burn/sleep reader checks reproduced15223events and excluded an empty
  timestamp window; strict task stack+name filtering, known counters, GC/JIT and
  new-thread cases passed. Root independently reran the check script.
- Initial separate snapshot JSON return added~45ms transport before collect.
  Moving begin/finish counter capture entirely into JVM removed that pollution:
  final sample-minus-collect0.158-0.318ms, counter-epoch-minus-sample0.624-1.269ms.
  Snapshots remain sequential and their durations are recorded. Draft build13/
  build14 tight stacks are valid, but final CPU counter claims use build15/16.
- Final profiles cpu-final-{pcbs,snappy,cascaded}-256 passed correctness, actual
  GPU readers, unchanged population counters during reads, all4partitions
  resident/no disk, unchanged heap16/host32incpin2/RSS64/GPU24GiB. Peak RSS
  4.15-4.79GiB. One warmup plus5 measured full/id reads. No OOM/guard stops.
- Small combined CUDA/NVTX + CPU profile smoke passed, both analyzers ran;
  NVTX closes before the large CPU JSON reply. This smoke is capability
  validation, not large-workload critical-path attribution.
- Build17 is final metadata checkpoint: application classes byte-identical to
  build16, provenance verified. Manifest now states git_head is the CHECKOUT
  revision only and does not attest source alignment of prebuilt plugin jars.

Mean final CPU diagnostics, milliseconds per action. Worker CPU sums both task
slots and can exceed wall time; driver/task/process time is NOT an additive
wall-time breakdown. Counter windows are slightly wider than collect.
| Format | Phase | Wall ms | Executor-worker CPU ms | Driver-query CPU ms |
| --- | --- | ---: | ---: | ---: |
| PCBS measured jar | Population | 1132.78 | 1043.86 | 295.53 |
| PCBS measured jar | Full read | 276.67 | 238.83 | 114.74 |
| PCBS measured jar | Id-only read | 167.81 | 137.82 | 59.98 |
| Packed Snappy | Population | 1498.55 | 1587.55 | 260.59 |
| Packed Snappy | Full read | 543.10 | 734.04 | 118.23 |
| Packed Snappy | Id-only read | 226.93 | 270.17 | 54.87 |
| Packed Cascaded | Population | 1866.49 | 2339.09 | 283.72 |
| Packed Cascaded | Full read | 704.38 | 1098.73 | 92.09 |
| Packed Cascaded | Id-only read | 426.96 | 638.91 | 51.15 |

Evidence-backed findings (CPU sample shares, not wall fractions):
- Packing: Encoder.pack -> ColumnView.slice / Table.contiguousSplit appears in
  452/1487 Snappy and628/2166 Cascaded population task CPU samples. Heap array
  allocation/zeroing is also visible (206/984 PCBS population task samples).
- Full-read task CPU leaf CUDA driver shares PCBS366/1154(31.7%),
  Snappy1473/3498(42.1%), Cascaded2445/5183(47.2%). Packed stacks include RMM
  alloc/free, host copies, synchronization, metadata copies and many downstream
  scalar/reduction/cast/null-check/column-close calls. Not all work is decoding.
- Cache records are PCBS4 total vs packed256 (64/partition), same source
  GpuRange4batches/4194304rows. GPU profile fullread0 kernels732 vs Snappy28804 /
  Cascaded28908. Full-minus-id kernels478 vs27202 for BOTH packed codecs.
  Plans put partial GpuHashAggregate above cache scan without explicit coalescing.
  Cache record counts are observed; scan output batch counts were not separately
  instrumented. Code emits one packed output per record. This supports small
  output batches amplifying downstream work, not simply a slower codec.
- Measured PCBS id-only arraycopy/memcpy leaf339/653 task CPU samples(51.9%):
  Table.readParquet(byte[]) -> HostMemoryBuffer.setBytes -> Unsafe.copyMemory.
  PCBS GPU projection uploads fewer bytes, but this binary copies entire encoded
  arrays to native host memory first.
- Driver query wall samples mostly wait for tasks; task wall samples mostly
  runnable. Background JIT remains substantial after1warmup. Warm full-read GC
  counters total0ms PCBS/Snappy and5ms Cascaded, not a dominant explanation.
- Longer warmup check cpu-warmed-snappy-256 (8warmups/5reads) stayed~527.5ms full
  read vs543.1ms before, task CPU746.3ms vs734.0ms. Main per-batch cost persists;
  no statistically significant speedup claim.

CRITICAL provenance correction: prebuilt sql-plugin jar SHA256
53714451c0fd3d829f4fd60086d4c69f29355b229848976faf74bef4e5ecaf4f
contains a direct Table.readParquet(byte[]) call in its PCBS reader (javap and
runtime stacks). Current branch source instead uses ParquetCachedBatchReader.
Lead had checked source and artifact hashes but had NOT established their
alignment; this was a material verification omission and was reported to user.
All earlier PCBS comparisons remain observations of that frozen older artifact,
not current WIP branch behavior. Do not silently replace/relabel those results.
A fresh current-branch PCBS build and new baseline are required before a final
comparison/decision. No production rebuild or optimization was attempted here.

Artifacts all under /data/tmp/nvcomp-spark-probe:
- build17 (final source/provenance), build15/16 (accepted CPU captures).
- cpu-final-{pcbs,snappy,cascaded}-256/{result.json,cpu-analysis.json,cpu/*.jfr}.
- cpu-warmed-snappy-256, cpu-gpu-combined-smoke/{gpu-analysis,cpu-analysis}.json.
- cpu-final-raw-row-cache.json.gz, cpu-final-raw-interpretation.json,
  cpu-final-context-evidence.json, batch-metadata-evidence.json.
- verify-cpu-final.py, check-cpu-analysis.py, check-cpu-gaps.py reproduce checks.

On resumption: read this tracker first; confirm repo/branch/GPU ownership and
MCP-only tools with every worker. First resolve current-source PCBS artifact
baseline. Potential prototype improvements to discuss are fewer packing JNI
calls and coalescing decoded GPU outputs while retaining small cache records;
neither change is implemented/accepted. Larger task/string matrix still pending.

## Resumed current-source cohort — READY FOR REVIEW

User returned repo and authorized resumption/branch restoration. Root switched
clean tracked checkout from fix-gpu-output-metadata to pcbs-rowgroup-production;
preserved unrelated untracked work. GPU was idle. Every project operation and
worker handoff/checkpoint used MCP docker_exec only; workers confirmed this and
did not run GPU/Maven jobs. Root owns serial GPU jobs.

Release classification unchanged: prototype tools/interfaces are UNRELEASED,
no compatibility fallback/alias needed; released production contracts and
current branch PCBS implementation are untouched. No POM/profile/shim source
changes. This is bounded local Spark3.5.5 validation, not a multi-profile
compatibility or production-readiness claim.

Current sources built in detached worktree
/data/tmp/nvcomp-pcbs-current-397812926b at397812926b0af807f1b30622f4ba65d1c4b71a43.
This excludes unrelated untracked Scala sources in the main checkout and
preserves its previous target outputs. Current SNAPSHOT aliases had changed,
but exact historical JNI/private jars were recovered by SHA256 from Oct23
timestamped Maven artifacts and copied to /data/tmp/nvcomp-pcbs-rebuild-01/dependencies:
JNI972b04a70d3a2719fd7fbebe8e76a14b95197fb102de668937a7a06e3b60357d,
private223f492cea205fb85ae0bea502d219e9918dd148a51b3173578562f592999d83.
Both match the historical cohort; required chunked-reader varargs API and
batched-codec callback signatures verified with javap before building.

Maven wrapper command (normal checks retained, tests explicitly skipped):
rapids-plugin-mvn -o -nsu -Dbuildver=355
-Dcudf-spark-jni.version=26.12.0-20261023.150441-28
-Dcudf-spark-private.version=26.12.0-20261023.142435-23
-DsecondaryCacheDir=/data/tmp/nvcomp-pcbs-rebuild-01/zinc
-pl aggregator -am verify -DskipTests
All15reactor modules passed, including duplicate-class enforcement/license
checks;7m38s. Initial clean build stopped at unwritable default ~/.sbt cache;
redirected the supported secondaryCacheDir and resumed without disabling checks.
Compiler code-heap/deprecation warnings were build-JVM diagnostics, not measured
benchmark-JVM options. No native rebuild or mutable SNAPSHOT overwrite.

Fresh aggregator SHA256
122466bc6c9eaf3126b74d8ad24659c079a8d35e0334b3baa10134ab3cfaf6fa.
Root inspected actual bytecode: serializer constructs ParquetCachedBatchReader,
rather than direct Table.readParquet(byte[]) calls. Detached tracked sources
remain clean. Maven log and source proof are persisted at
/data/tmp/nvcomp-pcbs-rebuild-01/{maven-verify.log,source-build.json}.

Builder now requires --source-build and --source-head. Implicit globbing could
mix stale raw/aggregate jars and could not establish source alignment, so that
selection was replaced by the explicit source-built aggregator + pinned JNI.
Validation binds success/clean source/expected commit/exact worktree output,
log/artifact hashes, reader bytecode reference, every private class and version
metadata, and exclusion of JNI payload in aggregator. Actual private bytes all
matched (no shading exception used). Launcher revalidates proof/dependency
inventory. Experimental manifest keeps checkout head separate from source-build
revision. Runtime origins of public/internal PCBS, reader, cache trait, cuDF
Table and ParquetChunkedReader must match the submitted jars before any action.
No legacy implicit-artifact fallback is retained.

Root read decisive reader/serializer/POM and all delegated implementation/test
changes, independently reran13 proof-rejection tests (now committed as
tools/cache-exploration/nvcomp/spark/test_source_build.py), rebuilt experimental
JNI/Java/Scala sources, and verified actual output hashes/class origins.
Peer review found/fixed missing expected-head/path binding and private input
verification, and added the configured public PCBS wrapper to runtime checks.
No remaining focused review finding.

Runtime prerequisites: current-pcbs-smoke/current-pcbs-origin-smoke checked every
one of1024rows; current-snappy-probe passed edge/null/projection/CPU-rejection/
early-stop cleanup cases on fresh cohort. Large deterministic aggregate checks
and all runtime source origins passed in every measured run. Full production
unit/integration suite was not run; untouched production implementation is
being used as a measured baseline, not delivered as a new production change.

Corrected cohort: build-20, 4x256MiB logical partitions, local[2],1GiB total.
One cold population,1warmup +5measured full/id reads; read medians. Whole-job
seconds, payload MiB includes each format's audit-defined framing/metadata.
| Format | Population s | Full read s | Id-only read s | Encoded MiB | Resident MiB | Peak RSS GiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Current-source PCBS | 1.443 | 0.304 | 0.162 | 489.668 | 489.674 | 4.447 |
| Packed Snappy | 1.631 | 0.476 | 0.180 | 520.510 | 520.522 | 4.304 |
| Packed ANS | 1.517 | 0.551 | 0.218 | 573.347 | 573.358 | 4.650 |
| Packed Zstd | 1.743 | 0.483 | 0.201 | 386.431 | 386.442 | 3.903 |
| Packed LZ4 | 1.536 | 0.466 | 0.210 | 514.034 | 514.046 | 4.465 |
| Packed typed Cascaded | 1.831 | 0.678 | 0.371 | 390.111 | 390.123 | 3.903 |
| Packed uncompressed | 1.674 | 0.513 | 0.241 | 1024.319 | 1024.329 | 4.554 |
| Default Spark | 20.863 | 0.697 | 0.171 | 415.697 | 417.421 | 7.695 |

Default is CPU cache conversion inside GPU-enabled job, confirmed by plan.
All other cache readers actualGPU, aggregate checks exact. All4partitions
resident/no disk across population and12read actions, no repopulation (nvCOMP
encode counters unchanged), no guard stops. Caps unchanged heap16GiB/host32inc
pin2/RSS64/GPU24; actual managed host32GiB minus30MiB per-task overhead.
Current PCBS has140cachedrecords (35/partition), versus4 in the historical
artifact; all packed formats256. Do not attribute old PCBS CPU stacks to the
current reader. Current-source profiling is recorded in the following section.

Artifacts /data/tmp/nvcomp-spark-probe/current-{pcbs,snappy,ans,zstd,lz4,cascaded,
uncompressed,default}-fixed-256; source-bound build-20. Root repro script
/data/tmp/nvcomp-pcbs-rebuild-01/verify-current-cohort.py passed all8runs including
six runtime class origins, rows, storage, caps, counters and audited bytes.
Old artifacts were not overwritten/relabelled; no codec/batching optimization
implemented. The current-source profiling follow-up is recorded below.

## Current-source CPU/GPU diagnosis — READY FOR REVIEW

Root completed serial diagnostic runs using the same source-bound build-20,
settings and 4x256MiB fixed-wide data. Every operation, worker handoff and
checkpoint remained MCP docker_exec only; worker confirmed read-only use and
did not run GPU/Maven jobs. No production or prototype codec changes were made.

CPU profiles: cpu-current-{pcbs,snappy,cascaded}-256 under
/data/tmp/nvcomp-spark-probe. Packaged async-profiler used CPU ctimer 1ms and
wall 5ms; one population plus five full and five id-only measured actions per
format, with warmups excluded. Root inspected decisive pinned PCBS source and
independently verified results/counter arithmetic. Worker checked all33 measured
JFRs against raw event/thread metadata and analyzer output; root also inspected
JDK jfr print output from the current PCBS full-read trace. No old
Table.readParquet stack appeared; the current reader/chunked-reader paths did.

Diagnostic read means below are separate from the unprofiled cohort medians.
Worker CPU sums both concurrent task slots; it is not elapsed wall time.

| Format | Phase | Wall ms | Worker CPU ms | Driver CPU ms |
| --- | --- | ---: | ---: | ---: |
| PCBS | Population | 1440.13 | 1446.62 | 271.28 |
| PCBS | Full read | 278.39 | 303.12 | 86.94 |
| PCBS | Id-only read | 163.51 | 150.58 | 53.10 |
| Snappy | Population | 1844.98 | 2218.27 | 269.80 |
| Snappy | Full read | 503.66 | 706.58 | 102.59 |
| Snappy | Id-only read | 232.86 | 256.25 | 62.83 |
| Cascaded | Population | 1954.79 | 2459.93 | 285.10 |
| Cascaded | Full read | 718.89 | 1109.17 | 98.51 |
| Cascaded | Id-only read | 351.56 | 520.41 | 52.77 |

Current PCBS still stages complete encoded records from heap into native host
memory in ParquetCachedBatchFiles.stage, through HostMemoryBuffer.setBytes and
Unsafe.copyMemory. Root read this implementation and its caller:
ParquetCachedBatchReader stages against the full cache schema; selected columns
are applied later by native ParquetOptions/includeColumn. Logical-file footer
rewriting/slicing adds no second payload copy. Full/id task CPU sample membership
for this staging copy was361/305 respectively (overlapping stack memberships,
not exclusive wall-time totals).

The packed formats spend substantial CPU in CUDA driver calls. Driver leaf
samples comprise26.6%/43.4%/46.5% of full-read task CPU samples for
PCBS/Snappy/Cascaded respectively. Nearest Java callers in Snappy include scalar
creation, fromScalar, casts, reductions, binary operations, null checks and column
cleanup as well as decoding. Cascaded additionally shows substantial memcpy and
allocation/free activity. Broad aggregate-iterator ancestry includes upstream
cache consumption; it cannot by itself assign those samples to aggregation.
These sample shares must not be converted to elapsed-time percentages.

GPU diagnostic runs profile-current-{pcbs,snappy}-256 used Nsight CUDA/NVTX,
one warmup and two measured reads. Root independently reproduced first full-read
kernel counts and duration sums from SQLite/NVTX windows. No GPU event crossed
any of the seven measured phase boundaries per run.

| Format | Phase | Wall ms | Kernel events | Kernel union ms | H2D MiB | D2H MiB | Any GPU activity union ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| PCBS | Population | 1471.07 | 1330 | 164.05 | 7.881 | 494.110 | 199.67 |
| PCBS | Full read 0 | 401.52 | 1024 | 29.09 | 497.685 | 14.652 | 56.69 |
| PCBS | Id-only read 0 | 196.45 | 434 | 4.33 | 16.496 | 0.340 | 5.84 |
| Snappy | Population | 1917.32 | 36858 | 210.40 | 1.893 | 524.348 | 285.41 |
| Snappy | Full read 0 | 698.70 | 28804 | 105.43 | 522.194 | 0.844 | 146.25 |
| Snappy | Id-only read 0 | 234.35 | 1602 | 13.22 | 521.640 | 0.831 | 48.21 |

These are perturbed diagnostic runs, not replacement headline timings. Events
include downstream query work, not codec kernels alone. Union times overlap and
must not be added; time outside GPU activity is not measured CPU time. CPU and
GPU profiles are separate jobs, so this is not an exact critical-path breakdown.

Projection evidence is direct: PCBS id-only transfers16.5MiB versus497.7MiB
full-read, whereas packed Snappy still transfers521.6MiB versus522.2MiB.
Packed full-read kernel count28804 versus current PCBS1024, together with
nearest CPU call contexts, supports investigating repeated small decoded GPU
batches and downstream launch overhead. Cache audits count140 PCBS records and
256 packed records, but these are NOT independently measured scan output batch
counts: PCBS combines records through its chunked reader. Exact read-side batch
counts remain unverified.

GC collection totals across five full reads were6/18/7ms for
PCBS/Snappy/Cascaded; GC is not the dominant observed explanation. Compilation
counters show one warmup is not a fully steady-state guarantee.
Sequential counter snapshots slightly widen the tight JFR/query window:
maximum extra1.234ms Snappy and1.295ms Cascaded, with one PCBS id-only iteration4
outlier4.800585ms (before-snapshot4.795587ms, after1.250858ms, no GC).
The tight PCBS JFR window itself remained within0.306ms of collect.
Root's initial arbitrary3ms counter-gap check caught that outlier; it was
investigated and explicitly retained as a measurement limitation. No claim of
an exact additive wall-time decomposition or uniformly sub1.3ms gaps is made.

Root reproduction script
/data/tmp/nvcomp-pcbs-rebuild-01/verify-current-profiles.py passed all three CPU
and both GPU runs, including exact aggregates, rows, source origins, resident
storage and counter arithmetic; it explicitly reports the counter-window
outlier. Raw CPU evidence/thread/context JSON and reproduction scripts are
beside it; original JFR/Nsight artifacts remain in their run directories.
No guard stops, cache eviction or repopulation occurred. Peak CPU-run RSS was
4.196/4.611/4.056GiB. All experiment processes finished and GPU was idle at
checkpoint.

Remaining: larger512/1024/2048MiB/task and representative string matrix.
Recommended next controlled change for discussion: coalesce decoded GPU batches
while retaining small compressed cache records, then measure launch/CPU/transfer
effects. Reducing packing JNI calls is another candidate. Neither optimization
is accepted or implemented at that checkpoint; no architecture/acceptance
criteria were changed then. The user subsequently approved concatenation below.

## Bounded decoded batching and impact reruns — READY FOR REVIEW

User authorized the proposed bounded concatenation and rerunning impact tests.
Scope: combine decoded projected tables within each existing decode group,
retaining 64 KiB codec chunks, at-most-8 MiB cache records, 248 MiB raw group
bound, 256 MiB wire bound and all approved memory settings. No projection,
predicate pushdown, population packing, CPU fallback or production PCBS changes.
Release classification remains UNRELEASED experimental tools; no compatibility
scaffold needed. Released production contracts are preserved. This tools-only
Spark 3.5.5 harness change does not modify shims/shared shim callers/profiles;
shim preflight is not applicable, and no multiversion validation is claimed.

Decoder now constructs projected table views for a group and concatenates them
before publishing one spillable output batch. Single-table groups bypass the
copy. Long row accounting stops groups before Int.MaxValue; all GPU allocations
remain within the existing retry and resource scopes. Existing heap records
are replayable on retry. Counters publish successful native calls/concatenations
after the retry succeeds; output counters track batches actually returned.
Counters exclude the unchanged empty-selected-column/count-only path.
Output byte totals/maxima describe materialized outputs, not allocation peaks;
maxima and RMM high water are process-cumulative.

Root read Table.concatenate usage in GpuCoalesceBatches, prototype ownership,
retry code and all changed source. Reused cuDF concatenation with the surrounding
ARM pattern rather than replacing codec/storage code. Added an NVTX range for
concatenation plus spill-wrapper construction, and output batch/row/byte counters.
No user-facing configuration was introduced.

Every operation and worker handoff/resumption/checkpoint used MCP docker_exec
only. Reviewer confirmed MCP-only read-only work, with root owning serial GPU
runs and builds. Review identified that consuming the new first combined output
would leave only heap input in the old cleanup probe: fixed it to prefetch the
second decode group before early task completion. Reviewer independently
inspected the fixed source and actual passing probe artifacts; no substantive
review finding remains. Root independently ran decisive checks below.

Build-21 initially caught Scala overload inference for withRetryNoSplit; explicit
SpillableColumnarBatch result type fixed it. Build-22 compiled and passed the first
probe/benchmark. Build-23 includes the strengthened pending-GPU cleanup probe;
all final results use it. Both builds use the same verified current-production
source proof/JNI/private artifacts as build-20. No Maven/native production
rebuild, POM or dependency change was needed.

Probe concat-snappy-probe-verified passed exact ordered multi-record values with
strings, nulls, reordered and duplicate columns; full small-row collection,
single-record path, empty-selected count, serialized DISK_ONLY byte equality,
explicit CPU rejection and actual GPU-off rejection. Early stop left the second
GPU group pending; consumed 253208 of 320000 repeated-input rows. TaskEnd restored
GPU live allocation 134217728 bytes and managed-host ratio to their exact prior
values. Fixture repeats existing cached heap records through an iterator to
exercise multiple groups without growing cache residency. CPU columnar input
rejection remains a previously recorded runtime coverage gap; no new support.

Impact cohort uses the same fixed-wide 4x256 MiB tasks (1 GiB total), local[2],
two task waves, one cold population, one warmup and five measured reads. Read
values are medians; population is one cold sample. Previous source-correct
cohort is the before column; fresh controls use the unchanged production code.

| Format | Population after s | Full before s | Full after s | Id before s | Id after s | Encoded after MiB | Peak RSS GiB | Whole-job RMM high water GiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| PCBS control | 1.514 | 0.304 | 0.305 | 0.162 | 0.136 | 489.667 | 4.652 | 1.597 |
| Packed Snappy | 1.322 | 0.476 | 0.231 | 0.180 | 0.165 | 520.509 | 4.512 | 1.887 |
| Packed ANS | 1.257 | 0.551 | 0.270 | 0.218 | 0.180 | 571.399 | 4.566 | 2.436 |
| Packed Zstd | 1.817 | 0.483 | 0.272 | 0.201 | 0.166 | 382.769 | 4.562 | 3.107 |
| Packed LZ4 | 1.498 | 0.466 | 0.248 | 0.210 | 0.151 | 509.767 | 4.294 | 1.810 |
| Packed typed Cascaded | 1.614 | 0.678 | 0.419 | 0.371 | 0.340 | 390.957 | 4.489 | 1.727 |
| Packed uncompressed | 1.757 | 0.513 | 0.276 | 0.241 | 0.186 | 1024.319 | 4.496 | 1.555 |
| Default Spark control | 21.172 | 0.697 | 0.740 | 0.171 | 0.159 | 415.697 | 7.049 | 1.133 |

Population/encoding code did not change; population and compressed-size
differences across runs are not evidence of an encoding improvement.
All aggregates match the same deterministic reference. All four partitions
remain resident with zero disk bytes; no repopulation or guard stop. Packed
cache record count stays 256; PCBS 140/default 420. Every packed full/id action
returns exactly eight output batches and 4194304 rows. Full output totals
1023.508 MiB, id-only totals 32 MiB, maximum full batch 244 MiB. Eight decode
groups/calls for byte codecs are unchanged; uncompressed calls zero. Typed
Cascaded retains its 28 native calls/action (width groups plus rounded workspace
block-budget splitting), matching its encoder submission count for this data.

Fresh before-control: detached tools worktree /data/tmp/nvcomp-concat-before
at bc04d26c4c, build-before-concat using the same pinned source proof/dependencies.
Its unchanged Snappy full median 0.479907s reproduces the prior 0.475968s;
id-only 0.191389s, population 1.492955s. This supports the roughly 2.1x Snappy
full-read improvement being attributable to batching rather than baseline drift.
PCBS full control remains about 0.305s. No statistical confidence interval or
general workload ranking is claimed from this bounded experiment.

Separate Nsight run concat-profile-snappy-256 (one warmup/two measured reads),
compared with source-correct profile-current-snappy-256:

| Full read 0 diagnostic | Before | After |
| --- | ---: | ---: |
| Elapsed wall ms | 698.699 | 278.208 |
| Total kernel launches | 28804 | 1256 |
| Snappy decompression launches | 8 | 8 |
| Null-check launches | 7936 | 248 |
| Boolean-to-long cast launches | 7936 | 248 |
| Null-count addition launches | 7680 | 240 |
| Row-hash launches | 256 | 8 |
| Kernel duration sum ms | 112.214 | 30.907 |
| Kernel activity union ms | 105.432 | 28.235 |
| Any GPU activity union ms | 146.249 | 57.011 |

Root independently grouped all raw SQLite kernel events by demangled name and
phase JVM, reproducing 1256 and the exact expression multiples. CPU launch-API
correlation IDs within the eight concatenation NVTX ranges attribute 252 kernels
and 4.747317ms summed GPU duration to concatenation. Their summed host range
duration is 42.227248ms across concurrent task threads, includes spill wrapping/
synchronization and must not be treated as an additive wall-time cost.
Snappy decompression summed kernel time 10.868ms before versus 11.383ms after.
Id-only total kernels 1602 -> 110. Population kernels unchanged at 36858.
No traced event crossed the seven phase boundaries. Compression/decompression
already batch small chunks; the fix reduces downstream work per output batch.
The validation query's unfused 31 null checks/casts and 30 additions per input
batch explain most original launches; its overhead is query-dependent.

Separate concat-cpu-snappy-256 run confirms task-thread CPU mean per full read
706.584ms -> 219.619ms, driver mean 102.594ms -> 90.793ms. Id-only task CPU
256.245ms -> 156.179ms. CPU sums both concurrent workers and is not elapsed
time. CPU/GPU profiles are separate perturbed runs, not headline medians or
an exact wall-time decomposition. Root analyzed the JFRs and independently
checked counter arithmetic; focused reviewer checked fresh result/profile data.

Verification: final Java/Scala experimental build passed; all 13 source-proof
tests passed; git diff --check passed. Root reproduction scripts
/data/tmp/nvcomp-pcbs-rebuild-01/verify-concat-cohort.py,
/data/tmp/nvcomp-pcbs-rebuild-01/concat-profile-summary.py and
/data/tmp/nvcomp-pcbs-rebuild-01/concat-range-cost.py reproduce the cohort
checks and raw kernel attribution. First verifier assumed three Cascaded width
calls/group; observed 28/action prompted inspection of the bounded planner's
extra splitting, and the check now uses the unchanged actual submission count
rather than the false width-only assumption.

Artifacts: /data/tmp/nvcomp-spark-probe/concat-{codec}-fixed-256 (Snappy final
suffix -final), concat-profile-snappy-256, concat-cpu-snappy-256 and the probe
directory above. Build-22's preliminary Snappy run is retained separately and
excluded from final tables. Original artifacts are preserved.

Limits: the extra concatenated output coexists temporarily with compressed
uploads and original decoded allocations. The 248 MiB bound limits raw grouped
input, not total GPU memory. Whole-job RMM peaks above include population and
can hide a smaller read-only transient peak; a precise before/after read-only
allocation peak was not measured. RSS is sampled, not a hard process limit.
No pressure/OOM search, larger-size or large-string performance claim is made.
Larger 512/1024/2048 MiB/task and representative string matrix remain pending.
No further optimization was implemented at that checkpoint. Architect acceptance
remains pending. Subsequent user authorization and target-size work follow.

## Configured target and large-task spill observations — READY FOR REVIEW

User clarified: algorithms must honor configured targets across hardware,
using spilling/retry, with roughly 4x target working memory per task as a goal.
The 24 GiB GPU pool is acceptable as a fixed benchmark condition, not an
algorithm requirement; do not rerun old results solely because of that setting.
Root's earlier choice of 24 GiB and derived 256/248 MiB bounds was not
hardware-derived. A workspace failure under that pool does not establish a
hardware limit. Hardware audit: RTX A6000 about48GiB, host about188.5GiB;
availability is a point-in-time observation, not a new experimental budget.

User explicitly authorized raising decoded output grouping to the configured
GPU target, releasing temporaries promptly, and running through2GiB/task while
measuring spill. No forced pressure test or additional algorithm optimization.
Same heap16GiB, managedhost32GiB including pinned2GiB, sampledRSSstop64GiB,
GPU24GiB benchmark settings. Root keeps GPU jobs serial; each uses local[2],
four partitions and two waves. MCP docker_exec only remains mandatory for all
project operations, delegations, resumes and checkpoints; both workers confirmed
continued MCP-only use and performed no GPU/build jobs.

Release classification unchanged: experimental tools are UNRELEASED, so their
method signatures/callers change directly with no compatibility adapters.
Production PCBS and released contracts are untouched. No production operator,
shim, POM, profile or configuration declaration changed. Tools-only Spark3.5.5
scope; no new multiversion readiness claim or shim source-selection change.

Reader takes RapidsConf.gpuTargetBatchSizeBytes (default1GiB), groups by exact
stored packed-buffer lengths and Int.MaxValue rows, and no longer stops on the
old248MiB raw/256MiB encoded group constants. Grouping remains conservative for
projections, using the full cached schema as agreed while projection is deferred.
Native decompression planning uses that target as its chunk-rounded submission
budget, allowing non-chunk-aligned targets and at least one indivisible segment.
Removed the aggregate256MiB rejection; per-record layout/coverage validation
remains. Population packing/compression and small stored records are unchanged.

Decoded records become spillable pieces. Raw decoder owner wrappers, compressed
uploads and scratch close before concatenation; decoded data itself remains
owned by those pieces. Concatenation reuses ConcatAndConsumeAll and
withRetryNoSplit(Seq[SpillableColumnarBatch]), so inputs are spillable between
attempts and consumed safely. Native decode/concat/output counters publish only
successful results. This does not claim full algorithmic memory=4xT or guarantee
arbitrary targets fit; actual allocation peaks and spill are measured.

Root read decisive PCBS target path, retry sequence ownership and existing
concatenation helper. Read-only reviewer inspected these changes independently,
found/fixed misleading owner-lifetime comment, and confirmed ownership/target
behavior. CPU planner tests cover1GiB, odd and tiny targets; default compression
planner checks remain. Bounded GPU probe explicitly sets odd32MiB+1234byte
target, checks actual combined output approaches it, verifies ordered
multi-record strings/nulls/duplicate/reordered fields, disk-byte equality,
CPU rejection and second-prefetched-output TaskEnd cleanup. Build25 probe PASS;
subsequent changes affect only spill observation, not that algorithm.

New SparkSpillMonitor records task-attributed RAPIDS spill-to-host/disk bytes/
times, retries and maximum concurrency, plus separate Spark spill metrics.
They overlap and must not be added. Per-task raw values and coverage distinguish
zero from unavailable. Counts of positive tasks are not event counts.
Listener drain/serialization occur outside query/profile timers. Effective
target is recorded. No allocation callbacks or forced spills are introduced.

Material instrumentation repair: initial builds25-29 captured no observations.
Root investigated actual task events and found TaskMetrics.accumulators()
contains unregistered internal accumulators: reading their name throws due to
null metadata. Worker also used errors.toSeq, aliasing a mutable buffer cleared
before JSON serialization; root fixed errors.toVector and filters metadata!=null
before name access. Prior phase-tag propagation hypothesis was not established
and is retracted. Final helper uses the serial local action window with queue
drains before/after, records total job/task event deltas and verifies coverage.
This is not concurrent-job attribution. Build30 smoke has9population and5/read
events/observations with complete coverage of all18RAPIDSmetrics, explicit zero
spill/retries, maximumGPUconcurrency2. Reviewer independently reproduced this
from source and runtime JSON. No remaining substantive focused findings.
Preliminary runs target-snappy-fixed-{256,512} have unavailable spill and are
excluded from the final cohort; retained with honest unavailable fields.

Final source-bound build30 uses the same production397812926b proof and pinned
JNI/private artifacts as previous cohorts; no production rebuild. Final dirs:
target-final-{codec}-fixed-{256,512,1024,2048} under
/data/tmp/nvcomp-spark-probe. One cold population, one warmup plus five measured
full/id reads. Root verify-target-run.py checks exact aggregates/rows/source
origins, actual caps, every spill-metric coverage/raw aggregation, unchanged
encoding counters, cache audit and actual returned batch counts; emits
verified-summary.json beside results. Never infer zero from missing values.

Completed and independently verified so far:
- Snappy all4sizes: population1.675/2.294/3.994/6.636s,
  full0.242/0.298/0.477/0.808s, id0.160/0.189/0.271/0.533s.
- PCBS all4sizes: population1.476/1.947/2.868/4.381s,
  full0.278/0.379/0.622/1.089s, id0.160/0.188/0.309/0.516s.
- All these population/read phases: task-attributed GPU->host/disk spill0,
  retries0; observed maximum GPU concurrency2. No guard stops or disk cache.
- Snappy returned4/4/4/12outputs per query; maximum full outputs
  255.875/511.750/1023.500/1020.000MiB. At2GiB/task the exact packed-size
  boundary and record granularity leave small final tails; do not assume
  nominal task bytes divide into an exact output count.
- PCBS output counts are not directly instrumented by the prototype counter.
  Historical four-count came from kernel fingerprints, not this larger sweep.

The remaining24runs subsequently completed or failed as recorded below.
Re-read this tracker and use MCP tools only after any resumption. Do not restart
completed final runs. Benchmark conditions remained fixed. Architect acceptance
remains pending; implementation and measured outcomes are READY FOR REVIEW.

### Completed final size matrix

Root resumed after capacity messages from signed source checkpoint2dcc305537,
read the committed tracker and confirmed tracked checkout clean/GPU idle.
Continued using MCP docker_exec only; no source/settings changes or repeated
completed final runs. Final build30 remains source-bound to the same production
artifact cohort. All32cases attempted,31PASS and1FAIL during population.
Independent verifier final-target-matrix.py reran all successful per-run checks,
compared expected aggregates across formats at each size, verified complete
metadata/cap/metric coverage and reproduced all medians/audited byte counts.
Totals:403observed phases (population+warmups+measuredreads),2139successful task
attempts. Every observed spill/retry metric is an explicit zero with coverage,
not a missing/default value. All successful cases retain all4partitions,
all logical rows and zero disk-cache bytes across population/read snapshots.
No successful-case encoding counter increases during reads; no guard stops.

Fixed conditions: target1GiB, four tasks/two slots/two waves; task sizes below
are canonical logical bytes per task, total volume4x. Cold population1sample;
read medians5samples after1warmup per full/id query. These are whole Spark action
times. GPU-enabled jobs throughout; default Spark uses CPU cache encode/decode
feeding GPU query operators, not a GPU cache serializer. Population code is
unchanged; do not attribute population differences to output-target changes.
Audited encoded payload includes nvCOMP/PCBS framing; default Spark excludes its
separate statistics row. Encoded sizes are actual observations, not guaranteed
bit-identical between runs. Stored Spark resident sizes are estimates and can
differ from audited payload bytes.

| Task MiB | Format | Population s | Full read s | Id read s | Encoded MiB |
| ---: | --- | ---: | ---: | ---: | ---: |
| 256 | PCBS | 1.476 | 0.278 | 0.160 | 489.7 |
| 256 | Snappy | 1.675 | 0.242 | 0.160 | 520.3 |
| 256 | ANS | 1.480 | 0.263 | 0.180 | 572.1 |
| 256 | Zstd | 1.710 | 0.213 | 0.149 | 385.8 |
| 256 | LZ4 | 1.534 | 0.208 | 0.145 | 505.8 |
| 256 | Typed Cascaded | 1.831 | 0.400 | 0.324 | 392.4 |
| 256 | Uncompressed | 1.868 | 0.259 | 0.195 | 1024.3 |
| 256 | Default Spark | 19.893 | 0.847 | 0.214 | 415.7 |
| 512 | PCBS | 1.947 | 0.379 | 0.188 | 979.3 |
| 512 | Snappy | 2.294 | 0.298 | 0.189 | 1041.0 |
| 512 | ANS | 2.286 | 0.330 | 0.238 | 1156.9 |
| 512 | Zstd | 2.421 | 0.341 | 0.199 | 763.3 |
| 512 | LZ4 | 2.378 | 0.264 | 0.186 | 1016.5 |
| 512 | Typed Cascaded | 2.761 | 0.711 | 0.565 | 791.6 |
| 512 | Uncompressed | 2.286 | 0.381 | 0.290 | 2048.6 |
| 512 | Default Spark | 41.332 | 1.427 | 0.260 | 831.4 |
| 1024 | PCBS | 2.868 | 0.622 | 0.309 | 1958.6 |
| 1024 | Snappy | 3.994 | 0.477 | 0.271 | 2082.2 |
| 1024 | ANS | 3.693 | 0.577 | 0.422 | 2314.1 |
| 1024 | Zstd | 4.415 | 0.483 | 0.349 | 1526.8 |
| 1024 | LZ4 | 3.840 | 0.448 | 0.271 | 2033.5 |
| 1024 | Typed Cascaded | 4.480 | 1.219 | 1.060 | 1559.3 |
| 1024 | Uncompressed | 4.775 | 0.578 | 0.467 | 4097.3 |
| 1024 | Default Spark | 80.520 | 2.687 | 0.342 | 1662.8 |
| 2048 | PCBS | 4.381 | 1.089 | 0.516 | 3917.4 |
| 2048 | Snappy | 6.636 | 0.808 | 0.533 | 4158.0 |
| 2048 | ANS | 6.476 | 0.917 | 0.697 | 4639.1 |
| 2048 | Zstd | 7.586 | 0.841 | 0.568 | 3067.5 |
| 2048 | LZ4 | 6.791 | 0.747 | 0.559 | 4048.2 |
| 2048 | Typed Cascaded | 7.947 | 2.308 | 2.017 | 3122.1 |
| 2048 | Uncompressed | HEAP OOM | — | — | — |
| 2048 | Default Spark | 170.260 | 4.893 | 0.446 | 3325.6 |

At2GiB/task all six successful GPU serializers and default Spark report actual
task-attributed GPU->host spill0, GPU->disk spill0, Spark execution spill0 and
retry/split-retry0; Spark cache disk bytes0. This is a common-case throughput
measurement on the fixed setup, not validation of active spill/retry behavior.
Raising input per task does not force a GPU working set of that entire size.
Maximum observed GPU concurrency reaches2, but default Spark reads sometimes
reach only1 (e.g.all measured1GiB fullreads and2GiB idreads); configured slots
are not proof of sustained overlap. No best-case full-concurrency claim.

| 2GiB/task format | Peak sampled process-tree RSS GiB | Whole-job RMM tracked high water GiB |
| --- | ---: | ---: |
| PCBS | 9.065 | 12.051 |
| Snappy | 9.901 | 5.639 |
| ANS | 9.341 | 6.103 |
| Zstd | 6.837 | 9.080 |
| LZ4 | 8.596 | 5.639 |
| Typed Cascaded | 8.197 | 5.639 |
| Default Spark | 19.338 | 8.186 |
| Uncompressed FAILED | 11.757 | unavailable |

RMM high water excludes unused pool reservation and allocations outside its
tracking; includes population and query work, not a codec-only/per-read peak.
RSS is sampled, not allocator accounting. Failed-run RSS is not comparable to
a completed cached dataset and does not establish lower memory requirements.
Prototype output counters:4/4/4/12outputs per query at256/512/1024/2048MiB per
task for every successful packed codec. The last case has near-target outputs
and small tails; source groups exact full-schema packed sizes. PCBS/default
output counts are not directly instrumented here.

### Uncompressed heap failure and bounded diagnosis

Uncompressed2GiB/task failed in population (exit52), not decompression or GPU
allocation. Result FAIL/readsempty/no population_spill; do not report zero spill
for this failed case. Actual recorded JVM heap maximum16GiB. No RSS guard stop;
GPU was idle after process cleanup. Stack: java.lang.OutOfMemoryError:
Java heap space at ByteBuffer.wrap -> wire line233, following newbytearray
allocation at232. The wrapper is small, so this proves heap exhaustion, not
that the particular failing allocation itself was the large array.

Root preserved artifacts and continued independent default controls without
raising caps, changing inputs, rerunning the OOM or attempting a heap repair.
Read-only reviewer independently verified failure artifacts and all6successful
2GiB GPU-format spill coverages (9population/5eachread taskattempts, all explicit0).

Likely mechanism: successful1GiB uncompressed audit gives4195609bytes/record
(4MiB+1305). The same Java17 -Xmx16g defaults report G1 with8388608byte regions.
G1 humongous allocation uses whole regions for arrays above half-region size;
see https://docs.oracle.com/en/java/javase/17/gctuning/garbage-first-g1-garbage-collector1.html.
The expected2048records at2GiB/task carry8.0025GiBpayload but can occupy16GiB of
G1 region capacity before ordinary objects and temporary allocations.
This is consistent with the failure; failed-process GC telemetry/heap dump
was not captured, so do not claim a complete live-object census or sole cause.

Root ran a safe, tiny standalone allocation check after GPU jobs ended:
HeapArrayFootprint.java under /data/tmp/nvcomp-pcbs-rebuild-01, sameJava17,
-Xmx16g,10retainedarrays only, no pressure/OOM search:
- array4195609bytes: heap-used delta83885672bytes (~80MiB for~40MiBpayload).
- controlarray4193280bytes (below4MiB): delta41939080bytes (~40MiB).
Both reported G1region8388608 andheapMax17179869184. This reproduces the
representation's heap amplification on the same JVM defaults without exhausting
memory; it is not GC tracing of the failed process. Initial standalone javac
from the main cwd hit an unrelated Unicode filename/locale issue; compiling
from the ASCII scratch directory fixed it, and the generated main-cwd javac
arguments file was removed. No unrelated repository files changed.

Final verification: final-target-matrix.py PASS31+recognizedFAIL1 with403phases/
2139taskattempts; verify-target-run.py emits every successful verified-summary;
13source-proof tests PASS; CPU spill lifecycle/zero/unavailable/failure test PASS;
planner1GiB/odd/tiny target checks PASS; bounded GPU edge/cleanup probe PASS.
Independent review covered target planning, ownership/retries and spill semantics,
including root's metadata/error fixes. All GPU jobs ended and GPU idle. No
production source/POM/shim/profile changes, no published PR, no changed budgets.

Remaining limits: active GPU spill/retry behavior and large-string performance
not exercised; CPU fallback remains intentionally unsupported. Uncompressed
heap representation needs investigation before calling it robust for this size.
No repair, GC tuning or larger heap was silently applied. Architect alone may
accept/mark DONE.

## Direct multi-boundary GPU packing — implementation READY FOR REVIEW

Latest user priority: optimize GPU first, assess whether CPU support is worth
building, then compare CPU-only row-major->column-major encode/decode. Root
briefly misread the CPU clarification as changing the immediate order; user
corrected it before any CPU-path code was written. No CPU implementation/wire
change was made. CPU fallback still throws. Host CPU profiles of GPU jobs below
must not be presented as CPU-only cache encode/decode throughput.

Root re-read tracker/decisive code and maintained MCP docker_exec-only project
operations. Read-only worker confirmed MCP-only use at handoff/resumption/
checkpoints; no worker GPU/build jobs. Production code, memory budgets and
released contracts unchanged. Tools are UNRELEASED, so changed the codec input
signature and sole caller directly without compatibility adapters. No shim,
profile, POM or production operator change; Spark3.5.5-local validation only.

Why minimal repair: the previous writer called ColumnVector.subVector for every
column/record (owned copies), then contiguousSplit() on that already sliced table.
cuDF exposes contiguousSplit(allRowBoundaries), packing directly from the original
table. This adapts working APIs and retains existing codec/wire rather than
replacing the cache design. Pinned JNI javap confirmed multi-boundary API.
Existing PCBS large-table/row-group path and archived population profiles showed
the mismatch:1330PCBS versus36858packed-Snappy kernels, including31992validity
slicing/counting kernels on the packed path. Both already used batched compression.

Encoder now packs the parent with one multi-boundary call per attempt. Average
row-size estimates initialize boundaries; if any packed output exceeds8MiB,
all trial outputs close and row interval halves before retrying. This deliberately
preserves the existing record bound rather than silently accepting oversize.
For fixed-wide data, layout overhead causes a rejected initial attempt; profiles
must include it rather than claiming one successful pack with zero trials.
Variable-width skew may require several whole-parent passes.

Each accepted piece retains copied metadata, typed layout, rowcount and a
SpillableBuffer. Raw ContiguousTable owners close, leaving independent buffers
spillable; parent closes before compression. The codec now borrows restored
device buffers plus retained metadata/layout, avoiding a second pack after
spill restoration. Existing248MiB compression grouping/chunks/wire and1GiB
decoded target stay fixed to isolate this GPU packing change. Source-parent+
all packed outputs/scratch coexist during a native pack call; this can raise
transient memory before outputs become spillable. No hardware-portability or
strict4xT memory guarantee is established by the fixed24GiB tests.

Worker found/fixed handle-append exception safety: closeOnExcept(spillable)
guards PackedRecord allocation/collection insertion. Root inspected all code
and independently built/reran. Cleanup closes both queued spillable packed
pieces and remaining parent on early completion/failure. Worker reviewed the
final diff and found no remaining substantive issue; git diff --check passed.

Strengthened probe covers exact ordered2000rows with clustered64KiB strings,
smaller strings, nulls and empty values, forcing multiple packed pieces and
oversize refinement; every emitted header is<=8MiB and totalrows exact.
Existing20k-row/~80MiB fixture now checks all id order plus sum(id)/stringlength,
in addition to prefetched-output TaskEnd GPU/managed-host cleanup. Previous
edge/projection/duplicate/diskbytes/CPU-rejection checks retained.
Build31 initial probePASS; build32 caught Scala array/range inference in a new
assertion (no successful artifact), fixed to an indexed value check. Final
build33 + multisplit-snappy-probe-strengthened PASS. No test requirements were
weakened; skewed boundary coverage was added following review.

Bounded comparison: same fixed-wide fixture,4tasks/local2/two waves,16GiBheap,
32GiBhost incl2GiBpinned,24GiBGPU and64GiBRSS guard. One cold population,
one warmup+5measured reads. All11new unprofiled runs PASS with verify-target-run.py
after each, complete explicit spill/retry counters0, allrows resident/disk0,
same expected aggregates and source origins. Old target-final cohort is before;
PCBS is freshly rerun unchanged control.

| Task MiB | Format | Population before s | Population after s | Full after s | Id after s | Encoded after MiB |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 256 | PCBS control | 1.476 | 1.471 | 0.260 | 0.147 | 489.7 |
| 256 | Snappy | 1.675 | 1.229 | 0.204 | 0.129 | 521.2 |
| 256 | LZ4 | 1.534 | 1.170 | 0.208 | 0.151 | 508.7 |
| 256 | Zstd | 1.710 | 1.382 | 0.232 | 0.150 | 382.9 |
| 256 | ANS | 1.480 | 1.100 | 0.237 | 0.168 | 575.5 |
| 256 | Typed Cascaded | 1.831 | 1.319 | 0.409 | 0.342 | 395.3 |
| 256 | Uncompressed | 1.868 | 1.382 | 0.287 | 0.188 | 1024.3 |
| 2048 | PCBS control | 4.381 | 4.162 | 1.086 | 0.499 | 3917.4 |
| 2048 | Snappy | 6.636 | 3.727 | 0.823 | 0.531 | 4164.7 |
| 2048 | LZ4 | 6.791 | 3.884 | 0.775 | 0.498 | 4051.8 |
| 2048 | Zstd | 7.586 | 5.078 | 0.920 | 0.658 | 3028.6 |

Do not overstate cold single-sample rankings or claim reads improved: reader
code is unchanged; some timings vary/regress (Zstd2GiB0.841->0.920s). Dataset
aggregates/recordcounts stay consistent; payload bits/sizes need not match
because encode layouts/padding may differ. No CPU-only speed conclusion.

At2GiB Snappy/LZ4 tracked whole-run GPU high water is8.133GiB versus5.639GiB
before; source batching optimization trades higher transient memory for speed.
Zstd9.068GiB; fresh PCBS12.049GiB. Still no spill/retries under fixed settings.
RSS Snappy9.087/LZ48.193/Zstd8.049/PCBS9.061GiB. Pressure behavior remains
unverified, and known uncompressed2GiB heap failure was not rerun/repaired.
Artifacts /data/tmp/nvcomp-spark-probe/multisplit-{codec}-fixed-{256,2048},
build33. Missing larger sizes/codecs were not silently claimed as retested.

Separate GPU population tracing and host CPU profiling are completed below.
No CPU encode/decode implementation, layout redesign, projection or predicate-
pushdown work was performed.

## Multi-boundary packing profiles — READY FOR REVIEW

Source checkpoint e11dd37272 contains the implementation, strengthened probe
and initial timing evidence together. Final diagnostics use the same build33
and pinned production artifacts. Root and the focused read-only reviewer
continued using MCP docker_exec exclusively; no worker GPU/build execution.

Host CPU profiles: multisplit-cpu-snappy-256 and multisplit-cpu-pcbs-256 under
/data/tmp/nvcomp-spark-probe, with result.json, cpu-analysis.json and JFRs.
These are GPU-enabled jobs, NOT CPU-only serialization. Root independently
reconstructed ThreadMXBean deltas from raw before/after thread snapshots,
summing executor task-worker CPU and excluding read warmups. Reviewer reproduced
the population values independently.

| Format | Population task CPU ms | Full-read mean task CPU ms | Id-read mean task CPU ms |
| --- | ---: | ---: | ---: |
| Optimized Snappy | 1248.377 | 192.705 | 154.173 |
| Fresh PCBS control | 1744.747 | 297.354 | 167.352 |

Prior source-correct Snappy population task CPU was2218.270ms, so observed
task CPU fell44%; optimized Snappy used28.5% less than fresh PCBS. Population
is a single cold observation and includes JIT/startup variability. CPU sums
across task threads are not wall time; these data establish no CPU-only path
performance or exact wall-time decomposition.

GPU diagnostic: multisplit-profile-snappy-256/{profile.sqlite,analysis.json,
profile.nsys-rep,result.json}. Compare profile-current-snappy-256, the earlier
source-correct diagnostic. Root directly queried SQLite phase windows scoped
to the JVM process; reviewer independently reproduced counts and copy volume.
No kernels crossed the checked seven phase-window boundaries.

| Population diagnostic | Before | Optimized |
| --- | ---: | ---: |
| Kernel events | 36858 | 1594 |
| Summed kernel ms | 222.450 | 170.897 |
| Snappy compression launches | 8 | 8 |
| Summed Snappy compression ms | 139.200 | 143.235 |
| Packing copy kernels | 260 | 8 |
| Summed packing copy ms | 4.849 | 7.027 |
| Device-to-device memcpy MiB | 1561.346 | 520.643 |
| Device-to-host memcpy MiB | 524.348 | 525.042 |
| CUDA runtime API union ms | 1008.517 | 458.789 |
| Profiled population wall ms | 1917.322 | 1309.717 |

The31992 removed old slicing/mask-related kernels comprise four families:
subtract boundaries8060, segmented_count_bits8060, segmented_reduce8060,
copy_offset_bitmask7812. This does not mean validity processing disappeared;
multi-boundary packing performs its work in larger operations. Eight new
packing launches reflect four parents with initial oversize trial plus refined
successful trial. Compression work itself stayed approximately unchanged.
Reduced small operations, redundant copies and host submission/bookkeeping
explain the gain; faster compression is not the explanation.

CUDA API union, summed kernels and memcpy intervals overlap and cannot be
added into an exclusive wall-time budget. Memcpy counters omit data movement
inside kernels. Whole population includes upstream/query work; the profiler
perturbs timing, so use the unprofiled table for headline elapsed results.

Final independent verification: python3.9
/data/tmp/nvcomp-pcbs-rebuild-01/verify-multisplit.py PASS, reproducing all11
successful unprofiled acceptance checks, raw CPU arithmetic and direct SQLite
kernel/copy metrics. Focused final evidence review found no contradictions.
No further GPU runs were needed. At checkpoint nvidia-smi showed no compute
processes and ps showed no benchmark/build jobs. Tracked changes are limited
to this checkpoint; unrelated untracked user files remain untouched.

Remaining limits: no active-spill/retry behavior established by these no-spill
runs; whole-parent packing's higher transient memory remains a pressure risk.
Large-string correctness is covered, not representative string performance.
The full old32-case matrix was not rerun after packing; this bounded11-run
comparison is the verified scope. Known uncompressed2GiB heap failure remains.
CPU-only encode/decode is deferred pending the user's next scope decision.
Implementation and profiling are READY FOR REVIEW, not architect-accepted DONE.

## CPU versions authorized — reusable codecs READY FOR REVIEW, layout decision pending

User now asks to start CPU versions and explicitly requires reuse of expensive
compression/decompression codec instances, with Parquet lifecycle investigated.
This supersedes historical GPU-only planning, not the existing explicit rejection
behavior until a verified CPU table path is implemented. No production serializer
or GPU implementation changes in this checkpoint.

Release classification: CpuPackedCodec, its standalone checks and interop probe
are new UNRELEASED tools absent from main/release artifacts/documented production
configuration. No compatibility adapters needed. Released PCBS contracts remain
unchanged. Same experimental int/long/string scope; no new user configuration,
POM, shim/shared production shim caller or source-selection changes. Shim
preflight remains NOT_APPLICABLE to these isolated Java tool additions.

All project operations used MCP docker_exec; discovery used code-index first
for covered files. Both worker handoffs and review checkpoints reconfirmed
MCP-only use. Root read decisive source and matching-runtime bytecode rather
than accepting summaries; root alone ran GPU prerequisites serially.

Parquet evidence from runtime parquet-hadoop1.13.1 matching build33/35:
CodecFactory retains wrappers in per-codec maps. HeapBytesCompressor and
HeapBytesDecompressor borrow from Hadoop CodecPool, reset per operation and
return instances on release. Important exception: Parquet ZstandardCodec's
createCompressor/createDecompressor return null; its streams use recycling
buffers, not that pooled compressor/decompressor-context mechanism. Matching
classes were inspected with javap -c -p. CPU PCBS getRecordWriter selects
CompressionCodecName.UNCOMPRESSED (ParquetCachedBatchSerializer.scala1508);
the active CPU row writer calls it at1260. Future comparisons must label actual
codec/size, not assume CPU PCBS Snappy from GPU defaults.

CpuPackedCodec is a thread-confined AutoCloseable owner for one task iterator.
It lazily creates at most one ZstdCompressCtx and one ZstdDecompressCtx and
retains them across chunks/records. LZ4 uses the cached factory implementations;
Snappy uses its static native implementation and caller-owned arrays. Reuses
compression scratch and aggregate output capacity; decoder writes directly to
the final output array. Task completion/error cleanup wiring remains part of
future Spark integration; the helper currently requires caller ownership.
Close is idempotent and frees contexts/references. ANS/Cascaded CPU requests
throw explicitly; no installed CPU implementation was established.

Exact matched runtime libraries: lz4-java1.8.0, snappy-java1.1.10.5,
zstd-jni1.5.5-4. Framing follows cuDF BatchedCompressor/BatchedDecompressor:
ceil(rawBytes/chunkBytes) little-endian64-bit encoded sizes at the front, then
raw codec chunks concatenated. No Hadoop stream/LZ4 frame substituted. Decoder
checks size-vector/payload coverage, bounds and exact per-chunk output lengths.

Verification: build34 passed initial CPU and actual GPU/CPU interop prerequisites.
Focused review identified a root-written probe factory-after-incRef exception
leak: codec construction could throw before ownership transfer. Fixed by creating
GPU codec wrappers once per codec before buffer allocation/ref increments.
Added wrong nonzero decoded lengths and valid-envelope/corrupt-payload cases
following review. Final full tools build35 PASS with original pinned production
artifact/source proof; pre-existing broken classpath links and unused-resource
warnings remain, no new compile errors. No production artifacts rebuilt.

Root independently ran final CpuPackedCodecTest PASS: four CPU codecs,
empty/one-byte/exact chunk/tails/multi-chunk, random/repeated bytes, nonzero array
offsets, borrowed inputs unchanged, independent returned arrays, size corruption,
truncated/extra payloads, wrong raw lengths, corrupt codec bytes, Zstd identity
reuse, cross-thread rejection and use-after-close checks.

Root independently ran CpuCodecInteropProbe from build35 PASS:42 fixtures,
each checked both GPU encode->CPU decode and CPU encode->GPU decode (84 paths).
Three codecs x7sizes (1,7,257,65535,65536,65537,196615) x2data patterns.
GPU allocations returned to zero after each fixture; default RMM with only tiny
prerequisite buffers, no pressure/performance test. Artifact:
  /data/tmp/nvcomp-spark-probe/build-35/cpu-gpu-interop.json
includes exact Java command, exit0, logs and provenance SHA. Final source build:
  /data/tmp/nvcomp-spark-probe/build-35/spark-provenance.json
No compression-ratio or Spark CPU-throughput claim from these byte tests.

Pending architectural choice: GPU records carry opaque cuDF packed metadata.
Existing Java host APIs inspected do not directly provide CPU packed-table
encode/decode for that format; JCudfSerialization uses a different layout,
and HostTable's constructors copy existing GPU tables. Silently substituting
that format or copying a private ABI into Scala would change what is measured.
Preferred approach proposed to user: small native host-layout bridge using
cuDF's metadata APIs. Alternative explicitly offered: temporary version-pinned
JVM layout for exploration. Await choice before implementing table layout.

Matching-source prerequisite: pinned JNI artifact revision
6740b5ee74cd1c0d35c42422fb4cf0d8cfa322eb, cuDF revision
b9539f2b7cf98d0d6229a3fb46cec7f800331158, from jar version properties.
Mounted JNI HEAD0084b98500efa59ba809d5ae5eadc685df84082d and vendored cuDF
HEADaf84a550cbc9592581ced8ffe9fa5b03a8c4c818 differ; git cat-file cannot find
the pinned commits in mounted JNI, vendored cuDF or standalone cuDF. Metadata
format demonstrably differs between mounted cuDF trees (stub column versus
serialized_table_header), reinforcing need for exact-source/API verification.
User asked asynchronously for matching writable JNI worktree/source or explicit
temporary-layout preference; independent codec work completed meanwhile.

CPU row-major->column-major packing, row decoding, cross CPU/GPU table roundtrips,
task-lifetime integration and CPU/default/PCBS timing+size matrix remain
UNIMPLEMENTED/UNVERIFIED. Existing Spark prototype still throws on CPU fallback.
Only codec prerequisite is READY FOR REVIEW. No claim of completed CPU serializer.

## CPU row → columnar → packed/compressed cache — READY FOR REVIEW

User explicitly selected the native bridge and corrected the implementation
direction: MUST reuse existing row-to-columnar conversion; then convert those
columns into packed/compressed batches. Exact packing implementation is flexible
for this proof of concept; all datatypes are not required. Separate column
compression/indexing remains a future direction, not implemented here.

Release/compatibility classification unchanged: all additions/edits are
UNRELEASED experimental tools, absent from main/release artifacts/documented
production configuration. No compatibility aliases/migrations. Frozen released
PCBS/plugin artifacts and contracts unchanged. No POM, versioned shim, source
selection or production shared shim caller changed; preflight NOT_APPLICABLE
under the recorded isolated Spark3.5.5-local boundary. This is not a production
serializer or distributed deployment.

Root and workers used MCP docker_exec exclusively for all project operations;
code-index first for covered discovery. Every handoff/resume/review checkpoint
reconfirmed MCP-only use. Workers did bounded native bridge/launcher work and
focused reviews; root read decisive source, inspected changes and independently
built, tested and verified saved numbers. Only root ran serial GPU/Spark jobs.

Implementation:
- CpuCachePath reuses Spark ArrowWriter (ArrowWriter.scala31-118) to convert
  schema-specific InternalRows to Arrow columns. No custom row-value converter.
  Unique internal Arrow names preserve duplicate cached field names/order.
- Bounded row input forms at most8MiB packed records. The row-size upper bound
  accounts for fixed slots, UTF8 bytes, offsets, validity and alignment. A row
  retained across iterator advancement is copied because Spark codegen may
  overwrite it. One row exceeding the existing record bound rejects explicitly.
- Arrow column buffers are copied once into aligned packed heap bytes; no
  intermediate full-table GPU split. Native host bridge creates cuDF metadata
  borrowing these bytes, then existing CpuPackedCodec compresses64KiB chunks.
  Metadata/wire remains interoperable with GPU records.
- Snappy/LZ4/Zstd and uncompressed CPU paths support int/long/string/nulls.
  Context owner is per task iterator, reusing native Zstd contexts across
  chunks/records; LZ4/Snappy use retained/static implementations. Completion,
  exhaustion and errors release codec contexts and Arrow roots. No per-record
  codec construction.
- CPU reads decompress into packed heap bytes, parse metadata using the bridge,
  copy buffers into Arrow columns, then use Spark ColumnarBatch rows plus
  UnsafeProjection. CPU columnar output remains advertised unsupported, so
  Spark chooses its row scan. A direct CPU columnar-output route is not built.
  CPU columnar input currently uses its producer-owned row iterator and the
  same ArrowWriter; this route has not received an independent direct-input test.
- Native HostPackedBridge uses pinned libcudf host pack_metadata and bounded
  unpack(std::span,...) APIs, not a JVM copy of private metadata. Validates
  type/row/null counts, fixed buffers/masks, string offsets/ranges and absent
  empty chars. Explicit null counts avoid GPU null-count work.
- Reused public view headers are from mounted sources rather than exact pinned
  revision. Actual view ABI/API smoke and CPU/GPU table values pass; headers
  and runtime are hash-pinned. Bounded unpack's exported declaration is absent
  in the mounted header and is forward-declared with its tested signature.
  This is a proof-of-concept build limitation, not a general ABI guarantee.
- Own trivial JNI_OnLoad prevents inheriting libcudf's Java registration
  entrypoint when loading the standalone bridge without cuDF Java classes.

Actual copies are not hidden: row-to-Arrow field writes use existing Spark
code; Arrow->packed is one raw copy; compression uses reusable chunk/output
scratch plus copies into the returned encoded array and final cache framing.
CPU read is decompression->packed, packed->Arrow copy, then Spark row output.
Native metadata operations copy small metadata/descriptors, not whole raw
buffers explicitly; GetPrimitiveArrayCritical may copy depending on JVM.
No Arrow IPC intermediate. No claim that this is minimum-copy CPU encoding.
Whole-record compression still prevents skipping unselected compressed columns;
raw metadata describes columns but separate compressed-column indexing is pending.

Build/provenance:
host_build.py extracts only pinned libcudf.so to a shared hash-addressed cache,
builds the small bridge with current public headers, and hashes all headers
from BOTH bridge and RMM translation units. Review found/fixed a missing RMM
cuda/std/version dependency. Spark build/run manifests bind the bridge, library,
headers and source; launchers enforce hashes. No replacement/rebuild of pinned
production artifacts. Added explicit CPU population/read flags and separate
CPU/GPU correctness probe selection; default benchmark behavior remains GPU.
Root independently reran7mocked launcher cases,9invalid combinations,13source
proof tests; all PASS.

Verification:
- Build36 caught an Arrow version mismatch (ArrowBuf.setMemory does not exist);
  corrected to the available setOne API. Build37 compiled. Build38 passed all
  four CPU-supported codecs' actual Spark CPU->CPU,CPU->GPU,GPU->CPU,GPU->GPU
  table roundtrips with exact rows/nulls/UTF8/order, duplicate/reordered
  projections, count-only and multi-record strings.
- Focused review found/fixed Arrow root allocation-failure ownership:
  create/guard VectorSchemaRoot before ArrowWriter.create(root). Unique Arrow
  field names fixed duplicate cached names. Input CPU batches stay producer-owned.
- Final build39 includes stronger Snappy probe: partial-read TaskEnd Arrow memory
  returns to baseline, duplicate cached names, empty and zero-column caches.
  Root independently ran final probe PASS.
- Existing strengthened GPU Snappy probe PASS after replacing obsolete CPU
  rejection assertions for supported codecs with actual CPU reads; unsupported
  CPU codecs retain explicit rejection. Exact disk bytes, skewed large strings,
  multi-record concatenation and GPU/managed-host early cleanup remain checked.
- Root reran native metadata smoke with CUDA_VISIBLE_DEVICES empty and no cuDF
  Java classes using build39 bridge; PASS int/long/string/nulls and malformed
  bounds/counts. Native bridge operates without a visible GPU in this test.
- Root independently reran CpuPackedCodecTest PASS. Earlier both-direction
  chunk interop remains valid; full table interop now adds actual Spark evidence.

First bounded performance cohort (not all-size sweep):
4tasks/local[2]/two waves,256MiB canonical logical/task =1GiB total, fixed-wide
deterministic values. CPU population for EVERY row in this table. One cold
population sample;1warmup+5measured full and ID-only reads in each execution mode.
16GiBheap,32GiBmanagedhost including2GiBpinned,24GiBGPU,64GiBsampledRSS guard;
actual initialized pools checked. No budget changes. Times are whole-action
wall seconds, reads are independent warmed medians. GPU columns below read
the CPU-written cache, not the previous GPU-written cache.

| Format | CPU population s | CPU full s | CPU ID s | GPU-mode full s | GPU-mode ID s | Encoded MiB total |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Default Spark | 19.076 | 1.465 | 0.075 | 0.727 | 0.109 | 415.7 |
| PCBS CPU uncompressed Parquet | 13.339 | 5.580 | 4.141 | 0.309 | 0.151 | 701.2 |
| Packed Snappy | 2.719 | 2.509 | 0.904 | 0.225 | 0.142 | 517.6 |
| Packed LZ4 | 2.807 | 2.511 | 0.835 | 0.206 | 0.145 | 506.0 |
| Packed Zstd | 3.963 | 2.402 | 0.802 | 0.246 | 0.147 | 393.8 |
| Packed uncompressed | 2.562 | 2.274 | 0.643 | 0.255 | 0.177 | 1023.7 |

Default requested-GPU mode still uses a CPU cache reader (observed plan), with
GPU-enabled downstream execution. All other GPU-mode rows have actual
GpuInMemoryTableScan. Do not label default as native GPU decoding.
PCBS CPU explicitly uses UNCOMPRESSED Parquet codec, still benefiting from
Parquet encoding/dictionaries. Default payload excludes its separate statistics
row; PCBS/packed payload includes framing/metadata. Values are emitted payload
bytes from post-run audit, not serializer-defined default uncompressed stats,
resident accounting or process memory. No new compression-ratio assumption.

All6runs PASS, all4partitions resident/disk0, exact aggregates match source
and each other; all6runtime class origins and source/dependency hashes checked.
Packed records132each, PCBS136, default420. Root read verifier implementation
and independently ran /data/tmp/nvcomp-pcbs-rebuild-01/verify-cpu-first.py PASS:
150phases/774task attempts,360with RAPIDS metrics. Spark spill counters observed
and0; available GPU spill/retry counters0. RAPIDS counters are unavailable for
78CPU-only phases (population+12CPU reads each), not assumed zero. No pressure
or active-spill behavior established.
Whole-process sampled RSS GiB: default8.591,PCBS9.426,Snappy5.992,LZ45.181,
Zstd5.205,uncompressed5.511. These include mixed CPU/GPU read stages and startup,
not CPU-population-only peaks. Arrow native buffers are not RAPIDS host-budget
allocations; record bounding and cleanup constrain temporary use. No total
native/process hard memory cap is claimed.

Interpretation limited to this fixture/cohort: CPU population and reads beat
PCBS, but default Spark CPU reads remain faster, especially projected ID-only.
Whole-record decoding and row output remain material opportunities; no
unprofiled attribution of all remaining CPU time. Zstd produces the smallest
packed cache here with a higher CPU population cost. Population rankings are
single cold observations, not confidence intervals. GPU CPU-written-cache reads
remain fast; do not substitute them for earlier GPU-populated results.

Artifacts /data/tmp/nvcomp-spark-probe/cpu-first-{format}-fixed-256,
cpu-spark-{codec}-probe-01, cpu-spark-snappy-probe-final,
cpu-enabled-gpu-snappy-probe-01, build39 (build-39 directory).
No CPU512MiB/1GiB/2GiB performance sweep or representative string performance
matrix run yet. ANS/Cascaded CPU remain unsupported. Separate-column compression,
predicate pushdown, CPU columnar output, production integration and pressure
testing are outside this completed bounded checkpoint. Architect alone marks DONE.

## CPU scaling to512MiB/1GiB/2GiB per task — READY FOR REVIEW

User requested scaling the successful CPU cohort. Reuse finalbuild39 unchanged:
same6formats,CPU population,CPU/GPU read modes,4tasks/local2/two waves,1warmup
and5measured full/ID reads. Extend to512/1024/2048MiB/task, total2/4/8GiB.
No implementation/configuration/resource-budget changes.16GiBheap,
32GiBmanagedhost including2GiBpinned,24GiBGPU and64GiBsampledRSS guard.
Longer900s job timeout accommodates scale; it is not a memory/algorithm cap.
Root owns serial jobs. Review each size before proceeding to the next; stop
failed cases without changing caps, inspect saved failure evidence and continue
only independent safe work.

Release classification unchanged: experimental tools UNRELEASED; production
frozen released contracts unaffected. No source changes or compatibility paths.
All project operations MUST continue through MCP docker_exec; root and verifier
worker confirmed this at handoff/checkpoint. No worker GPU/build/benchmark jobs.

Success requires correct aggregates, complete execution-mode/timing observations,
pinned provenance and budget evidence. Spill/disk/eviction are measured outcomes,
not forced failures. Do not carry previous all-resident/zero-spill assumptions
into a scale verifier. Preserve metric arithmetic/coverage and explicitly
distinguish unavailable CPU RAPIDS metrics from observed zero. If eviction
causes recomputation, label timings/serializer accumulators accordingly;
post-run payload audit can recompute and is not a retained-memory statistic.

Safe prerequisite: current CPU uncompressed full record uses32752rows,
8,380,480packedbytes +1296metadata +72framing =8,381,848-byte arrays,
below8MiB. This differs from prior GPU4MiB+metadata arrays consuming8MiB G1
regions. Predicted2GiB/task CPU retained humongous regions are~8.03GiB total,
not~16GiB. No evidence of unavoidable old failure; larger heap pressure remains
empirical. No heap increase, OOM stress or algorithm change authorized/inferred.

First512MiB/task Snappy run completed exit0/no guard, resultPASS with all4
partitions resident/disk0. Full scale verifier/provenance crosscheck pending;
do not present the entire size matrix as completed.
Artifacts /data/tmp/nvcomp-spark-probe/cpu-scale-{format}-fixed-{512,1024,2048}.

### 512MiB/task cohort — READY FOR REVIEW

All6formats complete and independently verified with unchangedbuild39/settings.
Rootread scaleverifierdiff and reran fullcohort:150phases/774taskattempts,
360RAPIDSmetricattempts,78CPUphases explicitlyunavailable; allresident/disk0,
Sparkspills0 and observedGPUspill/retry0. Worker independentlyverifiedfirst5
whilePCBSwasrunning; rootverifiedPCBS and fullcrossformataggregate equality.
MCPdocker_exec-only confirmed atcheckpoint. Only root launched serialjobs.

|Format|CPU population s|CPU full s|CPU ID s|GPU-mode full s|GPU-mode ID s|Encoded MiB|RSS GiB|
|---|---:|---:|---:|---:|---:|---:|---:|
|default|39.987|2.802|0.107|1.167|0.142|831.394|12.308|
|pcbs|23.483|10.842|8.196|0.392|0.262|1402.372|8.414|
|snappy|5.432|5.038|1.760|0.320|0.254|1035.265|9.961|
|lz4|4.886|4.810|1.559|0.290|0.202|1011.914|6.295|
|zstd|7.598|4.808|1.563|0.291|0.209|787.642|6.764|
|uncompressed|4.511|4.472|1.208|0.359|0.249|2047.354|7.293|

GPU columns read CPU-populated bytes; default still usesCPUcache-reader fallback.
Payloadtotal covers2GiBlogicalinput across4tasks. Populationsinglecoldsample;
reads5samplemediansafter1warmup. No active-spill/pressure conclusion.
1GiB/task and2GiB/task cohorts remain IN PROGRESS; no source/cap changes.

### 1GiB/task cohort — READY FOR REVIEW

Recovered runningexec863 after modelcapacity interruptions; no completedcases
rerun. All6formats complete and independently rootverified with unchangedbuild39
and caps, crossformataggregates equal.150phases/774taskattempts,360RAPIDS
metricattempts,78CPUphases explicitlyunavailable; allresident/disk0, observed
Spark/GPUspills andGPUretries0. Workerindependentlyverifiedfirst5 and reviewed
heap/RSS before2GiB; rootverifiedfinalPCBS and completecohort.
MCPdocker_exec-only reconfirmed atresume/checkpoint, no workerjobs.

|Format|CPU population s|CPU full s|CPU ID s|GPU-mode full s|GPU-mode ID s|Encoded MiB|RSS GiB|
|---|---:|---:|---:|---:|---:|---:|---:|
|default|77.226|5.300|0.153|2.119|0.319|1662.788|16.783|
|pcbs|45.424|21.883|16.556|0.724|0.349|2804.713|10.922|
|snappy|9.044|9.986|3.515|0.424|0.283|2070.394|10.555|
|lz4|8.502|9.590|3.142|0.435|0.293|2023.840|6.755|
|zstd|13.979|9.201|3.023|0.397|0.311|1575.045|7.349|
|uncompressed|8.553|8.726|2.354|0.584|0.443|4094.703|10.228|

GPU reads CPU-populated cache; default stillCPUcache-reader fallback.
Encodedtotal covers4GiBlogicalinput across4tasks. Samecoldpopulation and
5readsamplemedians. No pressureoractive-spillvalidation claim.
2GiB/task remains IN PROGRESS. CurrentCPUrecordlayout differsfromoldGPU
heapfailure; measuredsnapshots and regionarithmetic support proceeding with
unchangedsettings, not a guaranteedfit.

### 2GiB/task Snappy — verified partial checkpoint

Snappy alone is READY FOR REVIEW in this partial checkpoint. The entire
2GiB/task cohort is NOT complete: remaining LZ4, Zstd, uncompressed, default
and PCBS formats remain IN PROGRESS for cohort acceptance. Root-owned serial
jobs are ongoing; this checkpoint does not claim their outcomes or rerun them.

Same frozen build39, fixed-wide fixture, CPU population, four tasks/local[2]/
two waves, one warmup plus five measured full/ID reads per execution mode.
Unchanged16GiBheap,32GiBmanagedhost including2GiBpinned,24GiBGPU and
64GiBsampledRSS stop guard. Experimental tools remain UNRELEASED; frozen
released production contracts unchanged. No code/configuration/acceptance
change. Root and worker reconfirmed MCP docker_exec-only operations at resume
and checkpoint; worker performed only read-only verification and this
explicitly authorized documentation checkpoint, no GPU/build/benchmark jobs.

|Format|CPU population s|CPU full s|CPU ID s|GPU-mode full s|GPU-mode ID s|Encoded MiB total|RSS GiB|
|---|---:|---:|---:|---:|---:|---:|---:|
|Snappy|17.540|19.811|7.061|0.828|0.509|4140.858|16.388|

Population exact17.539624819s; audited encoded bytes4,342,003,825 across
33,554,432rows/8GiBcanonical input. Peak sampled process-tree RSS
17,596,362,752bytes. Completion exit0/no guard, resultPASS; all four
partitions resident, disk0,1,028encoded records. Root independently verified
the case after launch; worker independently reran scale verifier PASS:
25phases/129taskattempts,60with RAPIDS metrics,13CPU phases explicitly
unavailable. Observed Spark spill and available GPU spill/retry counters0;
missing CPU RAPIDS counters are not interpreted as zero. Provenance,
runtime class origins, unchanged budgets and exact aggregates all pass.
Population remains a single cold sample; reads are warmed five-sample medians.
GPU-mode reads use actual GPU cache scans of these CPU-written bytes.

Observed decoder scaling is explained by stored-record granularity:
- Full CPU record32752rows has8,380,480packedbytes.128records occupy
  1,072,701,440bytes, leaving1,040,384bytes below the configured1GiB
  decoded-group target.
- At1GiB/task, the2048-row tail is524,032packedbytes and fits after128
  full records: one output group/task.
- At2GiB/task,256full records plus4096-row tail yield257records/task.
  Tail size1,048,064bytes exceeds remaining space after128full records
  by7,680bytes. The existing strict greedy grouping therefore emits three
  groups/task:128full,128full,small tail. No algorithm change was made.
- Saved per-GPU-read counter deltas confirm1GiB/task has4outputgroups/
  batches and8native decode calls across4tasks;2GiB/task has12groups/
  batches and12native calls. Concatenations increase4->8 because each
  single-record tail needs no concatenation.
- One1GiB output group still uses two native calls/task:64KiB-rounded
  chunk workspace for128full records consumes16384blocks, and its tail
  adds8more, exceeding the16384-block budget. At2GiB each128-record
  group fits one call, plus one call for its tail. Native calls therefore
  increase2->3/task, not1->3; output batches increase1->3/task.

Source inspection and process counters establish this fixed-wide grouping
effect; they do not establish an exclusive runtime cost attribution or a
general hardware limit. No optimization, cap adjustment, pressure conclusion
or architect DONE status is implied.
Artifacts /data/tmp/nvcomp-spark-probe/cpu-scale-snappy-fixed-2048 and
/data/tmp/nvcomp-pcbs-rebuild-01/verify-cpu-scale.py.

### 2GiB/task five-case partial cohort — READY FOR REVIEW; PCBS IN PROGRESS

Independent acceptance now covers default, Snappy, LZ4, Zstd and uncompressed,
superseding the prior Snappy-only checkpoint's completed-case coverage.
PCBS remains IN PROGRESS and the entire2GiB/task cohort is NOT complete.
Root-owned serial benchmark jobs continue; no completed case was rerun for
this mechanical checkpoint. No architect DONE status is implied.

Same frozen build39 and fixed-wide fixture: CPU population, four tasks/local[2]/
two waves, one warmup and five measured full/ID reads in CPU/GPU modes.
Budgets unchanged:16GiBheap,32GiBmanagedhost including2GiBpinned,24GiBGPU,
64GiBsampledRSS stop guard. Experimental tools UNRELEASED; frozen released
production contracts unchanged. No source/configuration/acceptance change.
Root and worker reconfirmed MCP docker_exec-only operations at resume and
checkpoint; worker performed read-only verification plus this explicitly
authorized tracker-only edit/commit, no GPU/build/benchmark jobs.

|Format|CPU population s|CPU full s|CPU ID s|GPU-mode full s|GPU-mode ID s|Encoded MiB total|RSS GiB|
|---|---:|---:|---:|---:|---:|---:|---:|
|default|160.721|11.225|0.280|4.014|0.405|3325.575|19.266|
|snappy|17.540|19.811|7.061|0.828|0.509|4140.858|16.388|
|lz4|17.444|18.540|6.162|0.956|0.602|4047.899|13.106|
|zstd|28.622|19.007|5.914|0.990|0.761|3149.886|16.138|
|uncompressed|18.339|23.630|7.127|0.978|0.769|8189.402|13.353|

GPU-mode reads use CPU-written cache bytes. Default's actual cache reader is
CPU fallback; four packed codecs use actual GPU cache scans. Encoded totals
cover8GiBcanonical logical input/33,554,432rows across four tasks; default
payload excludes its separate statistics row as before. Population is one
cold sample, reads are warmed five-sample medians. RSS is a sampled whole-job
process-tree peak, not a CPU-population or retained-cache memory measurement.

Root independently verified cases after launch. Worker independently reran the
five-case verifier with cross-format aggregates equal:125phases/645taskattempts,
300with RAPIDS metrics;65CPU phases explicitly unavailable. All four partitions
resident, disk0, every source aggregate/read mode/provenance/budget check passes.
Spark spill counters and available GPU spill/retry counters observed0; missing
CPU RAPIDS counters are not assumed zero. Packed records1028each;default3356.
Artifacts /data/tmp/nvcomp-spark-probe/cpu-scale-{format}-fixed-2048,
verifier /data/tmp/nvcomp-pcbs-rebuild-01/verify-cpu-scale.py.

Measured GPU working-set evidence at fixed1GiB decoded target:
Snappy tracked high-water4.123079->4.121127GiB from1GiB->2GiB/task,
LZ44.123079->4.121127GiB, Zstd9.105905->9.104490GiB. Maximum emitted
GPU batch bytes1,073,217,536->1,072,693,504. These observations support
target-bounded tracked GPU working memory for these codec/fixture/settings,
not a universal4xT guarantee or hardware-independent peak claim. RMM high-water
is cumulative across the JVM and excludes unused pool reservation and outside
allocations. Process RSS grows with the larger retained host dataset; it is
not evidence that the GPU working set doubled.

All four packed formats emit three output groups/task at2GiB as derived in the
Snappy checkpoint. Compressed Snappy/LZ4/Zstd use three native decode calls/task;
uncompressed uses no codec calls, only its copy path. The small final group and
chunk-rounded workspace budget remain measured behavior, not newly chosen caps.

Uncompressed2GiB succeeds with payload8,587,210,080bytes, exactly matching
the earlier CPU record-layout arithmetic; this is a different layout from the
historical GPU uncompressed heap failure. Its CPU full-read median grows
8.726->23.630s from1GiB->2GiB/task and ID read2.354->7.127s, more than linear.
This degradation is observed but UNPROFILED. Do not attribute it to GC, live
heap pressure, bandwidth or copying without further evidence. Heap-used
snapshots are not live-set or peak measurements. No active-spill/pressure
validation or total-memory-cap guarantee is established by these resident runs.

PCBS's remaining result and the complete six-format largest cohort require
independent verification and a later tracker update; do not present the whole
scaling experiment as complete from this partial checkpoint.

### Complete CPU scaling matrix — READY FOR REVIEW

All 18 new scaling cases passed. This final checkpoint supersedes the partial
statuses above, including PCBS. Completed cases were not rerun after model
capacity interruptions. Build39, implementation, concurrency and memory budgets
remained unchanged throughout. These are unreleased experimental tools;
released production contracts remain frozen and unchanged.

Root inspected the worker's tracker commits1d9f53ffd0 and dd79a883ab and reran
the full verifier with --require-complete:18 verified,0 failed/pending/invalid;
450 phases,2322 Spark task attempts and1080 attempts with RAPIDS metrics.
The234 CPU phases without RAPIDS metrics remain explicitly unavailable.
Correctness, cross-format aggregates, runtime class origins, source/dependency
hashes, execution modes, budgets and metric arithmetic all pass.
The final focused evidence review independently reproduced these results.

Root and workers confirmed MCP docker_exec-only operations at every resumption
and checkpoint. Only root launched serial benchmarks. Worker changes were
limited to the isolated verifier and explicitly authorized tracker checkpoints.
No implementation, POM, shim, source-selection or production configuration
changes were needed for scaling.

Every cache below was populated on CPU. Four tasks on local[2] provide two waves;
total logical input is four times the per-task size. The fixed-wide fixture has
256 canonical bytes per row. GPU columns measure reads of CPU-written bytes.
Default Spark's GPU-enabled queries still use a CPU cache reader; other formats
use actual GPU cache scans. PCBS CPU uses uncompressed Parquet, including its
encoding/dictionaries.

Times are whole-action seconds: one cold population sample, then independent
five-sample read medians after one warmup per mode/shape. Population and read-only
times are separate. The256MiB rows are the previous baseline, not rerun here;
root independently revalidated all24 saved cases.

| Task MiB | Format | CPU population s | CPU full s | CPU ID s | GPU-mode full s | GPU-mode ID s | Encoded MiB total |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 256 | default | 19.076 | 1.465 | 0.075 | 0.727 | 0.109 | 415.7 |
| 256 | pcbs | 13.339 | 5.580 | 4.141 | 0.309 | 0.151 | 701.2 |
| 256 | snappy | 2.719 | 2.509 | 0.904 | 0.225 | 0.142 | 517.6 |
| 256 | lz4 | 2.807 | 2.511 | 0.835 | 0.206 | 0.145 | 506.0 |
| 256 | zstd | 3.963 | 2.402 | 0.802 | 0.246 | 0.147 | 393.8 |
| 256 | uncompressed | 2.562 | 2.274 | 0.643 | 0.255 | 0.177 | 1023.7 |
| 512 | default | 39.987 | 2.802 | 0.107 | 1.167 | 0.142 | 831.4 |
| 512 | pcbs | 23.483 | 10.842 | 8.196 | 0.392 | 0.262 | 1402.4 |
| 512 | snappy | 5.432 | 5.038 | 1.760 | 0.320 | 0.254 | 1035.3 |
| 512 | lz4 | 4.886 | 4.810 | 1.559 | 0.290 | 0.202 | 1011.9 |
| 512 | zstd | 7.598 | 4.808 | 1.563 | 0.291 | 0.209 | 787.6 |
| 512 | uncompressed | 4.511 | 4.472 | 1.208 | 0.359 | 0.249 | 2047.4 |
| 1024 | default | 77.226 | 5.300 | 0.153 | 2.119 | 0.319 | 1662.8 |
| 1024 | pcbs | 45.424 | 21.883 | 16.556 | 0.724 | 0.349 | 2804.7 |
| 1024 | snappy | 9.044 | 9.986 | 3.515 | 0.424 | 0.283 | 2070.4 |
| 1024 | lz4 | 8.502 | 9.590 | 3.142 | 0.435 | 0.293 | 2023.8 |
| 1024 | zstd | 13.979 | 9.201 | 3.023 | 0.397 | 0.311 | 1575.0 |
| 1024 | uncompressed | 8.553 | 8.726 | 2.354 | 0.584 | 0.443 | 4094.7 |
| 2048 | default | 160.721 | 11.225 | 0.280 | 4.014 | 0.405 | 3325.6 |
| 2048 | pcbs | 89.353 | 41.663 | 32.262 | 1.154 | 0.655 | 5609.4 |
| 2048 | snappy | 17.540 | 19.811 | 7.061 | 0.828 | 0.509 | 4140.9 |
| 2048 | lz4 | 17.444 | 18.540 | 6.162 | 0.956 | 0.602 | 4047.9 |
| 2048 | zstd | 28.622 | 19.007 | 5.914 | 0.990 | 0.761 | 3149.9 |
| 2048 | uncompressed | 18.339 | 23.630 | 7.127 | 0.978 | 0.769 | 8189.4 |


All four partitions remained resident, with no disk storage or recomputation
observed. Spark spill and available GPU spill/retry counters were zero.
The scale verifier permits positive spill/eviction observations; no passing
criterion forced these values to zero. Actual spill/recovery behavior remains
untested.

Encoded size is the post-run payload audit total, not BlockManager's estimate
or process memory. Default excludes its separate statistics row; other formats
include framing/metadata. At2GiB/task, sampled whole-job RSS GiB was:
default 19.266, pcbs 13.954, snappy 16.388, lz4 13.106, zstd 16.138, uncompressed 13.353.

No timeout or RSS guard triggered. Settings stayed16GiB heap,32GiB managed host
including2GiB pinned,24GiB GPU and64GiB sampled RSS stop guard; initialized pools
were checked. RSS includes startup, CPU/GPU reads and GC, and is not retained
live-cache memory. Arrow allocations are outside RAPIDS host accounting.
No hard process-memory cap or complete native-memory peak is claimed.

Root independently reconstructed the raw GPU counters. From1 to2GiB/task,
tracked RMM high-water was4.123->4.121GiB for Snappy/LZ4/uncompressed and
9.106->9.104GiB for Zstd. This supports a working set bounded by the fixed1GiB
output target in these runs, not a universal4xT guarantee. RMM high-water is
cumulative and excludes unused pool reservation and allocations outside RMM.

Packed formats emitted one GPU output group/task at1GiB and three at2GiB:
two near-target groups and a small tail. Native compressed decode calls rose
two->three/task, not one->three, because chunk rounding already required two
calls for the single1GiB output group. Root reproduced48->144 output groups
and96->144 native calls across the12 GPU actions. Uncompressed has no codec calls.

Findings for this fixture and fixed two-slot input-size scaling:

- Compressed CPU full-read1->2GiB ratios were1.984 for Snappy,1.933 for LZ4
  and2.066 for Zstd; CPU ID-read ratios were2.009,1.961 and1.956.
  Read cost and encoded size therefore remain approximately linear.
- At2GiB/task, Snappy was5.094x faster than PCBS for population,2.103x for
  CPU full reads and4.569x for CPU ID reads. GPU full/ID reads were1.393x
  and1.289x faster in these samples.
- Default CPU full/ID reads remain faster than the packed CPU row scans,
  with much slower population. Zstd has the smallest packed payload,
 3149.9MiB versus Snappy's4140.9MiB, but higher population cost and no
  GPU-read speed advantage in the largest cohort.
- Uncompressed CPU full/ID reads grew2.708x/3.027x from1 to2GiB despite
  residency and no observed spill. The cause is unprofiled; do not attribute
  it to GC, bandwidth or copying without new evidence.
- CPU-written uncompressed2GiB succeeded with8,587,210,080 encoded bytes,
  exactly matching the layout prediction. The historical GPU-writer heap
  failure was not rerun or fixed.

Artifacts: /data/tmp/nvcomp-spark-probe/cpu-scale-{format}-fixed-{512,1024,2048}.
Verifier: /data/tmp/nvcomp-pcbs-rebuild-01/verify-cpu-scale.py.
Combined verified24-case JSON:
 /data/tmp/nvcomp-pcbs-rebuild-01/cpu-scaling-verified.json.

Representative string performance, other datatypes, concurrency/hardware
scaling, column-indexed compression, CPU columnar output and active spill
pressure remain outside this completed task or unverified. There is no
production rollout. Final task status is READY FOR REVIEW; architect alone
marks DONE.

## Column pruning and predicate research — READY FOR REVIEW

The user requested research before implementation, with autonomous work while
away. Scope here is source investigation and an isolated executable feasibility
proof. The Spark serializer, scan, wire format, configuration and main benchmark
artifacts remain unchanged. This is not a new Spark performance cohort.

Release classification carries forward: packing/nvCOMP public APIs and existing
Spark/PCBS behavior are RELEASED and must retain their contracts. The experimental
packed serializer, research bridge/probe and proposed column directory are
UNRELEASED; no old experimental format adapter is required. A future production
GPU scan change is a separate integration decision, with mandatory shim
preflight and applicable profile validation. No such change is included here.
The research pack-layout assumptions are narrower than the public cuDF contract.

All project operations used MCP docker_exec. Root and bounded read-only workers
confirmed MCP-only use at resumptions/checkpoints. Root read the decisive code,
owned serial GPU jobs and independently reproduced the evidence. Workers
researched column layout and predicate statistics and reviewed the proof.
Future handoffs/resumptions/checkpoints must repeat the MCP-only requirement.

### What the current implementation does

- GPU encoding already packs all columns with one multi-boundary contiguousSplit
  per parent attempt. The packed record is then compressed as a whole for byte
  codecs. Typed Cascaded subdivides typed physical buffers, without a root-column
  directory. Repacking each column separately would give up this batching gain.
- The current GPU reader uploads/decompresses all columns before projection;
  the CPU reader decompresses all columns and copies them into Arrow before
  UnsafeProjection. Narrow reads therefore still pay most codec/movement costs.
- CpuCachePath.pack already knows each column's start/end while constructing
  the packed bytes. It writes mask then fixed data, or mask/offsets/chars for
  strings; GPU packing writes mask/chars/offsets. Reuse Spark ArrowWriter for
  row-to-column conversion and preserve per-iterator codec reuse. A future CPU
  directory can record boundaries at write time; do not apply the GPU-layout
  indexer to CPU-produced bytes.

Root checked CpuCachePath.pack/encodeRows, PackedTableCodec encode/decode,
SpillableColumnarBatch and SpillFramework ownership, GPU scan conversion/filter
methods, PCBS readers/buildFilter, and Spark planning/statistics code. Specific
cuDF implementation evidence is vendored
cpp/src/copying/contiguous_split.cu: setup_src_buf_data, type-specific buffer
setup, 64-byte padded split sizes, exclusive scan and output-column construction.
Root-column subtrees are contiguous in that implementation, including string
offset children. Public contiguous_split.hpp promises contiguous partitions,
not this root-column ordering. Mounted cuDF sources and pinned binary revisions
differ; the actual loaded binary is exercised below, not inferred to match
every mounted implementation detail.

### Recommended column layout and execution

Keep existing small row records and one parent packed allocation. Store a
directory by schema ordinal: per-column raw span, rebased opaque cuDF metadata,
and compressed payload/segment locations. Compress each column independently,
but flatten all columns across many records into the SAME batched nvCOMP
submission. Codec chunks remain independent blocks within each column.

The proof uses public pack_metadata with one-column table views to rebase opaque
metadata without copying device data. DeviceMemoryBuffer.slice retains the same
allocation. Native unpack reads host metadata with the real device base, so the
indexer need not create Java ColumnVectors or fetch string-end GPU scalars.
An initial trace exposed 36 D2H copies from lazy ContiguousTable.getTable()
materialization; switching to the native host-metadata path removed those copies.
Do not resurrect that Java materialization in the writer index path.

The root-column ordering fast path is explicitly pinned and checked for the
supported flat types. Reject unsupported layouts/types rather than silently
guessing. It is not a general malformed-metadata validator: string extent is
trusted from the packer to avoid GPU scalar reads. Zero-row/no-buffer and nested
types need separate handling before expanding scope. The current encoder drops
zero-row input, while count-only scans can use record row counts.

Ownership: retain ONE spill owner per original packed record. Column slices are
borrowed within an attempt and closed before releasing its materialized parent.
Independent spill owners for aliased subranges could misleadingly report freed
bytes while another slice pins the parent.

Read only the unique selected columns and restore requested order/duplicates.
Match schema ordinals/exprIds, not field names. Group by SELECTED decoded bytes
toward the configured output target; do not leave narrow reads with tiny output
batches because grouping still counts unselected bytes. Preserve spill/retry
handling and close compressed/staging temporaries as soon as no longer needed.

Transfers must also be batched: the current synchronous payload-copy helper
must not become one synchronize per column. Enqueue grouped host transfers with
a bounded staging owner and synchronize per group. Existing multiBufferCopyAsync
is a loop of async copies, not a fused copy kernel. Measure copy/API overhead
separately from codec kernel counts.

The proof decodes one allocation per selected column, then combines column
views without copying. A proposed integration refinement is one dense projected
output allocation per row record: flatten selected segments into that owner,
rebase the per-column metadata to the selected offsets, and assemble one projected
table. Existing decompression supports segments into one owner; the additional
metadata-combination path has NOT been implemented/tested. It is a design
proposal, not an established result. Generic SpillableColumnarBatch materializes
views without an immediate repack; its generic handle packs on actual spill.

### Executable proof and independently checked evidence

New isolated sources:
tools/cache-exploration/nvcomp/research/{column_slice_jni.cpp,
ColumnPruningProbe.java,run.py,analyze.py}.

Final profiled artifacts:
 /data/tmp/nvcomp-column-pruning-research/metadata-only-profile
Includes source snapshots, hashes, compiler/commands, logs, completion,
results.json, profile.nsys-rep and exported profile.sqlite. Earlier final-profile
contains the negative lazy-Java-materialization result and is not final evidence.

Prerequisite dependency guard:
python3.9 imports spark/run.py and verify(build-39/spark-provenance.json, build-39);
root reran it successfully. The research compiles against the hashed application
jar and dependencies, not mutable main classes. C++ compiles with -Wall -Wextra
-Werror. The native bounded unpack overload is forward-declared as in the
existing bridge; binary/header ABI portability is not established.

Reproduce the small proof (new output directory required):
python3.9 tools/cache-exploration/nvcomp/research/run.py
 --build-dir /data/tmp/nvcomp-spark-probe/build-39 --output NEW_DIR --profile
nsys export --type sqlite --output NEW_DIR/profile.sqlite NEW_DIR/profile.nsys-rep
python3.9 tools/cache-exploration/nvcomp/research/analyze.py NEW_DIR

The final run exited 0 in 11.23 seconds with all 39 observations, exact
packed-byte/value/null/order checks and zero tracked GPU allocations at phase
boundaries. This elapsed time includes fixture/validation/profiling and is NOT
a codec/Spark throughput number. The small proof uses Xmx1g/CUDA_DEFAULT and
about 9.2MiB packed input, not the main 16GiB/32GiB/24GiB resource cohort.

Fixture: 32769 rows, 38 columns, two packed row records; int/long, variable UTF8,
nulls, all-null numeric/string columns, empty strings including the LAST root,
and odd fixed-width tails. Selections: all, ID, string, reordered mixed columns
with duplicate ID, and count-only. GPU read verifies all seven codecs; CPU read
of GPU-compressed selected columns and CPU-encode/GPU-decode verify uncompressed,
Snappy, LZ4 and Zstd with reused codec contexts. CPU encoding here consumes
downloaded GPU-packed spans; CPU Arrow-produced column indexing is NOT tested.

All numbers below are bytes for this SAME-input proof. Payload includes existing
codec chunk framing, but excludes the new column directory and outer wire format.

| Codec | Whole-record payload | Column payload | ID-only payload | Whole/column compression kernels |
| --- | ---: | ---: | ---: | ---: |
| uncompressed | 9664128 | 9664128 | 262208 | 0 / 0 |
| snappy | 4805532 | 4804419 | 133347 | 1 / 1 |
| lz4 | 4794913 | 4790948 | 133262 | 1 / 1 |
| zstd | 3562425 | 3545109 | 42601 | 6 / 6 |
| ans | 5598734 | 5517992 | 126804 | 5 / 5 |
| cascaded8 | 7293208 | 7008308 | 193304 | 1 / 1 |
| cascaded-typed | 4792360 | 4792788 | 3732 | 3 / 3 |

Raw full/ID bytes are 9664128 / 262208. Snappy ID projection avoids 97.2% of
encoded upload bytes and 97.3% of decoded bytes for this wide fixture.
Whole cuDF metadata is 3392 bytes; separate-column metadata totals 4576 bytes,
an increase of 1184 bytes before adding a directory. Do not assume this overhead
or compression result generalizes to different record widths/types. Padding
bytes are not canonical across independent pack runs.

Root re-queried raw Nsight SQLite, filtering PID and interval overlap and checking
kernel boundary crossings: 14 column-index ranges and 64 host view-assembly
ranges have no observed kernels or memcpy activity. This says nothing about all
possible CUDA API/memset activity or unpack outside those ranges. Compression
kernel counts and native submission counts do not increase in this small proof:
one submission for byte codecs, three for typed Cascaded. More blocks can force
additional submissions at larger scale.

Cost is not unchanged. Byte-codec blocks increase 148 to 219; typed blocks 255
to 292. LZ4 D2D stitching copy events increase 150 to 295 and pointer metadata H2D
bytes 3552 to 5256. The column-compress NVTX range also includes host retention
D2H copies, whereas whole-record compression excludes them; do not compare their
wall times or total copies as equivalent population timings. No full Spark
pruning speedup, spill behavior or improved population throughput is claimed.

Focused peer review found no blocker and independently checked source hashes,
completion, observation count and activity summaries. Root's analyze.py verifies
source/dependency hashes, raw log/results agreement, projection/count-only
observations, range counts and raw SQLite kernel/copy evidence. Root ran it
successfully on the final profiled artifact. Performance is REQUIRED for the
eventual serializer integration; this is a feasibility result, not an exemption.

### Predicate batch skipping

PCBS buildFilter currently returns its input unchanged. Its GPU Parquet reader
does include selected columns, but its CPU row path builds full column IO before
projecting; the CPU columnar route has selected readers. These are code-path
facts, not a complete profile attribution of the earlier timings.

Spark's CPU cache scan already invokes serializer.buildFilter when partition
pruning is enabled. DefaultCachedBatchSerializer's helper can be reused for
records implementing SimpleMetricsCachedBatch, with encoded size overriding the
statistics-size estimate. Actual per-column schema is lowerBound, upperBound,
nullCount:Int, count:Int, sizeInBytes:Long. Preserve schema/order and Spark
comparison semantics, including UTF8 string ordering and owned string bounds.

The GPU scan currently returns cachedColumnBuffers without invoking buildFilter.
Root checked InMemoryTableScanMeta.convertToGpu: it passes imts.predicates through,
so these are original Catalyst predicates, not converted GPU expression nodes.
A future host-statistics filter can therefore consume those predicates; it still
requires an explicit scan integration, production review and shim preflight.

Use conservative batch exclusion, never exact-row-filter claims. Spark retains
the residual filter. A query projecting A with a predicate on B must decode both
on surviving records under the current planner. Predicate-first selection-vector
decoding is a separate later design, not part of this recommendation.

Default helper supports selected literal comparisons, null checks, literal IN
and StartsWith; unsupported predicates must retain batches. Unknown bounds must
retain batches explicitly: setting bounds to null and blindly delegating can
drop real matches. Direct unnormalized EqualNullSafe(NULL) also needs handling
or normalization even though the normal optimizer rewrites it to IsNull.

For GPU statistics, avoid per-column/per-record reductions and scalar downloads.
cuDF Parquet already batches fragment statistics: gpu_calculate_group_statistics
uses one block per column/range group in one launch for min/max/null statistics,
then supports grouped merging. These are internal CUDA implementation routines,
not public JNI APIs or exported standalone wrappers. Reuse/adaptation needs
matching source/toolchain work and must keep string bounds alive through copying.
Java segmentedReduce only batches ranges of ONE column and ONE aggregation.
No statistics or GPU batch-filter implementation is included in this proof.

### Proposed next bounded work and park conditions

1. Review/accept the directory approach, implement CPU/GPU column-indexed records
   using existing row conversion/packing/codec contexts, and verify full/narrow,
   duplicate/reordered/count-only cross-mode reads. No old unreleased-format shim.
2. Benchmark the actual serializer at the established task sizes/settings,
   reporting population/full/narrow CPU and GPU times, encoded directory-inclusive
   sizes, native calls/copy activity, output batching and observed spill.
3. Add exact CPU statistics and conservative batch filtering first, then batched
   GPU statistics plus the scan hook under separate production/shim review.

Park if the pinned layout/ABI validation fails, source artifacts mismatch,
unsupported types reach the proof, resource guard trips, or a production/shared
scan change is needed before its integration scope is agreed. Do not alter
hardware-derived targets, accepted budgets, filters or unsupported-type behavior
to obtain a passing result. Large strings, nested/new types, Arrow-writer indexing,
active spill recovery and full Spark pruning performance remain unverified.
Research status: READY FOR REVIEW. Architect alone marks DONE.

## Column-pruning integration — IN PROGRESS

User clarified that overnight authorization included proceeding beyond research
to actual serializer integration and updated performance numbers. Root stopped
too early at research; no pruning timing was available at that checkpoint.
Continue now with column-indexed CPU/GPU records, cross-mode correctness and
the existing size/settings matrix. Predicate integration follows separately.

MCP docker_exec ONLY for all operations; repeat at worker resumptions and
checkpoints. Root owns serial GPU/build jobs and independently verifies all
delegated work. Native bridge and CPU-path workers own separate files; root
owns shared wire/GPU path. Experimental wire is UNRELEASED and replaced directly.
Preserve released production plugin, PCBS and cuDF contracts. No new knobs.

Keep one spill owner per packed record, one dense selected GPU output allocation
per row record, grouped async transfers with drained lifetimes, selected-byte
output grouping, existing codec contexts and Spark ArrowWriter. Native metadata
projection avoids a second GPU pack. Per-mode selected encoded/decoded byte and
column counters must establish real pruning rather than just correct results.
Main budgets remain16GiB heap/32GiB host including2GiB pinned/24GiB GPU/64GiB
sampled RSS stop guard. Guard/OOM/provenance failures park without relaxed caps.

No implementation/performance acceptance claim yet. Rebuild with a NEW manifest;
build39 is historical baseline and cannot verify against edited sources.

### Column directory implementation — READY FOR REVIEW; timings IN PROGRESS

Build42 compiles native C++ and Scala/Java with the same source-verified PCBS/JNI
dependencies. Experimental wire v2 replaces v1 directly, with one descriptor and
opaque cuDF metadata per root column, followed by its independently compressed
segments. No released format/configuration or production source changed.

GPU writer retains one parent spill owner, borrows zero-copy column slices and
compresses columns across records in batched submissions. One group host buffer
receives async D2H payload copies and is drained before release. GPU reader gathers
selected columns, queues grouped H2D transfers, decompresses into one dense output
allocation per row record, and combines metadata on the host (no second GPU pack).
Output grouping counts unique selected decoded bytes. Temporary owners drain
before release; existing spill/retry paths remain. CPU writer reuses ArrowWriter
and one CpuPackedCodec/iterator, indexes packed bytes once and compresses columns
independently. CPU reader materializes only selected Arrow columns and restores
requested ordinal order/duplicates. Count-only bypasses all payload decoding.

Root inspected all decisive changes, compiled build42, and independently ran
11 final-build correctness probes: CPU-supported uncompressed/Snappy/LZ4/Zstd
across all four CPU/GPU write/read combinations, plus GPU probes for all seven
codecs. Probes include ID/string-only pruning byte/root counters, reordered and
duplicate projections, final empty/all-null string roots, multiple records,
empty/count-only selection, CPU zero-column input, disk serialization, partial
consumption and resource cleanup. Direct CPU columnar input remains unverified.
Artifacts: /data/tmp/nvcomp-column-pruning-spark/probe-{cpu,gpu}-*-42 plus
probe-gpu-snappy-01 (build42). Early probe-cpu-snappy-01 uses build41 and is
supplemental, not the final-build acceptance evidence.

Focused peer review found a directory-inclusive heap-bound check occurred after
frame allocation. Root corrected it to calculate/check frameSize across the group
before allocating any frame byte arrays; reviewer independently verified the fix.
No remaining review finding. Build and runtime verification are root-owned.
All project operations/checkpoints remain MCP docker_exec-only; workers confirmed.

Snapshot appends six process-local counters: selected encoded bytes, decoded bytes
and unique roots for CPU and GPU separately. These are successful-read payload
observations, not total CUDA bus traffic (pointer metadata is excluded). The
benchmark maps all15 fields. Unsupported codecs/types still fail explicitly.

Serial matrix supervisor /data/tmp/nvcomp-column-pruning-spark/run_matrix.py
completed its ten prerequisites and started56 main cases (CPU+GPU population
separate, four sizes, CPU/GPU reads where supported; GPU-only ANS/typed Cascaded).
One additional build42 GPU Snappy prerequisite had already passed. All main
cases retain16GiB heap,32GiB host including2GiB pinned,24GiB GPU,64GiB sampled
RSS stop guard,4tasks/local[2], one warmup/five measured reads. The supervisor
parks on failure for root investigation. Timings and large-case acceptance are
IN PROGRESS; no speedup claim yet. Predicate statistics/scan hook remain separate.

### Pruning 256MiB/task cohort — READY FOR REVIEW; larger runs IN PROGRESS

Commit93a44449bf contains the implementation/probes. Root independently ran
/data/tmp/nvcomp-column-pruning-spark/verify_results.py against the frozen build42
and re-extracted first-case JSON medians, payload totals and per-action counter
deltas separately. The verifier hashes source/artifact dependencies, validates
actual budgets/backend/class origins, exact aggregates, 1warmup+5reads, audited
directory-inclusive sizes and task-level spill arithmetic. Its selected-byte
checks remain instrumentation checks, not independent CUDA bus measurements.
All14 cases in this cohort passed; full56-case matrix is still IN PROGRESS.
The ten supervisor prerequisites plus separate GPU Snappy build42 probe passed.

Population is one cold whole-action sample. Reads below are independent
five-sample medians, NOT write+read times. Four tasks on two slots; total logical
input1024MiB. Stored bytes include packed column directory and wire framing;
default Spark excludes its separate statistics row. CPU and GPU population
produce different bytes and remain separate cohorts. GPU-enabled default reads
still use a CPU cache reader. PCBS CPU population uses uncompressed Parquet;
GPU PCBS population uses its existing GPU writer encoding/compression.

| Writer | Format | Population s | CPU full s | CPU ID s | GPU-mode full s | GPU-mode ID s | Encoded MiB total |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| cpu | snappy | 2.838 | 2.577 | 0.148 | 0.252 | 0.126 | 516.8 |
| cpu | lz4 | 2.898 | 2.434 | 0.157 | 0.247 | 0.109 | 505.9 |
| cpu | zstd | 4.129 | 2.490 | 0.142 | 0.239 | 0.102 | 393.3 |
| cpu | uncompressed | 2.920 | 2.259 | 0.146 | 0.294 | 0.090 | 1023.9 |
| cpu | pcbs | 13.379 | 5.242 | 4.069 | 0.256 | 0.153 | 701.2 |
| cpu | default | 20.309 | 1.468 | 0.088 | 0.578 | 0.110 | 415.7 |
| gpu | snappy | 1.380 | 2.059 | 0.145 | 0.278 | 0.108 | 521.4 |
| gpu | lz4 | 1.489 | 1.856 | 0.116 | 0.280 | 0.118 | 510.0 |
| gpu | zstd | 1.436 | 1.989 | 0.122 | 0.268 | 0.103 | 382.7 |
| gpu | uncompressed | 1.380 | 1.830 | 0.133 | 0.372 | 0.106 | 1024.8 |
| gpu | pcbs | 1.478 | 5.117 | 3.875 | 0.253 | 0.136 | 489.7 |
| gpu | default | 19.978 | 1.459 | 0.105 | 0.797 | 0.109 | 415.7 |
| gpu | ans | 1.332 | unsupported | unsupported | 0.313 | 0.115 | 556.3 |
| gpu | cascaded | 1.173 | unsupported | unsupported | 0.390 | 0.145 | 391.3 |

All4 partitions stayed resident, no disk storage or recomputation observed.
Available Spark/RAPIDS spill and retry values were0; unavailable per-phase
CPU RAPIDS metrics remain unavailable. All cases retained approved budgets.
No guard triggered. No active-pressure/spill recovery claim is made.

For CPU-written Snappy, each full read observed4224 distinct column roots
(132records*32); ID-only132. Full/ID encoded payloads541433959/17072370bytes,
decoded1073225472/33554432bytes. CPU and GPU samples agree on these byte counts;
inactive-mode counters are0. GPU full/ID reads emitted4outputs and4native decode
calls each across the four tasks. Narrow reads therefore group by selected bytes.

GPU-written Snappy/LZ4/Zstd population observes8compression groups and12native
codec calls across four tasks. Column-boundary chunk rounding can require
additional submissions; the small-proof unchanged-call result is not generalized.

Fresh same-cohort GPU-written Zstd/PCBS compare382.7/489.7MiB payload,
GPU full0.268/0.253s, CPU full1.989/5.117s and CPU ID0.122/3.875s.
These support promising CPU/pruning tradeoffs; larger-size acceptance is pending.
Small timing differences and cold-population differences are not asserted as
statistically significant. Historical before-pruning CPU-ID comparisons use
the prior verified cohort, not a fresh v1 control run.

Stable checkpoint: /data/tmp/nvcomp-column-pruning-spark/verified-256.json.
Supervisor/state: run_matrix.py / matrix-status.json in the same directory.
MCP docker_exec-only operations and source freeze remain in effect. Root owns
serial jobs; workers are idle. No new source edits during the frozen matrix.
Verifier SHA256: d026852e4210c3ede24d1348988acd4c1c21d183f73b589ef1d559ebc7abf4e4.

### Comparison clarification from user

PCBS ALREADY stores independently compressed column chunks and their directory
inside Parquet files. This work adds equivalent column-addressable payloads to
the custom nvCOMP format; it does not introduce that capability to PCBS or change
PCBS to separate files. Root verified d88ebe87e1..HEAD contains no PCBS production
file changes. New packed records still use one byte array with a column directory.

Keep before/after nvCOMP comparisons separate from same-cohort nvCOMP/PCBS
comparisons. Differences in unchanged PCBS timings between cohorts are
unattributed run differences, not effects of this implementation. Independent
column compression itself cannot explain an advantage over PCBS. Its row
reader constructs full Parquet column IO before projection, while GPU and CPU
columnar routes support selected columns; prove the actual benchmark call path
before attributing timing differences to this source-path distinction. No PCBS
format/runtime optimization or intrinsic format advantage is established here.

### PCBS transfer follow-up — investigation complete, no code changes

User asked whether GPU PCBS should remove unused Parquet chunks before decoding,
or whether transfer time is too small. Root traced includeColumn -> managed
host logical-file buffers -> cuDF selected input-column chunk reads and the
native multi_host_buffer_source offset/size copies. Root independently queried
existing profile-current-pcbs-256 raw SQLite and a read-only reviewer reproduced
the numbers and verified exact production/JNI artifacts match build42.

Four256MiB tasks, GPU-written cache, diagnostic profile:
full samples H2D521860080bytes each, summedcopy27.239/27.395ms versus
collect401.358/324.751ms; ID samples H2D17296932bytes each,
summedcopy1.264/1.269ms versuscollect196.351/160.719ms. All event-boundary crossing
counts0. PCBS ALREADY avoids full-file H2D for projection (~96.69% fewer bytes).
Summed GPU durations overlap and are not critical-path percentages; these are
older perturbed profiles of the same unchanged PCBS binary, not new headline
pruning timings.

Separate opportunity: Files.stage still copies whole encoded records from JVM
heap into managed host, logicalFile creates all-column views/metadata, and
group estimates count full schema. Host staging/selected-byte grouping may be
worth measuring later; GPU transfer data does not quantify that CPU cost.
No PCBS implementation/config/artifact changed. User agreed to continue the
other experiments, so resume the frozen nvCOMP matrix rather than start a PCBS
rewrite. Predicate/PCBS follow-ups remain separate.

### Pruning 512MiB/task cohort — READY FOR REVIEW; 1/2GiB IN PROGRESS

Root reran the verifier over all completed artifacts:28main cases plus ten
supervisor probes verified,28main cases pending,0failed/invalid. The separate
GPU Snappy final-build probe also remains verified. Full source/dependency
hashes match build42; no source changes occurred during the matrix.

Same measurement protocol, dimensions, modes, defaults and caveats as256MiB.
Total logical input2048MiB across4tasks/two slots. All4partitions resident,
no disk storage/recomputation, available Spark/RAPIDS spill/retry totals0.
No guard fired. This does not establish active-pressure recovery.
Stable artifact: /data/tmp/nvcomp-column-pruning-spark/verified-512.json.

| Writer | Format | Population s | CPU full s | CPU ID s | GPU-mode full s | GPU-mode ID s | Encoded MiB total |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| cpu | snappy | 4.764 | 4.908 | 0.245 | 0.383 | 0.112 | 1033.5 |
| cpu | lz4 | 5.027 | 4.893 | 0.209 | 0.354 | 0.111 | 1011.6 |
| cpu | zstd | 7.572 | 4.908 | 0.227 | 0.322 | 0.114 | 786.2 |
| cpu | uncompressed | 4.996 | 4.596 | 0.201 | 0.513 | 0.102 | 2047.9 |
| cpu | pcbs | 22.716 | 10.765 | 8.039 | 0.449 | 0.267 | 1402.4 |
| cpu | default | 39.616 | 2.954 | 0.107 | 0.941 | 0.237 | 831.4 |
| gpu | snappy | 1.797 | 3.923 | 0.225 | 0.452 | 0.122 | 1047.1 |
| gpu | lz4 | 1.818 | 3.657 | 0.172 | 0.460 | 0.142 | 1019.1 |
| gpu | zstd | 2.133 | 5.341 | 0.219 | 0.381 | 0.110 | 760.5 |
| gpu | uncompressed | 1.884 | 3.541 | 0.207 | 0.557 | 0.148 | 2049.6 |
| gpu | pcbs | 1.956 | 9.913 | 7.778 | 0.355 | 0.190 | 979.3 |
| gpu | default | 40.053 | 2.832 | 0.110 | 1.229 | 0.236 | 831.4 |
| gpu | ans | 1.696 | unsupported | unsupported | 0.493 | 0.127 | 1126.7 |
| gpu | cascaded | 1.743 | unsupported | unsupported | 0.562 | 0.142 | 777.3 |

The1GiB/task cohort has started. The complete remaining matrix is still authorized
and IN PROGRESS, with serial jobs under the original caps/protocol. Large CPU
fallback scans dominate elapsed matrix time. Do not report the remaining1/2GiB
numbers as measured until their completion and independent verification.

### GPU read cost candidates identified during the frozen matrix

No algorithm/source change is made mid-matrix. At1GiB/task, uncompressed GPU
full reads are slower than compressed variants. Source shows an upload allocation
per payload plus D2D assembly into per-record outputs. A later direct-to-output
H2D path for uncompressed records could avoid the intermediate allocations/copies;
a single compressed upload slab could reduce allocations and H2D submissions for
compressed codecs. Keep decoded record allocations independently owned/spillable;
do not introduce aliased per-record spill owners over one large raw allocation.

BatchedDecompressor also fetches chunk-size headers back from device memory:
fetchMetadata gathers input prefixes with Cuda.multiBufferCopyAsync, copies the
gathered metadata to host, builds pointers/sizes there and copies those vectors
to device. Root read the implementation and independently inspected bytecode
from the actual pinned972b04a7... JNI jar with javap -p -c: fetchMetadata,
multiBufferCopyAsync, HostMemoryBuffer.copyFromDeviceBuffer and
DeviceMemoryBuffer.copyFromHostBuffer are present. The cached payload already
contains these size headers on CPU, so a host-metadata-aware batched decoder may
avoid that round trip and many tiny copies. This is a structural opportunity,
NOT a measured attribution or implemented optimization. Profile current large
read phases before claiming its wall-time contribution. Preserve GPU batching,
existing formats/contracts, codec reuse and spill-safe ownership.

## Predicate pruning follow-up authorized — RESEARCH IN PROGRESS

Latest user instruction: after finishing the size matrix, implement proper
predicate pruning across current setups, INCLUDING PCBS; resolve routine details
autonomously, ask only substantial design questions/blockers, and report decisions
when asked or at the end. At a genuine2GiB-stage blocker it is explicitly okay
to stop the size matrix, after considering evidence from the older2GiB runs.
Do not force a run through by relaxing accepted limits or changing the protocol.

Current coverage to design: default Spark's existing CPU filtering (including
GPU-enabled CPU cache-reader fallback), CPU/GPU PCBS, and CPU/GPU experimental
nvCOMP codecs supported in each mode. Preserve residual row filters. Unknown
statistics/predicates/types must KEEP records; rejection requires proof.
PCBS/default supported contracts and stored bytes remain RELEASED; nvCOMP's
experimental records/interfaces remain UNRELEASED and may change directly.

While the frozen build42 matrix continues, bounded workers perform READ-ONLY
MCP research on CPU/Parquet filtering reuse and fused GPU statistics. No source
edits/builds/GPU jobs are delegated during timing. Every handoff/resumption and
checkpoint repeats MCP-only tool use. Root reads decisive code and independently
verifies, then begins implementation after the timing checkpoint.

Research candidates: reuse PCBS's existing footer ROWGROUP statistics, use
conservative host batch filtering, add GPU scan buildFilter hook respecting
Spark's existing partition-pruning config, and generate nvCOMP per-record stats
without per-column/per-record GPU launch storms. New shared production JVM work
requires the repository shim skill/preflight, independent advisory evidence and
affected profile checks before being declared ready. No new user-facing config
or unsupported-data behavior is authorized implicitly.

### Pruning 1GiB/task cohort — READY FOR REVIEW; 2GiB IN PROGRESS

Root independently reran the frozen-build verifier:42main cases and ten
supervisor probes verified,14main cases pending,0failed/invalid. Separate final
GPU Snappy prerequisite remains verified. Four tasks/two slots, total4096MiB
canonical input; one cold population sample and separate five-sample read medians.
No available spill/retry value was positive and no disk storage was observed;
CPU missing RAPIDS values remain unavailable. No guard fired.
Stable snapshot: /data/tmp/nvcomp-column-pruning-spark/verified-1024.json.

| Writer | Format | Population s | CPU full s | CPU ID s | GPU-mode full s | GPU-mode ID s | Encoded MiB total |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| cpu | snappy | 8.966 | 10.006 | 0.430 | 0.623 | 0.114 | 2067.0 |
| cpu | lz4 | 9.120 | 9.438 | 0.325 | 0.589 | 0.114 | 2023.4 |
| cpu | zstd | 14.736 | 9.465 | 0.371 | 0.621 | 0.120 | 1572.0 |
| cpu | uncompressed | 8.660 | 8.960 | 0.375 | 1.056 | 0.212 | 4095.7 |
| cpu | pcbs | 47.420 | 21.135 | 15.586 | 0.659 | 0.366 | 2804.7 |
| cpu | default | 80.279 | 5.409 | 0.168 | 1.781 | 0.278 | 1662.8 |
| gpu | snappy | 2.841 | 7.737 | 0.341 | 0.901 | 0.121 | 2088.8 |
| gpu | lz4 | 2.848 | 7.071 | 0.333 | 0.936 | 0.185 | 2026.9 |
| gpu | zstd | 3.427 | 7.746 | 0.393 | 0.699 | 0.140 | 1516.8 |
| gpu | uncompressed | 3.462 | 6.685 | 0.350 | 1.076 | 0.170 | 4099.2 |
| gpu | pcbs | 2.596 | 20.042 | 15.247 | 0.580 | 0.267 | 1958.6 |
| gpu | default | 83.458 | 5.287 | 0.198 | 2.097 | 0.314 | 1662.8 |
| gpu | ans | 2.634 | unsupported | unsupported | 0.856 | 0.153 | 2267.7 |
| gpu | cascaded | 2.971 | unsupported | unsupported | 0.922 | 0.115 | 1560.0 |

Two writer modes remain separate; default GPU-mode reader is CPU; default payload
excludes its separate statistics row. Packed/PCBS audited sizes include framing.
These are same-cohort comparisons against unchanged PCBS, not PCBS before/after
effects. GPU-read population is not included in read-only medians.

2GiB/task is now running. User explicitly allows parking at a genuine2GiB blocker
after examining older2GiB evidence. No failure is present yet. Predicate research
is read-only; source stays frozen until timing completion/park checkpoint.

### Predicate implementation decisions from source research

Root read Spark SimpleMetricsCachedBatchSerializer.buildFilter, the shimmed
getParquetFilters constructor, existing DataSourceStrategy translation caller,
GPU Parquet filtering, PCBS writer ROWGROUP options, and cuDF fused statistics
and merge kernels. Bounded read-only workers independently traced CPU/Parquet
and GPU statistics. No predicate implementation has started during timing.

Plan after timing completion/park:
- PCBS: reuse Spark's Catalyst-to-datasource translation in an org.apache.spark.sql
  helper and existing SparkShimImpl.getParquetFilters. Rename attributes by exprId
  to physical _colN ordinals first. Use Parquet RowGroupFilter's statistics-only
  overload through FilterCompat. Reject a whole cached record only when no group
  can match. Keep existing constructor, serialized shape and Parquet bytes.
- Parse a footer only for supported predicates; initially allow duplicate parsing
  on survivors instead of a global cache or retained lazy metadata. Measure an
  all-match predicate's overhead. Preserve residual Spark row filtering.
- Initial range eligibility is the currently exercised top-level int/long/binary
  string semantics. Unsupported types, nonbinary collations, expressions or
  untrusted/missing statistics retain records. Float/NaN support is not inferred
  from reuse of Parquet filtering alone. Other released datatypes still decode.
- GPU scan: mirror the CPU scan's existing partition-pruning gate and invoke
  serializer.buildFilter with original Catalyst predicates and relation output.
- nvCOMP: exact per-record statistics in its unreleased wire format, including
  bounds/count storage in audited bytes. Reuse guarded Spark SimpleMetrics
  filtering, normalize raw null-safe equality with NULL, preserve five-field
  lower/upper/null/count/size ordering and actual encoded size accounting.
- CPU stats: native scan of already packed int/long/string buffers, preserving
  ArrowWriter and iterator codec reuse. GPU stats: extension-owned relative raw
  descriptors resolved after spill materialization, tiled fused min/max/null
  reduction and grouped merge, plus one batched bounds gather for strings.
  Avoid importing private cuDF structs across the mismatched library ABI.
- Exact string bounds require explicit source-owner lifetimes and checked,
  data-derived allocation sizes. Equal bounds can share storage. Group framing
  must account for actual statistics bytes before allocation; oversized bounds
  must not silently remove supported string coverage or trigger a new arbitrary
  memory policy. Add a large-bound functional case and retain accepted budgets.
- Test results AND avoided work: direct helper filtering, scan output rows before
  residual filtering, native decode/staging observations, and pruning-off controls.
  Include all-null/empty/missing bounds, Unicode/prefixes, NULL-safe/reversed ops,
  AND/OR with unsupported clauses, mixed/legacy groups, aliases and A-projection
  with B-predicate. Validate existing default CPU and GPU-enabled fallback too.

Resources: mounted Python3.13 can run the shim helper; python3.9 remains benchmark
Python. nvcc12.9/CUB are available. gh CLI and a GitHub MCP connector are absent;
origin is an internal GitLab mirror, not a GitHub upstream. Before production
handoff, run applicable local/core preflight and investigate current-base/advisory
access; report any unavailable evidence explicitly rather than claim a clean
online advisory. No repository-local Git configuration changes are authorized.

### Final pruning timing checkpoint — READY FOR REVIEW with explicit 2GiB limitation

The serial matrix is STOPPED at the user-permitted2GiB blocker. Root compared
old/current logs, source-bound settings and allocation stacks:
gpu-uncompressed-fixed-2048-42 failed during population with Java heap space,
exit52,31.03s elapsed and11.69GiB sampled process-tree RSS. The allocation is
SparkNvcompSerializer.frame(new Array[Byte]); no read times or completed encoded
size exist for that run. Historical target-final-uncompressed-fixed-2048 failed
in wire/ByteBuffer allocation, exit52,32.84s,11.76GiB RSS, with EXACTLY the same
16GiB heap/32GiB managed host/2GiB pinned/24GiB GPU/64GiB sampled guard settings.
No budget was raised and no RSS guard triggered. The underlying heap-allocation
cause remains unprofiled; do not call this GPU-memory exhaustion or a codec limit.

Root independently reran both original and archived verifiers: byte-identical
JSON,10supervisor prerequisites and51timing cases VERIFIED,1failed,4notrun,
0invalid. The additional GPU Snappy final-build prerequisite also passed.
No case after the failed uncompressed GPU writer was launched. Not run:
GPU-written PCBS/default/ANS/typed Cascaded at2GiB/task. This is not a complete
56-case success claim. CPU-written uncompressed2GiB and its CPU/GPU reads passed.

Final dataset size8192MiB total,4tasks/two slots. Population is one cold action;
reads are five-sample medians after one warmup. All other protocol/caveats from
earlier checkpoints remain. No positive available spill/retry metric was
observed in the nine successful2GiB cases; unobserved CPU metrics remain missing.

| Writer | Format | Population s | CPU full s | CPU ID s | GPU-mode full s | GPU-mode ID s | Encoded MiB total |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| cpu | snappy | 17.245 | 19.675 | 0.814 | 0.964 | 0.230 | 4134.1 |
| cpu | lz4 | 15.946 | 18.698 | 0.660 | 1.043 | 0.114 | 4047.2 |
| cpu | zstd | 26.997 | 18.906 | 0.636 | 0.928 | 0.139 | 3144.1 |
| cpu | uncompressed | 16.510 | 17.532 | 0.648 | 2.075 | 0.243 | 8191.4 |
| cpu | pcbs | 91.858 | 43.146 | 32.032 | 1.216 | 0.697 | 5609.4 |
| cpu | default | 149.719 | 10.957 | 0.295 | 4.377 | 0.438 | 3325.6 |
| gpu | snappy | 4.381 | 25.423 | 0.692 | 1.450 | 0.115 | 4165.4 |
| gpu | lz4 | 4.638 | 13.995 | 0.486 | 1.355 | 0.235 | 4053.9 |
| gpu | zstd | 6.095 | 15.790 | 0.596 | 1.217 | 0.220 | 3018.0 |

GPU-written Snappy CPU full reads show a notable1->2GiB increase7.737->25.423s;
cause is unprofiled. Retain samples rather than infer GC/bandwidth or silently
discard this cohort. GPU uncompressed full-read overhead and chunk-metadata
roundtrip candidates are recorded above; neither was changed during the matrix.

Results: /data/tmp/nvcomp-column-pruning-spark/verified-final.json.
Historical source snapshot: frozen-sources-42 under the same root,30referenced
repository sources matching Git93a44449bf9e1b391265ccb2015c69d3bcd7438e.
Source archive manifest SHA256:
7e2d967ac504bc8113f5c2bc322735dda1d96878c660c040fa9475bc4e213b09.
Original verifier SHA256 remains
d026852e4210c3ede24d1348988acd4c1c21d183f73b589ef1d559ebc7abf4e4.
Use verify_archived_results.py --json after current-source edits: it hash/Git
validates source snapshots and maps ONLY those old source paths. All binary,
external-header, production-proof and runtime-origin checks remain unchanged.
Root inspected this wrapper and independently reproduced original/archive
equality. Original launcher guards are NOT relaxed and cannot launch old
manifests against edited source. Failed-case return1 remains expected; no
success flag is synthesized. GPU idle confirmed after Spark JVM exit.

Predicate design has no outstanding large user question yet. Resources for
CPU/GPU stats are available. Local shim baseline coverage passed32profiles/
3839explicit requirements with0findings,19unmodelled wildcard imports; this is
baseline static evidence, not compilation or future-change validation.
Artifacts: /data/tmp/cache-predicate-pruning/shim-baseline.
Now proceed to predicate implementation under the user authorization.

### Predicate pruning implementation — IN PROGRESS

Resumed after checkpoint04931d8e0d. All project work and worker handoffs remain
MCP docker_exec-only; workers have reconfirmed. The frozen build42 source
archive preserves the completed column-pruning results while current sources change.

Release classification carries forward: PCBS record/API/configuration contracts
and the plugin GPU cache scan are RELEASED (present on main); preserve constructors,
serialized bytes, decoders, residual filters and existing pruning/pushdown controls.
The local nvCOMP wire/native bridge is UNRELEASED; replace wirev2 with v3 statistics
directly, with no obsolete-format adapter. Initial exact statistics support is flat
int/long/default binary string, matching the prototype's existing types. Other PCBS
types/predicates conservatively retain records.

PCBS uses its existing ROWGROUP footer statistics via Spark's shimmed ParquetFilters
and stats-only RowGroupFilter. Any surviving group retains the whole cache record.
The GPU cache scan will invoke the serializer filter behind Spark's existing
inMemoryPartitionPruning gate, matching the CPU scan. No extra column files or
Parquet rewrite is introduced. Unsupported predicates keep data; residual row
filtering remains necessary.

This commit checkpoints production source for a clean source-bound build; it is
NOT verified implementation or READY FOR REVIEW. New footer-only unit cases need
compilation/execution plus actual CPU/GPU cache-query checks. Root has read the
decisive helper, caller and test changes. A focused independent review is running.
The shim preflight skill applies; final proposed-tree coverage/profile builds
and advisory evidence must still be recorded. Current online advisory tooling
is unavailable (gh absent); investigate base-fetch evidence before final status.

First source-clean355 build rejected the deprecated footer-only RowGroupFilter
overload under fatal warnings. Root read its bytecode: the replacement needs
a live reader, while the older overload performs exactly statistics-only filtering.
Kept that shared API with a narrowly matched deprecation annotation and explanatory
comment, consistent with existing repository usage; no compiler check was disabled.
Independent production semantic review found no actionable correctness issue.
Upstream fetch failed: configured origin gitlab-master.nvidia.com does not resolve.
Current-main/advisory evidence remains INCOMPLETE, not a clean compatibility claim.

### Predicate pruning checkpoint — implementation verification in progress

Production runtime sourcee8491e4f5684f2b338445b155533cf676e3eed44 built cleanly
with the pinned JNI/private pair and Spark355, fatal warnings/scalastyle active.
Proof /data/tmp/cache-predicate-pruning/production-build-02/source-build.json;
Maven verify exit0,296.46s, sourceclean true.
Root compiled the new PCBS suite with fatal warnings and ran all8CPUtests PASS.
Updated fixtures use nondeprecated Statistics builders and genuinely unknown
nullcounts (the original default-constructor fixture did not establish unknown).
Logs /data/tmp/cache-predicate-pruning/pcbs-unit-01.

Root inspected all delegated CPU/GPU stats/native changes and ran independent
verification. Nativehost and rawCUDA+CUB libraries compile. A CUDA-disabled Java
host probe passed signed extrema/dirty mask tails/UTF8/nullcount and malformedoffset
rejection. Experimentalbuild44 compiles all JVM sources and verifies pinned source
artifacts/dependencies. Wirev3 stores exact percolumn min/max/nulls, deduplicating
equal stringbounds; stats framing is accounted before heapframe allocation.
CPU retains SparkArrowWriter and iterator-owned reusable codecs.
GPU stats use two groupwide reductions plus optional parallelstringbound gather;
source/scratch owners survive async drains and retry addresses are rematerialized.

ActualSparkPredicateProbe passed Snappy and PCBS, each with CPU/GPU writers and
CPU/GPU readers. For the20005row/fourpartition fixture a high-ID range retains
oneof4records; impossible ranges and allnull IsNotNull retain0. Selected/residual
query results equal pruning-disabled reads. Snappy also passed signed extrema,
multi-tile tail, Unicode/NUL/prefix/null/empty/equalbound cases including actual
131072/131073byte extrema, defaultPTDS stream2 and an explicit createdstream.
The probe exercises InSet normalization and unknownstatistics between knownruns.
Artifacts /data/tmp/cache-predicate-pruning/probe-{snappy,pcbs}-44.
A second focused peer review found no actionable resource/correctness issue;
its longbound/InSet test gaps were added before these runs.

Shim finalhead offlinecoverage PASS32profiles/3839requirements/0findings,
19unmodelledwildcards. This is HEAD-only static evidence:
both review/advisory commands returnINCOMPLETE because gh is absent; originfetch
failed DNS. Localmain1b5576adb95941f57bfd2ab22b96435d7057ecf8 is not an ancestor,
and read-only merge-tree reports preexisting conflicts in GpuRangeBoundaryPlan
and ProjectExprSuite. No conflicts were resolved or branch merged silently.
Full proposed-with-currentmain validation/allprofilecompilation remains unverified.

New runtime finding: Sparkdist corejar contains enabled CompLogging compression
tracing (spark.test.compression_logging.enabled defaulttrue). The build42 and44
Sparkdependency hashes are IDENTICAL, so this is not a new runtime substitution.
Earlier cohorts used this instrumentedruntime; its timing perturbation is not
quantified. It is not evidence that the cache codec itself is spending the logged
Spark task-result transport time. Future timing cohort will disable this existing
diagnostic switch, record the change, and not call historical data clean stockSpark.
No old result is silently relabeled or discarded.

Optional predicate timing harness added after existing normal scans: independently
computed uncached CPU references for allmatch/highquarter/no-match, both full/ID
widths, both requested executionmodes, pruningoff/on on the SAME resident cache.
Timings cover collectonly; eachaggregate is checked, plans/counters/spill/storage
recorded. No filter replaces residual row semantics. Original pruning flag restored.
Root independently inspected this delegated harness diff and parsed both Python
sources. First measured cohort will use256MiB/task, fourtasks/twowaves,1warmup and
5samples under the unchanged16/32/2/24/64GiB settings. Future timing runs explicitly
disable the existing Spark compression diagnosticlogging switch as announced.
Historical build44 sources all match Git173be5bf8d:36files verified against the
recorded main/native/host/statistics manifests; inventory in
/data/tmp/cache-predicate-pruning/build44-git-source-match.json.
Other codecs/default correctness plus new performance observations remain pending.

Build45 freezes the optional predicate timing harness and disables only existing
Spark compression diagnosticlogging. Validation supervisor executes17bounded
CPU/GPU proof applications serially; timing supervisor waits for them, then
pauses after the first256MiB/task result for independent root validation.
All8predicate setups now PASS (Snappy/PCBSbuild44; default/uncompressed/LZ4/Zstd/
ANS/typedCascadedbuild45). Remaining interoperability/cleanup probes are running.
No new timing results or compatibility completion is claimed yet.
Scripts/status: /data/tmp/cache-predicate-pruning/{validation,timing}-status.json.
Worker verification handoff repeats MCP-only/no builds/no GPUjobs; confirmed.

Additional whole-checkout scalastyle invocation exposed4long lines introduced
by root's fixture-builder correction; fixed. Other48failures are under an
unrelated preexisting .agent-worktrees tree and are not edited. A clean isolated
sourcecheck will distinguish final owned changes from those unrelated files.
The documented old scalastyle:check prefix is unavailable in this checkout;
use the existing parent antrun execution scalastyle-all-modules with its
spark.rapids.source.basedir property. No check is skipped or disabled.

### Predicate first timing / actual PCBS pruning checkpoint

All17build45 validation applications passed; root independently checked raw
completions, terminal assertion messages and production-source proof bindings.
Together with Snappy/PCBSbuild44 predicate cases, allcurrent8formatsetups have
realSQL predicate/residual correctness evidence. CPUcolumnar INPUT remains
unverified as before; CPUrows/ArrowWriter and allfourCPU/GPU write/read combinations
for supported CPUcodecs are verified. GPU-onlyANS/Cascaded CPUcodec scope unchanged.

Clean isolated scalastyle check at4d256e0d4a passed2041files,0errors/0warnings,
plus4provider-selection tests; log /data/tmp/cache-predicate-pruning/scalastyle-final.log.
Spark330 compilation was attempted with the pinned private330 artifact and failed
110missingjoin-symbol errors across8UNCHANGEDsourcefiles. No changed pruning file
appears in emittederrors; this is not a successful330compile or a proven baseline
failure (baseline330was not rebuilt). Artifacts compat330/{maven.log,error-scope.json}.
Do not broaden scope to repair unrelated join/backend compatibility here.

First timing case timing-cpu-snappy-256-45 VERIFIED independently by root:
152.80s whole job,6.907GiB sampledRSS,132records,516.921MiB encodedtotal.
Population2.912s; normal CPUfull2.538/CPUID0.145/GPUfull0.263/GPUID0.095s.
Predicate full-read medians pruningoff->on:
CPU allmatch3.261->3.219s, highquarter2.624->1.650s, none2.133->0.063s;
GPU allmatch0.196->0.203s, highquarter0.180->0.165s, none0.174->0.088s.
These are samecache/samepredicate pairs, not normal-vs-filtered timing claims.
Highquarter decoded1073225472->268306368bytes,4224->1056roots;
GPU decodecalls4->1 and cache outputrows4194304->1048576.
No-match on decoded0bytes/roots/calls/outputrows; allmatch preserves work.
No recomputation observed. Root reproduced sampledRSSpeak from470rawsamples.
A disposable rawresult with fabricated no-match selectedbytes was rejected;
no acceptance criterion was weakened.

Verifier /data/tmp/cache-predicate-pruning/verify_timing_results.py,
SHA dfcde35da9e1bdba90ae21e1dc1eb1de92d8cd14868d42a13121b7def2fee511.
Root read implementation, independently ran it and all10negative checks.
verified-first.json contains full evidence. First-case review marker released
remaining13serial256MiB/task timing cases; aggregate cohort still IN PROGRESS.
All operations/worker checkpoints remain MCP docker_exec-only.

Separate PCBS SQLmetrics diagnostic (DEBUGmetrics + RESTpolling, NO timingclaims)
uses the same e849production/build45 artifacts and approved memorysettings.
Root read decisive SparkREST/store/CPUscan and GPUmetric/scan code.
For BOTHCPU/GPU writers and BOTHCPU/GPU readers:
highrange cache rows20005->5002 with pruningoff/on, residual rows1004unchanged;
no-match cache rows20005->0, residual rows0unchanged.
This confirms actual scan pruning rather than only direct helper behavior.
Artifact pcbs-scan-metrics-45/verified-scan-pruning.json references captured
SQLexecutionIDs and rawRESTplans/metrics.89completedSQLexecutions captured;
not every last execution was captured, but ALL8required comparisonpairs were.
The diagnostic does not modify sources or enter headline timingresults.
