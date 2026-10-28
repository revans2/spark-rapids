# GPU contiguous packing and nvCOMP exploration

Status: IN PROGRESS — user returned repo; current-source baseline rebuilt and verified.
Source/provenance repairs and the corrected 256MiB/task cohort are READY FOR REVIEW.
Current-source CPU/GPU profiling is verified and READY FOR REVIEW below.
User-approved decoded batching and its impact reruns are READY FOR REVIEW below.
Configured-target batching is implemented; final 256/512/1024/2048 MiB/task
matrix is IN PROGRESS with spill observations. Large-string matrix remains pending.

IMPORTANT BASELINE CORRECTION: PCBS numbers before the resumed current-source
cohort section describe the older hash-pinned prebuilt jar, not this branch's
ParquetCachedBatchReader. The new cohort below explicitly fixes that mismatch.

Previous scope: REAL SPARK PROTOTYPE. User explicitly moved from the
codec/host round-trip microbenchmark to a real Spark cache serializer. CPU encode
and decode fallback MUST THROW for the new prototype (explicit user instruction).
The microbenchmark phase below is historical evidence, not Spark cache timings.
The original first step was GPU codec and compressed host round trips
before a full Spark cache serializer. No CPU encode/decode implementation or
claim is required. GPU-only capabilities of current installed nvCOMP are the
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

## Configured target and large-task spill observations — IN PROGRESS

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

Remaining final cohort: ANS, Zstd, LZ4, typed Cascaded, uncompressed and default
at all4sizes (24runs). Do not restart completed final runs after resumption.
Re-read this tracker and use MCP tools only. Benchmark conditions remain fixed.
Architect acceptance remains pending; implementation checkpoint READY FOR REVIEW,
complete matrix not yet delivered.
