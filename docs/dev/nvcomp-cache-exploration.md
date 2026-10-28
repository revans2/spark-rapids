# GPU contiguous packing and nvCOMP exploration

Status: IN PROGRESS — REAL SPARK PROTOTYPE. User explicitly moved from the
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
