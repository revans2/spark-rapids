# GPU contiguous packing and nvCOMP exploration

Status: IN PROGRESS. User approved GPU codec and compressed host round trips
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

FINAL cohort artifacts: staged-typed-05. Six 256 MiB/task fixture/block cases
(matrix-{fixed,low,wide}-256-{64k,256k}) all PASS, each with one recorded warmup
and two measured repeats, four work items and two slots. Independent raw JSON
checks reproduce framing/retained-byte sums, actual pinning, all values, zero final
allocations, budget limits, and timestamp inequalities proving phase barriers.
The uncompressed baseline repeats across block sizes as a consistency control.
Do not mix earlier pilot/staged-typed-03 results with this deterministic cohort.
Source generation and validation peaks are distinguished from encode/decode GPU
live peaks using scoped RMM counters reset only at completed phase barriers.

Next run 512/1024/2048 MiB/task in increasing risk order under the same caps.
Inspect failures before dependent work; no cap relaxation or host-OOM pressure
experiments. Report final results inline with encode, decode, stored size/ratios,
block-size tradeoffs and memory caveats. Full Spark serializer integration and
matched PCBS timings remain outside this first experiment.
