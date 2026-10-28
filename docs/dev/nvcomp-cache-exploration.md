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

