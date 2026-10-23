# PCBS cache experiments with public datasets

Status: IN PROGRESS. User authorized real-world compression, heap-size and storage-level
experiments on 2026-10-23. This extends the completed row-group prototype; it does not
resume the stopped synthetic matrix.

## Scope and controls

Use MCP docker_exec for every project operation, including delegated work. Confirm
this at handoffs and checkpoints. Root coordinates, reads decisive code, independently
verifies workers and commits completed tasks with tracker updates. Verified items are
READY FOR REVIEW; only the architect marks them DONE.

Existing Spark cache/StorageLevel and PCBS behavior is RELEASED (main and deployed
artifacts). Preserve defaults and supported contracts. New harnesses, manifests,
observation overlays and prototype formats are UNRELEASED; change them directly
without compatibility scaffolding. Do not modify public datasets, installed Spark,
production plugin/cuDF algorithms, or frozen prior benchmark sources/results.

The user explicitly deferred adapting the CPU branch of PCBS to row-group storage
and profiling it against existing PCBS. Those tasks remain LATER. Spark's default
CPU cache is a reference here, distinct from that deferred CPU PCBS work. Heavy
downstream workloads, profiling, native persistent payload ownership and Spark
OFF_HEAP storage remain outside this batch.

## Datasets and frozen inputs

- TLC high-volume FHV, all 2024 months: 239,470,448 rows, 12 files, mixed numeric,
  timestamps, short strings and real nulls.
- GHArchive, the available seven-day sample: 32,078,549 rows, 7 files, JSON/text,
  timestamps, boolean and nulls.

For these experiments the user authorized reading TLC's timezone-free timestamps as
Timestamp. Set spark.sql.parquet.inferTimestampNTZ.enabled=false and session timezone
UTC consistently for the oracle and every benchmark mode; verify the inferred schema.
This avoids extra cast operators and preserves the source files. The default inferred
TimestampNTZ is unsupported by the GPU path and is not silently benchmarked as fallback.

The inventory inspected 4,343 Parquet footers without errors. Source file sizes and
uncompressed column-page sizes are recorded, but the latter remain dictionary/RLE
encoded and are NOT decoded heap/device bytes.

Prepare 16 immutable partition files per dataset by copying complete encoded row
groups with Parquet-MR appendRowGroups. Keep sorted source-file/row-group order;
do not duplicate rows, shuffle, re-encode values or invent data. Preserve schema and
per-file metadata by putting partition boundaries at metadata changes (TLC has two
ARROW:schema metadata variants). Balance remaining cuts by page-byte sizes, and
report actual per-partition rows/size/skew rather than assuming equal partitions.
The API omits page/offset indexes; record this explicitly. Full scans use no
predicates, and all arms read the same canonical inputs. Source files remain intact.
Verify source and canonical rows, schema and column compressed/uncompressed totals;
a separate CPU oracle records all-column checksums and measured value-byte totals.

## Initial matrix

One fresh JVM for each of 48 cases: two datasets, four methods, three heaps and two
storage levels. One warmup and three measured reads per populated cache. These are
initial tradeoff measurements, not repeated fresh-JVM confidence runs.

| Dimension | Values |
|---|---|
| Cache methods | Existing GPU PCBS + Java; row-group PCBS + Java; row-group PCBS + custom codec; Spark default CPU cache |
| Configured heap | 48, 16, 8 GiB; record actual Runtime.maxMemory |
| Storage | DISK_ONLY; MEMORY_AND_DISK |
| Parallelism | local[8], 16 fixed input/cache partitions |
| GPU controls | 1 GiB global batch/decode target; 15 GiB pool; concurrency 2 |
| Other controls | RAPIDS host limit 40 GiB; pinned pool 2 GiB; memory fraction 0.6; ParallelGC with four GC workers |
| Serialization | JavaSerializer; default objectStreamReset=100, unrollMemoryCheckPeriod=16; RDD stream compression disabled |
| Cache compression | Existing enabled behavior: Snappy for PCBS; Spark's own column encodings for default CPU cache |

Keep data, partitioning, cache record policy and query identical as heap/storage
changes. No forced GC or tuning to match GC behavior. CPU reference runs without GPU
acceleration; label its different cache format and compute path. It cannot isolate
Java-versus-codec costs.

The existing-PCBS observation artifact is cap1024-only.jar from the controlled
benchmark, with its old observer. At the fixed global 1 GiB target its extra private
cap is redundant: both budgets equal target * 0.995, as root verified in source.
No immediate scan coalescing override is used. Row-group arms reuse artifacts-005;
only custom-codec mode adds the research SerializerManager overlay. BlockManager
and MemoryStore stay stock. Preserve class origins and artifact hashes.

Start with CPU oracle and small compatibility/integrity gates, then DISK_ONLY
compression references at 48 GiB, then MEMORY_AND_DISK and smaller heaps. Freeze a
concrete schedule before large runs. Unexpected fixture/native/format failures park
dependent work. Heap/GC-overhead OOM is a measured negative result; retain failed
phases and do not manufacture successful timings. No retries or acceptance weakening
to obtain PASS.

## Measurements and interpretation

- Exact row count plus two salted all-column xxhash64 fingerprints, split into low/
  high 32-bit sums to avoid signed-sum overflow. Include field-position and null flags.
  Compare every cached read to the CPU oracle. Report the hash-collision limitation.
- Oracle raw-value bytes: actual UTF-8 octets plus non-null scalar value widths.
  Offsets, null masks, allocator padding and intermediate objects are excluded.
  This is a defined common denominator, not a heap estimate.
- Actual producer/decoded GPU bytes and batch distributions are separate measured
  footprints, including representation overhead. Do not infer them from footer sizes.
- Cache payload bytes: PCBS Parquet arrays versus default CPU column buffers.
  CachedBatch.sizeInBytes differs in meaning across these formats; do not use it
  as a common uncompressed-size denominator.
- Actual per-partition serialized disk-block file bytes at phase boundaries, separately
  from Spark status and estimated memory accounting. DISK_ONLY provides a complete
  serialized footprint; MEMORY_AND_DISK disk bytes are residency of a subset and
  must not be extrapolated into a whole-cache compression ratio.
- Record actual encoded record ranges/outliers, footer overhead, stored byte totals,
  fill/read wall time, warmup, observed peak fill/read heap, GC, RSS/HWM and sampled
  physical/read/write I/O. Heap occupancy includes garbage; it is not live heap or
  minimum required memory.
- Preserve plans and operator metrics so checksum computation/CPU fallback cannot be
  silently mistaken for cache-storage cost. GPU PCBS arms must actually use their GPU
  writer/scan paths. Keep the query a verification reduction, without extra GPU work.

Serialize GPU jobs and own each process tree/scratch directory. Preserve manifests,
argv, logs, phase markers, verifier JSON and disk inventories. Snapshot block sizes
before blocking unpersist and process exit. Avoid retaining full spill stores across
cases; never delete a live or unowned process's data. The host limit covers managed
RAPIDS allocations, not a demonstrated whole-process cap on cuDF writer callbacks.

## Evidence and pending work

New evidence root: /data/pcbs-oom/pcbs-real-world-cache/.
Inventory: dataset-audit.json/.md and stable source-file lists.
Source/API and resource audits are recorded there. Immutable input preparation is
READY FOR REVIEW: both datasets preserve all original row groups and column bytes.
Root independently verified all 561 copied row-group hashes, contiguous complete
coverage, rows and column totals in root-input-audit001.json. Small decoded-value
roundtrips also pass; the initial footer-hash-definition mismatch was a fixture
error and is preserved as failed evidence. Harness and launcher/verifier remain in
review. Root will verify the oracle, class origins and small runtime gates before
starting the frozen large-case schedule.
