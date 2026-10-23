# PCBS row-group production implementation plan

Status: IN PROGRESS. The user authorized finishing the real-data sweep, then a
reviewable production patch with tests, performance checks and independent agent
reviews. The 48-case sweep is complete and READY FOR REVIEW: 46 PASS, two genuine
heap OOMs; root independently reverified every result and frozen hash. Only the architect marks completed work DONE; verified work is READY FOR
REVIEW. The real-data sweep remains the prerequisite for implementation.

## Implementation checkpoint

The isolated production worktree is /data/pcbs-oom/pcbs-rowgroup-production/worktree,
branch pcbs-rowgroup-production, based on 60c28e44f0. The original repository and
all experiment sources remain unchanged. The full sweep and independent hash/
result verification are READY FOR REVIEW; production gates remain IN PROGRESS.

- Metadata helper and focused new suites: worker producer_target_trace.
- Combined chunked reader and focused new suite: worker controlled_launcher,
  dependent on the agreed metadata-helper interface.
- Focused integration tests in cache_test.py: worker parked_case_audit.
- Existing PCBS integration, existing suite adaptations, builds/GPU scheduling,
  independent verification, tracker/commits and review coordination: root.

Every worker uses MCP docker_exec only, confirms it at checkpoints, and reports
files, verification and material deviations. No parallel GPU jobs or conflicting
builds. Root owns commits after inspecting code and verifying decisive checks.
No changes to frozen fixtures/artifacts, unrelated user files or Git configuration.

## Scope and compatibility

Use MCP docker_exec for every operation, including workers. Confirm at handoffs
and checkpoints. Root coordinates, reads decisive code and independently verifies
changes. Preserve frozen experiment sources/results and unrelated user files;
use an isolated production worktree after the sweep. Commit each completed piece
with its tracker update and DCO sign-off. No external posting or PR submission.

The released PCBS, Spark cache and StorageLevel behavior exists on main and in the
customer deployment. Preserve supported types, CPU/GPU reads, row counts, column
selection/reordering, compression settings, schema transformations and ordinary
BlockManager persistence/replication/removal. Keep ParquetCachedBatch(numRows,
Array[Byte]) and its actual encoded-byte size reporting. Every stored record must
remain an independently readable Parquet file. No new storage or ownership
protocol, public configuration, Spark OFF_HEAP, or Spark-core changes.

The prototype/helper/overlay are UNRELEASED. Promote only the useful algorithms;
remove research-only counters, hard-coded global targets, artifact loaders and
experimental guards from the production design. No compatibility adapter for the
prototype. A new custom cache codec is separate, deferred work: the initial patch
must work with ordinary Java/Kryo and the existing BlockManager/MemoryStore.

Initial production scope is GPU-written PCBS records, combined GPU reads and
compatibility with existing CPU readers and CPU-written PCBS records. Adapting
the separate parquet-mr CPU producer was explicitly deferred earlier; document
its remaining partition-wide materialization rather than claiming this patch
fixes that path. Evaluate a separate bounded follow-up after the GPU patch.

## Implementation choices to validate

1. Retain large GPU input batches and existing column-limit slicing. Set private
   Parquet row-group targets to the measured 65,536 rows / 8 MiB. These are soft
   writer targets, not hard encoded-record or heap limits.
2. Read footer/chunk metadata over writer callback buffers and lazily copy one
   complete row-group Parquet record at a time. Preserve distinct dictionary and
   first-data offsets, row counts, schema and compression; reuse the scan's
   metadata/framing patterns and GpuParquetUtils.newBlockMeta.
3. Make completed writer callback regions spillable immediately. Existing default
   allocator uses HostAlloc accounting, but retained raw buffers are not spillable.
   Hold SpillableHostBuffer handles and rematerialize one region at a time for
   copying; close references promptly. Retain normal host-limit admission. Active
   writer allocations remain distinct from row-group size and need tests.
4. Stage small read records incrementally into RAPIDS spillable host buffers.
   Bundle using configured GPU target, encoded size, row count and decoded-size
   hints. Hints are not bounds. Never retain a heap array for the whole bundle.
5. First validate reconstruction of one logical Parquet file over borrowed host
   column-chunk slices, a small header and a combined footer. Existing
   Table.readParquet/ParquetChunkedReader buffer APIs can read this without the
   prototype's extra native library. Keep parent references and slices alive
   through native consumption. Preserve page offsets and source order.
6. Prefer the existing chunked reader and retry/checkpoint patterns over a new
   all-at-once reader protocol. Verify output chunking, retry replay, task cleanup
   and string/nested limits. This is a candidate integration, not yet measured;
   the completed benchmarks used the research true multi-file helper.
7. Preserve large supported records. The prototype's 256 MiB rejection guard must
   not become a new production restriction. Permit an oversize singleton within
   existing representation limits; bound row counts before combining. Do not
   silently reject supported data or claim soft targets eliminate giant rows.

## Ordered acceptance gates

- Finish all 48 immutable real-data cases; independently verify exact data,
  controls, timing/heap/storage results and negative outcomes. Publish the single
  combined table with residency and sampling/single-JVM caveats.
- Validate logical-file reconstruction with CPU and GPU decoding before replacing
  the measured native helper. Cover multiple files/groups, dictionary and plain
  pages, nulls, nested arrays/maps/structs, binary, decimals, timestamps, projected
  and reordered columns, NONE/SNAPPY, empty schema/records and old multi-group files.
- Validate ownership on normal exhaustion, early stop, cancellation and exceptions.
  Force completed writer-region spill under a small managed host budget; verify
  exact results and cleanup. Materialized reader inputs/slices cannot spill until
  native consumption releases them: verify bundle admission and concurrent host
  pressure, not only writer-region spill. Exercise chunk-reader retry/replay without duplicates
  or lost rows. Large rows/oversize singletons must make progress.
- Explicitly verify ordinary Java and Kryo round trips, MEMORY_ONLY,
  MEMORY_AND_DISK and DISK_ONLY, serialized storage variants, and a replicated/
  remote-read path. Preserve blocking unpersist/early-reader cleanup. Report which
  paths were actually exercised rather than inferring replication from a local
  storage-level flag.
- Build/check relevant supported Spark shims and run existing PCBS suites plus
  focused new unit/integration tests. No acceptance weakening. Record unavailable
  coverage precisely. No pom changes without Scala 2.13 file synchronization.
- Verify the actual production artifact on the canonical 96 GiB synthetic workload
  at default reset100/unroll16 and unchanged GPU/heap/host controls, then representative
  real-data disk and mixed-residency cases. Preserve total rows/partitions. Record
  fill/read wall times, observed heap, actual stored and decoded layouts, compressed
  sizes and residency. Use fresh JVM repeats for the decisive performance result;
  do not compare new implementation claims solely against prototype timings.
- Have independent agents review resource/correctness and compatibility/performance
  risks. Root reads changes and independently runs decisive checks. Commit results,
  limitations and tracker together; deliver READY FOR REVIEW, not production acceptance.

## Park conditions and remaining decisions

Unexpected format/native failures, unavoidable supported-input regressions,
required Spark-core changes or unsupported API/ABI dependencies require a recorded
architecture checkpoint. A benchmark heap OOM is a measured negative result.
Routine fixture/build mistakes should be corrected without changing the experiment
controls or weakening correctness checks. If the logical-file reader fails its
correctness/performance gate, compare a supported cuDF multi-source API exposure
explicitly; do not ship the scratch native loader/header assumptions.

The private row-group targets need evidence across additional schemas, not a new
public tuning surface by default. Neither small records nor chunked reads promise
an arbitrary small heap or universal giant-value support. CPU-producer streaming
and the long-term per-cache Spark codec remain separate follow-ups.
