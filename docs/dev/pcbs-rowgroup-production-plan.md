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

Initial validation: the metadata helper's nine CPU tests passed in worker gate003
and root independently recompiled/reran them in root-helper-cpu-gate001. Maven
build003 compiled the production plugin but found new fixture compile errors
(reader class-name collision, event-handler import and Java varargs overload).
Root stopped the owned build after those diagnostics (exit143, 558.265 seconds);
no runtime test passed from that build. Workers are fixing the fixtures and adding
CPU-written dictionary/plain GPU decoding and whole-materialization retry cleanup
coverage. Review identified partial parent-pinning risk, not an observed deadlock.
Build004 then found only three deprecated Parquet-MR int overloads in fixtures;
root verified the long overload and retained identical sizes. Build005 PASS
(92.450 seconds): all 50 selected Scala tests and nine Java tests passed. This
includes GPU NONE/SNAPPY decoding after writer/staged-input spill, later-parent
retry cleanup, CPU-written dictionary/plain labels and projection, chunk replay,
early/task cleanup, existing writer/schema conversions and pending GPU-output
exception cleanup. Root inspected the raw test summaries and idle GPU state.
These focused correctness gates are READY FOR REVIEW; pressure, SQL integration,
remote/replication, cross-version and performance gates remain IN PROGRESS.

The baseline checkout at ../baseline-worktree is detached at 60c28e44f0. Its full
package build passed (196.777 seconds, tests deliberately skipped for baseline
packaging). Root froze the actual JNI and private-dependency artifacts and
byte-checked all 237 private classes in the shaded baseline. The new benchmark
adapter was independently reviewed, root reran its 28 Python checks, then
compiled it and passed its pure-JVM action gate (performance/bench-build001).
An initial mistaken claim that the old harness accepted a production action was
caught by root before execution and corrected in a separate minimal harness;
frozen originals were not modified. No production performance numbers exist yet.

Latest verification checkpoint (MCP docker_exec only): build010 full Maven verify
PASS (278.804 seconds), with dist, tests and integration-test artifacts packaged,
52 selected Scala tests plus nine Java tests, RAT and all module style checks.
Two new tests cover producer-finalization failures after one/multiple output chunks.
Build009 was stopped after the Maven compiler code cache filled; build010 increased
only the Maven process code cache to 1 GiB. Spark benchmark controls are unchanged.
The full RAT check also required adding only an Apache/NVIDIA XML license comment
to the older pcbs-reset-sweep.svg; chart data and rendering are unchanged.

Concurrent managed-host-pressure gate is READY FOR REVIEW: worker run001 and
root independent root-run002 both PASS (9.379 seconds each). Four 2,111,779-byte
records were forced to disk, two readers competed under a 6 MiB host limit with
a 1 MiB reservation, and one real RMM retry caused whole-attempt cleanup/progress.
All 4,096 rows decoded exactly; no injected failure and zero host usage after
shutdown. This proves concurrent host materialization, not concurrent GPU decode.
Evidence: ../pressure-gate/{run001,root-run002}/result.json and retained fixture.

The first Java SQL integration lane (integration-java001) failed all nine new
fixture cases before cache validation: its nested/binary xxhash64 oracle requested
an unsupported GPU/CPU bridge expression. This is not a passing production gate.
Repair the fixture with equally strong exact-data checks, then rerun Java/Kryo
in fresh JVMs. Remote/replication, cross-version and production performance remain
unverified. All temporary fixtures/runtimes are kept outside the production tree.

Core implementation is committed at c385525a51. Two independent agents found no
blocking resource/format issue; root reread the decisive implementations. The new
nine SQL cases are READY FOR REVIEW on Java (integration-java003): all nine pass,
no failures/errors/skips, exact reordered nested/binary/decimal/timestamp values,
CPU rereads, NONE/SNAPPY and disk/serialized-memory/deserialized-memory+disk levels,
partial consumption, blocking unpersist and recreation. Root inspected raw JUnit.
Java001/002 remain failed fixture runs: unsupported hash/binary length, then inert
CPU scan-predicate metadata in GPU assertions. The corrected tests retain strict
GPU scan/filter assertions and only exempt those metadata expression classes.
These failures happened during read validation after cache population; they do
not establish a cache-format defect. Java003's observed JVM heap was 1 GiB despite
a requested DRIVER_MEMORY=4g runner setting. This is a correctness-only gate, not
a performance comparison; later lanes must pass the heap explicitly to SparkSubmit.

The expanded-string correctness gate PASS and root-verifier rerun are READY FOR
REVIEW (string-chunk-audit/run001). Six 512 MiB GPU inputs contain 3 GiB of strings;
384 stored records total 665,472 encoded bytes with 12,599,040 estimated decoded
bytes. One logical bundle decodes to four chunks, exact 98,304 rows/all contents,
maximum 1,065,353,216 character bytes per chunk. No injected failures. Root verified
the actual loaded PCBS/helper/reader/native class bytes equal the frozen benchmark
runtime. This is not a timing result or a universal page/giant-value bound; the
separate legacy CPU-written expanded-string extreme remains unrun.

Isolated Spark330/Scala2.12 and Spark401/Scala2.13 compile/package gates PASS in
239.412/273.897 seconds, including production and test-source compilation and
dist/integration artifact packaging. Tests were explicitly skipped in this CPU-only
stage; runtime unit/integration and verify/style gates remain pending. Evidence:
../crossversion/compile{330,401}-002. The first launcher attempts failed before
Maven due incorrectly shell-quoted inherited MAVEN_OPTS; corrected with no source
or control changes. Both full logs/build outputs were inspected by root.

A matched production-performance runtime is frozen at ../runtime-matched001.
Root independently rehashed 246 artifacts and byte-checked all 237 private classes
in both baseline and patched against the same frozen dependency. Canonical/world
mode gates and layout fixtures compile; actual production performance remains
pending. Native/codec overlays are excluded; defaults reset100/unroll16 and memory
fraction0.6 remain unchanged. Benchmarks must run alone after correctness gates.

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
