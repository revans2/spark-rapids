# PCBS row-group production implementation plan

Status: IN PROGRESS. The user authorized finishing the real-data sweep, then a
reviewable production patch with tests, performance checks and independent agent
reviews. The 48-case sweep is complete and READY FOR REVIEW: 46 PASS, two genuine
heap OOMs; root independently reverified every result and frozen hash. Only the architect marks completed work DONE; verified work is READY FOR
REVIEW. The research real-data sweep is complete. The production implementation is
committed; its separate measurements and supported-version checks remain gates
for final review.

Current runtime checkpoint: the fixed 12-case byte-target screen remains the sole
benchmark in ../byte-target-screen/matrix001. Its first two DISK_ONLY cases passed:
old c385 settings filled in 151.324 s with median read 72.578 s; byte8 with the
normal cuDF row cap filled in 149.122 s with median read 72.074 s. Both produced
12,400 records. These are provisional single-JVM receipts, pending root replay of
the closed matrix; they establish no clear timing difference. No selection has
been made. The 16 MiB DISK case also passed: fill 156.266 s, median read
71.460 s, observed phase heap peak about 20.1 GiB and 6,256 records. The 32 MiB
DISK case passed: fill 154.756 s, median read 50.624 s, observed peak about
39.5 GiB and 3,184 records. Its later two reads recorded zero physical read
bytes, versus approximately 100 GB per read in earlier cases. That timing drop
is confounded by changed physical I/O/page-cache conditions; it is not evidence
of a clean target-size speedup. These remain worker receipts awaiting root's
closed-matrix replay. All eight canonical cases have now passed the queue checks.
For memory-and-disk, byte8 filled in 161.659 s/read median 38.262 s/observed peak
43,063,262,704 B; byte16 143.309 s/39.571 s/42,104,520,704 B; byte32
164.830 s/40.973 s/46,826,258,432 B. The old control was 148.849 s/38.428 s/
44,010,831,872 B. Most measured mixed-storage reads had zero physical read bytes;
byte32's first measured read recorded about 1.62 GB. These are single-JVM
receipts, not a confidence interval or minimum-heap result. Full TLC cases remain.
After the closed screen is independently verified, a bounded six-case full-GH
pressure check is planned: byte8/16/32 at the previously requested 8 GiB heap,
each DISK_ONLY and MEMORY_AND_DISK. Data, partitions, threads, GPU/host controls
and defaults remain fixed; only the heap dimension changes. Root will use the
existing unchanged world runner, one case at a time, and preserve genuine OOMs.
No new launcher framework or production change is required.
The passive OS sampler missed the first fill and started partway through the
first warmup; missing history will remain unknown.
Root read the new final-only OS phase adapter and its original metric decoder,
independently passed its four pure tests, and obtained independent source review.
Actual analysis waits for 12 READY receipts, a closed sampler and idle locks.
The versioned final-source boundary/grouped and vendor/SQL launchers are now
source-reviewed by root and independent agents; root independently passed all
18 new pure tests. Preparation is READY FOR REVIEW in ../crossversion/
SELECTED-SOURCE-VERIFY-PREP-V1.md and SELECTED-VENDOR-SQL-PREP-V1.md.
They require an explicit final source commit, fresh isolated worktrees, writable
compiler caches and current 53-Scala/9-Java evidence. Grouped OSS builds remain
compile/package-only; vendor attempts do not become runtime acceptance.
No selected-source build, dependency-resolution attempt or SQL run has occurred.
The 16/32 MiB choice may require a larger generated correctness fixture to
preserve its multiple-record assertion; that assertion will not be weakened.
No compilation or other workload runs concurrently with the benchmark.
The final-only outer-compression report/replay tools are prepared in
../compression-queue. Root read both complete adapters and independently passed
four pure tests; independent review also passed. Actual compression runs, reports
and raw-verifier replays remain unrun. They distinguish outer Spark compression
from inner Parquet SNAPPY, retain physical-file versus payload size separately,
and require the closed 20-case queue before analysis.
Source tracing identifies per-host-buffer-view CUDA copy calls as a hypothesis
for small-record overhead. The standalone cuDF and JNI-vendored trees differ;
the corrected trace names both absolute roots and does not attest either to the
frozen binary. Actual capture is required before assigning counts or causality.
The narrow full-TLC profiling pair is prepared in ../selected-world-profiling
to examine the observed public-data regression, retaining its original
data/query/resources/oracle. Root read the complete canonical-to-TLC difference,
independently passed all six pure tests, and obtained independent source review.
Preparation is READY FOR REVIEW; actual plan creation/admission/capture waits
for the closed verified screen and explicit root target selection.
No optimized source or profiler run has been introduced.
The closed-screen rendering checker is prepared and independently reviewed; root
read it and passed four pure tests. It requires exact JSON equality with the
independent raw replay and checks every CSV field and Markdown cell/limit note.
The selected-boundary acceptance helper is also prepared and reviewed; root
read it and passed six pure tests. Its private dependency proof uses each target's
actual Scala/Spark-classified dependency, with all embedded class bytes checked.
Only 3.5.5/Scala 2.12 has the historical 237-class/SHA reference; asserting that
reference unchanged across Scala/Spark targets would be incorrect. The shared
JNI still requires its frozen common hash. Actual acceptance remains unrun.
All root and worker project operations remain MCP docker_exec only.

Current implementation checkpoint: the c385 production comparison is independently verified
(15 synthetic JVMs and 16 public-data cases). The byte-only 8 MiB candidate at
d9ba8787f6 is committed separately. All three byte-only candidates passed full 53-Scala/9-Java verify and matched
artifact audits in builds004. Root independently replayed all reports, checked
9,304 frozen source entries, actual JNI dependencies, all 237 private classes and
all 36 implementation-family classes against fresh module/distribution bytes,
and confirmed final quiescence. These candidate artifacts are READY FOR REVIEW.
Decoded-layout diagnostics passed; 8/16/32 MiB screening and final selected-source
compatibility remain gates.
Earlier checkpoint paragraphs below retain historical failures and pending states;
the completed repeat checkpoint and current byte-target section supersede them.

## Current instrumentation and candidate gates

All operations remain MCP docker_exec only. The full canonical decoded-layout
pair is running alone, using the frozen c385 implementation and original baseline.
The baseline diagnostic passed strict version-2 validation: 112 returned GPU
batches across 16 partitions, 805,306,368 rows and 16 columns; represented returned
buffers range from 819,467,520 to 953,942,144 bytes. Its actual plan contains only
the raw scan, with no full-row coalescer. These are represented buffer sizes,
not allocation peaks or timing measurements. The patched diagnostic also passed: 112 batches and the same exact rows/columns,
with median/max represented buffers of 1,072,241,792 bytes and a smaller final
batch per partition. Its actual plan also has no separate full-row coalescer.
Root independently replayed both complete strict verifiers and required exact
retained-result equality, then confirmed no live JVM or GPU owner. Small stored
records therefore still produce large GPU batches in this measured workload.
Evidence: ../layout-queue/pair001 and ../support-audit/root-layout-pair001.json.
Diagnostic timings are excluded from the primary performance comparison.

The new byte8 Java/Kryo SQL owner is prepared in ../byte-target-sql, independently
reviewed, and root independently passed its four small acceptance tests. It requires
actual three-candidate 53-Scala/9-Java build manifests, the clean committed byte8
source, exactly nine non-skipped cases per lane, observed 4 GiB SparkSubmit heap,
actual serializer/registrator and exact full-path plugin class origins. Review
caught and corrected a filename-only origin check before execution. No candidate
build or SQL runtime result is claimed yet. After layout acceptance and verified
quiescence, root launched the reviewed three-build owner at
../crossversion/byte-target-builds001 (supervisor 528536). It builds committed
byte8 d9ba8787f6, byte16 cebb9206d5 and byte32 abec8b6da5 sequentially; each must
actually pass the fresh 53-Scala/9-Java verify and matched artifact checks before
any benchmark admission. Worker monitoring remains MCP docker_exec only; root
owns subsequent scheduling and independent acceptance.

The fixed 20-cell outer-compression queue is prepared in ../compression-queue,
independently reviewed, and root read the owner and passed all five small tests.
It compares baseline and an explicitly selected byte-target candidate on both
public datasets, at 48 GiB heap and DISK_ONLY, with compression OFF/LZ4/ZSTD/
SNAPPY/LZF. Parquet SNAPPY and the workload controls remain fixed. This is a
single-JVM screening matrix; actual candidate selection, artifact admission,
codec availability and runtime remain unverified. No compression result is
claimed. The selected-artifact Nsight preflight also records that the existing
frozen profiler owner cannot select new artifacts; any later adaptation must
preserve its controls, ownership and strict verifier. CUDA activity time and host
GPU-semaphore holding time must be reported separately.

The first byte-target build parked after 29.328 seconds before production
compilation/tests: Scala's compiler bridge tried to create a cache in read-only
/home/roberte/.sbt. This is a launcher environment failure, not a candidate result.
The failed queue/log/source hashes remain intact; its three generated target
folders were moved into its preserved-generated-targets directory after checking
clean committed sources and quiescence. A versioned v2 owner adds only the writable
-DsecondaryCacheDir setting already used by earlier supported-version builds;
root passed its seven small tests. Independent review and all seven tiny tests passed; root scheduled fresh
byte-target-builds002 with supervisor 529893. Its raw build log confirms the
compiler bridge installed successfully in the writable cache and passed the prior
failure boundary. Actual candidate verification remains pending. No source,
benchmark control or acceptance threshold changed.

Selected-artifact profiler preparation is source-reviewed in
../selected-profiling: a versioned immutable plan changes only the selected
matched descriptor, labels and fresh trace paths; both baseline and patch use
the same descriptor. It reuses the existing owned process and strict audit
helpers and requires actual target runtime/memcpy/kernel/NVTX data before marking
READY_FOR_CAPTURE_REVIEW. Root read the owner and independently passed seven
small tests; independent review passed. Actual permissions, capture completeness,
export schema, timestamps and phase attribution remain unverified. No profiler
run or new Spark setting is claimed. Native GPU activity must not be inferred
from semaphore-held task time.

Byte-target-builds002 then completed byte8 Maven verify successfully (6m54s),
with 53 Scala tests and nine Java tests and zero failures/skips. The queue parked
before artifact freezing: inspect_reports counted Spark SQL's matching tests.jar
as an extra main runtime jar. Root independently replayed the raw XML classpaths
and jar member inventory: all six suites use exactly the expected 3.5.5 main/test
pair, the same pair as build010, and the two jars share no class names (2,842 main
classes versus 6,501 test-support classes). This is a checker bug, not a production
or runtime test failure. Evidence: ../support-audit/root-sql-classifier-evidence001.json.
All original logs, reports and frozen owner sources remain intact. A narrow
versioned classifier-aware checker and explicit recovery of the completed build
are being prepared; byte16/32 builds have not started and no byte8 artifact or
benchmark is accepted yet. The checker must still reject wrong versions,
classifiers, duplicate main/test jars and incomplete/skipped test execution.

The v3 classifier-aware inspector and recovery owner passed independent review
and root's four small tests. They require exactly one expected main SQL jar and
an optional exact matching tests jar, retaining both hashes; other versions,
classifiers, duplicates and missing jars still fail. Root read the decisive
inspector/recovery/freeze path and launched ../crossversion/byte-target-builds003
(supervisor 538677). It replays the completed byte8 reactor against the original
command timestamp and unchanged source freeze, preserves its raw records with
explicit recovered-build receipts, and still requires all 53+9 tests, JNI/private
class and fresh module/distribution audits before accepting the artifact. Only
byte16/32 subsequently compile. Original attempts remain unchanged. Recovery and
remaining build acceptance are pending; no new benchmark result is claimed.

The first v3 recovery attempt (builds003) stopped before output creation or
Maven because its coverage guard treated Gitlink directories as missing files.
Root independently enumerated all 9,285 tracked entries: the only three absent
from the original file-only hash map are stage-0 mode-160000
thirdparty/parquet-testing directories, one per worktree. No regular file is
missing. V4 retains coverage for every tracked file and explicitly records those
Gitlinks; missing files and unexpected non-files still fail. Root and independent
review passed its five tests. Root launched builds004 (supervisor 539010), preserving
003 evidence. Its byte8 strict test/JNI/private/module/distribution audit has now
passed and its immutable artifact is recorded READY FOR REVIEW, explicitly reused
from the successful original reactor. Byte16 subsequently passed its fresh full verify (6m45s), all 53 Scala and nine
Java tests without failure/skip, and the same strict artifact audit. Byte32 is now
building; root's final three-artifact replay remains pending. All operations remain MCP docker_exec
only, with one owned build/runtime at a time. The lesson from the two audit
repairs is to test real dependency classifier and Gitlink metadata as well as
synthetic happy paths before trusting admission checks.

Three-candidate independent acceptance is READY FOR REVIEW:
../support-audit/root-byte-builds004.json. Actual owned Maven durations are
415.474 seconds (byte8 original build, explicitly recovered), 406.700 (byte16)
and 407.949 (byte32). All three ran all 53 Scala and nine Java tests without
failure/skip. Root's independent replay required exact retained test-evidence
agreement, actual JNI hash agreement, complete classifier-derived implementation
class coverage, unchanged source hashes/commits and exact fresh/module/distribution
bytes; no worker completion summary substituted for those checks. This is build
and focused correctness evidence, not all-version or performance acceptance.
Next gates are byte8 Java/Kryo SQL and the fixed full-size 12-cell screen.

Byte8 SQL correctness gates are READY FOR REVIEW on Java and Kryo. Java001
ran all nine expected tests successfully (127.365 seconds, actual 4 GiB), but its
v1 owner misparsed the actual jar:file shim origin and the Scala Files$ object
name. Root and independent review confirmed the fixture bugs and unchanged
frozen evidence. A narrow versioned parser retains exact outer-jar and approved
shim-root checks. Root ran the strict supplementary postcheck (2.4 seconds),
independently checked raw JUnit groups (2 shape, 6 storage, 1 partial/removal),
actual serializer/version/heap and all three exact class origins; the original
failed owner result remains intact. Evidence: ../byte-target-sql/java001-postcheck-v2.json
and ../support-audit/root-byte8-java001-acceptance.json. No Java workload rerun
or silent result rewrite occurred. Kryo001 passed v2 directly (125.663 seconds),
all nine tests and required Kryo registrator. Root independently replayed JUnit,
all 31 frozen entries, runtime/heap/origins and final quiescence:
../support-audit/root-byte8-kryo001-acceptance.json. These include the normal
injected-OOM correctness cases and are excluded from performance comparisons.

The fixed 12-cell byte-target screen is now running alone at
../byte-target-screen/matrix001 (supervisor 558869). Its four arms are the frozen
c385 8 MiB/65,536-row control and the new 8/16/32 MiB/default-row candidates.
Each runs the full canonical 96 GiB workload at DISK_ONLY and MEMORY_AND_DISK,
and full TLC at DISK_ONLY, all at 48 GiB heap with unchanged workload/default
retention/GPU controls. Worker monitoring remains MCP docker_exec only; root
owns acceptance and subsequent scheduling. The reviewed reporter has six small
passing tests and preserves incomplete OOM evidence with unknown whole-cache
sizes. Actual screen outcomes, report and independent hybrid raw replay remain
pending; one JVM per cell provides screening evidence, not confidence bounds.

The fixed12 report and hybrid raw replay adapters are source-reviewed and READY
FOR REVIEW as preparation only: ../byte-target-screen/report_byte_screen_v1.py
and reverify_byte_screen_v1.py, six small tests each passed independently by root
and review. Actual report/replay are pending closure of the running screen. The
replay retains original full canonical/world verifiers, exact approved canonical
sidecars, all metric equality and initial/final actual content checks through the
previously verified StableDigests implementation; it requires both locks and idle
admission before reading payload/artifact content. Root also retained the final
selected-source compatibility preflight: ../support-audit/final-selected-compatibility-preflight001.md.
It identifies old source pins/counts and the writable Zinc cache requirement in
future grouped/vendor owners; no final-source version coverage is claimed yet.

Root reused the existing read-only OS sampler during matrix001, after reading
its launcher and lifecycle. Sampler 560099 monitors exact parent 558869/start
425892959, checked independently against /proc and initial sample metadata. Its
source SHA is 96abe68e6b4878366c2b9d7968d5782c9698ea993e7169881731ec15da6cd56c.
It reads aggregate /proc/cgroup counters at a nominal two-second interval, writes
../byte-target-screen/matrix001/os-memory001.jsonl, stops on parent identity loss
or its 7,200-second bound, and launches no Java/GPU workload. Sampling starts at
1792817211941178921 epoch ns. The completed phase markers show that this is
about 17.6 seconds into the first warmup, after the 151.324-second fill finished;
the first fill has no OS-sampling coverage and that warmup is partial. This
corrects the initial interpretation of the live phase state; earlier history is
unavailable. Sampling overhead is unmeasured and not assumed zero.
It changes no data, storage or JVM/GPU control; resulting system observations
remain aggregate evidence, not causal attribution.

## Historical implementation checkpoints

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

The new SQL cases also pass Kryo (integration-kryo002): exactly nine nonskipped
cases, actual -Xmx4g, KryoSerializer plus the required GpuKryoRegistrator verified
from completed event logs. Kryo001 failed before tests because its launcher omitted
the required registrator; the ordinary launcher omission was corrected in a fresh
JVM. The separate Java compatibility lane passes all 122 selected existing cases
(integration-contract-java001, actual4 GiB): reordering, CPU/GPU mixed reads,
additional types, maps/arrays, binary and empty-column/converted-row cases. Root
independently parsed raw JUnit for all three lanes: root-integration-recheck001.json.
Some compatibility tests use the repository's normal injected-OOM mechanism; these
are correctness runs, not uninjected performance experiments. Existing mixed-test
string truthiness covers GPU-write/CPU-read, not both directions, and map/array
vectorized parameters are not applied by that existing test. Do not infer stronger
coverage from their parametrization count; old CPU-file GPU reads have separate units.

Ordinary stock BlockManager replication/remote gates pass Java and Kryo (17.246 /
17.649 seconds): ../remote-gate/{java001,kryo001}. Three distinct executor JVMs,
one GPU producer, three one-row-group records / 131,073 exact rows, clean replica
receiver CPU/native GPU decoding, third executor Network fetch without local
caching, identical copy digests within each run, and blocking SQL removal of all
copies/driver locations. Root inspected raw markers and independently reran both
verifiers. Actual driver2g/executor2048m flags, serializers/registrator and eager
RMM initialization are checked. These small tests share one physical GPU with
bounded per-executor pools; they do not prove production multi-node GPU isolation,
decommission/executor-loss behavior or the production combined-reader RDD path.
Fresh encodings across runs need not be byte-identical; the cause of different
cross-run file digests was not isolated. Exact decoded values were checked.

Coordinator checkpoint: all operations remain MCP docker_exec only. A next task
sent with send_message to a completed worker did not restart it, causing idle time.
Use followup_task for every new executable handoff, even when a worker may still
be running; reserve send_message for information. Check worker state after a
completion before assuming a queued action is active. This repeats an earlier
coordination mistake; no code/results changed.

Cross-version full verify gates are READY FOR REVIEW: Spark 3.3.0 / Scala 2.12
(crossverify330-001, 374.644 seconds) and Spark 4.0.1 / Scala 2.13
(crossverify401-001, 382.310 seconds). Each passed 52 selected Scala tests and
nine Java tests, zero failures/skips, RAT/style and the full selected reactor.
Root inspected raw logs, exact commands and successful process reports; no source
changes or live owned processes remained. This supplements the Spark 3.5.5 gate,
not a claim that three runtimes cover every supported release. The production
patch changes RAPIDS only; these version labels identify validation environments.

The release matrix contains 42 Scala/build pairs across 32 targets. All supported
versions remain in scope. A focused audit is checking the actual Spark-supplied
Parquet dependencies and both existing newBlockMeta/cache-scan shim boundaries;
the Maven test-artifact Parquet property is not production dependency evidence.
The [compatibility audit](pcbs-rowgroup-compatibility.md) records all groups,
exact shim selection and 34 required Parquet API descriptors. No OSS signature
or shim-selection defect was found. Actual dependencies range from Parquet
1.12.2 through 1.17.0; 401 uses 1.15.2, independently captured from its test JVM
classpath. Classfile equivalence is not runtime proof. Additional boundary gates
are 340/Scala 2.12, 350/Scala 2.13, 413 and 420; all three DBR vendor validation
lanes remain required. Their vendor artifacts/runtimes are absent locally; no
OSS substitute or all-target success is claimed. The audit is READY FOR REVIEW,
while all-target validation remains IN PROGRESS.
Four fresh Java/Kryo SQL lanes on 3.3.0 and 4.0.1 are now scheduled
serially. The first 330 Java run passed all nine tests but its audit rejected a
128 MiB Spark launcher bootstrap beside the actual 4 GiB application. A versioned
main-class-aware checker passed eight independent Python checks; the original
negative result is retained and fresh lanes remain pending.

Matched production measurement has started with unchanged original frozen
runners, runtime and default controls. The first baseline MEMORY_AND_DISK case
filled 96 GiB in 164.011 seconds, then failed read0 with Java heap OOM in
ObjectInputStream.readArray through MemoryStore disk recaching (exit 52, no
timeout). Actual baseline layout was 112 records: seven per 6 GiB partition,
807,025,755–939,457,451 encoded bytes each. The 1 GiB configuration is a target,
not a claim of exactly six encoded records per task. All sixteen partitions and
805,306,368 rows were verified from raw markers. No successful read timing exists.

The retained original verifier reports ERROR solely because SparkContext prefixes
standard module/IPv6 compatibility options to spark.driver.extraJavaOptions.
Root read the decisive stock SparkContext code; two independent raw audits prove
the exact prefix, unchanged requested options and actual 48 GiB heap/GC flags.
Root ran the independently reviewed strict verification sidecar successfully for
both completed cases, preserving the original ERROR results and frozen runners.
It reproduces the original checks, proves the exact known prefix and JVM arguments,
and changes only one copied configuration field for classification; every metric
is unchanged. Verified outcomes are baseline HEAP_OOM and patched PASS. Patched
fill is 181.988 seconds, warmup 60.319 seconds, and measured reads are 45.764,
47.327 and 40.242 seconds (median 45.764). Peak observed heap is 38.696 GiB versus
45.248 GiB for the failed baseline. Actual patched layout is 12,400 records,
7,428,469–8,323,781 bytes each, while retaining the 1 GiB GPU target.

The [first production results](pcbs-rowgroup-production-results.md) are READY FOR
REVIEW; an independent agent rechecked raw phase numbers, GC/sample peaks and all
four sixteen-column sum/row oracles. Root also generated the explicit audited
comparison canonical-first-pair001.json. This is one fresh JVM per arm, not a
statistical speed estimate or minimum-heap result. Baseline has no successful
read median; patched fill was about 11% slower in this pair. Remaining default
DISK_ONLY baseline/patched cases completed serially in remaining-default002,
both strict-sidecar PASS and all four row/sum oracles exact. Baseline fill/read
median/observed heap: 186.661 s / 53.764 s / 44.286 GiB. Patched: 164.026 s /
70.240 s / 18.054 GiB. Root generated canonical-first-four001.json from the four
explicit sidecars; an independent agent reparsed raw scalars, GC peaks and all
read oracles. The observed 30.65% disk-read slowdown remains unresolved: patched
reads physically fetched about 99 GB each, while baseline's last two fetched
164 KB and zero. Global patched GC was lower; no causal attribution or universal
speed claim. Do not assume excluded warmup means a fully warm filesystem cache.
A read-only code/native trace is checking logical-file range segmentation; no
production change or artifact replacement is authorized by that investigation.
Fresh four-lane cross-version SQL queue sql-queue540-002 completed successfully:
330 Java/Kryo and 401 Java/Kryo each passed all nine cases, no failures/errors/skips,
actual 4 GiB application heap and correct serializer/registrator/runtime verified.
Root independently reparsed raw JUnit, observed JVM main/heap and event environments:
../support-audit/root-cross-sql-recheck001.json. These 36 tests are READY FOR REVIEW.
The initial audit-negative Java001 remains unchanged. All owned JVM/GPU processes
exited before root scheduled ../world-queue/matrix001 (16 fresh production public-data
cases, unchanged frozen controls/runtime; currently running). Repeats and decoded
layout remain pending. Evidence is
under ../production-runs and ../support-audit/first-pair-*-audit.json. All workers
remain MCP docker_exec only; benchmark jobs own the sole JVM/GPU/build slot.

Public-data checkpoint: the first nine production cases completed without
unexpected errors: eight PASS and GHArchive 8 GiB MEMORY_AND_DISK HEAP_OOM on the
baseline. Patched GHArchive 8 GiB mixed storage passes (fill 47.466 s, read median
16.406 s, observed heap 6.425 GiB), while baseline fill 50.296 s precedes the
ObjectInputStream.readArray failure. TLC 8 GiB mixed storage passes both arms:
baseline/patched fill 14.370/14.661 s, read median 3.888/5.518 s, observed heap
7.308/5.740 GiB; patched encoded payload is about 9% larger. These are tradeoffs,
not universal speed/heap wins. Root generated explicit partial reports; an
independent agent rederived all first-four row/hash/null/value/schema oracles,
heap/GC peaks, layout and timings from raw evidence (world-first-four-raw002.json).
The initial raw001 also passed; 002 extends only missing-data handling for later
negative outcomes and keeps all first-four checks unchanged. World matrix001 is
still running; final verifier reexecution and complete report remain pending.

A separately owned OS sampler now records future host/cgroup cache/pressure
counters every two seconds, without changing the frozen benchmark. PID/start
identity was independently matched and actual sample availability/units checked.
History starts at 1792807172708375464 epoch ns, after the first world's phases;
it cannot explain past canonical cache misses. Native-RSS inflation is not supported
by that disk pair: patched read RSS was 22.4–23.1 GiB versus baseline 50.1–51.7 GiB.
Report adapter review raised a false formatting alarm from nested tool escaping;
actual file bytes and JSON/CSV/Markdown round-trip checks disproved it. The reporter
implementation never changed; six checks now include real output parsing.

Layout v2 fixture preflight is READY FOR REVIEW (21 root/independent Python checks):
original compiled workload/controls stay unchanged; the sole known canonical Java
options metadata issue uses the approved strict correction and all original base
and layout checks remain, with exact artifact/task linkage added. Actual decoded
layout runs remain pending. The optional owned Nsight adapter is also prepared
and reviewed (nine root/independent checks), not executed. Any captures require
further schema/phase/completeness review and supply no primary timing comparison.
All work continues through MCP docker_exec only; no production source changed.

The next eleven canonical repeat/control JVMs are PREPARED, not run:
../canonical-repeat-queue. Root read the full launcher and reran its eight tiny
Python checks; an independent agent found no material issue. Default orders are
counterbalanced across repeats 2/3; baseline reset1/unroll1 has three separately
labelled cases. All use the identical 96 GiB data, runtime, query and other controls.
Each case requires 208 GiB free disk; insufficient space parks rather than deletes
owned scratch or raw evidence. No cache flush, forced GC or memory-fraction change.

Read-only native tracing found one HMB view per column chunk in logicalFile; the
matching cuDF source coalesces logical reads but submits a transfer per input HMB
boundary. Adjacent views in one parent could merge without payload copies. This
is an unmeasured optimization hypothesis, not the established cause of the disk
read difference. Root read the decisive HMB transfer loop and production assembly.
The optional profiling-plan contains separate Nsight command plans only: no profile
or optimization has run, and any future artifact/profile is a distinct cohort.
All workers remain MCP docker_exec only, including checkpoint confirmations.

Root created six dedicated detached worktrees at full commit
1966a46404b49cbd6d122b807bdf9a7e92c411b6 for the four additional boundary checks
and two grouped OSS builds. This commit has the identical production main code.
The build launchers are prepared and root's tiny boundary/grouped checks pass;
no new Maven/dependency/runtime build is credited until actually scheduled.

Existing build/buildall can compile/package the 39 OSS Scala/build pairs, grouped
by Scala line, then assemble each multi-shim distribution. It hardcodes skipped
tests and cannot replace runtime/boundary gates. It also permits packaging fallback
to Maven artifacts: acceptance must prove every selected target was built from this
patch, not merely present in a packaged jar. Three DBR lanes require actual vendor
jar bundles through the existing install_deps.py workflow; no such bundles or DBR
runtime are available here. No vendor-resolution failure or universal readiness
is inferred from cache absence. All operations and worker handoffs remain MCP
docker_exec only.

## Completed production public-data checkpoint

All sixteen production public-data cases are READY FOR REVIEW: fifteen PASS and
one genuine heap OOM in existing PCBS at GHArchive / 8 GiB / MEMORY_AND_DISK.
The patched case passed. Every successful case has four exact read oracles and
zero failed tasks. Root independently checked the final table against raw phase,
GC, partition and oracle markers, then reran every unchanged original verifier
with exact saved-result equality. All frozen artifacts, 32 input files and 192
raw-evidence files passed content hashes and an independent final rehash.
Evidence: ../world-queue/{complete001.*,reverified001/result.json} and
../support-audit/world-complete-raw001.json. No production source changed.

The measurements expose real tradeoffs: TLC reads are 20–42% slower and payloads
about 8.9% larger; GHArchive 48 GiB mixed storage has 18% slower reads and 9%
higher observed heap. Small-heap disk cases reduce heap substantially, and the
patch avoids the observed mixed-storage OOM. These are one-JVM pairs, not
universal performance or minimum-heap claims. The complete results and units are
in pcbs-rowgroup-production-results.md. Independent interpretation review agrees.

Both the fixed world queue and its read-only OS sampler ended; root verified no
remaining JVM or GPU process before the next admission. The reviewed eleven-run
canonical repeat queue is now active at ../canonical-repeat-queue/repeats001:
eight additional default-control JVMs and three explicitly separate current
baseline reset1/unroll1 JVMs. Parent PID509641/startticks425053211 and bounded
sampler PID509661 were recorded. No repeat outcome is claimed at this checkpoint.
All other controls remain fixed, including memory fraction0.6. No cache flushing,
forced GC, automatic scratch deletion or overlapping builds/profiles.

Decoded-layout diagnostics, four boundary full verifications, grouped OSS
compilation and real Databricks coverage remain pending. Root created detached
boundary/grouped worktrees at 1966a46404b49cbd6d122b807bdf9a7e92c411b6; production
main code there equals c385525a51. Preparation is not a successful build.
Logical-input segment coalescing is a performance hypothesis only; no optimization
or artifact change has been made during the matched series. CPU-producer work
and heavy downstream GPU workloads remain deferred. All project and worker
operations continue through MCP docker_exec only.

## Future-build preflight correction

The future boundary, grouped OSS and vendor owners originally rejected every
nonempty MAVEN_CONFIG. Root's value-free preflight showed that this image uses
exactly '-s' plus an existing absolute settings-file path, with no MAVEN_ARGS.
Rejecting that existing resolver selection would block builds before Maven ran.
The shared ../crossversion/maven_environment.py now permits only empty config or
that exact two-token settings selection, preserving the environment unchanged.
All other arguments, extra flags, malformed values and nonfiles remain rejected.
No settings contents were read or hashed, and no Maven/JVM/network operation ran.

The three future owners freeze the validator and retain value-free classification.
Root read the changed code and independently passed four environment tests,
four grouped-build tests, six vendor-owner tests and the boundary exact52+9
acceptance checks. Independent review passed. This changes unreleased orchestration
only; selected tests, source identity, fresh-class and ownership gates are intact.
Completed/frozen build and benchmark helpers were not modified.

Exact three-vendor normal-resolver compile attempts are prepared at
../crossversion/run_vendor_compile_v1.py; actual attempts and runtime coverage
remain NOT RUN. Missing cached artifacts are not evidence of failed resolution.
Exit zero alone will be REVIEW_PENDING until root checks freshly compiled helpers
and real vendor dependency origins. No OSS substitution is allowed. Actual DBR
SQL validation still requires its matching runtime or authorized CI lane.
The local PR-description draft is ../support-audit/pr-description-draft001.md;
nothing was posted. The eleven-run benchmark queue remains the sole active
JVM/GPU workload, with no change to its controls or implementation.

## Weekend profiling and compression extension

The user authorized continuing to completion, profiling why cases get faster or
slower, and comparing Spark's general-purpose cache compression off/on and
different codecs. The active eleven-run series remains unchanged. No new matrix,
profile or writer-control artifact has run at this checkpoint.

The encoder difference is now explicit in the results report: existing PCBS
uses the cuDF Java defaults of 128 MiB uncompressed row-group data / 1,000,000
rows; the patch passes 8 MiB uncompressed / 65,536 rows. Native C++ defaults are
unlimited row-group bytes / 1,000,000 rows, overridden by JNI's Java arguments.
Root independently inspected the exact frozen Builder constructor without
starting a JVM, and read the native defaults/JNI call sites. Evidence:
../support-audit/java-writer-defaults-root001.json. The fixed row cap is independent
of column count; cuDF separately checks uncompressed bytes across columns when
choosing group boundaries. Neither value is a compressed cache-file-size target.

Planned experiments, with every outcome retained:
- Complete the current repeat queue and actual decoded-layout diagnostics first.
- After the new byte-target screen below selects a candidate, prepare an isolated
  baseline-plus-writer-settings-only artifact: only the byte setter changes, with
  the default row cap retained. The original large-array consumer and reader stay.
  Compare original baseline, this control and selected patch on canonical and
  TLC DISK_ONLY / 48 GiB (six fresh JVMs). This separates encoder-layout effects
  from the combined splitting/staging/reader changes, not each latter cost alone.
- Defer Spark stream compression until selecting the byte target, then screen
  fresh explicit OFF, LZ4, ZSTD, SNAPPY and LZF profiles on the original baseline,
  the explicitly selected patch artifact and both public datasets:
  twenty DISK_ONLY / 48 GiB JVMs. Parquet SNAPPY, data, sixteen partitions,
  eight slots, GPU/host/heap/GC/retention settings and query stay fixed.
- Choose a bounded canonical or mixed-storage confirmation from those results;
  do not multiply every dimension. Keep profiler runs outside primary timings.
- Profile actual transfers, kernels, CPU/metadata/allocation costs and physical IO
  before attributing speedups or regressions. A later diagnostic may snapshot
  cache-file residency with installed fincore, without reading payload, flushing
  or changing permissions; availability/accuracy still needs validation.

Detailed NOT RUN scope, source evidence, codec/global-shuffle caveat and reporting
fields: ../support-audit/compression-matrix-plan001.md. Preserve all fill/warmup/read
values, observed heap/RSS, per-phase IO, exact Parquet payload and captured complete
Java block-file sizes in CSV/JSON/Markdown. CPU historical results remain separate.
The agreed production architecture stays fixed. Spark-core changes, Spark
OFF_HEAP, new public config, synthetic data reduction and heavy downstream GPU
expansion remain excluded. Existing released behavior stays protected; research wrappers
and the writer-only control are unreleased and need no compatibility adapter.
All future operations, handoffs and checkpoints continue via MCP docker_exec.

## Byte-only targets: new candidate IN PROGRESS

The user explicitly requested dropping the added 65,536-row override, retaining
cuDF's default 1,000,000-row cap and comparing 8 / 16 / 32 MiB uncompressed targets.
The old cap reaches 8 MiB at 128 value-bytes per row and underfills narrow groups;
our sixteen-double canonical schema largely hides that constraint. No byte-size
choice is final before the measurements and architect review.

The isolated ../byte-target-worktree now commits the next 8 MiB candidate at
d9ba8787f609656395cdb3f127dd47325965c167 (based on 572fe681f3423465e6366ae1e19fe68400168ef4).
It remains unbuilt and has not replaced the frozen benchmark artifacts. Production changes are exactly two
deletions: TargetRowGroupRows and its writer-builder call. A new GPU test encodes
200,000 int rows through the production factory, requires one group over 65,536
rows, checks every decoded value and host-handle cleanup. Fixed deterministic
128-byte binary values keep the existing integration fixture genuinely above
the byte target; its exact-data and multi-record assertions remain intact.
Performance inputs and current benchmark artifacts were not changed.

Root read the diff and cuDF factory signature; independent source review passed.
Python syntax and diff checks pass. Scala compilation, GPU execution and SQL
validation are NOT RUN for this candidate. Its new selected suite count is
53 Scala (9/4/10/5/8/17) plus nine Java tests. Previous 52-test evidence describes
the frozen c385 artifact only and must not be relabelled. Tests for a finally
selected larger target also need actual validation; the fixed 128-byte fixture
does not automatically prove multiple groups at 32 MiB.

Bounded first screen: four explicitly distinct compiled artifacts (frozen
8 MiB/65,536 control and new 8/16/32 MiB/default-row-cap variants), full canonical
96 GiB DISK_ONLY and MEMORY_AND_DISK at 48 GiB heap, plus TLC DISK_ONLY at 48 GiB:
twelve fresh JVMs. Keep data/partitions/query/GPU/heap/GC/retention controls fixed.
A 32 MiB heap OOM is a valid negative, not a reason to reduce data. Then select
a small-heap GHArchive confirmation from the evidence rather than multiplying
every dimension. No new screen has run. Scope/identities/reporting:
../support-audit/byte-target-sweep-plan001.md; source review and build approach:
../support-audit/byte-target-review-build-plan001.md.

The 16 and 32 MiB experimental sources are now committed, clean and independently
reviewed: cebb9206d5c3b0cbd13a6e6e6069cf611264d6b3 in
../byte-target16-worktree and abec8b6da5578207e8118793f8f2eee5cc21c9ef in
../byte-target32-worktree. Each complete tree differs from byte8 only in the
private byte constant. No GPU target, input, test, POM or public config changes.
They remain unbuilt and unmeasured; preparation is READY FOR REVIEW, candidate
acceptance IN PROGRESS. Root created them during the diagnostic-only layout run;
no primary performance timings are being collected concurrently.

All supported-version acceptance must be rerun against the eventual selected
source, not just the earlier c385 implementation. Existing boundary/grouped/vendor
worktrees and fixed-commit plans remain preparations; update them explicitly to
the final source and new test count before execution. The eleven-run c385 series
is complete; its files/controls stay frozen. Root continues sole JVM/GPU/build
scheduling; workers continue MCP docker_exec only.

## Repeat verification and next-build preparation checkpoint

The eleven-repeat queue completed with nine PASS and two genuine heap OOMs.
Together with the first four runs, all fifteen JVMs are independently verified:
12 PASS and three HEAP_OOM, all default baseline MEMORY_AND_DISK. All six patched-
default JVMs (three per storage level) and all three baseline 1/1 JVMs pass. Root replayed all original
verifiers and the approved correction with exact result/metric equality, checked
16 actual inputs and 180 raw evidence files, and verified 106,043,786,691 bytes
both initially and at final rehash (140.391 seconds). Queue, sampler, Java and GPU
processes are quiescent. This comparison is READY FOR REVIEW.

The generated table includes all fifteen JVMs, three-per-arm medians/ranges,
fill/warmup/read times, observed heap/RSS, actual stored layout and negative
outcomes. MEMORY_AND_DISK patch/default fill/read medians are 157.327/41.013s;
baseline 1/1 is 147.397/38.943s. Patched heap peaks are 38.106–38.696 GiB versus
about 45.2 GiB for 1/1, with slightly higher patched RSS. Disk ordering varies
with physical IO; the initial slowdown is not a universal result. Full results:
../canonical-repeat-queue/report001/summary.{csv,json,md}, copied into the
production results document. Replay: ../canonical-repeat-queue/reverified001;
root final checks: ../support-audit/root-repeat-final-checks001.json.

Root also reran the completed-case GPU metric extraction and the OS analysis
after sampler closure; all eight metric cases and fifteen OS phase observations
exactly match the worker outputs. The closed sampler hash is
5b7a90126e6998d311c4ff1c9538ca1aa7f280cfbd75e4c857a9c62fac01b18c.
Source confirms gpuTime measures semaphore-held host elapsed time; staging/IO
can be inside it. Missing retry/spill updates remain unknown. OS measurements
show file refaults/cache turnover but do not identify per-file residency or
reconstruct the first pair's missing OS history. No causal GPU profile is claimed.

The idle-only canonical15 replay helper at
../canonical-repeat-queue/reverify_repeats_v1.py is prepared and source-reviewed.
Root and an independent worker each reran six tiny tests successfully. It retains
raw prefix ERROR/OOM evidence, requires exact original and approved normalized
results, checks every declared input/artifact content and rehashes unique files
at completion. Root has now run it against all fifteen real cases successfully; the queue
has ended and exclusive admission is available for the next diagnostic. The repeat report's eight tiny tests also pass.

The next three-artifact build owner at ../crossversion/build_byte_targets_v1.py
and its starter/freezer are prepared and source-reviewed by root and an
independent worker. Seven new tiny tests and the old boundary checks pass. The
candidate owner requires exact 53 Scala tests (9/4/10/5/8/17) plus nine Java tests,
clean actual source commits and fresh module/distribution class equality. The
existing report parser defaults remain 52 for old preparations; neither count
can masquerade as the other. New manifests must preserve the actual common
JNI/private artifacts and original baseline. No actual candidate Maven build,
artifact freezing, runtime gate or new benchmark has run. See
../crossversion/BYTE-TARGET-BUILD-PREP.md for exact prerequisites and limitations.

Root reread the decisive Spark Java stream-reset and MemoryStore admission loops
and recorded a source-based memory model in
../support-audit/byte-target-memory-model001.md. Independent review checked the
arithmetic and required clarifying Parquet versus outer Spark stream compression;
that clarification is applied. The record-retention and admission scales overlap
and are not additive heap bounds. Actual encoded sizes and observed heap still
need the new screen; these formulas predict neither universal success nor OOM.

The twelve-case screen owner/starter at ../byte-target-screen is prepared and
reviewed by root and an independent worker; each reran seven tiny tests PASS.
It admits the frozen old control and three actual completed 53+9 candidate builds,
preserves original runners/oracles and the strict option correction, and retains
one-JVM screening and OOM limitations. Actual artifact admission and runtime have
NOT RUN. The existing decoded-layout pair's earlier independent five-test review
is reconciled in ../layout-queue/README.md; only stale pending wording changed.
Root launched it after the successful repeat replay at ../layout-queue/pair001,
supervisor PID525296. It is the sole active JVM/GPU/build-slot owner, beginning
with baseline and then frozen c385. Its times are excluded from performance
comparisons; actual layout verification is IN PROGRESS. Workers remain MCP-only
and cannot launch follow-on jobs; root independently verifies its acceptance.

A minimal future world-compression adapter at ../compression-profiles is also
source-reviewed by root and an independent worker; both reran six tiny tests PASS.
It changes only spark.rdd.compress and an explicit spark.io.compression.codec
in the unchanged runner's complete launch evidence. Parquet SNAPPY and all other
controls remain fixed; the unchanged verifier checks both actual runtime values.
No selected artifact, twenty-case owner, actual integration or codec availability
is claimed. Execution remains deferred until byte-target selection. Retained
review: ../compression-profiles/independent-review001.md.

All operations and worker checkpoints continue via MCP docker_exec. Root owns
scheduling and actual acceptance checks. Preparations are READY FOR REVIEW;
production and measurement acceptance remain IN PROGRESS.

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

1. Retain large GPU input batches and existing column-limit slicing. The next
   candidate uses only an 8 MiB uncompressed row-group target, retaining cuDF's
   default row limit; compare 16 and 32 MiB before selecting the final byte target.
   Frozen c385 measurements retain their earlier 65,536-row override. These are
   soft writer targets, not hard encoded-record or heap limits.
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
