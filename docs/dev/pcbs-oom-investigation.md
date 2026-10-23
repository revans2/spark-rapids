# PCBS heap OOM investigation (#16362)

Status: READY FOR REVIEW — bounded BlockManager codec/native lifetime/recovery
research, scheduler-token research, newer-version audit and upstream proposal verified. Prior reproduction/prototype
results remain READY FOR REVIEW. Production implementation and architect
acceptance are pending.

## Controlled serialization and record-size experiments (2026-10-23)

Status: IN PROGRESS. Architect approved the first two benchmark questions.
Substantial downstream GPU workloads are explicitly DEFERRED; retain the existing
all-column sum/count consumer. This supersedes the earlier no-new-runs restriction
for these bounded experiments only. No memory-fraction tuning, native storage,
production code/default changes, public configuration or external posting.

Controls: immutable canonical 96 GiB values, 16 partitions, 6 GiB/partition,
local[8], 48 GiB configured heap, same JVM/ParallelGC four workers, global GPU
target 1 GiB, fixed GPU pool 15 GiB, GPU concurrency two, host limit 40 GiB,
pinned pool 2 GiB. Same source hashes/page-cache preparation and observer code.
No forced GC between reads. Report actual usable heap, record/device sizes and
MEMORY_AND_DISK residency; keep DISK_ONLY experiments separate.

- Serialization gate: record target 1 GiB, downstream target 1 GiB; Java and
  custom codec each with reset/unroll 100/16 and 1/1. DISK_ONLY first, then
  MEMORY_AND_DISK. Three fresh-JVM repetitions per setting in alternating order.
  Expected Java heap OOM is a measured negative result, not a correctness pass.
- Record-size matrix: private PCBS caps 64/128/256/512/1024 MiB, same global
  target/data/partitions. Java 1/1 versus codec defaults. Reuse matching gate
  cases only when the read schedule matches, three fresh JVMs per setting/storage.
  The concrete schedule uses 24 serialization-only JVMs plus 60 record/consumer
  JVMs (84 total); the latter repeat the 1 GiB reference to keep the first
  question's four-read schedule separate from the twelve-read consumer matrix.
  Hold downstream target 1 GiB
  for matched comparison. Then measure no coalescing, 256 MiB and 1 GiB output
  from the SAME populated cache, using fresh query plans and target-aligned
  warmup plus three reads. Do not silently substitute rebuilt caches.
- Primary results: fill/read wall time and separate maximum fill/read heap.
  Supporting evidence: cache residency/bytes serialized, producer/stored/decoded/
  combined batch distributions, allocation/GC, RSS/CPU/physical IO, GPU wait/spill.
  Heavy JFR/Nsight diagnostics separate from primary wall-time repetitions.
- Build/smoke correctness gates precede large runs. Preserve fresh output dirs,
  artifact/source/control hashes, raw logs, phase markers, result/verifier JSON.
  Exact rows/all-column oracle and blocking cleanup required. Park unexpected
  non-fixture failures; never weaken checks or tune controls to obtain passes.
- Existing Spark/PCBS/StorageLevel contracts are RELEASED (existing main and
  deployed behavior) and preserved. Research codec/overlays/fixtures are
  UNRELEASED; adapt directly without compatibility scaffolding.
- Root coordinates, reads decisive code and independently verifies worker changes.
  Separate workers own generated artifacts, launcher/verifier, and prerequisite
  audit. All project operations MUST use MCP docker_exec; require confirmation
  at every handoff/checkpoint. Commit completed tasks and tracker together,
  mark verified work READY FOR REVIEW, not DONE. GPU runs serialize under root
  coordination. Fixture preparation is READY FOR REVIEW: harness and all ten
  generated plugin variants compile; root independently rehashed all five dynamic
  variants, sources and runtime inputs. Ten fresh small gates pass both cache
  wrappers/settings/storage levels and consumer directions. Independent raw audit
  (also rerun by root) checked 120 exact all-column reads, no recomputation,
  coalescer placement/counts, cleanup and 2,360 successful tasks. Evidence under
  /data/pcbs-oom/controlled-cache-comparison/{root-artifact-audit.json,
  independent-raw-smoke-audit.json,smoke-r2-*/}. Smoke is 96 MiB/partition and
  cannot prove the 256 MiB versus 1 GiB coalescing ceilings; full runs must.
  Initial failed compile and verifier-error run remain preserved. Corrected
  verifier assumptions: actual JVM options include requested controls plus Spark
  defaults; BlockManager/MemoryStore remain stock, only SerializerManager comes
  from the codec overlay. Do not infer class origins without inspecting artifacts.
  Source reviews and whitespace checks passed. JFR/Nsight launch support is
  prepared separately; profiling runtime/tool behavior remains unverified.
  Long matrices use a detached supervisor with observable checkpoints, avoiding
  long MCP commands that prevent status reads on the same tool session.
  See tools/pcbs-real-data-comparison/controlled-experiments.md.
  Monitoring ownership: root remains responsible through completion and result
  verification; the user does not monitor or restart the runner. Root must not
  end with only a background launch and leave fixture failures unattended.
  Matrix1 PARKED on its first case: workload itself exited 0 with 100 successful
  tasks, exact 96 GiB/all-column oracle, fill 157.935 s/read median 55.367 s and
  observed peak 45.414 GiB. The only rejection was an unsupported 900 MiB minimum
  producer-batch assertion (actual maximum 895.753 MiB). Root and two independent
  code/raw audits confirm whole-batch packing has no such minimum. The corrected
  verifier enforces the exact canonical producer fingerprint (112 batches,
  805306368 rows, 103079215104 bytes, min806860032/max939265152), plus sixteen
  explicit 1 GiB producer-plan goals. All other controls remain. This is seven
  records/partition, not the historic six literal 1 GiB records; do not conflate.
  Original error output is preserved; separate recheck passes. Fresh matrix2
  will restart the complete schedule with this verified fixture correction.
  Focused peer review and independent raw audit passed; root reran the audit and
  seven negative mutations proving changed producer fingerprints/targets fail.
  Evidence: producer-contract-independent-negative-review.json; original result
  remains ERROR, separate producer-contract-recheck.json passes.
  Current stage: fixture correction READY FOR REVIEW; matrix2 restart next.
  Previous launch: Supervisor PID 302093;
  /data/pcbs-oom/controlled-cache-comparison/matrix1/status.json reports current
  case and completed/84 count; plan.json freezes controls/hashes; results.csv
  and results.json populate after completed cases. Launched from
  /data/pcbs-oom/controlled-cache-comparison/launch_matrix.py with fixture commit
  f5dd974700. First case: 1 GiB records, Java 100/16, DISK_ONLY. Large results,
  selected diagnostic profiles and independent large-result review remain pending.
  Unexpected failures park the supervisor; expected heap OOM remains a negative
  measurement. Do not edit frozen fixture sources while this matrix is running.

## Consolidated performance inventory (2026-10-23)

READY FOR REVIEW — historical data presentation only. Architect requested a
single broad table including fill times, 256 MiB cases, other record sizes,
failures and missing experiments. No new runs or batch-size decision authorized
or performed. The latest 32 MiB matrix is one measured case, not a selected
policy or a substitute for the earlier 256 MiB work.

[Consolidated table](pcbs-experiment-results.md) and
[full-precision CSV](pcbs-experiment-results.csv) contain 75 rows: 74 historical
settings/groups plus the explicitly unmeasured current real-PCBS codec 256 MiB
comparison. Real 256 MiB results exist in the small reset sweep; synthetic
256 MiB results exist at both 24 and 96 GiB totals. The large old global-target
128/256 MiB cases were planned but never executed. Those gaps remain blank;
no values are inferred from different workloads.

Root read the prior tracker/source evidence and independently checked delegated
historical extraction. A separate reviewer checked aggregation against CSV/JSON
inputs. Preserve fill completed before read OOM; distinguish SQL/raw/validation
reads, global target versus PCBS cap, data size, heap/slots/GPU concurrency,
storage/fraction, native shadow versus heap codec, and sampled peak scope.
Never substitute post-GC occupancy for maximum heap. Final presentation review
clarified that raw reloads still deserialize full cached records, without Parquet
decoding. CSV uses LF line endings so Git whitespace checks apply cleanly;
a failed whitespace check must stop a commit sequence. All operations use MCP
docker_exec; no benchmark, runtime source or released default changed.

## PCBS split/coalesce and CPU default-cache comparison (2026-10-23)

READY FOR REVIEW — fixtures, six full runs and independent raw audits passed.
Root ran CPU/split/split-coalesce; the worker repeated unchanged commands in
reverse order. Same exact 96 GiB values, 16 partitions, eight task slots,
48 GiB configured heap; no OOMs, failed tasks or timeouts. CPU fill range
99.30–114.32 s/read-median range 18.64–19.20 s; GPU split 110.20–138.37 /
26.84–28.20 s; GPU split/coalesce 113.52–138.55 / 24.96–26.96 s. Largest
observed heap (samples plus logged pre-GC occupancy) 42.73/42.01/42.10 GiB.
Coalescing consistently restored 112 batches/read, maximum 991.27 MiB,
from 3,184 small records; global GPU target stayed 1 GiB. Read-heap and fill
rankings reversed across JVMs, so no consistent heap/fill benefit is established.
Max heap and wall times are primary; no GC matching or tuning was performed.
Post-timing independent audits passed all six raw/hash/source/artifact/GC/event
checks; exact rows/sums, defaults, stock CPU classpath, stable cache residency
and cleanup verified. Evidence: /data/pcbs-oom/pcbs-real-data-comparison/
{root-full-audits.json,independently-verified-comparison.json}. No apps remain.
See [comparison evidence](pcbs-real-data-comparison.md). Architect requests PCBS-only
pre-splitting with the global 1 GiB target unchanged, optionally recombining
decoded records near 1 GiB, plus a pure CPU default-cache comparison at the
same input volume and task layout.

- Concrete first matrix: stock CPU/default cache, research-codec PCBS cap32 MiB,
  and the same cap32 MiB plus standard immediate post-cache GPU coalescing.
  Global target stays 1 GiB; no unsplit or additional-size sweep is scheduled.
  Use one immutable canonical set of 16 Parquet files: 805,306,368 rows of
  16 DOUBLE values, exactly 96 GiB logical values (6 GiB per cache partition).
  Generate on CPU once and verify shared file hashes and aggregate oracle.
  Fill all 16 cache partitions in parallel on local[8], then one warmup and
  three all-column reads. MEMORY_AND_DISK, ParallelGC, 48 GiB configured heap;
  report usable heap separately. Actual cache bytes are format-dependent.
  GPU uses the research per-cache codec, fixed 15 GiB pool, concurrency 2,
  host limit 40 GiB and pinned pool 2 GiB; CPU has no RAPIDS or overlay jars.
  Prior split-fill timings were sequential and are not directly comparable.
  Architect clarification: primary outcomes are maximum heap usage and
  fill/read wall times. GC is supporting evidence, not a target to match.
  Keep experimental controls fixed; do not tune to match CPU GC behavior.
- Reuse the prior real PCBS split experiment and standard GPU coalescing code.
  Keep baseline, split-only and split-plus-coalesce distinguishable. Measure
  actual encoded record sizes, decoded and downstream GPU batch sizes,
  heap/GC, cache fill and full-data read times. No global target reduction.
- CPU means Spark DefaultCachedBatchSerializer with default Java serialization,
  RAPIDS disabled, and normal CPU cache batching. The preceding synthetic codec
  fixture padded 384 SQL Long rows to 96 GiB inside its custom serializer;
  feeding those tiny rows into the CPU default cache would be invalid.
  Use actual SQL data, shared exactly between CPU/GPU where feasible.
- Preserve the same total logical data, 16 cache partitions, 6 GiB logical
  data per partition if exact 96 GiB input is feasible, local[8], 48 GiB heap.
  Confirm the real workload's storage level and source layout before running;
  do not silently compare MEMORY_AND_DISK with DISK_ONLY or change partitions.
  CPU/GPU compression and encoded sizes may differ and must be measured.
- GPU target/pool remain 1 GiB / fixed 15 GiB; reuse bounded RAPIDS host spill
  limits. Do not tune memory fraction, unroll period, stream reset or public
  configuration to obtain a pass. Expected heap OOM remains a negative result.
- Released PCBS/Spark contracts and defaults remain unchanged. Historical
  split overlays and new fixture work are UNRELEASED; adapt directly without
  compatibility scaffolding. No production default or production Spark/plugin
  change, native-cache backend, Spark OFF_HEAP pool or external posting is
  authorized. The existing research Spark codec overlay is used for GPU cases.
- Independent workers discover split/coalesce, CPU comparability and environment
  prerequisites. Root reads decisive code, settles a concrete bounded run plan,
  independently verifies delegated results and commits each item with tracker.
  Every project operation MUST use MCP docker_exec; require worker confirmation
  at each handoff/checkpoint. Park material design conflicts or non-fixture
  failures requiring production changes; never weaken an acceptance check.
  Deliver verified research READY FOR REVIEW, not architect-accepted DONE.

## Constant-data comparison and cleanup follow-up (2026-10-23)

READY FOR REVIEW. Architect direction: keep total payload and partition count fixed
when comparing record sizes; the previous A sweep changed total data from
24 to 96 GiB and therefore changed memory pressure. Preserve those observations
as historical evidence, not an equal-data record-size comparison.

- Constant-data benchmark: READY FOR REVIEW. Worker and root independently
  ran four fresh JVM cases each in opposite order. Four codec cases completed
  exactly; four Java cases OOMed during producer allocation, with zero blocks
  published. All eight raw/hash/event/GC audits passed, runtime sources match
  and no timeouts occurred. Root codec 256 MiB/1 GiB: fill 73.28/77.94 s,
  median read 18.14/24.34 s, GC across three reads 3,500/14,250 ms; sampled
  occupancy peak 28.13/45.44 GiB. Equal logical data and payload content verified.
  Native remains optional; this does not prove a PCBS MEMORY_AND_DISK fix.
  Evidence: /data/pcbs-oom/blockmanager-codec-confidence/constant-data/.
  Full paired data: confidence/constant_data_benefit/independently-verified-results.json.
  Controls: 96 GiB total, 16 partitions, 6 GiB per task,
  local[8], 48 GiB configured heap, ParallelGC, DISK_ONLY; 256 MiB records
  mean 24 per partition, 1 GiB records mean six. Compare Java and per-cache
  codec in separate fresh JVMs, serially, with one warmup and three reads.
  Memory fraction, unroll period and object-stream reset remain defaults.
  Record actual bytes, correctness, fill/read/heap/GC and negative OOM outcomes;
  root repeats independently in reverse order. No shuffle repartitioning.
- Cleanup: READY FOR REVIEW. Worker and independent root fresh cases passed;
  four raw/source/artifact/event audits verified 243 runtime jars. Partial reads
  close at task completion, errors close immediately, EOF/listener/caller share
  exactly one successful underlying close. Exact 1,000 rows/complete blocks
  and ordinary retry preserved. Explicit taskless early close/error cases pass.
  Original F counterexample remains unchanged. No close-throw, cancellation or
  abandoned taskless BlockManager iterator claim. Evidence:
  /data/pcbs-oom/blockmanager-codec-confidence/F-cleanup/{worker1,root1}/.
  MCP-only checkpoints and independent peer review passed. The
  architect selected the existing task-completion notification
  pattern for task-bound decoder streams. Prototype idempotent cleanup,
  early termination and decode-error handling in a separate F follow-up.
  Check exact later data, complete blocks, normal EOF and no double close.
  Reads outside TaskContext need explicit ownership and a separate bounded
  check; do not assume task listeners cover them. Preserve original F evidence.
- Release status: existing Spark behavior is RELEASED and unchanged by default.
  Research overlays/fixtures/proposal are UNRELEASED; no compatibility adapter.
  Native storage remains optional; no production changes, public config,
  external posting or use of Spark's separate OFF_HEAP pool.
- Delegation: independent benchmark and cleanup workers; root owns decisive
  reads, independent reruns, tracker and signed item commits. All operations
  MUST use MCP docker_exec tools; require confirmation at every checkpoint.
  Park non-fixture failures needing production changes; never weaken checks.
  Expected benchmark heap OOM is a measured negative outcome, not a pass of
  cache correctness. Completed work is READY FOR REVIEW, not architect DONE.

## Confidence handoff (2026-10-22)

Research results READY FOR REVIEW. Original F records the independently
reproduced cleanup gap; its task-bound follow-up passed on 2026-10-23 above.
B runtime remains blocked by missing exact dependencies.
No production implementation or architect acceptance is claimed.

Contradiction check before edits: the prior proposal and tracker
sometimes implied native integration was a required next stage and excluded
charging native size against Spark accounting. The architect now explicitly
makes native payload storage OPTIONAL (nice-to-have), independent of the codec
goal. Accounting choices remain open, not selected. Existing allocation checks
prove rejection of a single over-limit request, not pressure admission. Correct
those claims; the proposal remains READY FOR REVIEW, not accepted.

Release classification unchanged: existing Spark cache, serializer and
StorageLevel behavior is RELEASED and unchanged by default. The overlay,
ClassTag carrier, fixtures and proposal are UNRELEASED; change them directly
without compatibility code. More native implementation, production Spark/plugin
changes, public config and external posting are out of scope. Measuring the
existing native fixture's recorded size is explicitly authorized documentation
evidence only. Native research must not block any codec work.

Order/acceptance: document fixes first; confidence E (Kryo/compression/encryption),
C (4.0.1 recovery/migration), D (clean replica receiver); then B (upstream target),
F (partial consumption/decode failure), G (distributed blocking SQL unpersist);
A (controlled heap/GC/fill/read comparison) last. Every case needs a fresh output,
result/verifier JSON and tracker entry. E/C/D delegated independently; root owns
docs/tracker, decisive reads, independent verification and signed task commits.
All project operations remain MCP docker_exec-only; require confirmation at
every worker handoff/checkpoint. Park/report any non-fixture failure or proposed
production change; never weaken acceptance. Other independent work may proceed.

- Document fixes: READY FOR REVIEW. Native optional/independent, borrowed-reader
  lifetime gap, unselected accounting choices and precise buffered-IO claims
  recorded. Existing fixture size-only run measured 197,480 Spark memory bytes for 32,768 payload bytes
  before and after RAPIDS spill, still useMemory=true, diskBytes=0. Source/runner
  untouched except accounting markers; run has normalend/no taskfailures.
  Evidence: /data/pcbs-oom/blockmanager-codec-confidence/docs-native-size/.
  Focused independent review passed; no new native feature or pressure claim.
- E: READY FOR REVIEW. Spark 3.5.5; 3 independent settings; 44 successful tasks per case; all primary checks; actual LZ4/crypto paths. Combined settings and performance were not tested.
  Worker and independent root fresh runs passed; root read decisive code,
  rehashed inputs and reparsed raw logs/events; separate peer review passed.
  Evidence: /data/pcbs-oom/blockmanager-codec-confidence/E/root-matrix1/. MCP-only checkpoint confirmed.
- C: READY FOR REVIEW. Spark 4.0.1; 13 successes and 1 genuine FetchFailed; exact 10,000 rows; 2 removal gates; BlockManager migration; 287 hashes.
  Worker and independent root fresh runs passed; root read decisive code,
  rehashed inputs and reparsed raw logs/events; separate peer review passed.
  Evidence: /data/pcbs-oom/blockmanager-codec-confidence/C/root401/. MCP-only checkpoint confirmed.
- D: READY FOR REVIEW. Spark 3.5.5; 3 fresh JVMs; 1 cache partition; producer 1, receiver 0; receiver decoded without a task and never produced/encoded; exact 1,000 rows/2,000 values; source computed once; 248 hashes.
  Worker and independent root fresh runs passed; root read decisive code,
  rehashed inputs and reparsed raw logs/events; separate peer review passed.
  Evidence: /data/pcbs-oom/blockmanager-codec-confidence/D/root-verification/. MCP-only checkpoint confirmed.
- B: READY FOR REVIEW. Exact Spark 4.2.0 revision 32f7299601108917fb01920a54e084595b7b3bf8 compiled independently; 188 hashes; partition-keyed statistics code preserved. Runtime PARKED: exact RoaringBitmap 1.6.10 and flatbuffers-java 25.2.10 unavailable; no substitutes.
  Worker and independent root runs agree; decisive code and raw evidence
  inspected, inputs rehashed; focused peer review passed. Evidence:
  /data/pcbs-oom/blockmanager-codec-confidence/B/root420/. MCP docker_exec-only checkpoint confirmed.
- F: PARKED. PARKED: both first-action take(1) and once-injected decode failure returned exact later data and complete cache blocks, but each had one read stream without a successful close notification after completed actions. Snapshot precedes shutdown; no permanent leak is claimed. Decide codec versus integration cleanup ownership before a remedy.
  Worker and independent root runs agree; decisive code and raw evidence
  inspected, inputs rehashed; focused peer review passed. Evidence:
  /data/pcbs-oom/blockmanager-codec-confidence/F/root-verification/. MCP docker_exec-only checkpoint confirmed.
- G: READY FOR REVIEW. Spark 3.5.5; actual driver SQL unpersist(blocking=true) waited for a paused executor reader, then both replicas were absent. Exact 1,000 rows; two successful tasks; 249 hashes.
  Worker and independent root runs agree; decisive code and raw evidence
  inspected, inputs rehashed; focused peer review passed. Evidence:
  /data/pcbs-oom/blockmanager-codec-confidence/G/root-verification/. MCP docker_exec-only checkpoint confirmed.
- A: READY FOR REVIEW. Worker and root ran four fresh JVM cases each, in
  opposite mode order; identical runtime sources and controls. Six cases
  completed exactly, two 1 GiB Java cases hit producer-allocation heap OOM
  before publishing any block. Eight raw/source/artifact/event/GC hash audits
  passed; no timeouts. Local[8], 48 GiB heap, six records per task, 16 partitions,
  DISK_ONLY; fraction/reset/unroll defaults unchanged. Root 256 MiB Java/codec:
  fill 15.52/14.27 s, median read 4.72/5.07 s, GC across three reads
  3,199/984 ms, sampled peak 42.75/22.74 GiB. Root 1 GiB codec: fill
  81.40 s, median read 25.06 s, sampled peak 44.46 GiB; Java fill failed.
  These are synthetic heap-payload measurements, not a PCBS MEMORY_AND_DISK
  fix or production GPU performance claim. Occupancy includes garbage.
  Evidence: /data/pcbs-oom/blockmanager-codec-confidence/A/*-final/.
  Full paired results: tools/pcbs-blockmanager-codec-spike/confidence/
  benefit_measurement/independently-verified-results.json. Peer review passed;
  MCP docker_exec-only confirmed throughout. Native remains optional.

Architect reports independent reproduction of 81347f8ae9 at
/data/pcbs-oom/blockmanager-codec-spike/architect-rerun355,
/data/pcbs-oom/blockmanager-codec-spike/architect-recovery355 and
/data/pcbs-oom/blockmanager-native-spike/architect-rerun. Supplied
architect-rechecked-results.json was inspected: it records valid root evidence
paths, so it is not by itself an audit of those differently named architect runs.
Keep that attribution separate. Raw architect primary/native result JSONs
were inspected and report valid runs; no fresh hash audit of those paths
is claimed here. Root confidence evidence is separately recorded.

## Optional native path: open design notes

Native payload storage is a nice-to-have (human decision, 2026-10-22), not a
codec requirement. No codec design, implementation or confidence gate depends
on native work. More native implementation is outside the current handoff.
If pursued later, payload allocation uses RAPIDS-managed spillable storage;
Spark's separately partitioned OFF_HEAP pool remains excluded.

- **Borrowed values:** deserialized entries return the cache-owned object.
  Iterator exhaustion releases the block read lock even when downstream code
  still holds the last batch; eviction/removal can then close its owner.
  Readers must take their own buffer reference while holding that lock.
  The fixture does so before release. No test yet uses a borrowed batch after its iterator releases
  the read lock during eviction/removal.
- **Accounting:** MemoryStore uses SizeEstimator over the reachable heap graph,
  not native payload bytes. The instrumented existing fixture recorded
  **197,480 Spark memory bytes for a 32,768-byte native payload**. After RAPIDS
  spilled it, Spark still reported 197,480 memory bytes, zero disk bytes and
  useMemory=true. Heap bookkeeping can exceed a small payload; this is not a
  native residency measurement. Open options are: charge an estimate of native
  size to Spark; charge wrapper/heap bookkeeping only and report native residency
  separately; or have Spark ask RAPIDS for residency. None is selected.
- **Allocation evidence:** the configured 64 MiB HostAlloc test rejects one
  individually over-limit request. It does not establish admission, aggregate
  residency or progress under memory pressure.
- **Heap IO:** payload IO is still heap-buffered. Whole-partition serialization
  for remote sends explicitly collects 4 MiB heap chunks. Direct BlockManager
  disk eviction instead calls buffered streaming IO; it does not allocate those
  4 MiB chunks through that path. Neither establishes heap-free transfers.

Measurement: /data/pcbs-oom/blockmanager-codec-confidence/docs-native-size/
accounting-measurement.json. This instruments the existing lifetime fixture
without introducing another native backend or pressure workload.

## Current architecture priorities (read first)

Short-term options must work with existing, UNMODIFIED Spark. Modifying Spark
is the LAST RESORT, not a short-term solution: the user estimates4–5years for
broad rollout. Include a Spark-change option when comparing approaches, clearly
label its dependency and deployment horizon; do not omit it if it is the only
known option, and do not promote it as satisfying the short-term constraint.

Work as coordinator: delegate bounded discovery and keep full traces in worker
notes; request compact findings/citations. Independently read decisive code and
verify integrated conclusions without repeatedly importing entire source files.
Preserve priorities here and in every relevant worker handoff after resets.
All project actions remain MCP docker_exec-only.

## Active BlockManager codec integration spike (2026-10-22)

READY FOR REVIEW for the bounded mechanisms documented below. Full native
backend implementation and architect acceptance remain pending. Architect
authorized the focused research spike, including
isolated Spark research source overlays. This does not authorize production
Spark/plugin changes, public configuration, PRs or external posting. The earlier
no-Spark-patch wording below describes the previous token spike only.

Scope: first prove per-cache codec selection inside the real BlockManager,
allowing heap payloads. Cover serialized unroll, disk and receiver-side codec
context, mixed caches/defaults, existing recovery and blocking removal. Native
payload storage is now explicitly optional and independent; Spark OFF_HEAP storage
is excluded because its separate pool cannot exchange capacity with RAPIDS.
Keep existing 1 GiB PCBS/large-test controls unchanged; no new batch sweep.

Release classification remains established: existing Spark cache/StorageLevel/
serializer contracts RELEASED and preserved by default. Research hooks and
fixtures UNRELEASED; no compatibility scaffolding for discarded experiments.

Coordination: codec worker owns disposable overlay/fixture and runner; separate
workers audit native ownership and transfer/version paths. Root owns tracker,
reads decisive choices, independently runs acceptance checks, and requests
focused peer review. All actions and every handoff/checkpoint use MCP
docker_exec tools only; workers must confirm that continuing requirement.
Park only for a genuine architectural conflict or missing prerequisite after
reasonable investigation; report incomplete criteria explicitly. Verified
research deliverables are READY FOR REVIEW, never architect-accepted DONE.

Source-audit checkpoint: [codec spike report](pcbs-blockmanager-codec-spike.md)
records completed independent transfer/native audits and root decisive-source
inspection. Existing ClassTag propagation supports a temporary research carrier
without a new directory or wire field. Actual Java metadata roundtrip and
multi-JVM paths still need runtime proof. Root read selector, SQL wrapper,
serialized unroll, block local-read/remove and RAPIDS factory/materialization.
Native cap caution: direct factory uses trackNoSpill; enabled RAPIDS total host
limits are required for the bounded native fixture. Full native residency/status
integration remains distinct from AutoCloseable lifetime proof. Both workers
confirm MCP-only at checkpoint; no production changes or Spark pool adoption.

Verified mechanism checkpoint: root independently compiled/ran the bounded
heap-codec fixture on Spark 3.5.5 and 4.0.1 (44 successful tasks each) and native
lifetime fixture on 3.5.5 (4 successful tasks). All used two executor JVMs and
one normal ApplicationEnd, with zero unexpected task failures. Separate root
evidence audit rechecked 249/282/250 source/runtime/artifact hashes. Reviewed
research code and [results](pcbs-blockmanager-codec-spike.md) are READY FOR REVIEW.
No production files changed; tiny Spark source overlays live only in disposable
research outputs. Optional native uses actual RAPIDS SpillableHostBuffer and no
Spark OFF_HEAP. Its configured 64 MiB HostAlloc check demonstrates rejection
of one over-limit request, not admission under pressure. Balanced native owners, payload stores empty and spill
files deleted; only framework bounce memory remains until explicit shutdown.

Scope qualifications remain explicit: no full native backend/accounting/status,
bounded large transfers, native failure/admission/cancellation coverage or
production PCBS/SQL native pipeline. Primary blocking-reader test is local
removeRdd. Replica reads have no active task context but executors also produced
other partitions. Full current/default compatibility is a required contract,
not inferred solely from the small fixture. Source-reviewed newer Spark than
4.0.1 is not runtime tested. Separate genuine recovery/decommission extension
has now passed independent root runs and focused peer review as recorded below.

Recovery verification finding: first independent root recovery run at
/data/pcbs-oom/blockmanager-codec-spike/root-recovery355 FAILED the required
fetch-failure-path assertion. It removed four physical shuffle files and returned
correct rows but recorded no FetchFailed (10 successful tasks). Worker success
is therefore not accepted as recovery proof yet. Shutdown executor failures
followed the failed assertion; they are not the initial cause. Both base jars
were checked and contain no stale recovery classes. Worker is investigating
the fault-injection preconditions without weakening acceptance. Root and worker
then identified a fixture bug: executor-side code forwarded addressless
spark-client storage endpoint refs, which can resolve on the wrong connection.
NettyRpcEnv.scala around 520 explicitly prohibits sharing these refs with third
parties; root read that contract and the client rebinding/routing implementation.
The corrected fixture uses a routable driver fault-injection endpoint to call
existing master.removeRdd(blocking=true), checks all locations empty, and only
then permits parent reads. This is test control, not a new recovery protocol.
The repaired root run passed with one genuine FetchFailed and thirteen successful
tasks, plus exact block migration. Final source-guard root repeat also PASSED at
/data/pcbs-oom/blockmanager-codec-spike/root-recovery-final355; source/runner
snapshots match the committed versions. Root independently audited final raw
worker gates, task events, physical deletion and input/artifact hashes. Focused
peer review found no remaining blocker within scope. This extension is READY
FOR REVIEW, not architect-accepted DONE.
Retain failed evidence; do not infer Spark recovery instability from this test
bug. Primary/native independent passes remain valid. MCP-only throughout.

## Accepted upstream revision and next proof (2026-10-22)

READY FOR REVIEW: [revised proposal](pcbs-cache-storage-api-proposal.md).
Architect clarified priority: bypass generic Java object serialization/
deserialization for SQL cache data first; avoiding heap is secondary and
heap-backed payloads are acceptable for the first proof. Reuse the existing
BlockManager wherever possible. Match existing compute-on-miss and genuine
shuffle-fetch recovery; do not introduce a Deferred/resubmit task protocol.

Independent review accepted: reuse existing block write/read locks, completed
put/status publication, blocking removal and canonical locations. The prior
provider-owned directory, parallel commit/fence protocol and weakened blocking
unpersist recommendation are removed from the unshipped proposal. They need no
compatibility scaffolding. Existing released storage/serialization/lifecycle
behavior stays the default; no production code or new public config changed.

Bounded MCP-only source audits traced per-cache codec context through
SerializerManager, MemoryStore.SerializedValuesHolder, disk/remote IO and
replica reception. Root independently read those decisive paths, BM put/remove
locks and historical commits/code. Source evidence supports a narrow optional
per-cache serializer/codec association; no integration prototype is claimed.
Small control metadata is distinct from bulk cache payload serialization.

ExternalBlockStore history: SPARK-12667/ad1503f92e1 removed the rarely used API
in favor of standard filesystem interfaces; SPARK-13992/e41acb75732 added native
serialized OFF_HEAP afterward. The default value helpers and shipped Tachyon
backend still used BlockManager serialization; existing native placement is
not itself a generic-serialization bypass.

Next proposed proof: one real BlockManager-integrated codec prototype, first
allowing heap values, then RAPIDS-managed spillable payload storage. Spark's
existing OFF_HEAP pool is explicitly excluded by the architect: it is partitioned
from cuDF/Spark-RAPIDS memory and cannot exchange capacity with it. Reusing
BlockManager means coordination/lifecycle reuse, not its off-heap allocator.
The earlier suggestion to prototype that pool was incorrect and is removed.
Cover serialized
unroll, partial admission, disk, multi-JVM remote/replica context, mixed caches,
existing recovery and blocking-reader cleanup. A core-hook prototype belongs
to isolated long-term Spark research, not the unmodified-Spark short-term path.
This was the proposed next proof before the architect authorized the active
spike above; bounded compiled/runtime results are now linked there.

Two focused peer reviews verified the narrower revision after a historical
backend-override wording correction. Evidence links, prior art, a reuse diagram
and numbered rules replace the earlier dense API sketch. No search-result-only
adjacent proposals remain. MCP docker_exec-only continues at all handoffs and
checkpoints. Document checks passed; no builds/jobs/production changes.

Earlier short-term architect exploration: bypass existing RDD persist/unpersist
storage entirely, using our own BlockManager-equivalent for partition lookup,
native/disk retention, remote retrieval and cache-miss recomputation. The intended
payload path avoids heap (network staging may be acceptable). Do not assume
Spark must continue storing descriptors: that was the earlier split-store
candidate. SQL cache plan reuse can remain. The custom actual statistics-wrapper
RDD lifecycle/compute interception has passed the local research fixture on
Spark3.5.5 and4.0.1; it is not production implementation. The newer accepted
upstream direction above preserves existing recovery, including genuine retry.

## Active scheduler-token research spike (2026-10-22)

READY FOR REVIEW. User authorizes an executable disposable research spike, newer
Spark compatibility investigation, and a long-term Apache Spark storage-extension
proposal. This supersedes earlier discussion-only/no-implementation wording for
research code ONLY. No production cache implementation, Spark patch, new public
plugin configuration, external posting or PR is authorized.

Acceptance: prove actual SQL cached-builder factory/lifecycle interception and
native-store reads with genuine small regular-BM scheduler tokens; distinguish
SQL correctness from scheduler awareness using a real upstream shuffle and
controlled loss of its outputs. Cover explicit unpersist, empty partitions,
token/payload loss and cache miss/recompute; label unsupported/failing cases.
Small direct-native buffers are acceptable to isolate the storage mechanism,
but do not claim this establishes PCBS/GPU/spill or large-data performance.
Reuse the existing large PCBS evidence; preserve original 1 GiB batching.

Parallel work: one worker owns isolated mechanism code/runs under
/data/pcbs-oom/token-cache-spike; another owns source/binary/version comparison
under /data/pcbs-oom/token-cache-version-audit; another owns the upstream API
proposal under /data/pcbs-oom/token-cache-upstream-design. Root integrates,
independently reads decisive code, reruns acceptance checks and coordinates
focused peer review. MCP docker_exec ONLY at every handoff and checkpoint;
all three worker assignments explicitly repeat this requirement.

Released SQL/PCBS semantics remain unchanged. The research wrapper/token/native
store is UNRELEASED and can change directly. Newer-version source evidence is
not compile/runtime evidence; record exact references and missing coverage.
Short-term requires unmodified Spark. Long-term upstream proposal should honor
StorageLevel and offer supported storage/ownership/transport/scheduler hooks,
not just a cache-format hook. No community acceptance is presumed. Park a path
on a demonstrated architectural blocker; continue independent tracks.

Completed outputs: [research results](pcbs-token-cache-spike.md),
[portable fixture](../../tools/pcbs-token-cache-spike/README.md), and
[upstream API proposal](pcbs-cache-storage-api-proposal.md).
Root independently compiled/executed the final package on 3.5.5/4.0.1 with AQE
off/on. Each JVM recorded 142 TaskEnd events, exactly two expected FetchFailed
events in deliberate payload-only-loss controls, and normal ApplicationEnd.
All rows/count/sum and initial statistics passed. Small 56 B tokens preserved
native cache hits after four shuffle files and four map outputs were erased;
deleting token+payload enabled recomputation.
The fixture uses 10k Longs/80k logical bytes/local[4]/1 GiB, NOT PCBS/GPU/spill/
remote or production ownership. Payload-only loss with a token requires retry.
Final package source/artifact/runtime hashes and 55 pinned official source
files were independently audited; checked-in verified-results.json records
exact provenance. Two focused peer reviews passed after correcting proposal
status wording.

Source-only later references 4.0.4/4.1.3/4.2.0/master do NOT establish runtime
support. 4.2 has keyed statistics and a new constructor; master 5.0.0-SNAPSHOT
changes the wrapper factory to mapPartitionsWithIndexInternal. No GPU jobs,
production sources or Spark sources changed.
The upstream proposal keeps Spark lifecycle/locations/recovery authoritative
and adds backend storage/lease/transfer contracts honoring StorageLevel.
The initial draft's new commit protocol and scheduler defer/resubmit were
subsequently rejected in review and removed; the accepted revision above
reuses BlockManager and existing recovery. No external posts, PRs or community
approval.

Latest proposed refinement: keep genuine small scheduler tokens under the actual
owner's RDDBlockIds in regular BlockManager, while own custom RDD compute serves
all data through the native cache manager. DAGScheduler requires BOTH a non-NONE
getStorageLevel result and regular BM locations. The virtual getter can report
the requested level without setting RDD's internal field through super.persist,
so final iterator can still bypass regular data caching. Tokens must represent
committed, resolvable partitions; native host-to-disk spill is not eviction.
Token removal and payload removal need version-aware coordination; ordinary
Spark marker replication/decommission does not automatically copy native data.
The local stock-Spark fixture now proves the scheduler-token boundary with a
real upstream shuffle; production paired lifecycle/remote availability remains
unimplemented. Root independently compiled/reran both packaged versions and
verified raw events/artifacts. Newer source audit found private API changes;
see completed research below. No production protocol or implementation approved.

Bypassing super.persist leaves RDD's internal storage level NONE, so its final
iterator calls compute instead of Spark BlockManager.getOrCompute. Own compute
can implement local/remote native-cache lookup and parent computation on a miss,
preserving the supplied statistics wrapper function. Need investigate scheduler
integration: DAGScheduler.getCacheLocs426 uses getStorageLevel and Spark BM
locations; getMissingParentStages748 traverses upstream dependencies when those
locations are absent and can schedule missing shuffle stages despite our cache
hit. Custom preferred locations alone do not supply that cache-hit boundary.
Root independently read these paths; bounded worker audit uses MCP tools only.
Research implementation is authorized as recorded below; production changes and
Spark implementation changes are not. Released SQL/PCBS contracts remain; new
cache internals are unreleased.
Earlier design proposals and chronological evidence follow below.

## Production patch plan (2026-10-22)

READY FOR REVIEW: [production plan](pcbs-shadow-payload-plan.md). User authorized
a written plan and overview of unresolved design questions, not implementation
or an architecture change. Root coordinated three bounded research tasks
(ownership/retirement, transport/recovery, released compatibility/validation)
and independently re-read the decisive Spark/PCBS code through MCP docker_exec.
Continue using MCP docker_exec for all project actions; repeat at every worker
handoff and progress checkpoint.

Release classification: existing PCBS CPU/serializer-only and GPU behavior,
logical statistics, supported storage/serializer/recovery semantics are RELEASED
(main source, public API and docs/additional-functionality/cache-serializer.md).
The disposable map/ID representation and proposed shadow protocol are UNRELEASED;
they require no compatibility adapter. Preserve released contracts; storage-tier
policy changes or new mandatory plugin/config requirements need architect
decisions. This record supplements the prototype classification below.

Plan prerequisites remain unresolved: exact persisted wrapper/incarnation
association; operation retention before serialized-reference copies; ordered
retirement including blocking unpersist; independently usable remote payloads
or safe whole-partition recomputation. Async listeners/polling/GC/TTL alone do
not satisfy these gates. Source research is not runtime protocol verification.
Prefer existing Spark services where proven usable; include streaming
self-contained serialization as an alternative and Spark changes only as the
long-horizon last resort. No transport or retirement mechanism is selected.

Validation for this planning task: source-backed compatibility review, focused
peer review by two agents, local document-link and whitespace checks passed.
Review refinements landed: qualify the tracked host limit, explicitly test
retire-before-publication and discarded serialized copies, reject stale executor
incarnations, and bound obsolete successful versions in a live cache generation.
No production edits, builds, new GPU jobs or external messages/PRs. The prior
benchmark results remain unchanged. Architect acceptance is pending.

## Candidate RDD lifecycle override (2026-10-22)

Source candidate, NOT compile/runtime-proven: user asks whether RDD persist and
unpersist can be overridden. Spark3.5.5 public persist(level), persist(), cache()
and unpersist(blocking) are non-final. Overriding only the PCBS producer misses
the later persisted statistics wrapper. However mapPartitionsInternal (RDD888)
is also non-final, private[spark], and normally constructs MapPartitionsRDD.
A producer subclass in the org.apache.spark namespace could override that
factory to return a MapPartitionsRDD subclass preserving Spark's supplied
statistics function and partitioning flag, with lifecycle overrides on the
actual persisted wrapper. This is a concrete stock-Spark proof candidate;
no reflection or Spark source modification has been selected.

Explicit builder clearCache calls virtual RDD.unpersist, but ContextCleaner
calls SparkContext.unpersistRDD directly (ContextCleaner225), bypassing the
override. Driver unpersist observation/reconciliation remains necessary for
that path. Do not claim this mechanism catches block eviction, migration or
all cleanup. Preserve released statistics/lineage/storage semantics; the new
subclasses are unreleased. Root and worker use MCP docker_exec only and have
independently read decisive code. No production edits, compile or runtime jobs.
Prefer recovery WITHOUT task failure; task failure remains a fallback.

## Narrowing the research spike (2026-10-22)

READY FOR REVIEW: [recovery/store spike proposal](pcbs-shadow-recovery-spike.md).
User challenged the breadth of the plan. Existing refcounts, stream adapters,
primitive descriptor serialization and spill-store budgets should be reused.
Source research identifies a candidate post-lock-release executor failure hook
for acknowledged stale-partition invalidation before normal task retry. This
could avoid requiring every copied descriptor to retain payload ownership.
It is not runtime-proven; plain exceptions do not invalidate present metadata,
and shuffle FetchFailed is not a generic cache recovery signal.

Two proposed independent proofs: multi-JVM invalidation/retry/recompute plus
driver event mapping; and existing store admission/native CPU input adapters
with limits on/off. No production architecture or storage contract change is
selected. Released behavior remains preserved; the shadow protocol is unreleased.
MCP docker_exec-only confirmed for root and both workers. Root independently
re-read decisive Spark/PCBS/spill code. Both workers reviewed the narrower
proposal; the distinction between global host and retained-store budget tests
was corrected. Document checks passed. No jobs/builds or production edits.

## Active shadow prototype and parallel research

READY FOR REVIEW: local registry/native-cache proofs, reduced-heap/stress
results and source research verified. Production ownership and literal zero-heap
spill are not implemented/claimed. User authorized two parallel tracks on2026-10-22:
1. Disposable LOCAL-MODE PCBS payload registry: JVM map ID to existing
SpillableHostBuffer, small Spark cached descriptors, no deletion handling.
Fresh JVM for every cache population. Start from original1GiB splitting;
REVERT/EXCLUDE prior16/32MiB experimental caps. Preserve six approximately
1GiB payloads per cache partition in the original full workload.
2. Research missing production ownership/remote/recovery mechanisms and cache
metadata consumers, including newer mounted Spark releases. Report any planning
dependencies rather than assume metadata is never used by the optimizer.

Prototype source is isolated on pcbs-shadow-experiment, based on4ca06aded8,
worktree /data/pcbs-oom/pcbs-shadow-experiment. Main production source/artifacts
must remain unchanged. This experimental representation is UNRELEASED and
intentionally local-only/non-cleaning; it is not a proposed production contract.
Preserve numRows and original encoded-payload sizeInBytes even though actual
Spark descriptors are small. Released query/schema/filter/statistics semantics
remain required when comparing the workload.

Baseline test: Spark3.5.5/local[8]/48GiB heap, ParallelGC; globalGPUtarget1GiB,
GPUconcurrency/max2, fixed15GiB devicepool, host40GiB/pinned2GiB; fraction0.6,
defaultunroll16/reset100. Sixteen cache partitions (~6GiB each),96sources,
801279744rows and16Doublecolumns. Do not change memoryfraction or reducebatch
sizes to obtain a pass. Keep original sequential fill +warmup/three timed
full SQL reads for comparison; verify measured payload sizes, storage tiers,
correct rows/values, source/artifact/config hashes and fresh-JVM exit status.
Small functional checks must force descriptor serialization/disk reload.

Conditional next steps ONLY after registry proof passes: full GPU bulk path
whose encoded/column payload bytes avoid Java heap, then decrease ONLY heap
to find a measured passing/failing boundary. Metadata, bounded control/staging
buffers and final aggregate results must be distinguished from bulk payloads.
Source/plan/allocation evidence is needed; low observed heap alone is not proof
that no payload allocation occurred. Preserve all failures; do not blindly
repeat a failed variant or generalize local success to distributed correctness.

Workers: pcbs_only_plan owns isolated implementation/build/check preparation;
spark_cache_trace owns metadata/version research; heap_review owns full-design
lifecycle/remote research. Lead coordinates GPU/build exclusivity and benchmarks,
independently reads decisive changed code, verifies evidence and integrates.
Full details stay in worker notes; report compact findings with citations.
All use MCP docker_exec ONLY and confirm at handoff/checkpoint/resume.
No production fix, new public configuration, Spark patch, external post or PR
is authorized. Any Spark-change option remains explicitly labeled LAST RESORT.

### Conditional stress dimensions

User additionally authorized on 2026-10-22, only after the first proof passes:
compare a 2 GiB global batch target, removal of explicit GPU concurrency caps,
and local[16]. Change dimensions independently before combining them. Keep
roughly 6 GiB per cache partition; use 32 partitions for two waves at 16 threads.
Measure actual emitted batches and effective concurrency, not just targets.
Keep the 40 GiB host off-heap limit ENABLED, the 2 GiB pinned pool and 15 GiB GPU
pool throughout; spill payloads to disk rather than allow unbounded native
retention. Check scratch capacity before each fresh JVM and preserve failures.
Reduced-heap testing remains conditional on the heap-free GPU path passing.
No memory-fraction change or production-default change is authorized.

### Shadow proof checkpoints

On 2026-10-22, isolated prototype 98e5bcc897 built successfully; frozen artifact
/data/pcbs-oom/pcbs-shadow-sweep/rapids-shadow-array-cuda12.jar has SHA256
75c3de6df31cd7d37b0095db7638e15d3380aa6ec2a8a9abdb079292eb5d373d.
Build used package -DskipTests for artifact creation, not a test-pass claim.
Fresh-JVM functional checks passed for row and GPU-columnar writer entrypoints:
each encoded 1.2M ordered four-Long rows into one 19,203,662-byte payload and a
127-byte serialized descriptor. DISK_ONLY reload and explicit Java serialization
preserved IDs, rows and payload length; all decoded values/order passed. Root
independently read the check source, raw markers and provenance result. GPU was
released afterward. The first large workload subsequently PASSED as below;
the heap-free GPU path remains unverified.
Evidence: /data/pcbs-oom/pcbs-shadow-sweep/entry-shadow-array-{command,result}.json
and entry-shadow-array.log. All project actions use MCP docker_exec only.

First large registry proof PASSED: original 1 GiB/six-record layout, 48 GiB
heap, local[8], unroll/reset 16/100. Fill 246.403 s; warmup 81.244 s; measured
reads 74.095/73.939/76.581 s. All 801,279,744 rows/16-column aggregate checks
passed, five checkpoints preserved 96 payloads/102,579,664,416 bytes, and the JVM
exited 0 without heap OOM. Spark retained 4,992 bytes of references; shadow store
had 39 host/57 disk payloads after fill and 37 host/59 disk after reads. Root
independently verified raw evidence and unchanged artifact hashes. Full report:
pcbs-shadow-payload-proof.md; evidence stage1-independent-verification.json.
Read GC 3.90%; sampled heap peak 35.15 GiB is occupancy, not a live-set estimate.
735 recoverable pinned-pool capacity messages were preserved, not tuned away.
Stage 2 native GPU writer/reader is now authorized and IN PROGRESS in the isolated
worktree. Original artifacts remain frozen. GPU tests remain serialized with
builds; all project operations remain MCP docker_exec-only.

Native source d247fe7a6e built successfully into frozen artifact
/data/pcbs-oom/pcbs-shadow-native/rapids-shadow-native-cuda12.jar, SHA256
8259c4448e24899d6f145a2fee256a5cb1c658d0bce0686e221c56a272ebd267.
Root and peer reviewed the consumer ownership/copy/publication and read-retry
code. Fresh-JVM native functional checks PASSED (17.793 s): GPU row/columnar
writers and forced CPU row writer after GPU initialization, exact 1.2M ordered
four-Long rows through GPU and CPU decoders, Java/DISK_ONLY descriptor reload,
and unpublished callback-buffer cleanup/rejection/refcount checks. GPU decode
left array-get counters unchanged; CPU fallback deliberately used arrays.
Root read the check source/raw markers and verified normal exit and immutable
hashes. Versioned v3 native-path diagnostic benchmark compiled successfully;
the native large cache-path workload subsequently PASSED: fill 214.977 s,
warmup 21.591 s, measured reads 20.929/20.632/20.116 s; all native bulk counters,
96 original-sized records, exact rows and aggregates passed, normal exit, no
audit issues in independent raw-event analysis. Read GC was 0 ms; sampled heap
peak 16.02 GiB and whole-process RSS peak 56.71 GiB. Source/artifact unchanged.
Evidence cases/native-g1-slots8-capped2-heap49152m/{result,independent-analysis}.json.

IMPORTANT QUALIFICATION: this is native PCBS write/read, not literal end-to-end
zero heap. Source audit found GpuRand CPU-generates doubles into native storage,
and existing spill I/O stages payload through reusable 8 KiB Java buffers plus
default LZ4 block arrays (32 KiB blocks). Registry counters do not cover those
buffers. No compression/encryption/default change was made. The large temporary
allocation during fill is not yet attributed by an allocation profile. A raw
native file path under unchanged compression settings is not established.

The original-generator 16 GiB control PASSED: fill 215.200 s, measured reads
20.078/20.918/20.356 s (median 20.356 s), read GC 0.0098%, sampled heap peak
5.38 GiB and RSS peak 45.95 GiB. Independent raw-event analysis found no audit
issues; exact rows, original six-record layout, payload totals, native bulk
routing and normal JVM exit passed. Evidence: native-g1-slots8-capped2-heap16384m
under /data/pcbs-oom/pcbs-shadow-native/cases.

A separate untimed generation-only JFR probe PASSED without caching. Its
weighted allocation samples predominantly point to Integer.valueOf through
Scala Range.foreach in GpuRand. These are sampled temporary allocations, not
an exact allocation census or retained-cache bytes. Recorded stacks and compact
summary are under /data/pcbs-oom/generation-allocation-probe/gpu-rand-trial0.

Benchmark v4 replaces rand with salted GPU xxhash64/mask/cast/divide expressions.
The runtime oracle PASSED: 160,000 CPU/GPU Double raw-bit comparisons, GPU-only
computational expressions, 128 native bytes/row without validity buffers.
This changes the dataset and fill cost, so v4 requires a separate baseline.
The GPU-generated 48 GiB baseline PASSED: fill 125.243 s, median read 20.486 s,
read GC 0 ms, sampled heap peak 4.75 GiB and RSS peak 42.21 GiB. Its 96 payloads
totaled 102,584,813,088 encoded bytes; native bulk and independent raw-event
checks passed with normal JVM exit. The matching 16 GiB control also PASSED:
fill 127.860 s, median read 20.533 s, read GC 0 ms, sampled heap peak 2.58 GiB
and RSS peak 42.18 GiB, unchanged payload totals and no independent audit issues.
The read-only overlap/automatic-environment analyzer fixtures passed in the idle
window. Task timestamps measure driver-recorded task lifetime, not physical CPU
execution. Even per-stage overlap can exceed configured slots: local scheduler
slot reuse precedes asynchronous result handling that records task finish time.
Do not interpret these intervals as proof of simultaneous executor task bodies.
The first 2 GiB-target case exited normally but FAILED fixture shape: it still
produced six approximately 1 GiB records, not three approximately 2 GiB records.
Do not count it as a passing large-record stress. Independent analysis retained
the invalid shape. Root and worker missed GpuProject's independent pre-split
threshold, default GPU pool/8 = 1.875 GiB here, which halves projected output
before PCBS. Source review confirmed this in the isolated artifact worktree.
A separately named fixture will use the existing test-only splitUntilSize
3 GiB override; native limits stay unchanged. This additional test dimension
precludes attributing its performance difference solely to the PCBS batch target.
The 1 GiB/local[8]/48 GiB automatic-concurrency case PASSED: fill 126.816 s,
median read 13.013 s, read GC 0 ms, sampled heap peak 4.54 GiB/RSS 41.31 GiB.
Independent checks passed, including absence of ordinary/internal inherited
concurrency overrides. GPU admission high-watermark was 7 in read stages.
The direct 512 MiB heap probe PASSED: fill 126.489 s, median read 20.453 s,
read GC 0.0404%, sampled heap peak 0.194 GiB/RSS 40.82 GiB. Exact shape/payload,
rows/aggregates, native counters, normal exit and independent raw analysis passed.
Intermediate 8/4/2/1 GiB cases are NOT implied tested; 512 MiB is lowest tested,
not an exact minimum. Native limits and process memory remain substantial.
The project3g fixture also exited normally but FAILED shape: the direct
sc.runJob fill did not propagate the driver SQL setting to tasks. Raw population
JobStart properties were empty; each task still reported three pre-splits.
Root independently read SQLConf/ReadOnlySQLConf/SQLExecution propagation code.
Lesson: verify the executor getter and the first actual record before accepting
a large stress fixture. Both invalid-shape runs are retained, never counted PASS.
A separately versioned v5 benchmark now sets/restores ONLY that existing local
property around direct fill and requires its actual 3 GiB getter value in every
population task. Independent review found a byte-size acceptance gap; runner
now requires each measured encoded record >95% of the 2 GiB target as well as
strict three-record row shape. V5 compiled in the idle window (3.085 s), SHA256
828e016bd211bd894533e64eb631983e84a6bcc62213b65e89b73fbbac352637.
The local[16]/32-partition/48 GiB/1 GiB-record/capped2 case PASSED: 192 payloads,
205,169,626,176 encoded bytes, fill 300.317 s, warmup 104.013 s, measured reads
94.637/95.151/94.997 s, read GC 0 ms, sampled heap peak 7.74 GiB/RSS 43.86 GiB.
Independent checks passed with no audit issues. Stock executor gauge sampled
16 active task threads in every read; GPU admission remained 2. Initial servlet
HTTP302 samples are retained separately; corrected /metrics/json/ sampling had
valid read coverage, plus 18 connection errors after JVM shutdown. These are retained.
Root ran new large-record acceptance fixtures PASS and independently reran the
completed-read comparison. Warmup spill read/materialization summed task time
was 182.749 s versus baseline 29.728 s; GPU utilization averaged 9.6% versus40%.
This is open/read/decompression/copy time, not proof of physical disk misses.
One additional 1 GiB write and no registry retry inflation argue against a
rewrite storm. Page-cache pressure remains a hypothesis pending IO evidence.
The actual 2 GiB-record local[8]/48 GiB/capped2 case PASSED: 48 payloads of
2,137,181,644 encoded bytes each; total 102,584,718,912 bytes. Fill 161.977 s,
warmup 34.619 s, measured reads 34.361/34.748/33.942 s, read GC 0 ms, sampled
heap peak 5.34 GiB/RSS 41.85 GiB. All task getter, strict row/encoded-byte,
native-counter, query and independent raw checks passed; normal exit/no audit
issues. Spark reference storage was 2,688 bytes. Source/plugin remained frozen.
Late-start process IO sampling found zero kernel-accounted storage-read bytes
in the measured-read sampled subwindows (84.4/95.0/97.2% coverage), despite
approximately 55–61 GiB logical rchar deltas. These are partial process-wide
windows, not exact payload-only/full-phase totals; cold storage reads are not
supported as the cause of this case's slowdown.
The combined case gpuhash-g2-project3g-taskconf-slots16-automatic-heap512m
PASSED: local[16], 32 partitions, 2 GiB target, automatic GPU concurrency and
512 MiB configured heap. Exact 96 payloads/205,169,437,824 encoded bytes; Spark
reference storage 4,096 bytes. Fill 358.876 s, warmup 115.575 s, measured reads
106.227/105.117/109.340 s (median 106.227 s), read GC 47 ms/0.0147%, sampled heap
peak 0.234 GiB (~239 MiB), process RSS 39.66 GiB. Normal JVM exit 0, all 32 actual
threshold markers 3 GiB, native-path counters, row/aggregate/config checks and
independent strict analysis passed with no audit issues. Effective concurrency
overrides were absent; raw task-local GPU admission MAX 3 and stock sampled
active executor threads MAX 16 in each read. Recoverable pinned-pool allocation
fallback messages remain in raw logs; no heap OOM occurred.
Measured IO subwindows covering 98.8–98.9% of reads recorded 155.5–160.0 GiB kernel backing
reads. These are sampled process-wide subwindows, not exact payload/phase totals.
Root reran the warmup comparison, and independent final evidence review found
no material issue. Sampler startup/shutdown errors and coverage remain recorded.
Root rehashed 8 frozen source/artifact checks after final exit: all matched,
including original baseline plugin/benchmark. GPU was released. Root also
reconstructed all seven CSV rows from raw phase/partition/TaskEnd events:
report-csv-verification.json passed. CSV line endings were normalized after the
Git whitespace check, then the numeric audit was rerun unchanged. Final focused
report/CSV peer review found
no material issue. Seven accepted GPU-generator cases are in
docs/dev/pcbs-shadow-performance.csv; full scratch
aggregate retains both invalid-shape fixtures and all raw evidence under
/data/pcbs-oom/pcbs-gpu-generator. Exact minimum/failing heap boundary, combined
48 GiB and intermediate heaps were not tested. No further benchmark is queued.
Local proof/results READY FOR REVIEW; architect acceptance pending. Existing
compressed spill still uses bounded heap buffers, so literal zero heap remains
unmet. Production owner registration, cleanup leases, remote fetch/recovery and
Spark-version runtime coverage remain future design/verification work. Main
production source/artifacts were unchanged by this experiment; native code is
isolated at d247fe7a6e. All workers reconfirmed MCP docker_exec-only.
Literal zero-heap compressed spill remains open. GPU jobs/builds/tests stay
serialized; MCP docker_exec-only confirmed by workers at this checkpoint.

Clarified accounting contract: SQL cache accumulators retain exact rows and
encoded Parquet payload bytes for SQL/AQE statistics. BlockManager separately
reports physical Spark reference-object memory/serialized disk sizes; these are
not the SQL cache relation-size input to AQE. Shadow resources need separate
accounting. No attempt to override BlockManager size is selected.

Parallel design research is READY FOR REVIEW in pcbs-shadow-metadata-research.md
and pcbs-shadow-lifecycle-research.md. Independent focused reviews confirmed the
SQL-statistics/storage distinction and qualified lifecycle source claims. Root
read the decisive cache wrapper, persistence, unpersist, payload-store and reader
code. These are source findings, not runtime distributed correctness evidence.
The prototype also received an independent source/compiled-field review with no
in-scope defect found. Production owner association, transport, recovery and
retirement remain unimplemented; no architecture has been selected.
A follow-up source review found an alternative owner-registration candidate:
observe InMemoryTableScanExec.relation during driver plan preparation before
GPU-only gates, then bind its actual cachedColumnBuffers wrapper and direct
producer. Existing AQE preparation runs before cache-stage submission. This
avoids arbitrary transitive ancestry for that SQL path, but eager construction,
namespace binding, CPU/direct-RDD bypasses and publication timing need runtime
proof. Metadata research records this option and its independent focused review.
Source review also found a pre-lookup reader race: remote Spark cache bytes can
already be copied into the receiver before lazy descriptor deserialization.
After source transfer releases its block lock, unpersist could delete a hidden
payload while that copy still awaits decoding. Lookup/readObject leases alone
do not cover the interval. Lifecycle research records the race and unproven
coarse task-admission/transfer-handoff alternatives; TTL alone is not a proof.

## Scope and acceptance

Reproduction only, timeboxed through tomorrow morning (America/Chicago; exact
cutoff not specified). Reproduce Java heap exhaustion while loading GPU-produced
Parquet cached batches. Record measured batch sizes, heap, task concurrency,
GPU concurrency, storage level, data, artifact revision, commands and logs.
Compare reduced task concurrency and smaller batches. A creation-time allocation
failure is not the target. No new configuration, production changes or PR.
Park experiments that threaten other workloads or exceed bounded resources.

Use ONLY mcp__docker_exec__* tools for sandbox filesystem and command operations.
Repeat at worker handoffs/checkpoints and after context resets; workers must
confirm continued MCP use. Use code-index before code discovery. Do not touch
unrelated untracked files. No earlier committed tracker was found with
`git ls-files '*tracker*' '*TRACK*' '*16362*'`.

## Release and compatibility

The starting branch equals main at 3cdf711ded8e8b72d0f3ee92866401df5d9d2a06.
ParquetCachedBatch's Array[Byte] representation and configured batch sizing are
RELEASED because they exist on main. The >2 GiB slicing fix 39f8cceb61 is also
on main. Preserve serializer contracts and all configuration behavior. No
unreleased behavior is being modified; no compatibility scaffolding is needed
for this standalone diagnostic script.

## Reproduction and environment

Confirmed on Spark 3.5.5, Scala 2.12.18, Java 17.0.20.1, RTX A6000 (49140 MiB).
Host RAM is 188 GiB; the test JVM heap is explicitly limited to 3 GiB.
The plugin was rebuilt from the starting revision, artifact version
26.12.0-SNAPSHOT, cuDF 26.12.0-SNAPSHOT. The pre-existing 26.10 jar was stale
and was not used for the reported results.

Successful build command:

```sh
rapids-plugin-mvn -nsu package -pl dist -am -Dbuildver=355 -DskipTests \
  -DsecondaryCacheDir=/data/pcbs-oom/zinc
```

The first build failed because its default Zinc directory under ~/.sbt was
unwritable. sbt.global.base did not fix it; secondaryCacheDir did. The successful
package build took 5m43s. Tests were explicitly skipped for this artifact build;
this is not a claim of a full test-suite pass. Full build output is at
`/tmp/sandbox_outputs/run_32e8ae99d01345b39fdc7538b32665f5.stdout`.

Run from the repository root in a dedicated JVM. The script terminates the JVM
and an expected OOM is a failed run. GPU generation is real; data is generated
in-process, without external inputs. The script persists the DataFrame with
MEMORY_AND_DISK, populates the serializer's actual cached RDD one partition at a
time, then reloads that RDD with the configured task concurrency. It prints the
plan, actual cache batch sizes, storage summary and row-count result. Reading
CachedBatch objects directly deliberately isolates deserialization from GPU
Parquet decode. It does not exercise the entire downstream SQL scan.

```sh
env PCBS_ROWS=4000000 PCBS_PARTS=8 PCBS_COLUMNS=16 \
  /home/roberte/src/spark_3.5.5/bin/spark-shell \
  --master 'local[8]' --driver-memory 3g \
  --jars dist/target/rapids-4-spark_2.12-26.12.0-SNAPSHOT-cuda12.jar \
  --conf spark.plugins=com.nvidia.spark.SQLPlugin \
  --conf spark.sql.cache.serializer=com.nvidia.spark.ParquetCachedBatchSerializer \
  --conf spark.sql.adaptive.enabled=false \
  --conf spark.rapids.sql.enabled=true \
  --conf spark.rapids.sql.exec.InMemoryTableScanExec=true \
  --conf spark.rapids.sql.concurrentGpuTasks=1 \
  --conf spark.rapids.memory.gpu.allocSize=2g \
  --conf spark.rapids.memory.gpu.maxAllocSize=8g \
  --conf spark.rapids.sql.batchSizeBytes=536870912 \
  --conf spark.memory.fraction=0.15 \
  --conf spark.serializer.objectStreamReset=100 \
  --conf spark.local.dir=/data/pcbs-oom/manual-local \
  -i tools/pcbs_oom_repro.scala </dev/null > /data/pcbs-oom/manual.log 2>&1
```

Use `local[2]` for the concurrency control, keeping eight partitions and all
other settings. Set batchSizeBytes=67108864 for the smaller-batch control.
The small memory fraction intentionally forces disk caching reproducibly; it
is not a claim about the customer's configuration. local mode shares the
executor/driver heap. The reproduction establishes the failure mechanism,
not the customer's exact memory threshold or multi-executor behavior.

## Results

All runs use GPU concurrency 1, heap 3 GiB, memory fraction 0.15,
MEMORY_AND_DISK deserialized storage, eight partitions, random Double columns.
Each column uses a distinct fixed rand seed. Batch targets are not measurements.

| Run | Task slots | Rows/partition | Columns | Batch target | Measured bytes/batch | Result |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| baseline | 8 | 2,000,000 | 16 | 256 MiB | 256,039,547 (one/partition) | PASS, 16M rows |
| large8 | 8 | 4,000,000 | 16 | 512 MiB | 256,039,547 (two/partition) | reload heap OOM |
| large2 | 2 | 4,000,000 | 16 | 512 MiB | 256,039,547 (two/partition) | PASS, 32M rows |
| single512 | 8 | 2,000,000 | 32 | 1 GiB | 256,042,218 (two/partition) | reload heap OOM |
| verified8 | 8 | 4,000,000 | 16 | 512 MiB | 256,039,547 (two/partition) | reload heap OOM |
| small64 | 8 | 4,000,000 | 16 | 64 MiB | 66,784,684 / 55,689,868 (eight/partition) | reload heap OOM |
| small64reset1 | 8 | 4,000,000 | 16 | 64 MiB | 66,784,684 / 55,689,868 (eight/partition) | PASS, 32M rows; stream reset=1 |

Despite its scratch name, single512 did not produce a single 512 MiB array.
Actual measured sizes are reported above. The upstream plan produced smaller
batches than the configured target. Initial scratch runs included a 50ms pause
per batch during reload. verified8 uses the reviewable script with no pause
and reproduced the same failure. Therefore no artificial delay is required.

For verified8, all eight partitions were populated before reload; the storage
summary reports MemorySize 0 B and DiskSize 3.8 GiB. Stage 8 starts eight tasks
and fails allocating a Java array, matching the customer's second trace:

```text
java.lang.OutOfMemoryError: Java heap space
  java.lang.reflect.Array.newInstance(Array.java:78)
  java.io.ObjectInputStream.readArray(ObjectInputStream.java:2121)
  org.apache.spark.serializer.JavaDeserializationStream.readObject(JavaSerializer.scala:87)
  org.apache.spark.serializer.DeserializationStream$$anon$1.getNext(Serializer.scala:168)
  org.apache.spark.util.NextIterator.hasNext(NextIterator.scala:73)
  org.apache.spark.storage.memory.MemoryStore.putIterator(MemoryStore.scala:223)
  org.apache.spark.storage.memory.MemoryStore.putIteratorAsValues(MemoryStore.scala:302)
  org.apache.spark.storage.BlockManager.maybeCacheDiskValuesInMemory(BlockManager.scala:1735)
  org.apache.spark.storage.BlockManager.getLocalValues(BlockManager.scala:961)
```

Raw run logs, GC logs and exact argv records are under `/data/pcbs-oom/`, named
by the run column (e.g. verified8.log, verified8-gc.log, verified8-command.txt).
The temporary launcher is launch.py; the durable diagnostic is
`tools/pcbs_oom_repro.scala`. Historical logs are retained separately from
subsequent controls.

The small64 control kept total rows and columns fixed, produced eight smaller
batches per partition, populated all eight disk partitions (3.8 GiB, 0 B in
memory), then failed in ObjectInputStream.readArray via
PartiallyUnrolledIterator.hasNext (MemoryStore.scala:774). This matches the
other customer trace's iterator path. Smaller batches alone were insufficient
for this workload with the default Java stream reset interval. This does not
establish the performance or efficacy of smaller batches under other settings.

## Source findings and limits

Read directly in Spark 3.5.5:

- MemoryStore.scala:223 evaluates values.hasNext before keepUnrolling, stores
  the next object, and only then estimates/reserves memory. The default memory
  check period is 16 objects, initial reservation 1 MiB, growth factor 1.5
  (core internal config package.scala). A failed reservation can therefore be
  followed by another deserialization before the loop tests keepUnrolling.
- Serializer.scala:165 and NextIterator.scala:70 show that hasNext can read a
  complete object. Thus allocation precedes the reservation for its true size.
- BlockManager.scala:951 deserializes disk values before attempting to recache
  them through MemoryStore; this is the reproduced stack. Final deserialized
  storage accounting uses SizeEstimator, not merely CachedBatch.sizeInBytes.
- DiskStore.scala:179 uses a file input stream for ordinary disk reload. A full
  second serialized heap copy is not required for this reproduction. Serialized
  in-memory storage is a different path and can retain bytes while decoding.
- JavaSerializer.scala:44 resets its ObjectOutputStream periodically (default
  objectStreamReset=100). Its ObjectInputStream may retain earlier batches via
  object handles. small64reset1 changed only this interval to 1 compared with
  small64 and passed, with the same measured batch sizes and 3.8 GiB on disk.
  This supports retention as a contributor. It is one diagnostic pass, not a
  production recommendation, heap-retention measurement or performance result.

GPU concurrency 1 did not prevent the failure because CPU cache deserialization
runs before GPU decode. No claim is made that GPU concurrency never affects
heap pressure during other stages. No off-heap lifetime or spill design chosen;
callbacks, eviction ownership and cleanup remain design questions for discussion.

## CPU-only follow-up

Status: reproduction established with and without PCBS. No production changes.
All work remains through MCP sandbox tools; the release record above still applies.

The same diagnostic ran with CPU Project/Range plans, eight task slots, eight
partitions, 4M rows per partition and 16 seeded random Double columns. This
matches the GPU workload's shape; CPU and GPU random values are not claimed to
be bit-identical. SQLPlugin is absent in every CPU run. Default-serializer runs
omit the RAPIDS jar entirely. PCBS CPU runs include the existing built jar but
set spark.rapids.sql.enabled=false. A GPU check during CPU jobs showed only
16 MiB in use and 0% utilization. No GPU code change or rebuild was needed.

All runs use memory fraction 0.15 and MEMORY_AND_DISK deserialized storage.
Every failing run completed sequential population of all eight partitions,
reported 0 B in memory / 3.8 GiB on disk, and then failed during concurrent
reload. Outcomes are based on success markers or explicit OOM stacks, not
persisted exit codes. CPU runs used the same committed diagnostic without edits.

| Run | Serializer | Heap | SQL cache row limit | Java stream reset | Reported bytes/batch | Result |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| cpu-default | Spark default | 3 GiB | 10,000 (default) | 100 | 1,280,000 | PASS, 32M rows |
| cpu-default100k | Spark default | 3 GiB | 100,000 | 100 | 4,194,304; final 294,912 | reload heap OOM |
| cpu-default100kreset1 | Spark default | 3 GiB | 100,000 | 1 | same as above | PASS, 32M rows |
| cpu-default1g | Spark default | 1 GiB | 10,000 (default) | 100 | 1,280,000 | reload heap OOM |
| cpu-default1greset1 | Spark default | 1 GiB | 10,000 (default) | 1 | 1,280,000 | PASS, 32M rows |
| cpu-pcbs64 | PCBS CPU writer | 3 GiB | 10,000 | 100 | about 62,874,200; final 9,243,960 | reload heap OOM |
| cpu-pcbs64reset1 | PCBS CPU writer | 3 GiB | 10,000 | 1 | same range as above | PASS, 32M rows |

Spark-default failures show ObjectInputStream.readArray followed by
PartiallyUnrolledIterator.hasNext (MemoryStore.scala:774). PCBS CPU failure
shows readArray followed by MemoryStore.putIterator:223 and
maybeCacheDiskValuesInMemory:1735. Both match the relevant customer paths.
PCBS CPU used batchSizeBytes=67108864, producing nine records per partition.
The default serializer at 100,000 rows still stopped at 32,768 rows / 4 MiB,
producing 122 full records and one tail record per partition.

CPU-only command (from repository root):

```sh
env PCBS_ROWS=4000000 PCBS_PARTS=8 PCBS_COLUMNS=16 \
  /home/roberte/src/spark_3.5.5/bin/spark-shell \
  --master 'local[8]' --driver-memory 3g \
  --conf spark.plugins= \
  --conf spark.sql.cache.serializer=org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer \
  --conf spark.sql.adaptive.enabled=false \
  --conf spark.memory.fraction=0.15 \
  --conf spark.serializer.objectStreamReset=100 \
  --conf spark.sql.inMemoryColumnarStorage.batchSize=100000 \
  --conf spark.local.dir=/data/pcbs-oom/cpu-manual-local \
  -i tools/pcbs_oom_repro.scala </dev/null > /data/pcbs-oom/cpu-manual.log 2>&1
```

For the default-setting control use batchSize=10000. For the smaller-heap
control use driver-memory=1g with batchSize=10000. For reset controls change
objectStreamReset to 1. For CPU PCBS add the built jar via --jars, change the
serializer to com.nvidia.spark.ParquetCachedBatchSerializer, and add
spark.rapids.sql.enabled=false plus spark.rapids.sql.batchSizeBytes=67108864.
Do not add SQLPlugin. Full logs, GC logs and exact argv are saved under
/data/pcbs-oom/<run>.log, <run>-gc.log and <run>-command.txt. The scratch CPU
launcher is /data/pcbs-oom/launch_cpu.py.

### Interpretation

This is a general Spark/Java-serialization heap-pressure mechanism, amplified
by PCBS's larger records, rather than a GPU-only issue. Default CPU caching is
substantially more conservative: DefaultCachedBatchSerializer's loop in
InMemoryRelation.scala:89 limits estimated payload to 4 MiB and also honors the
10,000-row default. A single oversized row can exceed the byte target. The
large row-batch setting reaches the byte target on ordinary Double rows.

CachedBatchSerializer.scala:38 defines numRows and sizeInBytes, and the conversion
API imposes no documented 4 MiB maximum on custom serializers. PCBS therefore
is not shown to violate that API contract. However, the implementation does not
provide a hard per-byte heap reservation before Java deserialization, and Java
stream resets count objects rather than bytes. Spark's SQLConf.scala:483
explicitly warns that larger cache row batches risk OOM. Reproducing an OOM
therefore supports an upstream robustness issue/limitation; it does not by itself
establish a violated Spark guarantee or identify the exact customer's root cause.

The reset controls support object-stream retention as a contributor in both
serializers. They are single diagnostic comparisons, not a recommendation to
change a production setting globally. These initial controls did not evaluate
SQL performance; the follow-up sweep below does. Custom serializer lifetime
contracts remain unevaluated.

## Reset interval performance sweep (READY FOR REVIEW)

Authorized follow-up: measure performance versus heap pressure and OOM frequency
for objectStreamReset=1,2,4,8,16,100. Production serializers, configs and plugin
artifact remain unchanged. New standalone benchmark is unreleased diagnostic code.
Use MCP docker_exec tools for all sandbox operations, including workers; confirm
at checkpoints. Existing release classification continues to apply.

Plan: Spark-default CPU serializer (4 MiB batches), GPU PCBS with 64 MiB and
256 MiB batch targets. Same 32M-row / 16-Double / eight-partition data shape,
eight local task slots and GPU concurrency 1. Fresh 8 GiB JVM per configuration
for performance; one warmup plus five timed disk reloads and full all-column
SQL aggregates. Fresh 3 GiB JVMs for pressure, three independent trials per
configuration, one warmup plus two reloads each. OOM frequency is per JVM,
not per loop. Configuration order is shuffled deterministically.

Memory fraction=0.03 intentionally keeps each 512 MiB partition larger than
storage capacity at either heap size. Assert all partitions are on disk and
0 B stored in memory after population and every scan. This differs from the
initial reproduction's 0.15 and must not be treated as an identical workload.
Record population separately, reload wall time, SQL wall time, JVM GC count/time,
and heap use sampled every 10 ms. Sampled heap peaks are approximations, not live
retained bytes or process RSS. Explicit GC precedes each timed scan, outside the
timing. Population timing includes explicit GC between partitions. JVM startup
is excluded. Save raw JVM and GC logs, structured metrics, exit codes, manifests,
exact argv and source hashes. Timeout or unexpected errors park the sweep;
classify only explicit heap OOMs as OOM. Validate smoke tests and benchmark review
before the full sweep. No claims about production workload failure probabilities.

The smoke tests passed on GPU PCBS64 with reset1/3GiB and reset100/8GiB.
A review caught potential shuffle-output reuse when repeating one SQL aggregate;
the benchmark now constructs a fresh execution each iteration and requires its
cache-scan numOutputRows metric to equal 32M. Existing GPU metrics.level=DEBUG
is enabled consistently. Both smoke cases verified six full SQL scans each.
The lead read ShuffleExchangeExec's caching code to verify the concern.
Strict runner checks require zero exit, complete markers, exact phase/scan counts;
only non-timeout reload OOMs are accepted as expected failures. Reviewer verified
these repairs. The source, compiled benchmark jar and plugin jar are hashed in
manifests. Full sweep launched only after smoke validation and review.

The initial grid has 72 JVMs: 18 headroom timing cells and 54 pressure trials.
Because the initial timing differences are small, reset1 and reset100 endpoints
each received two more fresh timing JVMs per serializer/batch mode after the
grid, giving three independent JVMs per endpoint (12 additional jobs). All jobs
run sequentially to avoid benchmark interference. Every sandbox checkpoint
continues to use MCP-only access. No production code/config has changed.

Completed: 72 grid jobs plus 12 endpoint confirmation jobs. Logs, manifests,
commands and raw JSONL are under /data/pcbs-oom/sweep/full-* and confirm-*.
All jobs ran sequentially. There were 54 successful JVMs and 30 expected heap
OOMs during disk reload; no unexpected errors or timeouts. Every failure had
already populated all eight partitions on disk. The lead ran verify.py against
all 84 raw logs and independently recomputed all per-JVM timing medians.
export_report.py checked unchanged source/artifact hashes, configuration, plans,
partition metrics, storage and trial counts before exporting the report/CSVs.
Analysis: analyze.py (combines full/confirm, excludes warmup, median of per-JVM
medians); compare.py (endpoint deltas); plot.py (Matplotlib); export_report.py
(requires all 84 records, valid outcomes and unchanged source/jar hashes before
export). Plot with /home/roberte/miniconda3/bin/python; the script appends the
image Python site-packages for its missing pure-Python six dependency. All
benchmark JVMs and other Python scripts use the image tools, not Miniconda.
Independent analysis review recomputed representative medians, checked every
SQL scan's 32M rows and verified GC timestamp alignment. Post-GC values are
occupied heap after various pause types, not measured live data. OOM predicates
now match the runner (nonzero exit, no timeout, heap OOM, reload phase).
Final results and measurement limits: [reset sweep report](pcbs-reset-sweep.md).
At 8 GiB, reset1 versus reset100 changed PCBS reload times by less than 1%;
SQL aggregate times increased 4.2% (64 MiB) and 1.2% (244 MiB), with overlapping
fresh-JVM timing ranges. Population increased about 3.6–3.7%. CPU SQL scans
improved 23.3%. At 3 GiB, PCBS64 passed 3/3 at reset1/2/4 and failed 3/3 at
reset8/16/100; PCBS244 failed 3/3 at every interval, including reset1.
CPU passed 3/3 through reset16 and failed 3/3 at reset100. These frequencies
are observations under this controlled load, not production probabilities.
The larger-batch failures qualify the earlier reset1 success: it is a mitigation,
not a guarantee. Smaller PCBS batches cost about 60% in the tested SQL scan
at reset100. No global config change or production fix is proposed.
The benchmark compiled, smoke checks passed, raw-log verification passed and
exported metrics passed consistency checks. A read-only worker's final review
found no material issues: all 18 timing rows, endpoint percentages, 36 summary
groups, 84 outcomes and chart/CSV exports agreed with the analysis. It confirmed
MCP-only access. Generated CSV line endings and SVG trailing whitespace were
normalized for git diff --check; CSV cells and SVG element data were verified
unchanged. The staged whitespace check passed. All operations used MCP docker_exec.

## Larger-batch follow-up (control READY FOR REVIEW; customer comparison pending)

User requested a closer match to 1–2 GiB cached batches and clarification of
objectStreamReset. Same released behavior and unchanged artifacts; diagnostics
remain unreleased. Continue using MCP docker_exec for every sandbox operation,
including workers, and confirm this at checkpoints.

Source check: JavaSerializer.scala:44 increments a counter once per top-level
writeObject and calls ObjectOutputStream.reset after the configured interval.
The reader consumes the reset marker before its next object; it has no separate
configured counter. This clears Java stream references, not task/byte admission.
SerializerManager creates a separate stream for each serialized block. Cache
must be rebuilt to change existing serialized reset markers. No per-thread,
executor-wide or GPU concurrency limit is imposed by objectStreamReset.
For a stream containing three cached records, reset4 and reset100 both retain
stream references through all three reads; neither has a reset before the last
record. Reset1/2 can release references earlier, but Spark/unroll/iterator owners
can still retain those records. Closing a partition stream ends its retention;
a task thread's next partition uses a new stream/counter. Count records in the
Java stream, not downstream GPU decode batches. A read-only worker independently
verified these semantics from Spark source and installed Java17 bytecode, and
confirmed MCP-only operations.

First validate unchanged CacheReloadBench with 64M rows per partition, eight
partitions and 16 random Doubles: 61.035 GiB raw total, 7.629 GiB per partition.
Use local[8], GPU concurrency8, GPU initial allocation32g/max40g, 1 GiB batch
target and 80g heap for an initial full-scan smoke. Capture actual encoded batch
sizes, storage, phase wall time/heap/GC, SQL row metrics and 1-second nvidia-smi
utilization/memory telemetry. Raw evidence and runner: /data/pcbs-oom/large-reset/.
Timeout1200s; unexpected failures park; no overlapping JVMs. Only after validating
this prerequisite compare reset1/100 and consider 2 GiB targets and reduced heap.
Customer context subsequently supplied in chat: attempts19/20/23 used 48g
heap and eight task slots, GPU concurrency unrestricted or capped at two, and
1 GiB batch target; reported average cached partitions were about2.2–2.7GiB.
Earlier four-slot/48g/GPU2 run succeeded. Hardware was T4 16GiB with device
pool13.9GiB, host off-heap limit40g and pinned pool2g; whole cached data4.6–5.6TiB.
These are reported production observations, not locally verified measurements.
The currently running 80g/7.6GiB-partition case is a stress control, not a claim
to match that executor. Subsequent controls should use48g,8slots,GPU2,1GiBtarget
and actual encoded partitions around2–3GiB, then separately test mixed cache
residency and natural GC. Average partition sizes do not bound the largest.
All-disk cache at memory.fraction0.03 and local-mode/explicit-GC caveats remain.
Do not modify production code/config or report the customer comparison as complete.

The initial control completed: /data/pcbs-oom/large-reset/smoke-results.jsonl,
exit0, no OOM/timeout, wall496.27s. Actual largest encoded batch1,068,538,171bytes
(1019.037MiB); eight batches/partition,8,193,265,624encodedbytes/partition,
512Mrows total,65,546,132,640diskbytes,zero cached memory. Four successful raw
reloads and four full SQL scans include one warmup each; each SQL scan reported
512Mrows. Three timed iterations had median raw reload12.110s and SQL28.826s.
Maximum sampled occupied heap across timed SQL scans61.634GiB despite reset1;
this is not live data and does not identify which owner retained it.
SQL GPU telemetry ranged0–80%utilization; memory33068–33072MiB includes the
32GiBpool reservation and is not a measure of actively used device buffers.

The lead verified raw-log events against structured records, sizes, row counts,
phase counts and timing medians with analyze_large.py. An MCP-only read-only
worker independently verified the same evidence and GPU samples. No GPU
process remained after completion. No reset100 comparison was performed in this
control, so it yields no reset overhead claim. Current customer-scale48g,
GPU2,2–3GiBpartition and mixed-residency/natural-GC comparisons remain pending;
the completed 84-JVM sweep and this larger smoke are distinct experiments.

## Six-batch customer-shaped test (IN PROGRESS)

User explicitly requested approximately6GiB per task with eight slots and a
second wave of tasks, matching the customer heap/core/concurrency configuration.
Authorized scope is diagnostic changes and experiments only; production
serializers/config contracts remain released and unchanged. CacheReloadBench
is unreleased diagnostic code. All sandbox operations, including worker tasks,
must use MCP docker_exec; confirm continued compliance at checkpoints.

Use16partitions,local[8],48gheap,ParallelGC with4GCthreads and2ConcGCthreads,
GPUconcurrent2/max2,hostoffheaplimit40g,pinnedpool2g. User corrected GPU memory
to a fixed15GiB pool (allocSize15g/maxAllocSize15g), which supersedes the initial
12g/14g proposal. A6000 with a fixed15GiB pool does not reproduce T4 throughput. Same installed Spark3.5.5 and plugin
artifact reporting3cdf711ded; customer runtime was3.5.3/platform26.04.2.
Java17 and native allocator thresholds131072 are retained. Local driver/executor
share heap; no Kubernetes108GiB process/pod limit is imposed.

Configured batch target remains1GiB. Writer subtracts0.5% for metadata; with16
Double values/row,8,346,664rows fit its per-record budget. Use6times that count:
50,079,984rows/partition,5.970GiB raw values,95.520GiB over16partitions. Require
exactly six measured cached records per partition, recording every row/encoded
byte count; do not infer record count from config. Random Doubles compress poorly,
so this is a large encoded-payload stress case, not the customer's schema.

Extend diagnostic only to allow mixed MEMORY_AND_DISK cache and optional forced
GC; new environment switches default to prior behavior, but do not constitute a
production configuration or released contract. This experiment uses both false,
normal memory.fraction0.6, and emits storage residency after every phase. Preserve
historical benchmark jar/hash in /data/pcbs-oom/sweep; new artifact/scripts/logs/
eventlogs/manifests are under /data/pcbs-oom/customer-six. Historical report source
hash corresponds to gitca8cb52e14 before this diagnostic extension.

Prerequisite calibration: one full-sized partition, one warmup and one measured
reload/SQLscan. Then fresh JVMs at reset100/1 and reset4/2 with16partitions,
one warmup plus three measured rawreloads and full SQL aggregates. Every SQL
execution must independently report all expected scan rows; eventlogs verify
actual task overlap rather than assuming it from configured slots. Track sampled
occupied heap, GC, storage, GPU telemetry and phase wall time. Sequential cache
population is not a benchmark of concurrent producers. Intermediate intervals
have one JVM initially; no production OOM-probability/statistical claims.

Runner requires exact batch shape, exit0/completion/counts/storage/SQLrows for
success. Non-timeout explicit JavaheapOOM duringreload/SQL after verified
population is an expected outcome; population OOM, GPU error, timeout or shape
mismatch parks for investigation. Jobs run sequentially, timeout1800s each,
cleanup only their own scratch cache directories. No weakening shape/scan
validation to pass the experiment. Source compiled independently by worker and
lead against3.5.5.

Calibration1 ran successfully but failed shape acceptance (eight uneven records).
It is preserved as non-accepted calibration evidence, not a measurement of the
six-record scenario. Generate96source partitions then narrow-coalesce to16cache
partitions (pilot6to1) to avoid large projection intermediates changing record
shape. New diagnostic CACHE_SOURCE_PARTS is recorded; root read GpuCoalesceExec
and Spark CoalescedRDD to verify this preserves child batch iteration. Preserve
first source/jar as calibrate-first-*; source/jar hashes in every manifest.

Calibration2 passed in28.07s: exactly6records,each8,346,664rows and1,068,538,171
encodedbytes,50,079,984rows total,exit0,noOOM/timeout. Rawreload and SQL warmup
plus measured scans passed; SQL scans each counted50,079,984rows. Entire pilot
partition stayed in memory, so this validates shape/decoding, not mixed pressure.
Add-Xms48g because Spark's Kubernetes executor entrypoint setsXms=Xmx; with
ParallelGC this produced27.4GiBMemoryStore capacity,matching the supplied UI.
The first pilot's default initial heap gave25.4GiBcapacity and is not a matching
control. Root independently verified raw events/structured records and medians;
read-only worker review requested before the full-run handoff. All operations
continue through MCP docker_exec.

Read-only worker independently verified calibration2 and runner, finding no
material issues. After the user's fixed15GiB instruction, the incomplete12g/14g
full comparison was stopped before any outcome. It is explicitly aborted in
endpoints-aborted.json and excluded from failure/performance counts. Its logs,
events and manifest remain; only its own scratch cache was removed after the
JVM stopped. No old-pool result is mixed into the new comparison.

Calibration15 passed at fixed15GiB in28.07s with exactsix-record shape,exit0,
noOOM/timeout and verifiedSQLrows. Full fixed15GiB endpoint supervisor PID64319
runs reset100 thenreset1, under /data/pcbs-oom/customer-six/endpoints15-*.
Heap-dump-on-OOM startup flags are enabled for full runs; failed-run wall time
may include dump writing and is not a performance measurement. Every job retains
its own raw JVM/GC log, GPU telemetry, eventlog, effective argv/environment and
hash manifest. Reset4/2 intermediate phase remains pending; no overlapping JVMs.

The user approved the full16-column sum query after clarification. Raw cachedRDD
reloads count batch rows before the SQL phase, measuring Java deserialization
without GPU decode. A raw-phase OOM preventsSQL in that JVM; do not claim SQL
query performance or failure location from a JVM that never reached it.
Both fixed-15-GiB raw-reload endpoint runs have now completed with exit 52,
Java heap OOM, no timeout, and accepted six-record shape. Reset100 failed in
readArray -> PartiallyUnrolledIterator; reset1 failed in readArray ->
MemoryStore.putIterator. Each populated16partitions /96records, totaling
102,579,664,416 encoded bytes. Initial cache residency was23.884GiB in memory
and71.651GiB on disk. Neither reachedSQL or completed a timed reload, so there
is no SQL performance comparison from these JVMs.

A metadata-only parser of the reset100 heap dump found eight distinct executor
task-thread stacks in ObjectInputStream.readArray: five through putIterator
and three through PartiallyUnrolledIterator. This establishes overlapping
reader paths, not ownership of retained arrays. Spark event-log outstanding
TaskStart minus TaskEnd counters reached ten because callbacks can lag; that
counter must not be interpreted as ten physically executing tasks.

To exercise the approved SQL query independently, add diagnostic CACHE_RELOAD
(defaulttrue) and run fresh JVMs with it false. It guards only the raw-reload
prefix; cache population, SQL query and row-count assertions are unchanged.
Lead independently compiled the change against Spark3.5.5. Preserve prior
source/jar as customer-six/frozen-*; new source SHA256 is
70b05672362009d39ae32cddf2897227c9451a32847ab3ddd9331fe9f0f207b2,
new jar SHA256 is395f1a2601d2cac253ff46aaa2d40b2a5222360fbc57298a3a8d903df6137e35.
New evidence goes under /data/pcbs-oom/customer-query-only. Run a shape pilot,
then fresh reset100/1 endpoint JVMs with all resource settings unchanged.
Prioritize this query test over raw reset4/2 intermediates; intermediates remain
unperformed. All project operations and delegated reviews remain MCP-only.
The full SQL-only comparison is complete: reset100 andreset1 both exited52
with Java heap OOM on the first SQL scan, accepted shape, no timeout, and no
completed timed SQL read. All experiment JVMs/monitors stopped; GPU returned to
16MiB idle. No performance-overhead or OOM-probability estimate is available
from these four single-JVM pressure cells.

The user explicitly prioritized understanding the heap over further reruns or
fixes. Captured all four heap dumps and analyzed actual field/root ownership
with bounded disk-backed HPROF tooling under /data/pcbs-oom/heap-analysis.
Both raw dumps contain44 arrays of1,068,538,171bytes =43.787GiB payload:
24cached,7unroll,6completed reader records outside unroll/cache,7being filled.
Reset100 retains all20noncached arrays through handle tables;3completed records
are handle-only and3also iterator-held. Reset1 removes that history, but two
unroll holders retain4+3records and6NextIterator.nextValue fields retain previous
records. Its7in-progress arrays remain live in readArray. This is temporary
strong retention at failure, not evidence of a permanent leak.

Reset100 SQL dump independently confirms44large arrays:24cached,10active unroll,
2returned unroll,2handle-only,2previous iterator values,1additional active GPU
decode input,3arrays being filled. Of8task threads,2decode,2wait forGPU,3readArray,
1fails to allocate. The semaphore is acquired after obtaining the CachedBatch,
so GPU concurrency2 does not admit all preceding CPU-cache allocations.

Storage accounting in raw dump reconciles exactly from cached entry sizes,
small broadcast entry and unroll reservation map; reservations are~2.99GiB,
while real noncached large arrays occupy~19.90GiB. Lead read MemoryStore:
store before check, first-record then16-record-period checks, final check only
after whole block, and values.hasNext before keepUnrolling. Lead read
NextIterator: nextValue is not cleared by next(), and is replaced only after
the next deserialization returns. FullGC before reset100 failure leaves only
~99MiB old-gen and556MiB Eden free, neither enough for the next1019MiB array.

Lead independently checked all44primitive headers, PCBS/layout fields, raw
run events/configs/hashes/shape and source; second worker independently verified
raw byte field offsets (including inheritance), strong Java-frame roots and
typed ownership paths in both raw dumps and reset100SQL. Generic reverse BFS
is not a dominator proof; conclusions use direct strong paths only.
All sandbox work and review remained MCP docker_exec-only.
Reset1SQL dump also has44large arrays:24cached,9active unroll,3returned unroll,
4previous iterator values,4being filled; no historical handle-only arrays.
One returned record is also a GPU decode input and is counted once. Independent
review verified all four dumps' direct fields, array headers, inherited layouts,
roots and exact accounting. Lead independently reran SQL typed-path checks,
reset1SQL header/root checks and exact accounting verification.
Detailed findings and limitations: pcbs-six-batch-test.md; compact measured
ownership data: pcbs-six-batch-heap.csv. Diagnosis READY FOR
REVIEW; production fixes remain for architect discussion.

## Follow-up: check unroll accounting every record

The user approved trying spark.storage.unrollMemoryCheckPeriod=1 together with
objectStreamReset=1. Reuse the exact SQL-only six-record pressure scenario:
16partitions,8task slots,48GiB heap,15GiB fixed GPU pool,GPU concurrency2,
normal memory fraction0.6,no harness GC. No source/artifact changes and no
storage-level or user-API changes. Evidence is isolated under
/data/pcbs-oom/unroll-check1; supervisor70450. One fresh JVM requests a warmup
plus3measured SQL scans. Acceptance preserves exact96records and full
801,279,744rows per scan; otherwise classify explicit failure honestly.
Status READY FOR REVIEW. The fresh JVM exited0 with noheapOOM/timeout, accepted
96recordshape, warmup+3SQLscans each801,279,744rows, finite repeatable sums, and
unchanged16partition memory/diskresidency. Measuredscans38.868/39.569/39.165s;
median39.165s. GC10.414/9.651/9.635s (~25%elapsed),11/12/11fullcollections.
Sampledoccupiedpeak44.287GiB includesgarbage; largestpostfullGC40.872GiB.
OnefreshJVMisnotanOOMprobabilityestimate. No performanceoverheadcomparison is
possibleagainstthefailingcheck16case. Rootindependentlyauditedfullraw
events/config/hash/shape/SQLcounts/GC logandreransummarychecks.
A read-only worker independently verified final logs, hashes, exact record/row
counts, effective SparkEnvironmentUpdate settings, GC data and report/CSV;
no material findings. No source, storage-level or user-API changes were made;
all processes stopped.
All project operations/review remain MCP docker_exec-only.

Design steering: serialized/off-heap StorageLevel is not a transparent fix,
because it requires application API changes. Small cache records may be tried
later by adapting the existing splitting path; do not assume a GPU writer can
efficiently emit many independent Parquet buffers. Off-heap ownership remains
future design work, potentially requiring an RDD persistence layer. Spark
patches are last resort. No implementation beyond the approved config
experiment is authorized. Clarified that bounded serialization chunks meant
bytes within one CachedBatch object, not fragments as separate RDD records;
the latter would introduce ordering/predicate concerns and is not proposed.

## Follow-up: unroll-period performance sweep

Status IN PROGRESS. User requested scope verification (including task Java
deserialization), a range of periods, comparison with other passing settings,
and numbers suitable for the issue/Slack. Do not post externally.

Lead and worker verified sole runtime lookup of UNROLL_MEMORY_CHECK_PERIOD in
MemoryStore.putIterator. It governs both putIteratorAsValues and
putIteratorAsBytes, not Executor task-envelope or ResultTask/ShuffleMapTask
closure deserialization. Normal task execution can subsequently access cached
blocks, and changes to cache behavior/GC can affect application performance
indirectly. Spark3.5.3/3.5.5 share this code. No production changes.

Screening evidence: /data/pcbs-oom/unroll-sweep; original supervisor71952 was
stopped. Current approved screen supervisor74128 runs screen-approved.
Fresh JVMs retain exact same95.535GiBencoded cache,16partitions,sixrecords each,
48gheap/local8,GPU2fixed15g,host40g,pinned2g,ParallelGC4,no harness GC.
Source/benchmark/plugin hashes unchanged. Main periods1/2/4/8/16,reset1,
memoryfraction0.6. Reset comparisons use period1 with reset2/4/100 (reset1 is in main sweep).
Reset2/4 are queued under reset-range-jobs.json after the resumed main screen.
Memory fraction is fixed0.6 for all
accepted cells. Original interleaved plan in screen-jobs.json included an
unauthorized fraction0.4 comparison; see scope correction below. Remaining
approved cells are in screen-approved-jobs.json.
Periods8/16 have identical check points for six records: first and final;
period4 checks records1/5,period2 checks1/3/5,period1 everyrecord.

One warmup plus3timed full SQL scans per fresh JVM. Screen then repeat each
passing setting twice in fresh JVMs, shuffled per round with fixed seed16362.
Aggregate medians of per-JVM medians; scans within one JVM are correlated.
Retain failures in later repeats, do not silently select only passing runs.
Absolute read time includes cache/disk/GC effects from the approved period/reset
changes; no memory-fraction comparison is authorized in this sweep.

Acceptance: exact16unique cachepartitions/96records/measured batchsizes,
801,279,744rows in everySQLscan, finite repeatable sums,all5storagecheckpoints,
exit0+complete or explicit postpopulation JavaheapOOM. Park unexpected errors,
population OOM, wrongshape,timeoutorinsufficientresources. Diagnostic heap dumps
disabled for this sweep because four original dumps already establish the path;
preserve JVM/GC/eventlogs,telemetry,commands,hashes,exitcodes. Timeout1800s/cell,
scratchfreeguard140GiB; only one JVM at a time,remove only own scratchcache.
MCP docker_exec-only for ALL work/reviews; repeat at handoffs/checkpoints.
Existing released serializer/config contracts remain unchanged. Results and
ready-to-paste report pending; no claims of statistical OOM probability.

Material scope mistake/correction: lead interpreted "other settings" as allowing
a memory-fraction comparison without checking with the user. User rejected that
expansion and explicitly requested the non-memory-fraction sweep first.
Stopped supervisor71952 and its fraction0.4 JVM73541 before a read outcome;
metadata aborted-memory-fraction.json marks it ABORTED_EXCLUDED. Preserve its
logs for audit, exclude from all result/timing counts. Only its scratch cache
was removed after the JVM stopped. Completed fraction0.6 cells remain valid:
period1/reset1PASS; period4/reset1heapOOM. Resume remaining periods2/8/16reset1
and period1/reset100 under screen-approved. Runner now asserts fraction==0.6
and sets the Spark property literally to0.6. No other parameter experiments
without prior discussion. Worker notified; MCP-only requirement reaffirmed.

Checkpoint: approved p1/reset1 and p2/reset1 both passed warmup+3scans; medians
37.383596457s and39.628477960s, measured GC24.703% and28.953%. p4/reset1 failed
JavaheapOOM on firstSQLscan. Allfraction0.6, exact96records, totalencoded
102,579,664,416bytes, same23.884GiBmemory/71.651GiBdisk. Rootanalysis and
independentworker verified raw events, exact scan/storage iteration IDs,
SparkEnvironmentUpdate effective settings/JVMflags, source/jar/pluginhashes.
These are single screening JVMs, not final performance estimates. Current
p1/reset100 screen follows; p8/p16reset1 remain pending. reset2/4 atperiod1
queued for the same two-parameter sweep; no resource or other settings vary.

Root independently read Executor task deserialization, ResultTask and
ShuffleMapTask closure deserialization. None consults the unroll period.
Root sha256 comparison confirms upstreamSpark3.5.3/3.5.5MemoryStore files
identical (57cf599b18d239c079cea63c8a39b3cebafb39dc67e2988e93758a74a4269e8a).
SizeTracker.estimateSize is O(1); its object-size sampling happens on updates
independently of this period. Extra checks can nevertheless change reservations,
cache behavior and GC; this sweep measures end-to-end reads, not isolated
per-check CPU cost. All project operations/reviews remained MCP-only.

Main screening complete: period1/reset1 and period2/reset1 PASS; periods4/8/16
with reset1, and period1/reset100, all first-SQL heapOOM after accepted population.
Six accepted JVMs independently audited; no timing is assigned to failures.
All accepted cases fraction0.6, same cache residency/shape/resource settings.
Root reran full analysis against raw logs and effective event settings.
After confirming no active JVM, reset-range supervisor76742 started period1
with reset2 then4. Passing settings still require two additional fresh JVMs
each; generate randomized round-robin repeats only after reset screening.
The aborted memory-fraction case is excluded and never retried. No production
changes or external posting. Full sweep/report remains IN PROGRESS.

Screening checkpoint: all8 approved cells complete. At period1, reset1 and2
PASS; reset4 and100 heapOOM in firstSQLscan. period2/reset1PASS;
periods4/8/16reset1 heapOOM. Root reran accepted-case validation against
raw artifacts and effective settings. Repeats supervisor78119 started after
screening ended: two fresh JVMs for each of the three initially passing
settings, deterministic shuffled round-robin order. All fraction0.6; no
resource changes. Worker independently audits using MCP docker_exec only.
Report/performance acceptance remains IN PROGRESS until repeats and review.

Final fixed-memory sweep checkpoint — READY FOR REVIEW:
All 14 accepted JVMs completed: nine PASS and five first-SQL heap OOMs.
Period/reset 1/1, 1/2 and 2/1 each passed three fresh JVMs. Median of JVM
scan medians: 37.459692076s, 37.763941794s and 42.298277415s respectively.
Median timed GC: 24.703%, 23.898%, 33.276%; maximum post-full-GC whole-heap
occupancy: 41.867, 41.867, 42.862 GiB. Periods 4/8/16 with reset1 and
period1 with resets4/100 failed on first SQL warmup, each one screening JVM.
No default-period successful timing exists; do not claim generic check
overhead. Reset1/reset2 ranges overlap; no clear reset2 performance win.
Three JVMs do not establish production reliability or an OOM probability.

Root reran analysis against all raw evidence and independently executed the
worker's separate raw/config/GC/math checks. Both workers reviewed report
interpretation; final size-check wording corrected to be conditional.
No production or benchmark changes. Memory fraction 0.6 for every accepted
case; aborted 0.4 case excluded. GPU compute processes cleared after final
JVM exit. All project operations and reviews remained MCP docker_exec-only.
No further experiments or external posting. Final report and all 14 case
rows: [sweep report](pcbs-unroll-period-sweep.md) and
[CSV](pcbs-unroll-period-sweep.csv). Architect acceptance remains pending.
The earlier IN PROGRESS checkpoints above are superseded by this result.

## Batch-size performance sweep (IN PROGRESS)

User authorized 64, 128, 256, 512 MiB and 1 GiB batch targets, comparing cache
build time and cache read time on the same large workload. Two explicit
profiles: default unroll16/reset100 and frequent-check unroll1/reset1.
No other tuning knobs vary. Memory fraction stays 0.6, heap48g, local[8],
GPU concurrency2/max2, fixed15g GPU pool, host40g/pinned2g; same plugin,
benchmark source/jar and 801,279,744 rows/16 Double columns. Sixteen cached
partitions from96source partitions preserve the input across targets.
Actual encoded size/counts are measured, not inferred from the target.

Released PCBS/configuration behavior is preserved without production changes.
Existing standalone diagnostic remains UNRELEASED; adapt its external runner
and validation rather than replace the Scala benchmark. MCP docker_exec-only
for every operation and worker; confirm at handoffs/checkpoints. All prior
experiments/evidence remain immutable and unrelated files remain untouched.

Population stays sequential per partition for direct comparison with the
previous sweep. It measures generation+encoding+cache storage+job overhead,
not parallel customer cache-build throughput. Read timing is a fresh all-column
SQL sum, one warmup plus three timed full scans per fresh JVM, natural GC.
Record fill time even when a subsequent read OOMs; no read-time estimate for
failed JVMs. Start with all ten profile/size cells, then two fresh repeats per
initially passing cell in shuffled rounds, keeping any later failures.
The64MiB case may have six tiny8row tails from existing source partitioning:
expected102records/partition versus48/24/12/6 at larger targets. Do not change
the dataset to hide that rounding; audit actual batch rows and bytes.

Evidence/commands/logs/GC/telemetry and manifest under
/data/pcbs-oom/batch-size-sweep. Runner timeout1800s/cell, free disk140GiB
guard, one JVM at a time, cleanup only its own cache after exit. OOM heap dumps
remain disabled; previous failure dumps retained. Park unexpected errors,
wrong shape/row counts, population OOM or unavailable resources. No production
fix, new configuration definitions or external posting. Smoke smallest target
before full sweep, independently validate effective configs/artifact hashes,
all16population partitions, exact scan rows and five storage checkpoints.
Deliver build/read medians, GC/heap, measured batch sizes, retained OOMs and
limitations; independently review and commit tracker/report together.
Prior1GiB fill medians were249.370s at1/1,250.435s at1/2 and249.901s at2/1.

Batch sweep smoke checkpoint: default64MiB built all16cache partitions in
259.472247414s (6.744% GC), then first SQL scan heapOOM exit52. Actual
102records/partition including six8row tails; full records66,784,684bytes.
Total encoded102,581,534,784bytes; initial23.884GiBmemory/71.652GiBdisk.
Root analyzer and independently written worker checker passed raw effective
configs/env/shape/scan/storage/artifact checks. Root read and executed checker.
All source/jar/plugin hashes match preceding unroll sweep. No PCBS changes.
Remaining nine shuffled screening cells started supervisor84071 only after
smoke JVM ended. Retain negative default64 result; no read timing assigned.
Only current external runner/analyzer adapted; smoke validates actual splits.
MCP docker_exec-only reaffirmed for root and worker; no resource knobs changed.

Discussion checkpoint: user requested back-of-envelope memory math before
continuing. Supervisor84071 SIGSTOP verifiedTs; current512/default JVM84081
allowed tofinish, no subsequent queued launches. Worker instructed hold,
MCP-only confirmed. discussion-pause.json records resume/timing boundary.
Current phase timings remain usable; supervisor wall_s includes pause and
must not be compared. Resume queued work after discussion indicates continue.
No PCBS experiment started. Default64 failed read despite successful fill.

User follow-up: current sweep changes only global batchSizeBytes, not PCBS
code. Finish it first. User wants a subsequent PCBS-only splitting experiment
only if the smaller-target approach works without crashes. Keep global GPU
batch target at1GiB for that comparison; do not mix it into current results.
That conditional follow-up has not started; evaluate this sweep before choosing
the minimal implementation/test boundary. No new user-facing config authorized.

## Default-settings small-batch follow-up (IN PROGRESS)

User explicitly rejected reset1-controlled comparisons: objective is to make
cache reads fit with default unroll16/reset100 by shrinking batches. Supersedes
unstarted two-profile grid/repeats above. Test32MiB and16MiB; try8MiB only if
16MiB fails. Same48gheap/local8/GPU2/15gpool/host40g/pinned2g/fraction0.6,
same801,279,744rows/16Doublecolumns/16cachepartitions/96sourcepartitions.
No reset, unroll, fraction, storage or production changes. Record fill/read
performance, exact measured records and heap/GC. Repeat passing candidate(s)
in two additional fresh JVMs before interpreting timing/stability.

Back-of-envelope: about24GiB existing cache plus8*100*batchSize gives49GiB
at32MiB,36.5GiB at16MiB,30.25GiB at8MiB, before other heap/GC headroom.
This is an exposure estimate, not a measuredpeak or guaranteed bound.
Stream handles/unroll/iterator aliases to the same arrays are not additive.

Paused512/default child finished with kernel exit52 (13312 waitstatus),
all16partitions populated in256.590222328s, then firstSQLheapOOM. Root saved
/proc exit evidence, recovered one validated record with wall_s=None (discussion
pause contaminates supervisorwall), and cleaned only its scratchcache. Initial
recovery helper wrote record then failed formattingNone; follow-up verified
the intactrecord and completedcleanup. Terminated stopped84071 supervisor and
its84080monitor without launching any remaining jobs. Verified both andJava
gone andGPUempty. Old unstarted manifestjobs remain preserved but unexecuted.

At targets below64MiB,GpuRange target/8 also splits original8,346,664row
sourcepartitions. Root read GpuRange/GpuProject/GpuCoalesce andPCBSsplit code;
worker independently traced. Exact nested predicted batchrows: split source
into Rangechunks(target/8), thenPCBSchunks(floor(target*.995/128)), repeat
six sources/cachepartition. Expected records198/402/810 at32/16/8MiB. Actual
plan/rows/bytes must validate before accepting results. Benchmarksource/jar
andpluginunchanged; only externalrunner/shapevalidation adapted. Data shape
anddistribution unchanged; byte-identical randomvalues across different batch
boundaries are not claimed. Source/jar/pluginhashes must matchprior manifest.

MCP docker_exec-only for all operations and worker handoffs/checkpoints.
Acceptance remains exact16populationpartitions, all801,279,744 scanrows,
onewarmup+3timedSQLscans, finite repeatableaggregates, five storagecheckpoints,
effectiveconfigs/artifacthashes/exitcodes/rawlogs. Keep expectedSQLheapOOMs;
park populationOOM, unexpectederror,timeout orwrongshape. No new heapdumps.
PCBS-only code experiment remains conditional and has not started.
Reportcurrent32/16(and8ifneeded)results alongside prior64/512 failures and
the previous1GiB1/1 baseline, clearly distinguishing configurations.

Ordering correction: user wants small first, then work upward;32MiB remains
authorized after a smaller success. Lead unnecessarily launched32 first.
Stopped supervisor85141/Java85144 andmonitor85143; verified allgone/GPUempty
before cleanup of only that privatecache. small32-stopped.json marks
ABORTED_EXCLUDED, no result. Retain logs/manifest, do not classify asOOM.
Start16MiB/default now; ifPASS test32 next, ifFAIL test8. Repeats of a passing
candidate still required for performance comparison. No old matrix resumes.
MCP-only confirmed; conditionalPCBSfollow-up remains unstarted.

16MiB first completion: default16/100PASS exit0, fill271.168204198s,
median timedread44.989283228s, fillGC2.510%, readGC9.270%. Four exact
801,279,744row scans;402records/partition, full16,697,307bytearrays.
Total102,589,204,992encodedbytes; maxsampledoccupied41.509GiB,
maxpostFullGC31.083GiB (occupancy, not dominatedretainedsize).
Root analyzer+separateworkerchecker both passed all3completedcases after
root read actualshape/effectiveconfig checks. Started32upward supervisor86529
only after16JVM completed. 8MiB not needed at this stage. 16MiB still requires
twofresh repeats; no reliability claim from oneJVM. ConditionalPCBS experiment
still unstarted. Allprojectoperations remainedMCPdocker_exec-only.

32MiB upward first completion: default16/100PASS exit0, fill264.049571285s,
medianread42.894320794s, fillGC3.720%, readGC15.039%. Four exactfullscans,
198records/partition, full33,393,244bytearrays,102,584,285,760encodedbytes.
Maxsampledoccupied41.927GiB, maxpostFullGC34.417GiB. This disproves any
deterministic prediction that32mustfail: earlier49GiB arithmetic was possible
retention exposure before overhead, not simultaneous observed ownership.
Do not call it a guaranteed bound; task/reset timing can avoid that peak.

Root reran analyzer and independentworkerrawchecker (fouracceptedJVMs).
Default repeat supervisor87672 started after32JVMexit:16trial1,32trial1,
16trial2,32trial2, all defaults/fraction0.6. Initialpasses selectcandidates;
laterfailures remaincounted. 8MiB not needed unless16repeatfails. No old
matrixlaunch, noreset1experiments. ConditionalPCBSsourceexperiment still
parked untilrepeatedsmallpasses. Read-onlypreparation identified GPU budget
helper as minimalcap point; no newconfig, no change toCPUwriter. It does not
bound eageroutputList/rowmaterialization orcoalesceGPUreads. Preserve those
limits in eventualcomparison. MCPdocker_exec-only reaffirmed.

Global batch-size comparison READY FOR REVIEW:
Eight accepted JVMs:16MiB3/3PASS,32MiB3/3PASS,64MiB0/1 and512MiB0/1
readheapOOM. Abortedfirst32excluded, oldunstartedmatrixcancelled,8notneeded.
All currentcases defaultunroll16/reset100, fraction0.6 andsamefixedresources.
Final medians:16fill271.168204198s/read44.989283228s/GC9.269873316%;
32fill264.037786516s/read42.894320794s/GC15.039396887%.
MaxpostFullGC31.129/35.006GiB, occupiedheap notdominatorretainedsize.
Prior1GiB1/1 reference fill249.370s/read37.460s changesbothmemorychecks
andbatchtarget; do notlabeldifference isolatedbatch-sizeoverhead.
Root reran analyzer+separateworkerchecker onall8cases; rawGCwindows,
effectiveconfigs/source+jar+pluginhashes/exactrows+sizes/storage verified.
Worker reviewedreportmethodclaims. GPUemptyandallbenchmarkJVMsexited.
No production orScala benchmarkchanges. AllprojectworkMCPdocker_exec-only.
[Report](pcbs-batch-size-sweep.md), [caseCSV](pcbs-batch-size-sweep.csv).
The syntheticaggregate, warmOSpagecache, sequentialfill andA6000 limitations
are explicit. Earlier IN PROGRESS checkpoints superseded for this comparison.

ConditionalPCBS-only experiment now eligible: user authorized it after
smallerbatches workwithoutcrashes. Compare16MiB cached-record budget with
globalGPUbatchtarget1GiB, defaultchecks andunchangedresources/data. Start
small, then32MiB if16passes. Use isolatedsourceworktree andseparateartifacts,
retainbaselineartifacthash, adapt existingGPUbudgethelper only; no new
user-facingconfig, lifecycle/format/API changes, CPUwriter orGPUoperators.
This is an intentionally different experimentalGPUcachelayout, not a selected
productionpolicy or compatibilitypromise. Existingreleasedrepresentation,
schema/values/order, empty/count-only handling andCPUfallbackcontracts remain.
Read-onlydiscovery/rootreads verifiedcolumnar+rowGPUcallers andGPUdecode.
EagerListproduction, whole-rowpartitionmaterialization andone-fileGPUdecode
remain; smallerrecords alone do not guarantee largeGPUprocessingbatches.
Verifyentry-point roundtrip andactualsplitlayout, build/sourceprovenance,
targetedexistingtests andsamefullbenchmark before performanceclaims. No
PCBSimplementation started before this globalcomparison completed.

PCBS-only implementation checkpoint IN PROGRESS:
Globalreportcommitted971abb7d4c. Isolatedworktree
/data/pcbs-oom/pcbs-split-experiment, branchpcbs-cache-size-experiment.
Prototype16MiB source+tracker commitba6d57a9ab5e349fe53a5089f4c3449268d713b6:
onlygetGpuBytesAllowedPerBatch cap, same0.5%metadata reserve, smallerglobal
targets and2GiBsafetyguard preserved. RootreaddecisiveGPU/row/CPU/decode
callers; workerfocusedsource+trackerreview foundnoblockingissue. NoCPU/API/
ownership/config changes. Currentuserbranchsource remainsunchanged.

Build supervisor90979, evidence/scripts /data/pcbs-oom/pcbs-only-sweep,
sourceworktree build usesrapids-plugin-mvn package -pldist -am -Dbuildver355
-DskipTests andisolatedsecondaryCacheDir. Testsnotyetclaimed; buildartifact
willcopytoimmutable rapids-cap16-cuda12.jar withsource/revision/SHA metadata.
No builds concurrentwithtimedbenchmarks. Next: targetedCachedBatchWriterSuite,
known-valuepublicGPUrow+columnarentryroundtrip, fullsameoriginaldata3JVMs
atglobal1GiB/default16/100. Actualexpectedlayout390records/cachepartition
(64full130416rows+40tail peroriginalsource), notglobal16's402. Preserve
allrawconfigs/source+artifacthashes/metrics/failures; stopfailedprototype
beforeanylargercap. Onlyafter16success try32. MCPdocker_exec-only required
onresume/handoff/checkpoint. Sourceexperiment remainsINPROGRESS.

PCBS-only checkpoint: isolated16MiB build PASSED (383.661s), artifact
/data/pcbs-oom/pcbs-only-sweep/rapids-cap16-cuda12.jar SHA256
ba863f88d8470866bf1d55038e1d941faa5fe565b60d7bf3aae83066d0c2d83d.
Known-value publicGPU row+columnar entry roundtrip PASSED: each1.2Mrows,
3records, everyfield/roworder checked, effectiveglobal1GiB/default16/100.
Worker independently checked rawmarkers/effectiveconfigs/artifactprovenance;
lead read source, script and successful result.

Targetedunit initialtest lifecycle failedbeforetests due missing spark355
classified reactor dependency; valid=false/zeroXML. Preserved allrawfiles as
unit-cap16-dependency-failure*, with relocationmetadata unit-cap16-retry.json.
Root+worker verified parentPOM attachesclassifier atpackage. Retrying same
CachedBatchWriterSuite selector throughpackage -pltests -am, no skips orPOM
changes; supervisor96226. Acceptance requires positive executedXMLtestcount,
zeroerrors/failures, rawsuccess andbefore/after sourceHEAD/SHA tiedtobuild.
Timedbenchmark notstarted whilechecks outstanding. Stopanyfailedprototype
beforelargercap. MCPdocker_exec-only confirmed atcheckpoint.

PCBS-only16MiB performance checkpoint READY FOR REVIEW:
Three of three fresh JVMs passed; median fill273.718999901s, median read
45.197849639s, median readGC8.8002816008%, maxpostFullGC31.14453125GiB.
Actual390records/partition, largest16,697,307bytes, encoded102,588,907,584bytes.
All four scans/JVM returned exact801,279,744rows. Targeted17tests and both
known-value writerentry checks passed. Root reran analysis+rawchecker and
worker independently audited evidence. Prototype verificationcommit97cd111bae.

User-authorized upward32MiB follow-up now IN PROGRESS, prototype source
b65451d391: only16->32 budgetconstant plustracker; global1GiB/default16/100,
fraction0.6/resources/data unchanged. Frozenpcbs-source-cap32.scala; build
supervisor100154, no concurrentbenchmark. Entry/unit checks mustpassbefore
three fullJVMs. Stopfailedprototype; no64MiBprototype scheduled.
ExistingCPU/API/format/ownership contracts preserved; isolatedchangedlayout
is not a productiondefaultdecision. EageroutputList/whole-rowmaterialization
remain. Actualscanplan has no coalescer beforepartialaggregate. MCPdocker_exec
only confirmed and required onresume/delegation/checkpoints.

PCBS-only32MiB prerequisite checkpoint:
Build passed152.943s, artifactSHA256
ab3167db471c90a97068ee3ca1923d028e941e74edfe324a75c99de2ca0efaf7.
BothpublicGPUwriterentryroundtrips passed1.2Mknownorderedrows/tworecords.
TargetedCachedBatchWriterSuite passed17tests/zeroerrors/failures/skips;
freshXMLmtime/testcasecount andbefore/afterHEAD/sourceSHA validated.
Root+worker independently verified entry/unit evidence. Full benchmark
supervisor105278 now runs3JVMs, stoponfailure, no build/test concurrency.
Source+artifact andallrawlogs retained inpcbs-only-sweep. MCP-onlyconfirmed.

## PCBS-only split-budget experiment result

Status: READY FOR REVIEW; only the architect marks DONE.
Both 16 and 32 MiB variants passed three fresh JVMs, each with one warmup
and three timed full-data reads. Global GPU target remained1GiB; defaults
unroll16/reset100 and fraction0.6/resources/data stayed fixed.

| PCBS target | Median fill s | Median read s | Read GC % | Max post-full-GC GiB |
|---|---:|---:|---:|---:|
|16MiB|273.718999901|45.197849639|8.8002816008|31.14453125|
|32MiB|262.728192850|42.963548705|16.0298599809|35.15234375|

32MiB source b65451d39134eb9f6475c5ae16fb437534d7b760; sourceSHA
b3c11561bbe40bb8fd78f64e308cb864d1fe1eda09214776d777b07495cb5c32;
artifactSHA ab3167db471c90a97068ee3ca1923d028e941e74edfe324a75c99de2ca0efaf7.
All 32MiB partitions had198records, largest33,393,244bytes; total encoded
102,584,305,728bytes. Both variants passed CachedBatchWriterSuite17/17 and
both public GPU writer entry round trips with1.2M known ordered rows.
No OOM, timeout or failed check among the six accepted benchmark JVMs.
Initial unit dependency-resolution failure remains preserved separately.

Lead reran analysis and independent raw checker for all6cases, inspected
source and correctness checks, and reviewed report/CSV calculations. Worker
independently audited raw evidence and substantive source/report changes.
Report: docs/dev/pcbs-split-budget-sweep.md, aggregateCSV and per-JVM casesCSV.
Raw evidence/scripts/artifacts: /data/pcbs-oom/pcbs-only-sweep.
Signed-off source changes remain isolated on pcbs-cache-size-experiment;
no production default or fix selected. Main workspace plugin/source baseline
hashes stayed fixed. All benchmark children and GPU processes exited.

For each size, the two approaches have overlapping read-time ranges in this
synthetic workload; fill medians differ by less than1%.
PCBS-only sizing retains the global GPU target but scan still feeds small
decoded batches directly into partial aggregation. Eager output-list retention,
whole-row materialization and estimated rather than strict encoded-byte sizing
remain. Fill is sequential; page-cache/A6000/synthetic-schema limitations and
released CPU/API/format/ownership boundaries are recorded in the report.
No8MiB test was needed; no larger PCBS-only cap is scheduled.
MCP docker_exec-only confirmed throughout; required on every future resume,
checkpoint and worker handoff. User/architect acceptance remains pending.

## Whole-RDD cache ownership investigation

READY FOR REVIEW: source investigation, not an implemented ownership design.
User clarified the owner must be the entire cached RDD; data must survive
memory-reference eviction and remain reloadable. Initial investigation focused
too heavily on AutoCloseable. That hook describes memory-object cleanup, not
the requested payload lifetime; the corrected record centers the whole RDD.

Verified: explicit unpersist and ContextCleaner GC cleanup both reach
SparkContext.unpersistRDD and post SparkListenerUnpersistRDD(rddId).
Memory eviction does not post that event. A driver SparkListener can observe
whole-RDD retirement; an ExecutorPlugin cannot directly receive the event or
BlockManager's RemoveRdd RPC. Executor polling through PluginContext.ask can
relay driver state; push needs a separate transport/internal RPC path.
Existing event delivery is asynchronous and can drop on queue overflow.

Spark persists a mapPartitions statistics wrapper after PCBS returns its RDD.
Payload ownership must map to that persisted wrapper's ID, not blindly to the
serializer-output ID. GPU scan exposes the actual cached RDD; CPU/filter/AQE
population paths and race-free registration still need a design. Driver
getPersistentRDDs is logical registration/reconciliation input, while storage
stats and block locations report physical copies and may be stale.

Cross-executor reads, replicas and migration serialize Spark cache blocks.
A token-only payload needs independent remote lookup/transfer or self-contained
transport bytes. Retiring an RDD needs active/in-flight read protection,
generation handling, late-registration handling and executor coordination.
Keeping payloads until whole-RDD retirement can retain otherwise unreachable
payloads longer; it avoids relying on per-batch serialized-byte destruction.

Evidence and open questions: docs/dev/pcbs-rdd-cache-ownership.md. Lead read
decisive source; independent workers traced cleanup and remote paths and
reviewed the report. Released Spark3.5.3/3.5.5 persistence and existingPCBS
contracts remain unchanged. Proposed protocols are unimplemented; no runtime
prototype or new benchmark claimed. MCP docker_exec-only confirmed throughout
and required at every resume/checkpoint/delegation. No source/config changes.

## Existing RDD and BlockManager mechanism trace

READY FOR REVIEW: user requested identity/mapping/synchronization mechanics
before extension-point or ownership-design proposals. Earlier driver-listener
suggestions are exploratory, not a selected architecture. This follow-up
documents the existing flow only.

Coordinated independent tasks: spark_cache_trace traced identity and task
serialization; heap_review traced registration, reports, driver maps, local
disk metadata, removal and recovery. Full notes persist under
/data/pcbs-oom/ownership-research/cleanup/identity-flow.md and
/data/pcbs-oom/ownership-research/remote/registration-removal-mechanics.md.
pcbs_only_plan assembled docs/dev/pcbs-rdd-block-manager-flow.md with a
mapping table and sequence diagram. A separate focused review found no
material findings. Lead independently read the decisive source and report.

Verified mechanism: driver allocates RDD.id during construction, persist
registers that existing ID, and task serialization carries it to executors.
RDD.getOrCompute constructs RDDBlockId(rddId,partition.index). Local
BlockInfoManager metadata/write lock precedes producing/storing partition
records. Each partition block can contain many PCBS batches.

Executor BlockManager registration supplies its identity and storage RPC
endpoint. Successful local puts synchronously report UpdateBlockInfo; the
driver records per-manager status and BlockId-to-locations. This physical
directory is separate from persistentRdds. The driver does not broadcast the
entire location directory to all executors.

Whole-RDD RemoveRdd clears matching driver metadata and fans out to ALL
registered BlockManagers. Each scans local BlockInfoManager keys for the RDD
and removes matching memory, disk and metadata entries. Disk-backed eviction
keeps that local metadata; filenames derive from block names, so deletion
requires no PCBS deserialization. Replies follow local removal-body execution;
blocking waits aggregate futures, subject to documented failure handling.
Updates/re-registration/executor loss maintain the directory; they are not a
global transaction or a guarantee that every executor has identical state.

Source evidence is upstream release tag v3.5.5, with relevant v3.5.3 identity
paths compared by workers. Released persistence/PCBS contracts unchanged;
the new report is investigation documentation. No implementation, extension,
new configuration, runtime RPC trace or failure-injection result claimed.
MCP docker_exec-only confirmed by every worker and lead, and required at all
future resume/handoff/checkpoints. Only documentation/tracker files changed.

## BlockManager replacement feasibility

Source-only answer to follow-up: BlockManager and removeRdd are nonfinal
Spark-internal classes/methods. SparkEnv.scala:365 directly constructs
new BlockManager and :68 stores it as a val; no replacement factory/config
is consulted. Executor.scala:151 initializes/registers it before plugins
at :335–337. Standard ExecutorPlugin initialization cannot select a replacement.

If SparkEnv construction supplied a subclass, the inherited storage endpoint
would dispatch existing RemoveRdd commands to its override: BlockManager
:240–242 constructs the endpoint with this; storage endpoint:51–53 calls
virtual removeRdd. This does not require replacing the driver master.
A Spark construction-path change would therefore allow a focused subclass
rather than a complete block-manager rewrite. This is feasibility reasoning,
not a selected or implemented design; runtime safety remains unverified.

Lead independently read construction/field/class declarations and previously
verified RemoveRdd dispatch; worker checked construction order, virtual dispatch
and matching3.5.3 code. Released contracts unchanged. MCP docker_exec-only
confirmed; no source/config/runtime changes. Investigation READY FOR REVIEW.

## Shadow payload-manager discussion checkpoint

User's proposed separation is coherent in principle: Spark owns metadata
partition blocks; a plugin store owns spillable payloads, local/remote lookup
and lifetime until whole-RDD retirement. No runtime design has been implemented.
Worker reviewed this proposal against the previously verified source; lead
checked its claims against the decisive code already read.

Replacement is difficult because construction is hardcoded and precedes plugin
initialization. Directly subscribing to existing BlockManager RPCs also lacks
a plugin API; this is a separate limitation from startup ordering. Driver
onUnpersistRDD is an observation after removal request initiation/completion
according to blocking mode, not interception of the executor RemoveRdd RPC.

Unresolved: actual persisted wrapper RDD association; separate reader/transfer
leases; late registrations and cleanup reconciliation; owner lookup/recovery
when a payload executor disappears but Spark metadata remains. Metadata replica
creation does not replicate plugin payloads automatically. Listener delivery
alone is not a reliable cleanup acknowledgment. A Spark-change factory/lifecycle
hook remains an explicitly labeled LAST RESORT alongside unmodified-Spark
options, per the priority recorded at the top. No production option selected.

MCP docker_exec-only confirmed by lead/worker. Documentation-only checkpoint;
released contracts unchanged, proposed architecture unimplemented/unreleased.
READY FOR REVIEW; no implementation or runtime verification claimed.

## Review and handoff

A read-only worker reviewed the diagnostic and verified8/large2 logs, confirmed
MCP-only sandbox use, and independently confirmed disk population, matching stack,
concurrency and successful control. The lead read the decisive source and checked
all run logs. The final diagnostic ran successfully in small64reset1 and produced
the expected OOM in verified8 and small64. Staged whitespace checks passed.

Caveats: the script prints but does not enforce the serializer/disk population;
check those markers before accepting a new run. Compilation errors before the
try block can bypass the explicit exit handling. Require both a successful
process exit and PCBS_SUCCESS for a successful reproduction control. The initial
launcher saved logs and argv but did not persist process exit codes; historical
outcomes above are classified from the explicit success marker or OOM stack,
not from exit status. Run new experiments with an external timeout as well.

A discovery worker initially received an erroneous possible-cybersecurity-risk
rejection while reading ordinary Spark memory-management code. A narrower
read-only continuation succeeded. No code or workload was blocked in the main
investigation; no alternate execution environment was used.

The diagnostic, standalone benchmark, this tracker and sweep report/data are
included in signed-off commits. The sweep adds repeated full SQL aggregate
measurements; broader application performance and statistical confidence bounds
remain unverified. Production fixes, customer-specific sizing, serialized-memory
controls and off-heap ownership remain for architect discussion.
This task is READY FOR REVIEW, not DONE.
