# Vortex cache exploration

Status: READY FOR REVIEW. All 24 primary cases passed with the user-selected
16 GiB JVM heap, 4 GiB RAPIDS-managed host cap and 32 GiB sampled RSS guard.
Both GPU-enabled and CPU-only population are measured at 256/512/1024/2048 MiB
logical data per task, four partitions/two task slots. Final report:
vortex-cache-results.md; machine-readable data: vortex-cache-results.csv.
Earlier 1 GiB-heap/RSS4GiB runs are the separate constrained-budget appendix,
not the normal-host baseline. Historical report references below predate that
move. Only the architect marks work DONE. Updated 2026-10-27.
AI-assisted work; no production implementation changes or PR publication.

Profiling follow-up: READY FOR REVIEW; see vortex-cache-profiling.md. One original-
artifact 256 MiB/task diagnostic reproduced 14.095 s full GPU reads. Independent
SQLite verification found 7,936 pinned allocations/frees inside from_arrow, taking
20.594 summed thread-seconds; host copy APIs took another 15.455 seconds. Whole-
recording GPU H2D activity was 0.173 seconds. Thread sums overlap and include both
full reads; API-category nesting remains unverified. Focused independent review
checked the report against saved trace evidence; git diff --check passed. The prior transfer label was too broad: current GPU latency includes
expensive Arrow import overhead. No baseline numbers or code changed. A streaming/
batched-reader experiment remains proposed, not verified. Existing release and
compatibility classifications below still apply. All operations and profiling used
MCP docker_exec; no machine-exhaustion testing or additional scale jobs performed.

API follow-up: source plus runtime javap inspection found existing pinned-pool
sharing and direct buffer upload APIs. Proposed first control is a modest shared
pinned pool in the same small diagnostic; no new run or implementation performed.
GpuDeviceManager disables cuDF's default pool and RAPIDS pool defaults to zero,
which supplies a concrete alternative explanation to test before restructuring
the bridge. Direct HostColumnVector.copyToDevice / device-buffer constructors
accept known null counts; ColumnVector.fromArrow avoids IPC but still enters the
native Arrow importer in mounted source. See profiling report for ownership/type
limits and native revision caveat. Released allocator defaults remain untouched;
a configuration-only speedup would not establish out-of-box acceptance. All work
used MCP docker_exec; delegated API discovery was independently checked.

## Bridge controls and 2 GiB matrix READY FOR REVIEW; copy follow-up IN PROGRESS

Pinned-pool matrix COMPLETE, READY FOR REVIEW: 12 attempted outcomes independently
verified, 11 PASS plus Default at 2048 MiB per task failed during GPU-to-row
export/population with managed CpuRetryOOM (2,147,475,456-byte request, 2,018 MiB
non-pinned allowance, sampled RSS 3,424,374,784 bytes). No budget raised or failure
retried. Vortex at 2048 MiB/task improved full GPU read median 178.681 -> 9.127 s;
PCBS 1.578 -> 1.090 s. Every successful case retained all logical data, disk zero.
Report: vortex-cache-pinned2g-results.md; CSV has 48
consumer/projection rows. Exact aggregates, old/new source and dependency hashes,
actual 2 GiB pool, heap/managed/RSS budgets, scans, payload and resident snapshots
reproduced through /data/tmp/verify-pinned2g-lead.py; failure evidence separately
verified through /data/tmp/verify-pinned2g-failure-lead.py. Further copy experiment
IN PROGRESS: separate build compiled, ten lifecycle cases plus sink boundary/cap
and four CPU/GPU-producer numeric/string correctness probes passed. First small
256 MiB/task control: full GPU median 1.383 s (samples 1.099/1.667), compared with
original 2 GiB-pool matrix 1.169 s; no benefit demonstrated. One fresh original /
candidate pair will confirm; no chunk-size tuning or broader experiments. Park
or revert the candidate if no reproducible useful gain. Artifact /data/tmp/
vortex-spark-chunked-ipc remains separate; original matrix binaries unchanged.

Small controls and direct-buffer implementation are verified READY FOR REVIEW:
see vortex-cache-bridge-results.md and its three-row CSV. All use 256 MiB/task,
four partitions/two slots, heap 16 GiB, managed host cap 4 GiB, RSS guard 32 GiB.
Original IPC full GPU read median: 13.241 s at pool zero, 1.339 s at pool 256 MiB;
direct buffers with pool zero: 2.506 s (samples 3.230/1.782). Every case retained
the same 1 GiB logical data / 489,897,376 encoded bytes, 128 records, disk zero.
The 64 MiB pool exhausted; 256 MiB eliminated raw pinned calls inside import.
Independent lead verification reproduced results/profiles and inspected decisive
code. Peer review found no blockers. Separate direct build compiled; seven
lifecycle cases, CPU/GPU-producer numeric edge probes and IPC string regression
passed. Direct route supports cached int/long only; slices, duplicate projections,
injected retry/task cancellation remain unverified. No production defaults changed.
Shim preflight is not applicable: only Spark355-only exploratory tools changed,
with no production shared callers, versioned shims or source-selection profiles.

New user authorization: set pinned pool to 2 GiB and rerun every original GPU-enabled
case (three formats x four task sizes), then investigate a bounded number of
realistic further improvements and report. Keep the baseline CSV/artifacts intact.
Matrix: original artifact78e..., original Vortex class, GPU population, CPU/GPU
reads full+narrow, 256/512/1024/2048 MiB per task, four partitions/two slots, heap
16 GiB, managed host cap 4 GiB including pinned reservation, sampled RSS stop
32 GiB, per-run timeout 1200 s, one warmup/two timed reads, post-timing payload
audit. Fresh /data/tmp/cache-pinned2g-16g/{size}/{format}-gpu. No CPU-only-producer
reruns are required by this request. Default's large host allocations may encounter
the smaller non-pinned allowance; record/park failures rather than relax budgets.
No concurrent GPU jobs; ascending size prerequisites must verify before larger
cases. Preserve released contracts/classifications below. Do not change production
allocator defaults or claim configuration-only results meet out-of-box acceptance.
One further candidate is being prepared independently while immutable baseline
jobs run: fixed 64 KiB chunked IPC output for GPU reads, removing growing-array
and final trimming copies sampled in pooled JFR. Encoding/cache records/CPU
route stay unchanged; one valid IPC stream per decoded batch. Multi-file grouping
was investigated and rejected for this batch: dtype equality and native footer
concurrency add risk without eliminating per-file footer work. Measurements wait
for the matrix; start at 256 MiB/task, then at most a larger confirming case if
justified. Stop or revert the candidate on no reproducible useful benefit; do not
pursue multiple speculative optimizations or implement native GPU decoding.
Broader native/production policy changes require architect review.
MCP docker_exec is required at every operation, handoff, resumption and progress
checkpoint; workers must confirm continued use. Independently verify delegated
work and commit completed tasks with tracker updates. Only architect marks DONE.

## Scope, operating rules and accepted decision criteria

Explore Vortex through host Arrow as an alternative cache representation to branch
ParquetCachedBatchSerializer (PCBS) and Spark's DefaultCachedBatchSerializer.
The user approved the qualified plan below. Do not replace PCBS, add production
configuration, change supported contracts or silently change acceptance criteria.

Use MCP docker_exec for EVERY project operation, worker handoff, resumption and
progress checkpoint. Require workers to confirm continued MCP use. Re-read this
committed tracker after context resets. Independently inspect delegated code and
reproduce decisive checks. Commit each completed task with tracker evidence; mark
verified work READY FOR REVIEW. Keep benchmark execution serial on the GPU.
Park material architecture/scope conflicts, missing prerequisites after reasonable
investigation, or evidence contradicting the release classification. Ordinary build
and implementation errors should be corrected autonomously.

The decision is whether an option gives a useful, reliable Spark cache under default
settings. Compare these dimensions separately:

| Dimension | Required evidence |
| --- | --- |
| Execution mode | CPU and GPU population/consumption; CPU execution explicitly configured off/on as appropriate; unsupported-data fallback; CPU/GPU consumers of the same cache. |
| Formats | Spark default, actual branch PCBS, Vortex prototype. Unsupported combinations are explicit gaps. A cuDF Parquet proxy is not PCBS. |
| Population | End-to-end cache materialization including transfers, Arrow conversion, compression and Spark storage. |
| Reuse | Full and narrow projections, repeated reads and reuse-count tradeoffs; CPU and GPU consumers. |
| Stored representation | Rows/logical input represented, encoded bytes, bytes per row and compression against a consistently defined logical-input size. Report Spark estimates separately. |
| Retained capacity | Actual resident cached bytes, logical rows/bytes resident, spilled bytes and recomputation where applicable. More resident useful data can explain higher memory use. |
| Working memory | Temporary encoder/decoder/transfer buffers and overlapping inputs/outputs, distinct from retained cache. |
| Process footprint | JVM heap, native host/RSS and GPU memory, GC and failures; allocator counters alone do not cover all native allocations. |

Two experiments are required: (1) fixed identical logical data/schema/partitions to
compare time, output size and temporary memory; (2) fixed identical resource budget
and concurrency to compare logical data retained, spilling and query performance.
Default Spark storage/unroll settings are the starting point; do not tune around
failures to obtain passing results. Pressure experiments vary data volume and task
concurrency with budgets recorded. Report the tested envelope, not universal OOM
resilience. Distinguish MEMORY_AND_DISK from other supported storage-level cases.

Begin with representative numeric, low-cardinality string, varying longer string,
wide/nested and skewed/oversized-row inputs including nulls; extend where sensitivity
appears. Validate decimals, timestamps, empty/zero-column cases and fallback types
before claiming compatibility. Use deterministic fixtures, check values/order/rows,
and record physical plans for execution/fallback claims. Warmups and repeated samples
must be separate from correctness and data generation.

Fairness: PCBS receives a large parent GPU batch and encodes once before framing
small records. Vortex must receive the same logical parent through bounded export.
Small stored records and large GPU processing batches are separate dimensions.
The initial per-small-unit codec proxy cannot answer this fairness question.

## Release and compatibility classification

RELEASED: PCBS CPU/GPU paths, public serializer configuration, supported types,
projection/reordering, row counts, compression and Spark storage-level behavior.
Evidence: present on local main d587232490 and documented since plugin 0.4+ in
[cache-serializer.md](../additional-functionality/cache-serializer.md). Preserve these.
Spark's default cache contracts also remain unchanged.

UNRELEASED: branch-added small-record framing, splitting/bundling, internal 8 MiB
record target and lazy CPU writer. Evidence: recovered tracker from
`git show a9a66345cd^:docs/dev/pcbs-rowgroup-production-plan.md`; removal commit
a9a66345cd; branch-only target commit aa1e5c6fb3 in main..HEAD. No compatibility
scaffolding is required for these additions. Vortex plugin integration is new and
experimental, with no released plugin contract. This does not permit breaking
existing Vortex/Arrow library contracts.

## Revisions and decisive source findings

Plugin implementation baseline: 2cbdb23451300b5d0aca9d3a4f5aa5232f2634ff on
pcbs-rowgroup-production. Initial tracker commit: efc54fa28f. Preserve unrelated
untracked work. Vortex /data/vortex: a5f574e69bbf9010029f0e62ca59d56cd0c4aa6e,
0.90.0-92-ga5f574e69, upstream checkout clean. Indexed plugin/Spark discovery used
code-index; Vortex is unindexed. All file reads/builds used MCP docker_exec.

Lead independently read decisive code following worker discovery:

- PCBS serializer: spillable host regions and lazy record materialization; each
  retained CachedBatch owns a Java byte array. GPU writer row-group framing and
  input slicing are distinct. Internal record target is 8 MiB, not a hard bound.
- Spark 3.5.5 MemoryStore.putIterator allocates/stores iterator values before
  periodic memory checks; default period is 16 elements. Smaller records reduce
  allocation between checks but do not reserve encoder working memory.
- cuDF Table.java ArrowIPCTableWriter and native TableJni.cpp:2814 export the entire
  GPU input through to_arrow_host before IPC row chunking. Reader row targets are
  minima over complete record batches. Split input BEFORE export for byte budgets.
- Existing plugin GpuArrowWriter/GpuArrowReader provide reusable adapters and
  semaphore/ownership patterns. A direct native C Data bridge is a separate design
  option; the current experiment uses public IPC APIs and includes their costs.
- Vortex VortexWriter accepts Arrow C Data; Partition.scanArrow returns an owned
  reader. The queue bounds four batches, not bytes. NativeWritable creates fresh
  Java byte arrays; its JNI upcalls may reach 1 GiB. Layout defaults of 8192 rows
  and 1 MiB per-column targets are not executor memory budgets.
- Vortex JavaWriter schema exports are borrowed by native create/createStream.
  ArrowSchema.close frees the struct, whereas release frees exported contents.
  A minimal overlay-only fix is preserved in the harness patch. Per-batch exports
  likewise release borrowed schemas; native code consumes ArrowArray ownership.
- Actual runtime accepts Vortex Utf8View through IPC; per-row normalization was
  tested and removed as unnecessary. Arrow19/Spark older-Arrow coexistence remains
  to verify. Cleaner-only Vortex DataSource/Scan/Session lifetimes remain a risk
  to measure separately from Arrow Java allocator zero.

Mounted cuDF source and runtime provenance differ. Installed cudf-spark-jni
26.12.0-SNAPSHOT reports revision 6740b5ee74cd1c0d35c42422fb4cf0d8cfa322eb; its
cudf-java-version-info reports b9539f2b7cf98d0d6229a3fb46cec7f800331158. Public APIs
and actual round trips were checked against the installed jar. Historical PCBS
results under /data/pcbs-oom are not mounted and were not reproduced.

## Completed feasibility batch: READY FOR REVIEW

Standalone harness: [tools/cache-exploration](../../tools/cache-exploration/README.md).
No production source/build changes. Three deterministic fixtures validate every
returned GPU value/null/order and row count. Capped byte sinks are per-sink limits,
not total heap/native bounds; array growth can retain old+new buffers. Input is
allocated before its byte-size check. Large-parent splitting remains unimplemented.

Environment now works without Corretto: Gradle9.5.1 wrapper downloads via sandbox
proxy; Java classes compile with existing Red Hat JDK17 in an isolated build copy.
Removed only Amazon vendor selection/daemon criterion there. Plain cargo still
tries unwritable /opt/rustup/tmp; explicit rustup run1.99.1 works. Native release build:

```text
env LIBCLANG_PATH=/home/roberte/miniconda3/envs/spark-accel/lib \
  rustup run 1.99.1 cargo build --release --locked -p vortex-jni -j 12
```

Release library: /data/vortex/target/release/libvortex_jni.so; SHA256
`eaf2ebef071e5dec7c8067f76091a4cf466dc490b2683ff4928ebec5b741ccee`.
ldd resolved dependencies. Java overlay: /data/tmp/vortex-java-build; runtime CP:
/data/tmp/vortex-java-runtime-classpath.txt. Builder schema repair is applied there;
upstream source stays clean. Stage library in overlay src/main/resources/native/
linux-amd64, not only build/resources (Gradle can remove the latter).

Lead ran JNIWriterTest + NativeIOBridgeTest: 14 passed, 1 upstream disabled variant
case, no failures. XML: overlay vortex-jni/build/test-results/test. Initial missing
resource failure was corrected by source-resource staging. Initial standalone fixed
run leaked 4060 Arrow bytes over seven Vortex iterations; the ownership repair fixed
this without weakening the final allocator-zero assertion. Worker smoke validated
all three fixtures at 1024 rows. Focused worker source reviews and lead inspection
covered ownership, timing sums, caps and scope limitations.

Lead's exploratory serial sweep: each of three fixtures at 65536 and 524288 rows,
3 warmups + 7 measured iterations per codec, alternating order, 512 MiB JVM heap.
All six invocations passed every value and allocator-zero check. Raw CSVs/commands/
stderr and summary: /data/tmp/cache-bridge-results; runner:
/data/tmp/run-cache-bridge-sweep.py. Median full GPU round-trip milliseconds:

| Rows | Fixture | Vortex IPC bridge | cuDF Parquet proxy |
| ---: | --- | ---: | ---: |
| 65536 | fixed | 8.30 | 22.31 |
| 65536 | strings-low | 19.83 | 24.07 |
| 65536 | strings-wide | 39.56 | 44.02 |
| 524288 | fixed | 36.40 | 27.27 |
| 524288 | strings-low | 88.20 | 28.95 |
| 524288 | strings-wide | 216.69 | 55.00 |

Vortex output was smaller in every case. These are independently finalized bounded
units and a cuDF codec proxy, NOT real PCBS/default Spark/CPU/capacity evidence.
Do not choose a cache format or infer heap resilience from these measurements.

## Active batches, prerequisites and remaining verification

1. Commit/review qualified measurement plan and initial harness evidence together.
2. Establish fresh actual branch PCBS and stock Spark baselines with CPU/GPU paths;
   verify exact artifact provenance before claims. Existing jars report e9666723d6,
   while SpillFramework changed later. First Maven attempt failed in7.89s because
   Zinc wanted unwritable /home/roberte/.sbt/1.0/zinc/org.scala-sbt; log
   /data/tmp/vortex-pcbs-build.log. Resolve using an external writable compiler cache.
3. Extend experimental bridge CPU/GPU measurement and introduce a Spark-level
   Vortex prototype without production contracts. Inspect classloading/dependency
   constraints before selecting the implementation. Escalate real design conflicts.
4. Add output/resident/working/process memory accounting and fixed-data then
   fixed-budget experiments; only run meaningful matrices once correctness passes.
5. Complete compatibility/fallback, projection/reuse, concurrency/pressure and
   resource cleanup coverage. Report unsupported and unverified cells explicitly.

Runtime pressure, CPU paths, actual PCBS/Spark default comparisons, Spark classloading,
projection and full type coverage remain UNVERIFIED. No current blanket conclusion.
Shim preflight becomes mandatory if changes touch versioned sources, shared shim
callers or build profiles; this isolated tools/documentation batch changes none.

## CPU/GPU endpoints and real-PCBS tooling batch: READY FOR REVIEW

User approved the qualified comparison matrix; implementation resumed after the
capacity interruption. MCP-only confirmed again at every worker handoff/checkpoint.
The standalone bridge now selects cpu-cpu, cpu-gpu, gpu-cpu or gpu-gpu endpoints,
with optional same-encoded-byte alternate-reader verification outside timed work.
CPU-only execution hides CUDA and makes no Cuda calls. Canonical Arrow-layout logical
size, encoded bytes/row and device bytes are separate metrics. Cross-reader checking
is disabled for isolated memory measurements. All four small nullable-string modes
passed value/null/order/row and Arrow allocation checks; independent worker source
review is clear. Lead independently ran CPU-only sampling: return0, five samples,
no GPU process or unmatched GPU PID, populated heap/RSS/PSS observations. Evidence:
/data/tmp/cache-bridge-lead-cpu-memory and /data/tmp/cache-bridge-worker-cpu-sampler-review.

Actual PCBS harness calls branch encode/framing and ParquetCachedBatchReader, using
separate large-parent and small-record targets. No Spark storage claim. Fresh build
501da9cb5e63882f0c3833503130e239152b7d6e passed in1m12s without bypassing RAT/checks:

```text
rapids-plugin-mvn -nsu -ntp -DsecondaryCacheDir=/data/tmp/vortex-pcbs-zinc \\
  -Dbuildver=355 -pl sql-plugin -am package -DskipTests \\
  -l /data/tmp/vortex-pcbs-build-cache-fixed.log
```

Supported writable Zinc option resolved the earlier failure; changing user.home
was investigated and discarded because it lost the existing Maven cache settings.
The runner validates expected artifact revision and records SHA256/property provenance.
Explicitly pin expected-revision501da9 when using these jars after tools-only commits.
Unrelated pre-existing untracked sources remain in the checkout; artifact hashes
identify the binaries actually tested. No production source edits were made.

Worker checked all three1024-row PCBS fixtures. Lead read complete harness/runtime
setup and independently reran final-default framing:16384 varying-string rows,
688579 canonical logicalB,686531 deviceB,1GiB GPU target,4GiB reader soft target,
64KiB cache record target =>10 records,467233 encodedB,max49193B. Every decoded
value/null/order/row matched and host spill handles returned zero. Evidence:
/data/tmp/pcbs-codec-lead-final-smoke/provenance.json. Cold smoke timings are NOT
performance comparisons. Earlier worker bytes vary slightly due Parquet null-buffer
contents; logical values/counts agree.

Process sampler measure_process.py executes a precompiled exported Java argv,
excluding javac, and records sampled treeRSS/PSS, jstat heap/GC and NVIDIA process
residency. Fields distinguish missing data and unmatched PID namespaces; no heap
subtraction is used to invent native allocation. Collection overhead and JVM startup
are included; sampled peaks can miss short allocations. Independent review identified
interrupt cleanup; process-group TERM/wait/KILL and timeout now handle it. Lead's
0.3s timeout probe terminated both Python parent and sleep child (pgrep found none).
On timeout/collector errors the sampler preserves partial logs and exits nonzero
without summary.json. This initial tool expects an isolated JVM root, not a launcher
that exits while leaving detached descendants.

## Spark integration/accounting work in progress

Local Spark runner now uses DataFrame's actual MEMORY_AND_DISK_DESER default;
serialized storage is an explicitly separate experiment. Initial serialized smoke
and corrected default-DESER smoke passed every small-row value. Driver heap snapshots
are labeled; local execution is enforced. Reads collect the exact aggregation and
record final executed plans, with separate read warmups. Population is one cold
materialization per invocation: repeat fresh-cache/job samples before comparisons.

Important source finding: Spark default sizeInBytesStats sums uncompressed column
statistics; PCBS sums encoded array lengths. Never compare these as uniform encoded
size. Accumulators also count recomputation and do not describe retained rows.
The runner separates BlockManager footprint and canonical logical size; a tools-only
post-run payload audit sums actual cache arrays and returns only per-partition scalar
summaries. Audit is excluded from timed/capacity snapshots because it can recompute
and change residency. New local block metadata snapshots map resident source partitions
to logical rows/bytes; final runtime validation of this addition is pending.

Experimental Spark serializer uses a PCBS wrapper subtype only to pass the existing
concrete GPU scan gate; every serialization entry is overridden. A platform-parent
URLClassLoader isolates the Vortex/Arrow19/Netty closure from SparkArrow12. Only JDK
IPC bytes/primitive args cross the boundary. CPU output normalizes view strings to
old Arrow; GPU output stays direct IPC. Supported types currently int/long/string;
others are explicit gaps. This is local Spark3.5.5-only, not a released integration.

Early small CPU/GPU/mixed same-cache and reordered-projection probes passed actual
GpuInMemoryTableScan and full values. Supplemental UDF/Delta33x startup dependencies
were rebuilt current501da9 (39.333s); provenance is at
/data/tmp/vortex-spark-prototype/provenance.json. These probes precede final lifecycle
review. Lead found closed decoder/source task-listener retention and Spark Arrow
helper per-unit listener retention; fixes are in progress before pressure tests.
No pressure, capacity, or comparative Spark performance result is established yet.

## Local Spark prototype and first comparison: READY FOR REVIEW

Implementation is confined to tools/cache-exploration; production contracts/source
and build profiles remain unchanged. Lead inspected the decisive Scala/Java code,
worker peer review covered native ownership/projection/cancellation, and independent
lead runs reproduced CPU and direct GPU scans, per-partition payload/storage audit,
all small-row values, and the unchanged four-case weak-reference lifecycle probe.
The corrected native readable explicitly releases its byte-array reference after
reader/scan/source/session closure. No background worker or Rust/ABI change was
introduced. Deferred canceled Rust tasks may retain native buffers until subsequent
runtime work; this is not a demonstrated total-native-memory budget.

CPU scanning in this first Vortex prototype is row-based, whereas supported default
Spark/PCBS schemas can use vectorized CPU output. GPU consumption currently passes
separate Arrow IPC units through cuDF. Compare these implemented routes, not an
intrinsic best-case limit for the format. Other Vortex schemas remain explicit gaps.

Memory controls were qualified with the user: the Docker limit is the whole box,
so NEVER target machine exhaustion. Use conservative bounded data/concurrency and
an explicit RAPIDS-managed host limit; it does NOT cover Arrow/Rust allocations.
A 256 MiB managed-cap smoke failed with CpuSplitAndRetryOOM in default row-to-column
builders at only1024rows (128MiB already tracked;236912656B requested). Sampled RSS
was1266384896B; no host OOM occurred. Retain failure at
/data/tmp/vortex-lead-final-capped-gpu. Baseline now uses1GiB JVM heap,4GiB RAPIDS
managed host limit,0pinned(default),2task slots. Startup logs confirm4066MiB nonpinned
plus30MiB overhead. Dataset/unit caps and a sampled4GiB RSS early-stop guard remain;
the guard is not an OS hard limit. Its deliberately lowered1-byte check recorded
trigger sample and terminated the owned process group, with no survivor found.

Launcher snapshots/verifies the exact ordered parent/child dependency closure,
classes/native libraries and source; later builds cannot overwrite per-run evidence.
Future runs snapshot the job source itself. JVM/Rust/Arrow working memory stays
separate from encoded payload, BlockManager footprint and logical resident data.
RMM tracked live/high-water counters exclude unused pool reservation; high water
is cumulative for that JVM, not a phase peak. No high-water subtraction is used.

Lead independently verified all six first-comparison JSON results and medians.
Same524288rows /46476067canonical logicalB /2partitions/tasks /1GiBheap /4GiBmanaged
cap, one warmup and two timed reuse samples, serial fresh JVMs. All data resident,
no disk. Full query consumes all columns using xxhash64; no CPU expression bridge.
Default requested-GPU reads still scan cache on CPU; PCBS/Vortex use direct GPU scans.
Cold population is one sample per case, so all numbers remain preliminary.

| Format / producer | Payload B | Resident B | Population ms | CPU read-only median ms | GPU-enabled read-only median ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| Default / CPU |41236397|41273328|775.594|185.129|258.774|
| Default / GPU |41236397|41273328|2245.839|146.216|250.506|
| PCBS / CPU |44941123|44941536|1157.194|224.079|204.775|
| PCBS / GPU |36709004|36709320|976.781|246.615|198.712|
| Vortex / CPU |21124088|21124504|1221.914|248.621|815.468|
| Vortex / GPU |21110496|21110824|1318.547|236.199|871.027|

Whole-job sampled RSS ranged1527726080..1771909120B and includes startup/reference/
audit work; do not label the difference pure encoder working memory. Evidence:
/data/tmp/cache-first-comparison/<format>-<producer> (result,command,memory,
verified-summary,source/artifact provenance). All six build-provenance snapshots
share SHA7265fbd1efaddae77f05d9de0b2fbaf7c80ea7941d9c6bbba6640c391fa6fd22.
Verification script /data/tmp/summarize-first-comparison.py. No guard/timeouts,
autotuning, or host failures occurred. These are the first real Spark comparisons;
the older cuDF codec-proxy timings must remain separately labeled.

## User-qualified scale and final-report requirements: IN PROGRESS

Final report MUST separate cache population, read-only reuse (first vs warmed,
full vs projected) and write+read, broken down by CPU/GPU producer and consumer.
Totals formed by adding measured phases are COMPUTED models, not independent
end-to-end measurements. Consumer order is CPU then GPU on the same stored bytes.

User requested about256MiB,512MiB,1GiB,2GiB LOGICAL data per TASK with multiple
tasks and ideally two waves. Use2concurrent slots and4source/cache partitions.
Task size is independent of GPU batch target and small cache records. Stream data;
never collect a large dataset to the driver, search machine-OOM boundaries, or
raise limits automatically after a guard/managed failure. Advance upward only after
reviewing lower-size footprint. Preserve explicit unsupported/failed/incomplete cells.

New fixed-wide fixture has32fields: ID/key plus10monotonic,10low-cardinality and
10hash-derived longs with nulls. Its standardized nullable Arrow-layout estimate is
252fixed bytes +32validity bits =256B/row for the selected row counts. Exact rows/task
are1048576,2097152,4194304,8388608; total across4partitions up to33554432. Full queries
hash all30extra columns and count nulls; narrow queries select ID only. Keep the
original string fixture results separate: the workloads are different.

Only the explicit --task-size-mib path may exceed the small-run row limit. It checks
exact total/per-partition logical size and partitions==2*task slots. Small1024-row
CPU/GPU smokes passed all32values and canonical262144B,4partitions/2waves, including
actual GPU scans and payload audit. Producer SQL counters are reported with their
node names: GpuRange exposed4batches; GpuProject exposed no output-batch counter.
Do not invent a final-wide batch count from the child counter. Lead read the full
fixture/validation code; independent peer review is clear. Large scale cases have
NOT yet run as of this implementation checkpoint. The following batch records actual scale results.

## 256 MiB/task first scale checkpoint: IN PROGRESS

MCP docker_exec only confirmed at worker resumption/checkpoint; source/build frozen
at a4aead9cc2. No production or release classification changes. Two local slots,
four partitions/two waves, 4,194,304 rows, 1 GiB total canonical logical data,
1 GiB Java heap and 4 GiB RAPIDS-managed host cap, 4 GiB sampled RSS stop guard.

Default/GPU producer: population completed in24564.578ms; all four partitions
resident, representing all1GiB logical data in437697968BlockManager bytes,0disk.
All six CPU full/narrow samples passed expected aggregates and actual CPU scans.
Full first1740.541ms/warmed median1340.473ms; narrow first227.744ms/warmed91.204ms.
FIRST GPU-enabled full read failed with Java heap space in
DefaultCachedBatchSerializer.createAndDecompressColumn -> OnHeapColumnVector ->
AbstractGpuCoalesceIterator. This was a bounded1GiB JVM heap failure, NOT host OOM
or RSS guard. Whole-job sampled RSS peak2683441152B; process exited/GPUidle.
No GPU-enabled read completed, and payload audit was not reached. Do not substitute
serializer sizeInBytesStats for encoded payload. Preserve failure without tuning.

Lead independently read stack/logs and reproduced result/row/partition/aggregate/
phase/guard assertions with /data/tmp/verify-failed-default-scale.py. Evidence:
 /data/tmp/cache-scale/256m/default-gpu/lead-verified-summary.json and original
result/command/memory/source/provenance/logs. After review, independent PCBS/Vortex
256MiB cases are authorized under unchanged limits; no default rerun or larger
cases yet. Stop/review again on any new failure.

Read-only versus population plus read evidence for the six original small-string
cases is now in vortex-cache-results.md: first and warmed, full and narrow,
CPU/GPU producers/consumers, actual reader, explicitly COMPUTED combined sums.
Lead independently reproduced all24phase-table rows from original nanoseconds
with /data/tmp/verify-first-phase-report.py; no independent end-to-end claim.

## 256 MiB/task GPU cohort and accounting repair: READY FOR REVIEW

PCBS/GPU and Vortex/GPU both passed all full/narrow CPU/GPU aggregates and audited
rows, actual CPU/GPU cache plans and no CPU expression bridges. All use the identical
fixed-wide1GiB logical input, two slots/four partitions and unchanged caps.
Lead independently reran /data/tmp/verify-cache-scale.py against both raw results.

| GPU producer | Payload B | Population ms | CPU full read warm median ms | GPU full read warm median ms | Whole-job sampled RSS B |
| --- | ---: | ---: | ---: | ---: | ---: |
| PCBS |513454363|2689.563|5092.274|420.687|2537283584|
| Vortex |489897376|6946.398|2736.383|10903.511|2756464640|

PCBS retained768MiB logical data in385095928local memoryB and256MiB logical
data only on disk (90851217B). Vortex retained768MiB logical in367427984memoryB
and256MiB logical only on disk;223125668diskB ALSO includes a partition present
in memory. Never sum overlapping memory/disk logical subsets. Default retained all
logical data but failed its GPU-enabled read, as recorded above.

Lead caught and fixed a material measurement error: storage_info mixed local
BlockManager.getStatus blocks/logical subsets with getRDDStorageInfo byte totals.
Vortex's aggregate244961864B lagged its actual local block sum367427984B.
Decisive Spark3.5.5 source: BlockManager.getStatus reads memoryStore/diskStore;
SparkContext.getRDDStorageInfo reads statusStore reports. Primary storage totals
now sum the SAME sequential local-block observations; separate RDD metadata remains
under rdd_reported_storage_info with an explicit lag caveat. This minimal tools-only
repair changes no workload/serializer/timing. Source-only peer review clear; lead
read the complete diff and reproduced corrected sums from original raw evidence.
Original result files remain unchanged; lead-verified-summary.json derives corrected
totals. This avoids mixing different metadata sources rather than claiming an
atomic snapshot.

Lead's independent CPU-producer Default256 case exercised the corrected function:
all1GiB logical/437697968memoryB resident,0disk; counters match block sums. CPU
full first1684.012/warm1421.167ms; narrow181.940/82.148ms; population21521.448ms.
Its FIRST GPU full read also failed with the same Java heap stack; no GPU sample
or payload audit. Whole-job RSS2521595904B, below guard; process exited. No caps
were raised. Evidence /data/tmp/cache-scale/256m/default-cpu; independently verified
with /data/tmp/verify-failed-default-scale.py. Broader Default GPU-enabled scale
cases are parked after these controlled failures; this does not establish failure
for CPU-only execution at larger sizes.

PCBS/Vortex CPU-producer256 cases are the next bounded batch, IN PROGRESS. Still
MCP-only; no concurrent GPU jobs, source/build frozen, stop/review on failure.
No512MiB/task or larger case had run at that checkpoint.

## 256 MiB/task complete producer/consumer matrix: READY FOR REVIEW

PCBS/CPU and Vortex/CPU both PASS with identical logical input and all32field
aggregates, CPU/GPU scan plans, no CPU expression bridge, audited row counts and
unchanged caps. Lead independently ran /data/tmp/verify-cache-scale.py, including
all14 per-action local-storage consistency checks. Peer review covered the small
accounting change; production/build sources remain unchanged.

| CPU producer | Payload B | Population ms | CPU full warm median ms | GPU full warm median ms | RSS peak B |
| --- | ---: | ---: | ---: | ---: | ---: |
| PCBS |735262340|14060.908|5644.279|696.969|2611281920|
| Vortex |489848544|7170.255|2465.181|10715.521|2668290048|

PCBS/CPU:183817312memoryB represents256MiB logical;382166705diskB represents
three disk-only partitions/768MiB logical. Vortex/CPU:244925792memoryB represents
512MiB logical;223294310diskB represents512MiB logical disk-only. Population and
after-read snapshots retain these subsets. These figures show why producer mode
and logical capacity must be separate from physical bytes. Evidence in
/data/tmp/cache-scale/256m/<format>-cpu; full/narrow first/warm and computed
population-plus-read totals are in vortex-cache-results.md.

Lower-scale successful cases stayed below2.76GB whole-job sampled RSS, so the next
bounded batch is PCBS/GPU and Vortex/GPU at512MiB/task,2GiB logical total, same
two slots/four partitions and all budgets unchanged. Larger CPU-produced caches
remain explicitly unmeasured. No higher-size Default GPU-enabled repeats: both
producer modes already reached the bounded heap failure at256MiB. This is not a
claim about larger CPU-only Default reads. MCP-only confirmed at resumption;
source frozen at0bd17258c6; no concurrent jobs/builds. Stop/review on any failure.
Lead independently reproduced every completed256MiB report phase row (20 cells),
six-case storage/population values and all14 unchanged storage snapshots per
PCBS/Vortex case with /data/tmp/verify-wide-report.py; report READY FOR REVIEW.

## 512 MiB/task GPU cohort and CPU-only Default control: READY FOR REVIEW

Both512MiB/task GPU-producer cases PASS at8388608rows/2GiB logical, same frozen
source/caps/concurrency, CPU+GPU full/narrow aggregates/plans/audit/14local storage
snapshot checks. Lead independently ran verify-cache-scale.py for each result.

| GPU producer | Payload B | Population ms | CPU full warm ms | GPU full warm ms | Whole-job RSS B |
| --- | ---: | ---: | ---: | ---: | ---: |
| PCBS |1026901869|4847.272|10642.250|748.136|2849349632|
| Vortex |979946776|13329.875|5829.208|22329.740|2988404736|

Each retained one512MiB logical partition: PCBS256727704memoryB/545052644diskB;
Vortex244969488memoryB/668576200diskB. Other three partitions were disk-only;
no overlap in these snapshots. Whole-job RSS remains below the4GiB guard.
Evidence /data/tmp/cache-scale/512m/<format>-gpu. No CPU producer above256 tested.

A separate Default CPU-only256 run PASS with plugin loaded and GPU SQL disabled
for population and all reads. This completes the previously unavailable payload
measurement WITHOUT replacing failed-run evidence:435890015payloadB/420records,
all1GiB logical resident in437697968storageB,0disk,1973252096RSSpeakB.
Population21087.453ms, CPU full warmed1351.400ms, narrow102.831ms.
Default's numeric payload is SMALLER than both PCBS/Vortex, unlike the initial
string workload's ranking. Do not imply Vortex universally compresses best.
Evidence /data/tmp/cache-scale/256m/default-cpu-only. Lead independently launched
and verified all aggregate/plan/size/snapshot/guard checks with verify-cache-scale.py.
Its success does not turn earlier GPU-enabled Default failures into passes.

Worker report tables for both512 cases and this Default control were independently
reproduced by lead with /data/tmp/verify-report-additions.py: population, all first/
warmed read cells, computed combined additions, payload and local storage values.
The report labels sequential observation/capacity/RSS limitations and partition
atomicity; lead re-read Spark MemoryStore.putIterator decisive whole-block commit.

Next bounded cohort is PCBS/GPU then Vortex/GPU1024MiB/task,4GiB logical total,
unchanged budgets/300s timeout, IN PROGRESS. MCP-only confirmed at handoff and
checkpoint; source remains frozen. No2GiB/task case or larger CPU producer run
yet. Any new failure/guard/timeout stops the batch before a larger test.

## 1 GiB/task original-record cohort and heap investigation: READY FOR REVIEW

PCBS/GPU PASS at16777216rows/4GiB logical total, original8MiB-record Vortex
comparison build and same source/caps/concurrency. Payload2053789695B/560records;
population7070.628ms; CPU full warmed21635.687ms/GPU full1371.336ms.
All4partitions are Spark-disk-only:1453409220diskB,0resident bytes/logical data.
Whole-job RSS3429646336B. Lead independently verified all14snapshots, full/narrow
CPU/GPU aggregates/plans/audit/guard with verify-cache-scale.py. Warmed file reads
may benefit from OS page cache; these are not cold physical-disk IO measurements.

Original Vortex/GPU at1GiB/task: population completed27066.909ms, all4logicalGiB
on Spark disk (1783189437B),0resident. FIRST CPU full-read warmup FAILED with
Java heap space at Arrays.copyOf -> VortexIpcCodec.Sink.reserve -> writeIpc ->
Decoder.next -> cpuRows. Exit52; reads=[], no read timing or audited payload.
RSS2379546624B, below guard, no host OOM or timeout. Original source/provenance/
logs preserved in /data/tmp/cache-scale/1024m/vortex-gpu. Lead independently read
the stack and reproduced assertions with verify-vortex-failed-scale.py. All owned
processes exited. No cap increases or repeated failing case; batch stopped.

Read-only worker investigation and lead decisive-code reading found:
- Spark JavaSerializationStream resets ObjectOutputStream every100outer objects;
  ObjectInputStream.readObject retains its handle table between resets. Both
  VortexCacheRecord and ParquetCachedBatch are plain case classes strongly owning
  byte arrays. This is a shared retention risk, not a Vortex-format-specific leak.
- BlockManager.maybeCacheDiskValuesInMemory eagerly tries putIteratorAsValues.
  On failure, PartiallyUnrolledIterator retains its vector until prefix exhaustion.
  Logs before failure show412.7MiB and62.1MiB estimated concurrent prefixes.
  These are estimates, and prefix/handle references can overlap: do NOT add them
  as independent heap allocations. No heap dump establishes exact attribution.
- No growing IPC-list/listener chain found in corrected CPU iterator. Its current
  reader/decoder/pending arrays are cleared; IPC sink growth/finalization still
  overlaps temporary Java arrays. Failure at allocation is not proof oversized unit.
- Custom writeUnshared(array) alone would not remove outer-record handle ownership.
  Mutating encoded cache bytes during decode would corrupt reusable/shared caches.
  ObjectStreamReset/storage-level overrides are diagnostics/workarounds, not the
  out-of-box design requested, so none were applied.

## Smaller internal Vortex units: IN PROGRESS, isolated experiment

Within the existing exploratory scope, change only the UNRELEASED prototype's
internal target8MiB ->1MiB plus a plain-purpose comment. No production/config/
API/shim/native changes; released PCBS/default contracts stay preserved. This is
a mitigation experiment, not a claim of universal OOM safety or proven root cause.
Original8MiB build remains untouched at /data/tmp/vortex-spark-prototype.

New isolated /data/tmp/vortex-spark-prototype-1m build PASS; appSHA
cdb79de3114e7cf1864e020b06831e90d21a4820f03229220cbe8363a5695ab4,
original app78e04232b44e4a0569eb024ec0d911649719c560808c9bc457bceabe1f5f8824.
Source/build-isolation peer review clear. Lead independently read the complete diff
and verified BOTH full runtime closures, identical parent dependencies/native,
and unchanged original app with /data/tmp/verify-vortex-build-variants.py.
Worker also verified original29filetree byte-for-byte unchanged. Build needed
modern sandbox Python instead of system3.6 for existing text=True; no environment
or build-script changes were made. Source/build now frozen.

Next ONLY runtime check is GPU-producer/CPU-consumer1GiB/task using NEWbuild-dir,
same heap1g/managed4g/RSSguard4GiB/timeout300,2slots/4parts,warmup1/repeats2/audit:
 /data/tmp/cache-scale-1m/1024m/vortex-gpu-cpu-only.
MCP-only confirmed at every handoff/review/checkpoint. GPU consumption of this
variant is unmeasured. Original failed8MiB result and its comparators remain
separately labeled. No2GiB/task test had run at this checkpoint.

## User correction and primary host-memory baseline: ACTIVE

The user challenged whether our constrained envelope was creating the memory
problem and noted substantial host RAM. That concern is valid: a4GiB RSS guard
can stop legitimate JVM+plugin+Arrow/Rust use; it is not evidence of host OOM,
native leak or an inherently unsuitable format. Lead moved too quickly from the
constrained result to a record-size fix. Do not promote that inference.

Isolated1MiB trial was ABORTED by our RSS guard at123.549s:
RSS4473876480B/PSS4465615872B >4294967296threshold. memory.json records RSS_GUARD;
command.json records ABORTED. No result.json: all completed timings/read checks/
audit and exact active read iteration unverified. Lead independently read samples/
logs and checked owned processes gone. No cap increases or reruns of that case.
Trial source archived in /data/tmp/vortex-spark-prototype-1m; experimental source
constant restored8MiB, no failed mitigation promoted. Old artifact tree unchanged.

The user EXPLICITLY selected16GiB JVM heap for the PRIMARY comparison (async reply).
Keep4GiB RAPIDS-managed host cap; use an explicit32GiB sampled RSS guard with
conservative data/concurrency and no machine-exhaustion testing. Host MemTotal
197679172KiB / MemAvailable183416320KiB observed before cohort. The guard is
reactive, not an OS memory limit; Arrow/Rust remain outside the plugin cap.
This is a separately authorized baseline, not retroactively raising limits to
erase failures. All old negative evidence remains in constrained-results appendix.
Further old-budget runs and record-size optimization are stopped.

Minimal tools-only launcher change permits explicit --heap16g and guard<=32GiB;
defaults1g/4GiB stay unchanged. Manifest reports actual configured guard and
sampling limitations. Kill logic/data/task limits unchanged. Lead read entire
diff; peer review clear; worker parser boundary tests pass. First actual new run
independently verified16GiB heap max,32GiBguard,4gmanaged and original app SHA.
Source/prototype remains original8MiB-target build78e042...; no native/prod/shim
changes or compatibility scaffolding.

PRIMARY256MiB/task Default/GPU PASS at1GiB logicaltotal: population23012.288ms,
payload435890015B/420records; all four partitions memory resident437697968B,
0disk. CPUfullwarm1524.406ms/GPU-enabledfull1170.159ms; actual defaultcache
decoder remains CPU followed by HostColumnarToGpu/GpuHashAggregate. Whole-job
sampled RSS5122056192B. Prior constrained GPU-read failure did not recur under
this budget, which confirms that result was resource-envelope-specific.
Lead independently ran verify-cache-scale.py with explicit main16g budget checks,
all14local storage snapshots/aggregates/plans/audit and actual application SHA.
Evidence /data/tmp/cache-main-16g/256m/default-gpu. PCBS/Vortex GPU producers
same256MiB task size now in progress, serial; no higher main-size case yet.

Encoding question: tested default adaptive Vortex choices against actualPCBS
CPU/GPU and defaultSpark formats, NOT forced Vortex alternate encodings.
Lead read nativeWriter.createStream -> session.write_options -> Rust default
WriteStrategyBuilder/BtrBlocks path. Java/JNI exposes storage options/footer
metadata, not encoding strategies. Rust supports alternate compressor/field
strategies, requiring an explicit experimental binding extension to compare them.
Default adaptive selector evaluates compression ratio, not measured decode speed.
Actual per-column winners have not been audited; do not claim a forced codec or
invent Java encoding flags. Encoding variation is a recorded gap; finish the
representative resource baseline before deciding whether that extension is useful.

MCP-only confirmed at user-steered handoff, launcher review and every worker
checkpoint. Main budgets stay fixed; failure/guard/timeout stops for review.
Next main GPU-producer256cohort uses /data/tmp/cache-main-16g and default timeout300;
larger task sizes remain subject to lower-size footprint review.

## PRIMARY16GiB / 256MiB-task cohort: READY FOR REVIEW

All3GPU-producer cases PASS, original8MiB Vortex build,4partitions/2slots,
1GiB canonical logical total,16GiB heap/4GiB managed/32GiB reactiveRSSguard.
All logical data resident/no disk in every14snapshot set. Lead independently
verified actualheapmax,guard,managedcap,artifactSHA,aggregates,actualreaderplans,
auditedrows/payload and localstorage sums with verify-cache-scale.py.

| Format | Payload B | Population ms | CPU full warm ms | GPU-enabled full warm ms | Whole-job RSS B |
| --- | ---: | ---: | ---: | ---: | ---: |
| Default |435890015|23012.288|1524.406|1170.159|5122056192|
| PCBS |513454990|2502.820|5576.536|454.920|3980845056|
| Vortex |489897376|6918.857|2675.958|13852.579|5558865920|

Localresidentbytes437697968/513461696/489903072 respectively all represent1GiB
logical data. DefaultGPU-enabled query usesCPUdecoder thenGPUconversion/aggregation;
PCBS/Vortex actualGPUcache scans. Payload rankings numeric-workload-specific.
Whole-jobRSS is not live-native/temporary-allocation measurement; don't infer
pressure from these peaks alone or format speed limits from prototype routes.

Worker primaryreport appended separatepopulation/read-only first+warmed/full+
narrow/computedcombined tables. Lead independently reproduced ALL12phase rows,
population/storage/payload/RSS from rawJSON using /data/tmp/verify-primary-report.py256.
Historical report verification scripts updated to the constrained-report path and
rerunPASS after the move, preserving reproducibility. Focused peer review of
budget/encoding/failed-trial documentation clear. No production changes.

After reviewing largest256-case RSS5558865920B against the32GiB guard and ample
availablehostmemory, nextPRIMARY512GPU-producer cohort is IN PROGRESS, same
budgets/concurrency/originalVortexunits,timeout300,warmup1/repeats2/audit.
Default512PASS independentlyverified:2GiB logical allresident875395032B,
payload871780099B,population42646.828ms,CPUfullwarm2779.819ms/GPU-enabled2006.628ms,
RSS7943970816B. PCBS/Vortex512 pending. No1GiB/2GiBtaskprimaryrun yet, no16GiB
CPU-producercohort yet. MCP-only confirmed at resumption/checkpoint; no concurrent
benchmarks or builds. Stop/review on failure, no automatic limit changes.

## PRIMARY16GiB / 512MiB-task cohort: READY FOR REVIEW

All3GPU-producer cases PASS at2GiB canonical logical total, all4partitions
memory-resident/no disk across all14snapshots per case. Same original8MiB
Vortex build,16GiBheap/managed4GiB/RSSguard32GiB/2slots/4parts.
Lead independently verified all rawcases with verify-cache-scale.py, including
actualresourcebudgets/artifact,expectedaggregates,readerplans,auditcounts/storage.

| Format | Payload B | Population ms | CPU full warm ms | GPU-enabled full warm ms | Whole-job RSS B |
| --- | ---: | ---: | ---: | ---: | ---: |
| Default |871780099|42646.828|2779.819|2006.628|7943970816|
| PCBS |1026907683|3381.433|10525.747|523.472|3675885568|
| Vortex |979946776|12008.372|5744.754|29643.115|7785480192|

Localstorage-accountedbytes875395032/1026921048/979958104 respectively each
represent2GiB logical data. Lead generated the same first/warm/full/narrow/
computedcombined tables from rawJSON with append-primary-cohort.py512 and
independently reproduced ALL12phase rows plus size/RSS/population using
verify-primary-report.py256512. Java reportworker hit model capacity; lead
continued from saved data without restarting or duplicating benchmarks.
Data source/artifacts/resourcebudgets remained unchanged.

Next main1GiB/task cohort IN PROGRESS, same memorybudgets/concurrency/methodology;
EXPLICIT timeout600 selected BEFORE runs based on observed~30s/full Vortex read
at512MiB/task. This extends duration only, not resourcecaps or repeatcriteria.
MCP-only confirmed at resumption/checkpoints; no builds/concurrent benchmarks.
No2GiB/task or16GiB CPU-producer run yet.

Default primary1GiB/task PASS independently verified:4GiB logical allresident,
payload1743560044B/1680records, population85826.450ms,CPUfullwarm5381.009ms/
GPU-enabled3979.743ms,RSS15233388544B,0disk. BlockManager memory1662574592B
is LOWER than payloadsum; lead read MemoryStore.DeserializedValuesHolder.getBuilder
which uses SizeEstimator.estimate(arrayValues). Therefore residentbyte counters
are Spark storage-accounting estimates, not exact physical/liveheap size. Report
now says this explicitly; don't infer physical heap smaller than unique payload
or use estimates as allocation census. PCBS/Vortex main1GiB cases were pending
at that checkpoint.

## PRIMARY16GiB / 1GiB-task cohort: READY FOR REVIEW

All3GPU-producer cases PASS at4GiB logical total, all4partitions memory resident/
0disk across14snapshots, same original8MiB Vortex app and16GiBheap/4gmanaged/
32GiBRSSguard/2slots/4parts. Lead independently verified all three raw cases via
verify-cache-scale.py, including actualbudgets/artifacts/aggregates/plans/audits.

| Format | Payload B | Population ms | CPU full warm ms | GPU-enabled full warm ms | Whole-job RSS B |
| --- | ---: | ---: | ---: | ---: | ---: |
| Default |1743560044|85826.450|5381.009|3979.743|15233388544|
| PCBS |2053799024|5759.752|20290.702|1043.448|10749976576|
| Vortex |1959914440|20788.708|11997.896|71252.518|9508372480|

Storage-accountedresidentbytes1662574592/2053825704/1959937032 respectively
representall4GiB logical data. Vortex completes the EXACT CPU-read path that
failed with1GiBheap; no record-size fix is necessary to pass this normal-budget
case. Preserve oldnegative as different-resource evidence, not a fixedbug claim.
LargestRSS15.23GB still below32GiBguard and amplehostRAM. PeakRSS includes dead
heap objects/temporaries as well as live data, so it is not a live-memory census.

Lead appended same full/narrow first/warm/read-only/computedphasecombined tables
via append-primary-cohort.py1024 and independently reproduced ALL12phase rows,
population/payload/storage/RSS via verify-primary-report.py1024. Mainreport now
explicitly distinguishes CODEC hardware: Vortex always encodes/decodes onCPU,
even when Spark GPUscan emitsGPUbatches through Arrow; PCBSusesGPUParquetcodec
onGPU path; DefaultusesCPUcodec thenGPUconversion. GPUscan label never implies
VortexGPUdecompression. Current per-IPC-batch cuDF reader overhead is not isolated
from Vortex decode; format-intrinsic performance remains unestablished.

Focused independent peer review of lead-generated512report and SizeEstimator
qualification clear; verified source SizeEstimator samples largeobject arrays.
One header wording made consistent as 'Sampled whole-job peak RSS B'.

Next final requested size is PRIMARY2GiB/task GPU-producer cohort,8GiB logical
total, IN PROGRESS. Same memorycaps/concurrency/data/schema/records/repeats;
EXPLICIT timeout900 chosen BEFORE launch because1GiBVortexwholejob307.8s predicts
>600s at2GiB/task. Duration only, not memory/acceptance relaxation. Stopbatchon
failure/guard/timeout; no automaticlimit changes. MCP-only confirmed at handoff
and progresscheckpoints. No CPU-producer16GiB cohort yet.

A minimal7line Pythonjob observability change prints flushed JSON population/
read phase scalars AFTER existing timerstop/checks/snapshots. No extraJVMcalls,
fixtures/budgets/operators/encoding changes. py_compilePASS and focused peerreview
clear; current2GiB run captures updatedjobsource. Runtime matching of13stdout
events to finalresult remains pending. This fixes evidence loss on future guard/
timeouts without claiming a completedoverallrun from partialphase events.

## Phase evidence and first2GiB-task cases: READY FOR REVIEW / cohort ACTIVE

Untimed7line phase-print change now VERIFIED on real8GiB logical jobs: lead
independently ran verify-cache-progress.py for Default andPCBS main2GiB/task.
All13flushed JSONrecords match finalpopulation/read nanoseconds/mode/projection/
warmup/iteration/actualreader exactly; aggregate_checked onlyafterpassedvalidation.
Focused sourcepeerreview andpy_compilePASS. No newJVMcalls/timedregion changes.
Implementation READY FOR REVIEW; capturedsourcehashc652baf... identifies change.

Default primary2GiB/task PASS:8GiB logical allresident, payload3487118351B/
3356records, storage-accounted3335300728B,0disk, RSS19117694976B.
Population321273.290ms, CPUfullwarm10094.548ms/GPU-enabled7645.673ms.
Lead independently verified allresource/app/aggregate/plan/audit/14snapshot checks.
Logs show roughly2GiB host requests while~2.3GiB already managedallocated, exceeding
fixed4GiBcap; repeated HostAlloc first-attempt backpressure before population
completed. Do not treat this as hostOOM or attribute321s to formatintrinsics.
This resource-cap interaction must accompany the reported population time.

PCBS primary2GiB/task PASS:8GiB logical allresident, payload4107649355B/
1124records, storage-accounted4107702824B,0disk, RSS9637453824B.
Population9738.901ms, CPUfullwarm42574.593ms/GPUfull1578.360ms.
Same independent raw/result/progresschecks PASS. Vortex2GiB primary run ACTIVE;
no stoppingcondition reported. Same900s allowance and fixedbudgets; no otherjobs.

User explicitly asked whether population numbers are GPU-enabled and whether
CPU-only versions ran. Lead clarified: primary published cases so far GPU input
execution, but Default/Vortex codecCPU versusPCBS GPUcodec. CPU-produced old1GiB
budget cases exist, cannot mix with16GiB baseline. NEXT required mainseries is
CPU-produced/CPU-read at ALL FOURtask sizes (256/512/1024/2048MiB), GPU SQL disabled
throughout, same16GiBheap/managed4g/RSSguard32GiB/2slots4parts/records/methodology.
Use --load-plugin torepresent serializer-on/GPU-SQL-off scenario. Primary
CPU-produced→GPU-consumer performance remains unmeasured; earlier mixed-budget
correctness/timing evidence stays separately qualified. Do not claim full
cross-product performance coverage from CPU-only runs.

MCP-only confirmed each runtime checkpoint; no source/build changes while current
comparative cohort runs. Finish currentVortexcase and reviewfootprint before
next boundedCPUseries.

## PRIMARY GPU-producer matrix complete: READY FOR REVIEW

All12mainGPU-producer cases (3formats x256/512/1024/2048MiB/task) PASS under
16GiBheap/4gmanaged/32GiBreactiveRSSguard/2slots4parts, original8MiB Vortexapp.
At2GiB/task all8GiB logicaldata resident/no disk in every14snapshot set.

| Format | Payload B | Population ms | CPU full warm ms | GPU-enabled full warm ms | Whole-job RSS B |
| --- | ---: | ---: | ---: | ---: | ---: |
| Default |3487118351|321273.290|10094.548|7645.673|19117694976|
| PCBS |4107649355|9738.901|42574.593|1578.360|9637453824|
| Vortex |3919838784|41934.176|26639.627|178680.817|14475481088|

Lead independently verified Vortexfinalrawcase/progress and allGPUcase reports
using verify-cache-scale.py/verify-cache-progress.py/verify-primary-report.py
25651210242048 (48first/warm/full/narrowphase rows pluscomputedtotals).
LargestRSS19.12GB below32GiBguard; no normal-budgetOOM/guard stops. Default321s
population capbackpressure explicitly included in primaryreport, not framed as
intrinsicformatcost. GPU-scan labels clarified codecCPU forVortex/Default versus
GPUParquet forPCBS. Newreport includesgeneration/transfer/cachematerialization
in population definition. No full-liveness/native-working-memory census.

ActualdecodedIPC/GPUoutputbatchcounts still UNVERIFIED. Read-only SparkUI REST
discovery confirmedAPI exists, but GET localhost4040 returnedConnectionRefused
after thebenchmarkfinished. No retries/portscans/newqueries/profiling/GC or added
runtime jobs. Do not substitute1024outerrecords for IPCbatches or claim a proven
GPUbottleneck without phaseprofiling.

## PRIMARY CPU-only256/512MiB-task cohorts: READY FOR REVIEW

All6cases PASS, pluginloaded but GPU SQLdisabled throughout population/ALLreads;
actualCPUproducer/CPUreadplans, same originalapp/data/budgets/concurrency.
Lead independently verified7progress events/8localstorage snapshots percase,
all32-fieldaggregates/auditcounts, exactdata/logicalsize vsGPU-producerreference,
actual16GiBheap/32GiBguard/4gcap and Sparkpluginloaded/SQLfalse.

| Task MiB | Format | Payload B | Population ms | CPU full warm ms | Whole-job RSS B |
| --- | --- | ---: | ---: | ---: | ---: |
|256|Default|435890015|20019.692|1469.287|2120806400|
|256|PCBS|735262340|12673.226|5561.717|5190844416|
|256|Vortex|489848544|6278.025|2714.931|4223963136|
|512|Default|871780099|38603.799|2857.102|3100147712|
|512|PCBS|1470493190|23495.837|10611.852|6050312192|
|512|Vortex|979576576|12091.614|5183.774|5709697024|

Alllogicaldata resident/no disk atbothsizes; reportcacheaccounting separate from
payload/RSS. CPUwriterPCBS payload differs materially fromGPUwriter; never reuse
a GPU-producerencoded-size row for CPUproduction. Population and CPUfirst/warm/
full/narrow/computedcombined tables generated via append-cpu-cohort.py and
independently reproduced via verify-cpu-primary-report.py256512 (12phase rows).
Independent worker review CPU256/2GiBcapqualification clear, no material findings.
CPU-only here means SQLexecution, not proofprocessneverinitializesGPUhardware.

CPU1GiB/taskcohort IN PROGRESS after512footprintreview(maxRSS6.05GB<32GiB).
Sameprofile exceptpreselectedtimeout600; no memorycap/repeat/operator changes.
DefaultCPU1GiBPASS independentlyverified:4GiBlogicalresident,0disk,
payload1743560044B,population82457.703ms,RSS4000350208B.
PCBS/VortexCPU1GiB pending, CPU2GiB notrun at that checkpoint. MCP-only confirmed every
handoff/checkpoint; no parallel benchmarks/builds/source changes. Primary
CPU-produced→GPU-consumed performance, alternate forced encodings and unsupported-
type Vortex fallback remain explicit gaps, not covered by CPU-only timings.

## Final primary matrix and delivery: READY FOR REVIEW

All 24 primary cases PASS: 3 formats x 4 task sizes x CPU/GPU population modes.
GPU-produced caches were read on CPU and GPU; CPU-only jobs kept GPU SQL disabled
throughout population and all reads, with RAPIDS still configured. Actual producer
and consumer plans were verified, not inferred from configuration. All source
partitions/rows and canonical logical data fit in cache at recorded snapshots,
including 8 GiB total at 2 GiB/task. This does not rule out transient managed/GPU
spill activity or prove maximum capacity.

CPU-only 1 GiB/task and 2 GiB/task final data, independently reproduced:

| Task MiB | Format | Payload B | Population ms | CPU full warm ms | Sampled whole-job RSS B |
| --- | --- | ---: | ---: | ---: | ---: |
|1024|Default|1743560044|82457.703|5485.507|4000350208|
|1024|PCBS|2940954398|46421.181|20762.743|9390419968|
|1024|Vortex|1958941632|22510.843|11287.980|7809339392|
|2048|Default|3487118351|151765.038|10830.367|7251562496|
|2048|PCBS|5881877158|88446.376|41659.611|15266975744|
|2048|Vortex|3917795072|43817.920|24099.541|17076637696|

Budgets/artifacts/data/method unchanged: actual16GiBheap/4gmanaged/32GiBreactive
guard, original8MiB app78e042... and native/library provenance, two slots/four
partitions, one population and one warmup/two measured reads per projection/mode.
CPU-only cases have6reads/7matchingprogress events/8storage snapshots; GPU-input
cases have12reads and14snapshots (new progress records in largest cases).
Lead independently ran raw-case/progress checks, exact aggregate comparison across
all six cases per size, every phase table, and final artifact-closure verification.
No machine OOM or normal-budget guard/JVM failure occurred. All owned benchmark
processes exited; ps and nvidia-smi compute-process query showed none remaining.

### Material encoding-policy qualification

Lead and worker read the decisive actual CPU/GPU PCBS writer paths. CPU
ParquetOutputFileFormat.getRecordWriter explicitly uses UNCOMPRESSED; GPU
getParquetWriterOptions selects SNAPPY when cache compression is enabled, as in
these tests. CPU Configuration(false) does not copy normal Hadoop codec options.
Git grep of main shows the same CPU UNCOMPRESSED call at line1412, so it is a
released implementation fact, not introduced by this experiment. This adds
evidence to the established release record; it does not invent a byte-identical
encoding compatibility requirement. No production policy or configuration changed.

Separate implementations also have different delegated dictionary/page policies.
No actual footer/page audit or compression-matched CPU control was performed:
do not attribute the measured byte gap solely to Snappy, dictionary choices or
format-intrinsic compression. Parquet value encodings still apply with the
UNCOMPRESSED codec. Forced Vortex strategies remain unexposed by Java/JNI and
untested. Source evidence, hardware path distinctions and limitations are now
prominent in the report.

### Verified artifacts and review evidence

Final report has separate population, full/narrow first/warm read-only and clearly
COMPUTED population-plus-read tables, plus encoded payload, storage-accounted
resident bytes, represented logical bytes, disk and sampled RSS. Overview shows
largest CPU-only and GPU-enabled cases without mixing modes, budgets or read
semantics. The CSV has72consumer/projection rows for24underlyingcases; population/
payload/storage attributes repeat per cache and must not be summed as new data.

CSV SHA256 d6939ff3fd4c33dbe6bb2a372e5c6b6f93c423b02b42581a8d8f7b399ead8a99.
CSV uses LF line endings; Git whitespace checks pass without policy overrides.
An independent worker recomputed1512fields from original JSON/manifests/source
snapshots, preserving Python-int aggregate precision. Lead read that checker and
independently reran it: PASS,24cases/72rows/errors[]. Evidence:
 /data/tmp/review-cache-csv-independent.py and .json.
All72detail phase rows, rounded report values and combined sums reproduced with
verify-primary-report.py and verify-cpu-primary-report.py for all four sizes.
Final runtime closure verification PASS; original app/native/dependencies unchanged.
Review found no material numerical or scope issue. One wording finding changed
'without spilling' to the evidenced absence of disk-backed cache blocks at recorded
snapshots. Final tracker status updated from historical in-progress checkpoints.

Remaining unverified criteria are explicit: primary CPU-produced→GPU-consumed
performance, unsupported-type/decimal/timestamp/nested Vortex coverage, additional
Spark versions, actual decoded IPC batch counts, bridge component profiling,
forced/compression-matched encoding controls, larger managed-cap control, total
native live allocation/endurance/maximum-capacity guarantees. These are follow-up
evidence, not claimed completed. Next useful experiments are compression-matched
CPU Parquet and Arrow bridge profiling/streaming before a format adoption decision.

All project operations, worker tasks, resumption and checkpoints used MCP sandbox
tools. No production serializer source, public configuration, Git local config or
released contract changed. All task commits are signed off; no PR was published.
Architect final acceptance remains outstanding; implementation/results are READY
FOR REVIEW, not DONE. No extraencodingpolicy/batch-size optimization authorized
as a substitute for completing these baseline measurements.

### Pinned2GiB worker case 256MiB/task default: READY FOR REVIEW

MCP docker_exec only; original78e042 artifact and released contracts preserved.
Exact reference aggregates/readers/provenance/budgets/audit checked; actual2048MiB
pinned pool startup recorded. Evidence /data/tmp/cache-pinned2g-16g/256/default-gpu/worker-verified.json.
Worker summary: {"status":"PASS","format":"default","task_mib":256,"output":"/data/tmp/cache-pinned2g-16g/256/default-gpu","logical_bytes":1073741824,"payload_bytes":435890015,"records":420,"population_ms":19532.499451,"rss_peak_bytes":4938690560,"snapshot_count":14,"pool_exhaustion_lines":33,"reads":[{"mode":"cpu","narrow":false,"first_ms":1679.235396,"median_ms":1487.18586},{"mode":"cpu","narrow":true,"first_ms":190.684469,"median_ms":102.66789750000001},{"mode":"gpu","narrow":false,"first_ms":859.391117,"median_ms":572.678949},{"mode":"gpu","narrow":true,"first_ms":137.489368,"median_ms":133.0502785}],"host_available_before":187543429120,"resident_storage_bytes":437697968,"resident_logical_bytes":1073741824,"disk_storage_bytes":0}
Lead independent verification/final acceptance remains outstanding.

### Pinned2GiB worker case 256MiB/task pcbs: READY FOR REVIEW

MCP docker_exec only; original78e042 artifact and released contracts preserved.
Exact reference aggregates/readers/provenance/budgets/audit checked; actual2048MiB
pinned pool startup recorded. Evidence /data/tmp/cache-pinned2g-16g/256/pcbs-gpu/worker-verified.json.
Worker summary: {"status":"PASS","format":"pcbs","task_mib":256,"output":"/data/tmp/cache-pinned2g-16g/256/pcbs-gpu","logical_bytes":1073741824,"payload_bytes":513454580,"records":140,"population_ms":1531.180698,"rss_peak_bytes":5236621312,"snapshot_count":14,"pool_exhaustion_lines":0,"reads":[{"mode":"cpu","narrow":false,"first_ms":6217.567998,"median_ms":5423.882254},{"mode":"cpu","narrow":true,"first_ms":4195.025585,"median_ms":4148.281150000001},{"mode":"gpu","narrow":false,"first_ms":460.209946,"median_ms":262.350739},{"mode":"gpu","narrow":true,"first_ms":163.851699,"median_ms":143.688081}],"host_available_before":187647840256,"resident_storage_bytes":513461272,"resident_logical_bytes":1073741824,"disk_storage_bytes":0}
Lead independent verification/final acceptance remains outstanding.

### Pinned2GiB worker case 256MiB/task vortex: READY FOR REVIEW

MCP docker_exec only; original78e042 artifact and released contracts preserved.
Exact reference aggregates/readers/provenance/budgets/audit checked; actual2048MiB
pinned pool startup recorded. Evidence /data/tmp/cache-pinned2g-16g/256/vortex-gpu/worker-verified.json.
Worker summary: {"status":"PASS","format":"vortex","task_mib":256,"output":"/data/tmp/cache-pinned2g-16g/256/vortex-gpu","logical_bytes":1073741824,"payload_bytes":489897376,"records":128,"population_ms":5894.824981,"rss_peak_bytes":7911780352,"snapshot_count":14,"pool_exhaustion_lines":0,"reads":[{"mode":"cpu","narrow":false,"first_ms":3013.353489,"median_ms":2905.849256},{"mode":"cpu","narrow":true,"first_ms":306.437923,"median_ms":347.043055},{"mode":"gpu","narrow":false,"first_ms":1340.001668,"median_ms":1169.2595805},{"mode":"gpu","narrow":true,"first_ms":266.640512,"median_ms":228.282546}],"host_available_before":187580043264,"resident_storage_bytes":489903072,"resident_logical_bytes":1073741824,"disk_storage_bytes":0}
Lead independent verification/final acceptance remains outstanding.

### Pinned2GiB worker case 512MiB/task default: READY FOR REVIEW

MCP docker_exec only; original78e042 artifact and released contracts preserved.
Exact reference aggregates/readers/provenance/budgets/audit checked; actual2048MiB
pinned pool startup recorded. Evidence /data/tmp/cache-pinned2g-16g/512/default-gpu/worker-verified.json.
Worker summary: {"status":"PASS","format":"default","task_mib":512,"output":"/data/tmp/cache-pinned2g-16g/512/default-gpu","logical_bytes":2147483648,"payload_bytes":871780099,"records":840,"population_ms":38719.420155,"rss_peak_bytes":7443652608,"snapshot_count":14,"pool_exhaustion_lines":37,"reads":[{"mode":"cpu","narrow":false,"first_ms":3002.440311,"median_ms":2764.8142635},{"mode":"cpu","narrow":true,"first_ms":218.847945,"median_ms":128.2925775},{"mode":"gpu","narrow":false,"first_ms":1411.93787,"median_ms":1069.827001},{"mode":"gpu","narrow":true,"first_ms":207.9447,"median_ms":173.59348}],"host_available_before":187526320128,"resident_storage_bytes":875395032,"resident_logical_bytes":2147483648,"disk_storage_bytes":0}
Lead independent verification/final acceptance remains outstanding.

### Pinned2GiB worker case 512MiB/task pcbs: READY FOR REVIEW

MCP docker_exec only; original78e042 artifact and released contracts preserved.
Exact reference aggregates/readers/provenance/budgets/audit checked; actual2048MiB
pinned pool startup recorded. Evidence /data/tmp/cache-pinned2g-16g/512/pcbs-gpu/worker-verified.json.
Worker summary: {"status":"PASS","format":"pcbs","task_mib":512,"output":"/data/tmp/cache-pinned2g-16g/512/pcbs-gpu","logical_bytes":2147483648,"payload_bytes":1026899551,"records":280,"population_ms":1871.449762,"rss_peak_bytes":8676737024,"snapshot_count":14,"pool_exhaustion_lines":0,"reads":[{"mode":"cpu","narrow":false,"first_ms":11624.973828,"median_ms":10339.6714105},{"mode":"cpu","narrow":true,"first_ms":7810.706555,"median_ms":7786.411474},{"mode":"gpu","narrow":false,"first_ms":602.899804,"median_ms":458.3164305},{"mode":"gpu","narrow":true,"first_ms":251.89995,"median_ms":219.944318}],"host_available_before":187639808000,"resident_storage_bytes":1026912888,"resident_logical_bytes":2147483648,"disk_storage_bytes":0}
Lead independent verification/final acceptance remains outstanding.

### Pinned2GiB worker case 512MiB/task vortex: READY FOR REVIEW

MCP docker_exec only; original78e042 artifact and released contracts preserved.
Exact reference aggregates/readers/provenance/budgets/audit checked; actual2048MiB
pinned pool startup recorded. Evidence /data/tmp/cache-pinned2g-16g/512/vortex-gpu/worker-verified.json.
Worker summary: {"status":"PASS","format":"vortex","task_mib":512,"output":"/data/tmp/cache-pinned2g-16g/512/vortex-gpu","logical_bytes":2147483648,"payload_bytes":979946776,"records":256,"population_ms":9966.956979,"rss_peak_bytes":9308450816,"snapshot_count":14,"pool_exhaustion_lines":0,"reads":[{"mode":"cpu","narrow":false,"first_ms":5925.553031,"median_ms":5429.9417095},{"mode":"cpu","narrow":true,"first_ms":383.139081,"median_ms":648.519423},{"mode":"gpu","narrow":false,"first_ms":3151.084349,"median_ms":2415.9725295},{"mode":"gpu","narrow":true,"first_ms":368.077067,"median_ms":378.745828}],"host_available_before":187657662464,"resident_storage_bytes":979958104,"resident_logical_bytes":2147483648,"disk_storage_bytes":0}
Lead independent verification/final acceptance remains outstanding.

### Pinned2GiB worker case 1024MiB/task default: READY FOR REVIEW

MCP docker_exec only; original78e042 artifact and released contracts preserved.
Exact reference aggregates/readers/provenance/budgets/audit checked; actual2048MiB
pinned pool startup recorded. Evidence /data/tmp/cache-pinned2g-16g/1024/default-gpu/worker-verified.json.
Worker summary: {"status":"PASS","format":"default","task_mib":1024,"output":"/data/tmp/cache-pinned2g-16g/1024/default-gpu","logical_bytes":4294967296,"payload_bytes":1743560044,"records":1680,"population_ms":79209.949277,"rss_peak_bytes":16868012032,"snapshot_count":14,"pool_exhaustion_lines":39,"reads":[{"mode":"cpu","narrow":false,"first_ms":5794.587066,"median_ms":5562.287463500001},{"mode":"cpu","narrow":true,"first_ms":272.866975,"median_ms":177.92969649999998},{"mode":"gpu","narrow":false,"first_ms":2733.638903,"median_ms":2623.9239509999998},{"mode":"gpu","narrow":true,"first_ms":198.801473,"median_ms":350.793324}],"host_available_before":187611234304,"resident_storage_bytes":1662574592,"resident_logical_bytes":4294967296,"disk_storage_bytes":0}
Lead independent verification/final acceptance remains outstanding.

### Pinned2GiB worker case 1024MiB/task pcbs: READY FOR REVIEW

MCP docker_exec only; original78e042 artifact and released contracts preserved.
Exact reference aggregates/readers/provenance/budgets/audit checked; actual2048MiB
pinned pool startup recorded. Evidence /data/tmp/cache-pinned2g-16g/1024/pcbs-gpu/worker-verified.json.
Worker summary: {"status":"PASS","format":"pcbs","task_mib":1024,"output":"/data/tmp/cache-pinned2g-16g/1024/pcbs-gpu","logical_bytes":4294967296,"payload_bytes":2053797644,"records":560,"population_ms":2788.434355,"rss_peak_bytes":11135090688,"snapshot_count":14,"pool_exhaustion_lines":0,"reads":[{"mode":"cpu","narrow":false,"first_ms":21704.989952,"median_ms":20289.086413},{"mode":"cpu","narrow":true,"first_ms":15668.842329,"median_ms":15691.7482385},{"mode":"gpu","narrow":false,"first_ms":827.989689,"median_ms":787.410743},{"mode":"gpu","narrow":true,"first_ms":345.135374,"median_ms":351.5103255}],"host_available_before":187548172288,"resident_storage_bytes":2053824352,"resident_logical_bytes":4294967296,"disk_storage_bytes":0}
Lead independent verification/final acceptance remains outstanding.

### Pinned2GiB worker case 1024MiB/task vortex: READY FOR REVIEW

MCP docker_exec only; original78e042 artifact and released contracts preserved.
Exact reference aggregates/readers/provenance/budgets/audit checked; actual2048MiB
pinned pool startup recorded. Evidence /data/tmp/cache-pinned2g-16g/1024/vortex-gpu/worker-verified.json.
Worker summary: {"status":"PASS","format":"vortex","task_mib":1024,"output":"/data/tmp/cache-pinned2g-16g/1024/vortex-gpu","logical_bytes":4294967296,"payload_bytes":1959914440,"records":512,"population_ms":18752.621466,"rss_peak_bytes":12832215040,"snapshot_count":14,"pool_exhaustion_lines":0,"reads":[{"mode":"cpu","narrow":false,"first_ms":11928.543498,"median_ms":10736.320306000001},{"mode":"cpu","narrow":true,"first_ms":1118.591905,"median_ms":973.2702235},{"mode":"gpu","narrow":false,"first_ms":4411.993613,"median_ms":4060.4372045},{"mode":"gpu","narrow":true,"first_ms":497.102917,"median_ms":763.6122915}],"host_available_before":187725426688,"resident_storage_bytes":1959937032,"resident_logical_bytes":4294967296,"disk_storage_bytes":0}
Lead independent verification/final acceptance remains outstanding.

### Pinned2GiB worker case2048MiB/task Default: PARKED failure

MCP docker_exec only. Original78e042 artifact, actual16GiBheap/4GiBmanaged
host/2GiBpinned/2018MiBnonpinned/30MiBoverhead/32GiBguard verified. Population
failed with CpuRetryOOM when GPU column-to-row copyToHost requested2147475456B;
HostAlloc logged allocation refusal. RSS peak3424374784B; guard did not trip.
No completed population/read/storage/audit outcome or encodedpayload measured.
No retry or budget relaxation. Evidence /data/tmp/cache-pinned2g-16g/2048/default-gpu/
worker-failure-verification.json plus exact manifests/result/stack/logs preserved.
Independent2048PCBS/Vortex cases continue as authorized; do not reportall12PASS.

### Pinned2GiB worker case 2048MiB/task pcbs: READY FOR REVIEW

MCP docker_exec only; original78e042 artifact and released contracts preserved.
Exact reference aggregates/readers/provenance/budgets/audit checked; actual2048MiB
pinned pool startup recorded. Evidence /data/tmp/cache-pinned2g-16g/2048/pcbs-gpu/worker-verified.json.
Worker summary: {"status":"PASS","format":"pcbs","task_mib":2048,"output":"/data/tmp/cache-pinned2g-16g/2048/pcbs-gpu","logical_bytes":8589934592,"payload_bytes":4107660739,"records":1124,"population_ms":4512.125605,"rss_peak_bytes":12090564608,"snapshot_count":14,"pool_exhaustion_lines":2,"reads":[{"mode":"cpu","narrow":false,"first_ms":42667.214093,"median_ms":41437.66351300001},{"mode":"cpu","narrow":true,"first_ms":31420.528025,"median_ms":31427.1250805},{"mode":"gpu","narrow":false,"first_ms":1487.421849,"median_ms":1090.177071},{"mode":"gpu","narrow":true,"first_ms":512.790032,"median_ms":522.015491}],"host_available_before":187679199232,"resident_storage_bytes":4107714192,"resident_logical_bytes":8589934592,"disk_storage_bytes":0}
Lead independent verification/final acceptance remains outstanding.

### Pinned2GiB worker case 2048MiB/task vortex: READY FOR REVIEW

MCP docker_exec only; original78e042 artifact and released contracts preserved.
Exact reference aggregates/readers/provenance/budgets/audit checked; actual2048MiB
pinned pool startup recorded. Evidence /data/tmp/cache-pinned2g-16g/2048/vortex-gpu/worker-verified.json.
Worker summary: {"status":"PASS","format":"vortex","task_mib":2048,"output":"/data/tmp/cache-pinned2g-16g/2048/vortex-gpu","logical_bytes":8589934592,"payload_bytes":3919838784,"records":1024,"population_ms":36847.511711,"rss_peak_bytes":16960266240,"snapshot_count":14,"pool_exhaustion_lines":0,"reads":[{"mode":"cpu","narrow":false,"first_ms":28254.215511,"median_ms":27068.816574999997},{"mode":"cpu","narrow":true,"first_ms":1699.681647,"median_ms":1748.4573814999999},{"mode":"gpu","narrow":false,"first_ms":10229.768138,"median_ms":9126.903113},{"mode":"gpu","narrow":true,"first_ms":1313.255089,"median_ms":1431.7087385}],"host_available_before":187741192192,"resident_storage_bytes":3919883904,"resident_logical_bytes":8589934592,"disk_storage_bytes":0}
Lead independent verification/final acceptance remains outstanding.
