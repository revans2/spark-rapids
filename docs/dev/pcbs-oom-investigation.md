# PCBS heap OOM investigation (#16362)

Status: READY FOR REVIEW. Reproduction and controls verified; independent
review completed. No production fix selected. Architect acceptance pending.

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
GPUconcurrent2/max2,hostoffheaplimit40g,pinnedpool2g. A6000 GPU pool12g/max14g
approximates T4 capacity, not T4 throughput. Same installed Spark3.5.5 and plugin
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
continue through MCP docker_exec. Full16partition reset comparison pending.

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
