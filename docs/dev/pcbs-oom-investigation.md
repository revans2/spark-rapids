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
change a production setting globally. No end-to-end SQL performance or custom
serializer lifetime contracts have been evaluated.

## Reset interval performance sweep (IN PROGRESS)

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

Only the diagnostic and this tracker are included in the signed-off commit.
Production fixes, end-to-end performance testing, repeatability statistics,
customer-specific sizing, serialized-memory controls and off-heap ownership
remain for architect discussion. This task is READY FOR REVIEW, not DONE.
