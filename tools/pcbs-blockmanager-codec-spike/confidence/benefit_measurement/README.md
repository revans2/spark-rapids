# Codec versus generic Java cache IO benefit measurement

Status: READY FOR REVIEW. Worker and root reverse-order measurements agree
on outcomes; paired results are in independently-verified-results.json and
the committed tracker. This remains bounded research.

This compares two encodings of exactly the same heap-backed CachedBatch object.
The default object is genuinely Java-serializable. The optional per-cache codec
writes its seed, length and existing byte array directly through Spark's
streaming Serializer interface. Neither mode uses native payload storage.

The SQL input is 96 Long seed rows across 16 range partitions. The cache format
expands each seed to a synthetic encoded blob of a measured byte-array length,
filled with the seed byte. Each task caches six blobs. This is deliberately a
storage/serialization mechanism benchmark, not Parquet, PCBS, realistic SQL row
width, GPU processing or a production speed estimate. Compression is disabled
in both modes, so repeated content is not compressed away.

Controls are identical: fresh JVM, local[8], configured 48 GiB heap,
ParallelGC, DISK_ONLY, default memory fraction 0.6, default unroll period 16 and
default Java stream reset 100. There is no memory-fraction/reset/unroll tuning.
The fixture-only spark.codecBenefit.recordBytes argument has no production
configuration contract.

DISK_ONLY ensures real byte encoding/decoding on every cache access and isolates
generic Java handle retention from MEMORY_AND_DISK unrolling. Passing this test
does not establish a fix for the original PCBS OOM.

Run each case separately through MCP docker_exec; do not run cases concurrently:

~~~bash
python tools/pcbs-blockmanager-codec-spike/confidence/benefit_measurement/run_benefit.py \
  --base-run /data/pcbs-oom/blockmanager-codec-spike/root-acceptance355 \
  --spark-home /home/roberte/src/spark_3.5.5 \
  --output-dir /data/pcbs-oom/blockmanager-codec-confidence/A/new-case \
  --mode codec --record-mib 256 --measured-reads 3 --remove-block-files
~~~

Use mode java or codec and record-mib 256 or 1024. The worker ran codec then Java
at each size; the independent root run reverses that order. The default
per-process cap is 1,200 seconds.

Each completed case fills the cache once, performs one warmup and three measured
reads, checking every payload byte and all 96 returned seed values every time.
**Fill includes generation, serialization, disk materialization, first decode
and full-byte validation.** It is not pure encoder time. Published BlockManager
disk bytes and filled partitions are recorded independently of target sizes.
Path counters require Java-only bulk IO or codec-only bulk IO as selected.

Heap usage is sampled every 20 ms; one-second progress records preserve partial
metrics if the process OOMs. GC bean deltas are the per-phase GC time/count
measurements. GC notifications and unified GC logs are also retained; notification
phase labels can lag asynchronously across a phase boundary. Heap peaks and
post-GC occupancy are not live-set measurements. The initial MXBean maximum was
45,813,334,016 bytes despite a 48 GiB configured heap; ParallelGC's usable maximum
can change with survivor-space ergonomics, so it is an initial observation,
not a fixed ceiling for later samples.

Worker final results are in worker-results.json. Preliminary worker-256-codec
and worker-256-java outputs predate the final instrumentation and are excluded.

| Record size | Mode | Fill seconds | Median measured read seconds | GC ms across 3 reads | Sampled heap peak GiB | Outcome |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| 256 MiB | Codec | 14.144 | 5.046 | 967 | 22.84 | Completed |
| 256 MiB | Java | 15.907 | 4.930 | 2,509 | 37.88 | Completed |
| 1 GiB | Codec | 86.899 | 34.281 | 12,294 | 43.46 | Completed |
| 1 GiB | Java | Not completed | Not reached | Not available | 31.11 before failure | Heap OOM during fill |

Completed 256 MiB cases each contain 25,769,803,776 payload bytes (24 GiB).
BlockManager disk totals were 25,769,804,928 bytes for the codec and
25,769,807,680 for Java. The completed 1 GiB codec case contains
103,079,215,104 payload bytes (96 GiB), with 103,079,216,256 disk bytes.
Each completed case published all 16 blocks and validated 480 records across
fill plus four reads, with 80 successful tasks and no task failures.

The 1 GiB Java case failed at new Array[Byte](bytes) in
JavaBlobSerializer.convertInternalRowToCachedBatch (source line 115), not in
ObjectInputStream. Its last successful progress sample recorded 24 generated
and 24 Java-written 1 GiB arrays, no decoded records and no published blocks.
Observed GC notifications reported nine events totaling 4,859 ms; this is partial
observed GC evidence, not a completed fill-phase metric. The final GC log shows
a 32 GiB old generation about 97% occupied after full collection, with the next
1 GiB allocation failing. The lower peak than the completed codec case reflects
early failure, not lower memory needs.

These are descriptive fresh-JVM observations, not confidence bounds. At 256 MiB,
read wall times are close despite less recorded GC for the codec. The large
case supports a serialization-retention benefit under these controls; it says
nothing by itself about MEMORY_AND_DISK unroll pressure.

Re-audit any completed or heap-OOM case without rerunning its workload:

~~~bash
python tools/pcbs-blockmanager-codec-spike/confidence/benefit_measurement/verify_benefit.py \
  --run-dir /data/pcbs-oom/blockmanager-codec-confidence/A/worker-1024-codec-final
~~~

The verifier rechecks source/runtime/artifact and raw log/event/GC hashes.
A valid heap_oom means a classified negative experiment, not successful cache
correctness. Other failures and timeouts without an explicit heap OOM do not pass;
a secondary timeout after an observed OOM remains visible in process metrics. The raw-block cleanup record
lists removed local paths and sizes after verification; raw content hashes were
not computed. Logs, manifests, events, GC logs and verification evidence remain.
All four worker cases had only gc.log, with no rotated GC files.
