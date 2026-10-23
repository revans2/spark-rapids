# Constant-total-data cache codec comparison

Status: READY FOR REVIEW. Worker and independent root reverse-order runs
and eight-case evidence audits completed; paired results are recorded below.

This adapts the earlier benefit_measurement fixture into a separate namespace
and preserves its evidence. Both batch sizes now use exactly **96 GiB total**,
16 partitions and **6 GiB per partition**, with identical logical input and
concatenated payload bytes.

The input is 384 Long unit seeds, evenly divided into 24 seeds per partition.
Each seed represents a fixed 256 MiB byte region filled with that seed's byte.
The 256 MiB cache record contains one region; the 1 GiB record contains four
consecutive regions. Primitive Long values are extracted before grouping so
mutable InternalRow reuse cannot change the contents.

| Target record | Regions per record | Records per partition | Total records | Total logical SQL rows | Total payload |
| --- | ---: | ---: | ---: | ---: | ---: |
| 256 MiB | 1 | 24 | 384 | 384 | 96 GiB |
| 1 GiB | 4 | 6 | 96 | 384 | 96 GiB |

CachedBatch.numRows reports the represented region count, and the decoder
validates every payload byte before reconstructing all represented seed rows.
Every completed phase checks the same 384 SQL rows and exactly 103,079,215,104
payload bytes. SQL accumulator statistics, actual BlockManager disk sizes,
record counts and codec/Java path counters are checked.

This remains a synthetic padded-seed cache format, not Parquet, PCBS or GPU
processing. Both modes use the same heap-backed serializable record.
DISK_ONLY isolates generic Java serialization retention; it does not test
MEMORY_AND_DISK unroll pressure or establish a fix for the original PCBS OOM.

Controls remain local[8], configured 48 GiB heap, ParallelGC, default memory
fraction 0.6, default unroll period 16 and default Java reset 100. There is one
fill, one warmup and three measured reads per fresh JVM, with no concurrent
benchmarks. Fill includes generation, serialization, disk materialization,
first decode and full-byte validation. Heap/post-GC values are occupancy, not
a measured live set. The initial MXBean heap maximum can change with ParallelGC
survivor ergonomics.

Use only MCP docker_exec for project operations. Compile without running:

~~~bash
python tools/pcbs-blockmanager-codec-spike/confidence/constant_data_benefit/run_benefit.py \
  --base-run /data/pcbs-oom/blockmanager-codec-spike/root-acceptance355 \
  --spark-home /home/roberte/src/spark_3.5.5 \
  --output-dir /data/pcbs-oom/blockmanager-codec-confidence/constant-data/new-compile \
  --mode codec --record-mib 256 --measured-reads 3 --compile-only
~~~

For a full case use a fresh output directory and replace --compile-only with
--remove-block-files. Select mode codec/java and record-mib 256/1024.
Root and independent peer source reviews passed before full runs. The worker and root run cases
serially; independent root repetition reverses mode order.

The per-case cap is 1,200 seconds. Explicit Java heap OOM is a classified
negative experiment, not completed cache correctness. Other non-fixture
failures/timeouts park the work. Immutable source/runtime/artifact and raw
log/event/GC hashes are retained; raw local block files can be removed only
after verification and a cleanup record. No native storage, production change
or public configuration is introduced.

Re-audit preserved evidence using verify_benefit.py --run-dir CASE_DIRECTORY.


## Worker observations

All four fresh worker JVMs used the same frozen runtime sources. Codec then
Java ran at 256 MiB, followed by codec then Java at 1 GiB. No benchmarks ran
concurrently. The complete controls, measurements, raw evidence directories
and verification hashes are in worker-results.json.

| Record size | Mode | Fill seconds | Median read seconds | GC across three reads, ms | Sampled peak heap occupancy, GiB | Outcome |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| 256 MiB | codec | 73.022 | 18.191 | 3314 | 28.65 | Completed |
| 256 MiB | java | Not completed | Not reached | Not available | 38.06 | Heap OOM during fill |
| 1024 MiB | codec | 80.614 | 25.926 | 15216 | 45.38 | Completed |
| 1024 MiB | java | Not completed | Not reached | Not available | 31.05 | Heap OOM during fill |

Both completed codec cases validated exactly 103,079,215,104 payload bytes
and the same 384 SQL seed rows per phase, with 16 published blocks and 80
successful tasks. The 256 MiB case had 384 records per pass; the 1 GiB case
had 96. Across fill, warmup and three measured reads this is the same
515,396,075,520 validated payload bytes and 1,920 logical seed rows in each case.

Measured BlockManager disk sizes were 103,079,219,712 bytes for 256 MiB records
and 103,079,216,256 bytes for 1 GiB records. The 3,456-byte difference is record
framing, not different payload content or total data.

Both Java cases failed during fill at the producer's new Array[Byte](bytes),
ConstantDataBenefitSpike.scala line 129, before publishing any cache block.
Neither reached a completed read phase; these are classified negative
experiments, not successful cache correctness or measurements of a Java
deserialization failure.

The 256 MiB Java case's last successful progress sample recorded 171 generated
records (45,902,462,976 bytes), 165 Java writes and no decoded records. Observed
GC notifications totaled 78 events and 53,486 ms before termination. The
1 GiB Java case recorded 24 generated/written records (25,769,803,776 bytes)
and no decoded records; nine observed GC notifications totaled 4,982 ms.
These are partial observations, not completed fill-phase metrics.

The codec cases recorded zero generic Java bulk reads or writes. These
observations support a codec benefit for this heap-backed DISK_ONLY workload.
They do not prove a production PCBS performance improvement or solve
MEMORY_AND_DISK unroll pressure. Heap peaks include garbage; a failed Java
case's lower peak than a completed codec case is not evidence that it needs
less memory. All runs were configured with 48 GiB heap; the initial MXBean
usable maximum was 45,813,334,016 bytes and can change with ParallelGC
ergonomics. No runtime setting was changed between cases.

All worker process timeout flags are false. Only gc.log was present in each
case, without rotated files. Raw logs/events/GC and provenance remain;
verified raw local block cleanup was recorded. Prior benefit_measurement
evidence remains unchanged and must not be combined with this constant-data
comparison.

Independent root repeat and eight-case raw/hash audit completed. Paired results
are in independently-verified-results.json; observations are READY FOR REVIEW.
