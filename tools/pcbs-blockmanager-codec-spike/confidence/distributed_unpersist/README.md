# Distributed blocking SQL unpersist confidence proof

Status: READY FOR REVIEW. Heap-codec mechanism only; no native storage work.

A fresh Spark 3.5.5 application uses two executor JVMs and actual DataFrame
`persist(MEMORY_AND_DISK_2)`. Its single SQL cache partition contains 1,000 rows
and has two executor-local replicas. A normal RDD traversal of the actual SQL
cached wrapper consumes the first cached batch and pauses while its real
BlockManager read lock is held.

The reader waits through an addressed DRIVER test gate; no addressless endpoint
reference is forwarded between executors. The driver invokes actual
`frame.unpersist(blocking = true)` on a separate thread. It must observe both
`Dataset.unpersist` and `BlockManagerMaster.removeRdd` in that thread's live stack,
then establish that the call does not finish while the reader is held. After
releasing the reader, both the exact full-data read and SQL unpersist must finish
within 30 seconds. All master locations and both executor-local copies must be
absent afterward; CacheManager must no longer contain the DataFrame.

The fixture reuses D's inspection plugin jar for endpoint registration and local
absence checks without payload encoding. It does not invoke manual removeRdd as
its removal action. Raw driver/worker markers, the matching unpersist-RDD event,
runtime version, normal ApplicationEnd and zero failed tasks are audited.

```sh
python tools/pcbs-blockmanager-codec-spike/confidence/distributed_unpersist/run_spike.py \
  --spark-home /home/roberte/src/spark_3.5.5 \
  --codec-jar /data/pcbs-oom/blockmanager-codec-spike/root-acceptance355/blockmanager-codec-spike.jar \
  --inspection-jar /data/pcbs-oom/blockmanager-codec-confidence/D/trial2/clean-receiver-spike.jar \
  --output-dir /data/pcbs-oom/blockmanager-codec-confidence/G/new-run
```

Output directory must be new. Source snapshots, source/runtime/dependency hashes,
event logs, result.json and verifier.json are retained. Installed Spark and both
supplied dependency jars are unchanged. Worker verified run:
`/data/pcbs-oom/blockmanager-codec-confidence/G/trial1`.

This proves successful distributed blocking SQL removal with a held cached
reader in this bounded fixture. It does not establish cancellation/decode-failure
cleanup, all StorageLevels/versions, native-reader ownership, timeout/failure
semantics, a supported ClassTag API or production deployment.
