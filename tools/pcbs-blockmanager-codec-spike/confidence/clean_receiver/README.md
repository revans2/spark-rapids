# Clean replica receiver confidence fixture

Status: READY FOR REVIEW. D-only research proof; no native memory/backend work.

Fresh Spark 3.5.5 JVMs, three executors, exactly one SQL cache with one partition
and deserialized MEMORY_AND_DISK_2. The selected existing research SQL codec
replicates 1,000 rows / 2,000 exact Long values. The source computation records
its executor identity and must execute exactly once across both SQL reads.

A small test-only ExecutorPlugin registers an inspection endpoint. The driver
uses each executor's endpoint reference only on its original connection; it
never forwards addressless client references to another executor. Inspection
reads each replica locally and checks every value without returning or
reserializing the payload. Thus the clean receiving executor must have:

- no source computation;
- no codec WRITE anywhere in its complete log;
- exactly one codec READ: eight records, two columns, taskContext=false;
- exactly 1,000 rows and 2,000 correct values in the local replica.

The producing executor must be identified independently by the source
accumulator/log and advertise one of the two replica locations. The third
executor must hold no block and have no cache codec reads/writes. All three
inspection results, exact replication locations, SQL stats, Java/Kryo tripwires,
normal ApplicationEnd and zero failed tasks are asserted.

```sh
python tools/pcbs-blockmanager-codec-spike/confidence/clean_receiver/run_spike.py \
  --spark-home /home/roberte/src/spark_3.5.5 \
  --codec-jar /data/pcbs-oom/blockmanager-codec-spike/root-acceptance355/blockmanager-codec-spike.jar \
  --output-dir /data/pcbs-oom/blockmanager-codec-confidence/D/new-run
```

Output directory must be new. The runner snapshots sources, hashes source,
codec/fixture/runtime artifacts, retains logs/events and writes result.json plus
verifier.json. Installed Spark and the supplied codec jar remain unchanged.

Verified worker run: `/data/pcbs-oom/blockmanager-codec-confidence/D/trial2`.
The first bootstrap attempt failed before any cache operation because the test
assumed executor RPC environments had public addresses. The corrected fixture
uses direct, original-connection endpoint references. No storage change or
acceptance relaxation was required.

This proves independent replica codec context for this bounded SQL fixture.
It does not establish native storage, performance, all version/storage-level
compatibility, a supported ClassTag API or a production Spark patch.
