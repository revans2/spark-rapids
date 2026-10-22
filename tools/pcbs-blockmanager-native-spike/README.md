# BlockManager / RAPIDS native lifetime research fixture

Status: READY FOR REVIEW. Research code only, not a production storage provider.

This uses the Stage A Spark 3.5.5 codec overlay and real RAPIDS `SpillableHostBuffer`
values in ordinary **deserialized MEMORY_AND_DISK** MemoryStore entries. It
manually initializes RAPIDS HostAlloc/SpillFramework; SQLPlugin is disabled.
No Spark OFF_HEAP allocator, reservation, storage level or pool is used.

The fixture checks deterministic 32 KiB native buffers through:

- normal BlockManager local storage;
- forced RAPIDS host-to-disk spill and native restore;
- actual BlockManager memory-to-disk eviction and reload through the custom codec;
- blocking master removeRdd while a local iterator holds the real read lock;
- a separately materialized HostMemoryBuffer reference surviving entry removal;
- two executor JVMs producing native values, remote fetch/decode on the driver,
  and per-executor owner/host-store/disk-store cleanup after removal.

Driver owner counts must balance, spill files must disappear, executor owner counts
must balance, and Spark off-heap storage use must stay zero. HostAlloc has a 64 MiB
pageable limit and no pinned pool. An over-limit allocation is explicitly rejected.
One 1 MiB host bounce buffer remains after payload cleanup; framework shutdown
returns driver host usage to zero. The framework also initializes one 1 MiB device
bounce buffer; there are no query GPU operators or benchmark workloads.

The runner snapshots source, hashes supplied runtime/codec/RAPIDS artifacts, and
requires every acceptance marker plus a Spark 3.5.5 event log with normal
ApplicationEnd and zero task failures. A writable symlink runtime keeps installed
Spark unchanged.

```sh
python tools/pcbs-blockmanager-native-spike/run_spike.py \
  --spark-home /home/roberte/src/spark_3.5.5 \
  --codec-jar /data/pcbs-oom/blockmanager-codec-spike/cluster4/blockmanager-codec-spike.jar \
  --rapids-jar /data/pcbs-oom/pcbs-shadow-experiment/dist/target/rapids-4-spark_2.12-26.12.0-SNAPSHOT-cuda12.jar \
  --output-dir /data/pcbs-oom/blockmanager-native-spike/new-run \
  --master 'local-cluster[2,1,1024]'
```

`--output-dir` must not already exist. `--master local[2]` runs the local subset.
The supplied RAPIDS artifact is an existing experimental build: this fixture does
not exercise its unrelated Parquet registry. To use a different build, record the
new artifact and independently rerun the acceptance checks. Research flattening
exposes its already built spark355/shared classes without enabling SQLPlugin.

Root independently verified the frozen source with the final codec overlay at
`/data/pcbs-oom/blockmanager-native-spike/root-final-overlay/result.json`.
Earlier worker run: `/data/pcbs-oom/blockmanager-native-spike/cluster3/result.json`.
Driver owners 4/4; each executor 1/1; all payload stores empty; driver/executor
Spark off-heap use zero; 1 normal ApplicationEnd and no task failures.

## Limits of this proof

This is direct BlockManager CachedBatch-like value lifetime research, not SQL plan
integration, a complete native store, native status/accounting adoption, PCBS codec,
heap-scaling/performance comparison or supported API. Spark charges normal heap
metadata while RAPIDS owns/limits native bytes. BlockManager still reports its
existing memory/disk entries, not complete RAPIDS resident/spilled payload bytes.
Its existing remote sender may materialize serialized partition heap chunks;
bounded end-to-end transfers are not established here.

The fixture does not validate other StorageLevels, disabled host-allocation limits,
cap-aware receive builders, payload cancellation/partial consumption cleanup,
failed admission, replication/decommission native ownership or newer Spark.
Those remain explicit follow-on criteria; do not infer them from this pass.
