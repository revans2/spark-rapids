# Disposable BlockManager codec research

This is an unreleased, small CPU-only mechanism fixture for the long-term
upstream Spark proposal. It does not change installed Spark or implement PCBS.
Spark's OFF_HEAP storage is excluded. The heap-backed first proof is separate
from the RAPIDS-owned native lifecycle fixture.

The generated Spark 3.5.5 overlay changes two methods: the actual SQL cache
statistics wrapper receives a per-cache codec context, and SerializerManager
recognizes it before ordinary serializer selection. A ClassTag subtype carries
the immutable streaming Serializer through existing BlockInfo, memory entries
and replica transfer metadata. This is a disposable research carrier, not a
proposed public ClassTag contract or accepted upstream API.

Run through the MCP docker_exec sandbox:

~~~bash
python tools/pcbs-blockmanager-codec-spike/run_spike.py \
  --spark-home /home/roberte/src/spark_3.5.5 \
  --output-dir /data/pcbs-oom/blockmanager-codec-spike/my-primary-run
python tools/pcbs-blockmanager-codec-spike/run_recovery.py \
  --base-run /data/pcbs-oom/blockmanager-codec-spike/my-primary-run \
  --output-dir /data/pcbs-oom/blockmanager-codec-spike/my-recovery-run
~~~

Each output directory must be new. The primary acceptance uses
local-cluster[2,1,1024]: a driver and two executor JVMs. It compiles an isolated
overlay against installed jars and places that overlay ahead of Spark's classes.
Its writable runtime is a symlink tree; original sources and jars remain intact.
The output retains source snapshots, generated patch, compile/run logs, worker
logs, event logs, manifests and result.json.

The primary checks cover two simultaneous codec/schema contexts with the same
runtime CachedBatch class; ordinary Java-object and primitive RDDs; an actual
default SQL statistics wrapper; Java/Kryo bulk-serialization tripwires;
serialized/deserialized memory; DISK_ONLY; MEMORY_AND_DISK_2 replicas; explicit
Netty reads with exact values; empty input; accurate SQL row/byte accumulators;
forced memory-to-disk eviction; a held reader delaying local removeRdd; and
failed serialized admission with a real prefix plus remainder through both
reconstruction paths. Positive replica decoder logs on both executors show
decoding outside task context. Both executors also produce partitions, so this
does not claim a receiver that never produced any partition for the cache.
The default SQL check constructs CachedRDDBuilder directly; it is not a second
public cache-serializer configuration in the same Spark application.

The recovery fixture deletes real shuffle data/index files on the shared local
filesystem, removes tracker map-output status, and removes a cached parent
after the scheduler has used its cache locations. Genuine missing-shuffle
metadata raises FetchFailed; normal Spark recovery rebuilds the map stage and
returns exact data. The test uses a routable driver control endpoint solely to
invoke existing blocking master.removeRdd and verify all cache locations are gone
before parent reads. Do not forward addressless executor endpoint refs to another
executor: NettyRpcEnv documents that these cannot be shared with third parties.
An earlier injection did that and could remove the wrong local block; its failed
root repeat is retained in root-recovery355, and is not Spark recovery evidence.
It also invokes the existing BlockManager decommission
endpoint, waits for a sample block to move off its original executor, compares
its payload before/after transfer, and checks the whole SQL result. This tests
RDD block migration, not executor termination or complete decommission policy.

The same primary proof has also been independently run on Spark 4.0.1:

~~~bash
python tools/pcbs-blockmanager-codec-spike/run_spike_401.py \
  --spark-home /home/roberte/src/spark_4.0.1 \
  --source-git /home/roberte/src/spark_3.5.5 \
  --output-dir /data/pcbs-oom/blockmanager-codec-spike/my-401-run
~~~

The adapter extracts exact upstream 4.0.1 source and adapts generated test copies
for its builder constructor and classic SparkSession. Shared sources are unchanged.
Committed verified-results.json records independently rerun primary/native cases;
verify_evidence.py rechecks source/artifact hashes and raw Spark events. Native
ownership is a separate fixture in tools/pcbs-blockmanager-native-spike.

~~~bash
python tools/pcbs-blockmanager-codec-spike/verify_evidence.py \
  --codec-355 /data/pcbs-oom/blockmanager-codec-spike/root-acceptance355 \
  --codec-401 /data/pcbs-oom/blockmanager-codec-spike/root-acceptance401/run \
  --native-355 /data/pcbs-oom/blockmanager-native-spike/root-final-overlay \
  --recovery-355 /data/pcbs-oom/blockmanager-codec-spike/root-recovery-final355 \
  --output /data/pcbs-oom/blockmanager-codec-spike/rechecked-results.json
~~~

These are small correctness/route proofs, not a performance or heap-pressure
benchmark. They do not establish a production API, RAPIDS payload accounting,
all storage-level/native combinations, cancellation/corruption safety or a
large PCBS end-to-end integration. Primary and recovery runs are separate:
the primary result's remaining_unverified list is scoped to that run.
