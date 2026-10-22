# RDD cache lifetime and PCBS payload ownership

Status: READY FOR REVIEW. Source investigation only; no implementation or production policy selected.

The useful lifetime signal is **SparkListenerUnpersistRDD(rddId)** on the driver.
It can retire a payload store owned by the entire cached RDD. Payloads can stay
in our off-heap/disk store when Spark evicts its in-memory reference objects.
Per-object close and memory-eviction events are not the end of this ownership.

## Whole-RDD removal

Both paths converge on SparkContext.unpersistRDD:

- Explicit RDD.unpersist: core/.../rdd/RDD.scala:211.
- SQL uncache/clear/recache: CachedRDDBuilder.clearCache calls
  _cachedColumnBuffers.unpersist (InMemoryRelation.scala:231).
- Driver GC: ContextCleaner.doCleanupRDD calls sc.unpersistRDD
  (ContextCleaner.scala:222). Its CleanerListener is GC-only; the Spark listener
  event below covers explicit and GC cleanup.

SparkContext.scala:2069–2072 performs, in order:

1. blockManager.master.removeRdd(rddId, blocking).
2. Remove the RDD from persistentRdds.
3. Post SparkListenerUnpersistRDD(rddId).

BlockManagerMaster.removeRdd:189 requests RemoveRdd from the driver master.
The master endpoint:335 removes directory metadata and sends RemoveRdd to
registered executor storage endpoints; it also handles relevant disk copies
served by an external shuffle service. Executor storage endpoint:51 calls
BlockManager.removeRdd:1989, which removes the matching partition blocks from
both MemoryStore and DiskStore. With blocking=false, requests can still be
running when the listener event is posted. With blocking=true, the master
waits for the removal futures before returning; listener dispatch remains
asynchronous.

Memory eviction follows BlockManager.dropFromMemory:1938 instead. For
MEMORY_AND_DISK, it writes a disk copy first if needed, then removes the memory
entry. **It does not post SparkListenerUnpersistRDD.** This is the distinction
needed for a store whose data survives reference-object eviction.

## Tapping the signal

A driver plugin receives SparkContext in DriverPlugin.init. It can call
sc.addSparkListener (SparkContext.scala:1824) and override onUnpersistRDD.
This uses an existing DeveloperApi; a Spark fork is not required merely to
observe whole-RDD retirement.

A possible ownership model, not an implemented design:

- Register a plugin payload namespace against the actual persisted cache RDD.
- Keep cache-owned payloads spillable across Spark memory eviction and later
  disk/reference reloads. Keep metadata such as row count/statistics with each
  reference, preserving the ability to select individual batches.
- On RDD retirement, release that namespace's cache ownership across executors.
- Separately protect active readers, writes and transfers. Retiring cache
  ownership must not invalidate already-started operations.

The event is useful, but does not itself provide a reliable distributed
ownership protocol. Specific remaining work:

- Public listeners use an asynchronous shared queue. AsyncEventQueue.scala:152
  drops events if its queue is full. Keep listener work short and reconcile
  ownership with the driver's persistence registration; do not rely on delivery
  of a single notification to prevent leaks.
- getPersistentRDDs (SparkContext.scala:2018) reports logical persistence even
  when no partition is currently stored. It can inform reconciliation, but
  snapshots, registration and retirement need ordering/generation handling.
- Same RDD IDs can be unpersisted and persisted again. The event contains only
  rddId. SQL CachedRDDBuilder normally creates a new cached wrapper after
  clearing; generic RDD reuse still needs a persistence generation.
- Retired generations need tombstones for late registrations from running work.
  Such work may still need task-owned payloads even when cache ownership ends.
- In-flight reads/transfers need coordinated acquisition and release, including
  the interval after reference bytes are fetched but before payload lookup.
  Simply deleting all payloads on listener delivery is not established safe.
- Keep IDs/weak references in driver bookkeeping. Strongly retaining the RDD
  can prevent the GC cleanup being observed.
- PluginContext.send/ask provides executor-to-driver messaging. Executors can
  poll for retirement state through this API. Driver push needs another
  transport/internal RPC integration. Delivery, acknowledgments, shutdown and
  lost executors remain part of the design.

These are proposed requirements inferred from source paths, not claims that a
specific lease/reconciliation implementation has already been validated.

## Identifying the owning RDD

PCBS's conversion output is not the RDD Spark ultimately persists.
CachedRDDBuilder.buildBuffers (InMemoryRelation.scala:271–296) adds a statistics
mapPartitionsInternal wrapper and persists that wrapper. Its ID is the ID in
SparkListenerUnpersistRDD. Registering only the serializer-output RDD ID is wrong.

The driver GPU scan already obtains that actual RDD through
relation.cacheBuilder.cachedColumnBuffers (GpuInMemoryTableScanExec.scala:151).
CPU scan paths can add filter/metrics RDDs before calling serializer conversion
methods (InMemoryTableScanExec.scala:97–135,175–184). AQE can execute baseCacheRDD
directly (:197). A registration design must cover population as well as GPU,
CPU, count-only and AQE paths; registering only during GPU decoding is not yet
shown sufficient. Driver RDD lineage/persistence discovery is another possible
source of the mapping, but its race-free use remains unverified.

## What Spark reports back

| Signal/API | Information | Meaning for plugin ownership |
|---|---|---|
| SparkListenerUnpersistRDD | Entire RDD ID retired from persistence | Candidate trigger to retire a payload namespace |
| getPersistentRDDs | Registered persistent RDDs, including unmaterialized ones | Reconciliation input; not physical residence |
| getRDDStorageInfo | Cached partition count and memory/disk totals | Observability, not proof that the logical cache is dead |
| SparkListenerBlockUpdated | Reported partition-block location/tier/size changes | Useful for observability; memory eviction is not RDD retirement |
| BlockManagerMaster.getLocations | Executors/services holding a partition block | Internal directory; does not describe individual PCBS payloads |

Status updates are asynchronous and can be stale. RDD-wide removal uses
tellMaster=false on each executor block removal, so there need not be a
separate BlockUpdated deletion event for every removed block. If Spark stores
small references, its byte metrics will describe those references rather than
all externally owned payload bytes.

## Cross-executor behavior

RDD.getOrCompute and BlockManager.get (:1275) try local then remote data.
Locations are scheduling preferences, not executor or node affinity.

A remote read fetches the serialized Spark block, even if the source had
deserialized objects. BlockManager.doGetLocalBytes (:1019) serializes memory
objects or reads an existing disk copy; getRemoteValues (:1060) deserializes
the result on the requester. Replication and decommission migration also
transfer serialized block bytes. Same-host file access does not make a
process-local handle valid in another executor.

Consequently a reference-only design needs an executor-independent resolution
and transfer mechanism, or must carry self-contained payload bytes on the
disk/network path. Which to choose is an architectural decision still open.
A destination copy must not depend on an original executor that may disappear.
Spark can recompute a missing partition from lineage when no cache copy
survives; a hidden payload disappearing behind an otherwise present reference
does not automatically produce that ordinary cache miss.

Whole-RDD ownership is compatible with keeping data after memory eviction.
It does not by itself solve remote payload lookup, transfer ownership or
recovery. These are the next issues to specify once the RDD association is
established.

## Verification and compatibility

Lead independently read the decisive SparkContext, ContextCleaner, RDD,
CachedRDDBuilder, master/executor removal, eviction, local/remote read,
listener-queue and plugin API code. Independent workers traced cleanup and
cross-executor paths and reviewed this account. Source evidence is the clean
upstream v3.5.5 checkout under /home/roberte/src/spark_3.5.5. The corresponding
v3.5.3 cleanup/eviction code has the same relevant lifetime behavior; removal
status reporting differs. No claim is made for uninspected versions.

Current Spark persistence behavior and PCBS CPU/GPU/schema/filter/storage
contracts are treated as released. This work changes none of them. A future
implementation must preserve remote reads, disk reload, fallback and recovery.
Proposed namespace/lease/registration mechanisms are unimplemented and have no
compatibility scaffolding. No runtime proof-of-concept or new benchmark was
run for this source investigation. All operations used MCP docker_exec.
