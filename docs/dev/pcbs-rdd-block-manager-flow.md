# Existing Spark RDD cache and BlockManager flow

Status: source investigation, READY FOR REVIEW. No implementation or extension design.
This account describes upstream Spark 3.5.5, not a new PCBS ownership protocol.

Spark gives the persisted RDD an ID on the driver, carries that ID in task
serialization, and keys each cached partition by `RDDBlockId(rddId, partitionIndex)`.
An executor reports where it stored that block. Whole-RDD removal sends the RDD ID
to **every registered BlockManager**, and each scans its own keys for that ID.
The driver does not need a per-PCBS-batch directory to perform that removal.

## Identity and lazy materialization

An RDD constructor assigns `id = sc.newRddId()`, unique within its SparkContext.
`persist` registers the RDD with SparkContext/ContextCleaner and sets its storage
level; it does not compute partitions or register their locations.
For SQL caching, PCBS returns an RDD, then `CachedRDDBuilder.buildBuffers` wraps it
with a statistics `mapPartitionsInternal` and persists **that wrapper**. Its ID is
the cache-block owner, not the ID of the serializer-output RDD.
Sources: [RDD.scala](https://github.com/apache/spark/blob/v3.5.5/core/src/main/scala/org/apache/spark/rdd/RDD.scala#L146),
[InMemoryRelation.scala](https://github.com/apache/spark/blob/v3.5.5/sql/core/src/main/scala/org/apache/spark/sql/execution/columnar/InMemoryRelation.scala#L265).

DAGScheduler serializes the stage RDD and function/shuffle dependency into a
broadcast task binary. ResultTask/ShuffleMapTask deserialize that RDD lineage;
the existing `id` field is retained, rather than allocating a new identity for
each task. TaskSetManager separately serializes the Task envelope and selected Partition; the executor restores that envelope before reading the broadcast RDD graph. Narrow parent iteration reaches the persisted wrapper; its
`RDD.iterator → getOrCompute` constructs `RDDBlockId(id, partition.index)`.
The task-attempt ID is passed separately and is not part of the cache key.
Sources: [DAGScheduler.scala](https://github.com/apache/spark/blob/v3.5.5/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala#L1569),
[ResultTask.scala](https://github.com/apache/spark/blob/v3.5.5/core/src/main/scala/org/apache/spark/scheduler/ResultTask.scala#L77),
[ShuffleMapTask.scala](https://github.com/apache/spark/blob/v3.5.5/core/src/main/scala/org/apache/spark/scheduler/ShuffleMapTask.scala#L82),
[RDD.scala](https://github.com/apache/spark/blob/v3.5.5/core/src/main/scala/org/apache/spark/rdd/RDD.scala#L326).

## Registration, local storage and reporting

Each executor has a BlockManager with a storage RPC endpoint. Initialization
registers its `BlockManagerId(executorId, host, port)`, local directories, memory
capacities and endpoint reference with the driver's BlockManagerMasterEndpoint.
The BlockManager ID's host/port locate data transfer; the separately registered RPC endpoint receives storage commands. Registration uses synchronous `askSync`; it does not require any RDD blocks.
Sources: [BlockManager.scala](https://github.com/apache/spark/blob/v3.5.5/core/src/main/scala/org/apache/spark/storage/BlockManager.scala#L504),
[BlockManagerMaster.scala](https://github.com/apache/spark/blob/v3.5.5/core/src/main/scala/org/apache/spark/storage/BlockManagerMaster.scala#L72).

On a cache miss, BlockManager creates local block metadata and acquires its write
lock before consuming the partition iterator. Successful storage can be in memory
or disk, depending on the storage level and available memory. All PCBS records
in that partition belong to the **one partition block**; there is no PCBS record
index in `RDDBlockId`. Failed storage need not create a reported cache copy.
After a successful put, `reportBlockStatus` sends `UpdateBlockInfo` with the
block key, BlockManager ID, actual storage level and memory/disk sizes.
`UpdateBlockInfo.writeExternal` writes the block's name (for example `rdd_42_7`) with `writeUTF`; the driver reconstructs it with `BlockId(in.readUTF())`. No RDD-ID negotiation occurs. Ordinary reporting uses synchronous `askSync`; listener event delivery is a separate asynchronous step. The driver does not broadcast its complete location map to all executors; callers query locations for needed blocks.
Sources: [BlockManager.scala](https://github.com/apache/spark/blob/v3.5.5/core/src/main/scala/org/apache/spark/storage/BlockManager.scala#L1511),
[reportBlockStatus](https://github.com/apache/spark/blob/v3.5.5/core/src/main/scala/org/apache/spark/storage/BlockManager.scala#L844),
[master update](https://github.com/apache/spark/blob/v3.5.5/core/src/main/scala/org/apache/spark/storage/BlockManagerMaster.scala#L99),
[wire format](https://github.com/apache/spark/blob/v3.5.5/core/src/main/scala/org/apache/spark/storage/BlockManagerMessages.scala#L78).

| State | Key → value | Example |
|---|---|---|
| Driver logical persistence | RDD ID → registered RDD | `42 → persisted wrapper` |
| Executor local metadata | BlockId → BlockInfo/locks | `rdd_42_7 → level, size, readers/writer` |
| Executor stores | BlockId → memory entry or disk file | `rdd_42_7 → partition's PCBS records` |
| Driver executor directory | executor ID → BlockManagerId | `3 → BM3`, `9 → BM9` |
| Driver manager directory | BlockManagerId → status map + RPC endpoint | `BM3 → {rdd_42_7 → status}, endpoint3` |
| Driver location directory | BlockId → set of BlockManagerIds | `rdd_42_7 → {BM3, BM9}` |

The last example assumes both executors have a copy; default replication need
not produce two copies. These maps describe physical copies, separately from
logical persistence. Memory eviction can leave a disk copy under the same key.
Local/remote reads reuse that key; if no usable copy exists, Spark can recompute
the partition. Sources: [master maps and updates](https://github.com/apache/spark/blob/v3.5.5/core/src/main/scala/org/apache/spark/storage/BlockManagerMasterEndpoint.scala#L80),
[block key](https://github.com/apache/spark/blob/v3.5.5/core/src/main/scala/org/apache/spark/storage/BlockId.scala#L53),
[local metadata](https://github.com/apache/spark/blob/v3.5.5/core/src/main/scala/org/apache/spark/storage/BlockInfoManager.scala#L133).

## Whole-RDD removal and acknowledgments

Explicit unpersist and ContextCleaner RDD cleanup reach
`SparkContext.unpersistRDD(42, blocking)`. It requests `RemoveRdd(42)`, removes
logical persistence registration, then posts `SparkListenerUnpersistRDD(42)`.
The master first removes matching location/status metadata, then asks **all
registered storage endpoints** to remove RDD42, including managers absent from
the location set for partition7. Each endpoint runs `BlockManager.removeRdd`
asynchronously: scan local `blockInfoManager.entries`, select matching
`RDDBlockId.rddId`, and remove each from memory, disk and local metadata under
write locking. Its Int completion reply is sent after that local operation returns. Disk removal derives filenames from block keys without deserializing any PCBS record. Managed disk-only blocks remain in local metadata after memory eviction; this scan does not enumerate orphan files absent from that metadata. Per-block removals use `tellMaster=false`; the master already removed metadata.
Sources: [SparkContext.scala](https://github.com/apache/spark/blob/v3.5.5/core/src/main/scala/org/apache/spark/SparkContext.scala#L2069),
[master removal](https://github.com/apache/spark/blob/v3.5.5/core/src/main/scala/org/apache/spark/storage/BlockManagerMasterEndpoint.scala#L335),
[storage endpoint](https://github.com/apache/spark/blob/v3.5.5/core/src/main/scala/org/apache/spark/storage/BlockManagerStorageEndpoint.scala#L51),
[local removal](https://github.com/apache/spark/blob/v3.5.5/core/src/main/scala/org/apache/spark/storage/BlockManager.scala#L1989).

```mermaid
sequenceDiagram
    participant D as Driver RDD / SparkContext
    participant M as Driver BlockManagerMaster
    participant E as Executor 3 BlockManager
    participant N as Executor 9 BlockManager
    E->>M: RegisterBlockManager(BM3, endpoint3)
    M-->>E: registration reply
    D->>E: task binary + partition (lineage retains RDD42)
    E->>E: getOrCompute → rdd_42_7; store partition
    E->>M: UpdateBlockInfo(BM3, rdd_42_7, tier, sizes)
    M->>M: update BM3 status and blockLocations
    M-->>E: Boolean reply
    Note over E,N: If executor 9 also stores a copy, it reports the same block key
    D->>M: RemoveRdd(42)
    M->>M: remove RDD42 directory metadata
    par all registered managers
        M->>E: RemoveRdd(42)
        E->>E: scan keys; delete matching memory/disk blocks
        E-->>M: completion reply
    and
        M->>N: RemoveRdd(42)
        N->>N: scan keys; delete matches (possibly none)
        N-->>M: completion reply
    end
    Note over D,M: blocking=true waits for removal future; false does not
```

`BlockManagerMaster.removeRdd` synchronously obtains the aggregate future;
`blocking=true` additionally waits for it. With `false`, local deletion may
continue after the unpersist listener event is posted. Endpoint exceptions
send RPC failures. The master recovers IOExceptions as zero; timeouts recover as zero if the executor is lost or liveness lookup fails, but propagate if it is confirmed alive. Other failures can fail the aggregate future. A failed disk-file deletion can log a warning without failing the reply, and completed removal does not prevent later concurrent task writes.
Relevant external-shuffle-service RDD disk copies have an additional deletion
path when that feature is enabled.
Sources: [waiting](https://github.com/apache/spark/blob/v3.5.5/core/src/main/scala/org/apache/spark/storage/BlockManagerMaster.scala#L189),
[failure handling](https://github.com/apache/spark/blob/v3.5.5/core/src/main/scala/org/apache/spark/storage/BlockManagerMasterEndpoint.scala#L304).

## Re-registration, executor loss and verification boundary

An unknown manager's status update normally returns false. The executor schedules
re-registration and reports all locally tracked blocks again; the master
rejects re-registration of an executor the scheduler considers lost. Heartbeats can also trigger re-registration; neither heartbeats nor initial registration carry a full block inventory. Removal of
an executor drops its manager/status entries and its locations from each block;
remaining copies can survive, with proactive replication when enabled.
These are storage-directory updates, not whole-RDD unpersist.
Sources: [re-registration](https://github.com/apache/spark/blob/v3.5.5/core/src/main/scala/org/apache/spark/storage/BlockManager.scala#L618),
[registration validation](https://github.com/apache/spark/blob/v3.5.5/core/src/main/scala/org/apache/spark/storage/BlockManagerMasterEndpoint.scala#L666),
[executor removal](https://github.com/apache/spark/blob/v3.5.5/core/src/main/scala/org/apache/spark/storage/BlockManagerMasterEndpoint.scala#L488).

Source evidence is the local clean tracked checkout at upstream tag `v3.5.5`.
This is RELEASED Spark behavior; existing PCBS representation and persistence
contracts are unchanged. Names beginning `private[spark]`/`private[storage]`
remain internal APIs. No runtime RPC trace, failure injection, concurrent
unpersist test or customer-build verification was performed for this account.
All filesystem/source operations used MCP docker_exec. No new hooks,
transport, polling, cleanup protocol or production policy is proposed here.
