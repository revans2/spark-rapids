# Draft upstream design: pluggable native/external cache storage

Status: READY FOR REVIEW — research proposal, not an Apache Spark API, approved SPIP, community agreement or implementation. Prepared 2026-10-22 using MCP docker_exec only. No code changes, builds, jobs, commits or external messages. This is the user's separately authorized long-term upstream track (estimated ~5 year rollout), NOT a prerequisite presented as available for the short-term stock-Spark solution.

Companion evidence: [executable token-cache spike](pcbs-token-cache-spike.md). Short-term implementation continues to target unmodified Spark.

## Problem and minimum scope

CachedBatchSerializer customizes SQL cache encoding/decoding, but does not define native-storage lifetime, atomic cache visibility, scheduler cache locations, transfer ownership or a pre-output cache-miss contract. An encoded record can be GiB-sized; putting its payload in a Java byte[] makes cache load/unroll/deserialization heap pressure proportional to concurrent records. Existing native engines need storage that owns off-heap buffers/files while Spark remains responsible for persistence semantics, lineage and scheduling.

Propose a cache-storage SPI INSIDE Spark's existing persistence path, rather than replacing BlockManager or inventing manually maintained scheduler tokens. Spark owns actual RDD registration/generation, partition-cache commit and retirement, location metadata and miss-to-parent-compute control. A provider owns the physical bytes/handles and bounded storage/read/transfer resources. Keep data format separate: existing CachedBatchSerializer remains supported and its schema/row/size/pruning contracts remain intact; SQL-native batch adapters can expose payload views without Java serialization of bulk buffers.

Minimum useful first version: partition-granular RDD cache backend, with SQL integration via an optional opaque-record codec; host-native/file implementation and default existing BlockManager adapter; explicit lifetime/miss/commit/retire/status/transfer operations. No CUDA, RAPIDS, UCX, Arrow layout, external durable service, whole-BlockManager replacement or new shuffle implementation is required. Device-native tiers and external persistent services can implement the same capabilities later.

## Existing mechanisms and limits

Audited upstream3.5.5 sources:
- RDD.iterator327–334 chooses getOrCompute from its real internal storageLevel; getOrCompute374–384 invokes BlockManager.getOrElseUpdateRDDBlock with the parent-compute callback. The proposed SPI belongs at this existing decision, not a custom-RDD override/private-field trick.
- DAGScheduler.getCacheLocs426–444 uses real persistence plus BlockManagerMaster locations. getMissingParentStages748–787 prunes ancestor shuffle stages for fully cached RDDs. Locations are correctness-relevant, not merely a locality UI.
- SparkEnv365 constructs BlockManager directly. ExecutorPlugin has init/shutdown/task callbacks; task callback exceptions are suppressed (ExecutorPlugin.java:68–70). PluginContext send/ask are serializable control RPCs to the driver, not a storage SPI or lease/miss barrier.
- CachedBatchSerializer.scala:35–78 is a DeveloperApi for SQL format conversion. It receives StorageLevel but does not own persistence registration/removal or BlockManager transactions.
- ManagedBuffer.java:29–78 offers native/file byte views, retain/release and Netty conversion. Its nioByteBuffer method itself warns allocation may be expensive. It does not supply RDD ownership, atomic publication or recovery. BlockData in BlockManager.scala77–99 is private[spark].
- ShuffleDataIO.java:23–55 and driver/executor components provide a useful split-lifecycle pattern, but are @Private and explicitly shuffle-specific. They are precedent, not a cache API to reuse unchanged.
- BlockManager1019–1065 serializes memory objects or serves disk/network bytes. SerializerManager188–199 can collect the full serialized partition into4MiB HEAP chunks. A wrapper namedManagedBuffer does not guarantee bounded native transfers.
- Decommissioner356–366 removes the source after replication reports success. A proposed provider must make that success refer to a usable independently owned payload, not a copied ID.

Pinned current-source check: master67b62cb4081eda45933e76797a7730596dc50369 still routes RDD434 through BlockManager; DAGScheduler640–657 uses persistence+BM locations and1126onward consults cache locations before visiting dependencies. This bounded audit does NOT prove no related API/proposal exists elsewhere.

Version agent fetched official pinned4.0.1/4.0.4/4.1.3/4.2.0/master source under /data/pcbs-oom/token-cache-version-audit/sources and recorded commit/URL/hash in source-download-manifest.json. CurrentmasterSQL cache wrapper uses mapPartitionsWithIndexInternal and fully-consumed/successful-partition statistics;4.2.0 already has partition-keyed statistics. Therefore do not make the old3.5 mapPartitionsInternal/global-accumulator implementation part of the new SPI. Preserve each version's existing materialization/statistics semantics through the core hook.

## Authority boundary

| Concern | Spark authority | Provider authority |
|---|---|---|
| Identity/lifecycle | App attempt, realRDD ID, persist incarnation, partition and task-attempt identity; retired epochs | Local physical version/file/buffer identity under those keys |
| Visibility | Accept committed partition version and usable locations; fence retirement | Prepare complete physical partition and attest capability/placement |
| Lookup/recovery | Select local/remote locations; interpret pre-output Miss; call parent-compute/checkpoint callback; schedule missing ancestors when necessary | Atomically acquire a readable version/lease or return structured Miss/Retryable/Fatal |
| Lifetime | Close read/write/transfer operations on completion/cancel/failure; invoke backend retirement directly from unpersist/cleaner | Reference-count resources and reclaim when cache ownership and operation leases are gone |
| Scheduling | Canonical cache locations, preferred locations, dependency pruning and cache-status invalidation | Timely versioned availability/tier updates, never locations for incomplete/unresolvable bytes |
| Accounting | Distinguish JVM/native/disk/transfer metrics; enforce or delegate declared budgets visibly | Reserve before allocation, report exact residency and release reservations once |
| SQL values | CachedBatch rows, encoded logical bytes, schema/selection/pruning and current materialization stats | Opaque physical data and optional codec-specific layout; no redefinition of SQL size |

BlockManager/Master remain the default registry/control/transfer machinery. A provider adapter participates in canonical publication/removal so scheduler/control code does not need a parallel descriptor map. No manually fabricated RDDBlockIds/heartbeat tokens are part of the proposed public contract.

## Proposed API sketch (illustrative only)

Names/types below are invented for discussion, NOT interfaces Spark currently exposes. Start as an explicitly evolving SPI; exact Java/Scala surface and naming require upstream review.

```scala
// Spark creates these from normal RDD.persist/unpersist, never backend inference.
case class CacheId(appAttempt: String, rddId: Int, epoch: Long)
case class PartitionKey(cache: CacheId, partition: Int)
case class AttemptId(executorEpoch: String, taskAttempt: Long)
case class PartitionVersion(key: PartitionKey, opaqueVersion: String)

// Separates format from storage. Default adapter uses existing serializer rules;
// optional native SQL codec avoids serializing bulk payloads through Java/Kryo.
trait CacheRecordCodec[T] {
  def formatId: FormatVersion
  def encode(value: T, out: RecordSink): Unit
  def decode(record: LeasedRecord): T
}
// RecordSink accepts bounded byte-stream/channels or adopted immutable buffer leases.
// Neither API requires an entire payload to become one Java array/ByteBuffer.

trait CacheStorageProvider {
  def capabilities: CacheCapabilities
  def driver(ctx: CacheDriverContext): CacheDriverStorage
  def executor(ctx: CacheExecutorContext): CacheExecutorStorage
}
trait CacheExecutorStorage {
  def beginPut(key: PartitionKey, attempt: AttemptId,
               policy: CachePolicy, codec: FormatVersion): PartitionWriter
  def acquire(version: PartitionVersion, read: ReadContext): AcquireResult
  def openTransfer(version: PartitionVersion, range: ByteRange,
                   read: ReadContext): TransferLease
  def retire(cache: CacheId, fence: RetirementFence): RetirementAck
  def close(): Unit
}
trait PartitionWriter extends AutoCloseable {
  def append(record: EncodedRecord): Unit
  def prepare(): PreparedPartition // full manifest, checksums/lengths, physical placement
  def commit(grant: CommitGrant): CommittedPartition // physical commit only; NOT visible
  def abort(): Unit               // idempotent; close aborts unless commit completed
}
sealed trait AcquireResult
case class Hit(partition: PartitionReadLease) extends AcquireResult
case class Miss(version: PartitionVersion, reason: MissingReason) extends AcquireResult
case class Retryable(reason: ResourceOrTransportFailure) extends AcquireResult
case class Fatal(reason: IntegrityOrContractFailure) extends AcquireResult

trait PartitionReadLease extends AutoCloseable {
  def version: PartitionVersion
  def records: CloseableIterator[LeasedRecord] // no mixed-version suffix replacement
}
trait CacheDriverStorage {
  def register(cache: CacheId, policy: CachePolicy, codec: FormatVersion): Registration
  def prepareCommit(prepared: PreparedPartition): CommitGrant // reserves; NO visibility
  def publishCommitted(data: CommittedPartition, grant: CommitGrant): PublishResult
  def cancelGrant(grant: CommitGrant): Unit
  def loseLocation(version: PartitionVersion, executorEpoch: String): Unit
  def beginRetire(cache: CacheId): RetirementFence
  def close(): Unit
}
```

Core may implement the driver methods itself with backend durability callbacks, rather than let plugins control master metadata. That is the recommended minimal split: ONE authoritative Spark cache directory and generation service, not arbitrary plugins rewriting scheduler state. Publication is a versioned visibility transaction, not a best-effort asynchronous listener. Required ordering: provider.prepare -> core.prepareCommit (grant, NO location visibility) -> provider.commit (physical ownership/placement complete) -> core.publishCommitted (revalidate active epoch/grant/version, then make locations visible). Only final accepted publication is a cache hit. A physical commit racing cancellation/retirement cannot revive an epoch; core rejects its final publication and the provider reclaims that unpublished version. Failure between physical commit and final publication leaves a bounded orphan tied to its grant, reconciled/canceled through core authority; it is never advertised optimistically. Lost acknowledgments use idempotent publication/status checks, not unconditional deletion of a possibly accepted commit. Backend methods return typed outcomes; provider callback exceptions propagate through this storage path, unlike advisory ExecutorPlugin callbacks.

The API need not publicly expose format internals for arbitrary RDD[T] in its first patch. A default codec can wrap the existing serializer; native SQL integration can supply an optional adapter behind CachedBatchSerializer. Do not require all format plugins to reimplement storage. Likewise a generic store must not infer schema, row counts or a GPU from opaque values. Codec publication/negotiation must be explicit before a cache is used.

## Core lookup/miss/compute flow

```scala
// Illustrative core control result, NOT today's RDD.iterator return type.
def getOrCompute[T](key, context, computeParent: () => Iterator[T]): CacheReadDecision[T] = {
  lookupCommittedVersion(key) match {
    case Some(version) =>
      acquireLocalOrRemoteCompletePartition(version, context) match {
        case Hit(readLease) =>
          Ready(taskOwnedIterator(readLease, decodeAllRecords(readLease)))
        case Miss(_, _) =>
          invalidateVersionLocation(version)  // only the observed version/location
          recomputeWhenDependenciesReady(key, context, computeParent)
        case Retryable(error) => boundedRetryOrTaskFailure(error)
        case Fatal(error) => failTask(error)
      }
    case None => recomputeWhenDependenciesReady(key, context, computeParent)
  }
}
def recomputeWhenDependenciesReady[T](key, context, computeParent): CacheReadDecision[T] = {
  core.checkParentStageReadiness(key) match {
    case ParentsReady =>
      Ready(computeAndOptionallyCache(key, context, computeParent))
    case MissingParents(stages) =>
      Deferred(core.requestParentStagesAndResubmit(key, context, stages))
  }
}
// Spark's core task adapter handles Deferred before output: release this task's
// leases/reservations and executor slot, schedule missing ancestors, then
// resubmit/restart once ready. Never block all executor slots waiting for parents.
```

Miss is legal only BEFORE cached output escapes. An acquired partition lease pins a complete physical version/manifest or a recoverable replica set; small metadata is not considered a cache hit by itself. A provider may reserve files/native ranges without materializing the whole partition. Missing/corrupt records discovered before first output may invalidate that version and recompute.

Storage/transport failure can still occur after first output even after successful acquisition. The API MUST NOT promise failure-free iteration or silently splice a recomputed suffix. After output, release leases and fail/retry the entire task under normal Spark task-attempt semantics. If a format requires all records verified before output, it can preflight/validate with bounded IO, but that is a performance option rather than a universal eager-memory requirement.

computeParent is Spark's existing computeOrReadCheckpoint callback for the actual persisted RDD (including SQL's original stats wrapper), not a user-supplied replacement SQL plan. A failed or declined cache put can return the computed iterator without advertising a cached partition. Partial consumption/cancellation aborts incomplete cache publication. Returned computed values retain the existing producer/consumer ownership contract.

Scheduler integration is mandatory: publication makes canonical locations visible only after commit; loss/retirement withdraws them and invalidates cached scheduler location snapshots. If a cache miss is discovered after ancestor stages were pruned, Spark must check/restore dependency readiness before evaluating computeParent. A native-cache miss is not a fabricated shuffleFetchFailed. The proposed Deferred control must be handled by a core scheduler defer/resubmit path, with task-owned resources released and executor slots available for ancestor tasks; do not call computeParent synchronously when its shuffle parents were pruned and remain unavailable. Exact task-protocol integration is an open upstream API question, not functionality supplied by the backend. Existing genuine shuffle recovery still applies when a real dependency fails after readiness was checked. Test real-shuffle ancestors whose map outputs are gone, not only narrow/localRDD parents.

## Atomic partition publication and attempts

An append-only private attempt remains invisible until the complete partition manifest is prepared and the chosen availability guarantee is satisfied. Driver/core prepareCommit reserves a candidate persist epoch and partition/version but advertises NO location. Provider.commit finishes the required local/replica placement; core.publishCommitted then validates that the grant/epoch/version are still current before atomically registering locations. A canceled grant or retired epoch rejects late physical completion; unpublished committed bytes are reclaimed through acknowledged abort/reconciliation. Physical commit and location visibility are separate states, and only the latter grants cache ownership. Failure/cancel before physical commit aborts private native/files; a physical commit awaiting publication remains provisional and grant-owned. Failure after accepted publishCommitted preserves cache ownership independently of writer task lifetime. Provisional-commit cleanup must first reconcile publication status so an acknowledgment loss cannot delete an accepted cache version.

Do not turn this into a new globally transactional RDD snapshot. Spark can materialize/recompute partitions at different times; nondeterministic parents retain current semantics. The promise is complete version per partition read, not a new cross-partition snapshot guarantee. Duplicate/speculative attempts have distinct immutable versions. A core commit policy chooses/recognizes an authoritative version and reclaims losing attempts safely; concurrent computations may still return their own computed task results as existing Spark permits. A delayed losing attempt cannot resurrect a retired epoch or replace a later accepted version accidentally.

While one cache stays persisted, repeated eviction/recompute must not retain every successful old version. Retire superseded physical cache ownership while old read/transfer leases continue; bound orphan attempts/version tombstones and directory traffic. Reader/transport leases are explicit refcounts/operations, not TTL proof that a paused reader has died.

## Unpersist, cleaner and shutdown

RDD.persist directly registers a new incarnation; RDD.unpersist directly fences it and invokes storage retirement BEFORE reporting removal complete. ContextCleaner uses the same core path. No asynchronous listener is the authority; listeners remain observations/diagnostics. A provider cannot keep a strong RDD reference that prevents cleaner initiation.

Retirement fence forbids new cache hits/publications for that epoch and withdraws scheduler locations. Existing acquired read/transfer operations may drain; physical bytes are reclaimed only after both cache ownership and operation leases release. blocking=false starts this operation and returns according to normal asynchronous semantics. For blocking=true, define/test exactly which completion is awaited: recommended contract is confirmed logical removal and backend cache-owner release on all live holders, plus explicit reporting of outstanding operation leases; if Spark promises full physical reclamation, it must additionally await their drain. This choice requires community agreement and parity analysis against existing block read-lock behavior; do not hide it in plugin callbacks.

Cancellation closes task-owned readers/writers/transfers. Executor death fences its incarnation, withdraws its locations, and triggers replica/recompute policy; a paused live executor is not declared dead solely to free buffers. Driver/application shutdown invokes provider cleanup with bounded reporting; truly external durable service needs its own ownership/reconciliation protocol and is not mandatory for v1. Native refcount release is deterministic on normal paths; Cleaner is a safety net, not the sole cache-lifetime contract.

## Persistence-level semantics

Provider selection must NOT silently reinterpret StorageLevel. It must either implement the requested semantics, use the existing built-in backend, or reject unsupported opt-in use before population with a clear error. Never silently downgrade replication or turn a storage miss into data loss.

- MEMORY_ONLY/deserialized: committed cache data lives in allowed memory tiers. On eviction it disappears as a cache copy and can recompute; it cannot silently become a disk-backed cache. Whether a new native-memory tier qualifies for the exact existing flag needs explicit API/policy review.
- MEMORY_ONLY_SER: same placement/no-disk semantics, self-contained chosen serialized representation; not permission to retain an unbounded native side store behind tiny serialized IDs.
- MEMORY_AND_DISK(+SER): payload ownership follows actual memory/disk placement. Native host->disk movement retains the same complete logical partition version and location; update tier/size, not invalidation.
- DISK_ONLY: committed payload resides on disk; allow bounded read/write staging, not a permanent native-memory cache disguised by small disk metadata.
- OFF_HEAP: respect configured Spark off-heap storage accounting and implementation capability. External/device memory is not automatically equivalent to that existing memory pool.
- ReplicationN: accepted replica locations must hold independently usable complete versions. Copying a descriptor is insufficient. Preserve existing best-effort replication/failure semantics; do not invent a strictN-durable-ack guarantee without an approved API change.
- Decommission: migrate actual payloads via standard control, verify destination commit/lease ownership, then remove source. A failed migration remains failed according to Spark's existing policy; provider cannot report success for a pointer to the source.

Memory eviction, partition replacement, whole RDD retirement and task/read closure are distinct operations. Respect them instead of using one callback for all.

## Memory, IO and diagnostics

Existing JVM heap/off-heap bookkeeping remains unchanged by default. Add explicit physical residency reporting: JVM bytes, native host bytes, optional device bytes, disk bytes, serialized transfer bytes and staging bytes. SQL logical encoded bytes/rows remain separate; never inflate metadata heap estimates to account for nonheap memory.

At minimum a provider registers a finite enforced budget or a declared externally enforced resource pool; it reserves credit before adopting/allocating bytes, spills/evicts under requested policy and releases credit exactly once. Expose reservations for cache retention AND active restores/transfers; a retained-cache cap alone does not limit concurrent full-record restores. Quota refusal is typed admission failure/uncached compute or a retryable resource error, not silent unlimited allocation. A shared Spark reservation broker for native pools is a concrete design option; first SPI can delegate enforcement to an audited provider pool with separately visible counters. Do not mandate CUDA or redesign UnifiedMemoryManager in the first proposal.

Transfers use existing Spark authentication/application isolation, block-master discovery, cancellation, bounded credits/windowed buffers and file/native ManagedBuffer references where feasible. Audit every consumer: nioByteBuffer/fetchBlockSync/dataSerialize can allocate whole buffers/partitions. Offer a bounded channel/window path so generic convenience methods cannot force contiguous heap materialization. Preserve compression/encryption and checksums; ordinary Java/Kryo descriptors are not inherently custom serialization work. The optional self-contained codec is different from a reference-only descriptor.

UI/eventlog reports provider/tier/status, physical and logical bytes, cache hit/miss/recompute reasons, commit/abort/retire counts, in-flight leases, native reservations, transfer/spill bytes and cleanup failures. Scheduler locality can consume location+tier weights later; v1 need not redesign locality costing.

## Defaults and compatibility

No existing application changes behavior when no cache provider is selected. Built-in provider wraps the existing storage path and preserves existing APIs, serializer selection, metrics meanings, persist levels and failure behavior. New native support is opt-in initially; exact selection API/config name is deliberately not invented here. Resolve multiple providers deterministically; fail capability/version mismatch before publication. Optional methods/default adapter support existing CachedBatchSerializer implementations; no mandatory GPU or new external service.

Wire manifests carry codec/provider version and immutable cache/partition versions, with incompatible executors rejected/fallback BEFORE publishing a cache. Cached data is ephemeral; no automatic promise of reading previous application/plugin-binary caches. Rolling/mixed-executor artifacts within one application need an explicit negotiate/reject policy, not a blind pointer decode. No compatibility adapter is required for our unreleased local-map prototype.

## Upstream deliverables and gates

1. Prepare a short problem/SPIP draft, this authority/state diagram and capability matrix; include at least native-host and file-backed generic use cases, released behavior comparison, risks, alternatives and benchmarks. Confirm community scope/ownership before a large implementation.
2. Add core lifecycle + backend hooks with a built-in compatibility adapter and a dummy native/file provider. Unit tests verify atomic prepare/publish/abort, read lease, exact owner incarnation, miss-before-output, failed task cleanup and blocking/nonblocking retirement. No GPU is needed for upstream tests.
3. Integrate canonical BlockManager locations/status, DAGScheduler cache awareness and missing-dependency recovery, transfer/replication/decommission and accounting. Tests require multiple executor JVMs and real shuffles; force source loss, late/duplicate publication, lost replicas and stale scheduler cache snapshots.
4. Add optional SQL/native codec integration without changing existing format plugins; preserve empty/zero-column/nested schemas, selected attributes, partitioning and pre/partial/fully consumed statistics/AQE/CBO behavior. Use current master's partition-keyed statistics contract where applicable, not an old wrapper assumption.
5. Run compatibility/default-provider suites plus bounded-allocation/performance tests. Require exact values across paused-reader/retire/cancel races; no old-prefix/new-suffix partition output, no leaked versions under long-lived evict/recompute, no stale locations after retirement, and no marker-only replicas.
6. Review performance budgets before measurement: small-record regression, large native record heap growth, native and staging budgets, warm/cold disk/network cache, fill/repeatedread/downstream task timing, cleanup latency and scheduler/driver traffic. Retain failing controls; a synthetic local RAM success is not a distributed correctness/performance result.
7. Only after authorized human review: public JIRA/SPIP proposal and dev@ discussion, incorporate community alternatives, then staged upstream PRs. Apache's official SPIP guidance calls for significant cross-cutting changes to be discussed publicly and shepherded; acceptance/votes are future community decisions. This task does not post anything.

Stop/reshape if the minimal backend API cannot uphold persistence levels, canonical scheduler visibility, bounded read/write memory or reliable native ownership. Decide whether narrower SQL-only scope is useful before expanding to all RDD types. Replacing BlockManager remains a separate much-larger alternative, not the recommended minimal extension.

## Transition from the private adapter

Keep native storage, spill, leases and transfer logic behind a backend boundary.
The short-term adapter handles private RDD interception, scheduler tokens and
version-specific Spark access. A future supported adapter supplies the same
backend through the upstream SPI, removing the private override and token
bookkeeping for runtimes that expose it. Preserve existing behavior on older
supported runtimes through their appropriate adapter; do not assume the new API
can be backported or rolled out immediately. The estimated five-year horizon is
a planning assumption, not an Apache release commitment.

Do not freeze the experimental token wire representation as a public contract.
Its implementation is unreleased. Upstream API capability/version negotiation,
default behavior and StorageLevel policy need human and community review before
implementation. No new selection configuration is chosen in this proposal.

## Official references and provenance

- Existing sources: local Apache Spark3.5.5 indexed checkout /home/roberte/src/spark_3.5.5; corresponding public source URLs use https://github.com/apache/spark/blob/v3.5.5/ plus paths above.
- Newer official sources/commits/URLs/hashes: /data/pcbs-oom/token-cache-version-audit/source-download-manifest.json and sources/{4.0.1,4.0.4,4.1.3,4.2.0,master}. Tags are reference snapshots, not a claimed Apache support-policy declaration.
- Official contributor guidance https://spark.apache.org/contributing.html; SPIP process https://spark.apache.org/improvement-proposals.html. Fetched copies are in this owned directory.
- Two bounded GitHubAPI searches saved here: github-cache-storage-plugin-search.json (36matches, first20fetched) and github-native-cache-search.json (233matches, first20fetched). They are NOT an exhaustive JIRA/dev-list/API inventory and do not prove absence of a related proposal.
- Adjacent returned proposals include optional execution memory SPARK-59461 https://github.com/apache/spark/pull/58763 and native data sources SPARK-60101 https://github.com/apache/spark/pull/59424. These are search-result observations, not accepted cache-storage API precedent. Search output alone establishes neither merge state nor endorsement.

## Open questions for upstream discussion

- Generic RDD storage SPI first, or narrower SQL/native-batch storage with a clear later extension?
- Where should provider selection occur without silently changing all existing persists?
- Is native host/device a new explicit tier or allowed implementation of an existing memory level?
- Exact blocking-unpersist completion with active leases, and whether physical reclamation has a separate future/diagnostic.
- Minimum replica commit guarantee matching current best-effort semantics, and who owns retries/decommission replication?
- How much native budget authority belongs to Spark versus provider-enforced pools?
- Should the first API expose a bounded generic transfer contract or normalize/expose selected ManagedBuffer operations?
- Which pre-output cache-miss path restores ancestor stage readiness without manufacturing shuffle failures?
- What currently proposed memory/native-data-source APIs overlap and need joint community design?
