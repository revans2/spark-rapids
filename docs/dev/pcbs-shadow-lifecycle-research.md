# PCBS shadow payload lifecycle research

Status: READY FOR REVIEW. Source research only; architecture not selected and
runtime proofs remain outstanding. A focused independent peer review found no
material source/contract contradiction in the association, ownership and
recovery analysis; it identified the explicit descendant ambiguity recorded
below. The lead independently read the wrapper/persist/discovery/unpersist
sources. This is source-only review, not validation of the proposed protocol.

Short-term candidates must run on **unmodified Spark**. A Spark change is an
explicit **last resort**, with the user's estimated 4–5-year broad rollout
horizon. The authorized disposable local-mode ID-to-SpillableHostBuffer map
is a separate experiment: NODELETES, one fresh JVM, original six roughly
1 GiB batches per task. It does not establish production ownership or recovery.

## Evidence and compatibility boundary

Spark paths below were read in the local upstream 3.5.5 checkout; the earlier
mechanics research verified the corresponding 3.5.3 construction, cache,
unpersist and removal paths. Neither source tree was changed.

The tracker classifies behavior on main as released. PCBS CPU/GPU/schema/
storage contracts and Spark remote reads, disk reloads and recovery must be
preserved. Existing local spill and shuffle components below are already on
main at 3cdf711ded8e8b72d0f3ee92866401df5d9d2a06; git status showed no edits
to those implementation files during this research. This is evidence for
reusing their current local behavior, not evidence that they implement the
new distributed cache contract. The shadow namespace, lookup protocol,
leases and retirement generation are unimplemented and need no compatibility
adapters for a previous shadow implementation.

No GPU jobs, builds, benchmark changes, implementation changes or commits were
performed for this document. All sandbox operations used MCP docker_exec;
code-index preceded discovery. This document complements
[cache ownership](pcbs-rdd-cache-ownership.md),
[BlockManager flow](pcbs-rdd-block-manager-flow.md), and the separate metadata
research. Logical row counts and payload size statistics must be preserved
even when physical Spark records become small.

## Establishing the actual owner before population

The serializer's returned RDD is not the owner Spark unpersists.
CachedRDDBuilder.buildBuffers adds a statistics mapPartitionsInternal wrapper
and calls persist on that wrapper (Spark InMemoryRelation.scala:265–297).
That persisted wrapper's RDD ID must own the shadow namespace.

Registering only in the GPU read operator is insufficient. The CPU row path
adds metrics/filter RDDs before invoking the serializer; the columnar path
also adds filtering. AQE baseCacheRDD deliberately returns the actual cached
RDD without decoder/filter wrappers (InMemoryTableScanExec.scala:95–135,
173–200; QueryStageExec.scala:293). AQE population and direct count of that
RDD can therefore occur before any GPU decode registration. Both PCBS writer
entry points must be covered: convertColumnarBatchToCachedBatch at
ParquetCachedBatchSerializer.scala:247 and convertInternalRowToCachedBatch
at :1331, including CPU fallback and zero-column cases.

A concrete unmodified-Spark association candidate is:

1. Allocate an opaque namespace while constructing either serializer output
   on the driver. Record its producer RDD ID after creating that RDD, without
   permanently retaining the RDD itself.
2. Before a task publishes its first namespace payload, synchronously request
   ownership resolution from the driver's plugin component. Cache a successful
   association per namespace; this is not one control RPC per batch.
3. On the driver, inspect getPersistentRDDs and their dependency graphs to
   find the persisted wrapper whose lineage contains that producer RDD ID.
   Return the actual wrapper ID and an admitted persistence generation.
4. Admit cache-owned publication only after the response. A missing,
   ambiguous or retired owner must not silently become an immortal map entry.

The source establishes a useful ordering prerequisite, not a runtime proof:
RDD.persist registers with SparkContext before execution (RDD.scala:164–175;
SparkContext.persistRDD:2062), and getPersistentRDDs returns the actual
registered RDD objects even before successful materialization
(SparkContext.scala:2018). Public RDD.dependencies exposes parent RDDs
(RDD.scala:253). Thus first population through either writer, including an
AQE base-cache action, has a possible synchronous discovery point after the
wrapper exists. Executor-to-driver PluginContext.ask is an existing API.

The publication fence must execute before writer output becomes an externally
owned payload. Merely appending a map after the current GPU flatMap is not
necessarily early enough: that flatMap can create the whole list of PCBS
records first. This is an implementation proof obligation, not an authorization
to restructure the writers here.

Outstanding association cases:

- Prove exactly one owner for ordinary GPU, CPU fallback, row-input AQE,
  zero-column/count-only and nested/reused-cache plans. Do not choose an
  arbitrary ancestor or assume the serializer input RDD is the persisted RDD.
- Resolve ownership before allocating/publishing the first shadow entry,
  rather than relying on asynchronous JobStart/StageSubmitted listener delivery.
- Handle retirement while the task's ownership request is in flight.
- After successful association, checkpointed/cleaned lineage must not be
  required for every later lookup; RDD.dependencies follows checkpoint
  replacement when present. Initial discovery through already-checkpointed
  or unusually persisted serializer RDDs remains unproven.
- Do not keep strong RDD references in the namespace directory: they can
  prevent the GC cleanup whose notification is needed.
- Containment alone can be ambiguous: producer P can feed persisted cache A,
  which feeds persisted cache B. Both A and B then have P in their dependency
  graph. The mapping must identify the actual owning wrapper, not select
  either match merely because P is an ancestor.
- Multiple persisted descendants, generic RDD re-persistence, and direct use
  of the serializer need explicit behavior. Ordinary SQL recache creates a
  fresh wrapper; that does not prove arbitrary same-RDD reuse is covered.

This is a credible registration experiment to run next; it is not a claim
that every cache population route has already passed.

## Existing local storage and transport pieces

SpillableHostBuffer (SpillableColumnarBatch.scala:463–508) owns a
SpillableHostBufferHandle, exposes materialize/getHostBuffer and closes the
handle. The host handle (SpillFramework.scala:730 onward) keeps host memory
or a disk handle, increments the host buffer reference when materialized,
and can spill host bytes to a local disk file. DiskHandle (:1581 onward)
reads through the spill serializer/compression wrappers and deletes its
local file on close.

These are appropriate local storage primitives to investigate. They do not
register global payload IDs, replicate files, announce executor-independent
locations or retain a cache-owned generation automatically. Do not assume a
materialization call by itself protects against concurrent namespace
retirement for its entire operation; the owner must keep the handle alive.
The current spill implementation documents disk-full/spill failure as a
task/executor-level failure, not transparent durable recovery.

The existing RapidsShuffleTransport (:304–413) supplies peer connections,
metadata buffers, transfer throttling, bounce-buffer pools and pending-request
cancellation. BufferSendState (:100 onward) demonstrates windowed transfer
and materialization. However its current service contract is shuffle-specific:
RapidsShuffleRequestHandler (RapidsShuffleServer.scala:43–84) looks up
ShuffleBlockBatchId/TableMeta and integer table IDs; the concrete catalog
returns device spill handles. UCX peer discovery expects RAPIDS topology
information (UCXShuffleTransport.scala:212–234).

Consequences:

- Reuse of bounce/window/throttling mechanics is plausible; the present
  shuffle service is not already a generic host-Parquet-payload fetch API.
- Do not insert cache IDs into existing shuffle ID maps without establishing
  separate ownership, collision and removal contracts.
- PCBS works in configurations that do not select the UCX shuffle path.
  An implementation depending on that transport needs a working alternative
  or explicitly authorized compatibility change; none is selected here.
- The association/control candidate assumes initialized driver/executor plugin
  components. Serializer-only or CPU configurations without those components
  need separate coverage; no new plugin requirement is authorized here.
- PluginContext.send/ask supports executor-to-driver control messages.
  PluginContext has no driver-to-every-executor push method and no bulk
  off-heap byte-transfer contract. Relaying GiB payloads through driver RPC
  would be a new data path, not reuse of the existing spill mechanism.
- New internal plugin RPC/control endpoints can be investigated on stock
  Spark, but their discovery, authentication, shutdown, failure and transfer
  semantics still need implementation and verification. No claim is made
  that a plugin can subscribe to existing BlockManager RPC commands.

## Bounded payload-resolution options

These options are alternatives, not a design decision.

| Candidate | Unmodified Spark? | What it addresses | Still unproven |
|---|---|---|---|
| Shadow directory plus local/peer payload service, using spill handles locally | Yes in principle; new plugin protocol | Small Spark records and remote resolution | Publish/lookup ordering, bulk transport, leases, replica durability, missing-payload recovery |
| Same shadow design with an independently durable payload copy | Yes in principle; new storage protocol | Surviving origin executor loss | Cost, location discovery, commit/checksum semantics, deletion and permissions |
| Self-contained serialized payload streamed into/out of off-heap storage | Yes in principle through serialization, subject to CPU/Kryo testing | Existing Spark disk/network copies remain independently usable | No-large-array read/write proof, owner lifetime after disk decode, compatibility/performance; this is not metadata-only serialization |
| Spark lifecycle/missing-block integration or a BlockManager construction hook | **Last resort: Spark change, 4–5-year rollout estimate** | Explicit cache ownership/miss signals or custom manager installation | Cross-version implementation and deployment; does not meet the short-term boundary |

For a reference-only representation, Spark's serialized disk block contains
only metadata, so its survival says nothing about the payload's survival.
A local file path or executor-local map ID is insufficient for another process.
Even same-host remote cache reads can open another executor's Spark disk file;
they do not gain access to that process's pointers.

## Failure, replication and recomputation

Spark RDD.getOrCompute (:374) calls BlockManager.getOrElseUpdateRDDBlock;
BlockManager.get (:1275) tries local and remote cached blocks before computing
lineage. getRemoteValues (:1060) deserializes received block bytes and returns
an iterator without putting the fetched objects into a new local cached block.
Therefore a temporary remote reader needs ownership even if it never becomes
a registered Spark cache holder.

Spark doGetLocalBytes (:1019) serves disk bytes or serializes in-memory objects.
Replication (:1650,1782) and decommission migration
(BlockManagerDecommissioner.scala:356) copy those bytes. In a reference-only
design those operations copy metadata references, not shadow payloads.
Decommission then removes the original Spark block; losing that origin
executor must not invalidate a supposedly surviving copy.

A missing shadow payload behind present metadata is not Spark's ordinary
cache miss. Neither successful metadata replication nor Spark's lineage
fallback proves recovery. A reader exception can simply cause another attempt
to fetch the same stale reference.

Before claiming production recovery, select and prove one of:

- Payload copies remain independently available whenever Spark advertises
  usable metadata copies. This includes migration completion and source loss,
  not just synchronous remote reads while the origin is alive.
- Missing-payload handling reliably invalidates/rebuilds the appropriate
  Spark partition and retries without returning inconsistent data. A simple
  synchronous removeBlock in the decoder is not established safe: Spark may
  still hold a read lock while the decoder tries to obtain a write lock.
- A durable external payload copy provides recovery independent of Spark's
  executor-local copies.

The shadow directory also needs attempt identity. Retried/speculative
partition attempts can produce different batches or publish before their
Spark block wins. Namespace plus partition plus ordinal alone is not enough
to identify a particular physical payload safely. Each visible reference must
identify immutable bytes, and uncommitted/losing-attempt data needs cleanup
without deleting the winning attempt.

## Ownership and retirement

The minimum separate ownership states are cache retention and active operation
retention. Dropping a Java metadata wrapper or evicting Spark's memory copy is
not whole-RDD retirement: its serialized disk reference may be read later.

Reader/transfer protection must cover:

1. Resolving a reference and acquiring its payload before concurrent retirement.
2. Materializing from local disk, decoding, or preparing transfer windows.
3. Transfer completion/cancellation and receiver installation.
4. Releasing operation retention on normal completion, failure, cancellation
   and executor shutdown.

Closing cache ownership can retire the namespace while operations drain.
Closing a temporary materialized HostMemoryBuffer does not release the
namespace's separate cache ownership. Conversely a task-completion callback
cannot free payloads that the persisted cache must retain. These requirements
are inferred from the verified read/spill/transfer paths; no new lease
implementation has been tested.

SparkContext.unpersistRDD (:2069) initiates/awaits Spark RemoveRdd, removes the
RDD from persistentRdds, then posts SparkListenerUnpersistRDD. ContextCleaner
(:222) uses the same path for GC cleanup. SQL clearCache unpersists the actual
cached wrapper (InMemoryRelation.scala:230).

Driver-listener relay is a useful trigger, not a reliable deletion protocol:
AsyncEventQueue.scala:151 drops events when full. With blocking=false the event
can arrive while Spark removals/readers are still draining. With blocking=true
the standard API still does not await plugin cleanup. No promise that existing
blocking unpersist synchronously completes shadow cleanup can be made solely
from the listener.

An unmodified-Spark lifecycle candidate needs explicit admitted generations,
idempotent retirement, acknowledgments and reconciliation against driver
persistence state. Executors can poll authoritative state through plugin
messages and retain retirement watermarks/tombstones to reject late cache
publication. Retry unacknowledged cleanup; do not infer success merely because
a message was sent. A departed executor and a live executor that has not
acknowledged are different cases.

Snapshots cannot recover every ordering edge by themselves. An unpersist and
re-persist of the same RDD ID between snapshots, combined with a dropped event,
is ambiguous without an additional incarnation signal. SQL's fresh wrappers
reduce this issue but do not establish a general solution. Exact synchronous
blocking-unpersist parity and arbitrary same-RDD lifecycle parity remain
hard requirements without a proven stock-Spark hook. A Spark lifecycle change
must remain listed as the last-resort route if these requirements cannot be
met without it.

## Proofs required before production acceptance

The local NODELETES experiment answers only whether removing large byte arrays
from Spark's object graph changes this workload's heap pressure/performance.
Its fresh-JVM process exit reclaims leaks. It proves none of the following:

- Owner association before first publication for GPU columnar input, CPU
  columnar/row fallback, AQE baseCacheRDD, count-only/empty schemas, repeated
  scans and multiple cached relations.
- Remote metadata reads on another executor, including a fetched reference
  that is paused before lookup while unpersist occurs.
- Cache memory eviction to disk and reload with the original wrapper gone.
- Retrying/speculative writers, failure before publish, and losing attempts.
- Origin loss after metadata replication, decommission migration, and a
  missing payload with still-readable metadata.
- Retirement during local disk materialization, GPU decode, queued transfer
  and active transfer; no use-after-close and no permanent orphan.
- Dropped listener notification, delayed/duplicate/out-of-order cleanup,
  executor restart, driver-side namespace reclamation, and re-persistence.
- Released CPU/GPU read APIs, schemas, storage levels and logical optimizer
  statistics. See the independent metadata research for size/statistics effects.

These are observable acceptance checks for later work, not extra claims about
the disposable experiment or permission to change production code now.
