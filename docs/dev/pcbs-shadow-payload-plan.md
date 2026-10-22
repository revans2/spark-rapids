# PCBS spillable payload production plan

Status: READY FOR REVIEW — design proposal, not an approved production architecture.
Prepared 2026-10-22 for issue #16362. No production implementation is included.

Follow-up: the [narrow recovery/store spike](pcbs-shadow-recovery-spike.md)
examines whether normal task retry after acknowledged cache invalidation can
replace the stronger pre-fetch ownership requirement below. It also narrows
CPU/serialization and host-budget work to adapters over existing mechanisms.
That alternative is source-supported but not runtime-proven.

## Goal and evidence

Keep large Parquet payloads out of Spark's Java cached objects while preserving
large GPU batches, correct cache lifetime, distributed reads and recovery.
The preferred design uses small cached descriptors and a spillable payload
manager. It must work with existing Spark. A Spark change is a last resort,
with the user's estimated four-to-five-year deployment horizon.

The disposable local prototype establishes the memory benefit, not distributed
correctness. With unchanged default Spark unroll/reset/memory-fraction settings,
the native path passed approximately 191 GiB of encoded cached data with
16 task threads, a 2 GiB batch target, automatic GPU concurrency and a 512 MiB
heap. Actual records were about 1.99 GiB; that case needed the documented
test-only projection threshold override. The baseline retained the original
six approximately 1 GiB records per partition. The RAPIDS tracked host off-heap
limit was enabled at 40 GiB, pinned memory at 2 GiB, and the GPU pool at 15 GiB.
That host limit is not a cap on all native allocations or whole-process RSS.

The prototype deliberately never deletes payloads and cannot resolve another
JVM's IDs. Its passing result does not establish remote-read correctness,
production cleanup, a minimum heap recommendation or literally zero heap
payload traffic. Existing spill/compression paths still use bounded Java
staging buffers. See [proof and measurements](pcbs-shadow-payload-proof.md)
and [performance CSV](pcbs-shadow-performance.csv).

## Released contracts and scope

Existing PCBS behavior is RELEASED: it is present on main and documented in
the [cache serializer guide](../additional-functionality/cache-serializer.md). Preserve CPU row/columnar decoding, serializer-only
deployment, GPU decoding, supported schemas/projections, row counts and order,
partitioning, Java/Kryo serializer choices and supported storage levels.
Replication, decommission and ordinary cache-loss recomputation must remain
usable; a metadata replica alone is not a usable payload replica.

The experimental map, ID representation and native prototype implementation
are UNRELEASED. They can be replaced directly; no compatibility adapter for
that prototype is required. This classification concerns behavior, not a promise
that application-local cached bytes survive a plugin binary upgrade. Any new
wire format must define its own version and mixed-reader failure behavior.

Keep original GPU batching and existing safety splits. Do not shrink batches,
change Spark's memory fraction, unroll checks or serializer reset to obtain
a passing production result. No new public configuration, external durable
storage dependency or mandatory plugin requirement is selected by this plan.
A compatibility fallback may preserve an existing deployment mode; it must be
explicit about whether that mode receives the heap improvement.

## Proposed components

1. **Cached descriptor.** Store a versioned namespace/generation and immutable
   payload identity, row count and encoded byte length, with enough location or
   directory information for another executor to resolve it. Preserve one
   independently decodable Parquet record per descriptor. Do not replace a
   record with an ordered sequence of separate cache elements.
2. **Executor payload manager.** Reuse `SpillableHostBuffer` and the existing
   host/disk spill framework. Own native buffers and spill files, keep local
   lookup fast, and coordinate publication, readers, transfers and cleanup.
   Finish the Parquet writer before publishing an immutable entry. Failed
   unpublished attempts release resources through ARM.
3. **Driver coordinator.** Associate a namespace with the actual persisted
   cached RDD and its persistence incarnation; track holders and retirement.
   Send control metadata, never bulk Parquet bytes, through the driver.
   Bound directory size and update traffic; choose executor/partition manifests
   versus per-payload entries from measured needs.
4. **Payload resolution and transport.** Resolve locally first, then obtain
   remote data into bounded native buffers or spill files. Specify how payload
   availability follows Spark replication/decommission and how missing payloads
   become an ordinary recomputation rather than repeated stale-reference reads.
5. **Statistics and diagnostics.** Preserve `CachedBatch.numRows` and
   `sizeInBytes` as the original encoded Parquet byte length. Spark's cache
   accumulators continue to supply logical cache statistics to planning.
   BlockManager reports actual descriptor storage bytes; do not inflate these
   to pretend payloads are in Spark's memory store. Report shadow host/disk
   usage, live generations, active operations and transfer/spill failures separately.

Use the existing local prototype as implementation evidence, not a drop-in
production manager. It lacks the ownership and recovery machinery below.
Replacing Spark's BlockManager or changing its reported sizes does not provide
that machinery.

## Lifetime protocol to prove before production implementation

Separate two ownership rights:

- **Cache ownership** retains a payload while its cache generation exists,
  including when Spark evicts descriptors from memory to disk.
- **Operation ownership** retains data while an admitted reader, builder or
  transfer can still use a descriptor, including copies not yet decoded.

Proposed namespace sequence: associate owner → active → fence publication →
drain existing operations and cache holders → reclaim → retain a tombstone.
Payload entries progress from private construction to prepared, published,
retired-but-in-use and closed. All protocol messages must be idempotent and
identify application attempt, namespace generation and executor incarnation.
Retries/speculation need immutable attempt identities; an ordinal is insufficient.

Publication must establish the selected availability guarantee before a
descriptor escapes. Cleanup must not release an admitted operation simply
because a job ended, cancellation was requested or a heartbeat/TTL expired.
Do not retain strong references to RDD objects and thereby defeat Spark's
ContextCleaner. Bound abandoned attempts, tombstones and driver directory state.

The following are unresolved correctness gates, not implementation details:

- **Actual owner binding.** Spark wraps the serializer's producer RDD with a
  statistics RDD and persists that wrapper. Register that exact owner before
  the first payload is published. Plan preparation is a candidate; prove AQE,
  CPU/fallback, count-only and direct cached-RDD entry points. A transitive
  lineage match can identify multiple persisted descendants.
- **References copied before decode.** Spark can copy serialized descriptors,
  release the source block lock and decode later. A lease acquired only in
  PCBS lookup or `readObject` is too late. Executor-plugin task-start admission
  is a candidate, but must cover non-task transfers and late tasks too.
  Spark suppresses plugin callback exceptions, so throwing cannot enforce
  admission; payload use must check successful admission explicitly.
- **Retirement ordering.** Spark's unpersist listener is asynchronous and is
  posted after the BlockManager removal call. It neither fences earlier
  publication nor makes blocking unpersist wait for shadow cleanup.
  Listener/poll reconciliation alone cannot distinguish every unpersist and
  re-persist of the same RDD ID. Prove an incarnation-aware barrier and define
  blocking versus nonblocking cleanup, including existing readers.
- **Availability after executor loss.** A surviving descriptor must resolve
  a surviving payload or trigger safe recomputation of the right partition.
  Throwing at decode can repeatedly fetch the same stale descriptor.
  Removing Spark blocks while their read locks are held may deadlock.
  Prove the path rather than treating an exception as a cache miss.
- **Replacement within a live generation.** Repeated partition loss and
  recomputation can publish new successful attempt payloads while the cache
  stays persisted. Specify bounded reclamation/replacement or deduplication
  without invalidating old copied descriptors or in-flight readers. Retaining
  every successful version until whole-RDD unpersist is not sufficient.

See [RDD/BlockManager flow](pcbs-rdd-block-manager-flow.md) and
[lifecycle research](pcbs-shadow-lifecycle-research.md) for source traces.

## Transport and representation decision

Evaluate these in order of reuse, without assuming any is already sufficient:

| Option | Benefit | Required proof / unresolved issue |
| --- | --- | --- |
| Auxiliary payload blocks through existing Spark services | Reuses location, transfer and potentially replication infrastructure | Block identity, storage policy and lifetime must remain separate and correct. Do not spoof `RDDBlockId` or assume convenience byte-fetch APIs avoid heap copies. |
| Plugin-owned peer payload service with a driver directory | Explicit native/file transfer and shadow lifetime control | Must supply discovery, Spark authentication integration, bounded in-flight memory, cancellation, replica/decommission handling and recovery. UCX cannot be required for all PCBS deployments. |
| Self-contained streaming serialization of off-heap payloads | Spark disk/network copies own their bytes, avoiding dangling ID-only references | This changes the preferred representation. Java and Kryo both need support. Spark may aggregate a whole serialized partition in heap chunks for remote transfer/replication, so local success does not prove distributed heap safety. |
| Modify Spark: lifecycle and recoverable payload hooks | Could expose an authoritative retirement/admission/missing-block contract | Last resort; does not meet the short-term deployment requirement. Present explicitly if stock Spark cannot satisfy the gates. |

Existing Spark `getRemoteBytes` and `fetchBlockSync` paths can copy into heap
buffers. Reusing their transport requires a reviewed streaming/native path,
not simply replacing a byte array with a `ManagedBuffer` at the call site.
Existing RAPIDS shuffle transport offers useful bounded-transfer patterns,
but its shuffle identity/catalog and UCX assumptions are not a cache contract.

An asynchronous listener plus indefinite retention is acceptable only for
disposable research, not a production ownership solution. If the ID-only gates
cannot be met, stop that implementation track and bring the evidence and
alternative representation to the architect.

## Implementation sequence and acceptance gates

### 1. Close the architecture gates with focused stock-Spark proofs

Delegate independent owner/lifetime and transport/recovery proofs. Root reads
decisive code and independently reproduces results. Use MCP docker_exec for
every operation and repeat that requirement at handoffs/checkpoints.

Use multiple JVMs, not local mode alone. Pause a remote reader after descriptor
bytes are copied but before decode, unpersist the owner, then resume. Also
copy and discard descriptors without ever decoding them. Pause a writer after
preparation/availability acknowledgement but before descriptor publication,
retire its namespace and resume: no late publication or orphan is permitted.
Exercise queued/active transfers, cancellation, speculation, executor death,
restarted executors receiving old-incarnation messages, same-RDD re-persist,
dropped/duplicate/reordered control events and blocking unpersist. Prove exact
owner association before publication with AQE on/off and fallback.

**Exit:** reviewed protocol, executable race/recovery tests, a chosen transport,
and explicit storage-level/CPU deployment policy. Failure of a hard gate parks
the production ID-only path; do not substitute TTL or weaken acceptance.

### 2. Implement the local payload manager and native PCBS path

After design acceptance, adapt the proven writer/reader and spill handles.
Add immutable identities, prepare/publish/abort transitions, reference counting,
resource cleanup and protocol diagnostics. Keep control structures bounded.
Preserve row decode and existing serializer-only operation through an approved
implementation or explicit existing-path fallback. Cover Java and Kryo.

**Exit:** exact rows/values/schema, default Spark checks, forced host-to-disk
spill/reload, concurrent access and failure cleanup; no batch-sized Java payload
arrays in the GPU bulk path. Retain original approximately 1 GiB cache records.
No exposure as a production default before distributed and cleanup gates pass.

### 3. Implement distributed resolution, retirement and recovery together

Implement the approved coordinator, transport and ownership protocol as
dependent pieces, not independently enabled features. Publish only after
availability acknowledgement; preserve active-operation rights during
retirement. Reconcile control state without treating suspicion as proof of
death. Clean abandoned writers, retired generations and application shutdown.

**Exit:** multi-executor local/remote reads, same-host cross-JVM reads,
replication, decommission, origin loss, partition recomputation and all paused
reader/retirement races pass. Cache-owned resources eventually return to the
measured baseline after operations drain. Repeatedly evict/lose and recompute
partitions within one still-persisted generation; obsolete successful versions
must remain bounded while old readers stay correct. Failed cleanup is observable
and bounded; no silent permanent orphan or stale-reference retry loop.

### 4. Verify released behavior and version boundaries

Add targeted tests beside existing PCBS/cache/spill suites; update all affected
Spark shims consistently. Start with Spark 3.5.3/3.5.5 and 4.0.x, then complete
the supported Spark/Scala/platform matrix. The existing research covers only
parts of that matrix; Databricks and older releases remain validation work.

Test CPU-only/disabled GPU and serializer-only launches; GPU/CPU fallback;
Java/Kryo; supported deserialized/serialized, memory/disk and replicated storage
levels; empty/zero-column data; projection/reordering and supported nested types.
Verify logical cache statistics, loading/failure timing, AQE/CBO decisions and
partitioning against the released path. Check metadata eviction does not
retire payloads. Keep future predicate pruning possible.

**Exit:** required builds/style/license checks and meaningful integration tests
pass without changing released contracts to make them pass.

### 5. Measure performance and review for rollout

Record cache fill, warm-up, repeated read and representative downstream GPU
processing separately. Compare released PCBS and the accepted native path using
the same data, plans, batching, heap, host/device limits and Spark defaults.
Include low/high compression, variable-width data, resident and disk-dominated
working sets, customer-like 48 GiB/8 slots, and the existing 1/2 GiB, 8/16-thread
stress controls. Lower-heap tests are stress evidence, not recommended sizing.

Report actual encoded and decoded sizes, batch counts, correctness, per-executor
heap/RSS/native/disk usage, GC, transfer/spill bytes, GPU admission/utilization,
and fill/read/compute distributions. Avoid treating sampled heap as a live-set
measurement or local A6000 throughput as T4 customer performance. Agree numeric
regression budgets with the architect before accepting the final patch.

**Exit:** reviewed performance evidence and failure/soak results, bounded
control/cleanup overhead and no memory leak across repeated persist/unpersist.
Keep literal zero-heap spill/compression as a separately measured requirement;
bounded staging and bulk payload arrays are different claims.

Root coordinates bounded workers and focused peer review, verifies changes and
decisive tests independently, and commits each completed task with its tracker
update and DCO sign-off. Mark verified work READY FOR REVIEW; only the architect
marks DONE. Obtain human review of the complete diff and accurate description
before opening/updating any PR. No PR is authorized by this planning task.

## Decisions still needed

1. Which stock-Spark hooks can provide owner incarnation, pre-fetch operation
   retention and an ordered retirement barrier for every supported path,
   including reclamation of superseded payloads in a still-active cache?
2. Do we guarantee payload replicas/durable copies, integrate safe Spark
   recomputation, or use self-contained serialized bytes for remote ownership?
3. What payload-tier policy preserves each StorageLevel? For example,
   MEMORY_ONLY must not silently acquire a disk-backed payload contract, and
   DISK_ONLY needs an explicit policy rather than accidental host retention.
4. How does the improved path serve CPU/serializer-only deployments without a
   new mandatory plugin, and which fallback receives no heap improvement?
5. When RAPIDS host off-heap limits are disabled, what existing policy gives
   this long-lived store a finite budget or forces disk placement? Do not rely
   on unlimited native allocation or invent a public config without approval.
6. Must the first production patch eliminate even bounded Java staging during
   compressed spill/transport, or is avoiding batch-sized heap payloads enough?
7. What measurable fill/read/downstream regression budgets and supported-version
   rollout gates should control default enablement?

The first two are production feasibility gates. The remaining decisions affect
scope and acceptance; this document does not silently resolve them.
