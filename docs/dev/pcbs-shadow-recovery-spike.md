# Narrow PCBS shadow-payload research spike

Status: READY FOR REVIEW — source findings and proposed experiments only.
2026-10-22. No production patch, runtime recovery proof or new benchmark.

## Clarification of the production plan

The architect challenges treating every item as a separate unresolved design.
Use existing buffer refcounts, ordinary Spark retry/recomputation, CPU stream
adapters, primitive descriptor serialization and existing spill-store budgets
where they suffice. Do not introduce bespoke serializers or a new memory policy
merely because payloads move off heap.

The earlier plan's requirement that every copied descriptor remain readable is
stronger than necessary if a missing payload can safely fail the whole task,
invalidate stale cache metadata and let Spark recompute. This is an alternative
to pre-fetch ownership admission, not an established runtime solution. Acquired
native buffers still require correct refcounted lifetime. Do not mix a prefix
from the old cached partition with a suffix recomputed under different batches
or nondeterministic values.

Existing PCBS behavior is RELEASED; the descriptor/map/protocol is UNRELEASED.
Normal Java/Kryo and storage-level support remains a regression-test obligation,
not a requirement to preserve old byte-array internals or write custom
serializers. No change to supported storage-tier/replication behavior has been
selected. Existing Spark remains the priority; Spark changes remain last resort.
All project work and worker handoffs/checkpoints use MCP docker_exec only.

## Source findings independently checked

- Spark 3.5.5 RDD.getOrCompute (RDD.scala374) delegates to BlockManager.
  BlockManager.getOrElseUpdate (1374 onward) returns a present cached iterator;
  it computes lineage only when the block lookup/put path needs computation.
  Missing executors remove block locations. Surviving replicas can be fetched;
  if none is usable, the partition can be recomputed.
- A shadow lookup exception after the descriptor cache hit does not itself
  invalidate that hit. Ordinary task retry can read the same descriptor again.
  Spark FetchFailed is specifically a shuffle failure: DAGScheduler1943 resolves
  shuffleIdToMapStage. It is not a generic cached-RDD invalidation signal.
- Executor.scala613–625 releases task block locks while unwinding task.run.
  The ordinary exception path then invokes plugin.onTaskFailed before
  statusUpdateFAILED (819–820). This provides a candidate point for a custom
  missing-payload marker and acknowledged invalidation before retry.
  TaskContext is already unset; the marker needs its own task identity.
- BlockManagerMaster.removeBlock is not a completed-removal barrier: the master
  schedules removal RPCs and replies without waiting. Existing internal
  getExecutorEndpointRef returns the storage endpoint, whose RemoveBlock reply
  follows local removal. A driver coordinator could investigate direct,
  acknowledged removal from every relevant holder using version shims.
  This needs runtime proof, holder/publication race handling and bounded timeout
  behavior. Plugin callbacks suppress exceptions; throwing from the callback
  does not ensure successful invalidation.
- Driver SparkListenerBlockUpdated observes accepted reported block changes;
  BlockManagerRemoved observes lost storage executors. UnpersistRDD observes
  whole-RDD retirement. These are asynchronous observations, not interception
  of the BlockManager RPC handler. Memory-to-disk movement is not payload
  deletion. There is no successful-payload-version identity in those events.
- PCBS ByteArrayInputFile is a Parquet InputFile adapter with seekable streams.
  CPU readers can be adapted to a HostMemoryBuffer-backed seekable InputFile.
  This removes the need for a batch-sized input array, not parquet-mr's own
  decoded page/row allocations. Descriptor primitives need no inherently
  custom Java/Kryo serialization.
- SpillFramework.initialize2494–2506 uses a finite host-store limit derived from
  the existing host spill-storage setting (default pinned pool plus 1 GiB) when
  the global off-heap limit is disabled. With the global limit enabled, the store
  relies on HostAlloc instead. makeBuilder1988–2028 uses host-or-disk admission;
  trackNoSpill1962 bypasses the retained-store budget. Current materialize
  allocates a full host buffer for restore, so retained bytes and simultaneous
  restore/writer/transfer buffers must be distinguished.

## Proposed executable work

1. **Recovery and block notification proof, two or more executor JVMs.**
   First verify ordinary cached-RDD source loss and recomputation as a control.
   Then retain/replicate a valid descriptor while deleting only its payload.
   Confirm a plain exception can re-hit stale metadata. Test a dedicated missing
   payload error plus acknowledged whole-partition invalidation after failed-task
   lock release and before reporting failure. Require automatic successful retry,
   correct values and evidence that the producer really ran again. Exercise two
   failing readers, a paused copied descriptor, source death, late publication,
   replicas and another consumer holding the block. Record block update/removal
   events to establish their usable mapping and ordering. No forced executor
   killing as the recovery mechanism, fabricated shuffle failures, weakened
   retry settings or Spark modifications.
2. **Existing store admission and decoder proof, independently.**
   Add a narrow host-payload admission adapter reusing existing store policy,
   plus seekable native InputFile coverage for both CPU reader paths. With
   global limits enabled, exceed HostAlloc's global host budget; with them
   disabled, exceed SpillableHostStore.maxSize. The separate spill-store limit
   is ignored in enabled mode. Verify disk placement, reload, concurrent restore
   accounting and cleanup. Test primitive descriptor Java/Kryo roundtrips and serialized/disk
   reload rather than implementing custom serializers. Keep original large
   cache records and Spark memory/unroll/reset defaults.

Stop the recovery track if invalidation can deadlock, retry can consume the
same stale metadata indefinitely, or generations cannot be distinguished during
replacement. Report the exact failure before choosing another architecture.
The store track can proceed independently. Only after these focused proofs
should the full distributed production protocol and performance work proceed.

Root delegated recovery/event tracing and CPU/store tracing, then independently
read the decisive paths above. This is source verification only. No execution
of the proposed spikes is claimed. See the [production plan](pcbs-shadow-payload-plan.md).
