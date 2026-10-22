# BlockManager cache codec research spike

Status: IN PROGRESS. This is disposable upstream research, not a production
Spark or RAPIDS change. Existing cache behavior is RELEASED and preserved by
default. The optional codec carrier and fixtures are UNRELEASED and have no
compatibility obligations. See the [accepted proposal](pcbs-cache-storage-api-proposal.md)
and [investigation tracker](pcbs-oom-investigation.md).

## Scope and boundaries

Prove per-cache byte encoding inside the real BlockManager before selecting a
public API. Heap-backed records are allowed for this first proof; that does not
establish a heap OOM fix. Native payloads must use RAPIDS-managed spillable
storage. Spark's existing OFF_HEAP pool is excluded, including as a fallback.

The research candidate associates an immutable streaming serializer with the
actual SQL statistics-wrapper RDD using a custom ClassTag as a temporary
carrier. Two small isolated source overlays select that codec in
SerializerManager and supply the tag in CachedRDDBuilder. This is not a
recommended public ClassTag API. Ordinary tags retain normal serializer
selection. No separate driver directory, publish protocol or recovery path is
introduced.

## Decisive source findings

The independent source audits and root inspection agree on the carrier path:
RDD.getOrCompute passes elementClassTag to BlockManager; BlockInfo and memory
entries retain it; local disk/memory readers and eviction use it. Serialized
unrolling calls SerializerManager.getSerializer directly, so the selector
hook covers a path missed by changing only dataSerializeStream. Replica uploads
already send (StorageLevel, ClassTag) as small Java-serialized control metadata.
Remote requesters supply the tag from their persisted RDD. Runtime tests must
prove subtype/context survival; type equality alone cannot identify a codec.

The native audit identifies a separate ownership boundary. MemoryStore closes
AutoCloseable deserialized values on removal; a value can own a RAPIDS
SpillableHostBuffer. getHostBuffer returns a separately owned materialized
reference. Block read locks must protect access to the spillable owner through
materialization and lazy consumption. The factory uses trackNoSpill, so wrapping
an allocation alone does not establish bounded host-store admission.

BlockManager status currently derives presence and sizes from MemoryStore and
DiskStore. Retaining a small native handle there can test lifetime, but does
not prove complete native residency/spill accounting. Native payload bytes must
not be reserved against Spark heap or its OFF_HEAP pool. A complete backend
integration needs explicit accounting/status semantics after this proof.

## Required runtime evidence

- Actual SQL persisted wrapper selects the codec; generic bulk serialization
  fails visibly; two different codec contexts coexist with ordinary defaults.
- Serialized memory, disk reload, failed/partial admission and deferred eviction.
- Multiple JVMs, remote fetch and replicas that never ran the producer task.
- Exact rows/statistics, empty partitions, existing recovery and blocking removal.
- Version-specific overlays preserve each version's SQL materialization logic.
- Separately: actual RAPIDS spillable owner, bounded native allocation, forced
  spill, disk reload and reader/removal cleanup. The first lifetime proof is
  limited to deserialized MEMORY_AND_DISK, not all StorageLevels.

## Verification record

No new runtime result is claimed yet. Independent transfer and native ownership
source audits are complete; prototype implementation and root verification are
in progress. Raw audit notes are under /data/pcbs-oom/blockmanager-transfer-audit/
and /data/pcbs-oom/blockmanager-native-audit/. All project operations use MCP
docker_exec tools, including every worker handoff and verification checkpoint.
