# BlockManager cache codec research spike

Status: READY FOR REVIEW for the bounded codec and native lifetime proofs.
Genuine recovery and BlockManager migration are also verified on Spark 3.5.5.
This is disposable upstream research, not a production
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

Root independently compiled and ran both frozen fixtures, then checked retained
source/artifact hashes and raw Spark events using a separate evidence verifier.
Two focused peer reviews found no blocking issues within the stated scope.

| Proof | Runtime | Executor JVMs | Task completions | Unexpected failures |
| --- | --- | ---: | ---: | ---: |
| Heap-backed per-cache codec | Spark 3.5.5 / Scala 2.12 | 2 | 44 | 0 |
| Same codec proof | Spark 4.0.1 / Scala 2.13 | 2 | 44 | 0 |
| RAPIDS native lifetime | Spark 3.5.5 / Scala 2.12 | 2 | 4 | 0 |
| Genuine recovery and BM migration | Spark 3.5.5 / Scala 2.12 | 2 | 14 | 0 unexpected; 1 required FetchFailed |

Each run had one normal ApplicationEnd. Heaps are 1 GiB for each driver/executor.
These are small mechanism tests, not another large PCBS pressure/performance run.

Codec passes: actual SQL wrapper association; two schema contexts with the same
CachedBatch runtime class; Java metadata tuple roundtrip; ordinary Java/primitive
RDDs and DefaultCachedBatchSerializer wrapper fallback; Java/Kryo bulk tripwires;
MEMORY_ONLY_SER, DISK_ONLY, MEMORY_AND_DISK_2 and MEMORY_ONLY; explicit Netty
fetches with exact values; two replicas; eviction to disk and reload; forced
partial serialized admission with both prefix replay and stream completion;
empty partitions; removal blocked by a held reader. Replica decode occurs with
TaskContext absent on both executor JVMs. Those JVMs also produce other
partitions, so this does not prove a receiver that never produced any cache data.
The held-reader race is local removeRdd, not a full distributed SQL unpersist race.

Native passes: real RAPIDS SpillableHostBuffer values in deserialized
MEMORY_AND_DISK entries; forced RAPIDS spill/restore and BlockManager disk
reload; master blocking removeRdd; independently materialized native reference
survives owner removal; remote decode and executor cleanup. Driver owners balance
4/4, each executor 1/1; native payload stores are empty after removal; spill files
are deleted. RAPIDS HostAlloc has a 64 MiB limit, over-limit allocation is rejected,
Spark off-heap storage usage is zero. A 1 MiB host bounce allocation remains until
framework shutdown, after which driver host usage is zero. Framework setup also
allocates a 1 MiB device bounce buffer; no query GPU operators are exercised.
SQLPlugin is disabled; the fixture manually initializes the same RAPIDS memory
and spill components. The original native PCBS artifact supplies those classes;
its unrelated payload registry is not exercised.

A suspected restore-cap bypass was checked and rejected during review:
HostMemoryBuffer.allocate delegates to DefaultHostMemoryAllocator, which
HostAlloc.initialize installs. Root verified the exact supplied artifact's
bytecode and the initialization source before accepting that conclusion.

Root evidence: /data/pcbs-oom/blockmanager-codec-spike/root-acceptance355,
/root-acceptance401/run (under the same parent), and
/data/pcbs-oom/blockmanager-native-spike/root-final-overlay. Committed
[results](../../tools/pcbs-blockmanager-codec-spike/verified-results.json) include
artifact hashes; [runners](../../tools/pcbs-blockmanager-codec-spike/README.md)
reproduce the tests. Independent verification rechecked 249/282/250 input hashes.
All project operations use MCP docker_exec tools, including every worker handoff
and verification checkpoint.

## Recovery and migration extension

Root independently ran the final extension at
/data/pcbs-oom/blockmanager-codec-spike/root-recovery-final355. It removed four
real shuffle data/index files and invalidated map outputs while all four cached
partitions were still advertised. After task scheduling, fixture-only driver
control called Spark's existing blocking master.removeRdd and confirmed every
cached location was absent before parent reads. One genuine
MetadataFetchFailed/FetchFailed triggered ordinary map-stage rebuilding. Thirteen
tasks succeeded, exact 10,000 rows returned and map outputs became available again.

The real DecommissionBlockManager endpoint migrated a sample cached block from
executor 0 to executor 1 before any later SQL job. The test compared the complete
sample payload before/after relocation and checked the whole SQL result. This
establishes BlockManager RDD migration, not process termination, full scheduler
decommission policy, or native ownership during migration. Recovery/migration
were not runtime-tested on 4.0.1.

The first root recovery repeat failed its mandatory FetchFailed assertion despite
correct rows. Investigation found a fixture error: it forwarded addressless
executor RPC references to another executor, potentially resolving the wrong
local endpoint and leaving cache blocks readable. Spark's NettyRpcEnv source
around line 520 explicitly forbids sharing those refs with third parties. Root
read the routing/deserialize implementation; endpoint/status diagnostics confirmed
the wrong target. The corrected routable driver fault-control endpoint changes
only injection, not Spark recovery. The old failed evidence is preserved at
/root-recovery355 under the same parent. Final source and runner snapshots match
the committed versions, and both the corrected worker and root runs pass.

## Remaining criteria

Full native residency/status accounting, bounded end-to-end transfers, native
cancellation/failed admission/replication, other native StorageLevels and disabled
host-limit admission remain unverified. Native values here use ordinary heap
metadata entries; no complete native store integration is claimed. Serialized
sender paths can still build partition-sized heap chunks. The native lifetime
proof is not yet an actual PCBS/SQL/GPU pipeline. Spark versions newer than 4.0.1
remain source-audited only. Full distributed SQL unpersist races and process-loss
recovery remain outside the bounded proof. The new codec recovery/migration
results above are separate from the earlier token research.
