# BlockManager cache codec research spike

Status: READY FOR REVIEW for the bounded codec and native lifetime proofs.
Genuine recovery and BlockManager migration are also verified on Spark 3.5.5.
This is disposable upstream research, not a production
Spark or RAPIDS change. Existing cache behavior is RELEASED and preserved by
default. The optional codec carrier and fixtures are UNRELEASED and have no
compatibility obligations. See the [proposal under review](pcbs-cache-storage-api-proposal.md)
and [investigation tracker](pcbs-oom-investigation.md).

## Scope and boundaries

Prove per-cache byte encoding inside the real BlockManager before selecting a
public API. Heap-backed records are allowed for this first proof; that does not
establish a heap OOM fix. Native storage is optional (nice-to-have), independent
of every codec gate. If pursued later it uses RAPIDS-managed spillable storage;
Spark's existing OFF_HEAP pool remains excluded.

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

## Required runtime evidence

- Actual SQL persisted wrapper selects the codec; generic bulk serialization
  fails visibly; two different codec contexts coexist with ordinary defaults.
- Serialized memory, disk reload, failed/partial admission and deferred eviction.
- Multiple JVMs, remote fetch and replicas that never ran the producer task.
- Exact rows/statistics, empty partitions, existing recovery and blocking removal.
- Version-specific overlays preserve each version's SQL materialization logic.

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

The historical optional native fixture passed successful lifetime/spill/reload
checks for deserialized MEMORY_AND_DISK. It manually initializes RAPIDS components
with SQLPlugin disabled; it is not an actual PCBS/SQL/GPU data path. Driver
owners balance 4/4 and each executor 1/1, payload handles are removed and spill
files deleted. A 1 MiB host bounce allocation remains until framework shutdown;
setup also creates a 1 MiB device bounce buffer. The over-limit request check is
qualified in the optional notes below, not treated as pressure admission.

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
decommission policy, or native ownership during migration. The original recovery run covered 3.5.5; confidence C below
adds an independently verified 4.0.1 run.

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

## Optional native path: open design notes

Native payload storage is a nice-to-have (human decision, 2026-10-22), not a
codec requirement. No codec design, implementation or confidence gate depends
on native work. More native implementation is outside the current handoff.
If pursued later, payload allocation uses RAPIDS-managed spillable storage;
Spark's separately partitioned OFF_HEAP pool remains excluded.

- **Borrowed values:** deserialized entries return the cache-owned object.
  Iterator exhaustion releases the block read lock even when downstream code
  still holds the last batch; eviction/removal can then close its owner.
  Readers must take their own buffer reference while holding that lock.
  The fixture does so before release. No test yet uses a borrowed batch after its iterator releases
  the read lock during eviction/removal.
- **Accounting:** MemoryStore uses SizeEstimator over the reachable heap graph,
  not native payload bytes. The instrumented existing fixture recorded
  **197,480 Spark memory bytes for a 32,768-byte native payload**. After RAPIDS
  spilled it, Spark still reported 197,480 memory bytes, zero disk bytes and
  useMemory=true. Heap bookkeeping can exceed a small payload; this is not a
  native residency measurement. Open options are: charge an estimate of native
  size to Spark; charge wrapper/heap bookkeeping only and report native residency
  separately; or have Spark ask RAPIDS for residency. None is selected.
- **Allocation evidence:** the configured 64 MiB HostAlloc test rejects one
  individually over-limit request. It does not establish admission, aggregate
  residency or progress under memory pressure.
- **Heap IO:** payload IO is still heap-buffered. Whole-partition serialization
  for remote sends explicitly collects 4 MiB heap chunks. Direct BlockManager
  disk eviction instead calls buffered streaming IO; it does not allocate those
  4 MiB chunks through that path. Neither establishes heap-free transfers.

Measurement: /data/pcbs-oom/blockmanager-codec-confidence/docs-native-size/
accounting-measurement.json. This instruments the existing lifetime fixture
without introducing another native backend or pressure workload.

## Remaining codec criteria

The new confidence handoff covers configurations, clean receiver context,
upstream-target compatibility, partial/failing reads, distributed SQL unpersist
and controlled heap/GC/timing benefit. Process-loss recovery and other unsupported
routes remain outside earlier bounded proofs. Native backend/accounting,
failure paths and transfer improvements are optional and out of scope; they do
not block the codec goal.

## Confidence handoff results

These are independent heap-codec confidence runs. Native work is optional.
Every listed case has a fresh worker run, fresh root rerun, input/artifact
hashes, raw logs/events and a result/verifier JSON. Each case is committed with
the tracker and marked READY FOR REVIEW, not architect-accepted DONE.

| Case | Status | Verified scope |
| --- | --- | --- |
| C | READY FOR REVIEW | Spark 4.0.1; 13 successes and 1 genuine FetchFailed; exact 10,000 rows; 2 removal gates; BlockManager migration; 287 hashes. |
| D | READY FOR REVIEW | Spark 3.5.5; 3 fresh JVMs; 1 cache partition; producer 1, receiver 0; receiver decoded without a task and never produced/encoded; exact 1,000 rows/2,000 values; source computed once; 248 hashes. |
| E | READY FOR REVIEW | Spark 3.5.5; 3 independent settings; 44 successful tasks per case; all primary checks; actual LZ4/crypto paths. Combined settings and performance were not tested. |
| B | READY FOR REVIEW | Exact Spark 4.2.0 revision 32f7299601108917fb01920a54e084595b7b3bf8 compiled independently; 188 hashes; partition-keyed statistics code preserved. Runtime PARKED: exact RoaringBitmap 1.6.10 and flatbuffers-java 25.2.10 unavailable; no substitutes. |
