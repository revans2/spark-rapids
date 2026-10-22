# Native cache with Spark scheduler tokens: research results

Status: READY FOR REVIEW. Mechanism research, not a production cache implementation.
2026-10-22, issue #16362. Architect acceptance pending.

## Result

The custom-RDD/token mechanism works in the tested local JVMs on unmodified
Spark 3.5.5 / Scala 2.12 and Spark 4.0.1 / Scala 2.13, with AQE disabled and
enabled. A real upstream shuffle distinguishes scheduler visibility from a
simple local cache hit.

The source audit also found changes that prevent treating the private override
as a permanent API: Spark 4.2 changes materialization statistics and an RDD
constructor; pinned development master changes the method used to create the
persisted SQL wrapper. These require shims and explicit regression checks.

This closes the narrow question of whether the proposed interception and
scheduler-token mechanism can execute. It does not establish production
distributed ownership, paired migration/removal, or PCBS performance.

## What was exercised

A research CachedBatchSerializer returns our producer RDD. Its overridden
mapPartitionsInternal returns a custom MapPartitionsRDD that preserves Spark's
supplied statistics function. That actual persisted wrapper:

- Registers persistence and cleaner bookkeeping without calling super.persist.
- Reports the requested storage level through getStorageLevel, while leaving
  RDD's internal storage field NONE so iterator reaches our compute path.
- Stores raw Long payloads in direct native ByteBuffers owned by a local manager.
- Publishes a genuine small SchedulerToken under its actual RDDBlockId.
- Serves cache hits through its own compute; tokens never enter the SQL decoder.
- Computes the parent on a miss, applies Spark's statistics function, and
  publishes native data before its token.
- Removes its tokens and native entries on explicit blocking unpersist.

The fixture is deliberately one Long column, 10,000 rows, four source partitions
and six cached partitions. Initial logical payload size is 80,000 bytes;
each token occupies 56 bytes of Spark MemoryStore accounting in these runs.
Rows are checked individually, and count/sum are checked through SQL.
A separate two-row/six-partition case verifies four empty partitions.

Each run uses a fresh JVM, local[4], 1 GiB heap, no RAPIDS plugin or GPU.
Normal Spark memory fraction, unroll period and object-stream reset are unchanged.
This is not the earlier 1 GiB PCBS workload and is not a performance benchmark.
Native cache retention is proved by direct-buffer source and store assertions,
not by claiming the entire CPU input/decoder path avoids Java heap.

## Acceptance results

Both runtimes passed each row with AQE off and on:

| Check | Observation |
| --- | --- |
| Actual cache owner | SQL's persisted statistics wrapper is our subclass; its real ID is in SparkContext persistence registration. |
| Correct values and initial statistics | All 10,000 Long values, SQL count/sum, 10,000 cached rows and 80,000 logical bytes agree. |
| Scheduler visibility | Every cached partition has a real small token and a regular BlockManager location. |
| Upstream outputs deleted | Registered shuffle data files and map-output tracker entries go from four to zero before the cache read. |
| Tokens and payloads intact | Values remain correct, native hits increase and upstream compute count does not increase. |
| One token and its payload removed | Normal scheduling rebuilds missing upstream outputs and computes the absent cache partition. |
| Token survives, payload and upstream outputs lost | Own compute withdraws the token and tries parent computation. A genuine MetadataFetchFailed causes one task/stage recovery per AQE mode; the final values are correct. |
| Explicit blocking unpersist | Spark locations, persistence registration and local native entries are absent afterward. |
| Empty partitions | Four empty partitions have valid entries/tokens and cleanup succeeds. |

The payload-only loss case is a verified **retry fallback**, not failure-free
recovery. It demonstrates why the manager must coordinate token validity with
payload availability. The scheduler has already used the surviving token to
skip ancestors; our local miss alone cannot make missing shuffle outputs ready.

Root independently reran the fixtures and parsed raw Spark event logs, rather
than accepting worker success summaries. Each audited JVM recorded 142 task-end
events, exactly two FetchFailed task ends corresponding to the two deliberate
payload-only loss cases, and a normal application end. No other task failures
occurred in those accepted reruns.

Initial statistics are verified. Replacement/recomputation statistics are not
claimed correct by these initial assertions; preserve each Spark version's
supplied function and add the relevant production tests.

## Reproduction and provenance

The [standalone fixture and runner](../../tools/pcbs-token-cache-spike/README.md)
compile against the selected installed Spark runtime. Separate tiny adapters
handle the tested CacheManager lookup API difference. They do not replace
Spark classes, modify Spark sources or depend on a rebuilt RAPIDS jar.

Original frozen evidence is under /data/pcbs-oom/token-cache-spike:

| Evidence | Spark | Source SHA-256 | Jar SHA-256 |
| --- | --- | --- | --- |
| Root run-3, frozen build 3 | 3.5.5 | bbd43a7f1f50c5b69da3eeeedecfe49f9e50f554acaedbed9b5f80790a6349ec | c66c624dcc61c80c45a124cb189fe36a2014f76f14c906cda43c44a397d3b036 |
| Root run-5, instrumented build 4 | 3.5.5 | f66bb057aa83cd2af1b65f6b894cb2d70e2707697ded627a30bcab6a557792b1 | 1baf1b8ce6d13d589ec49808ca64759ac9cdd9f2a4ede0df9738b3aabd427d08 |
| Root run-401-2, instrumented build 401-2 | 4.0.1 | a0173ddcf7bebadf247bdaf2c1872a4376f5faa784b20537b158a7292b7f5d72 | 47436d176047ec9fd3ad7dcce8a87aca5ad90b81afd21577420d9cadd3a4e875 |

independent-run-*.json records root event/marker/hash audits. Raw logs, event logs,
compiler commands and failed early attempts remain available. An early generic
InternalRow decoder failed Spark's UnsafeRow requirement; it was corrected with
UnsafeProjection before accepted runs. No acceptance check was removed to obtain
a pass. The portable packaged sources have their own build/run manifests. Root also
compiled and executed the checked-in package in root-portable-355 and
root-portable-401. The [verified summary](../../tools/pcbs-token-cache-spike/verified-results.json)
records final source/runner/jar and selected runtime hashes. Both final JVMs
passed the same 142-task/two-expected-failure event audit, structured diagnostics
and a negative check rejecting a log with its final acceptance marker removed.
55 downloaded official source files were hash-verified; unavailable lookup paths
are recorded rather than treated as inspected source.

## Newer Spark audit

The version audit is source-only except for the separately noted runtime tests
and installed 4.0.1 bytecode inspection. Official tags identify inspected
references, not an Apache maintenance-policy or announcement claim.

| Reference | Exact revision | Evidence and changes |
| --- | --- | --- |
| Mounted 3.5.3 | 32232e9ed33bb16b93ad58cfde8b82e0f07c0970 | Source inspected; not executed in this spike. |
| Mounted 3.5.5 | 7c29c664cdc9321205a98a14858aaf8daaa19db2 | Compiled/executed; AQE off/on. |
| Mounted 4.0.0 | fa33ea000a0bda9e5a3fa1af98e8e85b8cc5e4d4 | Source inspected; not executed. |
| Official 4.0.1 | 29434ea766b0fc3c3bf6eaadb43a8f931133649e | Bytecode inspected and fixture compiled/executed; AQE off/on. |
| Official 4.0.4 | c7d67e3f5d4c9d88a480367b44fc54d26adf99ab | Source inspected; same relevant interception shape. |
| Official 4.1.3 | 77bbf77e86ad48f58b5dfbc6ac882b3e70cf1989 | Source inspected; same relevant interception shape. |
| Official 4.2.0 | 32f7299601108917fb01920a54e084595b7b3bf8 | Source inspected; partition-keyed statistics replace old global fields; MapPartitionsRDD adds preservesPartitionSizes. |
| Development master | 67b62cb4081eda45933e76797a7730596dc50369, 5.0.0-SNAPSHOT | Source inspected; SQL uses mapPartitionsWithIndexInternal, adds fully-consumed statistics and repeatability checks. The current fixture's iterator-only override would miss that owner. |

The private storage field / overridable getter distinction remains in the
inspected sources, but it is not a supported compatibility contract.
Override both relevant factories only through reviewed version-specific
adapters, preserving all flags and the original supplied statistics function.
Do not ship an old compiled MapPartitionsRDD constructor against a changed
binary signature. Fail fast in regression tests if the actual owner is not
our expected subclass.

Pinned official URLs, file hashes and observation times are retained in the
[source manifest](../../tools/pcbs-token-cache-spike/spark-source-manifest.json).
The detailed audit remains at
/data/pcbs-oom/token-cache-version-audit/findings.md.
No newer Spark distribution was installed; 4.0.4/4.1.3/4.2.0/master have not been
compiled or executed. No vendor-runtime compatibility is established.

## Production questions left open

- A real PCBS/GPU integration must preserve original large batches and exercise
  native host/disk spill, including existing limits on/off and concurrent restores.
- Multi-executor native reads, token/payload replication and decommission need
  actual paired availability and failure tests. Local static maps prove none
  of these.
- Automatic eviction/removal notifications, cleaner-driven cleanup, late puts,
  superseded versions and repeated cache churn need bounded, generation-aware
  lifecycle handling.
- The fixture has no production reader leases or native memory budget and
  supports only one test type and a deliberately simplified storage policy.
  Do not infer StorageLevel compliance from its requested-level getter.
- Failure-free recovery needs a dependency-readiness solution when the scheduler
  has already pruned missing ancestors. Genuine stage retry remains the tested
  fallback.
- Publication/refcounts, cancellation/speculation and active-read cleanup need
  concurrency tests before deployment.

No production source, Spark source, public RAPIDS configuration, dependency,
batch size or existing GPU artifact changed. No external issue, message or PR
was posted. The [upstream storage API proposal](pcbs-cache-storage-api-proposal.md)
describes a proposed long-term replacement for the private adapter.
