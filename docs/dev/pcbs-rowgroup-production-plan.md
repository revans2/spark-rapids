# Parquet cache production tracker

Status: IN PROGRESS. Only the architect marks work DONE.
All project operations, worker handoffs and checkpoints use MCP docker_exec.
Work in the main checkout on pcbs-rowgroup-production; no new worktrees.

Human decisions (2026-10-24): both CPU and GPU writers are in scope, since CPU
records can still approach the large GPU input budget. Add an internal,
undocumented cache-record byte target (default 8 MiB) so tests can use small data.
Remove the specified storage/compression matrix, partial-unpersist and filter
checks; retain focused size, type and CPU/GPU read coverage.
These decisions supersede the former CPU-writer/config exclusions.

RELEASED: PCBS CPU/GPU read/write behavior, supported types, projection/reordering,
row counts, compression, storage levels, ParquetCachedBatch(numRows, Array[Byte])
and sizeInBytes, and ordinary Parquet scan behavior (present on main/deployments).
UNRELEASED: row-group splitting, read bundling, the internal conf and branch-added
tests; change directly without compatibility shims. No Spark changes or Spark
OFF_HEAP storage. Escalate unavoidable released-contract changes.

1. Shared internal target: IMPLEMENTED; runtime verification pending. Internal
   spark.rapids.sql.cache.serializer.rowGroupSizeBytes defaults to 8 MiB (min 1 KiB).
   Both GPU entry paths capture the query conf and pass the target explicitly;
   large GPU input budgets and cuDF's default row limit are preserved. Existing
   internal helper callers were updated together; no compatibility overloads.
   Independent diff/caller review and git diff --check pass.
2. CPU cap: IMPLEMENTED; runtime verification pending. The existing loop now uses
   min(existing budget, target), Long accounting and first-row admission, avoiding
   the former oversized-row stall. Initial output allocation is capped too, rather
   than allocating 32 MiB for every small record. Independent source review and
   git diff --check pass. UnsafeRows use actual row bytes; generic rows use schema
   estimates, so neither CPU nor GPU targets guarantee a hard encoded-size bound.
3. Focused tests: PENDING. Real GPU and CPU writes must produce multiple records
   with explicit size bounds and exact data. One ~20k-row GPU Python case retains
   nested/binary/decimal/timestamp/null/projection/reordering and CPU/GPU reads.
   Reuse the existing converted-row CPU test if it exercises the cap. Remove the
   storage matrix, partial-unpersist, filter checks/allowances and unused narrow shape.

Acceptance: builds for the Maven default (330), another 3.3.x and a 4.x target;
ParquetCachedBatch*Suite, ParquetBufferConsumerSuite, CachedGpuBatchIteratorSuite,
CachedBatchWriterSuite; full cache_test.py with PCBS and default parallel settings,
recording the new test duration. No weakened assertions. Record each rule's case,
commands, results and independent review; mark completed tasks READY FOR REVIEW.
Park for non-fixture CPU regressions, unaffordable GPU fragments, unavoidable
released behavior changes or an unresolved contradiction.

Prior cleanup removed 103 research tools/36 documents and shared reader lifecycle,
footer/stream/task cleanup. Its verification predates this follow-up. Historical
reports remain in Git; raw evidence is under /data/pcbs-oom. Databricks dependencies
and runtimes remain unavailable; no vendor validation claim.
