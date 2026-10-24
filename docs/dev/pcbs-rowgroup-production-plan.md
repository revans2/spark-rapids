# Parquet cache production tracker

Status: READY FOR REVIEW. Only the architect marks work DONE.

Existing PCBS, Spark caching, StorageLevel and Parquet scan behavior is RELEASED
(present on main and in deployed artifacts). Preserve CPU/GPU reads, supported
types, projection/reordering, row counts, compression and persistence. Keep
ParquetCachedBatch(numRows, Array[Byte]) and actual encoded-byte accounting.
The row-group implementation and research tooling are UNRELEASED and need no
compatibility adapters.

Scope: GPU-written row-group records and combined GPU reads, with existing CPU
reader/writer compatibility. Retain the soft 8 MiB uncompressed target and cuDF
row limit. No Spark changes, Spark OFF_HEAP, public config or CPU-writer redesign.
Escalate supported-input regressions or architecture changes; never weaken checks.
Use MCP docker_exec for every project operation and worker checkpoint. Work in
the main checkout; no new worktrees.

- Removed 103 research tools and 36 research documents; retained this tracker.
  Reports/tooling remain in Git history at 91d748957d; user files are untouched.
- Shared scan/cache chunk-reader replay and cleanup, Parquet footer serialization,
  seekable-stream handling and columnar iterator task cleanup. Production code
  is 40 lines smaller. Retained distinct dictionary/data offsets and ownership.
- Added stream-boundary and shared-reader replay regression tests; removed one
  redundant integration case. Independent cross-reviews found no blocking issues.

Fresh verification: normal Spark 3.5.5 verify passed 55 Scala and nine Java tests;
27 default-serializer integration cases passed (eight cache, nineteen scan), plus
eight cache cases with Kryo and GpuKryoRegistrator. No failures or skips.
Spark 4.0.1 / Scala 2.13 SQL-plugin package passed; no 4.0.1 runtime claim.
Checks remained enabled. The main checkout's license check found 22 unrelated
untracked files, so validation used a matching tracked-source export, not a Git
worktree. Evidence: /data/pcbs-oom/production-cleanup-verify.

Earlier 39 OSS compile/package pairs predate this refactor; they are not fresh
coverage. Databricks dependencies/runtimes remain unavailable. Performance was
not retimed during cleanup; historical measurements remain in Git and /data.
