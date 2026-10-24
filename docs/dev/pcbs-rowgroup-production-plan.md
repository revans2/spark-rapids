# Parquet cache production cleanup

Status: IN PROGRESS. Only the architect marks work DONE.

Scope: retain the production row-group cache patch and contract tests; remove
research tools/reports and consolidate duplicated Parquet functionality.
Use the main checkout; no worktrees. All project operations and worker handoffs
must use MCP docker_exec tools, confirmed at checkpoints.

Compatibility: existing PCBS, Spark caching, StorageLevel and Parquet scan behavior
is RELEASED (present on main and in deployed artifacts). Preserve CPU/GPU reads,
supported types, projection/reordering, row counts, compression and persistence.
Keep ParquetCachedBatch(numRows, Array[Byte]) with actual encoded-byte accounting.
The new row-group implementation and research tooling are UNRELEASED; no adapters
or compatibility scaffolding are needed for them.

Keep the soft 8 MiB uncompressed row-group target, cuDF default row limit and
large GPU read bundles. No Spark changes, Spark OFF_HEAP, new public config or
CPU-writer redesign. Unexpected supported-input regressions or required
architecture changes must be escalated rather than weakening checks.

- Research cleanup: removed 103 branch-added tools and 36 research documents;
  retained this minimal tracker. Prior reports and tooling remain in Git history
  at 91d748957d. Existing untracked user files are untouched.
- Reuse review: IN PROGRESS; inspect existing Parquet utilities/callers before edits.
- Research cleanup verification: READY FOR REVIEW. Reference search found no
  callers outside removed files; staged diff whitespace checks pass. Production
  and test sources are unchanged by this cleanup.
- Refactoring verification: pending focused tests and peer review.

Prior verification (before cleanup/refactoring): 39 OSS compile/package pairs,
seven focused version gates and selected Java/Kryo SQL checks. Databricks builds
were blocked by unavailable vendor dependencies; no vendor-runtime claim.
New code changes require fresh relevant verification; earlier measurements do
not establish performance after refactoring. Raw measurements remain under
/data/pcbs-oom and historical reports remain in Git history.
