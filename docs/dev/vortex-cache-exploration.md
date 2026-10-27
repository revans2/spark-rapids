# Vortex cache exploration

Status: initial source investigation READY FOR REVIEW; performance and integration PARKED
on missing build prerequisites. Only the architect marks work DONE.
Date: 2026-10-27. AI-assisted investigation; no production implementation changes.

## Scope and operating rules

Explore a CPU Arrow / Vortex alternative to ParquetCachedBatchSerializer (PCBS),
including GPU transfer costs and default-Spark heap behavior. This is not approval
to replace PCBS, introduce user-facing configuration, or change supported contracts.
Use MCP docker_exec tools for every project operation, worker handoff, resumption,
and progress checkpoint. Workers and lead confirmed exclusive MCP sandbox use.
Re-read this tracker when resuming. Commit completed tasks with tracker evidence.

Acceptance for the exploration: establish feasible APIs, then measure equivalent
round trips and real Spark cache workloads against this branch's PCBS. Report speed,
encoded size, JVM heap, native host memory, GPU memory, and correctness separately.
A codec-only benchmark cannot establish Spark cache performance or OOM resilience.
Park for unavailable dependencies, contradictory release evidence, or a proposed
change to architecture, scope, policy, or acceptance criteria.

## Release classification

RELEASED: PCBS's CPU/GPU paths, public serializer configuration, supported types,
projection/reordering, row counts, compression and Spark storage-level behavior.
Evidence: serializer is present on local main d587232490 and documented in
[cache-serializer.md](../additional-functionality/cache-serializer.md), including
availability since plugin 0.4+. Preserve these contracts.

UNRELEASED: branch-added small-record framing, splitting/bundling, internal 8 MiB
record target and lazy CPU writer. Evidence: recovered tracker from
`git show a9a66345cd^:docs/dev/pcbs-rowgroup-production-plan.md`; tracker was removed
in a9a66345cd. Branch-only target commit aa1e5c6fb3 appears in `main..HEAD` history.
No compatibility scaffolding is required for those additions. New Vortex cache
integration is exploratory and unimplemented; it has no released plugin contract.
This classification does not make existing Vortex or Arrow library APIs disposable.

## Revisions and inspected evidence

Plugin baseline: 2cbdb23451300b5d0aca9d3a4f5aa5232f2634ff,
branch pcbs-rowgroup-production. Existing untracked research artifacts were left alone.
Vortex: /data/vortex, clean develop a5f574e69bbf9010029f0e62ca59d56cd0c4aa6e
(`0.90.0-92-ga5f574e69`). Vortex is not covered by code-index; indexed plugin/Spark
symbols were discovered with code-index before reading source with MCP.

Lead independently read decisive writer/reader and native code after worker discovery:

| Source | Evidence |
| --- | --- |
| Plugin `parquet/ParquetCachedBatchSerializer.scala:119` | Spillable host output; each emitted cached record owns a Java byte array. |
| Same file `:319`, `:426` | GPU slice encoding and row-group writer; input slicing and cache-record framing are distinct. |
| `RapidsConfSqlEntries.scala:376` | Internal 8 MiB uncompressed record target, not a hard encoded-size bound. |
| Spark 3.5.5 `storage/memory/MemoryStore.scala:192` | Iterator values are allocated/stored before periodic storage-memory checks. |
| Spark `internal/config/package.scala:1394` | Default check period is 16 elements. Smaller records reduce potential allocation between checks; they do not reserve encoder memory. |
| cuDF `java/src/main/java/ai/rapids/cudf/Table.java:1930` | Public chunked Arrow IPC writer and done-on-GPU callback. |
| cuDF `java/src/main/native/src/TableJni.cpp:2814` | Whole-table `to_arrow_host` precedes IPC chunking. |
| Same native file `:459` | Reader accumulates complete record batches until a minimum row target. |
| Plugin `execution/python/GpuArrowWriter.scala:35` | Existing 128 KiB heap-copy adapter and schema/semaphore handling are reuse references. |
| Vortex `java/vortex-jni/src/main/java/dev/vortex/api/VortexWriter.java` | Arrow C Data write, byte sink, explicit finish, bytes-written and layout-buffered-byte metrics. |
| Vortex `vortex-jni/src/writer.rs:80` | Queue bound is four batches, not bytes. |
| Vortex `vortex-jni/src/io/write.rs:35` | Native output becomes fresh Java byte arrays, up to 1 GiB per upcall. |
| Vortex `java/vortex-jni/src/main/java/dev/vortex/api/Partition.java:49` | Arrow C stream read; returned reader owns partition resources. |
| Vortex `vortex-jni/src/arrow_compat.rs` | Nonzero-offset Arrow arrays may be copied before Java import. |
| Vortex `vortex-file/src/strategy.rs:70` | Default 8192-row blocks and per-column 1 MiB target are layout choices, not task memory limits. |

cuDF paths above are relative to
/home/roberte/src/spark-rapids-jni/thirdparty/cudf. The installed
cudf-spark-jni 26.12.0-SNAPSHOT reports build revision 6740b5ee74cd1c0d35c42422fb4cf0d8cfa322eb;
its public Arrow APIs were checked with javap by the discovery worker. Mounted JNI
and vendored cuDF source revisions differ from runtime provenance. Native source
observations therefore require verification against the runtime used in experiments.
Historical PCBS tracker test/benchmark files under /data/pcbs-oom are not mounted;
no old timings have been independently reproduced or used as performance results.

## Findings and proposed experiment

Feasible in principle: GPU table -> cuDF CPU Arrow IPC -> Arrow Java record batch
-> Vortex C Data writer -> encoded cache bytes. Reverse: Vortex Arrow reader ->
Arrow IPC -> cuDF chunked reader -> GPU table. This route adapts existing APIs;
it adds IPC work rather than providing a direct cuDF-to-Vortex C Data handoff.
A direct native C Data bridge could remove IPC overhead but would be a separate
architectural decision after validating and measuring this proposed bridge.

Vortex may improve compression and CPU scanning, but no measured claim is available.
PCBS already encodes/decodes on GPU and transfers compressed bytes. The proposed
Vortex path transfers uncompressed Arrow data across PCIe in both directions.
Whether compression or CPU decode wins outweigh that cost must be measured per workload.

Memory strategy must precede the prototype: split GPU input by a byte estimate
before Arrow export; process a bounded unit rather than retaining a partition's
outputs; bound the encoded record sink, in-flight Arrow buffers and decode output.
`withMaxChunkSize` limits IPC rows after whole-table host conversion, so it cannot
provide that bound. Reader row targets are also not byte limits. Small slices may
retain larger backing Arrow allocations; observe retained memory as well as sizes.
A single oversized string/nested row needs explicit handling. Avoid an unbounded
ByteArrayOutputStream. Native allocations outside JVM heap still need a budget and
cancellation cleanup; Java allocator statistics alone do not count all Rust memory.

Recommended experiment order, pending architect review:

1. Build the exact Vortex JNI revision and run its existing Arrow write/read tests.
   Stage an optimized release native library for performance runs: Gradle's default
   buildJniLibrary task invokes cargo build without --release and stages target/debug.
   Confirm the loaded library rather than benchmarking that debug artifact by accident.
   Verify Java 17 / Arrow 19 dependency coexistence with Spark 3.5.5 (Spark's older
   Arrow classes, relocation and C Data JNI loading are a compatibility risk).
2. Implement an isolated round-trip harness, adapting the plugin Arrow adapters
   and Vortex JNIWriterTest. Compare bounded 1, 4, 8 and 32 MiB input units, plus
   large parent GPU batches, against the current PCBS APIs. These are experimental
   harness parameters, not proposed plugin configurations.
3. Cover fixed-width, random and low-cardinality strings, skewed/oversized rows,
   nulls, decimals, timestamps and nested values. Preserve logical schema; verify
   Arrow view types, decimal scales, map/nullability and empty/zero-column cases.
   Use deterministic data and compare every value, order and row count.
4. Time export, IPC, encode, decode and GPU import separately, then complete
   round trips. Include warmup, repeated samples, encoded sizes and peak-memory
   measurements. Reuse identical data; include full and narrow projections. Record
   CPU threads and Vortex workers; synchronize GPU work at timing boundaries.
5. Only after the bridge passes, prototype a separate cache serializer and compare
   materialization and repeated scans against branch PCBS and stock Spark cache.
   Explicitly request ordered Vortex scans where the cache contract requires it.
   Exercise many large batches per task, concurrent tasks, default Spark storage
   settings, MEMORY_AND_DISK spills, constrained heap and early cancellation.
   Do not alter memory-check settings to obtain a pass. Record GC, heap/RSS/native
   peaks, GPU peaks, spill bytes and throughput. No claim of universally OOM-free
   execution: establish the tested resource envelope and oversized-row behavior.

## Environment request and remaining work

Available: Java 17, RAPIDS Maven wrapper, Spark 3.5.5 runtime/reference, visible
RTX A6000 (49,140 MiB), PyArrow 19.0.1. No additional repository mapping is needed.
Absent from PATH: cargo/rustc/uv; no built Vortex jar/native library was found in
/data/vortex or the local dev/vortex Maven cache. Python vortex-data and cuDF are
also absent, but are not required for the proposed JVM comparison.

Preferred addition to the sandbox image:

- Rust/Cargo 1.99.1, pinned by /data/vortex/rust-toolchain.toml, with rust-src,
  rustfmt and clippy; dependency registry/git access or populated Cargo caches.
- Gradle 9.5.1 wrapper distribution/cache; Maven Central and Gradle plugin
  dependencies reachable or cached.
- Amazon Corretto JDK 17, matching both the Gradle daemon criteria and Java build
  toolchain vendor, or its cached toolchain distribution. Existing generic Java
  17 alone has not been verified to satisfy those vendor constraints.
- Writable build/cache locations. Confirm /data/vortex remains writable for builds.
  If downloads are restricted, prepopulate the caches rather than changing pins.

Alternative: supply matching Java/JNI jar + Linux x86_64 native library and Arrow
19 dependencies for the exact checkout. Do not silently substitute published 0.90
for newer source APIs. No toolchain installation or long build was attempted.

Focused worker review and lead source inspection completed through MCP tools.
Review corrected the writer metrics description: they do not establish a bound on
all queued/native memory. Lead also identified Gradle's default debug-library
staging and added the release-build verification prerequisite.

Source investigation: READY FOR REVIEW. Runtime round-trip correctness, performance,
heap-pressure testing and dependency coexistence: UNVERIFIED, parked on environment.
No implementation tests were run because no executable prototype or runtime changes
were made. Shim compatibility preflight is not triggered by this documentation-only
investigation; it becomes mandatory if the prototype changes shared shim callers,
versioned sources or build profiles. No PR was opened or updated.
