# Vortex cache exploration

Status: agreed measurement plan ACTIVE. Initial source investigation and GPU codec
bridge READY FOR REVIEW; real PCBS/default-Spark baselines and CPU/GPU matrix remain
in progress. Only the architect marks work DONE. Updated 2026-10-27.
AI-assisted work; no production implementation changes or PR publication.

## Scope, operating rules and accepted decision criteria

Explore Vortex through host Arrow as an alternative cache representation to branch
ParquetCachedBatchSerializer (PCBS) and Spark's DefaultCachedBatchSerializer.
The user approved the qualified plan below. Do not replace PCBS, add production
configuration, change supported contracts or silently change acceptance criteria.

Use MCP docker_exec for EVERY project operation, worker handoff, resumption and
progress checkpoint. Require workers to confirm continued MCP use. Re-read this
committed tracker after context resets. Independently inspect delegated code and
reproduce decisive checks. Commit each completed task with tracker evidence; mark
verified work READY FOR REVIEW. Keep benchmark execution serial on the GPU.
Park material architecture/scope conflicts, missing prerequisites after reasonable
investigation, or evidence contradicting the release classification. Ordinary build
and implementation errors should be corrected autonomously.

The decision is whether an option gives a useful, reliable Spark cache under default
settings. Compare these dimensions separately:

| Dimension | Required evidence |
| --- | --- |
| Execution mode | CPU and GPU population/consumption; CPU execution explicitly configured off/on as appropriate; unsupported-data fallback; CPU/GPU consumers of the same cache. |
| Formats | Spark default, actual branch PCBS, Vortex prototype. Unsupported combinations are explicit gaps. A cuDF Parquet proxy is not PCBS. |
| Population | End-to-end cache materialization including transfers, Arrow conversion, compression and Spark storage. |
| Reuse | Full and narrow projections, repeated reads and reuse-count tradeoffs; CPU and GPU consumers. |
| Stored representation | Rows/logical input represented, encoded bytes, bytes per row and compression against a consistently defined logical-input size. Report Spark estimates separately. |
| Retained capacity | Actual resident cached bytes, logical rows/bytes resident, spilled bytes and recomputation where applicable. More resident useful data can explain higher memory use. |
| Working memory | Temporary encoder/decoder/transfer buffers and overlapping inputs/outputs, distinct from retained cache. |
| Process footprint | JVM heap, native host/RSS and GPU memory, GC and failures; allocator counters alone do not cover all native allocations. |

Two experiments are required: (1) fixed identical logical data/schema/partitions to
compare time, output size and temporary memory; (2) fixed identical resource budget
and concurrency to compare logical data retained, spilling and query performance.
Default Spark storage/unroll settings are the starting point; do not tune around
failures to obtain passing results. Pressure experiments vary data volume and task
concurrency with budgets recorded. Report the tested envelope, not universal OOM
resilience. Distinguish MEMORY_AND_DISK from other supported storage-level cases.

Begin with representative numeric, low-cardinality string, varying longer string,
wide/nested and skewed/oversized-row inputs including nulls; extend where sensitivity
appears. Validate decimals, timestamps, empty/zero-column cases and fallback types
before claiming compatibility. Use deterministic fixtures, check values/order/rows,
and record physical plans for execution/fallback claims. Warmups and repeated samples
must be separate from correctness and data generation.

Fairness: PCBS receives a large parent GPU batch and encodes once before framing
small records. Vortex must receive the same logical parent through bounded export.
Small stored records and large GPU processing batches are separate dimensions.
The initial per-small-unit codec proxy cannot answer this fairness question.

## Release and compatibility classification

RELEASED: PCBS CPU/GPU paths, public serializer configuration, supported types,
projection/reordering, row counts, compression and Spark storage-level behavior.
Evidence: present on local main d587232490 and documented since plugin 0.4+ in
[cache-serializer.md](../additional-functionality/cache-serializer.md). Preserve these.
Spark's default cache contracts also remain unchanged.

UNRELEASED: branch-added small-record framing, splitting/bundling, internal 8 MiB
record target and lazy CPU writer. Evidence: recovered tracker from
`git show a9a66345cd^:docs/dev/pcbs-rowgroup-production-plan.md`; removal commit
a9a66345cd; branch-only target commit aa1e5c6fb3 in main..HEAD. No compatibility
scaffolding is required for these additions. Vortex plugin integration is new and
experimental, with no released plugin contract. This does not permit breaking
existing Vortex/Arrow library contracts.

## Revisions and decisive source findings

Plugin implementation baseline: 2cbdb23451300b5d0aca9d3a4f5aa5232f2634ff on
pcbs-rowgroup-production. Initial tracker commit: efc54fa28f. Preserve unrelated
untracked work. Vortex /data/vortex: a5f574e69bbf9010029f0e62ca59d56cd0c4aa6e,
0.90.0-92-ga5f574e69, upstream checkout clean. Indexed plugin/Spark discovery used
code-index; Vortex is unindexed. All file reads/builds used MCP docker_exec.

Lead independently read decisive code following worker discovery:

- PCBS serializer: spillable host regions and lazy record materialization; each
  retained CachedBatch owns a Java byte array. GPU writer row-group framing and
  input slicing are distinct. Internal record target is 8 MiB, not a hard bound.
- Spark 3.5.5 MemoryStore.putIterator allocates/stores iterator values before
  periodic memory checks; default period is 16 elements. Smaller records reduce
  allocation between checks but do not reserve encoder working memory.
- cuDF Table.java ArrowIPCTableWriter and native TableJni.cpp:2814 export the entire
  GPU input through to_arrow_host before IPC row chunking. Reader row targets are
  minima over complete record batches. Split input BEFORE export for byte budgets.
- Existing plugin GpuArrowWriter/GpuArrowReader provide reusable adapters and
  semaphore/ownership patterns. A direct native C Data bridge is a separate design
  option; the current experiment uses public IPC APIs and includes their costs.
- Vortex VortexWriter accepts Arrow C Data; Partition.scanArrow returns an owned
  reader. The queue bounds four batches, not bytes. NativeWritable creates fresh
  Java byte arrays; its JNI upcalls may reach 1 GiB. Layout defaults of 8192 rows
  and 1 MiB per-column targets are not executor memory budgets.
- Vortex JavaWriter schema exports are borrowed by native create/createStream.
  ArrowSchema.close frees the struct, whereas release frees exported contents.
  A minimal overlay-only fix is preserved in the harness patch. Per-batch exports
  likewise release borrowed schemas; native code consumes ArrowArray ownership.
- Actual runtime accepts Vortex Utf8View through IPC; per-row normalization was
  tested and removed as unnecessary. Arrow19/Spark older-Arrow coexistence remains
  to verify. Cleaner-only Vortex DataSource/Scan/Session lifetimes remain a risk
  to measure separately from Arrow Java allocator zero.

Mounted cuDF source and runtime provenance differ. Installed cudf-spark-jni
26.12.0-SNAPSHOT reports revision 6740b5ee74cd1c0d35c42422fb4cf0d8cfa322eb; its
cudf-java-version-info reports b9539f2b7cf98d0d6229a3fb46cec7f800331158. Public APIs
and actual round trips were checked against the installed jar. Historical PCBS
results under /data/pcbs-oom are not mounted and were not reproduced.

## Completed feasibility batch: READY FOR REVIEW

Standalone harness: [tools/cache-exploration](../../tools/cache-exploration/README.md).
No production source/build changes. Three deterministic fixtures validate every
returned GPU value/null/order and row count. Capped byte sinks are per-sink limits,
not total heap/native bounds; array growth can retain old+new buffers. Input is
allocated before its byte-size check. Large-parent splitting remains unimplemented.

Environment now works without Corretto: Gradle9.5.1 wrapper downloads via sandbox
proxy; Java classes compile with existing Red Hat JDK17 in an isolated build copy.
Removed only Amazon vendor selection/daemon criterion there. Plain cargo still
tries unwritable /opt/rustup/tmp; explicit rustup run1.99.1 works. Native release build:

```text
env LIBCLANG_PATH=/home/roberte/miniconda3/envs/spark-accel/lib \
  rustup run 1.99.1 cargo build --release --locked -p vortex-jni -j 12
```

Release library: /data/vortex/target/release/libvortex_jni.so; SHA256
`eaf2ebef071e5dec7c8067f76091a4cf466dc490b2683ff4928ebec5b741ccee`.
ldd resolved dependencies. Java overlay: /data/tmp/vortex-java-build; runtime CP:
/data/tmp/vortex-java-runtime-classpath.txt. Builder schema repair is applied there;
upstream source stays clean. Stage library in overlay src/main/resources/native/
linux-amd64, not only build/resources (Gradle can remove the latter).

Lead ran JNIWriterTest + NativeIOBridgeTest: 14 passed, 1 upstream disabled variant
case, no failures. XML: overlay vortex-jni/build/test-results/test. Initial missing
resource failure was corrected by source-resource staging. Initial standalone fixed
run leaked 4060 Arrow bytes over seven Vortex iterations; the ownership repair fixed
this without weakening the final allocator-zero assertion. Worker smoke validated
all three fixtures at 1024 rows. Focused worker source reviews and lead inspection
covered ownership, timing sums, caps and scope limitations.

Lead's exploratory serial sweep: each of three fixtures at 65536 and 524288 rows,
3 warmups + 7 measured iterations per codec, alternating order, 512 MiB JVM heap.
All six invocations passed every value and allocator-zero check. Raw CSVs/commands/
stderr and summary: /data/tmp/cache-bridge-results; runner:
/data/tmp/run-cache-bridge-sweep.py. Median full GPU round-trip milliseconds:

| Rows | Fixture | Vortex IPC bridge | cuDF Parquet proxy |
| ---: | --- | ---: | ---: |
| 65536 | fixed | 8.30 | 22.31 |
| 65536 | strings-low | 19.83 | 24.07 |
| 65536 | strings-wide | 39.56 | 44.02 |
| 524288 | fixed | 36.40 | 27.27 |
| 524288 | strings-low | 88.20 | 28.95 |
| 524288 | strings-wide | 216.69 | 55.00 |

Vortex output was smaller in every case. These are independently finalized bounded
units and a cuDF codec proxy, NOT real PCBS/default Spark/CPU/capacity evidence.
Do not choose a cache format or infer heap resilience from these measurements.

## Active batches, prerequisites and remaining verification

1. Commit/review qualified measurement plan and initial harness evidence together.
2. Establish fresh actual branch PCBS and stock Spark baselines with CPU/GPU paths;
   verify exact artifact provenance before claims. Existing jars report e9666723d6,
   while SpillFramework changed later. First Maven attempt failed in7.89s because
   Zinc wanted unwritable /home/roberte/.sbt/1.0/zinc/org.scala-sbt; log
   /data/tmp/vortex-pcbs-build.log. Resolve using an external writable compiler cache.
3. Extend experimental bridge CPU/GPU measurement and introduce a Spark-level
   Vortex prototype without production contracts. Inspect classloading/dependency
   constraints before selecting the implementation. Escalate real design conflicts.
4. Add output/resident/working/process memory accounting and fixed-data then
   fixed-budget experiments; only run meaningful matrices once correctness passes.
5. Complete compatibility/fallback, projection/reuse, concurrency/pressure and
   resource cleanup coverage. Report unsupported and unverified cells explicitly.

Runtime pressure, CPU paths, actual PCBS/Spark default comparisons, Spark classloading,
projection and full type coverage remain UNVERIFIED. No current blanket conclusion.
Shim preflight becomes mandatory if changes touch versioned sources, shared shim
callers or build profiles; this isolated tools/documentation batch changes none.
