# PCBS row-group storage and combined GPU decode prototype

Status: IN PROGRESS — research/prototype requested by the architect on 2026-10-23.
Production implementation and final acceptance are not included.

The user redirected work from the controlled sweep to storing smaller Parquet
pieces while retaining large GPU input and decode batches. Matrix2 is stopped
at 24 completed cases (18 PASS, six expected heap OOM); case024 was interrupted
and is excluded from timing comparisons. Preserve its raw evidence and the
original schedule, sources, artifacts and completed results. Do not resume the
remaining sweep or profiles as part of this spike.

## Scope and boundaries

All project operations, including worker operations, MUST use MCP docker_exec.
Confirm this at handoffs and progress checkpoints. Root coordinates, reads the
decisive code, independently verifies worker results and commits completed work
with tracker updates. Mark verified work READY FOR REVIEW; architect acceptance
is required for DONE.

Existing Spark cache/StorageLevel and PCBS behavior is RELEASED (on main and
shipped). Preserve its defaults and supported contracts. Research overlays,
fixtures and prototype formats are UNRELEASED and need no compatibility adapter.
Use isolated generated artifacts; do not modify installed Spark/cuDF or the
five frozen controlled-benchmark sources. No new public configuration, Spark
OFF_HEAP storage, custom BlockManager, native ownership protocol, or production
Spark/plugin changes. The user explicitly authorized a thin Java/JNI exposure of
existing cuDF multi-source reads. Core writer/decoder algorithm changes and Spark
changes remain park/escalation conditions; the wrapper does not authorize them.

## Proposed proof

1. Encode the existing large GPU batch once with the current Parquet writer.
   Use existing row-group row/byte limits to make groups smaller; leave the
   global producer target at 1 GiB. Writer callbacks are buffer/column chunks,
   so derive row-group boundaries from the footer, not callback boundaries.
2. Intercept the writer's host buffers before PCBS concatenates them into one
   large heap array. Copy encoded column chunks into independent small Parquet
   records, with schema, row counts, dictionary/data offsets and footer corrected.
   Reuse/adapt the existing scan footer and offset helpers; do not decode and
   re-encode the values on the CPU or split GPU input batches into small writes.
3. Accumulate cached records for approximately a 1 GiB GPU decode. Check newer
   cuDF APIs for reading multiple independent files in one call, including Java
   exposure and installed-version availability. Current HostMemoryBuffer varargs
   concatenate one logical file. If necessary, use the scan's existing logical
   file reconstruction algorithm and off-heap input buffers instead.
4. Compare ordinary Java cache serialization (default reset100/unroll16) with
   the research codec bypassing Java payload serialization and handle retention.
   Hold group layout, data, native/plugin artifacts, GPU batches and unroll16
   fixed. Java uses stock SerializerManager; codec uses the existing isolated
   research overlay. Both keep stock BlockManager/MemoryStore. Measure actual
   stored-record sizes, encode/decode calls, input/output GPU sizes, cache fill/read
   times, peak heap and spill/residency. Small row groups are row/byte targets,
   not a universal hard encoded-size bound or a passing criterion.

Initial private writer targets: 65,536 rows and 8 MiB uncompressed row groups,
with the global producer and decode targets unchanged at 1 GiB. This deliberately
allows headroom for both the 16-record unroll interval and Java100 handle retention.
Record actual size distributions and outliers; do not tune the data to fit.
Use a coarse 256 MiB encoded-record safety guard for this experiment, separately
from the soft 8 MiB writer target. An indivisible group exceeding it is a reported
unsupported input, not a reason to change the data or silently split values.
Bundle by estimated decoded/device bytes, with encoded/native admission limits,
and copy each record incrementally into RAPIDS-managed off-heap staging. Retain
small metadata rather than a bundle of heap payload arrays; Java stream and Spark
cache references remain separate sources of retention.

The selected research binding is
`ai.rapids.cudf.ParquetMultiFileReader.read(ParquetOptions, HostMemoryBuffer[])`:
each input buffer is one complete independent Parquet file, inputs remain caller
owned and the returned table owns decoded columns. The C++ multi-source reader
already exists. The helper must share cuDF's package/classloader and reuse the
actual benchmark native library, with no replacement Table.class or second cuDF.
The current benchmark cuDF revision is 69866c121b4cf2d48b6ef631025292547cde528f;
existing local 25.12/26.08 headers are not assumed compatible. Exact cuDF headers
are pinned. Where the runtime lacks RMM/CCCL commit locks, as-of-build dependency
headers are explicitly inferred and pinned, with strict symbol/ABI and runtime
resource-accounting gates. This is a research assumption, not an exact dependency
provenance or production compatibility claim. Park unresolved ABI failures;
report cost before a full matched cuDF rebuild.

No broad size sweep or heavy downstream GPU query is authorized here.

## Verification gates

- Inspect actual output footers and confirm row-group boundaries, sizes and rows.
- Small correctness checks must cover multiple groups, nullable/dictionary string
  data, column projection and both existing compression modes where feasible.
  Verify independent records and combined decode against the original values.
  Measure encoded/decoded ratios against larger row groups with identical values
  and compression mode, including dictionary strings. The canonical random-double
  workload alone does not establish string compression behavior.
- Verify resource ownership on successful reads, failure and partial consumption;
  use existing ARM/task-cleanup patterns and bounded RAPIDS host/spill accounting.
- After small gates, run the same canonical 96 GiB, 16-partition, local[8], 48 GiB
  heap reproduction, global 1 GiB target, GPU pool 15 GiB/concurrency2, RAPIDS host
  limit40 GiB/pinned2 GiB, memory fraction0.6 unchanged. Require exact rows and all
  16 sums, stock BlockManager/MemoryStore in both arms (stock SerializerManager
  for Java and the research overlay for codec), default memory checks, no recomputation/task
  retries, actual small cache records and large combined GPU decode batches.
- A successful prototype is proof for these inputs, not production readiness or
  a universal heap bound. Do not hide oversize groups, make records artificially
  compressible, reduce total data, or weaken checks to obtain a pass.

## Evidence so far

The existing Java writer exposes row-group row/byte limits; defaults are one
million rows and 128 MiB uncompressed data. PCBS currently leaves them unchanged.
The scan reconstructs Parquet files by copying encoded chunks, adjusting column
metadata and writing a footer. Root has read the consumer, writer, decode and
scan reconstruction code. Workers are auditing newer true multi-file APIs and
installed runtime support before selecting the decode route.

Research evidence is under /data/pcbs-oom/controlled-cache-comparison/:
parquet-rowgroup-fragment-discovery.md, unroll-policy-audit/, and the forthcoming
multi-file API audit. The stopped benchmark remains under matrix2/ with its
stop record matrix2-stop-for-rowgroup-prototype.json.
