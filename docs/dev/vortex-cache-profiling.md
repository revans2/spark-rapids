# Vortex GPU cache-read profiling

Status: READY FOR REVIEW. Diagnostic follow-up to the
[primary comparison](vortex-cache-results.md), 2026-10-27. No implementation
or baseline CSV changes.

The slow GPU reads expose substantial host-side overhead in the prototype's
Arrow-to-cuDF import path. They do not measure PCIe transfer time or establish
Vortex's intrinsic decoding limit. The previous comparison needs this
qualification before a format decision.

## Reproduction and controls

One Nsight Systems CUDA/NVTX/OS-runtime trace plus JVM Flight Recording reproduced
the anomaly using the original application artifact
SHA256 `78e04232b44e4a0569eb024ec0d911649719c560808c9bc457bceabe1f5f8824`.
The fixed-wide fixture used 256 MiB logical data per task, four partitions,
two task slots, GPU population and GPU reads: 1 GiB logical data overall.
Heap remained 16 GiB, RAPIDS-managed host cap 4 GiB, sampled RSS stop guard
32 GiB. Peak sampled process-tree RSS was 5,254,914,048 bytes; the job passed.

Population took 7.327 seconds. Full reads took 12.936 seconds for warmup and
14.095 seconds for the measured read; ID-only reads took 0.861 and 0.920 seconds.
Read results and GPU scan execution were checked. This profiled diagnostic has
one measured read per projection and does not replace the unprofiled baseline.

Artifacts are under `/data/tmp/cache-profile-vortex-256/`: exact invocation in
`outer-command.json`, runtime inputs/results in `run/`, `trace.nsys-rep`,
`trace.sqlite`, `driver.jfr`, exported CUDA statistics and
`lead-independent-verification.json`. Nsight Systems version:
2026.3.1.157-263138048394v0. Trace options were
`--trace=cuda,nvtx,osrt --sample=none --cpuctxsw=none --wait=all`.
The runner used warmups=1, repeats=1, timeout=300 seconds.
No larger stress run was needed.

## Measured costs

Whole-recording device execution time for host-to-device copies was **0.173
seconds** across 44,446 copies / 2,214,342,176 bytes. Device-to-host copies took
0.088 seconds. These totals include population and reference work as well as
reads. They are GPU activity sums, not end-to-end query latency.

The trace contains 512 `from_arrow` NVTX ranges. Independently querying the
read-only SQLite export reproduced the following same-thread CUDA calls wholly
contained inside those ranges:

| Host API activity | Calls | Summed duration (seconds) |
| --- | ---: | ---: |
| cudaHostAlloc | 7,936 | 14.029 |
| cudaFreeHost | 7,936 | 6.565 |
| cudaMemcpyAsync_ptsz | 32,256 | 13.583 |
| cuMemcpyAsync | 7,936 | 1.872 |
| cudaStreamSynchronize_ptsz | 24,320 | 0.093 |

These durations sum across task threads and both full reads plus narrow reads;
they overlap in wall time. Same-thread nesting between API categories has not
been excluded, so API totals must not be treated as disjoint time either.
They must not be divided by one 14.095-second query
to obtain percentages. Host copy API duration includes time spent in the
driver/API; it is distinct from GPU copy execution time.

256 import ranges each contained 31 pinned-host allocations and together
lasted 39.640 summed thread-seconds. The other 256 had no such allocations and
lasted 0.182 seconds. This strongly implicates nullable-column import overhead:
the full projection has 31 nullable columns while ID is non-null. The trace
does not by itself identify the allocation size or prove the precise native
allocation call site.

Pinned allocation/free accounted for 20.594 summed seconds inside import, and
the two host copy APIs another 15.455 seconds. Explicit stream synchronization
was small in this recording. Recorded GPU kernel and copy execution time does
not account for the observed read latency.

## Code evidence and next experiment

`SparkVortexSerializer.scala` opens and closes a cuDF Arrow IPC reader for each
decoded IPC batch. `VortexIpcCodec.Decoder.next` serializes each decoded Arrow
root as its own complete IPC stream. Runtime cuDF Java bytecode confirms each
provider-backed reader allocates a 10 MiB host staging buffer. This repeats
reader setup, staging and per-column import work on the read path.

The checked-out native cuDF source also recomputes nullable masks' null counts
during host Arrow import. Its revision differs from the runtime native library;
that source is explanatory evidence, not proof of the exact runtime allocation
site. JFR additionally sampled Java IPC buffer copying and the native
Arrow-to-cuDF conversion, but its sample counts are not a native CPU cost
breakdown.

A bounded follow-up should test reusing a reader over a valid continuous IPC
stream and importing larger GPU batches, then profile the same small control.
Simply increasing the requested row count on the current reader cannot combine
separate streams; concatenating complete streams is not a valid substitute for
implementing the streaming protocol. Whether batching amortizes the measured
nullable-column cost remains unverified. A runtime-matched native allocation
stack trace would further identify which allocations need pooling or removal.

The GPU population/read asymmetry is therefore plausible: population uses cuDF
Arrow export followed by CPU Vortex encoding, whereas reading traverses this
expensive Arrow import path. A bridge repair and a small verified comparison
should precede another full scale matrix or a decision against Vortex.

## API investigation: candidate fixes

Read-only source and runtime Java API inspection found a smaller first experiment
than restructuring IPC. The initial API investigation below preceded implementation. Subsequent
[bridge experiments](vortex-cache-bridge-results.md) measured shared pooling and
the int/long direct-buffer route; retain the remaining candidates as proposals.

1. **Test pooling first.** GpuDeviceManager.initializePinnedPoolAndOffHeapLimits
   explicitly configures cuDF's default pinned pool to zero. RAPIDS's
   spark.rapids.memory.pinnedPool.size also defaults to zero. With a nonzero pool,
   initialization can share it with cuDF (setCuioDefault defaults true).
   Rmm/PinnedMemoryPool runtime signatures confirm the necessary APIs exist.
   The mounted native source's null-count device_scalar allocates a one-element
   pinned bounce buffer. This fits the allocation pattern, but native revision
   mismatch still prevents asserting the exact runtime call site. A modest pool
   in one otherwise unchanged small diagnostic is the cheapest test of whether
   raw pin/unpin is the bottleneck. Account for the pool in the existing managed
   host budget; fallback allocations mean a pinned pool is not a hard native
   memory limit. This diagnostic configuration would not meet the out-of-box
   acceptance criterion by itself or authorize changing production defaults.

2. **Direct Java buffer upload.** The actual runtime exposes public
   HostColumnVector constructors accepting data/validity buffers and
   Optional<Long> nullCount, followed by copyToDevice(). It also exposes
   ColumnVector constructors accepting device buffers and a known null count.
   Runtime/source inspection confirms these bypass Arrow IPC and build columns
   directly; the numeric fixture can supply its exact null counts instead of
   entering Arrow import's recount path. Owned buffers need correct validity
   padding, slice offsets and lifetime management; asynchronous copies require
   retaining sources until completion. Strings, booleans, view layouts, decimals
   and nested children require explicit layout/type handling and validation.
   This could provide a prototype-local fix without a native build, but it is
   not a ready general Arrow importer. Public HostMemoryBuffer APIs do not wrap
   arbitrary Arrow addresses, so this Java route still needs owned staging and
   copying; it is not a zero-copy host-to-device path.

3. **ColumnVector.fromArrow.** This existing runtime API accepts ByteBuffers and
   avoids IPC serialization. Its Java documentation limits the supported type
   contract to primitives. Mounted JNI code still invokes cudf::from_arrow, so
   using it alone is not established to remove nullable-column allocation cost.
   Do not infer wider support from undocumented JNI switch cases.

4. **Native Arrow C Data bridge.** C++ exposes from_arrow/from_arrow_host and
   stream import APIs. A JNI bridge could consume decoded Arrow buffers without
   serializing them back into IPC. Those APIs still use Arrow import machinery;
   removing IPC alone does not establish that the pinned-allocation issue goes
   away. The stream API consumes/releases its input stream, unlike the individual
   array APIs; bounded consumption and ownership need explicit design.

Reader reuse and larger bounded imports remain useful independent candidates,
but pooling should be isolated first. If the shared-pool comparison supports
this explanation, the final out-of-box solution needs a design review: manage small pinned
allocations within the existing budget, avoid unnecessary null-count work, or
use direct uploads. Do not silently change the released allocator defaults.
