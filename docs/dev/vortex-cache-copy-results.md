# Bounded Vortex GPU IPC copy experiment

Status: READY FOR REVIEW. Retained in the exploratory prototype; no production
serializer, native library, allocator default or cache encoding changed.
This is one follow-up to the completed [2 GiB pool matrix](vortex-cache-pinned2g-results.md).

The larger control showed a full GPU read reduction from 9.127 to 6.937 seconds
(about 24%). The small controls were mixed: the first was slower, while a fresh
repeat was faster. This supports a scoped experimental improvement, not a
universal speedup or a stable latency distribution.

## What changed and why

Pooled JFR still sampled growing-array copies in the GPU decoder IPC output;
the old Sink also copied the complete stream when returning its exact byte array.
A capacity hint alone would leave the final full-stream copy and uncertain
transient allocation size. The GPU route now writes one valid IPC stream into
owned 64 KiB chunks with the same 64 MiB total cap and feeds cuDF across them.
There is no flattening or concatenation of independent streams. The original
Sink remains for encoding and CPU reads. Cache records stay at the same size.
Chunks are retained across retries, with a fresh provider cursor per attempt,
then references clear after completion/failure. Pending task cleanup closes the
decoder and drops payload references. No new user-facing configuration was added.

Multi-file grouping was investigated and not implemented: exact dtype matching
and concurrent footer lifetimes add risk, and every file still opens its footer.
No chunk-size tuning, alternate encodings or native GPU decoding was attempted.

## Measurements

All controls use GPU population, CPU then GPU reads, four partitions/two slots,
16 GiB heap, 4 GiB managed host budget including the 2 GiB pinned pool, 32 GiB
sampled RSS guard, one warmup/two measured reads per projection and post-timing
payload audit. Task sizes are logical estimates; 256 MiB/task means 1 GiB total,
and 2 GiB/task means 8 GiB total. No guard tripped. All jobs have stopped.

Times below are seconds. Combined times are computed sums of measured phases,
not separately timed write-plus-read jobs. Each row is one fresh JVM/cache.
The two within-job samples do not establish independent-run confidence intervals.

| Case | MiB/task | Population | First full GPU read | Warm full GPU median | GPU samples | Population + warm GPU | Warm full CPU median | Warm ID-only GPU median |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: |
| small matrix baseline | 256 | 5.895 | 1.340 | 1.169 | 1.130 / 1.209 | 7.064 | 2.906 | 0.228 |
| small initial chunked | 256 | 6.090 | 1.292 | 1.383 | 1.099 / 1.667 | 7.473 | 2.585 | 0.254 |
| small repeat baseline | 256 | 5.605 | 1.268 | 1.272 | 1.185 / 1.360 | 6.878 | 2.807 | 0.280 |
| small repeat chunked | 256 | 5.884 | 1.026 | 0.870 | 0.840 / 0.900 | 6.754 | 2.669 | 0.263 |
| large matrix baseline | 2048 | 36.848 | 10.230 | 9.127 | 9.834 / 8.419 | 45.974 | 27.069 | 1.432 |
| large chunked | 2048 | 37.092 | 6.480 | 6.937 | 6.431 / 7.444 | 44.029 | 26.024 | 1.423 |

For the larger case, population remained about 37 seconds and the computed
population-plus-full-read sum changed from 45.974 to 44.029 seconds: roughly 4%
for one reuse, despite the larger read-only improvement. ID-only reads changed
little. CPU implementation is unchanged; CPU timing variation is not attributed
to the GPU copy change. CPU consumer times include row materialization and CPU
aggregation, so they do not isolate a CPU-decoder lower bound.
No 512 MiB/1 GiB-task candidate matrix was run.

## Size and memory

| Case | Encoded bytes | Resident storage estimate | Whole-job peak RSS bytes |
| --- | ---: | ---: | ---: |
| small matrix baseline | 489,897,376 | 489,903,072 | 7,911,780,352 |
| small initial chunked | 489,897,376 | 489,903,072 | 9,797,140,480 |
| small repeat baseline | 489,897,376 | 489,903,072 | 7,554,326,528 |
| small repeat chunked | 489,897,376 | 489,903,072 | 7,645,761,536 |
| large matrix baseline | 3,919,838,784 | 3,919,883,904 | 16,960,266,240 |
| large chunked | 3,919,838,784 | 3,919,883,904 | 17,148,686,336 |

Every small case stored 489,897,376 bytes in 128 records and retained all 1 GiB
logical input. Both large cases stored 3,919,838,784 bytes in 1,024 records and
retained all 8 GiB. Disk bytes were zero at every recorded snapshot. The copy
change did not increase useful cached capacity or change the encoded payload.
RSS includes reference work, heap garbage, reads and audit. It was slightly
higher in the large chunked run; this does not demonstrate lower process memory
or lower live temporary working memory. Removing the identified copying is a
code-level property, distinct from a measured whole-process memory reduction.

## Verification and review boundaries

The lead built the candidate separately and independently reproduced every
aggregate, observed CPU/GPU cache decoder, artifact/native/dependency closure,
budget, audit and resident snapshot through /data/tmp/verify-chunked-lead.py.
Correctness probes checked all values/nulls/order for CPU and GPU population,
numeric all-null/all-valid fields, reversed and ID-only projection, and 8,193-row
strings spanning chunk boundaries. Ten decoder lifecycle cases plus a separate
sink boundary/cap check passed. A provider close/reopen test over the same chunks
verified retry reuse; no GPU OOM injection or task-cancellation test was run.
Existing type, version and native-memory accounting limitations remain.

A focused peer reviewed ownership/bounds before execution. Only the architect
marks final acceptance DONE. No production operator was modified to address the
Default GPU-to-row host-allocation failure found in the separate matrix.

## Artifacts

[Machine-readable measurements](vortex-cache-copy-results.csv) include all 24
consumer/projection rows and raw samples. Raw directories are:
/data/tmp/cache-chunked-control-256, /data/tmp/cache-chunked-repeat-baseline,
/data/tmp/cache-chunked-repeat-candidate, /data/tmp/cache-chunked-control-2048.
The two matrix baselines are under /data/tmp/cache-pinned2g-16g/{256,2048}/vortex-gpu.
Candidate runtime/provenance: /data/tmp/vortex-spark-chunked-ipc, application SHA256
9324185bff86c0dc72a6f6dfab94747e5de7e4c0ce7d0b685f9fdb640f3ecf43.
Its source-snapshot directory retains the exact four measured implementation/probe
files. Original application SHA256 remains 78e04232b44e4a0569eb024ec0d911649719c560808c9bc457bceabe1f5f8824.
Correctness logs: /data/tmp/cache-chunked-validation.

After measurement, caller searches found the old flat-buffer provider unused.
It was removed from the unreleased prototype, followed by a separate final build
and a repeated lifecycle check. The measured GPU path is unchanged by that cleanup;
timings above belong to the explicitly identified measured binary, not a new run
of the cleanup build. Final build: /data/tmp/vortex-spark-chunked-ipc-final, application SHA256
51f50618ebb86a9cb5fe7f6c72183cb22627fc87f28bc993bc4db27cf17e7038.
Its source-cleanup-verification.json records the precise difference.
All project operations used MCP docker_exec.
