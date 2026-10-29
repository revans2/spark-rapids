# Explore a shared CPU/GPU packed-column cache

This Spark cache POC uses small packed records with independently compressed
columns. CPU population reuses Spark ArrowWriter and task-owned codec contexts;
GPU population uses cuDF contiguous packing and batched chunk compression. A
column directory enables projection; readers group records toward the GPU batch
target. Min/max/null statistics support predicate pruning.

Uncompressed, Snappy, LZ4 and Zstd interoperate across CPU/GPU writers and readers.
ANS and legacy Cascaded are GPU-only; their CPU paths throw. Redesigned Cascaded
was not measured.

## CPU fallback

256 MiB/task, four tasks/two waves (1 GiB total), 32 numeric columns, local[2].
Population includes upstream generation and row conversion. Reads are medians
of five samples after one warmup. Encoded size includes framing/metadata, except
default Spark's separate statistics row.

| CPU writer / CPU reader | Population s | Full read s | ID-only read s | Encoded MiB |
| --- | ---: | ---: | ---: | ---: |
| Default Spark | 20.118 | 1.479 | 0.077 | 415.7 |
| PCBS | 13.297 | 5.305 | 4.083 | 701.2 |
| Packed uncompressed | 2.754 | 1.874 | 0.110 | 1024.1 |
| Packed Snappy | 2.912 | 2.538 | 0.145 | 516.9 |
| Packed LZ4 | 2.777 | 2.468 | 0.123 | 506.0 |
| Packed Zstd | 4.301 | 2.406 | 0.130 | 393.4 |

Snappy population is **6.9× faster than default** and **4.6× faster than PCBS**.
Its full/projected CPU reads are **2.1×/28× faster than PCBS**. Default remains
the faster CPU reader. CPU-written PCBS uses uncompressed Parquet pages with
Parquet encoding; GPU-written PCBS uses Snappy.

The reuse tradeoff is `total = population + N × read`, where N counts reads after
population. The following crossovers are calculated from measured phases, not
long-run benchmarks:

| Packed format vs default | Full-read crossover N | Default wins from read | ID-only crossover N | Default wins from read |
| --- | ---: | ---: | ---: | ---: |
| Uncompressed | 43.87 | 44 | 522.48 | 523 |
| Snappy | 16.24 | 17 | 252.99 | 253 |
| LZ4 | 17.53 | 18 | 373.26 | 374 |
| Zstd | 17.05 | 18 | 297.92 | 298 |

Packed wins below each crossing, default above. Against PCBS, all four packed
formats win both CPU phases, so there is no crossover in this cohort.

![CPU total time versus reuse](figures/cpu-reuse-cost.png)

## GPU performance and size

2 GiB/task, four tasks/two waves (8 GiB total), 1 GiB GPU output target.
GPU-written caches and GPU-requested queries only; CPU population/reads were not
run in this supplement. Default Spark's cache encoder/decoder remain CPU paths.

| Format | Population s | GPU-mode full read s | GPU-mode ID read s | Encoded MiB |
| --- | ---: | ---: | ---: | ---: |
| Default Spark | 156.765 | 4.802 | 0.437 | 3325.6 |
| PCBS | 4.426 | 1.043 | 0.484 | 3917.4 |
| Packed Snappy | 4.505 | 1.293 | 0.201 | 4166.3 |
| Packed LZ4 | 4.830 | 1.391 | 0.217 | 4055.9 |
| Packed Zstd | 5.847 | 1.228 | 0.197 | 3020.2 |
| Packed ANS | 4.138 | 1.575 | 0.228 | 4517.9 |
| Packed Cascaded | 4.859 | 1.885 | 0.216 | 3138.7 |

Snappy population is within 2% of PCBS. PCBS wins full reads; packed wins
projection. Zstd stores 23% fewer bytes than PCBS with 18% slower full reads.
GPU-uncompressed is excluded after a previously reproduced Java heap failure.

For full reads, PCBS wins both phases against Snappy/LZ4/Zstd/Cascaded; ANS's
population advantage disappears before the first reuse read. For ID-only reads,
Snappy recovers its population penalty on read one, LZ4/Cascaded on read two,
and Zstd on read five.

## Pruning

The high-quarter predicate avoided 75% of selected packed bytes decoded;
no-match avoided all selected payload decoding. Same-predicate full-read medians
at 2 GiB/task, pruning OFF → ON:

| Format | High-quarter s | No-match s |
| --- | ---: | ---: |
| PCBS | 0.958 → 0.581 | 0.973 → 0.211 |
| Packed Snappy | 1.213 → 0.539 | 1.116 → 0.208 |
| Packed LZ4 | 1.122 → 0.488 | 1.219 → 0.192 |
| Packed Zstd | 1.066 → 0.480 | 0.996 → 0.169 |

All-match controls and full precision are in [results.json](results.json).

## Scope

The POC supports flat int/long/default-binary strings. Numeric timings use an
instrumented Spark 3.5.5 distribution with compression tracing disabled. Build45
provided timings; build46 separately validated final invalid-UTF8 guards.

Settings: 16 GiB heap, 32 GiB managed host including 2 GiB pinned, 24 GiB RMM,
64 GiB sampled RSS stop guard. All partitions stayed resident; available
spill/retry counters were zero. Spill recovery was not exercised. Whole-job peaks
were 19.830 GiB RSS and 12.049 GiB tracked RMM, not serializer-only working sets.

Large-string performance, broader types, CPU columnar input and cross-version
acceptance remain unverified. Next priorities are broader workloads, profiling
CPU reads and portable builds. All patches are in the plugin repository;
[build details and provenance](README.md).

*Proof of concept; implementation, analysis and draft prepared with AI assistance.*
