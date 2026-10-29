# Reduce Parquet cache heap pressure while preserving large GPU batches

PCBS can cache large Parquet arrays that accumulate between Spark's count-based
unroll checks and serialization resets. This POC stores approximately 8 MiB
uncompressed row groups, then bundles them toward the configured GPU batch target.

Records are extracted without reencoding column chunks and materialized lazily
from spillable staging. The reader combines column-chunk views and a rewritten
footer without concatenating the encoded payload. Existing record/reader
contracts and Parquet column pruning are preserved. The 8 MiB target is soft,
not a bound on encoded size or peak memory.

## Population, reads and heap

Matched historical results, before → after: Spark 3.5.5, sixteen partitions,
eight slots/two GPU tasks, 48 GiB heap, DISK_ONLY, 1 GiB GPU target, 15 GiB GPU
pool, 40 GiB managed host, 2 GiB pinned, inner Snappy, default reset100/unroll16.
Each arm used one fresh JVM, one warmup and three reuse reads; read time is the
median. Outer compression also affects other Spark paths.

| Dataset / outer compression | Population s | Read s | Observed heap GiB | Parquet payload GiB |
| --- | ---: | ---: | ---: | ---: |
| GHArchive / OFF | 48.721 → 46.500 | 15.645 → 15.407 | 19.984 → 16.311 | 23.512 → 23.336 |
| GHArchive / LZ4 | 54.949 → 52.193 | 14.131 → 13.808 | 38.505 → 16.157 | 23.512 → 23.336 |
| GHArchive / Zstd | 70.013 → 67.309 | 19.373 → 19.337 | 43.734 → 16.651 | 23.512 → 23.336 |
| TLC / OFF | 18.625 → 18.137 | 4.905 → 6.468 | 15.685 → 15.950 | 5.755 → 6.265 |

GHArchive: 32,078,549 rows; TLC: 239,470,448. Heap is sampled occupancy, including
garbage. GHArchive improves; TLC reads regress 31.9% and its payload grows 8.9%.

![PCBS read time and heap occupancy](figures/pcbs-historical-tradeoffs.png)

For `total = population + N × read`, the TLC crossover is 0.31 reuse reads per
population: the old layout wins from the first reuse read. The new layout wins
both phases in the three GHArchive cases.

These previously verified results come from
`91d748957d:docs/dev/pcbs-rowgroup-production-results.md`: baseline `60c28e44f0`
versus selected 8 MiB/default-row candidate `d9ba8787f6` (policy `fa29f4d90c`).
The raw archive is currently unavailable, so these have not been freshly replayed.

## OOM and batching evidence

An earlier 8 MiB/65,536-row-cap implementation (`c385525a51`) tested 96 GiB of
semantic values across sixteen partitions with a 48 GiB heap and default Spark
reset/unroll settings. Original PCBS failed MEMORY_AND_DISK readback in **3/3**
fresh JVMs; the patch passed **3/3**. Stored records increased from 112 to 12,400,
while the largest encoded record fell from 895.936 to 7.938 MiB. A separate
diagnostic returned **112 GPU batches in both implementations**.

This establishes the batching approach; it is distinct from the final default-row
policy measured above. Later lazy-writer changes were validated separately, not
retimed in that cohort.

## Predicate pruning

The POC connects GPU cache scans to the serializer's batch filter, reusing Spark
ParquetFilters and Parquet RowGroupFilter. Unsupported predicates retain records;
residual filters still run. A conservative guard covers lossy UTF-8 literals.

A separate 2 GiB/task numeric cohort measured the same predicates with pruning
OFF → ON, using GPU population and GPU full reads:

| Predicate | PCBS read s |
| --- | ---: |
| All rows match | 1.014 → 1.047 |
| Highest quarter | 0.958 → 0.581 |
| No rows match | 0.973 → 0.211 |

Actual scan metrics separately confirm skipping for all four CPU/GPU writer/reader
combinations. This cohort uses an instrumented Spark runtime with tracing disabled.

All patches are in the plugin repository. Cross-version acceptance remains
incomplete. [Build/provenance](README.md) · [Results](results.json).

*Proof of concept; implementation, analysis and draft prepared with AI assistance.*
