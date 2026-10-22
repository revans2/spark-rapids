# Default-settings cache batch-size experiment

Status: READY FOR REVIEW; architect acceptance pending. No production changes.

Only spark.rapids.sql.batchSizeBytes varies in these new runs.
spark.storage.unrollMemoryCheckPeriod=16 and
spark.serializer.objectStreamReset=100 remain at their defaults.
Memory fraction stays 0.6.

## Results

| Target MiB | Records/partition | Largest encoded record MiB | Passed JVMs | Cache fill s | Median read s | Read GC % | Max after full GC GiB |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 16 | 402 | 15.9 | 3/3 | 271.2 | 45.0 | 9.3 | 31.1 |
| 32 | 198 | 31.8 | 3/3 | 264.0 | 42.9 | 15.0 | 35.0 |
| 64 | 102 | 63.7 | 0/1 | 259.5 | — | — | — |
| 512 | 12 | 509.5 | 0/1 | 256.6 | — | — | — |

Every failed JVM is retained in the pass count and has no successful read
timing. Cache-fill timings remain available even when a later read fails.
Initially passing sizes received two more fresh JVMs; later failures would
remain in the results. Each successful JVM completed one warmup plus three
timed full SQL scans. The table gives the median of per-JVM read medians and
the median cache-fill time across that size's JVMs.

- 16 MiB: fill 270.7–271.6 s; JVM read medians 42.8–45.5 s.
- 32 MiB: fill 254.4–264.0 s; JVM read medians 41.8–43.5 s.

For reference, the prior 1 GiB target with unroll/reset 1/1 passed three JVMs:
cache fill 249.4 s and median read 37.5 s.
That reference changes the memory-check settings as well as the batch size;
it does not isolate the overhead of either parameter.

GC percentage is summed collection time divided by summed elapsed time for
the three timed scans, summarized by its median across passing JVMs.
Heap after full GC is occupied whole-heap size, not a dominator retained-size
measurement. The case CSV also includes sampled occupied peaks, which can
include garbage. None of these measurements establishes an OOM probability.

## Fixed workload and resources

Spark 3.5.5, Java 17.0.20.1, RTX A6000, one local[8] JVM with 48 GiB heap
and ParallelGC (four GC threads). GPU concurrency/max are both 2; fixed
15 GiB GPU pool, 40 GiB host off-heap limit and 2 GiB pinned pool.
Storage is MEMORY_AND_DISK. No forced GC; no additional heap dumps.

The data has 801,279,744 rows and 16 seeded random Double columns
(95.520 GiB of raw values). Sixteen cached partitions come from 96 source
partitions. Each cache partition has 50,079,984 rows, about 6 GiB.
Encoded totals are about 95.5 GiB and are measured for every case.
All sizes preserve row count, schema, partition count and data distribution;
bit-identical random values across changed GPU batch boundaries are not claimed.

Every SQL action computes sums of all columns, without a filter, and must
scan all 801,279,744 rows and produce finite repeatable aggregates.
Population is sequential, one cache partition at a time, exactly as in the
previous experiment. Fill time includes generation, encoding, serialization,
storage, metrics and job overhead. It does not measure eight-way parallel
cache creation. First-action initialization is included consistently.

The global target affects upstream generation and downstream GPU operators
as well as PCBS splitting. These are end-to-end configuration measurements,
not isolated serializer throughput. Below 64 MiB, Range's target/8 row limit
changes its output boundaries. PCBS then uses its existing target-minus-0.5%
budget and row estimate to split each input. Actual row sequences, sizes and
plans are verified, including tail records. No serializer source was modified.

## Scope changes and evidence

The user redirected the original 64/128/256/512/1024 MiB two-profile plan to
out-of-box memory checks and smaller batches. Completed 64 and 512 MiB
default-settings failures remain in the table. Unstarted old matrix jobs
were cancelled. The first 32 MiB run was stopped before completion to honor
the requested small-to-large order; it is ABORTED_EXCLUDED. A fresh upward
32 MiB run followed the first complete 16 MiB pass. An 8 MiB test was only
authorized if 16 MiB failed.

The 512 MiB child finished while its supervisor was held for discussion.
Its kernel exit status and raw logs established heap OOM; its result was
recovered and independently verified. Supervisor wall time is null because
the pause makes it incomparable; in-JVM phase timings are unaffected.
All original logs and cancellation/recovery metadata are retained.

Unchanged plugin artifact: 26.12.0-SNAPSHOT built from
3cdf711ded8e8b72d0f3ee92866401df5d9d2a06. Benchmark source, benchmark jar
and plugin hashes match the preceding unroll sweep's manifest.
Raw evidence and scripts: /data/pcbs-oom/batch-size-sweep.

Root and an independent worker verified raw events, effective Spark/JVM
configuration, source/artifact hashes, exact partition/record rows and bytes,
scan iteration IDs, storage checkpoints, exit status and GC calculations.
All project operations used MCP docker_exec. Production code and the Scala
benchmark are unchanged. Full unit/integration suites are not claimed.
Three fresh JVMs are a diagnostic check, not production reliability.
The synthetic sum workload and A6000 do not reproduce the customer's full
MERGE workload or T4 hardware. Local cache files can be served from the OS
page cache; physical disk throughput and the customer's larger spill working
set are not reproduced.

[Per-JVM results](pcbs-batch-size-sweep.csv).
