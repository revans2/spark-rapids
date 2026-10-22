# Cache unroll-period performance sweep

Status: READY FOR REVIEW; architect acceptance pending. No production code changes.

The reported sweep varies only `spark.storage.unrollMemoryCheckPeriod` and
`spark.serializer.objectStreamReset`. Memory fraction remains 0.6, with the
same data, query, storage level and resource settings throughout.

## Results

| Unroll period | Reset | Passed JVMs | Median scan seconds | JVM-median range | Median GC time % | Max after full GC, GiB |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 3/3 | 37.5 | 37.4–38.7 | 24.7 | 41.9 |
| 1 | 2 | 3/3 | 37.8 | 36.4–38.6 | 23.9 | 41.9 |
| 1 | 4 | 0/1 | — | — | — | — |
| 1 | 100 | 0/1 | — | — | — | — |
| 2 | 1 | 3/3 | 42.3 | 39.6–42.5 | 33.3 | 42.9 |
| 4 | 1 | 0/1 | — | — | — | — |
| 8 | 1 | 0/1 | — | — | — | — |
| 16 | 1 | 0/1 | — | — | — | — |

Period 1/reset 1 was about 11% faster than period 2/reset 1 in this test,
with less GC time. Reset 2 offered no clear speed improvement over reset 1.
There is no successful default-period timing to compare: the tested larger
periods failed. These results do not measure a general overhead percentage.

Times are the median of per-JVM medians. Each JVM runs one warmup plus three
timed full SQL scans. Initially successful settings receive two further fresh
JVMs in shuffled rounds (seed 16362), giving three JVMs per candidate.
Screening failures are retained; they receive no successful-scan timing.
All six repeats passed. All five screening failures were heap OOMs on the
first SQL scan after successful cache population.

GC percentage is calculated per JVM from summed GC time divided by summed
timed-scan elapsed time, then summarized by its median across passing JVMs.
Heap after full GC is whole-heap occupancy reported by the GC log, not a
dominator retained-size estimate. The CSV separately includes peak sampled
occupied heap, which can include garbage. JVMs that fail do not contribute
conditional successful timings to the table.

## Workload and environment

One `local[8]` JVM, `-Xms48g/-Xmx48g`, Java 17.0.20.1, Spark 3.5.5, RTX A6000.
RAPIDS GPU concurrency 2/max 2, fixed 15 GiB RMM pool, host limit 40 GiB,
pinned pool 2 GiB.
ParallelGC with four GC threads; `MEMORY_AND_DISK` and memory fraction 0.6.
No harness-forced GC. OOM heap dumps disabled during the sweep; original
failure dumps were already collected separately.

Sixteen cached partitions each have six records: 8,346,664 rows and measured
1,068,538,171 encoded bytes per record. Total 801,279,744 rows and
102,579,664,416 encoded bytes (95.535 GiB), from sixteen seeded random Double
columns. The random values compress poorly. Initial cache residency is
23.884 GiB in memory/71.651 GiB on disk for all reported settings. Each successful scan
retains all 16 partitions; storage checkpoints are audited individually.

SQL sums every column, without a filter, forcing a full decode of all rows.
Every scan must report exactly 801,279,744 rows and finite repeatable aggregates.
Population, exact record shape, exit status, effective Spark configuration,
source/jar hashes and scan/storage checkpoints are independently audited.
One JVM runs at a time and only its own scratch cache is removed afterward.

The plugin artifact is 26.12.0-SNAPSHOT built from
3cdf711ded8e8b72d0f3ee92866401df5d9d2a06. Diagnostic source/jar remain
unchanged from the preceding six-record experiment. Raw evidence, commands,
manifests, telemetry and analysis are under /data/pcbs-oom/unroll-sweep.
The aborted out-of-scope memory-fraction case has no accepted result and is
excluded; its audit metadata is retained.

## What the setting affects

The only runtime lookup is in `MemoryStore.putIterator`. Both deserialized
iterator caching and iterator-to-serialized caching use that loop. It affects
cache construction and deserialized disk-cache reloads, not generic Java
deserialization. Executor task-envelope and ResultTask/ShuffleMapTask closure
deserialization do not consult it. Subsequent task execution can fetch cached
blocks through the affected path, and changes to GC/cache behavior can affect
other task timings indirectly.

Checks occur after obtaining/storing a record. For six records, periods 1, 2 and 4
check records 1–6; 1, 3, 5; and 1, 5 respectively. If unrolling continues, a final
size check follows. Periods 8 and 16 both check the first record, so their scheduled
checks are identical in this experiment. Period 1 is not preallocation
admission or a global concurrency/heap limit.

The deserialized holder's `estimateSize` call is O(1); object-size sampling is
driven by updates independently of this period. Nevertheless, extra checks
can change reservations, cache residency, disk traffic and GC. These timings
measure end-to-end cache reads, not the isolated CPU cost of a check or a
universal Java deserialization overhead.

Sources inspected: upstream Spark 3.5.3/3.5.5 MemoryStore (byte-identical),
Executor.TaskRunner, ResultTask, ShuffleMapTask and SizeTracker.
[MemoryStore](https://github.com/apache/spark/blob/v3.5.3/core/src/main/scala/org/apache/spark/storage/memory/MemoryStore.scala),
[Executor](https://github.com/apache/spark/blob/v3.5.3/core/src/main/scala/org/apache/spark/executor/Executor.scala),
[SizeTracker](https://github.com/apache/spark/blob/v3.5.3/core/src/main/scala/org/apache/spark/util/collection/SizeTracker.scala).

## Limits and verification

This is a local synthetic cache scan on newer plugin/Spark artifacts, not the
customer's full MERGE workload or T4 hardware. Three fresh JVMs do not establish
an OOM probability or production reliability. Within-JVM scans are correlated;
the table therefore uses JVM-level medians and shows their range.

All sandbox commands and filesystem operations use MCP docker_exec. Root and
an independent worker verify raw results/configuration/hashes/metrics; final
report interpretation was independently reviewed before delivery. Full unit or
integration suites are not claimed; production code and benchmark code are
unchanged. Details: [case CSV](pcbs-unroll-period-sweep.csv).
