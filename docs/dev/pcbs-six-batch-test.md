# PCBS six-batch heap investigation

Status: READY FOR REVIEW. Diagnosis only; no production fix. Architect acceptance pending.

Both the customer-shaped raw cache reload and full SQL scan fail with
`spark.serializer.objectStreamReset=100` and `1`. Heap dumps show that the
readers retain substantially more than one batch each. Resetting Java stream
handles removes one source of retention, but Spark's unroll buffers and
iterators retain other batches.

## Workload and boundaries

Each of 16 cached partitions contains exactly six ParquetCachedBatch records.
Every record has 8,346,664 rows and a measured byte-array length of
1,068,538,171 bytes (1,019.04 MiB). Each partition has 50,079,984 rows and
6,411,229,026 encoded bytes. The whole cache has 801,279,744 rows and
102,579,664,416 encoded bytes (95.535 GiB).

Sixteen seeded random Double columns contribute 128 bytes per row, or
95.520 GiB of decoded value data overall. Random values compress poorly:
encoded and decoded sizes are similar here. These are measured encoded sizes
and calculated value-buffer sizes, not a reconstruction of the customer's
schema or compression ratio. Generation uses 96 source partitions narrowed to
16 cache partitions to obtain six evenly sized records.

The JVM uses local[8], -Xms48g/-Xmx48g, ParallelGC with four GC threads,
Spark's normal memory fraction 0.6, MEMORY_AND_DISK, and no explicit GC.
The GPU pool is fixed at 15 GiB, with concurrentGpuTasks=2 and
maxConcurrentGpuTasks=2; host off-heap limit is 40 GiB and pinned pool 2 GiB.
Batch target is 1 GiB. The cache initially holds four complete partitions in
memory (23.884 GiB) and twelve on disk (71.651 GiB).

Spark is 3.5.5, the plugin artifact is 26.12.0-SNAPSHOT built from
3cdf711ded8e8b72d0f3ee92866401df5d9d2a06, Java is 17.0.20.1, and the GPU is an
RTX A6000. These differ from the customer's Spark 3.5.3, platform 26.04.2 and
T4. Local mode also shares the driver and executor heap. This reproduces the
allocation path and memory pressure, not the entire production application.

## Separate raw reload and SQL tests

The raw test counts CachedBatch.numRows without decoding on the GPU.
The SQL test starts in a fresh JVM, skips the raw prefix, and runs:

```sql
SELECT sum(c0), sum(c1), sum(c2), sum(c3),
       sum(c4), sum(c5), sum(c6), sum(c7),
       sum(c8), sum(c9), sum(c10), sum(c11),
       sum(c12), sum(c13), sum(c14), sum(c15)
FROM cached_data
```

This consumes every column and row, with no selective filter. Successful scans
must report the exact cache row count, finite repeatable aggregates and retained
cache partitions. The runner requests one warmup plus three measured scans.
An OOM before completing those scans provides no performance comparison.

| Read path | Reset 100 | Reset 1 |
|---|---|---|
| Raw cached RDD | Heap OOM on first reload | Heap OOM on first reload |
| SQL aggregate in fresh JVM | Heap OOM on first scan | Heap OOM on first scan |

Each full-size cell is one fresh JVM, not an estimate of OOM probability.
Population passed with the exact accepted record shape in all completed cells.
A separate one-partition pilot passed the SQL scans and shape checks; it is
entirely memory-resident and is not a pressure result. The earlier incomplete
12/14 GiB GPU-pool experiment is excluded. Raw reset 4/2 cases were not run.

## What the raw heap dumps contain

Both raw endpoint dumps contain exactly 44 large byte arrays, each the measured
record size above: **47,015,679,524 bytes, or 43.787 GiB** of payload. All other
byte-array payloads together contribute only about 21 MiB in the reset-100 dump.
The counts below are mutually exclusive; shared references are counted once.

| Owner/state | Arrays, reset 100 | Arrays, reset 1 | Payload GiB |
|---|---:|---:|---:|
| Four completed Spark cached blocks | 24 | 24 | 23.884 |
| Spark unroll buffers | 7 | 7 | 6.966 |
| Completed reader records outside those buffers | 6 | 6 | 5.971 |
| Arrays currently being filled by deserializers | 7 | 7 | 6.966 |
| Total | 44 | 44 | 43.787 |

With reset 100, all 20 noncached arrays are also referenced by eight
ObjectInputStream handle tables. Of the six completed reader records outside
unroll buffers, three have only handle-table references and three also have
NextIterator.nextValue references.

With reset 1, only the seven arrays being filled remain in handle tables.
The seven unroll-buffer arrays belong to two task-local holders containing
four and three records. All six other completed records remain referenced by
NextIterator.nextValue. Resetting the streams worked; it could not remove
references held by those other objects.

Eight task-thread stacks are in readArray, but only seven new arrays exist:
one allocation failed. Java-frame roots identify the in-progress reads and
task-local unroll holders. This is an instantaneous ownership snapshot, not
a claim that 44 batches were all being decoded simultaneously on the GPU.

## What the SQL-query dump adds

The reset-100 SQL JVM also has 44 batch-sized arrays (43.787 GiB of payload).
Its mutually exclusive breakdown is:

| Owner/state | Arrays | Payload GiB |
|---|---:|---:|
| Completed cached blocks | 24 | 23.884 |
| Active unroll holders | 10 | 9.952 |
| Partially returned unroll buffers | 2 | 1.990 |
| Historical handle references only | 2 | 1.990 |
| Previous iterator values outside those buffers | 2 | 1.990 |
| Active GPU decode input outside those buffers | 1 | 0.995 |
| Arrays being filled by deserializers | 3 | 2.985 |

The eight reader task threads are at different points: two in Table.readParquet,
two waiting on GpuSemaphore, three filling arrays in readArray, and one failing
to allocate its next array. One of the two GPU decode inputs is already in the
24 cached arrays; the other is the separate decode-input row above.
The two semaphore waiters retain the two partially returned unroll records.
These counts therefore do not double-count either decoder inputs or shared
handle-table references.

The reset-1 SQL dump again contains 44 large arrays: 24 cached, nine in active
unroll holders (four/four/one), three in returned unroll buffers, four previous
iterator values, and four arrays being filled. No array is held only by
historical handles. One returned unroll record is also an active GPU decode
input, so it is counted once. Two other returned records belong to semaphore
waiters. Storage accounting includes 3,210,859,832 bytes of unroll reservations
against roughly 11.94 GiB in active/returned unroll payload.

These are direct observations from the approved SQL workload: capping GPU
access at two still allows cache deserialization and unroll retention in the
other tasks. Reset 1 removes historical handle retention but does not bound
the bytes retained by unroll buffers and iterators.

## Why Spark's memory budget did not prevent this

Spark's on-heap storage pool in the raw reset-100 dump has a 27.424 GiB budget. It accounts
for approximately 23.884 GiB of cached blocks and 2.99 GiB of unroll
reservations. The actual reader payload is about 19.90 GiB.

MemoryStore.putIterator stores each deserialized record before checking its
size. Its default check interval is 16 records: it checks after the first,
then after the seventeenth. Six roughly 1 GiB records can therefore accumulate
before the final block-size check. Its hasNext condition also invokes the
underlying reader before checking keepUnrolling.

The unroll holder keeps records already collected for the partition.
Separately, NextIterator.next() returns nextValue without clearing it;
hasNext() replaces that field only after getNext() finishes reading the next
record. The old and new arrays can coexist. At reset 100, Java's handle tables
add further history retention. GPU concurrency=2 does not limit all eight
Spark tasks' heap allocations in this disk-cache/deserialization path.

The next allocation needs 1,019 MiB in one allocation area. In the reset-100
raw JVM, the final allocation-failure full GC leaves old generation with
about 99 MiB free and Eden with about 556 MiB free. The empty survivor space
is not general allocation headroom. Subtracting occupied bytes from -Xmx
therefore overstates the room available for another large array.

The dump does not require a second full serialized copy of every cached batch
to explain the OOM. The large arrays are actual Parquet batch buffers and
arrays currently being filled. Multiple retaining references to an array are
not extra copies of it.

## Evidence and verification

Compact ownership counts are saved in [pcbs-six-batch-heap.csv](pcbs-six-batch-heap.csv).
Raw JVM logs, GC logs, event logs, telemetry, manifests, commands and dumps are
under /data/pcbs-oom/customer-six and /data/pcbs-oom/customer-query-only.
Analysis is under /data/pcbs-oom/heap-analysis. Heap payloads are seek-skipped;
object layouts, reference edges and roots are indexed on disk with a 3 GiB
process limit. Classification uses direct typed field paths, not broad
ancestor reachability or inferred retained sizes.

The lead independently compiled the diagnostic, audited raw events against
saved results, checked exact batch and scan counts, read the decisive Spark
and PCBS source, verified the large-array headers directly, and compared
accounting fields with GC logs. Independent review checked all four dumps directly, including inherited field layouts, array
headers, object-array slots and Java-frame roots. Storage accounting reconciles
exactly after including the small non-PCBS cached entries and the serialized
broadcast entry. Broad reverse BFS is not a dominator analysis; none of the
ownership conclusions relies on it.

All four full-size JVMs exited with Java heap OOM after successful population;
none completed a timed read. All test JVMs and GPU monitoring processes stopped.
No configuration or implementation fix has been selected.
