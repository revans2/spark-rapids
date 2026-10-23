# Controlled PCBS serialization and record-size experiments

Status: IN PROGRESS. Research only. The architect approved the serialization and
record-size questions; heavier downstream GPU workloads are deferred. All project
operations use MCP docker_exec. Released Spark/PCBS/StorageLevel behavior remains
unchanged. Fixtures and generated overlays are unreleased; no compatibility
adapter, production configuration, native store or Spark OFF_HEAP pool is added.

The prior comparison combined custom serialization with 32 MiB splitting. It
cannot isolate the codec benefit. These experiments retain the exact canonical
96 GiB values, sixteen cache partitions (6 GiB each), local[8], 48 GiB configured
heap, ParallelGC/four GC workers, 15 GiB GPU pool, concurrency two, 40 GiB RAPIDS
host limit, 2 GiB pinned pool and global 1 GiB producer target. Memory fraction
stays 0.6. Source SHA256 preparation is identical and warms the OS page cache.
No forced GC occurs between reads. The consumer remains all-column sum/count.

## Schedule

`run_controlled.py` launches one explicit case. `run_controlled_matrix.py` freezes
an 84-case schedule and runs GPU JVMs sequentially:

1. Twenty-four serialization cases: Java/custom codec crossed with reset/unroll
   100/16 and 1/1; record cap and downstream target 1 GiB. DISK_ONLY first,
   MEMORY_AND_DISK second. Three fresh JVM repetitions per case, alternating
   case order. Each JVM fills once, warms once and measures three reads.
2. Sixty record-size/consumer cases: private caps 64/128/256/512/1024 MiB,
   Java 1/1 versus codec 100/16, both storage levels, three fresh JVM repetitions.
   Each JVM fills once and changes only downstream coalescing (off/256 MiB/1 GiB)
   on the SAME populated cache. Each target has one warmup and three measured
   reads. Consumer and case order reverse on the middle repetition.

The second stage repeats the 1 GiB reference because its twelve-read schedule is
different from the first stage's four reads. Report results by stage, storage and
consumer target; do not treat different read schedules as identical controls.
The configured private cap is not an encoded/device allocation guarantee.
Record actual encoded distributions and producer/decoded/combined sizes.
The audited canonical producer has 112 batches (seven per partition), exactly
96 GiB device values, with minimum 806,860,032 and maximum 939,265,152 bytes.
The coalescer packs whole incoming batches below its 1 GiB target; it has no
900 MiB minimum-fullness contract. Enforce this exact producer fingerprint and
all sixteen 1 GiB plan goals across full cases. This dataset is not the historical
six literal 1 GiB records per partition. An initial successful DISK_ONLY run was
incorrectly rejected by that unsupported lower bound; its raw data and original
verifier result remain preserved under matrix1, with an independent audit and
separate producer-contract recheck. Subsequent measurements use a fresh matrix.

Dynamic consumer selection lives only in a process-local research observer. New
query plans capture the requested target before execution; cached payloads and
RDD identity remain unchanged. Coalescing does not split an existing decoded
batch: a 256 MiB target therefore has an input-size floor for 512 MiB/1 GiB
records. This is measured as an outcome, not reported as actual 256 MiB output.
Only the iterator immediately above the cache
scan receives the selected target. Its plan label can still show the global
1 GiB goal, so actual output sizes and placement must be checked independently.

## Evidence and gates

Before large runs, require fixture compilation, independent source review and
small exact-data gates for Java/codec, both storage levels, settings pairs and
consumer directions. Test small-record splitting and large-record code paths.
Small gates establish correctness, not large-memory safety or performance.

Every full case has a fresh directory, frozen source/artifact/control hashes,
raw Spark/event/GC logs, process CPU/RSS/I/O samples, optional device samples,
phase timings and result/verifier JSON. Exact canonical count/all-column sums,
producer/read row counts, actual batch limits, complete cache residency and
blocking unpersist are correctness requirements. Spark task failures/retries
cannot silently become a successful timing result.

Peak heap is the maximum of sampled occupancy and logged pre-GC occupancy for
each timed phase, not live payload or a guaranteed upper bound. Preserve fill
completed before read OOM. Successful phases after warmup are measured separately
for each consumer target. Residency is selected from that target's own checkpoint;
missing phases stay missing. MEMORY_AND_DISK residency can change across reads.

A genuine heap OOM with valid controls is negative evidence and does not stop the
matrix. Unexpected errors, verifier mismatches, timeouts, resource contention or
changed frozen inputs park the matrix. No automatic tuning or data reduction is
allowed. Only a completed JVM's own disposable `local` directory is removed;
logs/results remain. The runner records progress in `status.json`, raw case
results in `results.json`, and a sparse per-case/per-target table in `results.csv`.
Automatic completion means RUNS_COMPLETE; independent review remains required.

The physical I/O counters `/proc/PID/io` read_bytes/write_bytes are distinct from
syscall rchar/wchar and logical Parquet payload bytes. Warm page cache can yield
very little physical disk I/O. GPU samples are device-wide, with sampler failure
status retained separately. Profile selected cases with JFR/Nsight separately
from primary timing repetitions; do not sum overlapping operator times as wall
time. Neither serialization choice moves payload arrays off the Java heap.

## Mechanisms to check against the profiles

Root and a worker traced the actual benchmark JDK 17.0.20.1 bytecode and the
Spark 3.5.5 disk-stream path. Java object serialization stages byte-array data
through a reusable 1 KiB buffer on both write and read. The research codec uses
bulk stream calls but still allocates a payload-sized heap array. With fixed
total bytes, the smaller Java staging loop is byte-proportional; it alone cannot
explain a record-size effect.

Bulk disk writes have another cost: Spark uses a file channel, whose JDK heap
buffer path obtains a temporary direct buffer and copies into it. Those buffers
can be cached per writing thread. Larger codec writes may therefore increase
direct memory even if serialization CPU improves. Codec is neither zero-copy nor
a native cache. Compare phase-aligned RSS and JFR DirectBufferStatistics with
execution/allocation samples before attributing timings or memory differences.
These are code-supported mechanisms, not established causes of the measurements.
Bytecode evidence is under `jdk17-serialization-trace/`, including
`direct-buffer-followup.md`; actual native occupancy and JNI internals are not
established by that trace.

Evidence root: `/data/pcbs-oom/controlled-cache-comparison/`. The committed
investigation tracker owns task status, verification and any parked findings.
