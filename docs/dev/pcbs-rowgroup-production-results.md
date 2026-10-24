# PCBS row-group production measurements

Status: IN PROGRESS. The first matched pair is verified and READY FOR REVIEW.
Disk-only comparisons, fresh-JVM repeats, public-data measurements and decoded
batch-layout diagnostics are pending. These measurements use the production
logical-buffer reader, separately from the older research multi-file prototype.

## First matched pair

| Implementation | Storage | Fill (s) | Warmup (s) | Measured-read median (s) | Observed heap peak (GiB) | Result |
|---|---|---:|---:|---:|---:|---|
| Existing PCBS | MEMORY_AND_DISK | 164.01 | OOM | — | 45.25 | HEAP_OOM |
| Row-group PCBS patch | MEMORY_AND_DISK | 181.99 | 60.32 | 45.76 | 38.70 | PASS |
| Existing PCBS | DISK_ONLY | pending | pending | pending | pending | running |
| Row-group PCBS patch | DISK_ONLY | pending | pending | pending | pending | pending |

One fresh JVM per row so far. The patched measured reads were 45.764, 47.327 and
40.242 seconds; these are three reads in one JVM, not three independent repeats.
All four patched read row counts and sixteen column sums match the frozen oracle
within the existing 1e-10 relative tolerance. No patched task failed or retried.
The baseline failed its first read after 16.218 seconds in
ObjectInputStream.readArray → JavaDeserializationStream → MemoryStore.putIterator
→ BlockManager.maybeCacheDiskValuesInMemory. That failure duration is not a
successful read timing. Its fill completed normally.

The patch's first fill is about 11% slower. The baseline has no successful read
median at these defaults, so this pair does not establish a read-speed ratio.
Observed heap includes sampled occupancy and logged pre-GC occupancy across fill,
warmup and reads. It includes garbage; it is not a live-set or minimum-heap measure.
Mixed storage residency differs between runs and cannot isolate serializer costs.

## Controls and actual stored layout

Both runs use exactly the same immutable 96 GiB of double values: 805,306,368 rows,
sixteen columns and sixteen 6 GiB partitions. There are eight local task slots,
a 48 GiB initial/maximum heap, Parallel GC with four GC threads, the 1 GiB GPU batch
target, a 15 GiB managed GPU pool, two concurrent GPU tasks, a 40 GiB managed host
limit and a 2 GiB pinned pool. Java stream reset remains 100, MemoryStore unroll
check period remains 16, memory fraction remains 0.6, RDD compression is disabled,
and cache Parquet compression is SNAPPY. No forced GC or control tuning was added.

Hardware is the sandbox RTX A6000. Its managed pool is capped at 15 GiB; its
compute performance is not representative of a customer T4. Timings use warm
filesystem caches. Input checksums run before timed phases. Fill traverses cached
records and can include disk readback or cache repopulation; that definition is
identical in both arms.

| Implementation | Encoded Parquet bytes | Stored records | Smallest record (bytes) | Largest record (bytes) |
|---|---:|---:|---:|---:|
| Existing PCBS | 103,100,327,376 | 112 | 807,025,755 | 939,457,451 |
| Row-group PCBS patch | 103,126,054,960 | 12,400 | 7,428,469 | 8,323,781 |

Actual baseline layout is seven records per partition, not six: 1 GiB is the
configured GPU target. The patch stores 775 row-group records per partition
without reducing that GPU target. These byte counts sum actual Parquet array
lengths, including their file framing; they exclude Java serialization framing.
They are not physical cache-file sizes or GPU residency. The 8 MiB writer target
is soft, and this workload does not establish a bound for large rows or strings.
Actual decoded GPU batch sizes still need the separate layout diagnostic.

## Evidence and verification

Production sources are the implementation in commit c385525a51; subsequent
integration-test and documentation commits do not change that implementation.
Baseline source is 60c28e44f0. Both use the same frozen Spark 3.5.5, JNI/private
runtime and benchmark harness. Spark 3.5.5 identifies this measurement runtime;
the patch changes RAPIDS only and must preserve all supported Spark contracts.
See the [compatibility audit](pcbs-rowgroup-compatibility.md).

Evidence root: `/data/pcbs-oom/pcbs-rowgroup-production/`.

- `runtime-matched001/artifacts.json`: pinned runtime and source/class-byte audits.
- `production-runs/canonical-{baseline,patched}-memory-disk001/`: original commands,
  logs, process reports, phase metrics, event logs and original result JSON.
- `production-runs/canonical-{baseline,patched}-memory-disk001-verified001.json`:
  strict separate verification envelopes; root executed both successfully.
- `production-runs/canonical-first-pair001.json`: explicit audited comparison.
- `support-audit/first-pair-{scalar,gc-oracle}-audit.json`: independent agent
  verification of raw phase values, heap peaks and all four patched read oracles.

The original verifier reports ERROR solely because stock SparkContext prefixes
standard Java module/IPv6 options to the requested driver-options string. Those
original results and all frozen runners remain unchanged. The separately reviewed
verification sidecar first reproduces the original result, proves the exact known
prefix and actual JVM arguments, and delegates to the same original verifier with
only that one copied metadata field normalized. All metrics remain identical;
only classification/issues change. The baseline recheck is HEAP_OOM and the
patched recheck is PASS. This correction changes no benchmark execution or data.
