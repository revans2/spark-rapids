# Spark cache comparison with a 2 GiB pinned pool

Status: IN PROGRESS. Independently verified 10/12 attempted GPU-population cases: 9 passed, 1 failed.
The original zero-pool [comparison](vortex-cache-results.md) and CSV are preserved.
CPU-only producer cases were not rerun. No production defaults changed.

The original application/native dependency closure is unchanged. Only the pinned
pool setting changes to 2 GiB. Every run uses heap 16 GiB and a 4 GiB managed
host budget including the pool: startup reports 2,048 MiB pinned, 2,018 MiB
non-pinned and 30 MiB overhead. Arrow/Rust allocations remain outside that cap.
The sampled RSS stop guard remains 32 GiB, with a 1,200-second per-run timeout;
no machine-exhaustion test is authorized.

The deterministic fixed-wide numeric fixture, four partitions/two local task slots,
default memory-and-disk storage and CPU-before-GPU read order match the baseline.
There is one warmup and two measured reads per consumer/projection in each fresh
JVM. These are two within-job samples, not two independent whole-experiment runs.
Default cache decoding remains CPU even for GPU-enabled downstream execution.

All times are seconds. Combined columns are computed sums of measured phases,
not independently timed write-plus-read jobs. Encoded bytes, logical capacity,
Spark storage estimates and whole-job RSS are distinct measurements.

## Full reads: baseline to 2 GiB pool

| MiB/task | Format | Population 0 / 2 GiB | CPU read 0 / 2 GiB | GPU-enabled read 0 / 2 GiB | GPU read speedup | Population + GPU read, 2 GiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 256 | Default | 23.012 / 19.532 | 1.524 / 1.487 | 1.170 / 0.573 | 2.04x | 20.105 |
| 256 | PCBS | 2.503 / 1.531 | 5.577 / 5.424 | 0.455 / 0.262 | 1.73x | 1.794 |
| 256 | Vortex | 6.919 / 5.895 | 2.676 / 2.906 | 13.853 / 1.169 | 11.85x | 7.064 |
| 512 | Default | 42.647 / 38.719 | 2.780 / 2.765 | 2.007 / 1.070 | 1.88x | 39.789 |
| 512 | PCBS | 3.381 / 1.871 | 10.526 / 10.340 | 0.523 / 0.458 | 1.14x | 2.330 |
| 512 | Vortex | 12.008 / 9.967 | 5.745 / 5.430 | 29.643 / 2.416 | 12.27x | 12.383 |
| 1024 | Default | 85.826 / 79.210 | 5.381 / 5.562 | 3.980 / 2.624 | 1.52x | 81.834 |
| 1024 | PCBS | 5.760 / 2.788 | 20.291 / 20.289 | 1.043 / 0.787 | 1.33x | 3.576 |
| 1024 | Vortex | 20.789 / 18.753 | 11.998 / 10.736 | 71.253 / 4.060 | 17.55x | 22.813 |
| 2048 | Default | 321.273 / FAIL population | 10.095 / — | 7.646 / — | — | — |

## ID-only reads: baseline to 2 GiB pool

| MiB/task | Format | CPU read 0 / 2 GiB | GPU-enabled read 0 / 2 GiB |
| --- | --- | ---: | ---: |
| 256 | Default | 0.080 / 0.103 | 0.216 / 0.133 |
| 256 | PCBS | 4.334 / 4.148 | 0.249 / 0.144 |
| 256 | Vortex | 0.321 / 0.347 | 0.893 / 0.228 |
| 512 | Default | 0.127 / 0.128 | 0.217 / 0.174 |
| 512 | PCBS | 7.964 / 7.786 | 0.256 / 0.220 |
| 512 | Vortex | 0.733 / 0.649 | 1.678 / 0.379 |
| 1024 | Default | 0.167 / 0.178 | 0.396 / 0.351 |
| 1024 | PCBS | 15.734 / 15.692 | 0.523 / 0.352 |
| 1024 | Vortex | 0.948 / 0.973 | 3.399 / 0.764 |
| 2048 | Default | 0.272 / — | 0.538 / — |

## Size, retained data and memory

| MiB/task | Format | Logical bytes resident | Encoded bytes 0 / 2 GiB | Storage estimate, 2 GiB | Peak RSS bytes 0 / 2 GiB | Pool exhaustion log lines |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 256 | Default | 1,073,741,824 | 435,890,015 / 435,890,015 | 437,697,968 | 5,122,056,192 / 4,938,690,560 | 33 |
| 256 | PCBS | 1,073,741,824 | 513,454,990 / 513,454,580 | 513,461,272 | 3,980,845,056 / 5,236,621,312 | 0 |
| 256 | Vortex | 1,073,741,824 | 489,897,376 / 489,897,376 | 489,903,072 | 5,558,865,920 / 7,911,780,352 | 0 |
| 512 | Default | 2,147,483,648 | 871,780,099 / 871,780,099 | 875,395,032 | 7,943,970,816 / 7,443,652,608 | 37 |
| 512 | PCBS | 2,147,483,648 | 1,026,907,683 / 1,026,899,551 | 1,026,912,888 | 3,675,885,568 / 8,676,737,024 | 0 |
| 512 | Vortex | 2,147,483,648 | 979,946,776 / 979,946,776 | 979,958,104 | 7,785,480,192 / 9,308,450,816 | 0 |
| 1024 | Default | 4,294,967,296 | 1,743,560,044 / 1,743,560,044 | 1,662,574,592 | 15,233,388,544 / 16,868,012,032 | 39 |
| 1024 | PCBS | 4,294,967,296 | 2,053,799,024 / 2,053,797,644 | 2,053,824,352 | 10,749,976,576 / 11,135,090,688 | 0 |
| 1024 | Vortex | 4,294,967,296 | 1,959,914,440 / 1,959,914,440 | 1,959,937,032 | 9,508,372,480 / 12,832,215,040 | 0 |
| 2048 | Default | — | 3,487,118,351 / — | — | 19,117,694,976 / 3,424,374,784 | 5 |

All successful cases retained every logical row in memory with disk bytes zero at
all recorded snapshots. Residency does not prove no transient spilling occurred.
Storage bytes use Spark SizeEstimator accounting; encoded bytes are a post-timing
payload audit. Small PCBS payload-size differences are measured output variations,
not evidence that the data or compression-policy contract changed.
Pool exhaustion log counts record fallback attempts, not query failures or OOM
of the machine. A reserved pinned pool does not cap all native fallback allocations.
Whole-job RSS includes startup/reference actions, heap garbage and the audit;
it does not isolate per-phase live working memory.

## First and warmed reads

| MiB/task | Format | Requested consumer / cache decoder | Projection | First read | Warm median | Population + first | Population + warm |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: |
| 256 | Default | cpu / cpu | full | 1.679 | 1.487 | 21.212 | 21.020 |
| 256 | Default | cpu / cpu | id | 0.191 | 0.103 | 19.723 | 19.635 |
| 256 | Default | gpu / cpu | full | 0.859 | 0.573 | 20.392 | 20.105 |
| 256 | Default | gpu / cpu | id | 0.137 | 0.133 | 19.670 | 19.666 |
| 256 | PCBS | cpu / cpu | full | 6.218 | 5.424 | 7.749 | 6.955 |
| 256 | PCBS | cpu / cpu | id | 4.195 | 4.148 | 5.726 | 5.679 |
| 256 | PCBS | gpu / gpu | full | 0.460 | 0.262 | 1.991 | 1.794 |
| 256 | PCBS | gpu / gpu | id | 0.164 | 0.144 | 1.695 | 1.675 |
| 256 | Vortex | cpu / cpu | full | 3.013 | 2.906 | 8.908 | 8.801 |
| 256 | Vortex | cpu / cpu | id | 0.306 | 0.347 | 6.201 | 6.242 |
| 256 | Vortex | gpu / gpu | full | 1.340 | 1.169 | 7.235 | 7.064 |
| 256 | Vortex | gpu / gpu | id | 0.267 | 0.228 | 6.161 | 6.123 |
| 512 | Default | cpu / cpu | full | 3.002 | 2.765 | 41.722 | 41.484 |
| 512 | Default | cpu / cpu | id | 0.219 | 0.128 | 38.938 | 38.848 |
| 512 | Default | gpu / cpu | full | 1.412 | 1.070 | 40.131 | 39.789 |
| 512 | Default | gpu / cpu | id | 0.208 | 0.174 | 38.927 | 38.893 |
| 512 | PCBS | cpu / cpu | full | 11.625 | 10.340 | 13.496 | 12.211 |
| 512 | PCBS | cpu / cpu | id | 7.811 | 7.786 | 9.682 | 9.658 |
| 512 | PCBS | gpu / gpu | full | 0.603 | 0.458 | 2.474 | 2.330 |
| 512 | PCBS | gpu / gpu | id | 0.252 | 0.220 | 2.123 | 2.091 |
| 512 | Vortex | cpu / cpu | full | 5.926 | 5.430 | 15.893 | 15.397 |
| 512 | Vortex | cpu / cpu | id | 0.383 | 0.649 | 10.350 | 10.615 |
| 512 | Vortex | gpu / gpu | full | 3.151 | 2.416 | 13.118 | 12.383 |
| 512 | Vortex | gpu / gpu | id | 0.368 | 0.379 | 10.335 | 10.346 |
| 1024 | Default | cpu / cpu | full | 5.795 | 5.562 | 85.005 | 84.772 |
| 1024 | Default | cpu / cpu | id | 0.273 | 0.178 | 79.483 | 79.388 |
| 1024 | Default | gpu / cpu | full | 2.734 | 2.624 | 81.944 | 81.834 |
| 1024 | Default | gpu / cpu | id | 0.199 | 0.351 | 79.409 | 79.561 |
| 1024 | PCBS | cpu / cpu | full | 21.705 | 20.289 | 24.493 | 23.078 |
| 1024 | PCBS | cpu / cpu | id | 15.669 | 15.692 | 18.457 | 18.480 |
| 1024 | PCBS | gpu / gpu | full | 0.828 | 0.787 | 3.616 | 3.576 |
| 1024 | PCBS | gpu / gpu | id | 0.345 | 0.352 | 3.134 | 3.140 |
| 1024 | Vortex | cpu / cpu | full | 11.929 | 10.736 | 30.681 | 29.489 |
| 1024 | Vortex | cpu / cpu | id | 1.119 | 0.973 | 19.871 | 19.726 |
| 1024 | Vortex | gpu / gpu | full | 4.412 | 4.060 | 23.165 | 22.813 |
| 1024 | Vortex | gpu / gpu | id | 0.497 | 0.764 | 19.250 | 19.516 |
| 2048 | Default | cpu / unmeasured | full | — | — | — | — |
| 2048 | Default | cpu / unmeasured | id | — | — | — | — |
| 2048 | Default | gpu / unmeasured | full | — | — | — | — |
| 2048 | Default | gpu / unmeasured | id | — | — | — | — |

## Failed population: Default at 2 GiB per task

The 8 GiB-total Default GPU-populated case failed with CpuRetryOOM during
GPU-to-row host export before cache population completed. The stack is
AcceleratedColumnarToRowIterator.setupBatchAndClose -> ColumnView.copyToHost
-> HostAlloc -> preCpuAlloc. The requested contiguous buffer was 2,147,475,456
bytes, while tracked host allocations were about 201,326,344 bytes.
The non-pinned allowance was 2,018 MiB and the 2 GiB pinned pool could not
satisfy the request with its existing allocations. The job exited; no budget
was increased or retry launched. Peak sampled RSS was 3,424,374,784 bytes,
well below the 32 GiB guard. This is a managed-allocation failure, with no
completed population, read, payload or residency result for that case.
Its partial-work RSS is not comparable to successful full-job memory usage.
CSV phase rows explicitly say FAIL_POPULATION and leave unmeasured fields empty.
The failure occurs in the export feeding Default, before its CPU cache encoding;
it does not establish a limit on CPU-only Default caching or machine RAM.
The original zero-pool case succeeded under a different host-budget partition.
Changing the pinned reservation can improve transfer overhead while reducing
the space available for large non-pinned buffers. This configuration boundary
must remain visible when comparing formats.


[Machine-readable results](vortex-cache-pinned2g-results.csv) include every sample,
baseline value, size and memory measurement. Raw outputs are under
/data/tmp/cache-pinned2g-16g/{256,512,1024,2048}/{default,pcbs,vortex}-gpu.
Every accepted case has a lead-independent-verification.json tied to raw-result
and baseline SHA256s. Exact aggregates, observed scan plans, full dependency
provenance, settings, storage inventory and audits were independently checked.
All project operations used MCP docker_exec. No supported integration or
out-of-box default acceptance follows from changing this experimental setting.
