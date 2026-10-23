# Consolidated cache experiment results

Status: READY FOR REVIEW; historical data consolidation only. No new benchmark,
configuration, batch-size selection or production change is part of this update.
Existing released contracts are unchanged; these research fixtures are unreleased.
All project operations use MCP docker_exec tools.

This single table consolidates 74 historical performance settings/groups,
including planned but unexecuted sizes, plus the unmeasured current 256 MiB
PCBS/codec comparison. That blank row is a comparison gap, not a scheduled run. It includes all early reset values rather than selecting
only their endpoints. It is a history of experiments, not a controlled comparison
across every row or a recommendation for a particular batch size.

- **Data** is logical values/payload GiB; old large tests are about 95.52 GiB,
  new ones exactly 96 GiB. The 16-slot shadow cases double the total to 191.04 GiB.
- **Heap/slots/GPU** means configured heap GiB, task slots, and GPU concurrency.
  Automatic GPU admission is marked auto; CPU and synthetic tests have no GPU.
- **Global/record MiB** distinguishes the GPU target from the cache-record
  budget. In PCBS-only splitting, global stays 1024 and record is 16 or 32.
  Most record entries are nominal targets/caps; actual sizes can differ. Early
  256 MiB global-target records were about 244 MiB. Current CPU uses default
  10,000-row batching, maximum encoded size 1.22 MiB; early CPU4 is the old
  fixture's batching, not that comparator.
- **Fill/read** are ranges across fresh-JVM observations; reads are ranges of
  per-JVM measured-read medians. A single value means only one observation or
  no visible range at this precision. `raw` reloads/deserializes full cached
  records and inspects metadata without Parquet decoding; `val` validates
  synthetic bytes. Other reads sum real SQL columns.
- **Peak heap** is maximum observed occupancy, including garbage, across the
  available phases/JVMs. Historical values are sampled; current real-codec values
  also check pre-GC logs. `R` means read-only peak (fill peak unavailable); `F`
  means completed fill only before a failed read; `*` means partial observation
  before OOM, not a successful workload's memory requirement. Missing data stays —.
- **Storage/workload:** Small uses 3.815 GiB real data, fraction 0.03 to force disk,
  GPU concurrency 1 and a 2g-initial/8g-max GPU pool; explicit GC occurs outside
  scans/between fill partitions. Old large PCBS uses MEMORY_AND_DISK, fraction0.6,
  sequential partition fills including generation, GPU15/host40/pinned2 GiB.
  Synthetic uses DISK_ONLY, no GPU/Parquet, 43 ParallelGC workers; initial256
  uses24 GiB while fixed256 uses96 GiB. Shadow uses local metadata tokens plus
  spillable RAPIDS native payloads, ignores deletes and does not prove a production
  lifecycle. GPU-hash shadow rows used about 40–44 GiB process RSS despite
  their smaller heaps. New real-codec uses the identical canonical files, parallel fill,
  MEMORY_AND_DISK, four ParallelGC workers and GPU15/host40/pinned2 GiB.
  Except the early small sweep, memory fraction remains0.6. No timing ratios are
  interpreted as isolated parameter costs across these different profiles.

[Download the same table as CSV](pcbs-experiment-results.csv). The CSV retains
full precision, source paths, storage, memory fraction, metric scope and notes.
The extraction scripts and source snapshots are retained under
`/data/pcbs-oom/consolidated-tradeoffs/`.

| Experiment | Data GiB | Heap/slots/GPU | Global/record MiB | Reset/unroll | Fill s | Read s | Peak heap GiB | Outcome |
|---|---:|---|---|---|---:|---:|---:|---|
| Small CPU | 3.81 | 8/8/— | —/4 | 1/16 | 17.01–17.61 | 0.95–0.98 | 2.39 | 3/3 pass |
| Small CPU | 3.81 | 8/8/— | —/4 | 2/16 | 16.73 | 0.95 | 2.52 | 1/1 pass |
| Small CPU | 3.81 | 8/8/— | —/4 | 4/16 | 17.70 | 0.99 | 2.54 | 1/1 pass |
| Small CPU | 3.81 | 8/8/— | —/4 | 8/16 | 16.23 | 0.98 | 2.6 | 1/1 pass |
| Small CPU | 3.81 | 8/8/— | —/4 | 16/16 | 17.09 | 1.05 | 3.13 | 1/1 pass |
| Small CPU | 3.81 | 8/8/— | —/4 | 100/16 | 18.19–18.75 | 1.26–1.31 | 4 | 3/3 pass |
| Small CPU | 3.81 | 3/8/— | —/4 | 1/16 | 15.14–15.48 | 0.59–0.67 raw | 1.59 | 3/3 pass |
| Small CPU | 3.81 | 3/8/— | —/4 | 2/16 | 15.02–15.39 | 0.63–0.66 raw | 1.55 | 3/3 pass |
| Small CPU | 3.81 | 3/8/— | —/4 | 4/16 | 15.39–15.58 | 0.66–0.68 raw | 1.7 | 3/3 pass |
| Small CPU | 3.81 | 3/8/— | —/4 | 8/16 | 15.28–15.69 | 0.90–0.93 raw | 2.69 | 3/3 pass |
| Small CPU | 3.81 | 3/8/— | —/4 | 16/16 | 15.20–15.43 | 0.91–0.94 raw | 2.73 | 3/3 pass |
| Small CPU | 3.81 | 3/8/— | —/4 | 100/16 | 16.27–16.70 | — | 2.98* | 0/3 pass; OOM read |
| Small PCBS/Java | 3.81 | 8/8/1 | 64/64 | 1/16 | 16.84–17.24 | 5.15–5.45 | 4 | 3/3 pass |
| Small PCBS/Java | 3.81 | 8/8/1 | 64/64 | 2/16 | 16.76 | 5.34 | 4.03 | 1/1 pass |
| Small PCBS/Java | 3.81 | 8/8/1 | 64/64 | 4/16 | 16.89 | 5.43 | 4 | 1/1 pass |
| Small PCBS/Java | 3.81 | 8/8/1 | 64/64 | 8/16 | 16.54 | 5.14 | 4.01 | 1/1 pass |
| Small PCBS/Java | 3.81 | 8/8/1 | 64/64 | 16/16 | 16.60 | 5.15 | 4 | 1/1 pass |
| Small PCBS/Java | 3.81 | 8/8/1 | 64/64 | 100/16 | 16.12–17.12 | 5.10–5.40 | 4.02 | 3/3 pass |
| Small PCBS/Java | 3.81 | 3/8/1 | 64/64 | 1/16 | 15.47–16.74 | 0.83–0.84 raw | 2.85 | 3/3 pass |
| Small PCBS/Java | 3.81 | 3/8/1 | 64/64 | 2/16 | 14.42–17.16 | 0.81–0.87 raw | 2.74 | 3/3 pass |
| Small PCBS/Java | 3.81 | 3/8/1 | 64/64 | 4/16 | 15.02–16.33 | 0.83–0.85 raw | 2.93 | 3/3 pass |
| Small PCBS/Java | 3.81 | 3/8/1 | 64/64 | 8/16 | 16.60–17.56 | — | 2.92* | 0/3 pass; OOM read |
| Small PCBS/Java | 3.81 | 3/8/1 | 64/64 | 16/16 | 16.25–16.51 | — | 2.92* | 0/3 pass; OOM read |
| Small PCBS/Java | 3.81 | 3/8/1 | 64/64 | 100/16 | 16.07–16.88 | — | 2.85* | 0/3 pass; OOM read |
| Small PCBS/Java | 3.81 | 8/8/1 | 256/~244 actual | 1/16 | 15.38–16.19 | 3.25–3.30 | 4.02 | 3/3 pass |
| Small PCBS/Java | 3.81 | 8/8/1 | 256/~244 actual | 2/16 | 15.96 | 3.28 | 4 | 1/1 pass |
| Small PCBS/Java | 3.81 | 8/8/1 | 256/~244 actual | 4/16 | 14.85 | 3.27 | 4.02 | 1/1 pass |
| Small PCBS/Java | 3.81 | 8/8/1 | 256/~244 actual | 8/16 | 18.54 | 3.26 | 4.01 | 1/1 pass |
| Small PCBS/Java | 3.81 | 8/8/1 | 256/~244 actual | 16/16 | 16.50 | 3.45 | 3.99 | 1/1 pass |
| Small PCBS/Java | 3.81 | 8/8/1 | 256/~244 actual | 100/16 | 15.05–15.81 | 3.24–3.26 | 4.02 | 3/3 pass |
| Small PCBS/Java | 3.81 | 3/8/1 | 256/~244 actual | 1/16 | 15.39–16.04 | — | 2.7* | 0/3 pass; OOM read |
| Small PCBS/Java | 3.81 | 3/8/1 | 256/~244 actual | 2/16 | 16.01–16.97 | — | 2.7* | 0/3 pass; OOM read |
| Small PCBS/Java | 3.81 | 3/8/1 | 256/~244 actual | 4/16 | 15.65–16.64 | — | 2.7* | 0/3 pass; OOM read |
| Small PCBS/Java | 3.81 | 3/8/1 | 256/~244 actual | 8/16 | 14.95–15.78 | — | 2.7* | 0/3 pass; OOM read |
| Small PCBS/Java | 3.81 | 3/8/1 | 256/~244 actual | 16/16 | 15.92–16.62 | — | 2.71* | 0/3 pass; OOM read |
| Small PCBS/Java | 3.81 | 3/8/1 | 256/~244 actual | 100/16 | 14.76–15.91 | — | 2.7* | 0/3 pass; OOM read |
| Large PCBS/default | 95.52 | 48/8/2 | 1024/1024 | 100/16 | 255.19 | — | 41.17 F | 0/1 pass; OOM read |
| Large PCBS/checks | 95.52 | 48/8/2 | 1024/1024 | 1/1 | 247.45–251.25 | 37.38–38.71 | 44.89 R | 3/3 pass |
| Large PCBS/checks | 95.52 | 48/8/2 | 1024/1024 | 2/1 | 250.39–252.48 | 36.40–38.59 | 45.21 R | 3/3 pass |
| Large PCBS/checks | 95.52 | 48/8/2 | 1024/1024 | 4/1 | 253.71 | — | — | 0/1 pass; OOM read |
| Large PCBS/checks | 95.52 | 48/8/2 | 1024/1024 | 100/1 | 255.06 | — | — | 0/1 pass; OOM read |
| Large PCBS/checks | 95.52 | 48/8/2 | 1024/1024 | 1/2 | 249.66–250.29 | 39.63–42.51 | 44.95 R | 3/3 pass |
| Large PCBS/checks | 95.52 | 48/8/2 | 1024/1024 | 1/4 | 250.65 | — | — | 0/1 pass; OOM read |
| Large PCBS/checks | 95.52 | 48/8/2 | 1024/1024 | 1/8 | 257.59 | — | — | 0/1 pass; OOM read |
| Large PCBS/checks | 95.52 | 48/8/2 | 1024/1024 | 1/16 | 254.44 | — | — | 0/1 pass; OOM read |
| Large PCBS/global | 95.52 | 48/8/2 | 16/16 | 100/16 | 270.67–271.58 | 42.76–45.47 | 41.51 | 3/3 pass |
| Large PCBS/global | 95.52 | 48/8/2 | 32/32 | 100/16 | 254.37–264.05 | 41.76–43.49 | 42.45 | 3/3 pass |
| Large PCBS/global | 95.52 | 48/8/2 | 64/64 | 100/16 | 259.47 | — | 38.99 F | 0/1 pass; OOM read |
| Large PCBS/global | 95.52 | 48/8/2 | 128/128 | 100/16 | — | — | — | not run |
| Large PCBS/global | 95.52 | 48/8/2 | 256/256 | 100/16 | — | — | — | not run |
| Large PCBS/global | 95.52 | 48/8/2 | 512/512 | 100/16 | 256.59 | — | 41.71 F | 0/1 pass; OOM read |
| Large PCBS/split | 95.52 | 48/8/2 | 1024/16 | 100/16 | 273.55–276.41 | 44.62–45.46 | 42.73 | 3/3 pass |
| Large PCBS/split | 95.52 | 48/8/2 | 1024/32 | 100/16 | 262.62–263.16 | 41.91–43.35 | 42.08 | 3/3 pass |
| Shadow/array copies | 95.52 | 48/8/2 | 1024/1024 | 100/16 | 246.40 | 74.09 | 35.15 | 1/1 pass |
| Shadow/native, rand | 95.52 | 48/8/2 | 1024/1024 | 100/16 | 214.98 | 20.63 | 16.02 | 1/1 pass |
| Shadow/native, rand | 95.52 | 16/8/2 | 1024/1024 | 100/16 | 215.20 | 20.36 | 5.38 | 1/1 pass |
| Shadow/GPU hash | 191.04 | 48/16/2 | 1024/1024 | 100/16 | 300.32 | 95.00 | 7.74 | 1/1 pass |
| Shadow/GPU hash | 95.52 | 48/8/auto | 1024/1024 | 100/16 | 126.82 | 13.01 | 4.54 | 1/1 pass |
| Shadow/GPU hash | 95.52 | 48/8/2 | 1024/1024 | 100/16 | 125.24 | 20.49 | 4.75 | 1/1 pass |
| Shadow/GPU hash | 95.52 | 16/8/2 | 1024/1024 | 100/16 | 127.86 | 20.53 | 2.58 | 1/1 pass |
| Shadow/GPU hash | 95.52 | 0.5/8/2 | 1024/1024 | 100/16 | 126.49 | 20.45 | 0.19 | 1/1 pass |
| Shadow/GPU hash + projection | 95.52 | 48/8/2 | 2048/2048 | 100/16 | 161.98 | 34.36 | 5.34 | 1/1 pass |
| Shadow/GPU hash + projection | 191.04 | 0.5/16/auto | 2048/2048 | 100/16 | 358.88 | 106.23 | 0.23 | 1/1 pass |
| Synthetic initial/codec | 24 | 48/8/— | —/256 | 100/16 | 14.14–14.27 | 5.05–5.07 val | 22.84 | 2/2 pass |
| Synthetic initial/Java | 24 | 48/8/— | —/256 | 100/16 | 15.52–15.91 | 4.72–4.93 val | 42.75 | 2/2 pass |
| Synthetic initial/codec | 96 | 48/8/— | —/1024 | 100/16 | 81.40–86.90 | 25.06–34.28 val | 44.46 | 2/2 pass |
| Synthetic initial/Java | 96 | 48/8/— | —/1024 | 100/16 | — | — | 31.11* | 0/2 pass; OOM fill |
| Synthetic fixed/codec | 96 | 48/8/— | —/256 | 100/16 | 73.02–73.28 | 18.14–18.19 val | 28.65 | 2/2 pass |
| Synthetic fixed/Java | 96 | 48/8/— | —/256 | 100/16 | — | — | 38.06* | 0/2 pass; OOM fill |
| Synthetic fixed/codec | 96 | 48/8/— | —/1024 | 100/16 | 77.94–80.61 | 24.34–25.93 val | 45.44 | 2/2 pass |
| Synthetic fixed/Java | 96 | 48/8/— | —/1024 | 100/16 | — | — | 31.11* | 0/2 pass; OOM fill |
| Current CPU/default | 96 | 48/8/— | —/1.22 actual | 100/16 | 99.30–114.32 | 18.64–19.20 | 42.73 | 2/2 pass |
| Current PCBS/codec split | 96 | 48/8/2 | 1024/32 | 100/16 | 110.20–138.37 | 26.84–28.20 | 42.01 | 2/2 pass |
| Current PCBS/codec split+coalesce | 96 | 48/8/2 | 1024/32 | 100/16 | 113.52–138.55 | 24.96–26.96 | 42.1 | 2/2 pass |
| Current PCBS/codec split (256 gap) | 96 | 48/8/2 | 1024/256 | 100/16 | — | — | — | not run |
