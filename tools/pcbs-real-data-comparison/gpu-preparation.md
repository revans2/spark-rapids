# GPU PCBS research artifacts and small correctness gates

These are disposable Spark 3.5.5 / Scala 2.12 research overlays. They preserve
the original PCBS byte-array representation and original global 1 GiB target.
They do not contain the shadow/native cache prototype. Existing released
sources, installed Spark and baseline artifacts are unchanged.

## Preparation

Run through MCP docker_exec, from the repository root:

```
python3 tools/pcbs-real-data-comparison/prepare_gpu_overlay.py \
  --spark-home /home/roberte/src/spark_3.5.5 \
  --baseline-plugin /home/roberte/src/rapids-plugin-4-spark/dist/target/rapids-4-spark_2.12-26.12.0-SNAPSHOT-cuda12.jar \
  --spark-codec-overlay /data/pcbs-oom/blockmanager-codec-spike/root-acceptance355/blockmanager-codec-spike.jar \
  --output-dir /data/pcbs-oom/pcbs-real-data-comparison/gpu-overlay-build2
```

The existing output directory is immutable evidence; use a new directory for
another preparation. Frozen sources, all compiler commands/logs, baseline and
runtime hashes, output hashes and replacement class lists are in
`gpu-overlay-build2/manifest.json`. Both variants compiled successfully.

The fixed baseline is revision
`3cdf711ded8e8b72d0f3ee92866401df5d9d2a06`, artifact SHA-256
`f3bc760d51c1c8596ac9108242f2c9366f570d7b5612a1239aba88e73ee25af5`.

- `pcbs-record-codec.jar`: public-wrapper subclass plus per-cache codec marker.
  Each stream frames rows, length and the existing Parquet byte array directly.
  Decode reconstructs the actual shim-loaded PCBS record. There is no Java
  object-stream bulk path, array clone in the codec, native store or new payload
  lifecycle. Stream ownership is idempotent across caller, EOF/error and task
  completion. Close failures and cancellation are not separately tested here.
- `cap32-only.jar`: private PCBS GPU split budget of 32 MiB minus the existing
  0.5% metadata allowance; original global-budget and 2 GiB safety limits remain.
- `cap32-combine.jar`: identical split policy, plus common GPU cache scan
  `coalesceAfter=true`, invoking the normal transition rule and standard
  spillable/retry-capable `GpuCoalesceBatches` at the global target.
- Both artifacts observe actual decoded device bytes, and actual combined
  device bytes only when the standard coalescer directly consumes a GPU cache
  scan. Observation uses `closeOnExcept` and returns ownership unchanged.

The generated PCBS, scan and coalescer sources are common sources without
version-selection annotations; this research recompiles them only for 3.5.5.
It does not establish other Spark-version compatibility or modify production
shims. The 32 MiB budget estimates input slices, not a universal hard bound on
Parquet encoded bytes. Actual fixed-schema record sizes remain an acceptance
check.

Place the Spark codec overlay, fixture and chosen plugin on both driver and
executor extraClassPath, in that order. Supplying the fixture on the system
classpath and its PCBS superclass only through child-loader `--jars` fails.
Use exactly the manifest-published artifacts/hashes.

## Small correctness evidence

`gpu-small-gate.scala` is a separate local[2], 4 GiB, DISK_ONLY correctness
fixture. Its 1,048,576 rows have 16 known Double columns (128 MiB logical data).
It forces actual per-cache codec disk reads and explicitly roundtrips every
real PCBS record, asserting actual shim class identity, exact payload bytes,
rows, encoded size, framing and EOF behavior. It verifies all 16 exact sums
and count on the GPU. It is not the large MEMORY_AND_DISK performance workload.

The scratch runner and exact compilation/submission commands are retained at:

```
/data/pcbs-oom/pcbs-real-data-comparison/run_small_gate.py
/data/pcbs-oom/pcbs-real-data-comparison/gpu-small-gate2/
```

Both fresh-JVM modes passed with normal exit, required success markers,
application-end events and no failed tasks:

| Observation | Split only | Split plus standard coalesce |
|---|---:|---:|
| Actual PCBS records | 5 | 5 |
| Encoded payload bytes | 66,710,566 | 66,710,566 |
| Maximum encoded record bytes | 16,674,974 | 16,674,974 |
| Full-read decoded batches | 5 | 5 |
| Maximum decoded device bytes | 33,386,624 | 33,386,624 |
| Full-read decoded device bytes | 134,217,728 | 134,217,728 |
| Observed combined batches | 0 | 1 |
| Maximum combined device bytes | 0 | 134,217,728 |

The known-value input compresses; these encoded sizes are not estimates of the
future canonical hash-data workload. Full aggregate plans show
`GpuInMemoryTableScan -> GpuHashAggregate` for split-only, and
`GpuInMemoryTableScan -> GpuCoalesceBatches targetsize(1073741824) -> GpuHashAggregate`
for combine. All 16 sums and count execute in `GpuHashAggregate`.

Counters are local-JVM cumulative observations; retries can increase them.
They are not physical I/O throughput. Count-only scans can count decoded row
metadata with zero device bytes. Use phase deltas for sums/counts; maxima and
minima are cumulative. No per-record RPC or log is used in timed workloads.

Preparation build1 failed before Scala compilation because sandbox Python 3.6
does not support subprocess `text=True`; build2 uses `universal_newlines=True`.
Small gate1 failed before population on the superclass classloader issue
above; gate2 added the chosen plugin to extraClassPath. Both original failures
are retained. Neither repair changes PCBS source or weakens correctness checks.

## Remaining gates

The small gates alone do not prove large-data behavior. The later full shared-input
96 GiB MEMORY_AND_DISK comparison and independent raw audits passed; measured
coalesced batches reached 991.27 MiB. See the
[comparison report](../../docs/dev/pcbs-real-data-comparison.md) for maximum heap,
fill/read wall times, paired runs and limits. Research is READY FOR REVIEW.
The optional unsplit codec reference requires explicit artifact preparation
with `--include-baseline`; no 256 MiB sweep is included.
