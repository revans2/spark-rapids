# History-backed planning heuristics

Planning decisions that learn from earlier runs. A heuristic reads history when a query plans
and records what the query did when it ends. If history is off, missing, stale or unusable, the
static decision is used unchanged; the two are never blended. The one heuristic today sizes scan
splits for every GPU file reader: Iceberg, v1 file sources (catalog tables and path reads,
including Delta), Hive text tables, and DSv2 file scans (Parquet, ORC, CSV, JSON, Avro).

## Enabling

| Key | Default | Meaning |
|---|---|---|
| `spark.rapids.sql.history.metrics.provider` | `none` | any other value enables history-backed planning |
| `spark.rapids.sql.history.planningTimeoutMillis` | 100 | budget for one history lookup while planning |
| `spark.rapids.sql.history.declareTimeoutMillis` | 5000 | budget for declaring each metric family to the store |
| `spark.rapids.sql.history.scanSplit.enabled` | `true` | `false` turns off only the scan split heuristic |

Planning only uses history once a provider store is actually installed. This package talks to
the history metrics API (`MetricStores.current()`) only; the driver plugin's
`HistoryMetricsManager` owns the provider.

## How scan splits are sized

- **Measure**: when a query ends, each scan records its decode-expansion ratio, GPU output batch
  bytes / on-disk bytes. It is recorded only if every planned split was read to the end; a split
  read more than once (task or stage retries) counts once.
- **Key**: the table name (Iceberg `Table.name()`, catalog or Hive identifier), else `path:` + the
  sorted root paths (user info and trailing `/` dropped). Keys over 253 UTF-8 bytes become a
  readable prefix + `#` + 16 hex of their SHA-256. Reads of an explicit file list are not tracked.
- **Decide**: the next scan of that table reads the latest ratio (at most 7 days old) and sets
  `split = targetBatchBytes / ratio`, clamped to `[64 MiB, min(4 GiB, listedBytes / minPartitionNum)]`.
- **minPartitionNum**: `spark.sql.files.minPartitionNum`, `spark.sql.leafNodeDefaultParallelism`,
  `spark.default.parallelism`, executor instances x cores, then registered cores.
- **Iceberg precedence**: DataFrame `split-size` option > learned split >
  `spark.rapids.iceberg.*-setting.*.read-split-target-size` > table properties > Iceberg default.
  On Iceberg `listedBytes` is the unpruned table size, so the cap is loose for small slices.

| Step | Where |
|---|---|
| decide (Iceberg) | `RapidsSparkTable.newScanBuilder`, via `IcebergSplitAdvisor` |
| observe (Iceberg) | `GpuSparkScan.toBatch` |
| decide / observe (v1) | `GpuFileSourceScanExec.createNonBucketedReadRDD` / `internalDoExecuteColumnar` |
| decide / observe (Hive text) | `GpuHiveTableScanExec.createReadRDDForDirectories` / `internalDoExecuteColumnar` |
| decide / observe (DSv2) | `HistorySizedFileScan.planInputPartitions` / `observed(createReaderFactory)` |

v1 and Hive register once per execution; DSv2 once per reader factory.

Driver log lines, kept stable for tooling:

```
scan.split: table=<t> ratio=<r> targetBatch=<b> rawSplit=<s>
scan.split: table=<t> listed=<l> -> split=<n> bytes
scan.split: table=<t> listed=<l> -> no history, iceberg default | spark default
scan.split: table=<t> not recorded: <reason>
```

Store errors are logged once per JVM per reason and the decision falls back to static.

## Adding a heuristic

Code layers: `HistoryMetric` (family contract) -> `MetricHistory` (store access, `HistoryPolicy`)
-> `HistoryHeuristic` (decide/observe cycle, lifecycle) -> `ScanSplitHeuristic`.

1. Register a family id and name in the API's `HistoryMetricCatalog`.
2. Define it: `object X extends HistoryMetric`.
3. Extend `HistoryHeuristic` (key, static decision, formula, bounds, observation) as a singleton
   over `MetricStores.current()`, and add it and its `spark.rapids.sql.history.<name>.enabled`
   switch to `HistoryHeuristics`.
4. Call `decide` and `register` from the planning site.
