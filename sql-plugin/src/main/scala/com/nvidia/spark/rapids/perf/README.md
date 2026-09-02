# History-backed planning heuristics

Planning decisions that learn from what previous runs actually did.

A heuristic reads one or more measured quantities at planning time, decides, and records what the
query really did once it ends. If history is missing, stale, or unreadable the static decision
stands whole - a learned value is never blended with it.

Today there is one heuristic: scan splits sized from decode expansion.

## Where this sits

The history metrics modules decide how a measurement is stored. This package decides what is worth
measuring and what to do about it. Nothing here knows the provider is local; nothing in the
provider knows what a scan is.

```
RapidsConf          historyPath, history.maxAgeDays, history.planningTimeoutMillis
     |  resolved once at init
     v
HistoryPolicy       every value the provider requires, in one place
     |
     v
HistoryOwner        open / install / drain / save / shutdown
     |              heuristic-agnostic, one per application
     |  MetricStore
     +--------------------------+--------------------------+
     v                                                     v
HistoryMetric (abstract)                        HistoryObservations
  declare / record / latest / hashKey  [final]    exec-id registry, drains
     ^                                            on SQLExecutionEnd
     |  extends                                         ^
ScanExpansionRatio                                       | callbacks
     ^                                                   |
     |  read+written by                                  |
HistoryHeuristic (abstract)   decide / register [final]  |
     ^                                                   |
     |  extends                                          |
ScanSplitHeuristic --------------------------------------+
```

The only file this package needs outside itself is `HistoryMetricCatalog`, which belongs to the
history metrics API. Every measured quantity needs an entry there, so adding a heuristic is not
purely additive on this side.

## Files

| File | Holds |
|---|---|
| `HistoryPolicy.scala` | every value the provider requires, resolved in one place |
| `HistoryOwner.scala` | provider lifecycle: open, install, persist, close |
| `HistoryMetric.scala` | one metric family: declare, record, read most-recent |
| `HistoryHeuristic.scala` | the decide/observe cycle, and the execution-id registry |
| `ScanSplitHeuristic.scala` | the scan heuristic: family, context, arithmetic |

`HistoryOwner`, `HistoryMetric`, and `HistoryPolicy` know nothing about scans and never read a
Spark conf - they receive resolved values, so they can be driven from a plain harness with a fake
store.

## How a decision is made

```
planning site
  |  ctx = whatever this heuristic needs
  v
HistoryHeuristic.decide(ctx, now)                       [final]
  |
  +-- heuristic disabled ------------------------------> staticDecision(ctx)
  |
  observed = {}
  for each m in metrics:
      m.latest(store, keyFor(m, ctx), now, maxAge, timeout)
        Some(v) -> observed += m -> v
        None    -> omit
  |
  +-- sufficient(observed) ?
  |      yes -> clamp(decideFrom(observed, ctx), ctx)
  |      no  -> staticDecision(ctx)
  v
HistoryHeuristic.register(executionId, ctx)             [final]
```

Any non-OK status, malformed batch, missing evidence, or over-budget read simply omits that family.
`sufficient` decides whether what remains is enough.

## How an observation is recorded

Values come from accumulators that are zero until Spark merges task values back, so nothing can be
observed at planning time. The context is captured instead, and read when the query ends.

```
SparkListenerSQLExecutionEnd(executionId, errorMessage)
  |
  +-- errorMessage present -> discard(execId)           nothing recorded
  v
HistoryObservations.drain(execId)                       removes the entry
  |
  for each callback registered under this execution id, any heuristic:
      observe(ctx) -> Map[metric, value]
        for each (m, v): m.record(store, keyFor(m, ctx), v, now)
```

Keyed by SQL execution id, not globally: a global drain would let one query read another query's
still-merging accumulators. One registry and one listener serve every heuristic.

Contexts are captured at planning rather than found later because `CommandResultExec` has no
physical children for write queries.

## Lifecycle

```
Plugin init                                Plugin shutdown
  |                                          |
  HistoryPolicy.of(conf)                     HistoryObservations.shutdown
  HistoryOwner.open(path, ids, policy)       |   stop accepting
  |   restore snapshot or start empty        v
  |   MetricStores.install(store)            HistoryOwner.shutdown
  |                                          |   registration.close  (deregister first)
  for each heuristic:                        |   drain
  |   declare its families                   |   save snapshot
  |     accepted -> enable it                |   provider.shutdown
  |     rejected -> disable it only          v
  |
  sc.addSparkListener(HistoryObservations.listener)   once, not per heuristic
```

The provider holds everything in heap and never writes on its own. What one application learns
reaches the next only through the snapshot, so a killed driver loses that run's observations.

A rejected schema disables that heuristic alone. Rejection means `declare` returned anything but
`ACCEPTED` - most often the family id is missing from `HistoryMetricCatalog`, or its dimension or
retention changed without a version bump.

## The base classes

`HistoryMetric` owns one family's protocol. The subclass supplies identity and a key recipe; the
bodies are `final` so a heuristic cannot weaken the abstain rules.

```scala
abstract class HistoryMetric extends Logging {
  protected def metricId: Int                          // governed id from HistoryMetricCatalog
  protected def dimension: String
  protected def version: Int = 1                       // override only on a breaking change

  final lazy val metric: MetricVersionId = new MetricVersionId(metricId, version)

  final def declare(store: MetricStore, retention: Duration, budget: Duration): Boolean
  final def record(store: MetricStore, label: String, value: Double, atMs: Long): Unit
  final def latest(store: MetricStore, label: String, nowMs: Long,
      maxAge: Duration, timeout: Duration): Option[Double]

  /** SHA-256 of the parts joined by '|', truncated to 128 bits. */
  protected final def hashKey(parts: String*): String

  /** Which values are worth storing. Override to admit values this one rejects. */
  protected def isUsable(value: Double): Boolean
}
```

`HistoryHeuristic` owns the cycle. It reads and writes a *set* of families, since a formula may
take several measured quantities.

```scala
abstract class HistoryHeuristic extends Logging {
  type Ctx                                             // this heuristic's planning inputs
  type Decision                                        // Long for scan bytes, Int for a count

  def name: String
  def metrics: Seq[HistoryMetric]

  final def enable(policy: HistoryPolicy): Unit
  final def disable(): Unit
  final def isEnabled: Boolean

  protected def keyFor(metric: HistoryMetric, ctx: Ctx): String
  protected def staticDecision(ctx: Ctx): Decision
  protected def decideFrom(observed: Map[HistoryMetric, Double], ctx: Ctx): Decision
  protected def clamp(raw: Decision, ctx: Ctx): Decision = raw
  protected def observe(ctx: Ctx): Map[HistoryMetric, Double]
  protected def sufficient(observed: Map[HistoryMetric, Double]): Boolean =
    observed.size == metrics.size
  protected def shouldObserve(ctx: Ctx): Boolean = true

  final def decide(ctx: Ctx, nowMs: Long): Decision
  final def register(executionId: Option[Long], ctx: Ctx): Unit
}
```

## Adding a heuristic

1. Reserve an id and name in `HistoryMetricCatalog` for each measured quantity. That file belongs
   to the history metrics API, and allocation is permanent: retire in source rather than reuse.
2. `object X extends HistoryMetric` per quantity - id, dimension, key recipe.
3. `object XHeuristic extends HistoryHeuristic` - the overrides below.
4. Add it to `HISTORY_HEURISTICS` in `Plugin.scala`.
5. Call `decide` and `register` from the planning site.

Nothing else changes: not `HistoryOwner`, not `HistoryPolicy`, not the registry, not the snapshot
format.

### What a heuristic overrides

| Member | Purpose |
|---|---|
| `keyFor` | the dimension value this context maps to, per family |
| `staticDecision` | what planning would have chosen without history |
| `decideFrom` | the formula |
| `constrain` | bounds on its output (default: none) |
| `observe` | what the query actually did, read after it ran |
| `sufficient` | whether partial evidence is enough (default: all families answered) |
| `shouldObserve` | whether this context is worth remembering (default: yes) |

The first four are not optional. `decide` is shared code, so it has no way of its own to know
which key to look up, what to compute, or what to fall back to. Worked through the scan heuristic:

| Member | What the base needs it for | Scan's answer |
|---|---|---|
| `keyFor` | which key to look up, per family | hash of `table\|columns\|filters` |
| `staticDecision` | the answer when history cannot help | `maxSplitBytes` |
| `decideFrom` | the formula | `batchSizeBytes / ratio` |
| `observe` | what the query actually did | `decoded / listed`, at query end |

`constrain` receives whatever `decideFrom` returned and is always called - the base never inspects
the value, since `Decision` is an abstract type. Scan uses `0` from `rawSplit` to mean "no usable
answer" and turns it into `maxSplitBytes` in `bound`; that convention is private to the heuristic.

`decide` and `register` are `final`: the fallback path and the execution-id scoping are not a
heuristic's business. So are `declare`, `record`, `latest`, and `hashKey`, so a subclass cannot
weaken the abstain rules.

`Ctx` and `Decision` are abstract types, so each heuristic states its own planning inputs and its
own result type rather than sharing a parameter list that widens with every addition.

## Configuration

```
RapidsConf --> HistoryPolicy.of(conf) --> queue / execution / breaker policies
                                      --> maximumPlanningAge
                                      --> declare / drain / save / restore / shutdown budgets
```

| Key | Feeds | Default |
|---|---|---|
| `spark.rapids.sql.historyPath` | snapshot location; unset disables learning | none |
| `spark.rapids.sql.history.maxAgeDays` | planning age, retention, read window | 7 |
| `spark.rapids.sql.history.planningTimeoutMillis` | read budget **and** breaker slow-call | 100 |

The third is deliberately one value rather than two: they are the same quantity - how long a
planning read may take before it counts as slow - and declaring them separately lets them drift.

The provider validates shape only (positive counts, rates in `(0, 1]`) and ships no defaults, so a
consumer must choose all 16 values. That absence is deliberate: a driver recording once per scan
and a service recording thousands of times a second cannot share a queue depth, and a default in
the contract would be taken unread by consumers that never examined it.

The 14 values not listed above are placeholders picked inside those bounds, not fitted values. Each
becomes conf-backed by adding a parameter to `HistoryPolicy.of`, without touching a call site.

## Constraints from the metrics API

- **One observation is one `double`.** Several quantities means several families, each declared,
  versioned, and retained independently.
- **`limit(N)` requires every declared dimension to be bound.** An unbound dimension is a wildcard
  and forces `limit == 0`. Since `limit(1)` is how "most recent value" is expressed, keep one
  dimension or bind them all on every read.
- **Version is per family**, so adding a family does not invalidate another's observations.
- **Declaration gates recording.** Recording under a version whose declaration was not accepted is
  not allowed.
- **Keys are hashed**, so a snapshot shows 32 hex characters rather than table names. The `hashKey`
  debug log maps one back.
