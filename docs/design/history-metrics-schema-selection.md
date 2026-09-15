# History metrics local/JDBC schema selection

Status: accepted for the persistent-local MVP.

This record selects the physical representation for history-metric declarations and observations.
It complements [the persistent-local provider plan](history-metrics-persistent-local-plan.md). The
benchmark was a screening experiment on one development host, not a claim of formal statistical
significance or production network performance.

## Decision

Use one fixed inline observation table with slots `d0` through `d7`. A metric declaration's
ordered dimension list maps names and kinds to those slots. Store each value in a canonical,
kind-tagged binary encoding. The API already limits a declaration to eight dimensions, so this is a
provider-neutral representation of the public contract rather than a scan-specific schema.

Store each declaration in one row with retention fields and a canonical, versioned blob containing
the ordered dimension names and kinds. The blob is declaration identity; retention remains separate
because the first effective retention policy is persistent state rather than structural identity.

This decision applies to the SQLite local backend and is the observation model recommended for a
future PostgreSQL JDBC backend. SQL dialect details such as identity allocation and binary types
remain backend-specific.

## Candidates tested

All layouts used fixed DDL shared by every metric. None generated a table or column per metric.

| Layout | Physical representation |
| --- | --- |
| Name/kind EAV | One observation row plus one child row per dimension. Each child repeats metric ID/version, dimension name, kind, encoded value, and observation ID. |
| Ordinal EAV | One observation row plus one child row per dimension. Declaration ordinal replaces repeated name and kind. This layout was screened before the final four-layout runs. |
| Inline wide | One observation row containing `dimension_count` and encoded `d0` through `d7`; one fixed equality index per ordinal. |
| Deduplicated wide tuple | A separate unique eight-slot tuple row referenced by each observation. Tuple identity is exact database equality, without probabilistic hashes. |
| Six-field context plus tail | A scan-specific hybrid: deduplicate `d0` through `d5` as a context row and keep `d6` and `d7` on each observation. |

The declaration comparison independently tested normalized declaration header/spec rows against one
canonical schema blob. Observation and declaration choices were not scored together.

## Workloads and use-case assumptions

The final workloads modeled a seven-dimensional scan metric derived from archived design evidence:
table, format, codec, schema epoch, projection, predicate shape, and predicate literal. This is a
concrete anticipated use case, not a production metric contract. The production catalog was empty
when this decision was made.

Each primary workload contained 1,000,000 observations across ten metric versions and 20,000
six-field contexts. Two regimes bounded the sensitivity to the final literal:

- recurrent: five literals per context, each repeated ten times;
- churn: fifty literals per context, each occurring once.

Requests covered 30-day, one-day, and one-hour labeled windows. Runtime planning retention was seven
days, so exemplar selection and correctness used
`[max(request.from, retained cutoff), request.to)`. Each window tested wildcard, six-field context,
literal-only, context-plus-literal, and fully bound newest-1, newest-5, and newest-32 requests: 21
cases per layout and engine. All corrected runs kept every intended hit path nonempty.

The workload deliberately includes arbitrary-subset correctness outside the timed representative
cases. It does not assert that every future metric will divide dimensions into six context fields and
a literal tail.

## Experimental method and provenance

The corrected single-client runs used four fresh database loads per layout and engine. Layout order
was seeded and rotated so each layout occupied each position once. Each timed summary and bounded
write case had seven repetitions. Correctness was checked against an independent in-memory oracle.
PostgreSQL plans were captured outside timing with `EXPLAIN (ANALYZE, BUFFERS)`.

Fresh PostgreSQL 14.10 databases used `fsync=on` and `synchronous_commit=on`. Fresh SQLite
databases used rollback journal mode `delete` and `synchronous=2`. The source revision for both
corrected retained runs was private-repository commit
`075a9236a3a1ad55ee8b38e786c5b8c609d4fafb`.

From the benchmark module, the two runs were:

```bash
mvn -o -f pom.xml \
  -Dtest=HistoryMetricsSchemaBenchmarkTest \
  -Dhistory.metrics.benchmark.profile=primary \
  -Dhistory.metrics.benchmark.database_repetitions=4 \
  -Dhistory.metrics.benchmark.layout=NAME_KIND_EAV,INLINE_WIDE,DEDUPLICATED_WIDE_TUPLE,SIX_FIELD_CONTEXT_TAIL \
  -Dhistory.metrics.benchmark.shape=SCAN_CONTEXT_RECURRENT \
  -Dhistory.metrics.benchmark.output=target/benchmark-primary-scan-recurrent-retained-4loads test

mvn -o -f pom.xml \
  -Dtest=HistoryMetricsSchemaBenchmarkTest \
  -Dhistory.metrics.benchmark.profile=primary \
  -Dhistory.metrics.benchmark.database_repetitions=4 \
  -Dhistory.metrics.benchmark.layout=NAME_KIND_EAV,INLINE_WIDE,DEDUPLICATED_WIDE_TUPLE,SIX_FIELD_CONTEXT_TAIL \
  -Dhistory.metrics.benchmark.shape=SCAN_CONTEXT_LITERAL_CHURN \
  -Dhistory.metrics.benchmark.output=target/benchmark-primary-scan-churn-retained-4loads test
```

The raw result, plan, and metadata SHA-256 values were:

| Run | Results | Plans | Metadata |
| --- | --- | --- | --- |
| Recurrent | `488208c085e201706edf6576856ffabee192401d4982a3dc3ba325df42763403` | `ffd8ea8d7cc647c86b67dba8cbf3375d9b55cea6a71c3d02af8a3f7e842eaa9a` | `f76c4bb942f07de93f3b20fcb07d2ca0a825de0361a7029bd94a9d506bbac450` |
| Churn | `59ca85dcebee940f045052d8dd4b5f8d5123a895db1ff6fce9ec7f7ad89b5f7f` | `8f2eb60cadc25f646a5a545e40c45833baa598349efc1329c01cf8250bebb953` | `b91ff1b391b483375d3c32dc7409669e248e565ef66dfa7a9f82a1f838ec4de5` |

All 10,724 raw result rows in each corrected run passed their correctness gate. Each run contained
168 PostgreSQL plans: four layouts times 21 cases times two aggregation placements. The conservative
plan reducer reported no sequential-scan case and no temporary/disk case.

A separate strict concurrency run used fixed-total work, striped logical-row assignment, one and
eight independent PostgreSQL connections, batch sizes 32 and 128, and 12 fresh database repetitions.
Striping makes all eight writers encounter the same 50-row context groups concurrently. It therefore
tests a realistic hot-context assignment, including both additional per-transaction context upserts
and lock conflicts; it is not a pure lock-wait microbenchmark.

```bash
mvn -o -f pom.xml \
  -Dtest=PostgresConcurrentWriteBenchmarkTest \
  -Dhistory.metrics.benchmark.concurrent.work_mode=fixed_total \
  -Dhistory.metrics.benchmark.concurrent.partition=striped \
  -Dhistory.metrics.benchmark.concurrent.total_rows=102400 \
  -Dhistory.metrics.benchmark.concurrent.database_repetitions=12 \
  -Dhistory.metrics.benchmark.layout=NAME_KIND_EAV,INLINE_WIDE,SIX_FIELD_CONTEXT_TAIL \
  -Dhistory.metrics.benchmark.shape=SCAN_CONTEXT_RECURRENT \
  -Dhistory.metrics.benchmark.concurrent.output=target/concurrent-fixed-total-scan-recurrent-striped-12cycle test
```

That run used clean private-repository source
`bbcd75003e85994d2d21631720c51d656e04b73c`. Its 12 work items formed one complete positional
cycle and supplied 12 fresh-database experimental units per scenario. Raw, summary, metadata, and
reduced-output SHA-256 values were respectively
`b81de6cc4d5174b2409e2721731630c88b7f2a59abeebc3a2c682e9e343a1f6c`,
`bbe3d760566565b4cd5afb9621fd103c1629f7df6ddc239e518f1bf14240a623`,
`5ed3dad66366704de85a9333e7ecd6685c967b7eb3ac4040aa37d308a39132ef`, and
`693f77c5389981ab1b32548c88955164d575a17d3ad622011d36a7c5ae58e51c`.

Generated `target/` artifacts are intentionally not repository links. The hashes and summarized
tables in this record are the durable review evidence.

## Results

### Storage and cardinality

Bytes are physical observation-layout storage after loading 1,000,000 observations. Ratios are
relative to name/kind EAV within the same engine and workload.

| Engine | Regime | EAV | Inline wide | Full tuple | Context plus tail |
| --- | --- | ---: | ---: | ---: | ---: |
| PostgreSQL | Recurrent | 1,694,908,416 | 888,143,872 (0.524x) | 920,633,344 (0.543x) | 754,737,152 (0.445x) |
| PostgreSQL | Churn | 1,693,229,056 | 891,133,952 (0.526x) | 3,640,229,888 (2.150x) | 754,671,616 (0.446x) |
| SQLite | Recurrent | 810,070,016 | 509,001,728 (0.628x) | 575,463,424 (0.710x) | 463,552,512 (0.572x) |
| SQLite | Churn | 810,184,704 | 509,755,392 (0.629x) | 2,014,908,416 (2.487x) | 463,818,752 (0.572x) |

The full-tuple layout created 100,000 tuple rows for recurrent input and 1,000,000 tuple rows for
churn input. The hybrid created 20,000 context rows in both regimes.

### Representative server-side summary latency

Values are p50 latency ratios to name/kind EAV for the exact same engine, regime, and request. Lower
is faster. These are selected grounded cases, not an average or weighted score.

| Engine/regime | Layout | Full context | Day context | Hour context | Full literal | Day literal | Hour literal |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| PostgreSQL recurrent | Inline | 0.052x | 0.175x | 0.134x | 0.330x | 0.028x | 0.005x |
| PostgreSQL recurrent | Full tuple | 0.092x | 0.200x | 0.153x | 0.874x | 0.126x | 0.060x |
| PostgreSQL recurrent | Hybrid | 0.086x | 0.185x | 0.141x | 0.333x | 0.028x | 0.005x |
| PostgreSQL churn | Inline | 0.052x | 0.179x | 0.132x | 0.229x | 0.046x | 0.024x |
| PostgreSQL churn | Full tuple | 0.108x | 0.372x | 0.278x | 1.750x | 2.723x | 0.291x |
| PostgreSQL churn | Hybrid | 0.086x | 0.185x | 0.136x | 0.223x | 0.047x | 0.024x |
| SQLite recurrent | Inline | 0.013x | 0.080x | 0.318x | 0.196x | 0.111x | 0.268x |
| SQLite recurrent | Full tuple | 0.010x | 0.062x | 0.245x | 0.462x | 0.986x | 11.137x |
| SQLite recurrent | Hybrid | 0.010x | 0.062x | 0.247x | 0.192x | 0.109x | 0.272x |
| SQLite churn | Inline | 0.013x | 0.082x | 0.319x | 0.052x | 0.038x | 0.230x |
| SQLite churn | Full tuple | 0.012x | 0.075x | 0.290x | 0.288x | 1.109x | 11.945x |
| SQLite churn | Hybrid | 0.010x | 0.064x | 0.246x | 0.052x | 0.038x | 0.236x |

Full-window wildcard summaries were intentionally retained. Inline was 1.018x EAV on PostgreSQL and
1.123-1.126x on SQLite. The hybrid was 1.022/1.018x on PostgreSQL and 1.176/1.178x on SQLite.
The layouts therefore improve selective access, not every query.

### Writes, retention, and concurrency

Durable batch-128 rates below use the bounded write test, not the 10,000-row fixture loader.

| Engine/regime | EAV rows/s | Inline rows/s | Full tuple rows/s | Hybrid rows/s |
| --- | ---: | ---: | ---: | ---: |
| PostgreSQL recurrent | 5,127 | 11,230 | 14,363 | 13,020 |
| PostgreSQL churn | 4,880 | 10,078 | 12,078 | 11,272 |
| SQLite recurrent | 3,066 | 3,254 | 4,053 | 3,754 |
| SQLite churn | 2,662 | 2,916 | 4,074 | 3,280 |

The SQLite 1,000,000-row fixture load using 10,000-row batches was slower for inline than EAV:
72.5 versus 44.2 seconds in the recurrent run and 93.3 versus 56.0 seconds in churn. This exposes
the cost of maintaining eight indexes for very large batches; the provider's bounded write batches
are the MVP path.

| Engine/regime | EAV retain-32 | Inline retain-32 | Full tuple retain-32 | Hybrid retain-32 |
| --- | ---: | ---: | ---: | ---: |
| PostgreSQL recurrent | 4.3 ms | 4.1 ms | 664 ms | 180 ms |
| PostgreSQL churn | 4.4 ms | 3.9 ms | 1,279 ms | 173 ms |
| SQLite recurrent | 29.3 ms | 28.0 ms | 197 ms | 98.8 ms |
| SQLite churn | 31.4 ms | 29.2 ms | 97.1 ms | 99.3 ms |

Strict striped PostgreSQL concurrency results:

| Layout | Batch | 1-client rows/s | 8-client rows/s | 1-to-8 ratio | 8-client storage |
| --- | ---: | ---: | ---: | ---: | ---: |
| EAV | 32 | 2,007 | 18,641 | 9.29x | 178,642,944 |
| EAV | 128 | 3,645 | 32,583 | 8.94x | 178,987,008 |
| Inline | 32 | 5,014 | 24,462 | 4.88x | 88,137,728 |
| Inline | 128 | 12,612 | 63,591 | 5.04x | 88,973,312 |
| Hybrid | 32 | 4,285 | 23,918 | 5.58x | 72,425,472 |
| Hybrid | 128 | 7,407 | 59,262 | 8.00x | 74,358,784 |

Superlinear EAV and hybrid ratios reflect slow one-client baselines and must not be read as general
server scaling. Absolute eight-client throughput is the relevant comparison here; inline did not
collapse and remained fastest.

### Declaration representation

For 128 declarations, the canonical blob used one JDBC execution rather than two. PostgreSQL fresh
declaration took approximately 5.2-5.9 ms versus 14.4 ms for normalized rows and used 73,728 versus
188,416 bytes. SQLite took approximately 28.2 ms versus 31-32 ms and used 28,672 versus 61,440 bytes.
Reopen/load used one query instead of two. Correctness covered idempotent redeclaration, incompatible
detection, strict decoding, and reconstruction after a physical JDBC reconnect.

## Rejected alternatives

Name/kind EAV remains the generic reference but is not selected. It used approximately 1.6 times
inline storage on SQLite and 1.9 times on PostgreSQL, required child-row relational integrity, and
lost materially on selective queries and bounded/concurrent writes. The public eight-dimension bound
removes its principal flexibility advantage.

Ordinal EAV avoids repeating names and kinds, but retains child rows, joins, transactional coupling,
and retention cascades. Screening did not establish an advantage sufficient to take it into the
final four-layout comparison.

The full tuple met its predeclared kill conditions. Literal churn eliminated deduplication, produced
one tuple per observation, exceeded EAV storage, and caused literal-only regressions as high as
2.723x EAV on PostgreSQL and 11.945x on SQLite. Its bounded-write advantage did not offset those
failures.

The six-field hybrid is credible only for the modeled scan family. It saved about 15% PostgreSQL
storage and 9% SQLite storage relative to inline and performed well on context predicates. However,
the API does not label six dimensions as context and two as tail. Selecting it generically would
embed one metric's semantics in storage, add a context table, conflict-aware upserts, foreign keys,
orphan cleanup, and slower retention. The strict concurrency result showed it can work; it did not
justify that complexity for the provider-neutral MVP.

## Production schema and baseline

The observation table must contain acceptance order, metric ID/version, observation timestamp,
finite metric value, application provenance, `dimension_count`, and nullable encoded
`d0` through `d7`. It needs the common metric/version/time/acceptance selection index and one
fixed equality/time/acceptance index per dimension ordinal. Query construction maps reviewed
declaration names to ordinals and uses prepared values; no identifier comes from user input.

The declaration blob encoding and observation dimension encoding must be injective across STRING,
LONG, and BYTES and versioned independently of Java serialization. The selected initial on-disk
schema is version 1; it uses the canonical declaration blob and inline observation slots described
above. Normal open strictly validates the schema version and catalog, table and column shapes,
primary- and foreign-key definitions, indexes, and declaration blobs, and enables foreign-key
enforcement for subsequent writes. It does not decode every stored observation payload or run a
full foreign-key scan. That scan occurs during an explicit integrity check. Malformed declaration
blobs, including unknown versions, duplicate names, invalid UTF-8, invalid kinds, or counts above
eight, fail open. A malformed observation payload is corrupt data, not a value to coerce; it fails
the affected read or an explicit integrity check.

The backend must preserve the existing public semantics: exact declaration versions, exact typed
equality, arbitrary omitted-dimension wildcards, `[from,to)`, runtime planning-retention clamp,
deterministic timestamp/acceptance ordering, positive newest-N limits, deadline-bounded
`limit=0`, finite overflow-safe summaries, provenance, positional statuses, and best-effort
non-acknowledging record behavior.

Retention becomes one bounded observation delete without tuple/dimension-child cleanup. Writes remain
bounded transactions, but no provider-neutral whole-batch atomicity is introduced. An ambiguous
commit is terminal, is not retried automatically, and poisons the connection.

This schema version 1 is the initial format of an unreleased provider. There is no earlier durable
SQLite format to upgrade and no format transition in the MVP. If a future release changes the
on-disk format, it must introduce an explicit version transition that preserves declaration
ordinals, retention, acceptance order, observations, timestamps, values, dimensions, and
provenance. The version marker must advance atomically with the data transformation; failed,
partial, newer, malformed, or incompatible states must fail closed or roll back unambiguously.
Future migration acceptance must use real source-version fixtures, semantic round-trip oracles, and
fault injection appropriate to that specific transition.

## Limitations and residual risks

- Both engines ran embedded on one otherwise idle Linux host. PostgreSQL used loopback JDBC. The
  results do not predict WAN latency, managed-service behavior, different disks, or multi-host
  concurrency.
- Profiles and metadata explicitly described the scale runs as exploratory and not
  `decision_eligible`. This record makes an engineering MVP choice from converging exact-case
  evidence; it does not upgrade the experiment's statistical label.
- Eight equality indexes make large SQLite bulk loads expensive. Normal provider batches performed
  adequately, but this should be measured again if the writer batch bound or ingest rate grows.
- Arbitrary multi-dimension predicates rely on choosing among fixed single-ordinal indexes and
  post-filtering other bindings. No combinatorial indexes are proposed.
- Binary equality and the SQL were exercised with SQLite and PostgreSQL, not every JDBC database.
- The scan workload is grounded but prospective. A governed production metric and archived
  production distributions do not yet exist together.
- This schema decision does not change the persistent-local plan's component budgets. SQLite
  backend, schema versioning, and row mapping remain subject to the approximate 1,500 production
  Java and 1,600 local-test line ceilings and the existing stop-and-review rule when exceeded.

Reevaluate the layout when a governed producer supplies representative traces that materially exceed
the tested ingest rate or eight-dimension limit; when a required request misses its planning budget;
when full wildcard scans dominate; when SQLite file size or index maintenance is operationally
unacceptable; when a network PostgreSQL deployment supplies real latency/concurrency data; or when
several governed metric families demonstrate a stable, explicit context/tail convention worth
promoting into a separate provider schema.
