# History metrics persistent local provider plan

Status: approved for MVP implementation.

This document follows `history-metrics-repository-and-service-plan.md`. It narrows the next step:
replace the large custom in-memory-and-snapshot implementation with a small SQLite-backed local
provider, strengthen reusable conformance tests, and preserve a path to a later network service.
The first implementation does **not** add REST/gRPC, PostgreSQL, Aether integration, or transport
authentication.

## Why change the current local provider

The current `cudf-spark-history-metrics-local` artifact is much larger than a reference MVP. At
this review it contains roughly 9,900 lines of production Java and 10,800 lines of tests. Much of
that implements an in-memory database, asynchronous write machinery, planning executors, a circuit
breaker, detailed diagnostics, and a custom binary snapshot format.

Snapshots are explicit point-in-time exports created by `LocalHistoryMetrics.save` and restored by
`LocalHistoryMetricsFactory.openSnapshot`. They are not an automatic shutdown dump and do not
remove live data. Their watermark, codec, validation, temporary-file, atomic-replace, and restore
behavior is a substantial custom persistence subsystem.

Persistence is a local-mode requirement from internal developers testing history across
separate application runs. One application records history and shuts down; a later application opens
the same database file and uses that history to make a planning decision. They need continuous
persistence at backend transaction boundaries, not a transactional public acknowledgement, explicit
save points, or compatibility with existing snapshot files.

Extending the snapshot subsystem would reimplement facilities SQLite already provides:
transactions, crash recovery, indexes, concurrent readers, a durable file format, and JDBC access.
The plan therefore replaces snapshots and the collections backend rather than expanding them.
Existing tests remain useful as evidence: move provider-neutral semantics into the TCK before
deleting code, then implement only what that contract requires.

There are currently no production metric declarations or production calls to `record`, `declare`,
or `summarize` in either repository: `HistoryMetricCatalog.production()` is empty and the public
plugin only installs/selects a store. The API review nevertheless accepted the current API for the
MVP; no further pre-SQL reduction is required. The planning-safety boundaries remain necessary for
the first real optimizer caller.

## Decisions

- Keep the API Spark- and Scala-free. Do not add `SparkContext`, Scala types, configuration
  wrappers, or provider-supplied identity callbacks.
- Use the reviewed current API for the SQLite MVP. Do not delay SQL implementation for another
  API-reduction pass.
- Preserve three core boundaries: `record` performs no JDBC I/O and does not wait on storage;
  planning reads have a caller-supplied budget; ordinary history failures produce static fallback
  rather than query failure. A single bounded queue and writer can satisfy the first boundary
  without retaining the current pipeline.
- `MetricStore.record` is fire-and-forget and best-effort. Internal `WriteResult` and terminal
  accounting support provider lifecycle and diagnostics; they are not application acknowledgement.
  Provider-neutral whole-batch atomicity is not required.
- Preserve the reviewed batch limits, positional results, failure statuses, public value types,
  and `HistoryMetricsProvider.open(Map<String, String>, applicationId, applicationAttemptId,
  producerVersion)` construction boundary for this MVP.
- Use SQLite through JDBC for ephemeral and persistent local modes. SQLite `:memory:` provides
  no-persistence behavior; a configured file provides persistence across restarts.
- Use the reviewed fixed inline observation layout: declaration ordinals map at most eight typed
  dimensions to `d0` through `d7`. Store the ordered declaration structure in one canonical,
  versioned blob while keeping retention as scalar persistent state. The benchmark evidence and
  rejected alternatives are recorded in [the schema-selection report](history-metrics-schema-selection.md).
- Retain source compatibility only for the ServiceLoader-facing `LocalHistoryMetricsProvider`.
  Other current local construction, snapshot, and inspection types are not compatibility surfaces.
- Delete the custom snapshot format instead of supporting two persistence mechanisms.
- Do not read or import old snapshot files. They contain no required data. Developers regenerate
  test history in SQLite, and the custom snapshot implementation can be deleted without a migration
  tool or staged read compatibility.
- Do not require providers to expose clocks, stored rows, counters, or fault injection through
  production APIs. Such tests belong at the backend/adapter layer or in provider integration tests.
- Use JDBC, not ODBC. It avoids another driver manager, DSN, and native deployment layer.
- Defer REST versus gRPC. They are possible later transports, not part of this MVP.

## Requirements and use cases

### Ephemeral local use

A developer or test can open an isolated provider without a durable file. It supports declaration,
asynchronous recording, bounded summaries, and clean shutdown. Instances do not share state. This
uses SQLite in-memory mode so there is only one implementation of storage semantics.

### Persistent local use

The primary acceptance scenario uses two application runs:

1. Application A points the provider at a new file, declares metrics, records representative
   history, completes a successful provider shutdown, and exits.
2. Application B points the provider at the same file with its own application identity, reads the
   committed history during planning, and demonstrates that it can use the summary to select between
   a history-informed decision and the existing static fallback.

Persistence is continuous at transaction boundaries and does not depend on a successful shutdown
snapshot. Application B must see Application A's committed history while new records retain the
provenance of whichever application wrote them. An unclean exit must leave a database SQLite can
recover.

The automated storage acceptance test launches A and B as separate JVM processes with a test
catalog; reopening in the same process is insufficient. A true two-Spark-application test also
requires the first governed production metric plus its producer and decision consumer. None exists
today, so the SQLite MVP must not fake that coverage or add a production metric merely for this
backend. When the first heuristic lands, its acceptance test runs two Spark applications against one
database file and verifies both the history-informed branch and static fallback.

The MVP supports one provider/process owning a file on local or block storage. Multi-process
writers, network filesystems, replicated storage, and Kubernetes multi-replica use are not
local-mode requirements. URI-like and syntactic UNC paths are rejected, but an arbitrary
network-mounted filesystem can look like an ordinary local path to Java and cannot be detected
reliably. Such mounts are unsupported, and the deployer is responsible for selecting local or block
storage. Copying a live database file is not a supported backup operation. A future backup/export
feature must use an SQLite-supported mechanism.

### Planning safety

History remains optional evidence. Missing, malformed, unavailable, or late results cause existing
static behavior. `declare` and `summarize` retain one relative end-to-end deadline. `record`
returns without JDBC I/O, offers to a bounded queue, and may drop data when full or stopped.

The backend is synchronous behind that adapter and may batch admitted records for efficiency. An
observation is *admitted* when the bounded record queue accepts it; this is not acknowledgement that
it will persist. Successful shutdown means admission is closed and every previously admitted item
reached an attempted terminal outcome: confirmed backend acceptance, validation rejection, or
backend failure. It does not promise that every admitted item persisted. If the budget expires
before all admitted work reaches such an outcome, shutdown returns `false`. Repeated shutdown is
harmless.

### Semantic compatibility

The API review accepted the current API for the MVP. SQLite must preserve the behavior established
by its documentation and provider-neutral tests:

- permanent idempotent declarations and incompatible-redeclaration rejection;
- independent metric contract versions;
- typed dimension equality and wildcarding by omitted dimensions;
- `[from, to)` windows, planning-age visibility, and storage retention;
- positional batch results and explicit statuses;
- limits ordered by observation timestamp then provider acceptance order;
- finite, overflow-safe count, mean, min, max, first timestamp, and last timestamp;
- provenance from explicit application IDs/version, without treating it as authenticated identity.

If implementation and documentation disagree, the API documentation plus an explicit reviewed
decision wins; accidental behavior is not a contract.

### Platform, packaging, and local data

The API remains Java 8 bytecode and Spark-, Scala-, and JDBC-free. The local artifact depends on
the qualified SQLite JDBC driver at runtime while remaining thin and unshaded.

Xerial SQLite JDBC 3.53.4.0 is the qualified MVP dependency. Its artifact and inspected native
binaries provide practical glibc and musl coverage for Linux x86_64 and aarch64. Its Java
bytecode is Java 8 compatible. Before release, CI must still run four native-load/extraction
smokes in the exact supported glibc-x86_64, musl-x86_64, glibc-aarch64, and musl-aarch64
images. Read-only or `noexec` temporary storage may need supported driver configuration or image
packaging.

The driver must not enter the Spark RAPIDS distribution merely because the API does. For this MVP,
the local provider is a thin Maven artifact with SQLite JDBC as a transitive runtime dependency.
`--packages`/normal dependency resolution supplies both; a caller using only `--jars` must place
the documented driver artifact on the classpath too. We will not shade native driver resources into
the provider jar. The local provider is not part of the normal Spark RAPIDS distribution.

The database can contain dimensions and application provenance. Create new files with restrictive
permissions where supported, use prepared statements, and keep values and paths out of routine
diagnostics. SQLite JDBC does not itself provide encryption at rest. Encryption and key management
are outside this MVP.

## High-level design

```text
Spark/plugin consumer
        |
        v
MetricStore (bounded planning calls, non-blocking record)
        |
        v
small adapter (validation, deadlines, bounded queue, provenance)
        |
        v
SQLite HistoryMetricsBackend (synchronous JDBC/transactions)
        |
        v
SQLite :memory: database or one local file
```

The adapter need not be a new module or public API. Keep it in the local artifact until a real
network client demonstrates reuse. It needs only to validate inputs, contain ordinary backend
failures, enforce deadlines, stamp provenance, batch a bounded record queue, and perform bounded
idempotent shutdown.

The completed deadline/concurrency spike covered connection acquisition, SQLite lock waits,
bounded scans, cancellation/interruption, post-timeout connection recovery, and the writer lock
policy. It confirmed that `busy_timeout` bounds lock waits but is not a general query deadline.

The preferred minimal design is one physical SQLite connection per provider, with every backend
operation serialized by a deadline-aware lock. This also avoids the trap that separate
`jdbc:sqlite::memory:` connections create separate databases. The writer and planning caller
compete for that lock; failure to acquire it within the remaining budget returns/drops the
appropriate bounded result.

This simplicity has a known cost: a slow summary blocks the writer and a large write transaction
blocks planning, increasing drops or fallback. The completed concurrency spike supports one physical
connection, a shared deadline-aware lock, a remaining-budget progress handler, and a bounded SQLite
busy policy. It showed recovery after repeated progress interruptions and demonstrated that Java
thread interruption alone does not cancel an active SQLite query. The MVP therefore has no fixed
planning executor and never creates per-request threads or connections. Repeated timeouts must not
accumulate work or poison the connection.

Do not retain the current planning pool or circuit breaker merely by default. Retain only the
smallest measured mechanism that meets the contract. The writer must use the same bounded lock/busy
policy so shutdown cannot wait indefinitely in JDBC.

### SQLite backend

The backend owns connections, migrations, prepared statements, transactions, and API-to-row
mapping. It knows nothing about Spark, Aether, transports, auth, or optimizer decisions. Its schema
must represent:

- a migration version;
- immutable metric/version declarations whose ordered names and kinds use a canonical versioned
  blob;
- observations, values, timestamps, acceptance order, provenance, and zero through eight inline
  typed dimension slots;
- prepared exact predicates and fixed per-ordinal indexes for arbitrary declared-dimension subsets.

Schema version 1 is the existing normalized-declaration, name/kind-EAV format. Opening it performs
one atomic v1-to-v2 migration to the canonical declaration blob and inline observations. The
transaction preserves retention, acceptance order, observations, timestamps, values, dimensions,
and provenance; it advances the migration marker only on success. A failed migration leaves the
version-1 file reopenable, while newer, partial, corrupt, or incompatible states fail closed.
Normal version-2 open strictly validates version/catalog structure, tables, columns, primary- and
foreign-key definitions, indexes, and declaration blobs, and enables foreign-key enforcement for
subsequent writes. It does not decode every observation payload or run a full foreign-key scan.
Full foreign-key validation occurs during v1-to-v2 migration or an explicit integrity check;
malformed observation data fails the affected read or that explicit check.

Declaration is an insert-or-verify transaction. Persist both the structural declaration and the
first effective retention policy: redeclaring the same structure with another recommendation does
not change it, and reopening the file does not recompute it. If a later provider maximum is stricter
than the persisted planning policy, opening applies that stricter limit as a runtime visibility
clamp without rewriting the permanent declaration.

Transactions protect declaration integrity and bounded observation writes. SQLite may group
multiple observations in one bounded transaction for efficiency, but the portable backend contract
does not require whole-batch atomicity. An ambiguous commit is a terminal backend failure: do not
retry automatically, poison and close the connection, and allow that the data may later be present.
Summary queries map declared names to inline ordinals and use prepared time and dimension predicates
with the fixed indexes justified by the reviewed request shapes.

For consistent numerical behavior across SQLite and future PostgreSQL, SQL may select eligible
values and a small Java accumulator may summarize them. Preserve `limit = 0` as the existing
unlimited-row request: its work is bounded by the end-to-end deadline and progress interruption, not
by an invented row cap or silent truncation. Positive limits retain their row bound, and normal
indexed requests must fit the planning deadline. Do not silently use database `AVG` if it violates
the finite/overflow-safe mean contract.

Planning visibility applies retention cutoffs immediately. Physical deletion may run
opportunistically in small bounded batches, never as unbounded work on `record` or a planning
request. Fresh databases deliberately use SQLite's default rollback journal for the single-owner
MVP; behavioral crash-recovery and external-lock tests validate that choice. The provider does not
override an existing database's journal mode. Configuration is minimal and provider-owned.

### Provider lifecycle

`LocalHistoryMetricsProvider` remains ServiceLoader-discovered. The single MVP key is
`spark.rapids.sql.history.metrics.local.path`: absence selects isolated memory mode and a valid
local filesystem path selects file mode. This key is intentionally provider-owned and is not
registered in `RapidsConf`. Reject blank values, URI-like paths, syntactic UNC paths,
and otherwise invalid paths; only local/block storage is supported. Arbitrary network mounts cannot
be identified reliably from a Java path, so avoiding them is the deployer's responsibility. The
provider creates the backend/adapter and returns a store with cached `BackendInfo`. Open cleans up
resources acquired before failure. Test this with natural SQLite failures such as invalid paths or incompatible
databases, not a production cleanup-inspection hook.

Application ID and attempt ID identify record provenance. A new attempt may reopen an existing
database and write different provenance. These values neither select nor authorize a tenant.

## Test strategy

The goal is a stronger reusable contract, not moving every local test into the TCK. A reusable test
must not require another provider to adopt the same internals.

### API tests

Keep construction, equality, encoding, bounds, and public-surface tests in the TCK module. They do
not establish provider persistence or query correctness.

### Backend semantic TCK

Replace the weak `BackendContractTest` with a reusable `HistoryMetricsBackend` suite. A small
test factory supplies and closes an isolated backend. Since backend `record` is synchronous, this
suite can test declarations, windows, dimensions, ordering, retention-independent summaries,
positional results, and aggregation without an async drain or stored-row inspection hook.

Time-dependent retention may use a fixture clock where construction naturally permits it. A remote
backend may provide a test server clock or keep clock-manipulation cases in its integration suite.
This is a test-fixture capability, not a production SPI. Run the semantic suite against SQLite
in-memory and file modes and, later, server SQLite and PostgreSQL backends.

### Provider/SPI lifecycle tests

Keep a small production-SPI suite using ServiceLoader and the actual `HistoryMetricsProvider.open`
boundary. It can verify discovery/name, normal open, cached non-null compatibility information,
unknown/not-declared behavior, ordinary invalid public inputs, and repeated shutdown.

The current production local provider opens `HistoryMetricCatalog.production()`, which is empty,
while the semantic fixture injects a test catalog by bypassing that SPI. Until a governed production
metric exists, do not claim the production-SPI suite proves successful declaration/record/query.
Factory-created stores with injected catalogs belong to backend/adapter tests, not to this suite.

Do not claim that the SPI suite alone proves configuration/identity propagation, injected
partial-open cleanup, nonblocking record against a stalled backend, every backend exception, or
shutdown against a stuck dependency. Those requirements are not all observable through the
production SPI.

Verify identity/configuration in a SQLite integration test by reading its isolated database, and
cleanup with natural open failures. Test timing, exception containment, and stuck shutdown once on
the shared adapter with fake backends/queues/clocks/executors. A transport provider later uses a
test server. No provider ships those fixtures as production APIs.

The current semantic fixture already relies on `setProviderTime` and `awaitWrites`, so it is not
strictly black-box despite its description. Move synchronous semantics to the backend TCK and
adapter contract tests, retain only truly observable SPI tests, and do not broaden
`HistoryMetricsProviderFixture`.

Adapter contract tests must preserve the reviewed API boundary. At minimum, test monotonic
end-to-end planning budgets, total/nonblocking best-effort `record`, bounded queue admission, and
safe ordinary-failure fallback. Test the retained 128-item/null/oversized sentinels, positional
cardinality, and failure statuses exactly, without treating internal outcomes as application
acknowledgements or requiring provider-neutral whole-batch atomicity.

### SQLite integration tests

Keep focused tests for:

- in-memory isolation and file reopen/committed-data survival;
- fresh creation at the current schema version, plus upgrade fixtures only for versions that have
  actually shipped;
- deterministic failure and resource cleanup for incompatible/corrupt databases without deleting or
  replacing the caller's file;
- transaction rollback and recovery after a forked process is terminated abruptly, plus ambiguous
  commit handling without automatic retry or connection reuse;
- lock/busy behavior and planning deadlines;
- bounded physical retention;
- prepared statements for every dimension kind;
- file permissions where supported;
- bounded idempotent shutdown with pending records;
- x86_64 and ARM64 load plus a declare/write/summarize smoke test.

Prefer temporary databases and natural SQLite behavior. Add a test seam only when a failure cannot
be reproduced deterministically at the appropriate layer.

## Size and complexity budget

“Simplify” is not an acceptance criterion. Before implementation, measure the current components
with the same line-count method and review the replacement against these approximate ceilings:

| Local component | Production Java | Local tests |
| --- | ---: | ---: |
| Provider, configuration, ownership, and lifecycle | 400 | 500 |
| Planning validation/deadline adapter | 600 | 700 |
| Bounded record queue and single writer | 500 | 700 |
| SQLite backend, schema, migrations, and row mapping | 1,500 | 1,600 |
| Small local support types | 300 | 300 |
| **Total local artifact** | **3,300** | **3,800** |

Reusable API/TCK tests are outside the local-test column, but moving code into the TCK must not hide
provider-specific complexity. These are review budgets, not incentives for compressed or unreadable
code. Exceeding a component budget requires stopping to identify a missing requirement or a design
that remains too complicated; it is not resolved by changing the target after implementation.

In particular, do not port `LocalMetricStorePlanningAdapter`,
`LocalAsyncRecordPipeline`, `LocalHistoryMetricsImpl`, or
`LocalHistoryMetricsFactory` wholesale. Replace them with the minimal responsibilities listed
above. The queue/writer should be a straightforward bounded producer-consumer loop, not a reduced
copy of the current pipeline.

## Target local public surface

The final local artifact should expose the ServiceLoader provider and no second application-facing
construction/lifecycle API. `LocalHistoryMetricsProvider` must remain public with a no-argument
constructor for ServiceLoader. The SQLite backend, adapter, owner, queue, and configuration parsing
should be package-private wherever practical.

Move `LocalTestCatalog` and backend factories/fixtures to TCK or test sources. Remove public
`LocalHistoryMetrics`, `LocalHistoryMetricsFactory`, drain/test-handle/counter types, and snapshot
types unless the compatibility audit identifies a real external consumer. Tests can keep
package-private fixtures; they do not justify a supported production surface.

## Expected removals

After SQLite passes the strengthened contracts, remove:

- snapshot state/codec/deadlines/file guards/diagnostics and save/open-snapshot APIs;
- the collections declaration/observation database and inspection handles;
- snapshot-specific counters and companion values;
- the current `LocalMetricStorePlanningAdapter` and its tests, replacing it with the bounded
  adapter described above rather than editing it down in place;
- the current `LocalAsyncRecordPipeline` and its tests, replacing it with the simple bounded
  queue/writer described above;
- the planning circuit breaker and configurable executor unless justified by measurement;
- duplicate local tests now covered by the TCK;
- injection overloads used only by removed machinery.

Delete or simplify behavior before deleting its tests. The target is not a fixed line count, but the
result should be closer to a few thousand production and test lines than the current ~20,000
combined. Every retained subsystem needs a stated requirement.

## Implementation sequence

1. Use Xerial SQLite JDBC 3.53.4.0 as a thin runtime dependency. Complete native-load/extraction
   smokes in the four supported libc/architecture images and stop if it fails any real image. Do not
   add PostgreSQL or a network framework.
2. Retain source compatibility for `LocalHistoryMetricsProvider`; snapshot data, save points,
   snapshot import, and other local construction/inspection types are not retained.
3. Use the reviewed current API and provider-neutral contract as the SQL implementation boundary.
4. Add the backend semantic fixture, move reusable cases, reduce provider claims, and run the
   current backend as a baseline.
5. Apply the completed deadline/concurrency spike result: one serialized connection, a shared
   deadline lock, progress cancellation, bounded busy policy, and no fixed planning executor.
6. Implement SQLite `:memory:`, then file mode, schema creation, reopen, indexes, retention, and
   abrupt-process/locking behavior.
7. Replace—not port—the record/deadline machinery within the component budgets and add
   focused fake-backend fault tests. Retain no breaker, extra planning pool, counters, or
   diagnostics without evidence.
8. Switch the provider, remove snapshots/custom storage/public test surfaces, and update
   docs/examples to memory/file modes.
9. Run standalone API/TCK/local tests, public provider-selection tests, duplicate-class and
   dependency checks, restart tests, and architecture smoke tests. Confirm SQLite is not bundled
   with API-only consumers.
10. Stop for review. REST/gRPC, PostgreSQL, Aether, and auth start only after acceptance.

## SQLite MVP acceptance criteria

- The accepted current API remains Java-only, Spark-free, Scala-free, and JDBC-free.
- The local artifact supports isolated in-memory and durable file modes.
- Two separately launched JVM test applications share a file: A records and shuts down
  successfully; B reads A's committed summary and uses it in the test decision. A same-process
  reopen test alone is insufficient.
- The first real history-backed heuristic adds a two-Spark-application test of its history-informed
  and static-fallback branches; the storage MVP does not claim that coverage before a metric exists.
- `record` performs no JDBC I/O on its caller and is bounded by queue admission.
- Planning and shutdown meet budgets under normal, locked, failed, and stopped conditions. A
  `limit = 0` summary may examine all eligible rows but must stop at its end-to-end deadline without
  silent truncation.
- Both SQLite modes pass the backend TCK; the provider passes the production-SPI lifecycle suite.
- Successful shutdown means every admitted item reached an attempted terminal backend outcome; it
  does not acknowledge that every item persisted. A timed-out shutdown returns `false`.
- Adapter boundary and fault behavior are tested without production provider hooks.
- Custom snapshots and collections storage are gone; remaining subsystems map to requirements.
- The driver works in supported x86_64 and ARM64 Linux images.
- Public Spark integration still safely falls back when the provider is absent or fails.
- No REST/gRPC, PostgreSQL, Aether, authentication, or authorization implementation is included.

## Later REST or gRPC service

SQLite is a stepping stone because semantics remain behind `HistoryMetricsBackend`; remote clients
must not access JDBC or a SQLite file.

```text
Spark process                                  history service
-------------                                  ---------------
MetricStore                                    REST/gRPC handlers
  -> bounded record queue                        -> validation/deadlines
  -> transport client          ---- network ---> -> HistoryMetricsBackend
                                                     -> SQLite (local)
                                                     -> PostgreSQL (production)
```

A standard Java client can later ship with Spark RAPIDS. Aether configures its endpoint and operates
a versioned server image; it does not reimplement the Java database. The server may use this SQLite
backend for single-node tests and a later PostgreSQL backend passing the same semantic TCK.

That phase must decide:

- REST versus gRPC based on Aether integration, operations, compatibility, and dependency cost;
- a versioned wire schema and compatibility negotiation;
- batch limits, positional partial results, idempotency, retries, and backpressure;
- deadline propagation/cancellation and transport-to-status mapping;
- server concurrency, pooling, migrations, retention jobs, readiness, and health;
- redacted diagnostics and operational metrics;
- pluggable client credentials/request signing, server authz/authn, and server identity
  verification.

Authentication remains mechanism-neutral: AWS workload credentials/signing, side-band bearer
tokens, mTLS, or another mechanism may implement later extension points. Configuration may select a
mechanism and point to runtime-supplied material, but there is no hard-coded shared secret and
application ID is not an authenticated principal.

Do not mechanically expose every `MetricStore` call. Specify the wire and failure contracts first,
then decide whether transport implements `HistoryMetricsBackend` or a smaller mapping. REST and
gRPC are alternatives, not two required implementations.

## Resolved schema evidence

The initial physical layout, request shapes, row counts, concurrency gate, limitations, and
reevaluation triggers are recorded in
[the schema-selection report](history-metrics-schema-selection.md). The first governed heuristic
must still benchmark its own request shapes against its planning budget before production use.

## References

- Xerial SQLite JDBC: <https://github.com/xerial/sqlite-jdbc>
- Xerial usage/native loading: <https://github.com/xerial/sqlite-jdbc/blob/master/USAGE.md>
- SQLite WAL behavior: <https://www.sqlite.org/wal.html>
