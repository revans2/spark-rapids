# History metrics repository and service plan

Status: proposed implementation plan.

This document records the intended repository split and the later client/server
architecture for history metrics. The first implementation phase is deliberately
limited to refactoring and packaging the code that already exists. It does not
implement REST, JDBC, database schemas, Aether deployment, or authentication.

## Goals

- Make the history metrics contract available to both the public Spark RAPIDS
  plugin and private logical/runtime optimizations without tying the contract to
  a Scala binary version.
- Keep the reusable history metrics implementation in `spark-rapids-private`
  without adding another repository.
- Keep Spark-specific lifecycle and configuration extraction in
  `spark-rapids`.
- Preserve the existing provider discovery, safe no-op fallback, bounded
  operations, asynchronous record path, and provider conformance tests.
- Leave a clear path to a standard REST client, a JDBC-backed REST server,
  Aether-managed deployment, and pluggable authentication.

## Explicit non-goals for the first phase

The repository refactor must not add any of the following:

- an HTTP or REST client;
- an HTTP or REST server;
- PostgreSQL, SQLite, JDBC, or ODBC dependencies;
- database tables or migrations;
- Aether configuration or deployment changes;
- a credential format, authentication mechanism, authorization policy, or TLS
  policy;
- new production metric families or changes to heuristic behavior.

Those are later phases described below so that the repository boundary chosen
now does not prevent them.

## Baseline before this refactor

The original pull request added three Java 8 modules to `spark-rapids`:

- `history-metrics-api`: the data model, `MetricStore`,
  `HistoryMetricsBackend`, provider SPI, catalog, no-op implementation, and
  current-store registration holder;
- `history-metrics-local`: the in-memory implementation, asynchronous
  recording machinery, snapshots, diagnostics, and service-loader provider;
- `history-metrics-tck`: API contract tests and reusable provider-conformance
  tests.

The public SQL plugin contains the Spark-specific owner,
`HistoryMetricsManager`, its configuration selection, plugin lifecycle calls,
and tests. The distribution build puts the API classes at the distribution root
so that the plugin and separately loaded providers see one API class identity.

The original provider SPI accepted a `SparkContext`. That makes the Java method
descriptor independent of Scala 2.12 versus 2.13, but it still couples every
provider to Spark APIs and Spark-version compatibility. The repository move is
the right point to remove that dependency.

Spark's relevant lifecycle ordering is established across the supported 3.3,
3.4, 3.5, and 4.0 lines: `DriverPlugin.init` runs while the `SparkContext` is
being constructed, before the task scheduler supplies the application ID and
application-attempt ID. Spark assigns both values and later invokes
`DriverPlugin.registerMetrics`. The RAPIDS driver plugin already overrides that
callback. Provider activation will therefore move to `registerMetrics`; this is
a decided part of Phase 1 rather than an open design question.

## Target ownership

| Repository | Owns |
| --- | --- |
| `spark-rapids-private` | Java-only history metrics API, backend contract, provider SPI, local/reference implementation, TCK, and eventually the standard REST client and REST/JDBC server |
| `spark-rapids` | Spark configuration registration, extraction of inputs from Spark, provider selection/discovery, plugin lifecycle, installation into `MetricStores`, heuristic producers/consumers, and distribution assembly |
| `aether` | Eventually, configuration translation, server lifecycle/deployment, health integration, and passing the history service endpoint to Spark jobs |
| Database | Eventually, durable history data in history-server-owned tables and migrations |

Aether will not compile against or reimplement the private Java server. It will
consume a versioned server image or executable and communicate through the
versioned REST contract.

## Artifact layout

The first phase adds a standalone Maven subproject to
`spark-rapids-private`:

```text
history-metrics/
    pom.xml                         independent Java parent and aggregator
    api/                            API artifact
    local/                          reference/test provider artifact
    tck/                            reusable conformance artifact
```

The `history-metrics` parent must not inherit from the existing
`cudf-spark-private-parent`. Its children inherit only from the independent
Java parent. Listing `history-metrics` in the private repository reactor is an
aggregation convenience; it must not make these modules Spark-shim modules.
They must:

- be Java-only;
- use unsuffixed, unclassified artifact names;
- contain no Scala dependency;
- contain no Spark dependency after the provider-boundary refactor;
- use ordinary `target/` directories, with no inherited `target.classifier`,
  shimplify, Scala-plugin, or per-build-version behavior;
- have one canonical source/POM tree, with no generated
  `scala2.13/history-metrics-*` mirror;
- build and publish once outside the per-Spark-shim loop;
- be published separately, even when the API is also assembled into the Spark
  RAPIDS distribution.

The private `buildall` script currently selects `core` explicitly for each
Spark shim. Phase 1 must add a separate one-time history-metrics build step; it
must not add history metrics to every build-version profile. The default build
owns compilation, testing, installation, and publication of the unsuffixed Java
artifacts. Scala 2.12 and Scala 2.13 plugin builds consume those same artifacts
and never rebuild or redeploy them.

Later phases add only two runtime deliverables:

```text
history-metrics-rest-client.jar
history-metrics-server.jar / history-metrics-server image
```

The JDBC implementation belongs inside the server deliverable initially. There
is no need to publish separate protocol, SQL SPI, PostgreSQL-provider, or
SQLite-provider jars unless a concrete second consumer later justifies them.

## Phase 1: repository refactor

### 1.1 Establish the Spark-independent provider boundary

Keep the existing metric data model and `MetricStore` /
`HistoryMetricsBackend` separation. Change only the construction boundary
needed to remove `SparkContext` from the private API.

The provider construction signature is deliberately small and uses existing
Java types:

```java
MetricStore open(
    Map<String, String> configuration,
    String applicationId,
    String applicationAttemptId,
    String producerVersion) throws Exception;
```

`applicationAttemptId` is nullable. The other arguments are required. These
fixed inputs are not a `Provenance` instance because `Provenance.writtenAtMs`
is different for each backend write.

The boundary follows these rules:

- Configuration is passed as a defensive immutable copy of
  `Map<String, String>`. The API does not require a configuration wrapper
  class.
- The shared API does not require or perform configuration filtering. The
  caller defines the map contents and the selected provider reads the keys it
  understands. Phase 1 must not invent a filtering rule; any later restriction
  needs a demonstrated compatibility or security requirement.
- The API does not read `SparkConf`, `Hadoop Configuration`, environment
  variables, filesystem secret files, or Spark internals.
- Spark application ID, application-attempt ID, and plugin version come from the
  Spark integration. They are explicit provenance values, not authenticated
  identity.
- `DriverPlugin.init` captures a defensive configuration map and the temporary
  Spark-side state needed to finish initialization. It does not open or install
  the history provider.
- `DriverPlugin.registerMetrics(appId, pluginContext)` is the activation point.
  At that point Spark has assigned both application identifiers. The RAPIDS
  plugin uses the callback's `appId`, reads `sc.applicationAttemptId`, and asks
  `HistoryMetricsManager` to open and install the selected provider with those
  fixed values and the producer version.
- The captured `SparkContext` reference is an implementation detail of the
  public Spark adapter and is released after activation. It never crosses the
  provider API.
- Do not introduce a general `ApplicationIdentityProvider`, polling supplier,
  or provider callback. The provider receives the fixed provenance inputs
  explicitly when it is opened and creates time-stamped `Provenance` for each
  accepted backend write.
- An absent application-attempt ID remains representable because not every
  scheduler supplies one.
- Tests must establish that shutdown and no-op fallback remain correct when
  activation is absent, rejected, or fails during `registerMetrics`.

This is the only intentional API adjustment in the move. Avoid opportunistic
renaming or restructuring.

### 1.2 Add the private modules

Copy the API, TCK, and local implementation into the independent
`history-metrics` subproject in `spark-rapids-private`, preserving package names
and behavior. Add that aggregator once to the canonical root reactor, excluding
it from the generated Scala 2.13 reactor and every build-version profile.

The TCK must test both the backend contract and the provider-visible
`MetricStore` behavior. The local implementation remains the reference
provider used to prove that discovery, lifecycle, batching, deadlines,
retention, summaries, and failure containment still work.

Because the private root POM changes, regenerate and verify the Scala 2.13 build
files using the repository's existing synchronization script. The generated
Scala 2.13 reactor must not contain copies of the history modules. The one
canonical Java build supplies identical artifacts to both Scala variants.

### 1.3 Publish private artifacts before changing the public build

Build and install a snapshot of the new private artifacts with the standalone
history-metrics reactor before changing the public build. Publishing cannot be
left as an assumed consequence of the existing shim build: the checked-in
private build scripts currently build `core`, and no checked-in job explicitly
deploys these new coordinates.

Phase 1 must identify the owner of the private artifact publication job and add
an explicit build/deploy step for the unsuffixed API, local, and TCK artifacts.
The step must publish them once, without a Spark classifier, to the Maven
repository used by the public build. Local development may use `mvn install`,
but shared CI must resolve an actually published snapshot.

Use a dedicated public `history-metrics.version` property, initially aligned
with the private release version. Independent version numbers are not required
for Phase 1, but the dependency must not reuse the Scala-suffixed artifact ID or
Spark classifier. Artifact coordinates in both repositories are currently being
renamed; confirm the final group IDs and artifact IDs when implementing this
step rather than copying transitional coordinates into a lasting contract.

The private change must be independently reviewable and green, and the snapshot
must be resolvable in shared CI, before the public change removes its local
copies. If the external publication job cannot be changed from these
repositories, record its owner and required change as an explicit blocker on
the public PR.

### 1.4 Convert the public repository to a consumer

In `spark-rapids`:

- remove the three history metrics modules and their Scala 2.13 mirror POMs;
- replace the reactor dependency with the separately published, unclassified
  API artifact using `history-metrics.version`;
- retain `HistoryMetricsManager`, plugin lifecycle integration, RAPIDS
  configuration registration, and their tests;
- adapt the manager to extract the agreed plain Java inputs and call the
  Spark-independent provider boundary;
- keep API classes at the distribution root and prevent duplicate copies from
  private aggregate jars;
- update developer documentation to describe repository ownership and artifact
  acquisition accurately.

Run the public repository's Scala 2.13 POM synchronization after its POM
changes.

### 1.5 Preserve a buildable review sequence

Use two coordinated pull requests:

1. Private PR: add the standalone Java project, its one-time build/publication
   plumbing, and publish a consumable snapshot.
2. Public PR: consume that snapshot, retain the Spark adapter, and remove the
   original modules.

Mark the public PR as `Stacked on #NNNN`, referring to the private PR or its
cross-repository tracking reference as supported by the review system. Both PR
descriptions must disclose AI assistance and stop for human review of the full
diff and description before they are opened or updated.

The public PR is blocked on the private artifact being available. Release
versions must be updated together. Do not temporarily duplicate API classes in
the final distribution.

If preserving commit history is useful, move the existing module commits into
the private branch before reducing the public branch. Correct final ownership
and reviewable diffs matter more than preserving every intermediate commit
boundary.

## Phase 1 acceptance criteria

- `history-metrics-api` has no Spark, Scala, JDBC, HTTP-client, server, or
  authentication dependency.
- Its public signatures contain no Spark or Scala types.
- The API, local, and TCK artifacts have no Scala binary suffix or Spark
  classifier and are produced by exactly one canonical Java build.
- The independent history parent does not inherit the Spark-specific output
  directory, `target.classifier`, shimplify, or Scala build machinery.
- The private one-time build and TCK pass, and the per-shim loop does not rebuild
  the history modules.
- The local provider still passes service-loader and provider-contract tests.
- The public plugin builds for Scala 2.12 and 2.13 and for the supported Spark
  shim matrix.
- Provider selection, initialization failure, shutdown, and no-op fallback
  tests pass in the public repository.
- The assembled Spark RAPIDS distribution contains exactly one copy of every
  history API class and can load the private local provider with the expected
  class-loader identity. Enforcement includes the explicit
  `HistoryMetricsProvider.class` distribution-root assertion in
  `dist/maven-antrun/build-parallel-worlds.xml` and the generic
  `dist/scripts/binary-dedupe.sh` check.
- Dependency inspection confirms that no REST, JDBC, PostgreSQL, SQLite, or
  authentication library entered the distribution.
- Shared CI resolves the published private history artifacts without relying on
  a developer's local Maven repository.
- Aether is unchanged.
- No production heuristic is enabled solely by this refactor.

## Phase 2: standard REST client

After the repository move is stable, add a Java-only private client provider
that implements the existing backend contract over a versioned REST protocol.
It is packaged into the Spark RAPIDS distribution so Aether users configure a
provider and endpoint rather than supply application code.

This phase must define:

- a versioned OpenAPI/wire contract for backend information, declaration,
  batched recording, and batched summaries;
- preservation of positional results and per-item statuses;
- typed, lossless dimension encoding;
- compatibility negotiation and request-size limits;
- deadline propagation and failure mapping;
- client queueing/batching interaction with the existing non-blocking
  `MetricStore` framework;
- redacted diagnostics and health/error observability.

Exact endpoint names and user-facing configuration keys are intentionally not
specified by this plan. They require an explicit contract review.

## Phase 3: REST/JDBC server

Add a private executable server and container image. Its REST handlers adapt the
wire contract to `HistoryMetricsBackend`; its JDBC implementation remains an
internal server component.

Support:

- PostgreSQL through the PostgreSQL JDBC driver for production;
- SQLite through a maintained SQLite JDBC driver for local and single-node
  development;
- independent migrations owned by the history server;
- PostgreSQL connection pooling;
- serialized/conservatively pooled SQLite access;
- readiness and liveness endpoints;
- retention enforcement and bounded queries.

Use JDBC, not ODBC. JDBC is the JVM-native database interface and avoids ODBC
driver-manager, DSN, and native deployment requirements. JDBC does not make SQL
dialects identical, so PostgreSQL and SQLite require separately tested
migrations and small internal dialect-specific paths.

For PostgreSQL, the history service may share Aether's PostgreSQL installation
while owning separate tables, preferably in a separate schema and with
separately scoped database permissions. It does not share Aether's Peewee
models, migration history, or connection pool.

For SQLite, default to a separate history database file on the same managed
volume. Sharing Aether's exact SQLite file is optional local-mode behavior only
and would require disjoint table names, serialized migrations, WAL-compatible
settings, and explicit lock-contention testing. SQLite is not a production
multi-replica backend.

## Phase 4: Aether deployment integration

Aether adds only the integration needed to operate the private server artifact:

- local process/container lifecycle;
- a Kubernetes Deployment and ClusterIP Service in the Aether Helm release;
- database configuration translation and secret/identity mount plumbing;
- readiness dependency and diagnostic reporting;
- injection of the history-service endpoint and non-secret client settings into
  launched Spark jobs;
- version pinning and an integration test against the supported server image.

A separate production Deployment is preferred over one server sidecar per
Aether API replica. A local SQLite deployment may use a colocated container and
shared volume.

Aether must not duplicate the REST handlers, JDBC implementation, tables,
migrations, or Java metric model in Python.

## Phase 5: pluggable transport authentication and authorization

Authentication is required before production network exposure but is not part
of the repository refactor.

The later design must provide:

- a client request-authentication extension point;
- a server authentication/authorization filter;
- server identity verification by the client;
- explicit authorization for read, write, administration, and health
  operations as appropriate;
- credential refresh without restarting long-running jobs where the selected
  mechanism requires it;
- redaction and a prohibition on credential material in logs and diagnostics.

The extension points must not assume mTLS, bearer tokens, or any one cloud.
Candidate implementations may use ambient workload identity such as AWS IAM,
platform-delivered tokens, certificates, or another approved mechanism.
Ordinary configuration should select the mechanism and identify non-secret
resources; the deployment platform or sideband supplies credential material.
The trust chain and Aether-to-job delivery path must be proven for each
implementation before it is accepted.

## Cross-repository validation

Each later phase should keep the existing TCK as the semantic authority and add
integration coverage at the new boundary:

| Boundary | Required validation |
| --- | --- |
| Private API/local | Unit tests, public API-surface checks, provider TCK |
| Private client/server | REST compatibility tests and the same provider TCK through a real server |
| PostgreSQL | Migration, concurrency, deadline, retention, and restart tests |
| SQLite | Migration, locking, restart, corruption-safe shutdown, and local-volume tests |
| Public Spark integration | Provider discovery, class-loader identity, lifecycle, no-op fallback, Scala 2.12/2.13, Spark shim matrix |
| Aether | Deployment readiness, endpoint delivery to a real job, server-version mismatch, unavailable-service fallback |
| Authentication | Mechanism-specific positive, negative, expiry, refresh, identity-verification, and redaction tests |

## Decisions intentionally deferred

The repository move does not decide:

- REST framework or HTTP client library;
- exact REST resources, paths, or JSON representation;
- database table layout and indexes;
- migration library;
- whether local Aether mode shares one SQLite file or only one volume;
- authentication mechanism;
- authorization tenancy model;
- production service topology beyond the preference for an independent
  Deployment;
- exact user-facing configuration names.

Those decisions must be made with implementation evidence in their respective
phases. None is required to complete Phase 1.
