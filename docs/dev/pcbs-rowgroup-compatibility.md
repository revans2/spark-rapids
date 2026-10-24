# PCBS row-group patch compatibility

The patch changes common RAPIDS sources. It requires no Spark-core changes, new public
configuration or Spark `OFF_HEAP` storage. Existing cache/serializer/StorageLevel contracts
are released and must be preserved; the new private helpers are unreleased.

**All-target readiness is not established.** The gates below cover specific targets;
source sharing and classfile compatibility do not prove every supported runtime.

## Release matrix and dependency authority

The release profiles in [the root POM](../../pom.xml),
[the Scala 2.13 POM](../../scala2.13/pom.xml), and their
[`get_buildvers.py`](../../build/get_buildvers.py) enumeration contain **42 Scala/build
pairs across 32 distinct release targets**:

| Build targets | Scala | Actual OSS Parquet dependency |
|---|---|---|
| 330–334 | 2.12 | 1.12.2 |
| 340–344 | 2.12 | 1.12.3 |
| 350–359 | 2.12 and 2.13 | 1.13.1 |
| 400–404 | 2.13 | 1.15.2 |
| 411–413 | 2.13 | 1.16.0 |
| 420 | 2.13 | 1.17.0 |
| 350db143: DBR 14.3 | 2.12 | Vendor coordinate `3.5.0-databricks-143` |
| 400db173: DBR 17.3 | 2.13 | Vendor coordinate `4.0.0-databricks-173` |
| 410db183: DBR 18.3 | 2.13 | Vendor coordinate `4.1.0-databricks-183` |

For OSS builds, [shim-deps](../../shim-deps/pom.xml) imports Spark's parent BOM.
The cached Spark SQL POMs explicitly declare the Parquet versions above.
The root `parquet.hadoop.version` property selects test artifacts; it is **not**
the production dependency authority. For example, target 420's test property is
1.15.2 while Spark SQL depends on 1.17.0.

The [Databricks BOM](../../shim-deps/databricks/pom.xml) uses vendor-versioned
dependencies. Their underlying Parquet versions and behavior remain unverified
without those jars. Standard OSS jars cannot substitute for vendor validation.

Manual target 500 (`5.0.0-SNAPSHOT`, Scala 2.13) is enforcer-admitted through
`sparkMaster`, but is outside the release enumeration. It needs separate validation
if included in acceptance. Older annotation entries such as 330db/332db do not add
release targets.

## Affected API boundaries

- `GpuParquetUtils.newBlockMeta` delegates to existing shims: spark330 for 3.3.x,
  spark340 for every later target, including DBR. `BlockMetaData` row-index-offset
  methods are absent in Parquet 1.12.2 and present from 1.12.3; the shims isolate
  that difference.
- The existing cached-scan trait is empty before Spark 3.5.2, including DBR 14.3.
  The spark352 implementation supplies `InMemoryTableScanLike` for later targets.
  Exact annotation membership selects one implementation per family for all 42 pairs.
  [Source inheritance metadata](shims.md#source-inheritance-metadata) does not replace
  that selection or establish runtime compatibility.
- New framing code reuses the scan's `ColumnChunkMetaData.get` overload and
  existing Parquet footer/input APIs. Classfile inspection found all 34 required
  method descriptors in cached OSS Parquet 1.12.2, 1.12.3, 1.13.1, 1.15.2, 1.16.0
  and 1.17.0, with no new abstract input-stream obligations.
- The chunked-reader wrapper is common RAPIDS code. The pinned cuDF Java API
  provides the host-buffer-array reader constructor and row-group writer options.
  These APIs are not specific to Spark 3.5.5.

**Classfile API/ABI evidence is not compilation, behavior or runtime validation.**

## Selected byte-target source gates

The selected runtime policy is `fa29f4d90c`, using an 8 MiB uncompressed target
and cuDF's default row cap. Root verified that all tracked paths outside docs
match the measured `d9ba8787f6` tree, including production, tests and build files;
artifacts retain their original source identities. The optional adjacent-view
prototype is excluded. Evidence is under the production evidence root
`/data/pcbs-oom/pcbs-rowgroup-production/`.

| Target | Selected-source evidence | Status |
|---|---|---|
| 330 / Scala 2.12 | Fresh normal verify: 53 Scala and nine Java tests; root replayed reports and checked all 36 implementation classes and 237 actual target-private classes against fresh packaged bytes | READY FOR REVIEW |
| 355 / Scala 2.12 | Independently accepted d9ba full 53-plus-nine build and Java/Kryo SQL lanes, nine cases each at 4 GiB; explicit non-doc source equivalence to fa29 | READY FOR REVIEW for these gates |
| 340 / Scala 2.12 | Fresh normal fa29 verify: 53 Scala and nine Java tests; root independently replayed reports, tracked source, fresh implementation classes and all 237 actual target-private classes | READY FOR REVIEW |
| 350 / Scala 2.13 | Fresh normal fa29 verify: 53 Scala and nine Java tests; root independently replayed reports, tracked source, fresh implementation classes and all 237 actual target-private classes | READY FOR REVIEW |
| 401 / Scala 2.13 | Fresh fa29 normal verify at `crossversion/selected-verify401-001` | RUNNING |
| 413 / Scala 2.13; 420 / Scala 2.13 | Prepared clean fa29 worktrees and exact 53-plus-nine focused boundary owners | Pending actual execution |
| 39 OSS compile/package pairs | Prepared serial grouped owners; compile/package only, no runtime credit | Pending actual execution |
| Three DBR pairs | Real vendor dependency-resolution/compile attempts and matching runtime coverage required; no OSS substitution | Not yet attempted |
| 330 / 401 Java and Kryo SQL | Four prepared selected-source nine-case lanes | Pending actual execution |

The selected330 proof is `crossversion/selected-verify330-001-accepted.json`:
3,094 tracked regular files plus the explicit Gitlink, exact retained test-report
equality, actual Spark330/Parquet classpaths, fresh implementation classes and
all classes from its actual target-specific private dependency. The shared JNI
matches the frozen common runtime. Private dependencies are Scala/Spark-specific;
this does not assert their bytes equal the Spark355 dependency. The source
identity receipt is `support-audit/final-byte8-source-equivalence001.json` and
accepted d9 build evidence is `support-audit/root-byte-builds004.json`.

Remaining gates are explicit; this table does not establish all-target readiness.
Missing locally cached vendor artifacts are not a recorded resolver failure.
The older observations below remain historical and cannot be relabeled as
selected-source validation.

## Historical c385 gates and original follow-up scope

| Target | Observed gate |
|---|---|
| 330 / Scala 2.12 | Maven verify: 52 Scala and 9 Java tests passed; fresh Java/Kryo SQL lanes each passed 9 tests at 4 GiB |
| 355 / Scala 2.12 | Same focused verify passed; local Java and Kryo SQL lanes each passed 9 tests |
| 401 / Scala 2.13 | Same focused verify passed; Java/Kryo SQL lanes each passed 9 tests at 4 GiB; test classpath main Parquet 1.15.2 |

The 330 test JVM classpath has main Parquet 1.12.2. On 401, column-test/Avro
artifacts at 1.13.1 are separate from the main 1.15.2 libraries.
A recorded classpath does not by itself establish a loaded class's code source.
The 355 Java SQL lane actually used a 1 GiB heap; the Kryo lane used 4 GiB.
These are correctness gates, not matched performance measurements.

Minimum additional boundary gates are 340/Scala 2.12, 350/Scala 2.13
(pre-3.5.2 scan trait), 413/Scala 2.13, 420/Scala 2.13, and each of the three DBR
targets. Run the six focused suites through their actual Maven test classpaths;
an installed external Spark distribution is not required for those suites.
Normal supported-target compilation/packaging should cover all 42 release pairs.

Selected OSS Spark and Parquet jars are cached, but most additional private
classifiers are not; complete reactor resolution is not proven. Required DBR
vendor artifacts and runtimes are absent locally. DBR builds and generated-data
SQL checks therefore remain pending, along with the additional OSS boundaries.

Raw audit evidence is under
`/data/pcbs-oom/pcbs-rowgroup-production/support-audit/`:
`report.json`, `inventory.json`, `shim-mapping.json`, and `api-boundaries.json`.
Verify evidence is in `build010/` and
`crossversion/crossverify{330,401}-001/`; the latter retain
`resolved-test-classpath.json`. Local SQL evidence is in
`integration-java003/` and `integration-kryo002/`. Cross-version SQL evidence is
`crossversion/sql{330,401}-{java,kryo}002/` and `sql-queue540-002/`.
Root independently reparsed all four raw JUnit reports, actual JVM heap/main-class
evidence and completed Spark event environments: `support-audit/root-cross-sql-recheck001.json`.
All 36 tests passed with zero failures/errors/skips. The original 330 Java001
launcher-bootstrap audit false negative is retained, not overwritten.
