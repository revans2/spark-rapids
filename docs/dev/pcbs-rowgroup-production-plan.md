# Parquet cache production tracker

Status: IN PROGRESS (2026-10-26 follow-up). Only the architect marks work DONE.
Use MCP docker_exec for every project operation, worker handoff and checkpoint.
Work in the main checkout on pcbs-rowgroup-production; no new worktrees.

Human decisions (2026-10-24): both CPU and GPU writers are in scope because CPU
records could still approach the large input budget. Add an internal, undocumented
cache-record byte target (default 8 MiB) so tests can use small data. Remove the
specified storage/compression matrix, partial-unpersist and filter tests; retain
focused size, type and CPU/GPU read coverage. These supersede the former exclusions.

RELEASED: PCBS CPU/GPU read/write behavior, supported types, projection/reordering,
row counts, compression, storage levels, ParquetCachedBatch(numRows, Array[Byte])
and sizeInBytes, and ordinary Parquet scan behavior (present on main/deployments).
UNRELEASED: row-group splitting, read bundling, the internal conf and branch-added
tests; change directly without compatibility shims. No Spark changes or Spark
OFF_HEAP storage. Escalate unavoidable released-contract changes.

1. Shared internal target: READY FOR REVIEW. Internal
   spark.rapids.sql.cache.serializer.rowGroupSizeBytes defaults to 8 MiB (min 1 KiB).
   Both GPU entry paths capture query conf and pass the target explicitly; large
   GPU input budgets and cuDF's default row limit are preserved. Helper callers
   were updated together, without compatibility overloads or public documentation.
2. CPU cap: READY FOR REVIEW. The existing loop uses min(existing budget, target),
   Long accounting and first-row admission, avoiding the oversized-row stall.
   Initial output allocation is capped too. UnsafeRows use actual row bytes;
   generic rows retain schema estimates. The 2026-10-26 follow-up removes eager
   partition queuing. Neither CPU nor GPU targets guarantee a hard encoded bound.
3. Focused tests: READY FOR REVIEW. Four Scala cases check real writes and exact
   data: GPU 20k Longs at 16 KiB, records <= 2 * target; CPU target/global budgets
   16 KiB/1 MiB and 32 KiB/16 KiB, records <= budget + 4096; three oversized 2 KiB
   strings at a 1 KiB target each make a one-row record. Comments explain the
   data-specific fragment/footer allowances; assertions were not loosened.
   Python uses 20,001 rows, a 1 MiB internal target and a 1 GiB GPU target; checks
   nested/binary/decimal/timestamp/null values, projection/reordering, CPU/GPU reads,
   actual record count, row totals and sizes <= 2 * target + 64 KiB. The existing
   converted-row CPU case verifies multiple records at a 1 KiB internal target.
   Removed the authorized storage/compression matrix, partial-unpersist, filter
   checks and EqualTo/Remainder/LessThan allowances, and unused 131k-row narrow shape.

Follow-up (2026-10-26): no unresolved spec contradiction. The authorized lazy CPU
change supersedes the earlier cap-only scope. All released data contracts above
remain unchanged; no compatibility adapter is needed for the branch's eager loop.
- Uncompressed size guard: READY FOR REVIEW. Compression is false in the focused
  Python fixture. Baseline: 19 records, max 1,044,266 bytes, PASS (7.099s test).
  Scratch writer-only 8 MiB mutation: 4 records, max 4,882,036 bytes, expected FAIL
  at the unchanged 2,162,688-byte size assertion; row count and multi-record checks
  passed first. The test's 1 MiB assertion target stayed unchanged. Scratch file
  restored and compared with main. Evidence: size-baseline.xml, size-mutant.xml
  and size-mutant.stdout.log under the follow-up directory below.
- Lazy CPU records: IMPLEMENTED, runtime verification pending. Iterator-owned carry
  state replaces the partition queue; the host-columnar wrapper also stays lazy.
  New 200-row test checks no reads on hasNext, only one record plus one carried row
  on first next, then exact full row order/values. Existing expectations unchanged.
- Acceptance: focused Scala suites and full cache_test.py with PCBS on Spark 3.5.5.
  Park if any existing CachedBatchWriterSuite expectation needs more than timing changes.
  Evidence: /data/pcbs-oom/pcbs-rowgroup-followup2. Commit each task with this tracker.

Previous verification (2026-10-24), evidence E=/data/pcbs-oom/pcbs-rowgroup-followup:

| Check | Result | Evidence under E |
| --- | --- | --- |
| Default Spark 3.3.0 verify | 59 Scala + 9 Java passed; 5m09s reactor, 46s Scala | default330-tests.log |
| Spark 3.3.4 package | PASS, 1m44s; compilation/package only | package334.log |
| Spark 4.0.1 / Scala 2.13 package | PASS, 1m06s; compilation/package only | package401.log |
| Spark 3.5.5 runtime artifacts | PASS, 4m02s | package355.log |
| Full cache_test.py, PCBS, default parallel | 620 passed, 4 default large-data skips; 84.154s, 8 workers | cache-default-parallel.xml, .stdout.log, .stderr.log |
| New GPU Python test | PASS, 4.009s; 19 records, max 73,647 encoded bytes | XML properties cached_record_count / max_cached_record_bytes |

Commands below show the acceptance selections; logs retain output. Maven calls
also used -nsu -ntp, -l for the listed log, and separate -DsecondaryCacheDir paths.
The default verify uses the temporary mvn-with-code-cache.sh under E, which only
appends -XX:ReservedCodeCacheSize=512m to MAVEN_OPTS, preserving existing options.

```bash
# Build cwd: /data/pcbs-oom/production-cleanup-verify/source
E=/data/pcbs-oom/pcbs-rowgroup-followup
S=com.nvidia.spark.rapids.parquet.ParquetCachedBatch,com.nvidia.spark.rapids.parquet.ParquetBufferConsumerSuite,com.nvidia.spark.rapids.CachedGpuBatchIteratorSuite,com.nvidia.spark.rapids.CachedBatchWriterSuite
$E/mvn-with-code-cache.sh -pl tests -am verify -DwildcardSuites="$S" -Drapids.parallelUnitTests=false
rapids-plugin-mvn -Dbuildver=334 -pl sql-plugin -am package
rapids-plugin-mvn -f scala2.13/pom.xml -Dbuildver=401 -pl sql-plugin -am package
$E/mvn-with-code-cache.sh -Dbuildver=355 -pl dist,integration_tests -am package

env -u TEST -u TEST_TAGS -u TEST_PARALLEL -u MAX_PARALLEL -u TEST_TYPE \
  -u RANDOM_SELECT -u RANDOM_SELECT_SEED -u RUN_TEST_PARAMS -u PYTEST_ADDOPTS \
  -u SKIP_TESTS -u LOCAL_JAR_PATH -u INTEGRATION_TEST_VERSION_OVERRIDE -u PLUGIN_JAR \
  JAVA_HOME=/usr/lib/jvm/java-17-openjdk SPARK_HOME=/home/roberte/src/spark_3.5.5 \
  TESTS=cache_test.py \
  PYSP_TEST_spark_sql_cache_serializer=com.nvidia.spark.ParquetCachedBatchSerializer \
  ./integration_tests/run_pyspark_from_build.sh --durations=20 -rs \
  --junitxml="$E/cache-default-parallel.xml" --log-file="$E/cache-default-parallel.pytest.log"
```

All required ParquetCachedBatch*Suite, ParquetBufferConsumerSuite,
CachedGpuBatchIteratorSuite and CachedBatchWriterSuite cases passed. Scalastyle,
license and scheduled build checks remained enabled. Builds used the existing
tracked-source export to exclude unrelated untracked files from license checks;
all six changed implementation/test files were independently compared byte-for-byte
with the main checkout. No worktree was created. Independent worker review and
lead source/output inspection found no blockers; git diff --check passed.

Corrections preserved inputs, budgets and assertions: the existing Mockito writer
fixture retained every empty output buffer; stubOnly() removes that unused history.
The new direct GPU fixture initially wrote original column names; it now uses the
production cached-schema helper. Logs default330-verify.log and
default330-verify-final.log retain those failures. default330-verify-retry.log
records a compiler run stopped after its JIT code cache filled; the Maven-only
cache increase leaves forked test settings unchanged. Python teardown emitted
Py4J closed-log-stream warnings after successful tests. No production fix was
needed for these verification issues.

Prior cleanup removed 103 research tools/36 documents and shared reader lifecycle,
footer/stream/task cleanup. Historical reports remain in Git; raw evidence is under
/data/pcbs-oom. Databricks dependencies/runtimes remain unavailable; no vendor
validation claim. Park any non-fixture CPU regression, unaffordable GPU fragment,
unavoidable released-contract change or unresolved contradiction.
