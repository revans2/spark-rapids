# Parquet cache production tracker

Status: READY FOR REVIEW. Only the architect marks work DONE.
Use MCP docker_exec for every project operation, worker handoff and checkpoint.
Main checkout/branch: pcbs-rowgroup-production. No new worktrees.

Purpose: keep encoded cache records small without reducing GPU input/read targets.
Human decisions (2026-10-24): include CPU and GPU writers; internal, undocumented
8 MiB record target; remove the specified storage matrix, partial-unpersist and
filter tests. Follow-up (2026-10-26) explicitly authorizes lazy CPU encoding and an
uncompressed size/mutation test, superseding the earlier cap-only scope. No spec
contradiction or unresolved design decision was found.

RELEASED: PCBS CPU/GPU read/write behavior, types, projection/reordering, row counts,
compression, storage levels, ParquetCachedBatch(numRows, Array[Byte])/sizeInBytes
and ordinary Parquet scan behavior (present on main/deployments). Preserve these.
UNRELEASED: branch-added splitting/bundling, internal target and tests; change
without compatibility scaffolding. No Spark changes or Spark OFF_HEAP storage.

1. Uncompressed size guard: READY FOR REVIEW. The existing GPU fixture now sets
   spark.sql.inMemoryColumnarStorage.compressed=false. Its 20,001 rows, 1 GiB GPU
   input target, 1 MiB cache target, type/projection/CPU/GPU-read checks and encoded
   size bound (2 * target + 64 KiB) are unchanged. Baseline and mutation results:

   | Writer target | Records | Largest encoded record | Result |
   | --- | --- | --- | --- |
   | 1 MiB | 19 | 1,044,266 bytes | PASS |
   | 8 MiB, scratch only | 4 | 4,882,036 bytes | Expected size-assertion failure |

   Only the scratch writer configuration changed; the assertion's 1 MiB target
   and 2,162,688-byte bound stayed fixed. Record/row-count checks passed before
   the mutation failed. Scratch Python was restored and compared with main.
   Evidence: size-baseline.xml, size-mutant.xml, size-mutant.stdout.log;
   size-runs-results.json records exact commands. Harness-generated DATAGEN_SEED
   values differed; fixture data is deterministic range/SQL expressions, not random.
2. Lazy CPU writer: READY FOR REVIEW. Each next() encodes one record; iterator-owned
   carry state replaces the partition queue. The host-columnar wrapper consumes
   that iterator directly, without .toList. Row order, copied mutable carry rows,
   first oversized-row admission and zero-column behavior are preserved. New
   200-row Scala case proves hasNext consumes no rows, first next consumes only its
   record plus one carried row, then checks all decoded values/order. Every existing
   CachedBatchWriterSuite expectation passed unchanged; no park condition occurred.
3. Verification: READY FOR REVIEW. Focused Scala and full Python acceptance
   passed on Spark 3.5.5, as did both cross-version builds. Independent review and
   lead inspection of source, logs and XML found no blocker.

Evidence E=/data/pcbs-oom/pcbs-rowgroup-followup2 (2026-10-26):

| Check | Result | Evidence under E |
| --- | --- | --- |
| Spark 3.5.5 verify | 60 Scala + 9 Java passed; reactor 6m25s, Scala 45.798s | verify355.log |
| Full cache_test.py, PCBS | 620 passed, 4 default large-data skips; 94.835s, 8 default workers | cache-default-parallel.xml / .stdout.log / .stderr.log |
| Uncompressed GPU Python case | PASS, 5.860s; count/max match baseline above | XML record properties |
| Spark 3.3.4 SQL package | PASS, 1m07s; compile/package only | package334.log |
| Spark 4.0.1 / Scala 2.13 SQL package | PASS, 53.743s; compile/package only | package401.log |

Commands, from /data/pcbs-oom/production-cleanup-verify/source (tracked-source build
export; changed implementation/test files compared byte-for-byte with main):

```bash
E=/data/pcbs-oom/pcbs-rowgroup-followup2
MVN=/data/pcbs-oom/pcbs-rowgroup-followup/mvn-with-code-cache.sh
S=com.nvidia.spark.rapids.parquet.ParquetCachedBatch,com.nvidia.spark.rapids.parquet.ParquetBufferConsumerSuite,com.nvidia.spark.rapids.CachedGpuBatchIteratorSuite,com.nvidia.spark.rapids.CachedBatchWriterSuite
$MVN -Dbuildver=355 -pl dist,tests,integration_tests -am verify -DwildcardSuites="$S" -Drapids.parallelUnitTests=false -l "$E/verify355.log"
$MVN -Dbuildver=334 -pl sql-plugin -am package -DwildcardSuites="$S" -Drapids.parallelUnitTests=false -l "$E/package334.log"
$MVN -f scala2.13/pom.xml -Dbuildver=401 -pl sql-plugin -am package -DwildcardSuites="$S" -Drapids.parallelUnitTests=false -l "$E/package401.log"

env -u TEST -u TEST_TAGS -u TEST_PARALLEL -u MAX_PARALLEL -u TEST_TYPE \
  -u RANDOM_SELECT -u RANDOM_SELECT_SEED -u RUN_TEST_PARAMS -u PYTEST_ADDOPTS \
  -u SKIP_TESTS -u LOCAL_JAR_PATH -u INTEGRATION_TEST_VERSION_OVERRIDE -u PLUGIN_JAR \
  JAVA_HOME=/usr/lib/jvm/java-17-openjdk SPARK_HOME=/home/roberte/src/spark_3.5.5 \
  TESTS=cache_test.py \
  PYSP_TEST_spark_sql_cache_serializer=com.nvidia.spark.ParquetCachedBatchSerializer \
  ./integration_tests/run_pyspark_from_build.sh --durations=20 -rs \
  --junitxml="$E/cache-default-parallel.xml" --log-file="$E/cache-default-parallel.pytest.log"
```

Maven also used -nsu -ntp and separate Zinc cache paths under
/data/pcbs-oom/pcbs-rowgroup-followup. The temporary wrapper preserves MAVEN_OPTS
and adds only a 512 MiB compiler code cache; forked test settings are unchanged.
Scalastyle, licenses and scheduled checks stayed enabled; no assertions were
weakened. Python's four skips are the default large-data exclusions; Py4J teardown
still emits closed-log-stream warnings after success. Databricks runtime validation
is unavailable. CPU generic rows use schema size estimates; cuDF cuts at page
fragments, so the target is not a universal hard encoded-size limit.

Earlier cleanup removed 103 research tools/36 documents. Earlier build/results and
fixture corrections remain in Git and /data/pcbs-oom/pcbs-rowgroup-followup.
Park for changed existing writer expectations beyond call timing, released-contract
regressions or unresolved spec contradictions. Commit each task with this tracker;
mark READY FOR REVIEW, not DONE.

Footer/reuse follow-up (2026-10-26), READY FOR REVIEW: user authorizes removing
redundant footer validation and auditing scan/cache overlap. Parquet validates
footers; cache reads its footer position once per input. Shared column relocation
preserves distinct dictionary/data offsets, correcting the existing scan metadata
error, with an actual scan-entry regression test. Cache retains source-range and
native-slice checks. Remove the unused LogicalFile.schema field; keep separate
scan I/O and cache slice ownership/retry loops. No release-contract change intended.
Workers and lead confirm MCP docker_exec only; independent source review is clear.
Review caught an incorrect hasDictionaryPage substitution; the helper now uses
positive dictionary offsets, preserving the scan rule. Initial verify ran 62 Scala
cases: 60 passed; two cleanup tests expected the removed wrapper's exception.
Those now assert Parquet's specific tail-magic error; buffer/handle checks remain.

Evidence E=/data/pcbs-oom/pcbs-rowgroup-reuse, same tracked-source export and
Maven wrapper above; changed source/tests copied and compared byte-for-byte:

| Check | Result | Evidence under E |
| --- | --- | --- |
| Full cache_test.py, PCBS, default eight workers | 620 passed, four default skips; 101.066s | cache-default-parallel.xml / .stdout.log |
| GPU row-group case | 19 records, max 1,044,266 bytes; 5.268s | XML properties |
| Mixed dictionary/plain and coalescing scan reads | 32 passed, no skips; 22.58s | scan-dictionary.xml / .stdout.log |
| Spark 3.5.5 verify | 62 Scala + 9 Java passed; reactor 6m57s, Scala 45.060s | verify355.log |
| Spark 3.3.4 SQL compile/package | PASS, 53.841s | package334.log |
| Spark 4.0.1 / Scala 2.13 SQL compile/package | PASS, 49.038s | package401.log |

355 uses the preceding verify command with GpuParquetUtilsSuite appended to S.
Python uses the preceding full-cache command with this E. Scan command uses the
same clean environment and launcher, TESTS=parquet_test.py,
TEST='parquet_read_mixed_dictionary_plain_encoding or parquet_read_coalescing_multiple_files',
without the PCBS serializer override, and --durations=10 -rs
--junitxml="$E/scan-dictionary.xml" --log-file="$E/scan-dictionary.pytest.log".
No benchmark claim is made for these acceptance timings. Initial failure is
retained in verify355-initial.log. Lead independently inspected final logs/XML;
all required checks passed, with no weakened assertions. No further audit blocker.
The cross-version results are builds, not runtime suite runs.

Entire production patch, including tests and tracker:
`git diff github/main...pcbs-rowgroup-production`
