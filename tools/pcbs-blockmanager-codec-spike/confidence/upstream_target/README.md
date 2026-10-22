# Pinned upstream-target codec compile confidence

Status: READY FOR REVIEW for compilation; runtime testing is BLOCKED by missing
exact dependencies. This is heap-codec research only. Native storage is optional
and not part of this gate. Spark defaults are released; the hook/carrier and
fixtures are unreleased research. No production sources or installed Spark change.

Target: Spark 4.2.0, revision `32f7299601108917fb01920a54e084595b7b3bf8`,
Scala 2.13.18. Cached target binaries identify that exact revision in
`spark-common-utils`'s `spark-version-info.properties`. No Spark 4.0 classes are
substituted. The two hooks compile against matching Spark 4.2.0 modules.

The generated SQL source preserves the target's partition-keyed materialization
accumulator, completion listener, reset behavior and statistics folds. Only the
existing wrapper's output ClassTag association changes. SerializerManager uses
the same central codec selector hook. Generated test copies select classic
SparkSession, supply the current builder constructor, and assert the target's
materialized row/size methods instead of obsolete global accumulators.

Compile and independently verify:

```sh
python tools/pcbs-blockmanager-codec-spike/confidence/upstream_target/compile_spike_420.py \
  --source-git /home/roberte/src/spark_3.5.5 \
  --classpath-file /data/pcbs-oom/blockmanager-codec-confidence/B/dependencies/compile-classpath.txt \
  --output-dir /data/pcbs-oom/blockmanager-codec-confidence/B/new-compile

python tools/pcbs-blockmanager-codec-spike/confidence/upstream_target/compile_spike_420.py \
  --verify-only --output-dir /data/pcbs-oom/blockmanager-codec-confidence/B/new-compile
```

Output directory must be new. Sources, generated copies, adapter, artifacts,
compile argv and dependency hashes are retained; result/verifier JSON explicitly
say `runtime_tested=false` and `COMPILE_PASS_RUNTIME_BLOCKED`.

The classpath is Maven's target dependency graph with only these unavailable
transitives excluded for compilation, without replacement versions:

- `org.roaringbitmap:RoaringBitmap:1.6.10`
- `com.google.flatbuffers:flatbuffers-java:25.2.10`

Their exact jars are absent from Maven/Coursier/Ivy caches. Official Maven hosts
failed DNS during one bounded resolution attempt. Cached matching Spark modules
and other transitive dependencies suffice for normal compilation of the two
source files and primary fixture; that does not establish a complete runnable
Spark 4.2.0 installation. Dependency POM/settings and resolution logs are retained
under `/data/pcbs-oom/blockmanager-codec-confidence/B/dependencies/`.

Verified worker compile: `/data/pcbs-oom/blockmanager-codec-confidence/B/compile420b`,
empty compiler error/warning log, 188 verified hashes. Runtime, partition-keyed
statistics correctness under execution, remote/replica behavior and performance
on this target remain unverified. Do not infer them from compilation.
