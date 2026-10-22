# Partial-consumption and decode-failure confidence probe

Status: PARKED on a concrete stream-lifetime counterexample. This is an
unreleased observation fixture, not a cleanup implementation or production fix.

The probe wraps the existing research cache codec and reports stream open,
record progress and close through an explicitly addressed driver RPC endpoint.
It deliberately does not register task-completion cleanup or close streams on
read exceptions, because that would hide the contract being tested.

Run through MCP docker_exec using an already verified Spark 3.5.5 primary run:

~~~bash
python tools/pcbs-blockmanager-codec-spike/confidence/partial_failure/run_partial_failure.py \
  --base-run /data/pcbs-oom/blockmanager-codec-spike/root-acceptance355 \
  --spark-home /home/roberte/src/spark_3.5.5 \
  --output-dir /data/pcbs-oom/blockmanager-codec-confidence/F/my-probe
~~~

The runner executes two fresh driver/two-executor JVM applications. Its expected
current exit is nonzero because the required ownership checks fail.

- Partial consumption: the first action on a persisted DataFrame is take(1).
  Spark may fully unroll and publish that cache partition; this is permitted.
- Decode failure: first fill the cache, then inject exactly one read exception
  after one successfully decoded record. Spark's normal task retry must succeed.

Both cases independently validate all later data, SQL row/byte statistics,
actual BlockManager locations/status and complete per-partition cached contents.
Each partition contains four records, 500 rows and 4,032 encoded disk bytes.

Worker evidence at /data/pcbs-oom/blockmanager-codec-confidence/F/worker1 shows
these behavior checks pass in both cases. The independent root repeat at
/data/pcbs-oom/blockmanager-codec-confidence/F/root-verification agrees.
Partial consumption and the failed decoder each lack a successful close
notification for one read stream after one record / 1,024 logical bytes.
Normal fully consumed streams close after four records / 4,000 bytes.
The decode-failure case has one ExceptionFailure followed by successful retry.
Counters are captured after completed actions, before unpersist and shutdown.
The assertion against that snapshot fails after normal application shutdown;
this does not establish the stream state after shutdown. The close notification
is sent after the underlying close returns, so it measures successful completion.

The decisive existing paths are Spark 3.5.5 Serializer.scala
DeserializationStream.asIterator (around 164), which closes on EOF, and
BlockManager.getLocalValues (around 929), whose CompletionIterator cleanup runs
on exhaustion. A codec cannot assume its stream.close is always called after
partial consumption or a decoder exception. This does not prove a permanent
file-descriptor leak: GC or process teardown may release underlying resources.
It does prove absence of the deterministic codec close callback required here.

No acceptance criterion was weakened, no production/shared source was changed,
and no native payload work was added. Decide ownership of task-completion and
exception cleanup before implementing a remedy.
