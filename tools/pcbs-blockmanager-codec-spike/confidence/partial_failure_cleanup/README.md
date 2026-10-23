# Task-bound codec stream cleanup prototype

Status: READY FOR REVIEW. Separate follow-up to the preserved, parked original
F counterexample. Research-only codec implementation, not a production Spark or
RAPIDS change, public API or new cleanup protocol. Existing released defaults
remain unchanged; this fixture is unreleased.

The decoder registers Spark's existing task-completion listener when a
TaskContext exists. Normal EOF, decode exceptions, explicit caller close and
that listener share an idempotent synchronized guard. The guard invokes the
underlying close at most once; subsequent requests do not release it again.
Read exceptions remain the primary exception and receive any cleanup exception
as suppressed. Notifications are recorded after successful underlying close.

Two fresh Spark 3.5.5 driver/two-executor applications retain the original F
workload and assertions:

- The first action on DISK_ONLY SQL cache is take(1); its partial decoder closes
  on task completion after one record / 1,024 logical bytes.
- One decoder exception is injected after a record. Immediate cleanup runs;
  Spark's ordinary retry returns exact later data.
- Both complete cached partitions have four records, 500 rows and 4,032 disk
  bytes. SQL statistics, every value and normal full reads remain correct.
- Every observed stream has exactly one successful underlying close before
  unpersist/shutdown. Full task reads also receive a task listener callback;
  EOF, iterator and listener close requests cause no duplicate underlying close.

Bounded reads outside TaskContext include explicit early close (called twice)
and malformed framing cleanup, plus fully consumed remote block reads. No task
listener is assumed for those callers. Partial abandonment of BlockManager's
iterator-only remote API, replica receiver failure cleanup, actual close throwing,
cancellation and concurrent reader safety are not established by this proof.

Existing patterns read before implementation: Spark MemoryStore.scala around
849 uses task completion plus idempotent disposal; ResourceBoundedThreadExecutor
around 353 registers completion cleanup for successful/failed tasks; TestingV2Source
around 166 uses a guarded listener cleanup. Spark Serializer.scala around 164
explains why EOF-only cleanup did not cover the original counterexample.

Run only during a benchmark-free slot, through MCP docker_exec:

```sh
python tools/pcbs-blockmanager-codec-spike/confidence/partial_failure_cleanup/run_cleanup.py \
  --base-run /data/pcbs-oom/blockmanager-codec-spike/root-acceptance355 \
  --spark-home /home/roberte/src/spark_3.5.5 \
  --output-dir /data/pcbs-oom/blockmanager-codec-confidence/F-cleanup/new-run
```

Output directory must be new. Sources, artifact/runtime hashes, exact argv,
raw logs/events and per-case result/verification JSON are retained.
Worker evidence: `/data/pcbs-oom/blockmanager-codec-confidence/F-cleanup/worker1`.
Both cases passed with zero unclosed notifications and exactly one successful
inner close per stream. The failure case has the one expected ExceptionFailure
and successful retry. Original F files and evidence are unchanged.
