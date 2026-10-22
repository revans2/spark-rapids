# Disposable Spark scheduler-token cache mechanism

Research only: stock Spark 3.5.5 / Scala 2.12 and 4.0.1 / Scala 2.13 on Java 17, one fresh local[4] JVM, 1 GiB heap. This is NOT PCBS, Parquet, GPU, spilling, remote cache, reference counting or production ownership. Payloads are raw Long values in direct ByteBuffers; small genuine scheduler tokens live under actual owner RDDBlockIds. All project operations must use MCP docker_exec.

Run serially, using a new output directory each time:

    python3 run_spike.py --spark-home /path/to/spark-3.5.5 --output-dir /path/to/new-evidence
    python3 run_spike.py --spark-home /path/to/spark-4.0.1 --output-dir /path/to/other-new-evidence

The runner detects installed jars or the assembled 3.5.5 jar directory. Other Spark versions are rejected. It compiles common TokenCacheSpike.scala plus one tiny compatibility adapter; only CacheManager's 4.0.1 classic.Dataset boundary differs. No Spark or plugin source is modified, and no new product configuration is introduced.

Each output retains copied sources, exact compile/run argv, SHA256 for all runtime jars, source/runner/compiled artifact hashes, compiler log, raw Spark log, uncompressed/nonrolling event log and parsed results. Exit 0 alone is insufficient: the runner validates all required completion/case markers, 30 JSON diagnostic events, actual row/byte statistics, 56-byte physical tokens/locations, file/tracker shuffle erasure, own-hit/upstream counters, expected two FetchFailed tasks, actual Spark version and normal application end. It also removes the final required marker from an in-memory copy of the log and proves rejection. Old outputs are never overwritten.

Both AQE modes test 10,000 exact Long values, count and sum through the real SQL CachedRDDBuilder. Its supplied statistics function is preserved. Intact tokens prevent upstream recomputation after deletion of four actual shuffle datafiles and clearing four tracker outputs. Removing token+payload permits recomputation. **Payload-only loss while a token remains requires a FetchFailed retry in each mode; failure-free recovery is NOT demonstrated.** Empty partitions and sequential explicit blocking unpersist cleanup are checked.

The implementation intentionally skips super.persist so RDD's private storage field stays NONE; getStorageLevel reports the requested level to the scheduler, compute serves native data, and regular BlockManager stores only truthful small tokens. Token storage uses MEMORY_ONLY for this mechanism even though the requested cache level is MEMORY_AND_DISK. It does not preserve every production StorageLevel policy.

No concurrent-reader retirement, speculative writer safety, plugin-disabled PCBS path, replica/decommission, missing executor recovery, checkpointing or GC cleanup is supplied. ContextCleaner bypasses the overridden unpersist. Direct buffers are disposed only in these sequential checks. Do not turn this into a production patch without the outstanding lifecycle/availability design and independent review. Later Spark sources change internal APIs and are not covered by these runtime passes.

Existing research evidence and earlier failed attempts remain outside this deliverable under /data/pcbs-oom/token-cache-spike. Root independently verifies the package before repository delivery.

Results and version boundaries: [research report](../../docs/dev/pcbs-token-cache-spike.md).
Long-term direction: [upstream storage API proposal](../../docs/dev/pcbs-cache-storage-api-proposal.md).
