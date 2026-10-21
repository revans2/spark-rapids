/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.lang.management.ManagementFactory
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.storage.StorageLevel

/** Standalone cache benchmark; see docs/dev/pcbs-oom-investigation.md. */
object CacheReloadBench {
  private def emit(fields: (String, Any)*): Unit = {
    println("CACHE_BENCH " + new ObjectMapper().writeValueAsString(fields.toMap.asJava))
  }

  private def measure[A](phase: String, iteration: Int)(body: => A): A = {
    emit("event" -> "phase_start", "phase" -> phase, "iteration" -> iteration)
    val memory = ManagementFactory.getMemoryMXBean
    val collectors = ManagementFactory.getGarbageCollectorMXBeans.asScala
    val gcCount = collectors.map(_.getCollectionCount).filter(_ >= 0).sum
    val gcMs = collectors.map(_.getCollectionTime).filter(_ >= 0).sum
    val before = memory.getHeapMemoryUsage.getUsed
    val peak = new AtomicLong(before)
    val running = new AtomicBoolean(true)
    val sampler = new Thread(new Runnable {
      override def run(): Unit = {
        while (running.get()) {
          val used = memory.getHeapMemoryUsage.getUsed
          var previous = peak.get()
          while (used > previous && !peak.compareAndSet(previous, used)) {
            previous = peak.get()
          }
          Thread.sleep(10)
        }
      }
    }, "cache-benchmark-heap-sampler")
    sampler.setDaemon(true)
    sampler.start()
    val startEpochMs = System.currentTimeMillis()
    val start = System.nanoTime()
    var success = false
    try {
      val result = body
      success = true
      result
    } finally {
      val elapsed = (System.nanoTime() - start) / 1000000.0
      val endEpochMs = System.currentTimeMillis()
      val after = memory.getHeapMemoryUsage.getUsed
      running.set(false)
      sampler.join(1000)
      emit("event" -> "phase", "phase" -> phase, "iteration" -> iteration,
        "success" -> success, "elapsed_ms" -> elapsed,
        "start_epoch_ms" -> startEpochMs, "end_epoch_ms" -> endEpochMs,
        "heap_before" -> before, "heap_after" -> after,
        "sampled_heap_peak" -> math.max(peak.get(), after),
        "gc_count" -> (collectors.map(_.getCollectionCount).filter(_ >= 0).sum - gcCount),
        "gc_ms" -> (collectors.map(_.getCollectionTime).filter(_ >= 0).sum - gcMs))
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Cache reload benchmark").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    val parts = sys.env.getOrElse("CACHE_PARTS", "8").toInt
    val sourceParts = sys.env.getOrElse("CACHE_SOURCE_PARTS", parts.toString).toInt
    val rows = sys.env.getOrElse("CACHE_ROWS", "4000000").toLong
    val columns = sys.env.getOrElse("CACHE_COLUMNS", "16").toInt
    val iterations = sys.env.getOrElse("CACHE_ITERATIONS", "5").toInt
    val sqlScan = sys.env.getOrElse("CACHE_SQL", "true").toBoolean
    val forceGc = sys.env.getOrElse("CACHE_FORCE_GC", "true").toBoolean
    val requireDisk = sys.env.getOrElse("CACHE_REQUIRE_DISK", "true").toBoolean
    require(parts > 0 && rows > 0 && columns > 0 && iterations > 0)
    require(sourceParts >= parts && sourceParts % parts == 0)
    val expectedRows = Math.multiplyExact(rows, parts.toLong)
    val source = spark.range(0L, expectedRows, 1L, sourceParts)
      .selectExpr((0 until columns).map(i => s"rand($i) as c$i"): _*)
    val df = (if (sourceParts > parts) source.coalesce(parts) else source)
      .persist(StorageLevel.MEMORY_AND_DISK)
    val relation = spark.sharedState.cacheManager.lookupCachedData(df).get.cachedRepresentation
    val buffers = relation.cacheBuilder.cachedColumnBuffers
    emit("event" -> "config", "heap_max" -> Runtime.getRuntime.maxMemory(),
      "master" -> sc.master, "rows" -> expectedRows, "parts" -> parts,
      "source_parts" -> sourceParts,
      "columns" -> columns, "iterations" -> iterations,
      "force_gc" -> forceGc, "require_disk" -> requireDisk,
      "serializer" -> spark.conf.get("spark.sql.cache.serializer"),
      "reset" -> sc.getConf.get("spark.serializer.objectStreamReset"),
      "memory_fraction" -> sc.getConf.get("spark.memory.fraction"),
      "batch_target" -> spark.conf.get("spark.rapids.sql.batchSizeBytes", "unset"),
      "sql_batch_rows" -> spark.conf.get("spark.sql.inMemoryColumnarStorage.batchSize"),
      "plan" -> relation.cacheBuilder.cachedPlan.toString())
    def checkCacheStorage(phase: String, iteration: Int): Unit = {
      val info = sc.getRDDStorageInfo.find(_.id == buffers.id).get
      emit("event" -> "cache_storage", "phase" -> phase, "iteration" -> iteration,
        "memory_bytes" -> info.memSize, "disk_bytes" -> info.diskSize,
        "cached_partitions" -> info.numCachedPartitions)
      require(info.numCachedPartitions == parts, s"Missing cached partitions: $info")
      if (requireDisk) {
        require(info.memSize == 0 && info.diskSize > 0, s"Expected all disk cache: $info")
      } else {
        require(info.memSize > 0 || info.diskSize > 0, s"Expected retained cache data: $info")
      }
    }
    try {
      measure("populate", 0) {
        for (p <- 0 until parts) {
          val metrics = sc.runJob(buffers, (it: Iterator[CachedBatch]) => {
            var n = 0L
            var bytes = 0L
            var batches = 0L
            var largest = 0L
            var kind = ""
            val batchRows = new scala.collection.mutable.ArrayBuffer[Int]
            val batchBytes = new scala.collection.mutable.ArrayBuffer[Long]
            it.foreach { b =>
              n += b.numRows
              bytes += b.sizeInBytes
              batches += 1
              largest = math.max(largest, b.sizeInBytes)
              kind = b.getClass.getName
              batchRows += b.numRows
              batchBytes += b.sizeInBytes
            }
            (n, bytes, batches, largest, kind, batchRows.toArray, batchBytes.toArray)
          }, Seq(p)).head
          require(metrics._1 == rows)
          emit("event" -> "partition", "partition" -> p, "rows" -> metrics._1,
            "bytes" -> metrics._2, "batches" -> metrics._3,
            "largest_batch" -> metrics._4, "class" -> metrics._5,
            "batch_rows" -> metrics._6, "batch_bytes" -> metrics._7)
          if (forceGc) System.gc()
        }
      }
      checkCacheStorage("populate", 0)
      emit("event" -> "storage", "description" -> sc.getRDDStorageInfo.mkString(";"),
        "disk_bytes" -> sc.getRDDStorageInfo.find(_.id == buffers.id).get.diskSize)
      // Iteration zero warms the reader and is excluded from performance summaries.
      for (i <- 0 to iterations) {
        if (forceGc) System.gc()
        measure("reload", i) {
          val count = buffers.mapPartitions { it =>
            var n = 0L
            it.foreach(b => n += b.numRows)
            Iterator(n)
          }.collect().sum
          require(count == expectedRows, s"Unexpected row count: $count")
        }
        checkCacheStorage("reload", i)
      }
      if (sqlScan) {
        var expected: Option[Seq[Double]] = None
        for (i <- 0 to iterations) {
          // Fresh execution prevents reuse of a previous action's shuffle map outputs.
          val aggregate = df.selectExpr((0 until columns).map(j => s"sum(c$j) as s$j"): _*)
          if (i == 0) {
            emit("event" -> "sql_plan", "plan" -> aggregate.queryExecution.executedPlan.toString())
          }
          if (forceGc) System.gc()
          val sums = measure("sql", i) {
            val result = aggregate.collect()
            require(result.length == 1)
            (0 until columns).map(result.head.getDouble)
          }
          val scanMetrics = aggregate.queryExecution.executedPlan.collect {
            case node if node.getClass.getSimpleName.contains("InMemoryTableScan") =>
              val metrics = node.metrics.map { case (name, metric) => name -> metric.value }
              require(metrics.getOrElse("numOutputRows", 0L) == expectedRows,
                s"Expected a full cache scan: $metrics")
              s"${node.getClass.getSimpleName}:$metrics"
          }
          require(scanMetrics.nonEmpty, "Missing cache scan in executed plan")
          emit("event" -> "sql_scan", "iteration" -> i, "metrics" -> scanMetrics.mkString(";"))
          require(sums.forall(v => !v.isNaN && !v.isInfinite))
          expected.foreach { old =>
            require(old.zip(sums).forall { case (a, b) =>
              math.abs(a - b) <= math.max(1.0, math.abs(a)) * 1e-10
            }, "Aggregate changed between scans")
          }
          expected = Some(sums)
          checkCacheStorage("sql", i)
        }
      }
      emit("event" -> "complete", "success" -> true)
    } finally {
      spark.stop()
    }
  }
}
