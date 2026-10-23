/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.lang.management.ManagementFactory
import java.nio.file.{Files, Paths}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.execution.columnar.DefaultCachedBatch
import org.apache.spark.storage.StorageLevel

/** Standalone real-row CPU/PCBS comparison; never pads logical rows with synthetic payloads. */
object RealCacheBench {
  private val mapper = new ObjectMapper
  private def emit(fields: (String, Any)*): Unit =
    println("REAL_CACHE " + mapper.writeValueAsString(fields.toMap.asJava))

  private def measured[A](phase: String, iteration: Int)(body: => A): A = {
    val memory = ManagementFactory.getMemoryMXBean
    val collectors = ManagementFactory.getGarbageCollectorMXBeans.asScala
    def gcMs: Long = collectors.map(_.getCollectionTime).filter(_ >= 0).sum
    def gcCount: Long = collectors.map(_.getCollectionCount).filter(_ >= 0).sum
    val beforeMs = gcMs
    val beforeCount = gcCount
    val peak = new AtomicLong(memory.getHeapMemoryUsage.getUsed)
    val running = new AtomicBoolean(true)
    val sampler = new Thread(new Runnable {
      override def run(): Unit = while (running.get()) {
        val used = memory.getHeapMemoryUsage.getUsed
        var old = peak.get()
        while (used > old && !peak.compareAndSet(old, used)) old = peak.get()
        Thread.sleep(20)
      }
    }, "real-cache-heap-sampler")
    sampler.setDaemon(true)
    sampler.start()
    val startEpoch = System.currentTimeMillis()
    val start = System.nanoTime()
    emit("event" -> "phase_start", "phase" -> phase, "iteration" -> iteration,
      "start_epoch_ms" -> startEpoch)
    var success = false
    try {
      val result = body
      success = true
      result
    } finally {
      val elapsed = (System.nanoTime() - start).toDouble / 1e9
      running.set(false)
      sampler.join(1000)
      emit("event" -> "phase", "phase" -> phase, "iteration" -> iteration,
        "success" -> success, "seconds" -> elapsed, "start_epoch_ms" -> startEpoch,
        "end_epoch_ms" -> System.currentTimeMillis(), "gc_ms" -> (gcMs - beforeMs),
        "gc_count" -> (gcCount - beforeCount),
        "sampled_heap_peak" -> math.max(peak.get(), memory.getHeapMemoryUsage.getUsed))
    }
  }

  private def expressions: Seq[String] = (0 until 16).map(i =>
    s"CAST((xxhash64(id, CAST($i AS BIGINT)) & CAST(9007199254740991 AS BIGINT)) " +
      s"AS DOUBLE) / CAST(9007199254740992 AS DOUBLE) AS c$i")

  private def aggregate(
      df: DataFrame,
      cached: Boolean = false,
      iteration: Int = -1): (Long, Seq[Double], String) = {
    val result = df.selectExpr((Seq("count(*) as n") ++
      (0 until 16).map(i => s"sum(c$i) as s$i")): _*)
    val rows = result.collect()
    require(rows.length == 1)
    if (cached) {
      val scans = result.queryExecution.executedPlan.collect {
        case node if node.getClass.getSimpleName.contains("InMemoryTableScan") => node
      }
      require(scans.length == 1, "Expected one full cache scan")
      require(scans.head.output.length == 16, "Cache scan pruned columns")
      require(scans.head.metrics("numOutputRows").value == rows.head.getLong(0),
        "Cache scan row metric differs from aggregate count")
      result.queryExecution.executedPlan.collect {
        case node if node.getClass.getSimpleName.contains("Coalesce") => node
      }.foreach { node =>
        emit("event" -> "coalescer", "iteration" -> iteration,
          "class" -> node.getClass.getName,
          "children" -> node.children.map(_.getClass.getName).toArray,
          "metrics" -> node.metrics.map { case (k, v) => k -> v.value }.asJava)
      }
      emit("event" -> "scan", "iteration" -> iteration,
        "rows" -> scans.head.metrics("numOutputRows").value,
        "columns" -> scans.head.output.length, "class" -> scans.head.getClass.getName,
        "metrics" -> scans.head.metrics.map { case (k, v) => k -> v.value }.asJava)
    }
    val sums = (0 until 16).map(i => rows.head.getDouble(i + 1))
    require(sums.forall(x => !x.isNaN && !x.isInfinite))
    (rows.head.getLong(0), sums, result.queryExecution.executedPlan.toString())
  }

  def main(args: Array[String]): Unit = {
    require(args.length == 3, "action canonical-directory rows-per-partition")
    val action = args(0)
    val directory = Paths.get(args(1)).toAbsolutePath
    val rowsPerPart = args(2).toLong
    val parts = 16
    val totalRows = Math.multiplyExact(rowsPerPart, parts.toLong)
    val spark = SparkSession.builder().appName("Real SQL cache comparison").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    try {
      require(rowsPerPart > 0)
      emit("event" -> "config", "action" -> action, "parts" -> parts,
        "rows_per_partition" -> rowsPerPart, "rows" -> totalRows,
        "logical_value_bytes" -> Math.multiplyExact(totalRows, 128L),
        "master" -> sc.master, "spark_version" -> spark.version,
        "heap_max" -> Runtime.getRuntime.maxMemory(),
        "jvm_arguments" -> ManagementFactory.getRuntimeMXBean.getInputArguments,
        "runtime_origins" -> Seq("org.apache.spark.storage.BlockManager",
          "org.apache.spark.storage.memory.MemoryStore",
          "org.apache.spark.serializer.SerializerManager").map { name =>
          val clazz = Class.forName(name)
          name -> clazz.getProtectionDomain.getCodeSource.getLocation.toString
        }.toMap.asJava,
        "conf" -> sc.getConf.getAll.toMap.asJava,
        "serializer_class" -> org.apache.spark.SparkEnv.get.serializer.getClass.getName,
        "cache_serializer" -> spark.conf.get("spark.sql.cache.serializer"),
        "cpu_batch_rows" -> spark.conf.get("spark.sql.inMemoryColumnarStorage.batchSize"),
        "cpu_compression" -> spark.conf.get("spark.sql.inMemoryColumnarStorage.compressed"))
      if (action == "canonical") {
        require(!Files.exists(directory), "Never overwrite canonical input")
        val source = spark.range(0L, totalRows, 1L, 96).selectExpr(expressions: _*)
        measured("canonical_write", 0) {
          source.coalesce(parts).write.mode("errorifexists").parquet(directory.toString)
        }
      } else require(action == "cpu" || action.startsWith("gpu"))
      val stream = Files.list(directory)
      val files = try {
        stream.iterator().asScala.filter(p => p.getFileName.toString.endsWith(".parquet"))
          .toVector.sortBy(_.toString)
      } finally stream.close()
      require(files.length == parts, s"Expected $parts canonical files: ${files.length}")
      files.zipWithIndex.foreach { case (file, index) =>
        val reader = ParquetFileReader.open(HadoopInputFile.fromPath(
          new Path(file.toUri), sc.hadoopConfiguration))
        try {
          val footer = reader.getFooter
          val rows = footer.getBlocks.asScala.map(_.getRowCount).sum
          require(rows == rowsPerPart, s"Canonical file row mismatch: $file $rows")
          emit("event" -> "canonical_file", "partition" -> index,
            "path" -> file.toString, "rows" -> rows, "bytes" -> Files.size(file),
            "row_groups" -> footer.getBlocks.size(), "schema" -> footer.getFileMetaData.getSchema.toString)
        } finally reader.close()
      }
      // Each child owns one entire file despite the normal Parquet scan splitting policy.
      val input = files.map(p => spark.read.parquet(p.toString).coalesce(1)).reduce(_.union(_))
      require(input.schema.fields.length == 16 && input.schema.fields.forall(
        _.dataType == org.apache.spark.sql.types.DoubleType))
      if (action == "canonical") {
        val oracle = measured("canonical_oracle", 0) { aggregate(input) }
        require(oracle._1 == totalRows)
        emit("event" -> "oracle", "rows" -> oracle._1, "sums" -> oracle._2.toArray)
      } else {
        val level = if (sys.env.get("REAL_CACHE_DISK_SMOKE").contains("true")) {
          StorageLevel.DISK_ONLY
        } else StorageLevel.MEMORY_AND_DISK
        val df = input.persist(level)
        val relation = spark.sharedState.cacheManager.lookupCachedData(df).get.cachedRepresentation
        val buffers = relation.cacheBuilder.cachedColumnBuffers
        require(buffers.getNumPartitions == parts)
        emit("event" -> "cache_plan", "plan" -> relation.cacheBuilder.cachedPlan.toString,
          "serializer" -> relation.cacheBuilder.serializer.getClass.getName,
          "cache_rdd_id" -> buffers.id, "partitions" -> buffers.getNumPartitions,
          "storage_level" -> buffers.getStorageLevel.toString,
          "storage_flags" -> Array(buffers.getStorageLevel.useDisk,
            buffers.getStorageLevel.useMemory, buffers.getStorageLevel.useOffHeap,
            buffers.getStorageLevel.deserialized),
          "replication" -> buffers.getStorageLevel.replication)
        def storage(phase: String, iteration: Int): Unit = {
          if (action.startsWith("gpu")) {
            val snapshot = Class.forName("org.apache.spark.sql.pcbsrealdata.PcbsCodecSnapshot")
              .getConstructor().newInstance()
            val json = snapshot.getClass.getMethod("snapshotJson").invoke(snapshot).toString
            emit("event" -> "codec", "phase" -> phase, "iteration" -> iteration,
              "snapshot" -> mapper.readTree(json))
          }
          val info = sc.getRDDStorageInfo.find(_.id == buffers.id).get
          require(info.numCachedPartitions == parts)
          emit("event" -> "storage", "phase" -> phase, "iteration" -> iteration,
            "memory_bytes" -> info.memSize, "disk_bytes" -> info.diskSize,
            "cached_partitions" -> info.numCachedPartitions)
        }
        val populations = measured("fill", 0) {
          buffers.mapPartitionsWithIndex { (partition, it: Iterator[CachedBatch]) =>
            var rows = 0L
            var batches = 0L
            var encoded = 0L
            var logical = 0L
            var largest = 0L
            var maxRows = 0
            var kind = ""
            it.foreach { batch =>
              val bytes = batch match {
                case cpu: DefaultCachedBatch => cpu.buffers.map(_.length.toLong).sum
                case other =>
                  // Resolve the actual shim-loaded object's accessor, never a duplicate class.
                  other.getClass.getMethod("buffer").invoke(other)
                    .asInstanceOf[Array[Byte]].length.toLong
              }
              rows += batch.numRows
              batches += 1
              encoded += bytes
              logical += batch.sizeInBytes
              largest = math.max(largest, bytes)
              maxRows = math.max(maxRows, batch.numRows)
              kind = batch.getClass.getName
            }
            Iterator((partition, rows, batches, encoded, logical, largest, maxRows, kind))
          }.collect().sortBy(_._1)
        }
        require(populations.length == parts)
        populations.foreach { p =>
          require(p._2 == rowsPerPart && p._3 > 0 && p._4 > 0)
          if (action == "cpu") require(p._7 <= 10000 && p._8 ==
            "org.apache.spark.sql.execution.columnar.DefaultCachedBatch")
          emit("event" -> "partition", "partition" -> p._1, "rows" -> p._2,
            "records" -> p._3, "encoded_buffer_bytes" -> p._4,
            "cached_batch_size_stats" -> p._5, "max_encoded_bytes" -> p._6,
            "max_batch_rows" -> p._7, "class" -> p._8)
        }
        storage("fill", 0)
        var expected: Option[Seq[Double]] = None
        for (iteration <- 0 to 3) {
          val result = measured("read", iteration) { aggregate(df, cached = true, iteration = iteration) }
          require(result._1 == totalRows)
          expected.foreach { old => require(old.zip(result._2).forall { case (a, b) =>
            math.abs(a - b) <= math.max(1.0, math.abs(a)) * 1e-10
          }, "Cached aggregate changed") }
          expected = Some(result._2)
          emit("event" -> "read", "iteration" -> iteration, "rows" -> result._1,
            "sums" -> result._2.toArray, "plan" -> result._3)
          storage("read", iteration)
        }
        df.unpersist(blocking = true)
        require(!sc.getRDDStorageInfo.exists(_.id == buffers.id))
        emit("event" -> "unpersist", "success" -> true)
      }
      emit("event" -> "complete", "success" -> true)
    } finally spark.stop()
  }
}
