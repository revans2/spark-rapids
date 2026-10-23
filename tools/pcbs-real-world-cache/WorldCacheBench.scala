/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.pcbsworld

import java.lang.management.ManagementFactory
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.security.MessageDigest
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile

import org.apache.spark.SparkEnv
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.execution.columnar.DefaultCachedBatch
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.{RDDBlockId, StorageLevel}

/** Isolated research harness: real flat rows, fixed input partitions and explicit cache arms. */
object WorldCacheBench {
  private val mapper = new ObjectMapper
  private val Parts = 16
  private val Mask32 = 0xffffffffL
  private val Seeds = Seq(13L, 97L)
  private val ValueBytesDefinition =
    "nonnull fixed-width scalar values plus UTF8 string octets; excludes offsets/null masks/padding"
  private def emit(fields: (String, Any)*): Unit =
    println("WORLD_CACHE " + mapper.writeValueAsString(fields.toMap.asJava))

  private case class Evidence(rows: Long, hashes: Array[Long], nulls: Array[Long],
      bytes: Array[Long], schema: String) {
    val rawBytes: Long = bytes.foldLeft(0L)(Math.addExact)
    def fields: Seq[(String, Any)] = Seq("rows" -> rows, "hash_sums" -> hashes,
      "null_counts" -> nulls, "value_bytes" -> bytes, "raw_value_bytes" -> rawBytes,
      "schema_json" -> schema, "raw_value_bytes_definition" -> ValueBytesDefinition)
    def requireSame(other: Evidence): Unit = {
      require(rows == other.rows && schema == other.schema, "Oracle row/schema mismatch")
      require(hashes.sameElements(other.hashes), "Oracle hash limbs differ")
      require(nulls.sameElements(other.nulls), "Oracle column null counts differ")
      require(bytes.sameElements(other.bytes), "Oracle raw value bytes differ")
    }
  }

  private def evidenceFromJson(node: JsonNode): Evidence = {
    def longs(name: String): Array[Long] = node.get(name).elements().asScala.map(_.asLong()).toArray
    val evidence = Evidence(node.get("rows").asLong(), longs("hash_sums"), longs("null_counts"),
      longs("value_bytes"), node.get("schema_json").asText())
    require(evidence.hashes.length == 4 && evidence.bytes.length == evidence.nulls.length)
    require(evidence.rawBytes == node.get("raw_value_bytes").asLong())
    evidence
  }

  private def observed[A](phase: String, iteration: Int)(body: => A): A = {
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
    }, "world-cache-heap-sampler")
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

  private def field(name: String): Column = col("`" + name.replace("`", "``") + "`")

  private def scalarWidth(dataType: DataType): Int = dataType match {
    case BooleanType | ByteType => 1
    case ShortType => 2
    case IntegerType | FloatType | DateType => 4
    case LongType | DoubleType | TimestampType => 8
    case StringType => 0
    case other => throw new IllegalArgumentException("Unsupported research schema type: " + other)
  }

  private def aggregate(input: DataFrame, expectedRows: Long,
      cached: Boolean, mode: String, iteration: Int): Evidence = {
    require(expectedRows > 0 && expectedRows <= Long.MaxValue / Mask32,
      "Unsigned 32-bit hash limb sums must fit in Long")
    val schema = input.schema
    val hashInputs = schema.fields.zipWithIndex.flatMap { case (f, position) =>
      Seq(lit(position.toLong), field(f.name).isNull, field(f.name))
    }
    val hashes = Seeds.map(seed => xxhash64((Seq(lit(seed)) ++ hashInputs): _*))
    val limbs = hashes.flatMap(hash =>
      Seq(sum(hash.bitwiseAND(lit(Mask32))),
        sum(shiftrightunsigned(hash, 32).bitwiseAND(lit(Mask32)))))
    val nulls = schema.fields.map(f =>
      sum(when(field(f.name).isNull, lit(1L)).otherwise(lit(0L))))
    val bytes = schema.fields.map { f =>
      val width = scalarWidth(f.dataType)
      if (f.dataType == StringType) {
        sum(coalesce(octet_length(field(f.name)).cast(LongType), lit(0L)))
      } else {
        count(field(f.name)) * lit(width.toLong)
      }
    }
    val query = input.agg((Seq(count(lit(1))) ++ limbs ++ nulls ++ bytes).head,
      (Seq(count(lit(1))) ++ limbs ++ nulls ++ bytes).tail: _*)
    val collected = query.collect()
    require(collected.length == 1)
    val row = collected.head
    val result = Evidence(row.getLong(0), (1 to 4).map(row.getLong).toArray,
      schema.indices.map(i => row.getLong(5 + i)).toArray,
      schema.indices.map(i => row.getLong(5 + schema.length + i)).toArray, schema.json)
    require(result.rows == expectedRows)
    val plan = query.queryExecution.executedPlan
    plan.foreach { node =>
      emit("event" -> "operator", "iteration" -> iteration, "cached" -> cached,
        "node_id" -> node.id, "name" -> node.nodeName, "class" -> node.getClass.getName,
        "children" -> node.children.map(_.id).toArray,
        "metrics" -> node.metrics.map { case (k, v) => k -> v.value }.asJava,
        "metric_ids" -> node.metrics.map { case (k, v) => k -> v.id }.asJava)
    }
    emit("event" -> "query_plan", "iteration" -> iteration, "cached" -> cached,
      "plan" -> plan.toString())
    if (cached) {
      val scans = plan.collect {
        case node if node.getClass.getSimpleName.contains("InMemoryTableScan") => node
      }
      require(scans.length == 1 && scans.head.output.length == schema.length,
        "Expected one full-column cache scan")
      require(scans.head.output.map(_.name) == schema.fieldNames.toSeq,
        "Cached scan output names differ")
      require(scans.head.metrics("numOutputRows").value == result.rows,
        "Cached scan row metric differs")
      if (mode == "cpu-default") {
        require(!scans.head.getClass.getSimpleName.startsWith("Gpu"))
      } else {
        require(scans.head.getClass.getSimpleName.startsWith("Gpu"),
          "CPU-PCBS fallback is outside this experiment")
        val aggregates = plan.collect {
          case node if node.getClass.getSimpleName.endsWith("AggregateExec") => node
        }
        def pathToScan(node: org.apache.spark.sql.execution.SparkPlan)
            : Option[Seq[org.apache.spark.sql.execution.SparkPlan]] = {
          if (node eq scans.head) {
            Some(Seq(node))
          } else {
            node.children.iterator.flatMap(child =>
              pathToScan(child).map(path => node +: path)).take(1).toList.headOption
          }
        }
        val path = pathToScan(plan).get
        val firstReduction = path.lastIndexWhere(
          _.getClass.getSimpleName.endsWith("AggregateExec"))
        val fullRows = if (firstReduction >= 0) path.drop(firstReduction + 1) else path
        val gpuPath = aggregates.nonEmpty &&
          aggregates.forall(_.getClass.getSimpleName.startsWith("Gpu")) &&
          fullRows.forall(_.getClass.getSimpleName.startsWith("Gpu"))
        emit("event" -> "gpu_path", "iteration" -> iteration, "success" -> gpuPath,
          "aggregate_classes" -> aggregates.map(_.getClass.getName).toArray,
          "full_row_classes" -> fullRows.map(_.getClass.getName).toArray)
        require(gpuPath, "Full-row GPU projection/aggregation path is required; see query_plan")
      }
      emit("event" -> "scan", "iteration" -> iteration, "class" -> scans.head.getClass.getName,
        "rows" -> result.rows, "columns" -> scans.head.output.length,
        "metrics" -> scans.head.metrics.map { case (k, v) => k -> v.value }.asJava)
    }
    result
  }

  private def observer: AnyRef = Class.forName(
    "org.apache.spark.sql.pcbsrealdata.PcbsCodecSnapshot").getConstructor()
      .newInstance().asInstanceOf[AnyRef]

  def main(args: Array[String]): Unit = {
    require(args.length == 3, "mode input-manifest.json oracle.json")
    val mode = args(0)
    require(Set("oracle", "cpu-default", "pcbs-java", "rg-java", "rg-codec")(mode))
    val manifestPath = Paths.get(args(1)).toAbsolutePath
    val oraclePath = Paths.get(args(2)).toAbsolutePath
    val manifestBytes = Files.readAllBytes(manifestPath)
    val manifest = mapper.readTree(manifestBytes)
    val files = manifest.get("files").elements().asScala.toVector
    require(manifest.get("partitions").asInt() == Parts && files.length == Parts)
    require(files.map(_.get("partition").asInt()) == (0 until Parts))
    val totalRows = files.map(_.get("rows").asLong()).foldLeft(0L)(Math.addExact)
    require(totalRows > 0 && totalRows == manifest.get("total_rows").asLong())
    require(totalRows <= Long.MaxValue / Mask32)
    val spark = SparkSession.builder().appName("Real-world SQL cache comparison").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    try {
      require(sc.master == "local[8]", "This harness requires eight shared-JVM task slots")
      require(spark.conf.get("spark.sql.session.timeZone") == "UTC", "Require fixed UTC semantics")
      require(spark.conf.get("spark.sql.parquet.inferTimestampNTZ.enabled") == "false",
        "Require the explicitly selected timestamp read interpretation")
      if (mode == "oracle" || mode == "cpu-default") {
        require(!sc.getConf.get("spark.plugins", "").contains("com.nvidia.spark.SQLPlugin"),
          "Pure CPU actions must not initialize the GPU plugin")
      }
      files.foreach { file =>
        val path = Paths.get(file.get("path").asText())
        require(Files.size(path) == file.get("bytes").asLong(), "Input file bytes changed")
        val reader = ParquetFileReader.open(HadoopInputFile.fromPath(
          new HadoopPath(path.toUri), sc.hadoopConfiguration))
        try {
          val footer = reader.getFooter
          val rows = footer.getBlocks.asScala.map(_.getRowCount).sum
          require(rows == file.get("rows").asLong() && rows > 0, "Input footer row mismatch")
          emit("event" -> "canonical_file", "partition" -> file.get("partition").asInt(),
            "path" -> path.toString, "rows" -> rows, "bytes" -> Files.size(path),
            "row_groups" -> footer.getBlocks.size,
            "schema" -> footer.getFileMetaData.getSchema.toString)
        } finally reader.close()
      }
      val input = files.map(file =>
        spark.read.parquet(file.get("path").asText()).coalesce(1)).reduce(_.union(_))
      require(input.schema.nonEmpty)
      input.schema.fields.foreach(f => scalarWidth(f.dataType))
      val expected = if (mode == "oracle") None else {
        val value = evidenceFromJson(mapper.readTree(Files.readAllBytes(oraclePath)))
        require(value.rows == totalRows && value.schema == input.schema.json)
        Some(value)
      }
      emit("event" -> "config", "mode" -> mode, "dataset_id" -> manifest.get("dataset_id").asText(),
        "parts" -> Parts, "rows" -> totalRows, "columns" -> input.schema.length,
        "schema_json" -> input.schema.json,
        "session_timezone" -> spark.conf.get("spark.sql.session.timeZone"),
        "parquet_infer_timestamp_ntz" -> spark.conf.get("spark.sql.parquet.inferTimestampNTZ.enabled"),
        "input_manifest_path" -> manifestPath.toString,
        "input_manifest_sha256" -> MessageDigest.getInstance("SHA-256")
          .digest(manifestBytes).map(b => f"${b & 0xff}%02x").mkString,
        "oracle_path" -> oraclePath.toString,
        "raw_value_bytes" -> expected.map(_.rawBytes).map(Long.box).orNull,
        "raw_value_bytes_definition" -> ValueBytesDefinition,
        "master" -> sc.master, "spark_version" -> spark.version,
        "heap_max" -> Runtime.getRuntime.maxMemory(),
        "jvm_arguments" -> ManagementFactory.getRuntimeMXBean.getInputArguments,
        "runtime_origins" -> Seq("org.apache.spark.storage.BlockManager",
          "org.apache.spark.storage.memory.MemoryStore",
          "org.apache.spark.serializer.SerializerManager").map { name =>
          name -> Class.forName(name).getProtectionDomain.getCodeSource.getLocation.toString
        }.toMap.asJava,
        "conf" -> sc.getConf.getAll.toMap.asJava,
        "serializer_class" -> SparkEnv.get.serializer.getClass.getName,
        "cache_serializer" -> spark.conf.get("spark.sql.cache.serializer"),
        "cpu_batch_rows" -> spark.conf.get("spark.sql.inMemoryColumnarStorage.batchSize"),
        "cpu_compression" -> spark.conf.get("spark.sql.inMemoryColumnarStorage.compressed"))
      if (mode == "oracle") {
        require(!Files.exists(oraclePath), "Never overwrite an oracle")
        val evidence = observed("oracle", 0) {
          aggregate(input, totalRows, cached = false, mode, iteration = -1)
        }
        Files.write(oraclePath, mapper.writeValueAsBytes(evidence.fields.toMap.asJava),
          StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
        emit((Seq("event" -> "oracle") ++ evidence.fields): _*)
      } else {
        val level = sys.env.getOrElse("WORLD_CACHE_STORAGE_LEVEL", "") match {
          case "DISK_ONLY" => StorageLevel.DISK_ONLY
          case "MEMORY_AND_DISK" => StorageLevel.MEMORY_AND_DISK
          case other => throw new IllegalArgumentException("Unknown research storage: " + other)
        }
        val df = input.persist(level)
        val relation = spark.sharedState.cacheManager.lookupCachedData(df).get.cachedRepresentation
        val buffers = relation.cacheBuilder.cachedColumnBuffers
        require(buffers.getNumPartitions == Parts)
        emit("event" -> "cache_plan", "plan" -> relation.cacheBuilder.cachedPlan.toString,
          "serializer" -> relation.cacheBuilder.serializer.getClass.getName,
          "serializer_origin" -> relation.cacheBuilder.serializer.getClass
            .getProtectionDomain.getCodeSource.getLocation.toString,
          "cache_rdd_id" -> buffers.id, "partitions" -> buffers.getNumPartitions,
          "storage_level" -> buffers.getStorageLevel.toString,
          "storage_flags" -> Array(level.useDisk, level.useMemory, level.useOffHeap,
            level.deserialized), "replication" -> level.replication)
        def storage(phase: String, iteration: Int): Unit = {
          if (mode != "cpu-default") {
            val instance = observer
            val json = instance.getClass.getMethod("snapshotJson").invoke(instance).toString
            emit("event" -> "codec", "phase" -> phase, "iteration" -> iteration,
              "observer_class" -> instance.getClass.getName,
              "observer_origin" -> instance.getClass.getProtectionDomain.getCodeSource
                .getLocation.toString,
              "snapshot" -> mapper.readTree(json))
          }
          val manager = SparkEnv.get.blockManager
          val blocks = (0 until Parts).map { partition =>
            val id = RDDBlockId(buffers.id, partition)
            val status = manager.getStatus(id)
            val file = manager.diskBlockManager.getFile(id)
            Map[String, Any]("partition" -> partition, "block_id" -> id.name,
              "present" -> status.isDefined, "memory_bytes" -> status.map(_.memSize).getOrElse(0L),
              "disk_bytes" -> status.map(_.diskSize).getOrElse(0L),
              "storage_level" -> status.map(_.storageLevel.toString).getOrElse("NONE"),
              "disk_file_exists" -> file.isFile,
              "disk_file_bytes" -> (if (file.isFile) file.length() else 0L))
          }
          require(blocks.forall(_("present") == true), "A cached partition disappeared")
          val info = sc.getRDDStorageInfo.find(_.id == buffers.id)
          emit("event" -> "storage", "phase" -> phase, "iteration" -> iteration,
            "cache_rdd_id" -> buffers.id,
            "memory_bytes" -> blocks.map(_("memory_bytes").asInstanceOf[Long]).sum,
            "disk_bytes" -> blocks.map(_("disk_bytes").asInstanceOf[Long]).sum,
            "disk_file_bytes" -> blocks.map(_("disk_file_bytes").asInstanceOf[Long]).sum,
            "cached_partitions" -> blocks.count(_("present") == true),
            "local_blocks" -> blocks.map(_.asJava).toArray,
            "async_rdd_memory_bytes" -> info.map(i => Long.box(i.memSize)).orNull,
            "async_rdd_disk_bytes" -> info.map(i => Long.box(i.diskSize)).orNull,
            "async_rdd_cached_partitions" -> info.map(i => Int.box(i.numCachedPartitions)).orNull)
        }
        val populations = observed("fill", 0) {
          buffers.mapPartitionsWithIndex { (partition, records: Iterator[CachedBatch]) =>
            var rows = 0L
            var batches = 0L
            var encoded = 0L
            var stats = 0L
            var minimum = Long.MaxValue
            var maximum = 0L
            var maxRows = 0
            var kind = ""
            val bounds = Array(1L, 8L, 16L, 32L, 64L, 128L, 256L, 512L, 1024L)
              .map(_ * 1024 * 1024)
            val histogram = Array.fill(bounds.length + 1)(0L)
            records.foreach { batch =>
              val bytes = batch match {
                case cpu: DefaultCachedBatch => cpu.buffers.map(_.length.toLong).sum
                case other => other.getClass.getMethod("buffer").invoke(other)
                  .asInstanceOf[Array[Byte]].length.toLong
              }
              rows += batch.numRows
              batches += 1
              encoded += bytes
              stats += batch.sizeInBytes
              minimum = math.min(minimum, bytes)
              maximum = math.max(maximum, bytes)
              maxRows = math.max(maxRows, batch.numRows)
              kind = batch.getClass.getName
              val bucket = bounds.indexWhere(bytes <= _)
              histogram(if (bucket < 0) bounds.length else bucket) += 1
            }
            Iterator((partition, rows, batches, encoded, stats, minimum, maximum,
              maxRows, kind, histogram))
          }.collect().sortBy(_._1)
        }
        require(populations.length == Parts)
        populations.foreach { p =>
          require(p._2 == files(p._1).get("rows").asLong() && p._3 > 0 && p._4 > 0)
          if (mode == "cpu-default") {
            require(p._9 == "org.apache.spark.sql.execution.columnar.DefaultCachedBatch")
          }
          emit("event" -> "partition", "partition" -> p._1, "rows" -> p._2,
            "records" -> p._3, "encoded_buffer_bytes" -> p._4,
            "cached_batch_size_stats" -> p._5, "min_encoded_bytes" -> p._6,
            "max_encoded_bytes" -> p._7, "max_batch_rows" -> p._8, "class" -> p._9,
            "record_histogram_counts" -> p._10,
            "record_histogram_upper_mib" -> Array(1, 8, 16, 32, 64, 128, 256, 512, 1024, -1))
        }
        storage("fill", 0)
        for (iteration <- 0 to 3) {
          if (mode != "cpu-default") {
            val instance = observer
            instance.getClass.getMethod("resetReadExtrema").invoke(instance)
          }
          val result = observed("read", iteration) {
            aggregate(df, totalRows, cached = true, mode, iteration)
          }
          result.requireSame(expected.get)
          emit((Seq("event" -> "read", "iteration" -> iteration,
            "warmup" -> (iteration == 0)) ++ result.fields): _*)
          storage("read", iteration)
        }
        df.unpersist(blocking = true)
        val manager = SparkEnv.get.blockManager
        require((0 until Parts).forall { partition =>
          val id = RDDBlockId(buffers.id, partition)
          manager.getStatus(id).isEmpty && !manager.diskBlockManager.getFile(id).isFile
        }, "Blocking unpersist left local cache blocks")
        require(!sc.getRDDStorageInfo.exists(_.id == buffers.id))
        emit("event" -> "unpersist", "success" -> true, "cache_rdd_id" -> buffers.id,
          "remaining_local_blocks" -> 0)
      }
      emit("event" -> "complete", "success" -> true)
    } finally spark.stop()
  }
}
