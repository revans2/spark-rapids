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
package org.apache.spark.sql.cacheexploration

import ai.rapids.cudf.{HostColumnVector, Rmm}
import com.nvidia.spark.rapids.GpuColumnVector
import com.nvidia.spark.rapids.HostAlloc
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuInMemoryTableScanExec
import org.apache.spark.storage.StorageLevel

/** Small real-cache correctness and task-cleanup checks; no performance or pressure claims. */
object SparkNvcompProbe {
  private def expectCpuFailure(label: String)(operation: => Unit): Unit = {
    var rejected = false
    try {
      operation
    } catch {
      case failure: Throwable =>
        var cause = failure
        while (cause != null) {
          if (cause.isInstanceOf[UnsupportedOperationException] &&
              Option(cause.getMessage).exists(_.contains("CPU fallback are disabled"))) {
            rejected = true
          }
          cause = cause.getCause
        }
        if (!rejected) throw failure
    }
    require(rejected, label + " unexpectedly accepted CPU execution")
    println("PASS explicit CPU rejection: " + label)
  }

  private def gpuRows(frame: DataFrame, expected: Array[Row], label: String): Unit = {
    require(frame.collect().sameElements(expected), label + " values/nulls/order")
    val plan = frame.queryExecution.executedPlan.toString
    require(plan.contains("GpuInMemoryTableScan"), label + " lacks actual GPU cache scan: " + plan)
    println("PASS GPU cache " + label)
  }

  def main(args: Array[String]): Unit = {
    val serializer: SparkNvcompSerializer = args.headOption.getOrElse("lz4") match {
      case "uncompressed" => new SparkNvcompUncompressedSerializer
      case "lz4" => new SparkNvcompLz4Serializer
      case "zstd" => new SparkNvcompZstdSerializer
      case "snappy" => new SparkNvcompSnappySerializer
      case "ans" => new SparkNvcompAnsSerializer
      case "cascaded8" => new SparkNvcompCascadedByteSerializer
      case "cascaded-typed" => new SparkNvcompCascadedSerializer
      case _ => throw new IllegalArgumentException("Unknown nvCOMP probe codec")
    }
    val spark = SparkSession.builder().appName("nvCOMP real-cache correctness probe")
      .config("spark.sql.cache.serializer", serializer.getClass.getName)
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.rapids.sql.batchSizeBytes", (32L * 1024 * 1024 + 1234).toString)
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.rapids.sql.enabled", true)
    try {
      val expected = Array.tabulate[Row](1025) { i =>
        val key: java.lang.Integer = if (i % 13 == 0) null else Int.box((i * 37) % 1009)
        val text = if (i % 17 == 0) null else if (i % 19 == 0) "" else "µ-雪-" + i
        Row(i * 17L - 991L, key, null, null, null, text, i * 29L - 17L)
      }
      val cached = spark.range(0, 1025, 1, 4).selectExpr(
        "id * 17L - 991L as id",
        "cast(case when id % 13 = 0 then null else (id * 37) % 1009 end as int) as key",
        "cast(null as int) as null_int", "cast(null as long) as null_long",
        "cast(null as string) as null_string",
        "case when id % 17 = 0 then cast(null as string) when id % 19 = 0 then '' " +
          "else concat('µ-雪-', cast(id as string)) end as payload",
        "id * 29L - 17L as valid_long").persist()
      try {
        require(cached.count() == expected.length, "Cache population count")
        gpuRows(cached.selectExpr("*"), expected, "full edge values")
        gpuRows(cached.selectExpr("payload", "key", "id", "id as again", "null_long"),
          expected.map(r => Row(r.get(5), r.get(1), r.get(0), r.get(0), r.get(3))),
          "projected/reordered/duplicate fields")
        val count = cached.selectExpr("count(*) as n")
        gpuRows(count, Array(Row(1025L)), "count-only empty selection")
        require(count.queryExecution.executedPlan.collect {
          case scan: GpuInMemoryTableScanExec => scan.attributes
        }.exists(_.isEmpty), "Count-only GPU scan did not prune every selected attribute")
        val relation = spark.sharedState.cacheManager.lookupCachedData(cached).get.cachedRepresentation
        require(relation.cacheBuilder.serializer.getClass == serializer.getClass,
          "Spark loaded a different cache serializer")
        val buffers = relation.cacheBuilder.cachedColumnBuffers
        // Repeating the small records exercises concatenation with exact ordered edge values.
        val repeated = buffers.flatMap(record => Iterator(record, record))
        val selected = Seq(5, 1, 0, 0, 3).map(relation.output)
        val merged = serializer.gpuConvertCachedBatchToColumnarBatch(repeated,
          relation.output, selected, SQLConf.get)
        val checked = merged.mapPartitionsWithIndex { (partition, batches) =>
          val start = partition * 1025 / 4
          val end = (partition + 1) * 1025 / 4
          val count = end - start
          require(batches.hasNext, "Missing concatenated edge batch")
          val rows = withResource(batches.next()) { batch =>
            require(batch.numRows() == count * 2, "Records were not concatenated")
            withResource(new scala.collection.mutable.ArrayBuffer[HostColumnVector]()) { hosts =>
              (0 until batch.numCols()).foreach { i =>
                hosts += batch.column(i).asInstanceOf[GpuColumnVector].getBase.copyToHost()
              }
              (0 until batch.numRows()).foreach { row =>
                val i = start + row % count
                val text = if (i % 17 == 0) null else if (i % 19 == 0) "" else "µ-雪-" + i
                require(hosts(0).isNull(row) == (text == null), "Concatenated string validity")
                if (text != null) require(hosts(0).getJavaString(row) == text, "String order")
                require(hosts(1).isNull(row) == (i % 13 == 0), "Concatenated key validity")
                if (i % 13 != 0) require(hosts(1).getInt(row) == (i * 37) % 1009, "Key order")
                require(hosts(2).getLong(row) == i * 17L - 991L &&
                  hosts(3).getLong(row) == i * 17L - 991L, "Duplicate/reordered id values")
                require(hosts(4).isNull(row), "Concatenated all-null column")
              }
            }
            batch.numRows().toLong
          }
          require(!batches.hasNext, "Expected one combined edge batch per partition")
          Iterator.single(rows)
        }.collect().sum
        require(checked == 2050, "Concatenated edge row count")
        println("PASS multi-record concatenation: ordered strings/nulls/reordered/duplicate fields")
        val original = buffers.collect().map(_.asInstanceOf[NvcompCacheRecord])
        val disk = buffers.map(batch => batch).persist(StorageLevel.DISK_ONLY)
        try {
          require(disk.count() == original.length, "Disk cached-record count")
          val restored = disk.collect().map(_.asInstanceOf[NvcompCacheRecord])
          require(restored.length == original.length && restored.zip(original).forall {
            case (a, b) => a.numRows == b.numRows && java.util.Arrays.equals(a.bytes, b.bytes)
          }, "Serialized disk cached-record bytes")
          println("PASS Spark DISK_ONLY serialized cached records are byte-exact")
        } finally {
          disk.unpersist(blocking = true)
        }
        if (CpuCachePath.supports(args.headOption.getOrElse("lz4"))) {
          spark.conf.set("spark.rapids.sql.enabled", false)
          try {
            require(cached.selectExpr("*").collect().sameElements(expected),
              "GPU-populated cache CPU values/nulls/order")
            println("PASS GPU-populated cache CPU row decoding")
          } finally {
            spark.conf.set("spark.rapids.sql.enabled", true)
          }
        } else {
        expectCpuFailure("row encode") {
          serializer.convertInternalRowToCachedBatch(null, Seq.empty,
            StorageLevel.MEMORY_AND_DISK, SQLConf.get)
        }
        expectCpuFailure("row decode") {
          serializer.convertCachedBatchToInternalRow(null, Seq.empty, Seq.empty, SQLConf.get)
        }
        expectCpuFailure("CPU columnar decode") {
          serializer.convertCachedBatchToColumnarBatch(null, Seq.empty, Seq.empty, SQLConf.get)
        }
        spark.conf.set("spark.rapids.sql.enabled", false)
        try {
          expectCpuFailure("actual GPU-off cached read") {
            cached.selectExpr("id", "payload").collect()
          }
        } finally {
          spark.conf.set("spark.rapids.sql.enabled", true)
        }
        }
      } finally {
        cached.unpersist(blocking = true)
      }

      // Cluster large strings so average-size row estimates require boundary refinement.
      val skewed = spark.range(0, 2000, 1, 1).selectExpr("id",
        "cast(case when id % 13 = 0 then null else id * 7 end as int) as key",
        "case when id % 17 = 0 then cast(null as string) when id % 19 = 0 then '' " +
          "when id < 128 then repeat('Y', 65536) else repeat('x', 2048) end as payload").persist()
      try {
        val expected = Array.tabulate[Row](2000) { i =>
          val key: java.lang.Integer = if (i % 13 == 0) null else Int.box(i * 7)
          val text = if (i % 17 == 0) null else if (i % 19 == 0) "" else
            if (i < 128) "Y" * 65536 else "x" * 2048
          Row(i.toLong, key, text)
        }
        gpuRows(skewed, expected, "multi-split skewed strings/nulls/order")
        val relation = spark.sharedState.cacheManager.lookupCachedData(skewed).get.cachedRepresentation
        val records = relation.cacheBuilder.cachedColumnBuffers.collect()
          .map(_.asInstanceOf[NvcompCacheRecord])
        require(records.length > 1 && records.map(_.numRows).sum == 2000,
          "Skewed data did not produce multiple complete records")
        require(records.forall(record => java.nio.ByteBuffer.wrap(record.bytes)
          .order(java.nio.ByteOrder.LITTLE_ENDIAN).getLong(24) <= 8L * 1024 * 1024),
          "Refined packed record exceeded 8MiB")
      } finally {
        skewed.unpersist(blocking = true)
      }

      // About 80 MiB total: several 8 MiB packed records per partition, without pressure testing.
      val partial = spark.range(0, 20000, 1, 4)
        .selectExpr("id", "repeat('x', 4096) as payload").persist()
      try {
        require(partial.count() == 20000, "Partial-consumption cache population")
        gpuRows(partial.selectExpr("sum(id)", "sum(length(payload))"),
          Array(Row(199990000L, 81920000L)), "multi-split variable-width aggregates")
        val ids = partial.selectExpr("id").collect().map(_.getLong(0))
        require(ids.length == 20000 && ids.indices.forall(i => ids(i) == i.toLong),
          "Multi-split first/last row order")
        val relation = spark.sharedState.cacheManager.lookupCachedData(partial).get.cachedRepresentation
        val buffers = relation.cacheBuilder.cachedColumnBuffers
        val perPartition = buffers.mapPartitions(it => Iterator.single(it.size)).collect()
        require(perPartition.length == 4 && perPartition.forall(_ > 1),
          "Partial-consumption probe requires multiple cached records in every partition")
        val beforeGpu = Rmm.getTotalBytesAllocated
        val beforeHost = HostAlloc.getUsageRatio
        // Repeat existing heap records without enlarging the cached fixture. Each partition now
        // spans multiple bounded decode groups, so early consumption still leaves unread input.
        val repeated = buffers.mapPartitions(records => records.flatMap(record =>
          Iterator.fill(16)(record)))
        val decoded = serializer.gpuConvertCachedBatchToColumnarBatch(repeated,
          relation.output, relation.output, SQLConf.get)
        val consumed = decoded.mapPartitions { batches =>
          require(batches.hasNext, "Expected a first GPU cached batch")
          val rows = withResource(batches.next()) { batch =>
            val bytes = GpuColumnVector.getTotalDeviceMemoryUsed(batch)
            require(bytes > 24L * 1024 * 1024 && bytes <= 32L * 1024 * 1024 + 1234,
              "Combined batch did not approach the configured target")
            batch.numRows().toLong
          }
          require(batches.hasNext, "Expected a second prefetched group for TaskEnd cleanup")
          Iterator.single(rows) // Task completion must release every pending prefetched batch.
        }.collect().sum
        require(consumed > 0 && consumed < 20000L * 16, "Probe did not leave unread cached batches")
        val afterGpu = Rmm.getTotalBytesAllocated
        val afterHost = HostAlloc.getUsageRatio
        require(afterGpu <= beforeGpu && afterHost <= beforeHost + 1e-12,
          s"Task cleanup grew live resources: GPU $beforeGpu -> $afterGpu, " +
            s"managed host usage $beforeHost -> $afterHost")
        println(s"PASS early iterator TaskEnd cleanup: consumed=$consumed, " +
          s"records/partition=${perPartition.mkString(",")}, GPU=$beforeGpu->$afterGpu, " +
          s"managed host usage=$beforeHost->$afterHost")
      } finally {
        partial.unpersist(blocking = true)
      }
      println("PASS nvCOMP real Spark cache probe; CPU columnar input remains unverified")
    } finally {
      spark.stop()
    }
  }
}
