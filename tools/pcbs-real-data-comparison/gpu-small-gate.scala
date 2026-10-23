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
package org.apache.spark.sql.pcbsrealdata

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, EOFException}
import java.util.Arrays

import com.nvidia.spark.rapids.ShimLoader

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.execution.columnar.SqlCacheCodecProvider
import org.apache.spark.sql.functions.{col, count, lit, sum}
import org.apache.spark.storage.StorageLevel

/** A separate 128 MiB correctness gate, never a cache performance comparison. */
object PcbsSmallGate {
  private def snapshot(): Map[String, Long] = {
    val json = new com.fasterxml.jackson.databind.ObjectMapper()
      .readTree(new PcbsCodecSnapshot().snapshotJson())
    import scala.collection.JavaConverters._
    json.fields().asScala.map(entry => entry.getKey -> entry.getValue.asLong()).toMap
  }

  def main(args: Array[String]): Unit = {
    require(args.length == 1 && Set("only", "combine").contains(args(0)))
    val combine = args(0) == "combine"
    val spark = SparkSession.builder().appName("PCBS real payload small correctness gate")
      .getOrCreate()
    try {
      require(spark.version == "3.5.5")
      require(spark.conf.get("spark.rapids.sql.batchSizeBytes") == "1073741824")
      spark.sparkContext.setLogLevel("WARN")
      val rows = 1048576L
      val data = spark.range(0L, rows, 1L, 1).selectExpr((0 until 16).map { i =>
        "CAST(id + " + i + " AS DOUBLE) AS c" + i
      }: _*)
      val cached = data.persist(StorageLevel.DISK_ONLY)
      val relation = spark.sharedState.cacheManager
        .lookupCachedData(cached.queryExecution.analyzed).get.cachedRepresentation
      require(relation.cacheBuilder.serializer
        .isInstanceOf[ResearchParquetCachedBatchSerializer])
      require(relation.cacheBuilder.serializer.isInstanceOf[SqlCacheCodecProvider])
      val owner = relation.cacheBuilder.cachedColumnBuffers
      require(owner.partitions.length == 1)
      require(cached.count() == rows)
      val beforeWire = snapshot()
      require(beforeWire("codec_write_records") > 1L, "Actual PCBS cap did not split")
      require(beforeWire("codec_write_bytes") > 0L)
      require(beforeWire("codec_read_records") > 0L, "Disk cache did not use codec")

      // Compare every real Parquet payload byte after explicit stream roundtrip.
      // The extra gate-only arrays never enter the timed benchmark.
      val wire = owner.mapPartitions { iter =>
        val recordClass = ShimLoader.getShimClassLoader().loadClass(
          "com.nvidia.spark.rapids.parquet.ParquetCachedBatch")
        val bufferMethod = recordClass.getMethod("buffer")
        val codec = new PcbsRecordCodec
        var records = 0L
        var rowCount = 0L
        var payloadBytes = 0L
        var maxPayload = 0L
        iter.foreach { batch =>
          require(batch.getClass == recordClass, "Original record class identity mismatch")
          val original = bufferMethod.invoke(batch).asInstanceOf[Array[Byte]]
          val bytes = new ByteArrayOutputStream()
          val writer = codec.newInstance().serializeStream(bytes)
          try writer.writeObject(batch) finally writer.close()
          writer.close()
          val encoded = bytes.toByteArray
          require(encoded.length.toLong == original.length.toLong + 12L)
          val reader = codec.newInstance().deserializeStream(new ByteArrayInputStream(encoded))
          try {
            val decoded = reader.readObject[CachedBatch]()
            require(decoded.getClass == recordClass, "Decoded record class identity mismatch")
            require(decoded.numRows == batch.numRows && decoded.sizeInBytes == batch.sizeInBytes)
            require(Arrays.equals(original,
              bufferMethod.invoke(decoded).asInstanceOf[Array[Byte]]), "Payload changed")
            var sawEof = false
            try reader.readObject[CachedBatch]() catch { case _: EOFException => sawEof = true }
            require(sawEof)
          } finally reader.close()
          reader.close()
          records += 1L
          rowCount += batch.numRows.toLong
          payloadBytes += batch.sizeInBytes
          maxPayload = math.max(maxPayload, batch.sizeInBytes)
        }
        Iterator.single((records, rowCount, payloadBytes, maxPayload))
      }.collect()
      require(wire.length == 1 && wire(0)._1 > 1L && wire(0)._2 == rows)
      require(wire(0)._4 <= 32L * 1024L * 1024L)
      println("PCBS_SMALL_GATE WIRE_PASS records=" + wire(0)._1 +
        " rows=" + wire(0)._2 + " payloadBytes=" + wire(0)._3 +
        " maxPayload=" + wire(0)._4)

      val beforeRead = snapshot()
      val expressions = Seq(count(lit(1)).as("rows")) ++ (0 until 16).map { i =>
        sum(col("c" + i)).as("s" + i)
      }
      val query = cached.agg(expressions.head, expressions.tail: _*)
      val plan = query.queryExecution.executedPlan
      val gpuScans = plan.collect {
        case node if node.getClass.getName ==
            "org.apache.spark.sql.rapids.GpuInMemoryTableScanExec" => node
      }
      val directCoalescers = plan.collect {
        case node if node.getClass.getName == "com.nvidia.spark.rapids.GpuCoalesceBatches" &&
            node.children.exists(_.getClass.getName ==
              "org.apache.spark.sql.rapids.GpuInMemoryTableScanExec") => node
      }
      require(gpuScans.nonEmpty, "Known-value read did not use GPU cache scan")
      require(directCoalescers.nonEmpty == combine, "Unexpected cache coalescer placement")
      println("PCBS_SMALL_GATE PLAN_BEGIN\n" + plan.toString +
        "\nPCBS_SMALL_GATE PLAN_END")
      val result = query.collect()
      require(result.length == 1 && result(0).getLong(0) == rows)
      (0 until 16).foreach { i =>
        val expected = rows.toDouble * (rows - 1L).toDouble / 2.0 + rows.toDouble * i
        require(result(0).getDouble(i + 1) == expected, "Incorrect full-column sum c" + i)
      }
      val afterRead = snapshot()
      def delta(key: String): Long = afterRead(key) - beforeRead(key)
      require(delta("codec_read_records") > 0L && delta("codec_read_bytes") > 0L)
      require(delta("gpu_decoded_rows") == rows)
      require(afterRead("gpu_decoded_max_bytes") <= 32L * 1024L * 1024L)
      if (combine) {
        require(delta("gpu_combined_rows") == rows)
        require(delta("gpu_combined_batches") < delta("gpu_decoded_batches"))
        require(afterRead("gpu_combined_max_bytes") > 32L * 1024L * 1024L)
        require(afterRead("gpu_combined_max_bytes") <= 1073741824L)
        require(delta("gpu_combined_bytes") == rows * 128L)
      } else {
        require(delta("gpu_combined_batches") == 0L)
      }
      println("PCBS_SMALL_GATE FULL_GPU_READ_PASS mode=" + args(0) +
        " before=" + beforeRead.toSeq.sortBy(_._1).mkString(",") +
        " after=" + afterRead.toSeq.sortBy(_._1).mkString(","))
      cached.unpersist(blocking = true)
      println("PCBS_SMALL_GATE PASS")
    } finally {
      spark.stop()
    }
  }
}
