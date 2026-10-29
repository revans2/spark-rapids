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

import java.nio.{ByteBuffer, ByteOrder}

import ai.rapids.cudf.{Cuda, DefaultHostMemoryAllocator, DeviceMemoryBuffer}
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.types._

/** Actual CPU/GPU cache queries plus direct evidence that statistics exclude cache records. */
object SparkPredicateProbe {
  private def serializerClass(codec: String): String = codec match {
    case "default" => "org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer"
    case "pcbs" => "com.nvidia.spark.ParquetCachedBatchSerializer"
    case "uncompressed" => classOf[SparkNvcompUncompressedSerializer].getName
    case "snappy" => classOf[SparkNvcompSnappySerializer].getName
    case "lz4" => classOf[SparkNvcompLz4Serializer].getName
    case "zstd" => classOf[SparkNvcompZstdSerializer].getName
    case "ans" => classOf[SparkNvcompAnsSerializer].getName
    case "cascaded-typed" => classOf[SparkNvcompCascadedSerializer].getName
    case other => throw new IllegalArgumentException(other)
  }

  private def checkQuery(cached: DataFrame, predicate: String, readGpu: Boolean,
      expectGpuScan: Boolean, label: String): Unit = {
    val spark = cached.sparkSession
    // Compare exactly the same cache/filter/projection with pruning disabled and enabled.
    spark.conf.set("spark.sql.inMemoryColumnarStorage.partitionPruning", false)
    val baseline = cached.where(predicate).selectExpr("id", "payload").collect()
    spark.conf.set("spark.sql.inMemoryColumnarStorage.partitionPruning", true)
    val query = cached.where(predicate).selectExpr("id", "payload")
    val observed = query.collect()
    require(observed.sameElements(baseline), label + " residual-filter results: " + predicate)
    val plan = query.queryExecution.executedPlan.toString()
    require(plan.contains("InMemoryTableScan"), label + " missing cache scan")
    require(plan.contains("GpuInMemoryTableScan") == expectGpuScan, label + " mode: " + plan)
    println("PASS " + label + " " + predicate + " rows=" + observed.length)
  }

  private def directPruning(cached: DataFrame, codec: String, label: String): Unit = {
    val relation = cached.sparkSession.sharedState.cacheManager.lookupCachedData(cached)
      .get.cachedRepresentation
    val records = relation.cacheBuilder.cachedColumnBuffers.collect()
    val attributes = relation.output
    val id = attributes.find(_.name == "id").get
    val text = attributes.find(_.name == "payload").get
    val nullColumn = attributes.find(_.name == "empty_string").get
    def filter(predicate: Expression): Array[CachedBatch] =
      relation.cacheBuilder.serializer.buildFilter(Seq(predicate), attributes)(0, records.iterator)
        .toArray
    require(records.length >= 4, label + " expected at least one record per task")
    val selected = filter(GreaterThan(id, Literal(19000L)))
    require(selected.nonEmpty && selected.length < records.length &&
      selected.map(_.numRows.toLong).sum < records.map(_.numRows.toLong).sum,
      label + " no actual batch exclusion")
    val textSelected = filter(EqualTo(text, Literal("東京")))
    require(textSelected.nonEmpty && textSelected.length < records.length,
      label + " string statistics did not exclude records")
    require(filter(LessThan(id, Literal(-1L))).isEmpty, label + " impossible range retained")
    // Spark's default helper does not normalize raw null-safe NULL, so exercise that path
    // on our adapters and use the optimized SQL equivalent on all implementations below.
    if (codec != "default") {
      require(filter(EqualNullSafe(nullColumn, Literal.create(null, StringType))).length ==
        records.length, label + " raw null-safe NULL lost all-null rows")
      require(filter(EqualNullSafe(Literal.create(null, StringType), nullColumn)).length ==
        records.length, label + " reversed null-safe NULL lost rows")
    }
    if (codec != "default") {
      require(filter(InSet(id.asInstanceOf[AttributeReference], Set[Any](1L, 2L, 3L))).length <
        records.length, label + " InSet interval did not prune")
    }
    require(filter(IsNotNull(nullColumn)).isEmpty, label + " all-null IsNotNull retained")
    val unknown = AttributeReference("not-cached", LongType)()
    if (codec != "default") {
      require(filter(Or(LessThan(id, Literal(-1L)),
        GreaterThan(unknown, Literal(0L)))).length == records.length,
        label + " unknown OR branch lost data")
      require(filter(And(LessThan(id, Literal(-1L)),
        Contains(text, Literal("unsupported")))).isEmpty, label + " supported AND did not prune")
    }
    if (codec != "pcbs" && codec != "default") {
      val first = records.head.asInstanceOf[NvcompCacheRecord]
      val unknownBytes = first.bytes.clone()
      // A statistics/schema mismatch must retain the record; do not use an invented bound.
      ByteBuffer.wrap(unknownBytes).order(ByteOrder.LITTLE_ENDIAN).putInt(56, 3)
      val unknownStats = NvcompCacheRecord(first.numRows, unknownBytes)
      val selectedUnknown = relation.cacheBuilder.serializer.buildFilter(
        Seq(LessThan(id, Literal(-1L))), attributes)(0,
        Iterator(records.head, unknownStats, records.head)).toArray
      require(selectedUnknown.length == 1 && (selectedUnknown.head eq unknownStats),
        label + " unknown statistics consumed at a known-run boundary")
    }
    println("PASS " + label + " actual records=" + records.length +
      " range survivors=" + selected.length)
  }

  private def sameStatistics(left: PackedColumnStatistics,
      right: PackedColumnStatistics): Boolean =
    left.typeId == right.typeId && left.rowCount == right.rowCount &&
      left.nullCount == right.nullCount && left.minimum == right.minimum &&
      left.maximum == right.maximum &&
      java.util.Arrays.equals(left.minimumUtf8, right.minimumUtf8) &&
      java.util.Arrays.equals(left.maximumUtf8, right.maximumUtf8)

  private def nativeStatistics(stream: Cuda.Stream): Unit = {
    // Deliberately mixed signs/extremes and nullable tails spanning several reduction tiles.
    val rows = 8195
    val data = new Array[Byte](rows * 8 + 64)
    val buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN)
    (0 until rows).foreach(i => buffer.putLong(i * 8,
      if (i == 0) Long.MinValue else if (i == rows - 1) Long.MaxValue else -i.toLong))
    val metadata = HostPackedBridge.create(rows, Array(4), Array(0), Array(0L),
      Array(-1L), Array(-1L), data)
    val expected = HostPackedBridge.collectStatistics(metadata, data)
    val description = HostPackedBridge.describeColumns(Array(metadata), Array(data.length.toLong))
    withResource(DefaultHostMemoryAllocator.get().allocate(data.length, true)) { host =>
      host.setBytes(0, data, 0, data.length)
      withResource(DeviceMemoryBuffer.allocate(data.length, stream)) { device =>
        device.copyFromHostBufferAsync(host, stream)
        val actual = GpuPackedStatistics.compute(Array(device), description, stream)
        require(actual.length == 1 && sameStatistics(expected.head, actual.head),
          "native multi-tile signed extrema/stream ordering")
      }
    }
    println("PASS native multi-tile statistics stream=" + stream.getStream)
    val schema = StructType(Seq(
      StructField("integer", IntegerType), StructField("long", LongType),
      StructField("text", StringType), StructField("null_text", StringType)))
    val fixtures = Seq(
      Array(null, "", "a", "a\u0000", "é", "東京",
        "x" * 131073, "x" * 131072, "x" * 131073),
      Array(null, "a" * 131072, "a" * 131073, "a" * 131073),
      Array(null, "b" * 131073, "b" * 131073))
    fixtures.foreach { values =>
      val expectedText = values.filter(_ != null).map(
        org.apache.spark.unsafe.types.UTF8String.fromString)
        .sortWith((left, right) => left.compareTo(right) < 0)
      val input = values.indices.iterator.map { i =>
        org.apache.spark.sql.catalyst.InternalRow(
          if (i == 0) Int.MinValue else if (i == 1) Int.MaxValue else -i,
          if (i == 0) Long.MinValue else if (i == 1) Long.MaxValue else -i.toLong,
          if (values(i) == null) null else
            org.apache.spark.unsafe.types.UTF8String.fromString(values(i)), null)
      }
      val records = CpuCachePath.encodeRows(input, schema, "uncompressed").toArray
      withResource(new CpuPackedCodec("uncompressed", SparkNvcompSerializer.ChunkBytes.toInt)) {
          codec =>
        records.foreach { raw =>
          val parsed = SparkNvcompSerializer.parse(raw, "uncompressed")
          parsed.columns.foreach { column =>
            val bytes = codec.decode(raw.asInstanceOf[NvcompCacheRecord].bytes,
              column.payloadOffsets.head, column.payloadLengths.head, column.packedBytes.toInt)
            val expected = HostPackedBridge.collectStatistics(column.metadata, bytes).head
            val descriptors = HostPackedBridge.describeColumns(Array(column.metadata),
              Array(column.packedBytes))
            withResource(DefaultHostMemoryAllocator.get().allocate(bytes.length, true)) { host =>
              host.setBytes(0, bytes, 0, bytes.length)
              withResource(DeviceMemoryBuffer.allocate(bytes.length, stream)) { device =>
                device.copyFromHostBufferAsync(host, stream)
                val actual = GpuPackedStatistics.compute(Array(device), descriptors, stream).head
                require(sameStatistics(expected, actual), "CPU/GPU native edge statistics mismatch")
                require(sameStatistics(column.statistics, actual), "framed statistics mismatch")
                if (actual.typeId == 3) require(actual.minimum == Int.MinValue &&
                  actual.maximum == Int.MaxValue, "signed int extrema")
                if (actual.typeId == 4) require(actual.minimum == Long.MinValue &&
                  actual.maximum == Long.MaxValue, "signed long extrema")
                if (actual.typeId == 23 && actual.nullCount == actual.rowCount) {
                  require(actual.minimumUtf8 == null && actual.maximumUtf8 == null, "all-null string")
                } else if (actual.typeId == 23) {
                  require(actual.nullCount == 1 &&
                    java.util.Arrays.equals(actual.minimumUtf8, expectedText.head.getBytes) &&
                    java.util.Arrays.equals(actual.maximumUtf8, expectedText.last.getBytes),
                    "exact unsigned UTF8 string extrema/nulls")
                  if (expectedText.head == expectedText.last) {
                    require(actual.minimumUtf8 eq actual.maximumUtf8, "equal-bound deduplication")
                    require(column.statistics.minimumUtf8 eq column.statistics.maximumUtf8,
                      "wire equal-bound deduplication")
                  }
                }
              }
            }
          }
        }
      }
    }
    println("PASS native UTF8/null/long-prefix/packed-layout edges stream=" + stream.getStream)
  }

  def main(args: Array[String]): Unit = {
    val codec = args.headOption.getOrElse("snappy")
    val spark = SparkSession.builder().appName("Cache predicate pruning")
      .config("spark.sql.cache.serializer", serializerClass(codec))
      .config("spark.sql.adaptive.enabled", false).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try {
      val writes = if (Set("ans", "cascaded-typed").contains(codec)) Seq(true)
        else Seq(false, true)
      val reads = if (Set("ans", "cascaded-typed").contains(codec)) Seq(true)
        else Seq(false, true)
      for (writeGpu <- writes) {
        spark.conf.set("spark.rapids.sql.enabled", writeGpu)
        val cached = spark.range(0, 20005, 1, 4).selectExpr("id",
          "cast(case when id % 17 = 0 then null else id * -37 end as int) as key",
          "case when id < 5000 then 'alpha' when id < 10000 then 'éclair' " +
            "when id < 15000 then '東京' else '' end as payload",
          "cast(null as string) as empty_string").persist()
        try {
          require(cached.count() == 20005, "population count")
          directPruning(cached, codec, codec + " writerGpu=" + writeGpu)
          for (readGpu <- reads) {
            spark.conf.set("spark.rapids.sql.enabled", readGpu)
            val label = codec + " writerGpu=" + writeGpu + " readerGpu=" + readGpu
            Seq("id > 19000", "id < -1", "id >= 0", "key < -700000",
              "payload = '東京'", "payload like 'é%'", "empty_string <=> null",
              "empty_string is not null", "id in (1, 9000, 19999)",
              "id in (1,2,3,4,5,6,7,8,9,10,11,12,19999)",
              "id > 19000 and payload like '%x%'", "id > 19000 or payload = 'alpha'")
              .foreach(predicate => checkQuery(cached, predicate, readGpu,
                readGpu && codec != "default", label))
          }
        } finally {
          cached.unpersist(blocking = true)
        }
      }
      if (codec == "snappy") {
        nativeStatistics(Cuda.DEFAULT_STREAM)
        withResource(new Cuda.Stream(true))(nativeStatistics)
      }
      println("PASS predicate pruning " + codec)
    } finally { spark.stop() }
  }
}
