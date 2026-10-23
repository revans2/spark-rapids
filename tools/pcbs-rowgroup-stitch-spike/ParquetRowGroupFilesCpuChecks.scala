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
package com.nvidia.spark.rapids.parquet

import java.nio.file.{Files, Paths}

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.Arm.withResource
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.example.{ExampleParquetWriter, GroupReadSupport}
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.schema.MessageTypeParser

/** CPU-only independent-file decoder checks; does not allocate native buffers. */
object ParquetRowGroupFilesCpuChecks {
  def main(args: Array[String]): Unit = {
    require(args.length == 1, "Expected fresh output directory")
    val directory = Files.createDirectories(Paths.get(args(0)))
    val schema = MessageTypeParser.parseMessageType(
      "message test { required int64 id; optional binary text (UTF8); optional double value; }")
    val conf = new Configuration()
    val rows = 40000
    Seq(CompressionCodecName.UNCOMPRESSED, CompressionCodecName.SNAPPY).foreach { codec =>
      val original = directory.resolve(codec.name + "-original.parquet")
      val factory = new SimpleGroupFactory(schema)
      val builder = ExampleParquetWriter.builder(new Path(original.toUri))
        .withConf(conf).withType(schema).withRowGroupSize(8192)
        .withPageSize(1024).withDictionaryEncoding(true).withCompressionCodec(codec)
        .withExtraMetaData(Map("research-marker" -> "preserved").asJava)
      withResource(builder.build()) { writer =>
        (0 until rows).foreach { row =>
          val group = factory.newGroup().append("id", row.toLong)
          if (row % 5 != 0) group.append("text", "dictionary-" + row % 11)
          if (row % 7 != 0) group.append("value", row * 0.5)
          writer.write(group)
        }
      }
      val bytes = Files.readAllBytes(original)
      val source = ParquetRowGroupFiles.fromBytes(bytes)
      val originalFooter = ParquetRowGroupFiles.readFooter(source)
      require(originalFooter.getBlocks.size > 1, "Fixture needs multiple row groups")
      require(originalFooter.getBlocks.asScala.exists(_.getColumns.asScala.exists(
        c => c.hasDictionaryPage && c.getFirstDataPageOffset > c.getDictionaryPageOffset)),
        "Fixture must exercise distinct dictionary and first-data-page offsets")
      var rowIndex = 0
      var records = 0
      var encodedBytes = 0L
      var largest = 0
      ParquetRowGroupFiles.split(source, 1024 * 1024).foreach { record =>
        if (records == 0) {
          var bundleRejected = false
          try {
            ParquetRowGroupFiles.stageNextGroup(
              Iterator[org.apache.spark.sql.columnar.CachedBatch](record).buffered,
              new org.apache.spark.sql.types.StructType(), 1L, 1024 * 1024)
          } catch {
            case _: IllegalArgumentException => bundleRejected = true
          }
          require(bundleRejected, "Singleton encoded bundle must fail before native allocation")
        }
        val footer = ParquetRowGroupFiles.readFooter(
          ParquetRowGroupFiles.fromBytes(record.buffer))
        require(footer.getBlocks.size == 1)
        require(footer.getBlocks.get(0).getRowCount == record.numRows)
        require(footer.getFileMetaData.getKeyValueMetaData.get("research-marker") == "preserved")
        require(footer.getFileMetaData.getSchema == schema)
        val file = directory.resolve(codec.name + "-fragment-" + records + ".parquet")
        Files.write(file, record.buffer)
        withResource(ParquetReader.builder[Group](new GroupReadSupport(), new Path(file.toUri))
            .withConf(conf).build()) { reader =>
          var group = reader.read()
          while (group != null) {
            require(group.getLong("id", 0) == rowIndex.toLong)
            require(group.getFieldRepetitionCount("text") == (if (rowIndex % 5 == 0) 0 else 1))
            if (rowIndex % 5 != 0) {
              require(group.getString("text", 0) == "dictionary-" + rowIndex % 11)
            }
            require(group.getFieldRepetitionCount("value") == (if (rowIndex % 7 == 0) 0 else 1))
            if (rowIndex % 7 != 0) require(group.getDouble("value", 0) == rowIndex * 0.5)
            rowIndex += 1
            group = reader.read()
          }
        }
        records += 1
        encodedBytes += record.buffer.length
        largest = math.max(largest, record.buffer.length)
      }
      require(rowIndex == rows)
      require(records == originalFooter.getBlocks.size)
      var guardRejected = false
      try {
        ParquetRowGroupFiles.split(source, 12).next()
      } catch {
        case _: java.io.IOException => guardRejected = true
      }
      require(guardRejected, "Record guard must reject indivisible oversized row groups")
      val damaged = bytes.clone()
      damaged(damaged.length - 1) = 0
      var framingRejected = false
      try {
        ParquetRowGroupFiles.readFooter(ParquetRowGroupFiles.fromBytes(damaged))
      } catch {
        case _: IllegalArgumentException => framingRejected = true
      }
      require(framingRejected, "Malformed framing must fail")
      println(s"CPU_CHECK codec=$codec rows=$rowIndex records=$records " +
        s"encodedBytes=$encodedBytes largestRecordBytes=$largest dictionary=true nulls=true")
    }
    println("CPU_CHECK PASS")
  }
}
