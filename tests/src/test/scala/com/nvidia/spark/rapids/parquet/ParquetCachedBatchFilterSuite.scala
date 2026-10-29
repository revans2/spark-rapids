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

package com.nvidia.spark.rapids.parquet

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._

import org.apache.parquet.bytes.BytesUtils
import org.apache.parquet.column.Encoding
import org.apache.parquet.column.statistics.{BinaryStatistics, IntStatistics, LongStatistics, Statistics}
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.metadata.{BlockMetaData, ColumnChunkMetaData, ColumnPath, CompressionCodecName, FileMetaData, ParquetMetadata}
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.MessageTypeParser
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}

/** Footer-only fixtures deliberately contain no pages: exclusion must precede any data decoding. */
class ParquetCachedBatchFilterSuite extends AnyFunSuite {
  private val attributes = Seq(
    AttributeReference("duplicate.name", LongType)(),
    AttributeReference("duplicate.name", StringType)(),
    AttributeReference("integer", IntegerType)())
  private val schema = MessageTypeParser.parseMessageType(
    "message cache { optional int64 _col0; optional binary _col1 (UTF8); optional int32 _col2; }")

  private def longStats(low: Long, high: Long, nulls: Long = 0): LongStatistics = {
    val stats = Statistics.getBuilderForReading(schema.getType(0).asPrimitiveType())
      .withNumNulls(nulls).build().asInstanceOf[LongStatistics]
    stats.updateStats(low)
    stats.updateStats(high)
    stats
  }

  private def stringStats(low: String, high: String): BinaryStatistics = {
    val stats = Statistics.getBuilderForReading(schema.getType(1).asPrimitiveType())
      .withNumNulls(0).build().asInstanceOf[BinaryStatistics]
    stats.updateStats(Binary.fromString(low))
    stats.updateStats(Binary.fromString(high))
    stats
  }

  private def intStats(low: Int, high: Int): IntStatistics = {
    val stats = Statistics.getBuilderForReading(schema.getType(2).asPrimitiveType())
      .withNumNulls(0).build().asInstanceOf[IntStatistics]
    stats.updateStats(low)
    stats.updateStats(high)
    stats
  }

  private def block(stats: Seq[Statistics[_]], rows: Int = 4): BlockMetaData = {
    val result = new BlockMetaData
    result.setRowCount(rows)
    stats.zipWithIndex.foreach { case (columnStats, index) =>
      result.addColumn(ColumnChunkMetaData.get(ColumnPath.get(s"_col$index"),
        schema.getType(index).asPrimitiveType(), CompressionCodecName.UNCOMPRESSED, null,
        Set(Encoding.PLAIN).asJava, columnStats, 4L, 0L, rows.toLong, 0L, 0L))
    }
    result
  }

  private def record(blocks: BlockMetaData*): ParquetCachedBatch = {
    val footer = new ByteArrayOutputStream
    val metadata = new ParquetMetadata(
      new FileMetaData(schema, Map.empty[String, String].asJava, "parquet-mr version 1.13.1"),
      blocks.asJava)
    org.apache.parquet.format.Util.writeFileMetaData(
      new ParquetMetadataConverter().toParquetMetadata(1, metadata), footer)
    val output = new ByteArrayOutputStream
    val magic = "PAR1".getBytes(StandardCharsets.US_ASCII)
    output.write(magic)
    output.write(footer.toByteArray)
    BytesUtils.writeIntLittleEndian(output, footer.size())
    output.write(magic)
    ParquetCachedBatch(blocks.map(_.getRowCount.toInt).sum, output.toByteArray)
  }

  private def ordinary(low: Long, high: Long, text: String = "éclair"): ParquetCachedBatch =
    record(block(Seq(longStats(low, high), stringStats(text, text), intStats(1, 4))))

  private def selected(predicate: Expression, batches: Seq[ParquetCachedBatch],
      conf: SQLConf = new SQLConf): Seq[CachedBatch] =
    ParquetCachedBatchFilter.buildFilter(Seq(predicate), attributes, conf)(
      0, batches.iterator).toVector

  test("integer and reversed long comparisons exclude actual records and preserve identity") {
    val low = ordinary(0, 3)
    val high = ordinary(10, 13)
    assert(selected(GreaterThan(attributes.head, Literal(8L)), Seq(low, high)) == Seq(high))
    assert(selected(LessThan(Literal(8L), attributes.head), Seq(low, high)) == Seq(high))
    assert(selected(EqualTo(attributes(2), Literal(9)), Seq(low, high)).isEmpty)
    assert(selected(EqualTo(attributes.head, Literal(11L)), Seq(high)).head eq high)
  }

  test("Unicode strings and prefixes use physical names rather than duplicate original names") {
    val a = ordinary(0, 3, "éclair")
    val b = ordinary(0, 3, "東京")
    assert(selected(EqualTo(attributes(1), Literal("東京")), Seq(a, b)) == Seq(b))
    assert(selected(StartsWith(attributes(1), Literal("é")), Seq(a, b)) == Seq(a))
    assert(selected(EqualTo(attributes(1), Literal("missing")), Seq(a, b)).isEmpty)
  }

  test("null-safe NULL in both orders and all-null statistics") {
    val allNull: Statistics[_] =
      Statistics.getBuilderForReading(schema.getType(0).asPrimitiveType())
      .withNumNulls(4).build()
    val nullRecord = record(block(Seq(allNull, stringStats("x", "x"), intStats(1, 4))))
    val nonNull = ordinary(0, 3)
    val nullLiteral = Literal.create(null, LongType)
    assert(selected(EqualNullSafe(attributes.head, nullLiteral),
      Seq(nullRecord, nonNull)) == Seq(nullRecord))
    assert(selected(EqualNullSafe(nullLiteral, attributes.head),
      Seq(nullRecord, nonNull)) == Seq(nullRecord))
    assert(selected(IsNotNull(attributes.head), Seq(nullRecord, nonNull)) == Seq(nonNull))
    assert(selected(EqualTo(attributes.head, Literal(1L)), Seq(nullRecord)).isEmpty)
    assert(selected(GreaterThan(attributes.head, nullLiteral), Seq(nonNull)) == Seq(nonNull))
  }

  test("missing bounds and unknown null counts retain records") {
    val missing: Statistics[_] =
      Statistics.getBuilderForReading(schema.getType(0).asPrimitiveType())
      .withNumNulls(0).build()
    val batch = record(block(Seq(missing, stringStats("x", "x"), intStats(1, 4))))
    assert(selected(GreaterThan(attributes.head, Literal(100L)), Seq(batch)) == Seq(batch))
    val unknown: Statistics[_] =
      Statistics.getBuilderForReading(schema.getType(0).asPrimitiveType()).build()
    val unknownRecord = record(block(Seq(unknown, stringStats("x", "x"), intStats(1, 4))))
    assert(selected(IsNull(attributes.head), Seq(unknownRecord)) == Seq(unknownRecord))
  }

  test("legacy multiple-rowgroup record survives when any group matches or lacks bounds") {
    val outside = block(Seq(longStats(0, 3), stringStats("a", "a"), intStats(1, 4)))
    val inside = block(Seq(longStats(100, 103), stringStats("b", "b"), intStats(1, 4)))
    val batch = record(outside, inside)
    val predicate = GreaterThan(attributes.head, Literal(90L))
    assert(selected(predicate, Seq(batch)) == Seq(batch))
    assert(selected(predicate, Seq(record(outside))).isEmpty)
    val missing: Statistics[_] =
      Statistics.getBuilderForReading(schema.getType(0).asPrimitiveType())
      .withNumNulls(0).build()
    val unknown = block(Seq(missing, stringStats("x", "x"), intStats(1, 4)))
    assert(selected(predicate, Seq(record(outside, unknown))).nonEmpty)
  }

  test("unsupported and unmapped predicates retain records but top-level AND can prune") {
    val batch = ordinary(0, 3)
    val excludes = GreaterThan(attributes.head, Literal(100L))
    val unsupported = Contains(attributes(1), Literal("x"))
    val unknown = GreaterThan(AttributeReference("unknown", LongType)(), Literal(100L))
    assert(selected(unsupported, Seq(batch)) == Seq(batch))
    assert(selected(unknown, Seq(batch)) == Seq(batch))
    assert(selected(And(excludes, unsupported), Seq(batch)).isEmpty)
    assert(selected(And(excludes, unknown), Seq(batch)).isEmpty)
    assert(selected(Or(excludes, unsupported), Seq(batch)) == Seq(batch))
    assert(selected(Or(excludes, unknown), Seq(batch)) == Seq(batch))
  }

  test("disabled pushdown and unsupported predicates do not even parse the footer") {
    val malformed = ParquetCachedBatch(4, Array[Byte](1))
    val conf = new SQLConf
    conf.setConfString("spark.sql.parquet.filterPushdown", "false")
    assert(selected(EqualTo(attributes.head, Literal(99L)), Seq(malformed), conf) == Seq(malformed))
    assert(selected(Contains(attributes(1), Literal("x")), Seq(malformed)) == Seq(malformed))
    val unknown = AttributeReference("not-in-cache", LongType)()
    assert(selected(EqualTo(unknown, Literal(1L)), Seq(malformed)) == Seq(malformed))
    val empty = ParquetCachedBatch(4, Array.emptyByteArray)
    assert(selected(IsNull(attributes.head), Seq(empty)) == Seq(empty))
  }

  test("literal IN and safe OR exclude records outside every alternative") {
    val low = ordinary(0, 3)
    val high = ordinary(10, 13)
    assert(selected(In(attributes.head, Seq(Literal(10L), Literal(12L))),
      Seq(low, high)) == Seq(high))
    assert(selected(Or(EqualTo(attributes.head, Literal(10L)),
      EqualTo(attributes.head, Literal(11L))), Seq(low, high)) == Seq(high))
  }
}
