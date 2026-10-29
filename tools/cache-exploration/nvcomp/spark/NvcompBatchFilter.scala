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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.columnar.{CachedBatch, SimpleMetricsCachedBatch}
import org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/** Reuse Spark's conservative statistics predicates without changing the retained record shape. */
private[cacheexploration] object NvcompBatchFilter {
  private case class Metrics(original: CachedBatch, stats: InternalRow)
      extends SimpleMetricsCachedBatch {
    override def numRows: Int = original.numRows
    override def sizeInBytes: Long = original.sizeInBytes
  }

  private def normalize(expression: Expression, attributes: Seq[Attribute]): Expression = {
    val byId = attributes.groupBy(_.exprId)
    expression match {
      case And(left, right) => And(normalize(left, attributes), normalize(right, attributes))
      case Or(left, right) => Or(normalize(left, attributes), normalize(right, attributes))
      case other if !other.references.forall { a =>
        byId.get(a.exprId).exists(matches =>
          matches.length == 1 && matches.head.dataType == a.dataType)
      } => Literal(true)
      // Spark's metrics helper treats raw null-safe equality as a bounds comparison.
      case EqualNullSafe(a: AttributeReference, literal: Literal) if literal.value == null =>
        IsNull(a)
      case EqualNullSafe(literal: Literal, a: AttributeReference) if literal.value == null =>
        IsNull(a)
      // Spark's metrics prefix bounds use character substrings. A partial UTF8 prefix can
      // match raw bytes yet be ordered below that substring, so retain it conservatively.
      case StartsWith(_: AttributeReference, Literal(prefix: UTF8String, _))
          if UTF8String.fromString(prefix.toString) != prefix => Literal(true)
      case InSet(a: AttributeReference, values) if values.exists(_ != null) =>
        // A bounding interval is conservative and avoids generating an expression per set member.
        val ordering = TypeUtils.getInterpretedOrdering(a.dataType)
        val nonNull = values.iterator.filter(_ != null).toArray
        val lower = nonNull.reduce((left, right) => if (ordering.lteq(left, right)) left else right)
        val upper = nonNull.reduce((left, right) => if (ordering.gteq(left, right)) left else right)
        And(GreaterThanOrEqual(a, Literal.create(lower, a.dataType)),
          LessThanOrEqual(a, Literal.create(upper, a.dataType)))
      case other => other
    }
  }

  private def metrics(record: SparkNvcompSerializer.Parsed,
      attributes: Seq[Attribute]): Option[InternalRow] = {
    val expected = attributes.map(_.dataType match {
      case IntegerType => 3
      case LongType => 4
      case StringType => 23
      case _ => -1
    })
    val complete = record.columns.length == attributes.length &&
      record.columns.zip(expected).forall { case (column, kind) =>
        val stats = column.statistics
        stats != null && stats.typeId == kind &&
          (kind != 23 || stats.nullCount == stats.rowCount ||
            (stats.minimumUtf8 != null && stats.maximumUtf8 != null))
      }
    if (!complete) None else {
      val fields = new Array[Any](attributes.length * 5)
      attributes.indices.foreach { i =>
        val column = record.columns(i)
        val stats = column.statistics
        val start = i * 5
        if (stats.nullCount < stats.rowCount) {
          attributes(i).dataType match {
            case IntegerType =>
              fields(start) = stats.minimum.toInt
              fields(start + 1) = stats.maximum.toInt
            case LongType =>
              fields(start) = stats.minimum
              fields(start + 1) = stats.maximum
            case StringType =>
              fields(start) = UTF8String.fromBytes(stats.minimumUtf8)
              fields(start + 1) = UTF8String.fromBytes(stats.maximumUtf8)
            case _ => throw new IllegalStateException("Validated cached statistics type changed")
          }
        }
        fields(start + 2) = stats.nullCount
        fields(start + 3) = stats.rowCount
        fields(start + 4) = column.packedBytes
      }
      Some(InternalRow.fromSeq(fields.toSeq))
    }
  }

  def build(predicates: Seq[Expression], attributes: Seq[Attribute], codec: String):
      (Int, Iterator[CachedBatch]) => Iterator[CachedBatch] = {
    val normalized = predicates.map(normalize(_, attributes))
    val statisticsFilter = new DefaultCachedBatchSerializer().buildFilter(normalized, attributes)
    (partitionIndex: Int, input: Iterator[CachedBatch]) => {
      if (predicates.isEmpty) input else {
        val source = input.map { original =>
          metrics(SparkNvcompSerializer.parse(original, codec), attributes)
            .map(row => Metrics(original, row))
            .getOrElse(Metrics(original, null))
        }.buffered
        new Iterator[CachedBatch] {
          private var selected: Iterator[CachedBatch] = Iterator.empty
          override def hasNext: Boolean = {
            while (!selected.hasNext && source.hasNext) {
              if (source.head.stats == null) {
                selected = Iterator.single(source.next().original)
              } else {
                // A peeked run does not consume an unknown-statistics record at its boundary.
                val known = new Iterator[CachedBatch] {
                  override def hasNext: Boolean = source.hasNext && source.head.stats != null
                  override def next(): CachedBatch = {
                    if (!hasNext) throw new NoSuchElementException
                    source.next()
                  }
                }
                selected = statisticsFilter(partitionIndex, known).map(_.asInstanceOf[Metrics].original)
              }
            }
            selected.hasNext
          }
          override def next(): CachedBatch = {
            if (!hasNext) throw new NoSuchElementException
            selected.next()
          }
        }
      }
    }
  }
}
