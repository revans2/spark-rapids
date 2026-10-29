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

import com.nvidia.spark.rapids.shims.SparkShimImpl
import com.nvidia.spark.rapids.shims.parquet.ParquetStringPredShims
import org.apache.parquet.filter2.compat.{FilterCompat, RowGroupFilter}
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.schema.MessageType

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.PCBSFilterHelper
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}

/** Conservative whole-record pruning; surviving records still require Spark's residual filter. */
private[rapids] object ParquetCachedBatchFilter {
  private def conjuncts(expression: Expression): Seq[Expression] = expression match {
    case And(left, right) => conjuncts(left) ++ conjuncts(right)
    case other => Seq(other)
  }

  private def safeAttribute(attribute: AttributeReference): Boolean =
    attribute.dataType == IntegerType || attribute.dataType == LongType ||
      attribute.dataType == StringType

  // Keep the first implementation to exact flat types. In particular Parquet floating extrema
  // need not include NaNs, and binary bounds cannot represent nonbinary string collations.
  private def safeExpression(expression: Expression): Boolean = expression match {
    case And(left, right) => safeExpression(left) && safeExpression(right)
    case Or(left, right) => safeExpression(left) && safeExpression(right)
    case comparison: BinaryComparison if !comparison.children.exists {
      case literal: Literal => literal.value == null && !comparison.isInstanceOf[EqualNullSafe] &&
        !comparison.isInstanceOf[EqualTo]
      case _ => false
    } =>
      comparison match {
        case EqualTo(a: AttributeReference, _: Literal) => safeAttribute(a)
        case EqualTo(_: Literal, a: AttributeReference) => safeAttribute(a)
        case EqualNullSafe(a: AttributeReference, _: Literal) => safeAttribute(a)
        case EqualNullSafe(_: Literal, a: AttributeReference) => safeAttribute(a)
        case LessThan(a: AttributeReference, _: Literal) => safeAttribute(a)
        case LessThan(_: Literal, a: AttributeReference) => safeAttribute(a)
        case LessThanOrEqual(a: AttributeReference, _: Literal) => safeAttribute(a)
        case LessThanOrEqual(_: Literal, a: AttributeReference) => safeAttribute(a)
        case GreaterThan(a: AttributeReference, _: Literal) => safeAttribute(a)
        case GreaterThan(_: Literal, a: AttributeReference) => safeAttribute(a)
        case GreaterThanOrEqual(a: AttributeReference, _: Literal) => safeAttribute(a)
        case GreaterThanOrEqual(_: Literal, a: AttributeReference) => safeAttribute(a)
        case _ => false
      }
    case IsNull(a: AttributeReference) => safeAttribute(a)
    case IsNotNull(a: AttributeReference) => safeAttribute(a)
    case In(a: AttributeReference, values) =>
      safeAttribute(a) && values.forall(_.isInstanceOf[Literal])
    case InSet(a: AttributeReference, _) => safeAttribute(a)
    case StartsWith(a: AttributeReference, _: Literal) => a.dataType == StringType
    case _ => false
  }

  def buildFilter(predicates: Seq[Expression], attributes: Seq[Attribute],
      conf: SQLConf): (Int, Iterator[CachedBatch]) => Iterator[CachedBatch] = {
    // Respect the existing Parquet pushdown controls as well as the caller's cache-pruning gate.
    // Capture only immutable values and datasource filters, never SQLConf or compiled Parquet
    // predicates in the task closure.
    val enabled = conf.parquetFilterPushDown
    val stringPredicates = ParquetStringPredShims.pushDown(conf)
    val inThreshold = conf.parquetFilterPushDownInFilterThreshold
    val attributesById = attributes.zipWithIndex.groupBy(_._1.exprId)
    val filters = if (enabled) {
      predicates.flatMap(conjuncts).flatMap { predicate =>
        val mapped = predicate.references.forall { attribute =>
          attributesById.get(attribute.exprId).exists { matches =>
            matches.size == 1 && matches.head._1.dataType == attribute.dataType
          }
        }
        if (mapped && safeExpression(predicate)) {
          val renamed = predicate.transform {
            case a: AttributeReference =>
              a.withName(s"_col${attributesById(a.exprId).head._2}")
          }
          PCBSFilterHelper.translate(renamed)
        } else {
          None
        }
      }
    } else {
      Seq.empty
    }

    (_: Int, batches: Iterator[CachedBatch]) => {
      // A partition normally has one physical schema. Keep only the most recent compilation,
      // so even unusual mixed-schema inputs cannot grow an unbounded metadata cache.
      var lastSchema: MessageType = null
      var compiled: Option[FilterCompat.Filter] = None
      if (filters.isEmpty) {
        batches
      } else {
        batches.filter {
          case batch: ParquetCachedBatch if batch.buffer.nonEmpty =>
            val footer = ParquetCachedBatchFiles.readFooter(
              ParquetCachedBatchFiles.fromBytes(batch.buffer))
            val schema = footer.getFileMetaData.getSchema
            if (schema != lastSchema) {
              val parquetFilters = SparkShimImpl.getParquetFilters(schema,
                false, false, false, stringPredicates, inThreshold, true,
                footer.getFileMetaData.getKeyValueMetaData.get, "CORRECTED")
              compiled = filters.flatMap(parquetFilters.createFilter).reduceOption(FilterApi.and)
                .map(FilterCompat.get)
              lastSchema = schema
            }
            // This overload uses only footer statistics, never dictionaries or encoded pages.
            // Legacy records may contain several groups: keep the record if any group survives.
            compiled.forall { filter =>
              !RowGroupFilter.filterRowGroups(filter, footer.getBlocks, schema).isEmpty
            }
          case _ => true
        }
      }
    }
  }
}
