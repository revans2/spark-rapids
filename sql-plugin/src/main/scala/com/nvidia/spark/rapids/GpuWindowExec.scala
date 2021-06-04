/*
 * Copyright (c) 2020-2021, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.Scalar

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, CurrentRow, Expression, NamedExpression, SortOrder, UnboundedPreceding}
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.rapids.GpuAggregateExpression
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
 * Base class for GPU Execs that implement window functions. This abstracts the method
 * by which the window function's input expressions, partition specs, order-by specs, etc.
 * are extracted from the specific WindowExecType.
 *
 * @tparam WindowExecType The Exec class that implements window functions
 *                        (E.g. o.a.s.sql.execution.window.WindowExec.)
 */
abstract class GpuBaseWindowExecMeta[WindowExecType <: SparkPlan] (windowExec: WindowExecType,
                        conf: RapidsConf,
                        parent: Option[RapidsMeta[_, _, _]],
                        rule: DataFromReplacementRule)
  extends SparkPlanMeta[WindowExecType](windowExec, conf, parent, rule) with Logging {

  /**
   * Extracts window-expression from WindowExecType.
   * The implementation varies, depending on the WindowExecType class.
   */
  def getInputWindowExpressions: Seq[NamedExpression]

  /**
   * Extracts partition-spec from WindowExecType.
   * The implementation varies, depending on the WindowExecType class.
   */
  def getPartitionSpecs: Seq[Expression]

  /**
   * Extracts order-by spec from WindowExecType.
   * The implementation varies, depending on the WindowExecType class.
   */
  def getOrderSpecs: Seq[SortOrder]

  /**
   * Indicates the output column semantics for the WindowExecType,
   * i.e. whether to only return the window-expression result columns (as in some Spark
   * distributions) or also include the input columns (as in Apache Spark).
   */
  def getResultColumnsOnly: Boolean

  val windowExpressions: Seq[BaseExprMeta[NamedExpression]] =
    getInputWindowExpressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val partitionSpec: Seq[BaseExprMeta[Expression]] =
    getPartitionSpecs.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val orderSpec: Seq[BaseExprMeta[SortOrder]] =
    getOrderSpecs.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  override def tagPlanForGpu(): Unit = {
    // Implementation depends on receiving a `NamedExpression` wrapped WindowExpression.
    windowExpressions.map(meta => meta.wrapped)
      .filter(expr => !expr.isInstanceOf[NamedExpression])
      .foreach(_ => willNotWorkOnGpu(because = "Unexpected query plan with Windowing functions; " +
        "cannot convert for GPU execution. " +
        "(Detail: WindowExpression not wrapped in `NamedExpression`.)"))
  }

  def isRunningWindow(spec: GpuWindowSpecDefinition): Boolean =
    spec.frameSpecification match {
      case sf: GpuSpecifiedWindowFrame =>
        (sf.lower, sf.upper) match {
          case (GpuSpecialFrameBoundary(UnboundedPreceding), GpuSpecialFrameBoundary(CurrentRow)) =>
            true
          case _ => false
        }
      case _ => false
    }

  override def convertToGpu(): GpuExec = {
    val gpuWindowExpressions = windowExpressions.map(_.convertToGpu())
    // TODO I really should be doing something smarter and splitting this up
    //  into multiple WindowExecs to avoid too much memory being used...
    val allBatchedRunning = !getResultColumnsOnly && gpuWindowExpressions.forall {
      case GpuAlias(GpuWindowExpression(func, spec), _) =>
        val isRunningFunc = func match {
          case _: GpuBatchedRunningWindowFunction[_] => true
          case GpuAggregateExpression(_: GpuBatchedRunningWindowFunction[_], _, _, _ , _) => true
          case _ => false
        }
        isRunningFunc && isRunningWindow(spec)
      case exp =>
        logWarning(s"FOUND UNEXPECTED WINDOW EXPRESSION $exp")
        false
    }
    if (allBatchedRunning) {
      GpuRunningWindowExec(
        gpuWindowExpressions,
        partitionSpec.map(_.convertToGpu()),
        orderSpec.map(_.convertToGpu().asInstanceOf[SortOrder]),
        childPlans.head.convertIfNeeded(),
        getResultColumnsOnly)
    } else {
      GpuWindowExec(
        gpuWindowExpressions,
        partitionSpec.map(_.convertToGpu()),
        orderSpec.map(_.convertToGpu().asInstanceOf[SortOrder]),
        childPlans.head.convertIfNeeded(),
        getResultColumnsOnly
      )
    }
  }
}

/**
 * Specialization of GpuBaseWindowExecMeta for org.apache.spark.sql.window.WindowExec.
 * This class implements methods to extract the window-expressions, partition columns,
 * order-by columns, etc. from WindowExec.
 */
class GpuWindowExecMeta(windowExec: WindowExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends GpuBaseWindowExecMeta[WindowExec](windowExec, conf, parent, rule) {

  /**
   * Fetches WindowExpressions in input `windowExec`, via reflection.
   * As a byproduct, determines whether to return the original input columns,
   * as part of the output.
   *
   * (Spark versions that use `projectList` expect result columns
   * *not* to include the input columns.
   * Apache Spark expects the input columns, before the aggregation output columns.)
   *
   * @return WindowExpressions within windowExec,
   *         and a boolean, indicating the result column semantics
   *         (i.e. whether result columns should be returned *without* including the
   *         input columns).
   */
  def getWindowExpression: (Seq[NamedExpression], Boolean) = {
    var resultColumnsOnly : Boolean = false
    val expr = try {
      val resultMethod = windowExec.getClass.getMethod("windowExpression")
      resultMethod.invoke(windowExec).asInstanceOf[Seq[NamedExpression]]
    } catch {
      case _: NoSuchMethodException =>
        resultColumnsOnly = true
        val winExpr = windowExec.getClass.getMethod("projectList")
        winExpr.invoke(windowExec).asInstanceOf[Seq[NamedExpression]]
    }
    (expr, resultColumnsOnly)
  }

  private lazy val (inputWindowExpressions, resultColumnsOnly) = getWindowExpression

  override def getInputWindowExpressions: Seq[NamedExpression] = inputWindowExpressions
  override def getPartitionSpecs: Seq[Expression] = windowExec.partitionSpec
  override def getOrderSpecs: Seq[SortOrder] = windowExec.orderSpec
  override def getResultColumnsOnly: Boolean = resultColumnsOnly
}

object GpuWindowExec extends Arm {
  def fixerIndexMap(windowExpressionAliases: Seq[Expression]): Map[Int, BatchedRunningWindowFixer] =
    windowExpressionAliases.zipWithIndex.flatMap {
      case (GpuAlias(GpuWindowExpression(func, _), _), index) =>
        func match {
          case f: GpuBatchedRunningWindowFunction[_] =>
            Some((index, f.newFixer()))
          case GpuAggregateExpression(f: GpuBatchedRunningWindowFunction[_], _, _, _, _) =>
            Some((index, f.newFixer()))
          case _ => None
        }
      case _ => None
    }.toMap

  def computeRunningNoPartitioning(
      iter: Iterator[ColumnarBatch],
      boundProjectList: Seq[GpuExpression],
      numOutputBatches: GpuMetric,
      numOutputRows: GpuMetric,
      opTime: GpuMetric): Iterator[ColumnarBatch] = {
    val fixers = fixerIndexMap(boundProjectList)
    TaskContext.get().addTaskCompletionListener[Unit](_ => fixers.values.foreach(_.close()))

    iter.map { cb =>
      val numRows = cb.numRows
      numOutputBatches += 1
      numOutputRows += numRows
      withResource(new MetricRange(opTime)) { _ =>
        withResource(GpuProjectExec.projectAndClose(cb, boundProjectList, NoopMetric)) { full =>
          closeOnExcept(ArrayBuffer[ColumnVector]()) { newColumns =>
            boundProjectList.indices.foreach { idx =>
              val column = full.column(idx).asInstanceOf[GpuColumnVector]
              val fixer = fixers.get(idx)
              if (fixer.isDefined) {
                closeOnExcept(fixer.get.fixUp(scala.util.Right(true), column)) { finalOutput =>
                  fixer.get.updateState(finalOutput)
                  newColumns += finalOutput
                }
              } else {
                newColumns += column.incRefCount()
              }
            }
            new ColumnarBatch(newColumns.toArray, full.numRows())
          }
        }
      }
    }
  }

  private def cudfAnd(lhs: ai.rapids.cudf.ColumnVector,
      rhs: ai.rapids.cudf.ColumnVector): ai.rapids.cudf.ColumnVector = {
    withResource(lhs) { lhs =>
      withResource(rhs) { rhs =>
        lhs.and(rhs)
      }
    }
  }

  private def arePartsEqual(
      scalars: Seq[Scalar],
      columns: Seq[ai.rapids.cudf.ColumnVector]): Either[GpuColumnVector, Boolean] = {
    if (scalars.isEmpty) {
      scala.util.Right(false)
    } else {
      val ret = scalars.zip(columns).map {
        case (scalar, column) => scalar.equalToNullAware(column)
      }.reduce(cudfAnd)
      scala.util.Left(GpuColumnVector.from(ret, BooleanType))
    }
  }

  private def getScalarRow(index: Int, columns: Seq[ai.rapids.cudf.ColumnVector]): Array[Scalar] =
    columns.map(_.getScalarElement(index)).toArray

  def computeRunning(
      iter: Iterator[ColumnarBatch],
      boundProjectList: Seq[GpuExpression],
      boundPartitionSpec: Seq[Expression],
      numOutputBatches: GpuMetric,
      numOutputRows: GpuMetric,
      opTime: GpuMetric): Iterator[ColumnarBatch] = {
    var lastParts: Array[Scalar] = Array.empty
    val fixers = fixerIndexMap(boundProjectList)

    def saveLastParts(newLastParts: Array[Scalar]): Unit = {
      lastParts.foreach(_.close())
      lastParts = newLastParts
    }

    def closeState(): Unit = {
      saveLastParts(Array.empty)
      fixers.values.foreach(_.close())
    }

    TaskContext.get().addTaskCompletionListener[Unit](_ => closeState())

    iter.map { cb =>
      val numRows = cb.numRows
      numOutputBatches += 1
      numOutputRows += numRows
      withResource(new MetricRange(opTime)) { _ =>
        val fullProjectList = boundProjectList ++ boundPartitionSpec
        withResource(GpuProjectExec.projectAndClose(cb, fullProjectList, NoopMetric)) { full =>
          // part columns are owned by full and do not need to be closed, but should not be used
          // if full is closed
          val partColumns = boundPartitionSpec.indices.map { idx =>
            full.column(idx + boundProjectList.length).asInstanceOf[GpuColumnVector].getBase
          }

          // We need to fix up the rows that are part of the same batch as the end of the
          // last batch
          val partsEqual = arePartsEqual(lastParts, partColumns)
          try {
            closeOnExcept(ArrayBuffer[ColumnVector]()) { newColumns =>
              boundProjectList.indices.foreach { idx =>
                val column = full.column(idx).asInstanceOf[GpuColumnVector]
                val fixer = fixers.get(idx)
                if (fixer.isDefined) {
                  val f = fixer.get
                  closeOnExcept(f.fixUp(partsEqual, column)) { finalOutput =>
                    f.updateState(finalOutput)
                    newColumns += finalOutput
                  }
                } else {
                  newColumns += column.incRefCount()
                }
              }
              saveLastParts(getScalarRow(numRows - 1, partColumns))

              new ColumnarBatch(newColumns.toArray, numRows)
            }
          } finally {
            partsEqual match {
              case scala.util.Left(cv) => cv.close()
              case _ => // Nothing
            }
          }
        }
      }
    }
  }
}

trait GpuWindowBaseExec extends UnaryExecNode with GpuExec {
  val resultColumnsOnly: Boolean
  val windowExpressionAliases: Seq[Expression]
  val partitionSpec: Seq[Expression]
  val  orderSpec: Seq[SortOrder]

  import GpuMetric._

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME -> createNanoTimingMetric(MODERATE_LEVEL, OP_TIME)
  )

  override def output: Seq[Attribute] = if (resultColumnsOnly) {
    windowExpressionAliases.map(_.asInstanceOf[NamedExpression].toAttribute)
  } else {
    child.output ++ windowExpressionAliases.map(_.asInstanceOf[NamedExpression].toAttribute)
  }

  override def requiredChildDistribution: Seq[Distribution] = {
    if (partitionSpec.isEmpty) {
      // Only show warning when the number of bytes is larger than 100 MiB?
      logWarning("No Partition Defined for Window operation! Moving all data to a single "
          + "partition, this can cause serious performance degradation.")
      AllTuples :: Nil
    } else ClusteredDistribution(partitionSpec) :: Nil
  }

  lazy val partitionOrdering: Seq[SortOrder] = {
    val shims = ShimLoader.getSparkShims
    partitionSpec.map(shims.sortOrder(_, Ascending))
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(partitionOrdering ++ orderSpec)

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override protected def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not happen, in $this.")
}

case class GpuRunningWindowExec(
    windowExpressionAliases: Seq[Expression],
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    child: SparkPlan,
    resultColumnsOnly: Boolean
) extends GpuWindowBaseExec {

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputBatches = gpuLongMetric(GpuMetric.NUM_OUTPUT_BATCHES)
    val numOutputRows = gpuLongMetric(GpuMetric.NUM_OUTPUT_ROWS)
    val opTime = gpuLongMetric(GpuMetric.OP_TIME)

    val projectList = if (resultColumnsOnly) {
      windowExpressionAliases
    } else {
      child.output ++ windowExpressionAliases
    }

    val boundProjectList =
      GpuBindReferences.bindGpuReferences(projectList, child.output)

    val boundPartitionSpec =
      GpuBindReferences.bindGpuReferences(partitionSpec, child.output)

    if (partitionSpec.isEmpty) {
      child.executeColumnar().mapPartitions {
        iter => GpuWindowExec.computeRunningNoPartitioning(iter,
          boundProjectList,
          numOutputBatches, numOutputRows, opTime)
      }
    } else {
      child.executeColumnar().mapPartitions {
        iter => GpuWindowExec.computeRunning(iter,
          boundProjectList, boundPartitionSpec,
          numOutputBatches, numOutputRows, opTime)
      }
    }
  }
}

case class GpuWindowExec(
    windowExpressionAliases: Seq[Expression],
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    child: SparkPlan,
    resultColumnsOnly: Boolean
  ) extends GpuWindowBaseExec {

  override def childrenCoalesceGoal: Seq[CoalesceGoal] = Seq(outputBatching)

  override def outputBatching: CoalesceGoal = if (partitionSpec.isEmpty) {
    RequireSingleBatch
  } else {
    BatchedByKey(partitionOrdering)
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputBatches = gpuLongMetric(GpuMetric.NUM_OUTPUT_BATCHES)
    val numOutputRows = gpuLongMetric(GpuMetric.NUM_OUTPUT_ROWS)
    val opTime = gpuLongMetric(GpuMetric.OP_TIME)

    val projectList = if (resultColumnsOnly) {
      windowExpressionAliases
    } else {
      child.output ++ windowExpressionAliases
    }

    val boundProjectList =
      GpuBindReferences.bindGpuReferences(projectList, child.output)

    child.executeColumnar().map { cb =>
      numOutputBatches += 1
      numOutputRows += cb.numRows
      GpuProjectExec.projectAndClose(cb, boundProjectList, opTime)
    }
  }
}
