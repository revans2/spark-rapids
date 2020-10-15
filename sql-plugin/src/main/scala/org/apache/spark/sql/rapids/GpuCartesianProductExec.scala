/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

import ai.rapids.cudf.{JCudfSerialization, NvtxColor, NvtxRange, Table}
import com.nvidia.spark.rapids.{Arm, CoalesceGoal, GpuBindReferences, GpuBuildLeft, GpuColumnVector, GpuExec, GpuExpression, GpuSemaphore}
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.{Dependency, NarrowDependency, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.execution.{BinaryExecNode, ExplainUtils, SparkPlan}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.rapids.execution.GpuBroadcastNestedLoopJoinExecBase
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.util.{CompletionIterator, Utils}

@SerialVersionUID(100L)
class GpuSerializableBatch(batch: ColumnarBatch)
    extends Serializable with AutoCloseable with Arm {

  assert(batch != null)
  @transient private var internalBatch: ColumnarBatch = batch

  def getBatch: ColumnarBatch = {
    assert(internalBatch != null)
    internalBatch
  }

  private def writeObject(out: ObjectOutputStream): Unit = {
    withResource (new NvtxRange("SerializeBatch", NvtxColor.PURPLE)) { _ =>
      if (internalBatch == null) {
        throw new IllegalStateException("Cannot re-serialize a batch this way...")
      } else {
        val numRows = internalBatch.numRows()
        val columns = GpuColumnVector.extractBases(internalBatch).map(_.copyToHost())
        try {
          internalBatch.close()
          internalBatch = null
          GpuSemaphore.releaseIfNecessary(TaskContext.get())
          JCudfSerialization.writeToStream(columns, out, 0, numRows)
        } finally {
          columns.safeClose()
        }
      }
    }
  }

  private def readObject(in: ObjectInputStream): Unit = {
    GpuSemaphore.acquireIfNecessary(TaskContext.get())
    withResource (new NvtxRange("DeserializeBatch", NvtxColor.PURPLE)) { _ =>
      withResource(JCudfSerialization.readTableFrom(in)) { tableInfo =>
        val tmp = tableInfo.getTable
        if (tmp == null) {
          throw new IllegalStateException("Empty Batch???")
        }
        this.internalBatch = GpuColumnVector.from(tmp)
      }
    }
  }

  override def close(): Unit = {
    if (internalBatch != null) {
      internalBatch.close()
    }
  }
}

class GpuCartesianPartition(
    idx: Int,
    @transient private val rdd1: RDD[_],
    @transient private val rdd2: RDD[_],
    s1Index: Int,
    s2Index: Int
) extends Partition {
  var s1: Partition = rdd1.partitions(s1Index)
  var s2: Partition = rdd2.partitions(s2Index)
  override val index: Int = idx

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    s1 = rdd1.partitions(s1Index)
    s2 = rdd2.partitions(s2Index)
    oos.defaultWriteObject()
  }
}

class GpuCartesianRDD(
    sc: SparkContext,
    boundCondition: Option[GpuExpression],
    joinTime: SQLMetric,
    joinOutputRows: SQLMetric,
    numOutputRows: SQLMetric,
    numOutputBatches: SQLMetric,
    filterTime: SQLMetric,
    totalTime: SQLMetric,
    var rdd1 : RDD[GpuSerializableBatch],
    var rdd2 : RDD[GpuSerializableBatch])
    extends RDD[ColumnarBatch](sc, Nil)
        with Serializable with Arm {

  private val numPartitionsInRdd2 = rdd2.partitions.length

  override def getPartitions: Array[Partition] = {
    // create the cross product split
    val array = new Array[Partition](rdd1.partitions.length * rdd2.partitions.length)
    for (s1 <- rdd1.partitions; s2 <- rdd2.partitions) {
      val idx = s1.index * numPartitionsInRdd2 + s2.index
      array(idx) = new GpuCartesianPartition(idx, rdd1, rdd2, s1.index, s2.index)
    }
    array
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val currSplit = split.asInstanceOf[GpuCartesianPartition]
    (rdd1.preferredLocations(currSplit.s1) ++ rdd2.preferredLocations(currSplit.s2)).distinct
  }

  override def compute(split: Partition, context: TaskContext):
  Iterator[ColumnarBatch] = {
    val currSplit = split.asInstanceOf[GpuCartesianPartition]
    rdd1.iterator(currSplit.s1, context).flatMap { lhs =>
      val table = withResource(lhs) { lhs =>
        GpuColumnVector.from(lhs.getBatch)
      }
      // Ideally instead of looping through and recomputing rdd2 for
      // each batch in rdd1 we would instead cache rdd2 in a way that
      // it could spill to disk so we can avoid re-computation
      val ret = GpuBroadcastNestedLoopJoinExecBase.innerLikeJoin(
        rdd2.iterator(currSplit.s2, context).map(i => i.getBatch),
        table,
        GpuBuildLeft,
        boundCondition,
        joinTime,
        joinOutputRows,
        numOutputRows,
        numOutputBatches,
        filterTime,
        totalTime)

      CompletionIterator[ColumnarBatch, Iterator[ColumnarBatch]](ret, table.close())
    }
  }

  override def getDependencies: Seq[Dependency[_]] = List(
    new NarrowDependency(rdd1) {
      def getParents(id: Int): Seq[Int] = List(id / numPartitionsInRdd2)
    },
    new NarrowDependency(rdd2) {
      def getParents(id: Int): Seq[Int] = List(id % numPartitionsInRdd2)
    }
  )

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
  }
}

object GpuNoColumnCrossJoin extends Arm {
  def divideIntoBatches(
      rowCounts: RDD[Long],
      targetSizeBytes: Long,
      numOutputRows: SQLMetric,
      numOutputBatches: SQLMetric): RDD[ColumnarBatch] = {
    // Hash aggregate explodes the rows out, so if we go too large
    // it can blow up. The size of a Long is 8 bytes so we just go with
    // that as our estimate, no nulls.
    val maxRowCount = targetSizeBytes/8

    def divideIntoBatches(rows: Long): Iterable[ColumnarBatch] = {
      val numBatches = (rows + maxRowCount - 1)/maxRowCount
      (0L until numBatches).map(i => {
        val ret = new ColumnarBatch(new Array[ColumnVector](0))
        if ((i + 1) * maxRowCount > rows) {
          ret.setNumRows((rows - (i * maxRowCount)).toInt)
        } else {
          ret.setNumRows(maxRowCount.toInt)
        }
        numOutputRows += ret.numRows()
        numOutputBatches += 1
        ret
      })
    }

    rowCounts.flatMap(divideIntoBatches)
  }

  def divideIntoBatches(
      table: Table,
      numTimes: Long,
      targetSizeBytes: Long,
      numOutputRows: SQLMetric,
      numOutputBatches: SQLMetric): Iterator[ColumnarBatch] = {
    // TODO if we hit a point where we need to we can divide the data up into batches
    //  The current use case is likely to be small enough that we are OK without this.
    assert(numTimes < Int.MaxValue)
    withResource(table.repeat(numTimes.toInt)) { repeated =>
      numOutputBatches += 1
      numOutputRows += repeated.getRowCount
      Iterator(GpuColumnVector.from(repeated))
    }
  }
}

case class GpuCartesianProductExec(
    left: SparkPlan,
    right: SparkPlan,
    condition: Option[Expression],
    targetSizeBytes: Long) extends BinaryExecNode with GpuExec {
  override def output: Seq[Attribute]= left.output ++ right.output

  // no guarantee (although it should be at least as big as the min)
  override def outputBatching: CoalesceGoal = null

  override def verboseStringWithOperatorId(): String = {
    val joinCondStr = if (condition.isDefined) s"${condition.get}" else "None"
    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Join condition", joinCondStr)}
     """.stripMargin
  }

  override lazy val additionalMetrics: Map[String, SQLMetric] = Map(
    "joinTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "join time"),
    "joinOutputRows" -> SQLMetrics.createMetric(sparkContext, "join output rows"),
    "filterTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "filter time"),
    "dataSize" -> SQLMetrics.createMetric(sparkContext, "size of shuffled data"))

  protected override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException("This should only be called from columnar")

  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    import com.nvidia.spark.rapids.GpuMetricNames._
    val numOutputRows = longMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = longMetric(NUM_OUTPUT_BATCHES)
    val joinTime = longMetric("joinTime")
    val joinOutputRows = longMetric("joinOutputRows")
    val filterTime = longMetric("filterTime")
    val totalTime = longMetric(TOTAL_TIME)

    val boundCondition = condition.map(GpuBindReferences.bindGpuReference(_, output))

    if (output.isEmpty) {
      // special case for crossJoin.count.  Doing it this way
      // because it is more readable then trying to fit it into the
      // existing join code.
      assert(boundCondition.isEmpty)

      def getRowCountAndClose(cb: ColumnarBatch): Long = {
        val ret = cb.numRows()
        cb.close()
        GpuSemaphore.releaseIfNecessary(TaskContext.get())
        ret
      }

      val l = left.executeColumnar().map(getRowCountAndClose)
      val r = right.executeColumnar().map(getRowCountAndClose)
      // TODO here too it would probably be best to avoid doing any re-computation
      //  that happens with the built in cartesian, but writing another custom RDD
      //  just for this use case is not worth it without an explicit use case.
      GpuNoColumnCrossJoin.divideIntoBatches(
        l.cartesian(r).map(p => p._1 * p._2),
        targetSizeBytes,
        numOutputRows,
        numOutputBatches)
    } else {
      new GpuCartesianRDD(sparkContext,
        boundCondition,
        joinTime,
        joinOutputRows,
        numOutputRows,
        numOutputBatches,
        filterTime,
        totalTime,
        left.executeColumnar().map(cb => new GpuSerializableBatch(cb)),
        right.executeColumnar().map(cb => new GpuSerializableBatch(cb)))
    }
  }
}
