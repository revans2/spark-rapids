/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

package org.apache.iceberg.spark.source

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.iceberg.{IcebergFormatVersionSupport, ShimUtils}
import com.nvidia.spark.rapids.perf.{HistoryObservations, MetricHistory, ScanContext,
  ScanSplitHeuristic}
import org.apache.iceberg.ScanTaskGroup
import org.apache.iceberg.spark.GpuSparkReadConf
import org.apache.iceberg.types.Types

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}
import org.apache.spark.sql.connector.read.{Batch, Scan, Statistics, SupportsReportStatistics}
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.CollectionAccumulator


abstract class GpuSparkScan(val cpuScan: Scan,
    val rapidsConf: RapidsConf,
    val queryUsesInputFile: Boolean,
) extends GpuScan with SupportsReportStatistics with Logging {
  private val readConf: GpuSparkReadConf = new GpuSparkReadConf(
    GpuSparkScanAccess.readConf(cpuScan))

  def readTimestampWithoutZone: Boolean = readConf.handleTimestampWithoutZone()

  override def readSchema(): StructType = cpuScan.readSchema()

  override def estimateStatistics(): Statistics = GpuSparkScanAccess.estimateStatistics(cpuScan)

  override def toBatch: Batch = {
    val planning = synchronized {
      latestPlanning += 1
      latestPlanning
    }
    registerHistoryObservation(planning)
    new GpuSparkBatch(GpuSparkScanAccess.toBatch(cpuScan), this)
  }

  /** (split, decoded, listed) per split read; None when history is off. */
  @transient lazy val splitBytes: Option[CollectionAccumulator[(Int, Long, Long)]] = {
    if (ScanSplitHeuristic.isEnabled) {
      Some(SparkContext.getOrCreate().collectionAccumulator[(Int, Long, Long)])
    } else {
      None
    }
  }

  /**
   * Only the newest planning is recorded: runtime filtering re-plans via `toBatch`. Exec copies'
   * `toBatch` calls (equals/hashCode) see the same cached task groups.
   */
  @transient @volatile private var latestPlanning = 0

  /**
   * Registers the scan for recording at query end, keyed by `Table.name()`.
   */
  private def registerHistoryObservation(planning: Int): Unit = {
    val acc = splitBytes
    if (acc.isDefined) {
      try {
        if (!GpuSparkScanAccess.isMetadataScan(cpuScan)) {
          val table = GpuSparkScanAccess.table(cpuScan).name()
          val groups = GpuSparkScanAccess.taskGroups(cpuScan)
          if (table != null && table.nonEmpty && !groups.isEmpty) {
            val ctx = ScanContext(
              table = table,
              listedBytes = 0L,
              batchSizeBytes = 0L,
              minPartitionNum = 0L,
              maxSplitBytes = 0L,
              plannedSplits = groups.size(),
              drainSplits = () => acc.map(ScanSplitHeuristic.drainSplits).getOrElse(Seq.empty),
              isCurrent = () => planning == latestPlanning)
            ScanSplitHeuristic.register(
              HistoryObservations.currentExecutionId(SparkContext.getOrCreate()), ctx)
          }
        }
      } catch {
        case t: Throwable if MetricHistory.isContained(t) =>
          logDebug(s"scan not registered for history: ${t.getClass.getName}")
      }
    }
  }

  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream =
    throw new UnsupportedOperationException(
      "GpuSparkScan does not support toMicroBatchStream()")

  override def description(): String = cpuScan.description()

  override def reportDriverMetrics(): Array[CustomTaskMetric] = cpuScan.reportDriverMetrics()

  override def supportedCustomMetrics(): Array[CustomMetric] = cpuScan.supportedCustomMetrics()

  protected def groupingKeyType(): Types.StructType

  protected def taskGroups(): Seq[_ <: ScanTaskGroup[_]]

  def hasNestedType: Boolean = {
    GpuSparkScanAccess.expectedSchema(cpuScan)
      .asStruct()
      .fields()
      .asScala
      .exists { field => field.`type`().isNestedType }
  }
}


object GpuSparkScan {
  def isMetadataScan(scan: Scan): Boolean = {
    GpuSparkScanAccess.isMetadataScan(scan)
  }

  def tryConvert(cpuScan: Scan, rapidsConf: RapidsConf): Try[GpuSparkScan] = {
    Try {
      if (GpuSparkScanAccess.isBatchQueryScan(cpuScan)) {
        new GpuSparkBatchQueryScan(cpuScan, rapidsConf, false)
      } else if (GpuSparkScanAccess.isCopyOnWriteScan(cpuScan)) {
        ShimUtils.newCopyOnWriteScan(cpuScan, rapidsConf, false)
      } else {
        throw new IllegalArgumentException(
          s"Currently iceberg support only supports batch query scan and copy-on-write scan, " +
            s"but got ${cpuScan.getClass.getName}")
      }
    }
  }

  def tagForGpu(meta: ScanMeta[Scan], gpuScan: Try[GpuSparkScan]): Unit = {
    if (!meta.conf.isIcebergEnabled) {
      meta.willNotWorkOnGpu("Iceberg input and output has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_ICEBERG} to true")
    }

    if (!meta.conf.isIcebergReadEnabled) {
      meta.willNotWorkOnGpu("Iceberg input has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_ICEBERG_READ} to true")
    }

    Try {
      IcebergFormatVersionSupport.tagForFormatVersion(
        GpuSparkScanAccess.table(meta.wrapped), meta)
    } match {
      case Failure(e) => meta.willNotWorkOnGpu(s"error examining Iceberg table version: $e")
      case _ =>
    }

    FileFormatChecks.tag(meta, meta.wrapped.readSchema(), IcebergFormatType, ReadFileOp)

    Try {
      GpuSparkScan.isMetadataScan(meta.wrapped)
    } match {
      case Success(true) => meta.willNotWorkOnGpu("scan is a metadata scan")
      case Failure(e) => meta.willNotWorkOnGpu(s"error examining CPU Iceberg scan: $e")
      case _ =>
    }

    gpuScan match {
      case Success(_) =>
        // Nested types (map, list, struct) are now supported
      case Failure(e) => meta.willNotWorkOnGpu(s"conversion to GPU scan failed: ${e.getMessage}")
    }
  }
}
