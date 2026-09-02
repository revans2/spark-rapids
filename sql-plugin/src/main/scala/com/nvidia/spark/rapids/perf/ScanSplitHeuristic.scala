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

package com.nvidia.spark.rapids.perf

import com.nvidia.spark.history.HistoryMetricCatalog
import com.nvidia.spark.rapids.GpuMetric

import org.apache.spark.sql.rapids.GpuFileSourceScanExec

/**
 * How far one scan's decoded device bytes expand beyond the file bytes it listed.
 */
object ScanExpansionRatio extends HistoryMetric {

  protected def metricId: Int = HistoryMetricCatalog.SCAN_EXPANSION_RATIO_ID

  protected def dimension: String = "relation"

  /**
   * Keys on the table, the columns decoded, and the filters pushed into the scan - all three move
   * the ratio, so scans differing in any of them must not share a slot.
   */
  def scanKey(table: String, columns: Seq[String], filters: Seq[String]): String = {
    if (table == null || table.isEmpty) {
      ""
    } else {
      hashKey(table, columns.sorted.mkString(","), filters.sorted.mkString(","))
    }
  }
}

/** What planning has in hand when it sizes one scan. */
final case class ScanContext(
    table: String,
    columns: Seq[String],
    filters: Seq[String],
    listedBytes: Long,
    batchSizeBytes: Long,
    minPartitionNum: Long,
    maxSplitBytes: Long,
    node: GpuFileSourceScanExec)

/** Pure split arithmetic. No state, no store, no Spark. */
object ScanSplitSizer {

  val MAX_SPLIT_BYTES: Long = 4L * 1024 * 1024 * 1024
  val MIN_SPLIT_BYTES: Long = 64L * 1024 * 1024

  /** The formula: how big a split this ratio implies. Zero means no usable answer. */
  def rawSplit(ratio: Double, targetBytes: Long): Long = {
    if (ratio <= 0.0d || ratio.isNaN || ratio.isInfinite || targetBytes <= 0L) {
      0L
    } else {
      (targetBytes.toDouble / ratio).toLong
    }
  }

  /**
   * The bounds. The ceiling keeps roughly one task per core, so a low ratio cannot collapse a
   * table into a few huge tasks. The floor is an absolute minimum, so a high-expansion table can
   * take a smaller split than Spark chose.
   */
  def bound(raw: Long, listedBytes: Long, minPartitionNum: Long, maxSplitBytes: Long): Long = {
    if (raw <= 0L) {
      maxSplitBytes
    } else {
      val parallelismCeiling =
        if (minPartitionNum > 0L) math.max(1L, listedBytes / minPartitionNum) else MAX_SPLIT_BYTES
      val ceiling = math.min(MAX_SPLIT_BYTES, parallelismCeiling)
      math.max(MIN_SPLIT_BYTES, math.min(ceiling, raw))
    }
  }
}

/** Sizes a scan's split from the expansion ratio that scan last produced. */
object ScanSplitHeuristic extends HistoryHeuristic {

  type Ctx = ScanContext
  type Decision = Long

  def name: String = "scan.split"

  def metrics: Seq[HistoryMetric] = Seq(ScanExpansionRatio)

  protected def keyFor(metric: HistoryMetric, ctx: ScanContext): String =
    ScanExpansionRatio.scanKey(ctx.table, ctx.columns, ctx.filters)

  protected def staticDecision(ctx: ScanContext): Long = ctx.maxSplitBytes

  protected def decideFrom(observed: Map[HistoryMetric, Double], ctx: ScanContext): Long =
    observed.get(ScanExpansionRatio)
      .map(ratio => ScanSplitSizer.rawSplit(ratio, ctx.batchSizeBytes))
      .getOrElse(0L)

  override protected def constrain(raw: Long, ctx: ScanContext): Long =
    ScanSplitSizer.bound(raw, ctx.listedBytes, ctx.minPartitionNum, ctx.maxSplitBytes)

  protected def observe(ctx: ScanContext): Map[HistoryMetric, Double] = {
    val decoded =
      ctx.node.metrics.get(GpuMetric.GPU_OUTPUT_BATCH_BYTES).map(_.value).getOrElse(0L)
    if (decoded > 0L) {
      Map(ScanExpansionRatio -> decoded.toDouble / ctx.listedBytes.toDouble)
    } else {
      Map.empty
    }
  }

  override protected def shouldObserve(ctx: ScanContext): Boolean =
    ctx.table != null && ctx.table.nonEmpty && ctx.listedBytes > 0L
}
