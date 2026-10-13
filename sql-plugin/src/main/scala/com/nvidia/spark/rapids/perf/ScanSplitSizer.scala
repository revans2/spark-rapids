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

/** Pure split arithmetic. */
object ScanSplitSizer {

  val MAX_SPLIT_BYTES: Long = 4L * 1024 * 1024 * 1024
  val MIN_SPLIT_BYTES: Long = 64L * 1024 * 1024

  /** `targetBytes / ratio`, the split that decodes to one batch; 0 if unusable. */
  def rawSplit(ratio: Double, targetBytes: Long): Long = {
    if (ratio <= 0.0d || ratio.isNaN || ratio.isInfinite || targetBytes <= 0L) {
      0L
    } else {
      (targetBytes.toDouble / ratio).toLong
    }
  }

  /**
   * Clamps a raw split to `[MIN_SPLIT_BYTES, min(MAX_SPLIT_BYTES, listedBytes / minPartitionNum)]`,
   * or `maxSplitBytes` when `raw` is unusable.
   *
   * The ceiling keeps about `minPartitionNum` tasks.
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

  /**
   * Parallelism for the ceiling, as Spark resolves it for file scans, except configured executor
   * slots (instances x cores) come before registered cores, which vary with executor startup.
   * Non-positive values are ignored.
   */
  def minPartitionNum(
      filesMinPartitionNum: Option[Int],
      leafNodeDefaultParallelism: Option[Int],
      defaultParallelism: Option[Int],
      executorInstances: Option[Int],
      executorCores: Option[Int],
      registeredCores: => Int): Long = {
    def positive(v: Option[Int]): Option[Long] = v.filter(_ > 0).map(_.toLong)
    positive(filesMinPartitionNum)
      .orElse(positive(leafNodeDefaultParallelism))
      .orElse(positive(defaultParallelism))
      .orElse(for {
        instances <- positive(executorInstances)
        cores <- positive(executorCores)
      } yield instances * cores)
      .getOrElse(registeredCores.toLong)
  }
}
