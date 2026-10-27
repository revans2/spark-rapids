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

import com.nvidia.spark.rapids.parquet.ParquetCachedBatch

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.columnar.DefaultCachedBatch

/** Optional post-run audit; this action may recompute cache blocks and change residency. */
object CachePayloadAudit {
  def summarize(frame: DataFrame): String = {
    val cached = frame.sparkSession.sharedState.cacheManager.lookupCachedData(frame)
      .getOrElse(throw new IllegalArgumentException("DataFrame is not cached"))
    val summaries = cached.cachedRepresentation.cacheBuilder.cachedColumnBuffers
      .mapPartitionsWithIndex { case (partition, input) =>
        var records = 0L
        var rows = 0L
        var payload = 0L
        input.foreach { batch =>
          records += 1
          rows += batch.numRows
          payload += (batch match {
            case value: DefaultCachedBatch => value.buffers.map(_.length.toLong).sum
            case value: ParquetCachedBatch => value.buffer.length.toLong
            case value: VortexCacheRecord => value.bytes.length.toLong
            case other => throw new UnsupportedOperationException(
              "Unknown cached payload " + other.getClass.getName)
          })
        }
        Iterator.single((partition, records, rows, payload))
      }.collect().sortBy(_._1)
    val partitions = summaries.map { case (partition, records, rows, bytes) =>
      s"""{"partition":$partition,"records":$records,"rows":$rows,"payload_bytes":$bytes}"""
    }.mkString(",")
    val scope = "post-run cache payload audit; may recompute and change residency"
    s"""{"scope":"$scope","partitions":[$partitions]}"""
  }
}
