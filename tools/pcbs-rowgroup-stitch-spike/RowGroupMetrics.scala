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

import java.util.concurrent.atomic.AtomicLong

/** Process-local research counters. Snapshot and extrema reset occur between quiescent phases. */
object RowGroupMetrics {
  private val encodeAttempts = new AtomicLong
  private val encodeSuccesses = new AtomicLong
  private val encodedRows = new AtomicLong
  private val storedRecords = new AtomicLong
  private val storedRows = new AtomicLong
  private val storedBytes = new AtomicLong
  private val storedMaxBytes = new AtomicLong
  private val storedMaxEstimatedDecodedBytes = new AtomicLong
  private val decodeAttempts = new AtomicLong
  private val decodeSuccesses = new AtomicLong
  private val decodeFiles = new AtomicLong
  private val decodeRows = new AtomicLong
  private val decodeDeviceBytes = new AtomicLong
  private val decodeMaxDeviceBytes = new AtomicLong
  private val bundleMaxEstimatedDecodedBytes = new AtomicLong
  private val bundleMaxEncodedBytes = new AtomicLong

  private def maximum(counter: AtomicLong, value: Long): Unit = {
    var previous = counter.get()
    while (value > previous && !counter.compareAndSet(previous, value)) {
      previous = counter.get()
    }
  }

  def recordEncodeAttempt(): Unit = {
    encodeAttempts.incrementAndGet()
  }

  def recordEncoded(rows: Long): Unit = {
    require(rows >= 0)
    encodeSuccesses.incrementAndGet()
    encodedRows.addAndGet(rows)
  }

  def recordStored(info: ParquetRowGroupFiles.FileInfo): Unit = {
    require(info.rows >= 0 && info.encodedBytes >= 0 && info.estimatedDecodedBytes >= 0)
    storedRecords.incrementAndGet()
    storedRows.addAndGet(info.rows)
    storedBytes.addAndGet(info.encodedBytes)
    maximum(storedMaxBytes, info.encodedBytes)
    maximum(storedMaxEstimatedDecodedBytes, info.estimatedDecodedBytes)
  }

  def recordDecodeAttempt(): Unit = {
    decodeAttempts.incrementAndGet()
  }

  /** Called after a successful synchronous decode; counts attempts separately from outputs. */
  def recordDecoded(files: Int, rows: Long, deviceBytes: Long): Unit = {
    require(files > 0 && rows >= 0 && deviceBytes >= 0)
    decodeSuccesses.incrementAndGet()
    decodeFiles.addAndGet(files)
    decodeRows.addAndGet(rows)
    decodeDeviceBytes.addAndGet(deviceBytes)
    maximum(decodeMaxDeviceBytes, deviceBytes)
  }

  def recordBundle(estimatedDecodedBytes: Long, encodedBytes: Long): Unit = {
    require(estimatedDecodedBytes >= 0 && encodedBytes >= 0)
    maximum(bundleMaxEstimatedDecodedBytes, estimatedDecodedBytes)
    maximum(bundleMaxEncodedBytes, encodedBytes)
  }

  /** Retain cumulative counts and fill-time maxima; reset only read-phase high-water values. */
  def resetReadExtrema(): Unit = {
    decodeMaxDeviceBytes.set(0)
    bundleMaxEstimatedDecodedBytes.set(0)
    bundleMaxEncodedBytes.set(0)
  }

  /** No buffers, payload objects or CUDA synchronization are involved in observation. */
  def snapshotJson(): String = {
    val fields = Seq(
      "rg_encode_attempts" -> encodeAttempts.get(),
      "rg_encode_successes" -> encodeSuccesses.get(),
      "rg_encoded_rows" -> encodedRows.get(),
      "rg_stored_records" -> storedRecords.get(),
      "rg_stored_rows" -> storedRows.get(),
      "rg_stored_bytes" -> storedBytes.get(),
      "rg_stored_max_bytes" -> storedMaxBytes.get(),
      "rg_stored_max_estimated_decoded_bytes" -> storedMaxEstimatedDecodedBytes.get(),
      "rg_decode_attempts" -> decodeAttempts.get(),
      "rg_decode_successes" -> decodeSuccesses.get(),
      "rg_decode_files" -> decodeFiles.get(),
      "rg_decode_rows" -> decodeRows.get(),
      "rg_decode_device_bytes" -> decodeDeviceBytes.get(),
      "rg_decode_max_device_bytes" -> decodeMaxDeviceBytes.get(),
      "rg_bundle_max_estimated_decoded_bytes" -> bundleMaxEstimatedDecodedBytes.get(),
      "rg_bundle_max_encoded_bytes" -> bundleMaxEncodedBytes.get())
    fields.map { case (key, value) => "\"" + key + "\":" + value }
      .mkString("{", ",", "}")
  }
}
