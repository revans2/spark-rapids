/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shuffle

import java.io.{BufferedReader, File, FileInputStream, FileReader, IOException}
import java.nio.file.Paths
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import com.nvidia.spark.rapids.RapidsConf

import org.apache.spark.{SparkConf, SparkEnv, TaskContext, MapOutputTracker}
import org.apache.spark.shuffle._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.BlockManager
import org.apache.spark.internal.Logging

// Assuming RapidsShuffleIndexRecord is defined here or accessible.
// If it's in its own file and compiled, it would be imported.
import java.io.{BufferedReader, DataInputStream, File, FileInputStream, FileReader, IOException}
import java.nio.file.{Path, Paths}
import scala.collection.mutable.ListBuffer

import com.nvidia.spark.rapids.{RapidsConf, GpuColumnVector} // GpuColumnVector might be needed for deserialization
import org.apache.spark.TaskContext
import org.apache.spark.shuffle.ShuffleReadMetricsReporter
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.BlockManager
import org.apache.spark.internal.Logging


// RapidsShuffleIndexRecord should be accessible from writer's definition if in same package, or imported.

class RapidsShuffleMapOutputReader(
    private val shuffleId: Int,
    private val mapId: Long, // Retained for logging/context if needed
    // private val attemptId: Long, // No longer needed if index file path is direct
    private val targetPartitionId: Int, // Specific partition this reader instance will read segments for
    private val context: TaskContext,
    private val indexFilePath: Path, // Direct path to the (local) custom index file
    private val dataInputStream: InputStream, // Stream for the entire partition_<targetPartitionId>.data file
    private val rapidsConf: RapidsConf,
    private val readMetrics: ShuffleReadMetricsReporter) extends Logging {

  // mapAttemptDir is not strictly needed if dataInputStream is for the whole partition file
  // and segmentPaths in index are relative to nothing (or ignored if only offset/length used)
  // However, if segmentPath was relative to mapAttemptDir, we might need it.
  // The index currently stores "segmentPath" which was relative to mapAttemptDir.
  // But now we have a single data file per partition. The index records (offset, length)
  // are for *within* that single partition data file.

  def read(): Iterator[ColumnarBatch] = {
    logInfo(s"RapidsShuffleMapOutputReader: Reading shuffle $shuffleId, map $mapId, partition $targetPartitionId using index $indexFilePath")

    if (!Files.exists(indexFilePath)) {
      logWarning(s"Index file $indexFilePath does not exist.")
      return Iterator.empty
    }

    val recordsToRead = ListBuffer[RapidsShuffleIndexRecord]()
    var idxReader: BufferedReader = null // Renamed to avoid conflict
    try {
      idxReader = new BufferedReader(new FileReader(indexFilePath.toFile))
      var line: String = idxReader.readLine()
      while (line != null) {
        try {
          val stripped = line.stripPrefix("{").stripSuffix("}")
          val parts = stripped.split(",").map(_.trim).toList
          val fields = parts.map { part =>
            val kv = part.split(":", 2)
            kv(0).trim.stripPrefix("\"").stripSuffix("\"") -> kv(1).trim.stripPrefix("\"").stripSuffix("\"")
          }.toMap

          val currentJsonPartitionId = fields("partitionId").toInt
          // Filter for the specific targetPartitionId this reader instance is for.
          // The index file contains records for *all* partitions written by that map task.
          if (currentJsonPartitionId == targetPartitionId) {
            // segmentPath is no longer used to open a file, offset and length are key.
            // val segmentPath = fields("segmentPath")
            val offset = fields("offset").toLong
            val length = fields("length").toLong
            val numRows = fields("numRows").toLong
            // segmentPath is not needed in record if we operate on a single stream
            recordsToRead += RapidsShuffleIndexRecord(currentJsonPartitionId, offset, length, numRows)
          }
        } catch {
          case e: Exception => logWarning(s"Skipping malformed index line in ${indexFilePath.toFile.getName}: '$line': ${e.getMessage}")
        }
        line = idxReader.readLine()
      }
    } catch {
      case e: IOException =>
        logError(s"Error reading index file ${indexFilePath.toFile.getAbsolutePath}", e)
        return Iterator.empty
    } finally {
      if (idxReader != null) {
        try {
          idxReader.close()
        } catch {
          case e: IOException => logError(s"Error closing index file reader for $indexFilePath", e)
        }
      }
    }

    // The dataInputStream is for the entire partition file. We need to read segments from it.
    // This requires careful management of the stream if it's to be read sequentially for multiple segments.
    // For now, let's assume the placeholder reader consumes part of the stream for each record.
    // This means the DataInputStream should not be closed until all segments are read.
    // An alternative is to re-open/seek for each segment, but less efficient if already streamed.
    // Spark's ChunkedByteBufferManagedBuffer might provide a seekable view if needed.
    // For simplicity now, wrap dataInputStream in DataInputStream for each segment. This is NOT efficient
    // if the underlying stream doesn't support marking/resetting or if segments are not contiguous.
    // Given current writer writes segments contiguously, we can read sequentially from dataInputStream.

    val dataSegmentStream = new DataInputStream(dataInputStream) // Wrap once

    recordsToRead.iterator.flatMap { record =>
      logInfo(s"Reading data for partition ${record.partitionId} from stream, offset ${record.offset}, length ${record.length}, numRows ${record.numRows}")
      // The `record.offset` is from the start of the partition file.
      // If reading sequentially, we need to ensure we are at the correct offset.
      // The current placeholder writer writes segments back-to-back, so a sequential read is fine.

      try {
        // TODO: Implement actual ColumnarBatch deserialization (e.g., Arrow IPC)
        // Fallback: read numRows (as Int) and then dummy data.
        val readNumRowsInSegment = dataSegmentStream.readInt()
        if (readNumRowsInSegment != record.numRows) {
            logWarning(s"Row count mismatch for segment. Expected ${record.numRows}, found $readNumRowsInSegment in stream for partition $targetPartitionId")
        }

        val expectedDataPayloadLength = record.length - 4 // Subtract 4 bytes for the numRows int
        if (expectedDataPayloadLength < 0) {
            throw new IOException(s"Invalid record length ${record.length} for segment in partition $targetPartitionId")
        }
        val readBytes = new Array[Byte](expectedDataPayloadLength.toInt)
        dataSegmentStream.readFully(readBytes)

        val cb = new ColumnarBatch(Array(), readNumRowsInSegment)

        readMetrics.incRemoteBytesRead(record.length)
        readMetrics.incRecordsRead(1)
        Some(cb)
      } catch {
        case e: IOException =>
          logError(s"Error reading data segment for partition $targetPartitionId from stream, skipping this segment.", e)
          None
      }
      // NOTE: The main dataInputStream is NOT closed here. It's the responsibility of the
      // caller (RapidsShuffleReader, from the ManagedBuffer) to close it once the iterator is exhausted.
    }
  }
}
