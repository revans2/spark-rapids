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

import java.io.{File, FileOutputStream, IOException}
import java.nio.file.{Files, Paths}

import scala.collection.mutable.ListBuffer

import com.nvidia.spark.rapids.RapidsConf
import com.nvidia.spark.rapids.shuffle.RapidsShufflePartitioning.ShuffleBatch

import org.apache.spark.{Partitioner, ShuffleDependency, SparkEnv, TaskContext}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleHandle, ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.storage.ShuffleIndexRecord


// Define a case class for the index records, though Spark's ShuffleIndexRecord can also be used.
import java.io.{DataOutputStream, File, FileOutputStream, IOException, PrintWriter}
import java.nio.file.{Files, Path, Paths}
import scala.collection.mutable.ListBuffer

import com.nvidia.spark.rapids.{GpuColumnVector, RapidsConf, ShuffleBatch} // Assuming ShuffleBatch for input
import org.apache.spark.{Partitioner, SparkEnv, TaskContext}
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.BlockManager
import org.apache.spark.util.Utils


// Define the index record structure
case class RapidsShuffleIndexRecord(partitionId: Int, segmentPath: String, length: Long, numRows: Long)

class RapidsShuffleMapOutputWriter(
    private val shuffleId: Int,
    private val mapId: Long, // mapId is Long (from TaskContext.mapId() which is Int, but matches ShuffleManager getWriter)
    private val context: TaskContext,
    private val partitioner: Partitioner,
    private val blockManager: BlockManager,
    private val rapidsConf: RapidsConf,
    private val writeMetrics: ShuffleWriteMetricsReporter) {

  private val attemptId = context.taskAttemptId()
  private val numPartitions = partitioner.numPartitions
  private val partitionSegmentCounters = Array.fill[Int](numPartitions)(0)

  // Get a base shuffle directory from Spark's DiskBlockManager
  // Spark uses subdirectories like "blockmgr-<uuid>/shuffleId/..."
  // We will create our own attempt specific directory within one of these.
  private val localDirs = blockManager.diskBlockManager.getShuffleLocalDirs
  if (localDirs.isEmpty) {
    throw new IOException("No local shuffle directories configured for Spark")
  }
  // Pick a directory (e.g., round-robin or based on mapId hash)
  // For simplicity, using mapId to pick, ensuring it's non-negative.
  private val baseDir = localDirs(Math.floorMod(mapId.hashCode, localDirs.length))

  // Create the specific output directory for this map attempt
  // Format: baseDir / <blockmgr-uuid-if-any-from-spark> / shuffleId / mapId / attemptId
  // Spark's DiskBlockManager.getShuffleWritePath(shuffleId, mapId, reduceId) gives an idea.
  // We need a directory per map attempt to store all partition segments and the index file.
  // Let's use: chosenLocalDir / "rapids_shuffle" / shuffleId / mapId / attemptId
  // Note: Spark's own shuffle writers often use "shuffle" as a top-level dir within a blockmgr dir.
  private val mapAttemptDir = Paths.get(baseDir.getAbsolutePath,
    "rapids_shuffle", // Custom subdirectory to avoid conflict
    shuffleId.toString,
    mapId.toString,
    attemptId.toString).toFile

  if (!mapAttemptDir.exists()) {
    if (!mapAttemptDir.mkdirs()) {
      throw new IOException(s"Failed to create map attempt directory: ${mapAttemptDir.getAbsolutePath}")
    }
  }

  private val indexRecords = new ListBuffer[RapidsShuffleIndexRecord]()
  private val partitionLengths: Array[Long] = new Array[Long](numPartitions)

  def writePartitionedData[K, V](records: Iterator[Product2[K, V]]): Array[Long] = {
    records.foreach { case (key, value) =>
      val cb = value.asInstanceOf[ColumnarBatch]
      // Assuming key is already the partitionId as per ShuffleWriteProcessor
      val partId = key.asInstanceOf[Int]

      val segmentCounter = partitionSegmentCounters(partId)
      partitionSegmentCounters(partId) += 1
      val segmentFileName = s"${partId}_segment_${segmentCounter}.data"
      val segmentFile = new File(mapAttemptDir, segmentFileName)

      var stream: DataOutputStream = null // Using DataOutputStream for numRows
      var length: Long = 0
      val numRows = cb.numRows()

      try {
        stream = new DataOutputStream(new FileOutputStream(segmentFile))

        // TODO: Implement actual ColumnarBatch serialization (e.g., Arrow IPC)
        // Fallback: write numRows (as Int) + dummy data per row
        stream.writeInt(numRows)
        length += 4 // for the int

        // Simulate data write: 16 bytes per row as dummy
        val dummyBytesPerRow = 16
        val dataBytes = Array.fill[Byte](numRows * dummyBytesPerRow)(0x42.toByte)
        stream.write(dataBytes)
        length += dataBytes.length

        // Actual serialization would be something like:
        // val table = GpuColumnVector.from(cb)
        // length = table.writeArrowToFile(fos, codec = null) // or similar API
        // table.close() // if from(cb) creates a new table needing close

      } catch {
        case e: IOException =>
          // TODO: cleanup segmentFile if write fails partially?
          throw new IOException(s"Error writing shuffle segment ${segmentFile.getAbsolutePath}", e)
      } finally {
        if (stream != null) {
          stream.close()
        }
        cb.close() // Always close the incoming batch
      }

      partitionLengths(partId) += length
      // Store relative path to mapAttemptDir for index record
      indexRecords += RapidsShuffleIndexRecord(partId, segmentFileName, length, numRows)

      writeMetrics.incBytesWritten(length)
      // writeMetrics.incRecordsWritten(numRows) // This should be Spark task records, not GPU table rows.
      // Let's assume 1 input Product2[K,V] is one "record" for this metric.
      writeMetrics.incRecordsWritten(1)
    }

    writeIndexFile()
    partitionLengths
  }

  private def writeIndexFile(): Path = {
    // Index file name could be e.g., mapId.index, or attemptId.index
    // Let's use a name that incorporates mapId and attemptId for clarity if needed.
    val indexFile = new File(mapAttemptDir, s"map_${mapId}_attempt_${attemptId}.index")
    var writer: PrintWriter = null
    try {
      writer = new PrintWriter(new FileOutputStream(indexFile))
      indexRecords.foreach { record =>
        // Using a simple JSON format for the index.
        val line = s"""{"partitionId":${record.partitionId},"segmentPath":"${record.segmentPath}","length":${record.length},"numRows":${record.numRows}}"""
        writer.println(line)
      }
    } catch {
      case e: IOException =>
        throw new IOException(s"Error writing shuffle index file ${indexFile.getAbsolutePath}", e)
    } finally {
      if (writer != null) {
        writer.close()
      }
    }
    Paths.get(indexFile.getAbsolutePath)
  }

  def getIndexFilePath(): Path = {
    Paths.get(mapAttemptDir.getAbsolutePath, s"map_${mapId}_attempt_${attemptId}.index")
  }

  def stop(success: Boolean): Unit = {
    if (!success) {
      // Delete the entire map attempt directory
      try {
        if (mapAttemptDir.exists()) {
          Utils.deleteRecursively(mapAttemptDir)
        }
      } catch {
        case e: IOException =>
          System.err.println(s"Error deleting map attempt directory ${mapAttemptDir.getAbsolutePath}: ${e.getMessage}")
      }
    }
    // If success, files are kept.
  }
}
