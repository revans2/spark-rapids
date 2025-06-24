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

import java.io.{File, FileOutputStream, InputStream, IOException}
import java.nio.channels.Channels
import java.nio.file.Files

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{InterruptibleIterator, MapOutputTracker, SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.io.CompressionCodec
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleReadMetricsReporter, ShuffleReader}
import org.apache.spark.storage.{BlockManager, BlockManagerId, BlockId, ShuffleBlockId, ChunkedByteBuffer}
import org.apache.spark.util.Utils

import com.nvidia.spark.rapids.{RapidsConf, ColumnarOutputFromTable} // Assuming ColumnarOutputFromTable or similar for CB creation
import org.apache.spark.sql.vectorized.ColumnarBatch


class RapidsShuffleReader[K, C](
    handle: BaseShuffleHandle[K, _, C],
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    readMetrics: ShuffleReadMetricsReporter,
    blockManager: BlockManager = SparkEnv.get.blockManager,
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker,
    rapidsConf: RapidsConf = new RapidsConf(SparkEnv.get.conf))
  extends ShuffleReader[K, C] with Logging {

  private val dep = handle.dependency
  private val shuffleId = handle.shuffleId

  override def read(): Iterator[Product2[K, C]] = {
    logInfo(s"Reading shuffle $shuffleId partitions $startPartition - $endPartition using RapidsShuffleReader")

    val mapStatuses = mapOutputTracker.getMapSizesByExecutorId(shuffleId, startPartition, endPartition)
    logDebug(s"Got ${mapStatuses.size} map statuses for shuffle $shuffleId")

    val iterators = mapStatuses.iterator.flatMap { case (bmid: BlockManagerId, blockInfos: Seq[(BlockId, Long, Int)]) =>
      // blockInfos: Seq[(BlockId, Long, Int)] where Int is mapIndex / mapId (partition on map side for shuffle)
      // For shuffle, BlockId is ShuffleBlockId(shuffleId, mapId, reduceId)
      // The reduceId in ShuffleBlockId corresponds to the target *output* partition of the map task.
      // This is what we need to fetch. The 'startPartition' and 'endPartition' for the reader
      // refer to these output partition numbers from the map tasks.

      blockInfos.filter { case (blockId, _, _) =>
        blockId match {
          case sbid: ShuffleBlockId =>
            sbid.reduceId >= startPartition && sbid.reduceId < endPartition
          case _ =>
            logWarning(s"Unexpected BlockId type: $blockId, skipping.")
            false
        }
      }.map { case (blockId, blockSize, mapIndex) => // mapIndex is the map task id
        val sbid = blockId.asInstanceOf[ShuffleBlockId]
        val mapId = sbid.mapId // map task ID
        val reduceId = sbid.reduceId // partition ID we are fetching for this reduce task

        logDebug(s"Reading shuffle $shuffleId map $mapId reduceId $reduceId from BMid $bmid (size $blockSize)")

        // This is where we'd need to get the *attempt ID* for the given mapId.
        // MapOutputTracker.getMapSizesByExecutorId doesn't directly provide attempt IDs for blocks.
        // It provides successful BlockManagerIds.
        // A robust solution requires finding the successful attempt ID for the map task `mapId`.
        // This is a known complexity. Spark's NettyShuffleFetcher has logic for this.
        // For now, let's assume the ShuffleBlockResolver's `successfulAttempts` map (which tracks latest successful attempt)
        // is sufficient, or we fetch for a conventional attemptId (e.g. 0), which might not always be correct if retries happened.
        // The resolver's getBlockData for ShuffleBlockId already uses its `successfulAttempts` map.

        // Fetch the actual data block (ManagedBuffer for the whole partition file)
        var dataInputStream: InputStream = null
        try {
          blockManager.getBlockData(sbid) match {
            case Some(managedBuffer) =>
              dataInputStream = managedBuffer.createInputStream()

              // TODO: Phase 5 - Refactor RapidsShuffleMapOutputReader to take InputStream
              // For now, we can't directly use RapidsShuffleMapOutputReader as it expects to read files itself.
              // We need a way to get the custom index file for this specific map attempt.
              // Let's assume for this step that we can get the custom index file via another getBlockData call.
              // This is a placeholder for fetching and using the custom index.

              // Placeholder: Fetch custom index (this is a rough sketch of what's needed)
              // We need the actual attemptId that produced this sbid's data.
              // This is a critical piece missing from easy access via MapOutputTracker here.
              // Let's assume a fixed/latest attempt for now for the custom index.
              // The resolver's successfulAttempts map uses (shuffleId, mapId) and stores latest attempt.
              // So, we need to construct RapidsCustomShuffleIndexBlockId with that attemptId.
              // This is still problematic because the reducer doesn't know the specific attemptId
              // that the resolver on the executor stored.
              // A more robust way: the resolver's getBlockData for ShuffleBlockId should perhaps
              // return a custom ManagedBuffer that *also* gives access to its custom index.
              // OR MapOutputTracker needs to be augmented.

              // Simplification for now: We have the raw bytes of the partition file.
              // The actual deserialization logic using RapidsShuffleMapOutputReader with this stream
              // and the correct index file is deferred.
              // We'll create placeholder ColumnarBatches.

              val numRowsToSimulate = if (dataInputStream.available() > 4) {
                // This is reading the placeholder numRows written by the writer at the start of the partition file.
                // In a real scenario, this would be part of the segment deserialization.
                new java.io.DataInputStream(dataInputStream).readInt()
              } else { 0 }

              val batches = new ArrayBuffer[ColumnarBatch]()
              if (numRowsToSimulate > 0) {
                 // Placeholder: create one dummy batch from the stream
                val dummyBatch = new ColumnarBatch(Array(), numRowsToSimulate)
                batches += dummyBatch
              }
              // In a real scenario, we would use RapidsShuffleMapOutputReader with the dataInputStream
              // and the *correct* custom index for (shuffleId, mapId, attemptId) to get an
              // Iterator[ColumnarBatch] from the segments within this partition file.

              readMetrics.incRemoteBytesRead(blockSize)
              readMetrics.incRemoteBlocksFetched(1)

              batches.iterator.map(cb => (reduceId.asInstanceOf[K], cb.asInstanceOf[C]))
            case None =>
              throw new IOException(s"Failed to fetch data block $sbid from $bmid")
          }
        } catch {
          case e: Exception =>
            logError(s"Error fetching/processing data block $sbid from $bmid", e)
            Iterator.empty
        } finally {
          if (dataInputStream != null) {
            try dataInputStream.close() catch { case _: IOException => }
          }
        }
      }
    }
    new InterruptibleIterator(context, iterators.flatten)
  }
}
