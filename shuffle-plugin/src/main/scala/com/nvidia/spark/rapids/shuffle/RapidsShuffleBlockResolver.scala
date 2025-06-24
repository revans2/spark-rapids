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

import java.io.{DataOutputStream, File, FileOutputStream, IOException}
import java.nio.file.Paths

import com.nvidia.spark.rapids.RapidsConf
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.util.TransportConf
import org.apache.spark.shuffle.{ShuffleBlockId, ShuffleBlockResolver, ShuffleIndexBlockId}
import org.apache.spark.storage.{BlockId, BlockManager, ShuffleBlockId} // Added ShuffleBlockId explicitly
import org.apache.spark.util.Utils
import java.util.concurrent.ConcurrentHashMap


class RapidsShuffleBlockResolver(
    conf: SparkConf,
    blockManager: BlockManager,
    rapidsConf: RapidsConf) extends ShuffleBlockResolver with Logging {

  private val transportConf = SparkTransportConf.fromSparkConf(conf, "shuffle")
  // (shuffleId, mapId) -> (pathToCustomIndexFile, attemptId)
  private val successfulAttempts = new ConcurrentHashMap[(Int, Long), (String, Long)]()

  override def getBlockData(blockId: BlockId, execIdIfLocal: String): ManagedBuffer = {
    blockId match {
      case sbid: ShuffleBlockId =>
        logInfo(s"RapidsShuffleBlockResolver.getBlockData for ShuffleBlockId: $sbid (execId: $execIdIfLocal)")
        val shuffleId = sbid.shuffleId
        val mapId = sbid.mapId
        val partitionId = sbid.reduceId

        // Retrieve the path and attemptId for the LATEST successful attempt for this mapId
        val attemptInfo = successfulAttempts.get((shuffleId, mapId))
        if (attemptInfo == null) {
          throw new IOException(
            s"No successful attempt recorded for shuffle $shuffleId, map $mapId. Cannot serve partition data.")
        }
        val (customIndexFilePathStr, attemptId) = attemptInfo

        val mapAttemptDir = new File(customIndexFilePathStr).getParentFile // Dir of the custom index file
        val partitionDataFile = new File(mapAttemptDir, s"partition_$partitionId.data")

        if (!partitionDataFile.exists()) {
          throw new IOException(
            s"Partition data file not found: ${partitionDataFile.getAbsolutePath} for shuffle $shuffleId, map $mapId, attempt $attemptId, partition $partitionId")
        }
        new FileSegmentManagedBuffer(transportConf, partitionDataFile, 0, partitionDataFile.length())

      case RapidsCustomShuffleIndexBlockId(shId, mId, attId) =>
        logDebug(s"Getting custom shuffle index block: $blockId")
        // This case is for fetching our *custom* index file.
        // The path should be derivable if we know where the writer places it.
        // The writer places it in: baseDir/rapids_shuffle/shuffleId/mapId/attemptId/map_<mapId>_attempt_<attemptId>.index
        // We need to find which baseDir was used. This is tricky as BlockManager might have multiple.
        // For now, this assumes the request for custom index might come to an executor that has it locally.
        // A more robust way would be for MapOutputTracker to store this path if needed by reducers,
        // or for reducers to know which executor host the map task ran on.

        // Let's find the specific index file for the given attemptId.
        // The `successfulAttempts` map stores the latest one. If attId matches that, we can use it.
        // Otherwise, we have to reconstruct the path, assuming it's in one of the local dirs.
        val latestAttemptInfo = successfulAttempts.get((shId, mId))
        var specificCustomIndexFile: File = null

        if (latestAttemptInfo != null && latestAttemptInfo._2 == attId) {
            specificCustomIndexFile = new File(latestAttemptInfo._1)
        } else {
            // Path reconstruction logic (simplified, assumes first local dir)
            // This part is brittle if the actual map task ran on a different localDir on the executor.
            val localDir = blockManager.diskBlockManager.getShuffleLocalDirs.head
            val mapAttemptDir = Paths.get(localDir.getAbsolutePath,
                "rapids_shuffle", shId.toString, mId.toString, attId.toString).toFile
            specificCustomIndexFile = new File(mapAttemptDir, s"map_${mId}_attempt_${attId}.index")
        }

        if (!specificCustomIndexFile.exists() || !specificCustomIndexFile.isFile) {
          throw new IOException(s"Custom index file $specificCustomIndexFile not found for $blockId")
        }
        new FileSegmentManagedBuffer(transportConf, specificCustomIndexFile, 0, specificCustomIndexFile.length())

      case _ =>
        throw new UnsupportedOperationException(s"Unsupported BlockId type: ${blockId.getClass.getName}")
    }
  }

  def getBlockData(blockId: BlockId): ManagedBuffer = {
    getBlockData(blockId, null)
  }

  def writeIndexFileAndCommit(
      shuffleId: Int,
      mapId: Long,
      attemptId: Long, // Added attemptId
      lengths: Array[Long],
      customIndexFile: File): Unit = {

    if (customIndexFile == null || !customIndexFile.exists()) {
      logError(s"Custom index file path is null or does not exist: ${customIndexFile}. Cannot create Spark index file for shuffle $shuffleId, map $mapId, attempt $attemptId.")
      if (lengths == null) {
        throw new IOException(s"Custom index file path is invalid AND lengths are null for shuffle $shuffleId, map $mapId, attempt $attemptId.")
      }
      logWarning(s"Proceeding to write Spark index file for shuffle $shuffleId, map $mapId, attempt $attemptId using provided lengths despite custom index file issue.")
    } else {
      logInfo(s"Custom index file for shuffle $shuffleId, map $mapId, attempt $attemptId is at: ${customIndexFile.getAbsolutePath}")
      successfulAttempts.put((shuffleId, mapId), (customIndexFile.getAbsolutePath, attemptId))
    }

    val mapIdInt = mapId.toInt
    val indexFile = blockManager.diskBlockManager.getFile(ShuffleIndexBlockId(shuffleId, mapIdInt, 0))
    val tmpPath = Utils.tempFileWith(indexFile)
    var out: DataOutputStream = null
    try {
      out = new DataOutputStream(new FileOutputStream(tmpPath))
      var offset = 0L
      out.writeLong(offset) // First entry is always 0
      for (length <- lengths) {
        offset += length
        out.writeLong(offset)
      }
      logInfo(s"Successfully wrote Spark index file: ${indexFile.getAbsolutePath} for shuffle $shuffleId, map $mapId with ${lengths.length} partitions. Total size: $offset")
    } catch {
      case e: IOException =>
        logError(s"Error writing Spark index file ${tmpPath.getAbsolutePath} for shuffle $shuffleId, map $mapId", e)
        if (tmpPath.exists() && !tmpPath.delete()) {
          logWarning(s"Error deleting temp Spark index file ${tmpPath.getPath}")
        }
        throw e
    } finally {
      if (out != null) {
        out.close()
      }
    }

    if (!tmpPath.renameTo(indexFile)) {
      if (indexFile.exists() && !indexFile.delete()) {
         logWarning(s"Error deleting existing Spark index file ${indexFile.getPath}")
      }
      if (!tmpPath.renameTo(indexFile)) {
        throw new IOException(s"Error renaming temporary Spark index file ${tmpPath.getPath} to ${indexFile.getPath}")
      }
    }
  }

  /**
   * Removes data associated with a specific map output of a shuffle.
   * This includes:
   * 1. The Spark-generated shuffle index file (e.g., shuffle_${shuffleId}_${shuffleMapId}_0.index).
   * 2. The custom shuffle data directory for that map task (e.g., .../rapids_shuffle/shuffleId/shuffleMapId/attemptId/).
   *    Since multiple attempts might exist, it should ideally clean all attempts for that mapId.
   */
  override def removeDataByShuffle(shuffleId: Int, shuffleMapId: Int): Unit = { // shuffleMapId is the mapId
    logInfo(s"RapidsShuffleBlockResolver.removeDataByShuffle for shuffle $shuffleId, map $shuffleMapId")

    // 1. Delete Spark's shuffle index file
    val sparkIndexFile = blockManager.diskBlockManager.getFile(ShuffleIndexBlockId(shuffleId, shuffleMapId, 0))
    if (sparkIndexFile.exists() && !sparkIndexFile.delete()) {
      logWarning(s"Error deleting Spark index file ${sparkIndexFile.getPath} for shuffle $shuffleId, map $shuffleMapId")
    }

    // 2. Delete our custom shuffle data directories for all attempts of this map task
    val localDirs = blockManager.diskBlockManager.getShuffleLocalDirs
    for (baseLocalDir <- localDirs) {
      val mapIdDir = Paths.get(baseLocalDir.getAbsolutePath,
        "rapids_shuffle",
        shuffleId.toString,
        shuffleMapId.toString).toFile

      if (mapIdDir.exists()) {
        try {
          Utils.deleteRecursively(mapIdDir)
          logInfo(s"Successfully deleted custom shuffle data directory for map $shuffleMapId: ${mapIdDir.getAbsolutePath}")
        } catch {
          case e: IOException =>
            logError(s"Error deleting custom shuffle data directory for map $shuffleMapId: ${mapIdDir.getAbsolutePath}", e)
        }
      }
    }
    successfulAttempts.remove((shuffleId, shuffleMapId.toLong))
  }

  override def stop(): Unit = {
    logInfo("RapidsShuffleBlockResolver stopped.")
    successfulAttempts.clear()
  }
}
