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

import java.io.IOException
import java.nio.file.{Files, Paths}
import scala.reflect.ClassTag

import com.nvidia.spark.rapids.RapidsConf

import org.apache.spark.{SparkConf, SparkEnv, TaskContext, ShuffleDependency}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.sort.SortShuffleManager // For potential fallback or comparison
import org.apache.spark.storage.BlockManager
import org.apache.spark.util.Utils
import org.apache.spark.scheduler.MapStatus // Required for RapidsShuffleWriterImpl
import java.io.File // Required for RapidsShuffleWriterImpl


class RapidsShuffleManager(conf: SparkConf, isDriver: Boolean) extends ShuffleManager with Logging {

  private lazy val rapidsConf = new RapidsConf(conf)
  private lazy val blockManager = SparkEnv.get.blockManager
  private lazy val mapOutputTracker = SparkEnv.get.mapOutputTracker

  // Initialize RapidsShuffleBlockResolver, it requires BlockManager which might not be ready at constructor time on driver
  // It will be fully initialized when shuffleBlockResolver is first called.
  private var _shuffleBlockResolver: RapidsShuffleBlockResolver = null

  logInfo("Initializing RapidsShuffleManager")

  // val shuffleRootDir = rapidsConf.shuffleSeekyRootDir // To be removed
  // if (shuffleRootDir == null || shuffleRootDir.isEmpty) {
  //   throw new IllegalArgumentException(s"${RapidsConf.SHUFFLE_SEEKY_ROOT_DIR.key} must be set")
  // }

  // Create the root directory if it doesn't exist.
  // This is typically done by each executor for its local portion if not on shared storage.
  // If on shared storage, the driver might do this, or it's pre-provisioned.
  // For simplicity in local testing, ensure it's created.
  // private val rootDirPath = Paths.get(shuffleRootDir) // To be removed
  // if (!Files.exists(rootDirPath)) {
  //   logInfo(s"Shuffle root directory ${rootDirPath.toAbsolutePath.toString} does not exist. Creating it now.")
  //   try {
  //     Files.createDirectories(rootDirPath)
  //   } catch {
  //     case e: IOException =>
  //       throw new IOException(s"Failed to create shuffle root directory: ${rootDirPath.toAbsolutePath.toString}", e)
  //   }
  // } else if (!Files.isDirectory(rootDirPath)) {
  //   throw new IOException(s"Configured shuffle root dir ${rootDirPath.toAbsolutePath.toString} is not a directory.")
  // }


  // Used by getReaderV2 (ShuffleManager interface from Spark 3.x)
  // We are targeting the main getReader method for now.
  //  override def getReaderForRange[K, C](
  //      handle: ShuffleHandle,
  //      startMapIndex: Int,
  //      endMapIndex: Int,
  //      startPartition: Int,
  //      endPartition: Int,
  //      context: TaskContext,
  //      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
  //    logInfo(s"RapidsShuffleManager.getReaderForRange called for shuffle ${handle.shuffleId}")
  //    // This specific signature might not be used if the simpler getReader is overridden effectively.
  //    // For now, delegate to the main getReader or implement separately if specific range logic is needed.
  //    getReader(handle, startPartition, endPartition, context, metrics)
  //  }


  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    logInfo(s"RapidsShuffleManager registering shuffle $shuffleId")
    // The ShuffleDependency type parameters are K (key), V (value), C (combiner output, often same as V).
    // BaseShuffleHandle typically expects ShuffleDependency[K, V, V] if no map-side combine,
    // or ShuffleDependency[K, V, C] if there is a combiner.
    // The type of `dependency` passed in is ShuffleDependency[K, V, C].
    // If C is different from V (aggregator is defined), then it's a map-side combined shuffle.
    // If C is same as V (no aggregator or aggregator with same output type), then it's not combined or combine doesn't change type.
    // Casting to ShuffleDependency[K, V, V] is problematic if C is actually different.
    // Let's try to be more type-safe. The third param to BaseShuffleHandle is the map output value type.
    // If there's an aggregator, map output value type is C. Otherwise it's V.
    // Spark's ShuffleWriter takes <K, V>, and its input records are Iterator[Product2[K,V]].
    // Spark's ShuffleReader takes <K, C> and its output records are Iterator[Product2[K,C]].
    // So, the 'C' in ShuffleReader corresponds to the 'C' in ShuffleDependency if an aggregator is present,
    // or 'V' if no aggregator.
    // The ShuffleHandle is passed from registerShuffle to getWriter and getReader.
    // The important thing is that the handle carries the original dependency.
    new BaseShuffleHandle(shuffleId, dependency.asInstanceOf[ShuffleDependency[Any, Any, Any]])
  }

  override def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    logInfo(s"RapidsShuffleManager.getReader called for shuffle ${handle.shuffleId} for partitions $startPartition-$endPartition")
    new RapidsShuffleReader[K, C](
      handle.asInstanceOf[BaseShuffleHandle[K, _, C]],
      startPartition,
      endPartition,
      context,
      metrics,
      blockManager, // from RapidsShuffleManager scope
      mapOutputTracker, // from RapidsShuffleManager scope
      rapidsConf)
  }

  // This is the getReader signature from Spark 2.x, less common now but good to have a stub.
   override def getReader[K, C](
       handle: ShuffleHandle,
       startPartition: Int,
       endPartition: Int,
       context: TaskContext): ShuffleReader[K, C] = {
     logWarning("RapidsShuffleManager.getReader (3-arg with no metrics) called. This is deprecated and not fully supported by RapidsShuffleManager.")
     // To fully support, we'd need to create a ShuffleReadMetricsReporter instance.
     // For now, throwing UOE as it's unlikely to be hit in modern Spark versions configured for metrics.
     throw new UnsupportedOperationException("RapidsShuffleManager requires ShuffleReadMetricsReporter for getReader.")
   }

  // This is the getReader signature from Spark 3.1+ with map range.
  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    logInfo(s"RapidsShuffleManager.getReader (with mapIndex range) called for shuffle ${handle.shuffleId}. Partitions $startPartition-$endPartition, Maps $startMapIndex-$endMapIndex")
    // The current RapidsShuffleReader fetches all map statuses for the given reduce partition range.
    // It does not yet filter by startMapIndex/endMapIndex before fetching.
    // This could be an optimization in RapidsShuffleReader to pass these down.
    logWarning(s"RapidsShuffleManager.getReader with mapIndex range: The mapIndex range ($startMapIndex - $endMapIndex) is not explicitly used by the current RapidsShuffleReader to filter map statuses before fetching, but individual map reads might be skipped if not in range by Spark's logic upstream or if MapOutputTracker returns filtered statuses.")
    new RapidsShuffleReader[K, C](
        handle.asInstanceOf[BaseShuffleHandle[K, _, C]],
        startPartition,
        endPartition,
        context,
        metrics,
        blockManager,
        mapOutputTracker,
        rapidsConf)
  }


  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long, // mapId is Long
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    logInfo(s"RapidsShuffleManager.getWriter called for shuffle ${handle.shuffleId}, map ${mapId}")

    val baseHandle = handle.asInstanceOf[BaseShuffleHandle[K, V, _]] // Use _ for C as it's not directly used by writer logic here
    val partitioner = baseHandle.dependency.partitioner

    new RapidsShuffleWriterImpl[K, V](
      handle.shuffleId,
      mapId,
      context,
      partitioner,
      blockManager, // blockManager from RapidsShuffleManager scope
      shuffleBlockResolver(), // Call method to get/init resolver
      rapidsConf,
      metrics
    )
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    logInfo(s"RapidsShuffleManager unregistering shuffle $shuffleId.")
    // This will delegate to the shuffleBlockResolver or handle cleanup directly if needed.
    if (_shuffleBlockResolver != null) {
      // Assuming RapidsShuffleBlockResolver has a method to clean up all data for a shuffleId
      // For now, this is a conceptual call.
      // _shuffleBlockResolver.removeShuffle(shuffleId)
    }
    true
  }

  override def shuffleBlockResolver: ShuffleBlockResolver = {
    if (_shuffleBlockResolver == null) {
      synchronized {
        if (_shuffleBlockResolver == null) {
          // SparkEnv.get might not be initialized when ShuffleManager is created on driver if SparkContext isn't up.
          // However, shuffleBlockResolver is usually accessed later.
          // It's safer to pass conf directly.
          _shuffleBlockResolver = new RapidsShuffleBlockResolver(conf, SparkEnv.get.blockManager, rapidsConf)
          logInfo("Initialized RapidsShuffleBlockResolver for RapidsShuffleManager")
        }
      }
    }
    _shuffleBlockResolver
  }

  override def stop(): Unit = {
    logInfo("RapidsShuffleManager stopping")
    if (_shuffleBlockResolver != null) {
      _shuffleBlockResolver.stop()
      _shuffleBlockResolver = null
    }
  }

  // Define RapidsShuffleWriterImpl as an inner class or in its own file.
  // For this exercise, let's make it a private inner class.
  private class RapidsShuffleWriterImpl[K, V](
      shuffleId: Int,
      mapId: Long,
      context: TaskContext,
      partitioner: Partitioner,
      blockManager: BlockManager, // Passed from outer class
      resolver: RapidsShuffleBlockResolver,
      rapidsConf: RapidsConf,
      metrics: ShuffleWriteMetricsReporter)
    extends ShuffleWriter[K, V] with Logging {

    private val mapOutputWriter = new RapidsShuffleMapOutputWriter(
      shuffleId, mapId, context, partitioner, blockManager, rapidsConf, metrics)

    private var partitionLengths: Array[Long] = null
    private var committedMapStatus: Option[MapStatus] = None // Renamed to avoid conflict with ShuffleWriter.mapStatus

    override def write(records: Iterator[Product2[K, V]]): Unit = {
      logInfo(s"RapidsShuffleWriterImpl: writing for shuffle $shuffleId, map $mapId (attempt ${context.taskAttemptId()})")
      try {
        partitionLengths = mapOutputWriter.writePartitionedData(records)
        logDebug(s"RapidsShuffleWriterImpl: partitionLengths after write for map $mapId: ${if (partitionLengths == null) "null" else partitionLengths.mkString(",")}")
      } catch {
        case e: Exception =>
          logError(s"Error during write for shuffle $shuffleId, map $mapId", e)
          // It's important to let the Task know this attempt failed.
          // mapOutputWriter.stop(false) // Clean up partial files
          throw e // Rethrow to fail the task
      }
    }

    override def commitAllPartitions(): Option[MapStatus] = {
      logInfo(s"RapidsShuffleWriterImpl: committing for shuffle $shuffleId, map $mapId (attempt ${context.taskAttemptId()})")
      if (partitionLengths == null) {
        // This can happen if write() was never called or failed before setting partitionLengths.
        // If write() was never called, it implies no records, so lengths should be all zeros.
        logWarning(s"RapidsShuffleWriterImpl: partitionLengths is null for shuffle $shuffleId, map $mapId. Assuming no data was written or write failed early.")
        // Attempting to commit with zero lengths for all partitions.
        // If this scenario (write not called, but commit is) is invalid, an exception is better.
        // For now, let's assume it implies zero records.
        partitionLengths = new Array[Long](partitioner.numPartitions)
      }

      val indexFile = mapOutputWriter.getIndexFilePath().toFile
      try {
        resolver.writeIndexFileAndCommit(shuffleId, mapId, context.taskAttemptId(), partitionLengths, indexFile)
        // The mapId for MapStatus should be an Int for some Spark versions, but ShuffleWriter mapId is Long.
        // TaskContext.mapId() is Int. Let's use that for MapStatus consistency.
        committedMapStatus = Some(MapStatus(blockManager.blockManagerId, partitionLengths, context.mapId()))
        logInfo(s"RapidsShuffleWriterImpl: successfully committed map $mapId (attempt ${context.taskAttemptId()}), MapStatus: $committedMapStatus")
        committedMapStatus
      } catch {
        case e: Exception =>
          logError(s"Error during commit for shuffle $shuffleId, map $mapId (attempt ${context.taskAttemptId()})", e)
          // mapOutputWriter.stop(false) // Clean up partial files from mapOutputWriter
          throw e // Rethrow to fail the task
      }
    }

    override def stop(success: Boolean): Option[MapStatus] = {
      logInfo(s"RapidsShuffleWriterImpl: stopping for shuffle $shuffleId, map $mapId (attempt ${context.taskAttemptId()}), success: $success")
      try {
        mapOutputWriter.stop(success)
      } catch {
        case e: Exception =>
          logError(s"Error stopping mapOutputWriter for shuffle $shuffleId, map $mapId", e)
          // Don't rethrow if stop itself fails, but log it.
      }

      if (success) {
        committedMapStatus.orElse {
          logWarning(s"RapidsShuffleWriterImpl: stop(success=true) called for map $mapId but no prior commit status available. This might indicate an issue.")
          // If commit was supposed to happen but didn't, this is problematic.
          // If it's a valid scenario (e.g. no data to write, commit might be optimized out by Spark for some writers),
          // then returning None or a zero-length MapStatus might be okay.
          // For now, return None if not already committed.
          None
        }
      } else {
        None
      }
    }
  }
}
