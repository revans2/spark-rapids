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
package org.apache.spark.sql.codeccachespike

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEnv}
import org.apache.spark.util.RpcUtils
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.storage.{RDDBlockId, StorageLevel}

/** Scheduler sees a cached ancestor before its local block is removed in compute. */
class RemoveCachedParent(previous: RDD[CachedBatch])
    extends RDD[CachedBatch](previous.context, Seq(
      new NarrowDependency[CachedBatch](previous) {
        override def getParents(partitionId: Int): Seq[Int] = previous.partitions.indices
      })) {
  override protected def getPartitions: Array[Partition] = Array(previous.partitions.head)
  override def compute(split: Partition, context: TaskContext): Iterator[CachedBatch] = {
    val env = SparkEnv.get
    val gate = RpcUtils.makeDriverRef("CodecRecoveryFaultGate", env.conf, env.rpcEnv)
    require(gate.address != null, "fixture driver gate must have a routable RPC address")
    require(gate.askSync[Boolean](previous.id))
    println(s"CODEC_RECOVERY CACHE_REMOVAL_GATE blocks=${previous.partitions.length} " +
      "beforeParentRead=true")
    previous.partitions.iterator.flatMap(partition => previous.iterator(partition, context))
  }
}

object RecoverySpike {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Codec genuine shuffle recovery").getOrCreate()
    Seq(getClass, classOf[RemoveCachedParent]).foreach { clazz =>
      val location = clazz.getProtectionDomain.getCodeSource.getLocation.toString
      require(location.endsWith("/recovery-spike.jar"), s"stale recovery class: $location")
      println(s"CODEC_RECOVERY CLASS_SOURCE class=${clazz.getName} location=$location")
    }
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    val failures = new AtomicInteger()
    sc.addSparkListener(new SparkListener {
      override def onTaskEnd(event: SparkListenerTaskEnd): Unit =
        if (event.reason.isInstanceOf[FetchFailed]) failures.incrementAndGet()
    })
    val driverEnvironment = SparkEnv.get
    // Fixture-only fault injection; all recovery remains the existing Spark path.
    driverEnvironment.rpcEnv.setupEndpoint("CodecRecoveryFaultGate", new RpcEndpoint {
      override val rpcEnv: RpcEnv = driverEnvironment.rpcEnv
      override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
        case ownerId: Int =>
          driverEnvironment.blockManager.master.removeRdd(ownerId, blocking = true)
          require((0 until 4).forall(index => driverEnvironment.blockManager.master
            .getLocations(RDDBlockId(ownerId, index)).isEmpty))
          println(s"CODEC_RECOVERY DRIVER_CACHE_REMOVAL owner=$ownerId blocks=4")
          context.reply(true)
      }
    })
    val frame = spark.range(10000).toDF("id").repartition(4).persist(StorageLevel.MEMORY_ONLY_SER)
    require(frame.collect().map(_.getLong(0)).sorted.toVector == (0L until 10000).toVector)
    val owner = spark.sharedState.cacheManager.lookupCachedData(frame).get
      .cachedRepresentation.cacheBuilder.cachedColumnBuffers
    val shuffles = scala.collection.mutable.Set[Int]()
    val visited = scala.collection.mutable.Set[Int]()
    def visit(rdd: RDD[_]): Unit = if (visited.add(rdd.id)) {
      rdd.dependencies.foreach {
        case shuffle: ShuffleDependency[_, _, _] =>
          shuffles += shuffle.shuffleId
          visit(shuffle.rdd)
        case dependency => visit(dependency.rdd)
      }
    }
    visit(owner)
    require(shuffles.nonEmpty)
    val tracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    shuffles.foreach { id =>
      require(tracker.getNumAvailableOutputs(id) > 0)
      val walk = java.nio.file.Files.walk(
        java.nio.file.Paths.get(sc.getConf.get("spark.local.dir")))
      var deleted = 0
      try {
        val files = walk.iterator()
        while (files.hasNext) {
          val path = files.next()
          val name = path.getFileName.toString
          if (name.startsWith(s"shuffle_${id}_") &&
              (name.endsWith(".data") || name.endsWith(".index"))) {
            java.nio.file.Files.delete(path)
            deleted += 1
          }
        }
      } finally walk.close()
      require(deleted > 0, "test did not delete physical shuffle files")
      println(s"CODEC_RECOVERY SHUFFLE_REMOVED id=$id files=$deleted")
      tracker.unregisterAllMapAndMergeOutput(id)
      require(tracker.getNumAvailableOutputs(id) == 0)
    }
    require(owner.partitions.forall(partition => SparkEnv.get.blockManager.master
      .getLocations(RDDBlockId(owner.id, partition.index)).nonEmpty))
    println("CODEC_RECOVERY INITIAL_CACHE_LOCATIONS_PRESENT blocks=4")
    val child = new RemoveCachedParent(owner)
    val values = child.mapPartitions { batches =>
      batches.flatMap(_.asInstanceOf[BulkBatch].values.iterator)
    }.collect().sorted
    require(values.toVector == (0L until 10000).toVector)
    require(failures.get() > 0, "test did not exercise genuine fetch-failure recovery")
    shuffles.foreach(id => require(tracker.getNumAvailableOutputs(id) > 0))
    println(s"CODEC_RECOVERY GENUINE_RECOVERY_PASS fetchFailed=${failures.get()} rows=10000")
    val sample = RDDBlockId(owner.id, 0)
    val manager = SparkEnv.get.blockManager
    val beforeMigration = manager.getRemoteValues[CachedBatch](sample)(owner.elementClassTag)
      .get.data.toVector.flatMap(_.asInstanceOf[BulkBatch].values).sorted
    val original = manager.master.getLocations(sample).head
    manager.master.getExecutorEndpointRef(original.executorId).get.askSync[Unit](
      org.apache.spark.storage.BlockManagerMessages.DecommissionBlockManager)
    val deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(30)
    var locations = manager.master.getLocations(sample)
    while ((locations.isEmpty || locations.exists(_.executorId == original.executorId)) &&
        System.nanoTime() < deadline) {
      Thread.sleep(100)
      locations = manager.master.getLocations(sample)
    }
    require(locations.nonEmpty && !locations.exists(_.executorId == original.executorId),
      s"decommission did not migrate sample: $locations")
    val data = manager.getRemoteValues[CachedBatch](sample)(owner.elementClassTag).get
      .data.toVector.map(_.asInstanceOf[BulkBatch])
    require(data.map(_.numRows).sum == 2500)
    require(data.flatMap(_.values).sorted == beforeMigration)
    require(frame.collect().map(_.getLong(0)).sorted.toVector == (0L until 10000).toVector)
    println(s"CODEC_RECOVERY DECOMMISSION_PASS old=${original.executorId} " +
      s"new=${locations.map(_.executorId).mkString(",")} rows=10000")
    frame.unpersist(blocking = true)
    spark.stop()
  }
}
