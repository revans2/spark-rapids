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
package org.apache.spark.sql.distributedunpersistspike

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicReference, AtomicBoolean}

import scala.collection.JavaConverters._

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEnv}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cleanreceiverspike.{Inspect, Inspection, Registry}
import org.apache.spark.sql.codeccachespike.BulkBatch
import org.apache.spark.storage.{RDDBlockId, StorageLevel}
import org.apache.spark.util.RpcUtils

case class ReaderReady(executor: String, block: String) extends Serializable
case object ReleaseReader extends Serializable

object DistributedUnpersistSpike {
  def mark(value: String): Unit = println("DISTRIBUTED_UNPERSIST " + value)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Distributed blocking SQL unpersist proof").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    val ready = new CountDownLatch(1)
    val released = new AtomicBoolean(false)
    val readerExecutor = new AtomicReference[String]()
    val env = SparkEnv.get
    val gate = env.rpcEnv.setupEndpoint("DistributedUnpersistDriverGate", new RpcEndpoint {
      override val rpcEnv: RpcEnv = env.rpcEnv
      private var waiting: Option[RpcCallContext] = None
      override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
        case ReaderReady(executor, block) =>
          require(waiting.isEmpty && !released.get())
          waiting = Some(context)
          readerExecutor.set(executor)
          mark(s"READER_HELD executor=$executor block=$block")
          ready.countDown()
        case ReleaseReader =>
          require(waiting.nonEmpty && !released.get())
          released.set(true)
          mark("READER_RELEASE")
          waiting.get.reply(true)
          waiting = None
          context.reply(true)
      }
    })
    val registeredDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30)
    while (Registry.addresses.size() < 2 && System.nanoTime() < registeredDeadline) Thread.sleep(100)
    require(Registry.addresses.size() == 2)

    val frame = spark.range(10000, 11000, 1, 1).selectExpr("id as a", "id * 3 as b")
      .persist(StorageLevel.MEMORY_AND_DISK_2)
    require(frame.collect().map(row => (row.getLong(0), row.getLong(1))).toVector ==
      (10000L until 11000L).map(v => (v, v * 3)).toVector)
    val owner = spark.sharedState.cacheManager.lookupCachedData(frame).get
      .cachedRepresentation.cacheBuilder.cachedColumnBuffers
    require(owner.partitions.length == 1)
    val ownerId = owner.id
    val block = RDDBlockId(ownerId, 0)
    val locations = env.blockManager.master.getLocations(block).map(_.executorId).toSet
    require(locations.size == 2 && locations == Registry.addresses.keySet().asScala.toSet)
    mark(s"POPULATED owner=$ownerId replicas=${locations.toVector.sorted.mkString(",")} rows=1000")
    val errors = new AtomicReference[Throwable]()
    val readerResult = new AtomicReference[Vector[(Long, Long)]]()
    val readerDone = new CountDownLatch(1)
    val reader = new Thread(() => {
      try {
        val values = owner.mapPartitions { batches =>
          require(TaskContext.get() != null)
          val worker = SparkEnv.get
          val localBlock = RDDBlockId(ownerId, 0)
          require(worker.blockManager.memoryStore.contains(localBlock))
          val first = batches.next().asInstanceOf[BulkBatch]
          val driverGate = RpcUtils.makeDriverRef(
            "DistributedUnpersistDriverGate", worker.conf, worker.rpcEnv)
          require(driverGate.address != null, "reader gate must be addressed")
          require(driverGate.askSync[Boolean](ReaderReady(worker.executorId, localBlock.name)))
          println(s"DISTRIBUTED_UNPERSIST READER_RESUMED executor=${worker.executorId}")
          (Iterator(first) ++ batches.map(_.asInstanceOf[BulkBatch])).flatMap { batch =>
            require(batch.columns == 2)
            batch.values.grouped(2).map(pair => (pair(0), pair(1)))
          }
        }.collect().toVector
        readerResult.set(values)
        mark("READER_FINISHED rows=1000")
      } catch { case t: Throwable => errors.set(t) }
      finally { readerDone.countDown() }
    }, "held-sql-cache-reader")
    reader.start()
    require(ready.await(30, TimeUnit.SECONDS))
    require(locations.contains(readerExecutor.get()))
    require(errors.get() == null)

    val unpersistDone = new CountDownLatch(1)
    val unpersist = new Thread(() => {
      try {
        mark("SQL_UNPERSIST_CALL blocking=true")
        frame.unpersist(blocking = true)
        mark("SQL_UNPERSIST_RETURN")
      } catch { case t: Throwable => errors.set(t) }
      finally { unpersistDone.countDown() }
    }, "blocking-sql-unpersist")
    unpersist.start()
    val entryDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30)
    var entered = false
    while (!entered && unpersistDone.getCount > 0 && System.nanoTime() < entryDeadline) {
      val stack = unpersist.getStackTrace.toVector
      entered = stack.exists(x => x.getClassName == "org.apache.spark.sql.Dataset" &&
        x.getMethodName == "unpersist") &&
        stack.exists(x => x.getClassName == "org.apache.spark.storage.BlockManagerMaster" &&
          x.getMethodName == "removeRdd")
      if (!entered) Thread.sleep(10)
    }
    require(entered, "did not observe actual SQL unpersist waiting in BlockManagerMaster.removeRdd")
    require(!unpersistDone.await(250, TimeUnit.MILLISECONDS),
      "blocking SQL unpersist completed with reader held")
    require(!released.get() && readerDone.getCount == 1)
    mark("SQL_UNPERSIST_BLOCKED actualDatasetAndMasterStack=true readerHeld=true")
    require(gate.askSync[Boolean](ReleaseReader))
    require(readerDone.await(30, TimeUnit.SECONDS), "reader did not complete")
    require(unpersistDone.await(30, TimeUnit.SECONDS), "SQL unpersist did not complete")
    Option(errors.get()).foreach(throw _)
    require(readerResult.get() == (10000L until 11000L).map(v => (v, v * 3)).toVector)
    require(env.blockManager.master.getLocations(block).isEmpty)
    require(spark.sharedState.cacheManager.lookupCachedData(frame).isEmpty)
    Registry.addresses.asScala.toVector.foreach { case (executor, endpoint) =>
      val absent = endpoint.askSync[Inspection](Inspect(block))
      require(absent.executor == executor && !absent.present && absent.rows == 0 && absent.values == 0)
      mark(s"EXECUTOR_ABSENT executor=$executor block=${block.name}")
    }
    require(owner.getStorageLevel == StorageLevel.NONE)
    mark(s"PASS owner=$ownerId reader=${readerExecutor.get()} replicasRemoved=2 rows=1000")
    spark.stop()
  }
}
