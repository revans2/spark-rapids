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
package org.apache.spark.sql.cleanreceiverspike

import java.io.NotSerializableException
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.spark.{SparkContext, SparkEnv, TaskContext}
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.codeccachespike.BulkBatch
import org.apache.spark.sql.types._
import org.apache.spark.storage.{RDDBlockId, StorageLevel}

case class Register(executor: String, endpoint: RpcEndpointRef) extends Serializable
case class Inspect(block: RDDBlockId) extends Serializable
case class Inspection(executor: String, present: Boolean, rows: Int, values: Int)
    extends Serializable

object Registry {
  val addresses = new ConcurrentHashMap[String, RpcEndpointRef]()
}

/** Test-control endpoint only: reads local replica without reserializing its payload. */
class InspectionPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = new DriverPlugin {
    override def receive(message: AnyRef): AnyRef = message match {
      case Register(executor, endpoint) =>
        // Use only on this original receiving driver; never forward client refs to peers.
        Registry.addresses.put(executor, endpoint)
        java.lang.Boolean.TRUE
    }
  }
  override def executorPlugin(): ExecutorPlugin = new ExecutorPlugin {
    override def init(context: PluginContext, extra: java.util.Map[String, String]): Unit = {
      val env = SparkEnv.get
      val inspection = env.rpcEnv.setupEndpoint("CleanReplicaInspection", new RpcEndpoint {
        override val rpcEnv: RpcEnv = env.rpcEnv
        override def receiveAndReply(call: RpcCallContext): PartialFunction[Any, Unit] = {
          case Inspect(block) =>
            require(TaskContext.get() == null)
            val local = env.blockManager.getLocalValues(block)
            val answer = local match {
              case None => Inspection(env.executorId, false, 0, 0)
              case Some(result) =>
                var row = 0
                var values = 0
                result.data.foreach { raw =>
                  val batch = raw.asInstanceOf[BulkBatch]
                  require(batch.columns == 2)
                  batch.values.grouped(2).foreach { pair =>
                    val expected = 10000L + row
                    require(pair.toSeq == Seq(expected, expected * 3))
                    row += 1
                    values += 2
                  }
                }
                require(row == 1000 && values == 2000)
                Inspection(env.executorId, true, row, values)
            }
            println(s"CLEAN_RECEIVER INSPECT executor=${answer.executor} " +
              s"present=${answer.present} rows=${answer.rows} values=${answer.values} " +
              "taskContext=false")
            call.reply(answer)
        }
      })
      require(context.ask(Register(env.executorId, inspection)) == java.lang.Boolean.TRUE)
      println(s"CLEAN_RECEIVER REGISTER executor=${env.executorId}")
    }
  }
}

object CleanReceiverSpike {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Clean replica receiver codec confidence").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    val deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(30)
    while (Registry.addresses.size() < 3 && System.nanoTime() < deadline) Thread.sleep(100)
    require(Registry.addresses.size() == 3, s"expected 3 fresh executor JVMs: ${Registry.addresses}")
    val producers = sc.collectionAccumulator[String]("clean-cache-producers")
    val rows = sc.parallelize(10000L until 11000L, 1).mapPartitions { input =>
      val executor = SparkEnv.get.executorId
      producers.add(executor)
      println(s"CLEAN_RECEIVER PRODUCE executor=$executor")
      input.map(value => Row(value, value * 3))
    }
    val schema = StructType(Seq(StructField("a", LongType), StructField("b", LongType)))
    val frame = spark.createDataFrame(rows, schema).persist(StorageLevel.MEMORY_AND_DISK_2)
    def check(): Unit = {
      val actual = frame.collect().map(row => (row.getLong(0), row.getLong(1))).toVector
      require(actual == (10000L until 11000L).map(v => (v, v * 3)).toVector)
    }
    check()
    require(producers.value.asScala.size == 1)
    val producer = producers.value.asScala.head
    require(producer != "driver")
    val owner = spark.sharedState.cacheManager.lookupCachedData(frame).get
      .cachedRepresentation.cacheBuilder.cachedColumnBuffers
    require(owner.partitions.length == 1)
    val block = RDDBlockId(owner.id, 0)
    val env = SparkEnv.get
    val locations = env.blockManager.master.getLocations(block)
    require(locations.size == 2 && locations.map(_.executorId).distinct.size == 2)
    val holders = locations.map(_.executorId).toSet
    require(holders.contains(producer))
    val receiver = (holders - producer).head
    require(Registry.addresses.containsKey(receiver))
    val inspections = Registry.addresses.asScala.toVector.sortBy(_._1).map {
      case (executor, endpoint) =>
        val answer = endpoint.askSync[Inspection](Inspect(block))
        require(answer.executor == executor)
        require(answer.present == holders.contains(executor))
        answer
    }
    require(inspections.count(_.present) == 2)
    require(inspections.filter(_.present).forall(x => x.rows == 1000 && x.values == 2000))
    check()
    require(producers.value.asScala.toVector == Vector(producer), "hidden upstream recomputation")
    require(owner.getStorageLevel == StorageLevel.MEMORY_AND_DISK_2)
    val builder = spark.sharedState.cacheManager.lookupCachedData(frame).get
      .cachedRepresentation.cacheBuilder
    require(builder.rowCountStats.value == 1000 && builder.sizeInBytesStats.value == 16000)

    val forbidden = new BulkBatch(2, Array(1L, 3L))
    try {
      new JavaSerializer(sc.getConf).newInstance().serialize(forbidden)
      throw new IllegalStateException("Java tripwire failed")
    } catch {
      case e: NotSerializableException => require(e.getMessage.contains("GENERIC_JAVA_BULK"))
    }
    try {
      new KryoSerializer(sc.getConf).newInstance().serialize(forbidden)
      throw new IllegalStateException("Kryo tripwire failed")
    } catch {
      case e: Exception =>
        require(Iterator.iterate[Throwable](e)(_.getCause).takeWhile(_ != null)
          .exists(x => Option(x.getMessage).exists(_.contains("GENERIC_KRYO_BULK"))))
    }
    frame.unpersist(blocking = true)
    require(env.blockManager.master.getLocations(block).isEmpty)
    println(s"CLEAN_RECEIVER RESULT producer=$producer receiver=$receiver " +
      s"registered=${Registry.addresses.size} replicas=${holders.toVector.sorted.mkString(",")} " +
      "partitions=1 rows=1000 values=2000 upstreamComputes=1")
    println("CLEAN_RECEIVER TRIPWIRES_PASS")
    println("CLEAN_RECEIVER PASS")
    spark.stop()
  }
}
