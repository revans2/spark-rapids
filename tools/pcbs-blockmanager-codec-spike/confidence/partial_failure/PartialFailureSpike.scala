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

import java.io.{InputStream, OutputStream}
import java.nio.ByteBuffer
import java.util.UUID

import scala.collection.mutable
import scala.reflect.ClassTag

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEnv}
import org.apache.spark.serializer._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.storage.{RDDBlockId, StorageLevel}
import org.apache.spark.util.RpcUtils

case class OpenProbe(id: String, direction: String, executor: String, task: Long)
case class ProgressProbe(id: String, bytes: Long)
case class CloseProbe(id: String)
case class ShouldFailProbe(records: Int)

object ProbeTransport {
  def send(message: Any): Boolean = {
    val env = SparkEnv.get
    val endpoint = RpcUtils.makeDriverRef("CodecPartialFailureProbe", env.conf, env.rpcEnv)
    require(endpoint.address != null)
    endpoint.askSync[Boolean](message)
  }
  def open(direction: String): String = {
    val id = UUID.randomUUID().toString
    send(OpenProbe(id, direction, SparkEnv.get.executorId,
      Option(TaskContext.get()).map(_.taskAttemptId()).getOrElse(-1L)))
    id
  }
}

/** Observation only: deliberately adds no cleanup listeners or exception handling. */
final class InstrumentedCodec(val columns: Int) extends Serializer with Serializable {
  override def newInstance(): SerializerInstance = new SerializerInstance {
    private val underlying = new LongCacheCodec(columns).newInstance()
    override def serialize[T: ClassTag](value: T): ByteBuffer = underlying.serialize(value)
    override def deserialize[T: ClassTag](bytes: ByteBuffer): T = underlying.deserialize(bytes)
    override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T =
      underlying.deserialize(bytes, loader)
    override def serializeStream(out: OutputStream): SerializationStream = {
      val inner = underlying.serializeStream(out)
      val id = ProbeTransport.open("write")
      new SerializationStream {
        override def writeObject[T: ClassTag](value: T): SerializationStream = {
          inner.writeObject(value)
          ProbeTransport.send(ProgressProbe(id, value.asInstanceOf[BulkBatch].sizeInBytes))
          this
        }
        override def flush(): Unit = inner.flush()
        override def close(): Unit = { inner.close(); ProbeTransport.send(CloseProbe(id)) }
      }
    }
    override def deserializeStream(in: InputStream): DeserializationStream = {
      val inner = underlying.deserializeStream(in)
      val id = ProbeTransport.open("read")
      new DeserializationStream {
        private var records = 0
        override def readObject[T: ClassTag](): T = {
          if (records == 1 && ProbeTransport.send(ShouldFailProbe(records))) {
            throw new IllegalStateException("INJECTED_CODEC_DECODE_FAILURE")
          }
          val value = inner.readObject[T]()
          records += 1
          ProbeTransport.send(ProgressProbe(id, value.asInstanceOf[BulkBatch].sizeInBytes))
          value
        }
        override def close(): Unit = { inner.close(); ProbeTransport.send(CloseProbe(id)) }
      }
    }
  }
}

class PartialFailureCachedBatchSerializer extends ResearchCachedBatchSerializer {
  override def cacheCodec(schema: Seq[Attribute]): Serializer = new InstrumentedCodec(schema.length)
}

object PartialFailureSpike {
  final class State(val direction: String, val executor: String, val task: Long) {
    var records = 0
    var bytes = 0L
    var closed = false
  }

  def main(args: Array[String]): Unit = {
    val mode = args.head
    require(Set("partial", "failure").contains(mode))
    val spark = SparkSession.builder().appName("SQL codec partial/failure ownership probe")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    val env = SparkEnv.get
    val states = mutable.HashMap.empty[String, State]
    var armed = false
    var injected = 0
    env.rpcEnv.setupEndpoint("CodecPartialFailureProbe", new RpcEndpoint {
      override val rpcEnv: RpcEnv = env.rpcEnv
      override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
        case OpenProbe(id, direction, executor, task) =>
          states.synchronized { states(id) = new State(direction, executor, task) }
          context.reply(true)
        case ProgressProbe(id, bytes) =>
          states.synchronized { states(id).records += 1; states(id).bytes += bytes }
          context.reply(true)
        case CloseProbe(id) =>
          states.synchronized { states(id).closed = true }
          context.reply(true)
        case ShouldFailProbe(records) =>
          val result = synchronized {
            if (armed && records == 1) { armed = false; injected += 1; true } else false
          }
          context.reply(result)
      }
    })
    val frame = spark.range(1000, 2000, 1, 2).toDF("value").persist(StorageLevel.DISK_ONLY)
    if (mode == "partial") {
      require(frame.take(1).map(_.getLong(0)).toSeq == Seq(1000L))
      println("PARTIAL_FAILURE TAKE_ONE_PASS")
    } else {
      require(frame.collect().map(_.getLong(0)).sorted.toVector == (1000L until 2000L).toVector)
      armed = true
    }
    require(frame.collect().map(_.getLong(0)).sorted.toVector == (1000L until 2000L).toVector)
    val relation = spark.sharedState.cacheManager.lookupCachedData(frame).get.cachedRepresentation
    val owner = relation.cacheBuilder.cachedColumnBuffers
    require(owner.elementClassTag.isInstanceOf[CacheCodecClassTag[_]])
    require(relation.cacheBuilder.rowCountStats.value == 1000)
    require(relation.cacheBuilder.sizeInBytesStats.value == 8000)
    val manager = env.blockManager
    (0 until 2).foreach { index =>
      val id = RDDBlockId(owner.id, index)
      val status = manager.master.getLocationsAndStatus(id, manager.blockManagerId.host).get
      require(status.locations.nonEmpty && status.status.diskSize == 4032)
      val records = manager.getRemoteValues[CachedBatch](id)(owner.elementClassTag).get
        .data.toVector.map(_.asInstanceOf[BulkBatch])
      val first = 1000L + index * 500
      require(records.size == 4 && records.map(_.numRows).sum == 500)
      require(records.flatMap(_.values) == (first until first + 500).toVector)
      println(s"PARTIAL_FAILURE COMPLETE_BLOCK partition=$index records=4 rows=500 diskBytes=4032")
    }
    require(injected == (if (mode == "failure") 1 else 0))
    println(s"PARTIAL_FAILURE EXACT_LATER_READ_PASS mode=$mode injected=$injected")
    val unclosed = states.synchronized {
      states.toVector.sortBy(_._1).map { case (id, state) =>
        println(s"PARTIAL_FAILURE STREAM id=$id direction=${state.direction} " +
          s"executor=${state.executor} task=${state.task} records=${state.records} " +
          s"bytes=${state.bytes} closed=${state.closed}")
        state
      }.count(!_.closed)
    }
    println(s"PARTIAL_FAILURE OWNERSHIP mode=$mode unclosed=$unclosed")
    frame.unpersist(blocking = true)
    spark.stop()
    require(unclosed == 0, "codec streams remained unclosed after completed tasks")
    println("PARTIAL_FAILURE ALL_CHECKS_PASS")
  }
}
