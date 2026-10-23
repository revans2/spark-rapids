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
package org.apache.spark.sql.codeccleanupspike

import org.apache.spark.sql.codeccachespike.{BulkBatch, LongCacheCodec, ResearchCachedBatchSerializer}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, EOFException, InputStream, OutputStream}
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
case class CloseProbe(id: String, reason: String, actual: Boolean)
case class ShouldFailProbe(records: Int)

object ProbeTransport {
  def send(message: Any): Boolean = {
    val env = SparkEnv.get
    val endpoint = RpcUtils.makeDriverRef("CodecCleanupProbe", env.conf, env.rpcEnv)
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

/** Research decoder ownership: task completion, EOF and errors share one close guard. */
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
        override def close(): Unit = { inner.close(); ProbeTransport.send(CloseProbe(id, "write_close", true)) }
      }
    }
    override def deserializeStream(in: InputStream): DeserializationStream = {
      val inner = underlying.deserializeStream(in)
      val id = ProbeTransport.open("read")
      final class OwnedReadStream extends DeserializationStream {
        private var records = 0
        private var attempted = false
        def closeOnce(reason: String): Unit = synchronized {
          val actual = !attempted
          if (actual) {
            attempted = true
            inner.close()
          }
          // The notification follows successful underlying close, never precedes it.
          ProbeTransport.send(CloseProbe(id, reason, actual))
        }
        override def readObject[T: ClassTag](): T = {
          try {
            if (records == 1 && ProbeTransport.send(ShouldFailProbe(records))) {
              throw new IllegalStateException("INJECTED_CODEC_DECODE_FAILURE")
            }
            val value = inner.readObject[T]()
            records += 1
            ProbeTransport.send(ProgressProbe(id, value.asInstanceOf[BulkBatch].sizeInBytes))
            value
          } catch {
            case failure: Throwable =>
              try closeOnce(if (failure.isInstanceOf[EOFException]) "eof" else "decode_failure")
              catch { case cleanup: Throwable =>
                if (cleanup ne failure) failure.addSuppressed(cleanup)
              }
              throw failure
          }
        }
        override def close(): Unit = closeOnce("caller_or_iterator")
      }
      val stream = new OwnedReadStream
      Option(TaskContext.get()).foreach { context =>
        context.addTaskCompletionListener[Unit] { _ => stream.closeOnce("task_completion") }
      }
      stream
    }
  }
}

class CleanupCachedBatchSerializer extends ResearchCachedBatchSerializer {
  override def cacheCodec(schema: Seq[Attribute]): Serializer = new InstrumentedCodec(schema.length)
}

object CleanupSpike {
  final class State(val direction: String, val executor: String, val task: Long) {
    var records = 0
    var bytes = 0L
    var closed = false
    var actualCloses = 0
    var closeRequests = 0
    var taskCompletions = 0
    var firstClose = "unset"
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
    @volatile var armed = false
    @volatile var injected = 0
    env.rpcEnv.setupEndpoint("CodecCleanupProbe", new RpcEndpoint {
      override val rpcEnv: RpcEnv = env.rpcEnv
      override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
        case OpenProbe(id, direction, executor, task) =>
          states.synchronized { states(id) = new State(direction, executor, task) }
          context.reply(true)
        case ProgressProbe(id, bytes) =>
          states.synchronized { states(id).records += 1; states(id).bytes += bytes }
          context.reply(true)
        case CloseProbe(id, reason, actual) =>
          states.synchronized {
            val state = states(id)
            state.closeRequests += 1
            if (reason == "task_completion") state.taskCompletions += 1
            if (actual) {
              state.closed = true
              state.actualCloses += 1
              state.firstClose = reason
            }
          }
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
      println("CLEANUP TAKE_ONE_PASS")
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
      println(s"CLEANUP COMPLETE_BLOCK partition=$index records=4 rows=500 diskBytes=4032")
    }
    require(TaskContext.get() == null)
    val directCodec = new InstrumentedCodec(1).newInstance()
    val output = new ByteArrayOutputStream()
    val writer = directCodec.serializeStream(output)
    writer.writeObject(new BulkBatch(1, Array(42L)))
    writer.writeObject(new BulkBatch(1, Array(43L)))
    writer.close()
    val caller = directCodec.deserializeStream(new ByteArrayInputStream(output.toByteArray))
    require(caller.readObject[BulkBatch]().values.toVector == Vector(42L))
    caller.close()
    caller.close()
    println("CLEANUP TASKLESS_CALLER_CLOSE_PASS")
    val corrupt = directCodec.deserializeStream(new ByteArrayInputStream(Array.fill[Byte](16)(0)))
    try {
      corrupt.readObject[BulkBatch]()
      throw new IllegalStateException("malformed codec framing unexpectedly accepted")
    } catch {
      case expected: IllegalArgumentException =>
        require(expected.getMessage.contains("wrong per-cache codec context"))
    }
    corrupt.close()
    println("CLEANUP TASKLESS_ERROR_CLOSE_PASS")
    require(injected == (if (mode == "failure") 1 else 0))
    println(s"CLEANUP EXACT_LATER_READ_PASS mode=$mode injected=$injected")
    val unclosed = states.synchronized {
      states.toVector.sortBy(_._1).map { case (id, state) =>
        println(s"CLEANUP STREAM id=$id direction=${state.direction} " +
          s"executor=${state.executor} task=${state.task} records=${state.records} " +
          s"bytes=${state.bytes} closed=${state.closed} actualCloses=${state.actualCloses} " +
          s"closeRequests=${state.closeRequests} taskCompletions=${state.taskCompletions} firstClose=${state.firstClose}")
        state
      }.count(!_.closed)
    }
    val snapshot = states.synchronized { states.values.toVector }
    require(snapshot.forall(state => state.closed && state.actualCloses == 1))
    val taskReads = snapshot.filter(state => state.direction == "read" && state.task >= 0)
    require(taskReads.nonEmpty && taskReads.forall(_.taskCompletions == 1))
    require(taskReads.exists(state => state.records == 4 && state.closeRequests >= 2 &&
      state.firstClose == "eof"), "normal EOF and listener did not share the close guard")
    if (mode == "partial") require(taskReads.exists(state => state.records == 1 &&
      state.firstClose == "task_completion"), "take(1) did not test task-completion cleanup")
    if (mode == "failure") require(taskReads.exists(state => state.records == 1 &&
      state.firstClose == "decode_failure"), "decode error did not close immediately")
    val taskless = snapshot.filter(state => state.direction == "read" && state.task < 0)
    require(taskless.exists(state => state.records == 1 && state.firstClose == "caller_or_iterator" &&
      state.closeRequests == 2), "missing idempotent taskless caller close")
    require(taskless.exists(state => state.records == 0 && state.firstClose == "decode_failure" &&
      state.closeRequests == 2), "missing taskless exception cleanup")
    println("CLEANUP CLOSE_ONCE_AND_LISTENER_PASS")
    println(s"CLEANUP OWNERSHIP mode=$mode unclosed=$unclosed")
    frame.unpersist(blocking = true)
    spark.stop()
    require(unclosed == 0, "codec streams remained unclosed after completed tasks")
    println("CLEANUP ALL_CHECKS_PASS")
  }
}
