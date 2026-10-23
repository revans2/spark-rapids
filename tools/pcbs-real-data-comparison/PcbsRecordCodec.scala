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
package org.apache.spark.sql.pcbsrealdata

import java.io.{DataInputStream, DataOutputStream, EOFException, IOException, InputStream, OutputStream}
import java.nio.ByteBuffer
import java.util.concurrent.atomic.{AtomicLong, LongAdder}

import scala.reflect.ClassTag

import com.nvidia.spark.rapids.ShimLoader

import org.apache.spark.TaskContext
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, Serializer, SerializerInstance}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.execution.columnar.SqlCacheCodecProvider

/** Research counters are process-local, cumulative attempts; local-mode fixtures only. */
object PcbsCodecCounters {
  @volatile private var consumerTarget = 1024L * 1024 * 1024
  def consumerTargetBytes: Long = consumerTarget
  def setConsumerTargetBytes(bytes: Long): Unit = {
    require(Set(0L, 256L * 1024 * 1024, 1024L * 1024 * 1024).contains(bytes))
    consumerTarget = bytes
  }
  private val producedBatches = new LongAdder
  private val producedRows = new LongAdder
  private val producedBytes = new LongAdder
  private val producedMaxBytes = new AtomicLong
  private val producedMinBytes = new AtomicLong(Long.MaxValue)
  def recordProduced(rows: Long, bytes: Long): Unit = {
    producedBatches.increment()
    producedRows.add(rows)
    producedBytes.add(bytes)
    maximum(producedMaxBytes, bytes)
    var previous = producedMinBytes.get()
    while (bytes < previous && !producedMinBytes.compareAndSet(previous, bytes)) {
      previous = producedMinBytes.get()
    }
  }
  private val writtenRecords = new LongAdder
  private val writtenBytes = new LongAdder
  private val readRecords = new LongAdder
  private val readBytes = new LongAdder
  private val decodedBatches = new LongAdder
  private val decodedRows = new LongAdder
  private val decodedBytes = new LongAdder
  private val decodedMaxBytes = new AtomicLong
  private val combinedBatches = new LongAdder
  private val combinedRows = new LongAdder
  private val combinedBytes = new LongAdder
  private val combinedMaxBytes = new AtomicLong
  private val combinedMaxRows = new AtomicLong
  private val combinedMinBytes = new AtomicLong(Long.MaxValue)

  private def maximum(counter: AtomicLong, value: Long): Unit = {
    var previous = counter.get()
    while (value > previous && !counter.compareAndSet(previous, value)) {
      previous = counter.get()
    }
  }
  def resetReadExtrema(): Unit = {
    decodedMaxBytes.set(0L)
    combinedMaxBytes.set(0L)
    combinedMaxRows.set(0L)
    combinedMinBytes.set(Long.MaxValue)
  }

  def recordCombined(rows: Long, bytes: Long): Unit = {
    combinedBatches.increment()
    combinedRows.add(rows)
    combinedBytes.add(bytes)
    maximum(combinedMaxBytes, bytes)
    maximum(combinedMaxRows, rows)
    var previous = combinedMinBytes.get()
    while (bytes < previous && !combinedMinBytes.compareAndSet(previous, bytes)) {
      previous = combinedMinBytes.get()
    }
  }

  def written(bytes: Long): Unit = { writtenRecords.increment(); writtenBytes.add(bytes) }
  def read(bytes: Long): Unit = { readRecords.increment(); readBytes.add(bytes) }
  def recordDecoded(rows: Long, bytes: Long): Unit = {
    decodedBatches.increment()
    decodedRows.add(rows)
    decodedBytes.add(bytes)
    var previous = decodedMaxBytes.get()
    while (bytes > previous && !decodedMaxBytes.compareAndSet(previous, bytes)) {
      previous = decodedMaxBytes.get()
    }
  }
  def snapshotJson(): String = {
    val fields = Seq(
      "gpu_produced_batches" -> producedBatches.sum(),
      "gpu_produced_rows" -> producedRows.sum(),
      "gpu_produced_bytes" -> producedBytes.sum(),
      "gpu_produced_max_bytes" -> producedMaxBytes.get(),
      "gpu_produced_min_bytes" -> (if (producedBatches.sum() == 0L) 0L else producedMinBytes.get()),
      "codec_write_records" -> writtenRecords.sum(),
      "codec_write_bytes" -> writtenBytes.sum(),
      "codec_read_records" -> readRecords.sum(),
      "codec_read_bytes" -> readBytes.sum(),
      "gpu_decoded_batches" -> decodedBatches.sum(),
      "gpu_decoded_rows" -> decodedRows.sum(),
      "gpu_decoded_bytes" -> decodedBytes.sum(),
      "gpu_decoded_max_bytes" -> decodedMaxBytes.get(),
      "gpu_combined_batches" -> combinedBatches.sum(),
      "gpu_combined_rows" -> combinedRows.sum(),
      "gpu_combined_bytes" -> combinedBytes.sum(),
      "gpu_combined_max_rows" -> combinedMaxRows.get(),
      "gpu_combined_max_bytes" -> combinedMaxBytes.get(),
      "gpu_combined_min_bytes" -> (if (combinedMinBytes.get() == Long.MaxValue) {
        0L
      } else combinedMinBytes.get()))
    fields.map { case (key, value) => "\"" + key + "\":" + value }
      .mkString("{", ",", "}")
  }
}

/** No compile-time dependency on plugin-internal classes is required by the harness. */
class PcbsCodecSnapshot {
  def snapshotJson(): String = PcbsCodecCounters.snapshotJson()
  def setConsumerTargetBytes(bytes: Long): Unit = PcbsCodecCounters.setConsumerTargetBytes(bytes)
  def resetReadExtrema(): Unit = PcbsCodecCounters.resetReadExtrema()
}

/** Resolve the actual shim-loaded record, never a duplicate class from a fixture jar. */
private object RecordAccess {
  private lazy val recordClass = ShimLoader.getShimClassLoader().loadClass(
    "com.nvidia.spark.rapids.parquet.ParquetCachedBatch")
  private lazy val bufferAccessor = recordClass.getMethod("buffer")
  private lazy val constructor = recordClass.getConstructor(
    java.lang.Integer.TYPE, classOf[Array[Byte]])

  def buffer(value: Any): Array[Byte] = {
    require(recordClass.isInstance(value), "Unexpected cached record class: " + value.getClass)
    bufferAccessor.invoke(value).asInstanceOf[Array[Byte]]
  }
  def create(rows: Int, bytes: Array[Byte]): CachedBatch =
    constructor.newInstance(Int.box(rows), bytes).asInstanceOf[CachedBatch]
}

/**
 * Research wire format: magic, rows, payload length, existing Parquet payload.
 * There is no ObjectOutputStream, array clone, native store or separate payload ownership.
 */
final class PcbsRecordCodec extends Serializer with Serializable {
  override def newInstance(): SerializerInstance = new SerializerInstance {
    override def serialize[T: ClassTag](value: T): ByteBuffer =
      throw new UnsupportedOperationException("Stream-only PCBS research codec")
    override def deserialize[T: ClassTag](bytes: ByteBuffer): T =
      throw new UnsupportedOperationException("Stream-only PCBS research codec")
    override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T =
      throw new UnsupportedOperationException("Stream-only PCBS research codec")

    override def serializeStream(out: OutputStream): SerializationStream = {
      val stream = new SerializationStream {
        private val data = new DataOutputStream(out)
        private var closed = false
        override def writeObject[T: ClassTag](value: T): SerializationStream = {
          try {
            require(!closed, "Write after PCBS codec stream close")
            val batch = value.asInstanceOf[CachedBatch]
            val bytes = RecordAccess.buffer(batch)
            require(batch.numRows >= 0 && batch.sizeInBytes == bytes.length.toLong)
            data.writeInt(0x50434231)
            data.writeInt(batch.numRows)
            data.writeInt(bytes.length)
            data.write(bytes)
            PcbsCodecCounters.written(bytes.length.toLong)
            this
          } catch {
            case failure: Throwable =>
              try close() catch {
                case cleanup: Throwable =>
                  if (cleanup ne failure) failure.addSuppressed(cleanup)
              }
              throw failure
          }
        }
        override def flush(): Unit = data.flush()
        override def close(): Unit = synchronized {
          if (!closed) {
            closed = true
            data.close()
          }
        }
      }
      Option(TaskContext.get()).foreach { context =>
        context.addTaskCompletionListener[Unit] { _ => stream.close() }
      }
      stream
    }

    override def deserializeStream(in: InputStream): DeserializationStream = {
      final class OwnedReadStream extends DeserializationStream {
        private val data = new DataInputStream(in)
        private var closed = false
        override def close(): Unit = synchronized {
          if (!closed) {
            closed = true
            data.close()
          }
        }
        override def readObject[T: ClassTag](): T = {
          require(!closed, "Read after PCBS codec stream close")
          var started = false
          try {
            val first = data.read()
            if (first < 0) throw new EOFException()
            started = true
            val magic = (first << 24) | (data.readUnsignedByte() << 16) |
              (data.readUnsignedByte() << 8) | data.readUnsignedByte()
            if (magic != 0x50434231) throw new IOException("Invalid PCBS codec record magic")
            val rows = data.readInt()
            val length = data.readInt()
            if (rows < 0 || length < 0) throw new IOException("Invalid PCBS record metadata")
            val bytes = new Array[Byte](length)
            data.readFully(bytes)
            val batch = RecordAccess.create(rows, bytes)
            PcbsCodecCounters.read(length.toLong)
            batch.asInstanceOf[T]
          } catch {
            case failure: Throwable =>
              val reported = failure match {
                case eof: EOFException if started =>
                  new IOException("Truncated PCBS codec record", eof)
                case other => other
              }
              try close() catch {
                case cleanup: Throwable =>
                  if (cleanup ne reported) reported.addSuppressed(cleanup)
              }
              throw reported
          }
        }
      }
      val stream = new OwnedReadStream
      Option(TaskContext.get()).foreach { context =>
        context.addTaskCompletionListener[Unit] { _ => stream.close() }
      }
      stream
    }
  }
}

/** Concrete wrapper inheritance is required by GPU cache-scan tagging and execution. */
class ResearchParquetCachedBatchSerializer
    extends com.nvidia.spark.ParquetCachedBatchSerializer with SqlCacheCodecProvider {
  override def cacheCodec(schema: Seq[Attribute]): Serializer = new PcbsRecordCodec
}
