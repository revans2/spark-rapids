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

package org.apache.spark.sql.cacheexploration

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, OutputStream}
import java.net.URLClassLoader
import java.nio.ByteBuffer
import java.util.Optional

import scala.collection.JavaConverters._

import ai.rapids.cudf.{
  ArrowIPCOptions, ArrowIPCWriterOptions, ColumnVector => CudfColumn, HostBufferConsumer,
  DType, HostBufferProvider, HostColumnVector, HostMemoryBuffer, Table}
import com.nvidia.spark.ParquetCachedBatchSerializer
import com.nvidia.spark.rapids.{
  GpuColumnVector, GpuSemaphore, SpillableColumnarBatch, SpillPriorities}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.withRetryNoSplit

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, UnsafeProjection}
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.execution.arrow.ArrowConverters
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.apache.spark.storage.StorageLevel

case class VortexCacheRecord(numRows: Int, bytes: Array[Byte]) extends CachedBatch {
  override def sizeInBytes: Long = bytes.length
}

/**
 * Experimental wrapper subtype solely for the existing concrete GPU scan gate.
 * Every serializer entry is overridden; no PCBS encoder/decoder is called.
 */
class SparkVortexSerializer extends ParquetCachedBatchSerializer {
  import SparkVortexSerializer._

  override def supportsColumnarInput(schema: Seq[Attribute]): Boolean =
    supported(schema.toStructType)
  // CPU consumers deliberately use Spark's row route in this first prototype.
  override def supportsColumnarOutput(schema: StructType): Boolean = false
  override def vectorTypes(attributes: Seq[Attribute], conf: SQLConf): Option[Seq[String]] = None

  override def buildFilter(predicates: Seq[Expression], cachedAttributes: Seq[Attribute]):
      (Int, Iterator[CachedBatch]) => Iterator[CachedBatch] = (_, input) => input

  override def convertInternalRowToCachedBatch(input: RDD[InternalRow],
      schema: Seq[Attribute], storageLevel: StorageLevel, conf: SQLConf): RDD[CachedBatch] = {
    requireSupported(schema.toStructType)
    val renamed = namedSchema(schema)
    val zone = conf.sessionLocalTimeZone
    input.mapPartitions(rows => encodeRows(rows, renamed, zone))
  }

  override def convertColumnarBatchToCachedBatch(input: RDD[ColumnarBatch],
      schema: Seq[Attribute], storageLevel: StorageLevel, conf: SQLConf): RDD[CachedBatch] = {
    requireSupported(schema.toStructType)
    val renamed = namedSchema(schema)
    val zone = conf.sessionLocalTimeZone
    input.mapPartitions { batches =>
      batches.flatMap { batch =>
        if (batch.numCols() == 0) {
          val count = batch.numRows()
          withResource(batch) { _ => () }
          Iterator.single(VortexCacheRecord(count, Array.emptyByteArray))
        } else if (batch.column(0).isInstanceOf[GpuColumnVector]) {
          gpuEncode(batch)
        } else {
          // CPU input batches remain owned by their Spark producer.
          encodeRows(batch.rowIterator().asScala, renamed, zone)
        }
      }
    }
  }

  override def convertCachedBatchToColumnarBatch(input: RDD[CachedBatch],
      cached: Seq[Attribute], selected: Seq[Attribute], conf: SQLConf): RDD[ColumnarBatch] = {
    throw new UnsupportedOperationException("Experimental Vortex CPU scan uses InternalRow output")
  }

  override def convertCachedBatchToInternalRow(input: RDD[CachedBatch],
      cached: Seq[Attribute], selected: Seq[Attribute], conf: SQLConf): RDD[InternalRow] = {
    requireSupported(cached.toStructType)
    val indices = selectedIndices(cached, selected)
    val types = selected.map(_.dataType).toArray
    input.mapPartitions { records =>
      if (selected.isEmpty) {
        records.flatMap(record => Iterator.fill(record.numRows)(InternalRow.empty))
      } else {
        cpuRows(records.map(_.asInstanceOf[VortexCacheRecord]), indices, types)
      }
    }
  }

  override def gpuConvertCachedBatchToColumnarBatch(input: RDD[CachedBatch],
      cached: Seq[Attribute], selected: Seq[Attribute], conf: SQLConf): RDD[ColumnarBatch] = {
    requireSupported(cached.toStructType)
    val indices = selectedIndices(cached, selected)
    val types = selected.map(_.dataType).toArray
    input.mapPartitions { records =>
      records.flatMap { raw =>
        val record = raw.asInstanceOf[VortexCacheRecord]
        if (selected.isEmpty) {
          Iterator.single(new ColumnarBatch(Array.empty, record.numRows))
        } else {
          var decodedRows = 0
          val decoded = decodedChunkedIpc(record, indices)
          val batches = decoded.map { ipc =>
            val chunks = ipc(0).asInstanceOf[Array[Array[Byte]]]
            val length = ipc(1).asInstanceOf[Integer].intValue()
            try {
              GpuSemaphore.acquireIfNecessary(TaskContext.get())
              val result = withRetryNoSplit[ColumnarBatch] {
                withResource(Table.readArrowIPCChunked(
                    ArrowIPCOptions.DEFAULT, chunkProvider(chunks, length))) { reader =>
                  withResource(reader.getNextIfAvailable()) { table =>
                    require(table != null, "Expected one GPU IPC batch")
                    GpuColumnVector.from(table, types)
                  }
                }
              }
              decodedRows += result.numRows()
              result
            } catch {
              case failure: Throwable =>
                try { withResource(decoded) { _ => () } } catch {
                  case cleanup: Throwable => failure.addSuppressed(cleanup)
                }
                throw failure
            } finally {
              // Keep chunks intact across retry attempts, then release every payload reference.
              chunks.indices.foreach(i => chunks(i) = null)
            }
          }
          new Iterator[ColumnarBatch] {
            override def hasNext: Boolean = {
              val more = batches.hasNext
              if (!more) require(decodedRows == record.numRows, "Vortex GPU row count")
              more
            }
            override def next(): ColumnarBatch = batches.next()
          }
        }
      }
    }
  }
}

/**
 * Experimental GPU upload for caches containing only int/long, including projected scans.
 * Cache encoding and CPU reads are inherited unchanged; string-cache GPU reads are an explicit gap.
 */
class SparkVortexDirectBufferSerializer extends SparkVortexSerializer {
  override def gpuConvertCachedBatchToColumnarBatch(input: RDD[CachedBatch],
      cached: Seq[Attribute], selected: Seq[Attribute], conf: SQLConf): RDD[ColumnarBatch] = {
    require(cached.forall(a => a.dataType == IntegerType || a.dataType == LongType),
      "Direct buffer Vortex GPU decode supports only int/long")
    val indices = SparkVortexSerializer.selectedIndices(cached, selected)
    val types = selected.map(_.dataType).toArray
    input.mapPartitions { records =>
      records.flatMap { raw =>
        val record = raw.asInstanceOf[VortexCacheRecord]
        if (selected.isEmpty) {
          Iterator.single(new ColumnarBatch(Array.empty, record.numRows))
        } else {
          SparkVortexSerializer.directGpuBatches(record, indices, types)
        }
      }
    }
  }
}

object SparkVortexSerializer {
  // Harness constants, not production configuration or a total executor memory budget.
  val targetBytes: Int = 8 * 1024 * 1024
  val capBytes: Int = 64 * 1024 * 1024

  def supported(schema: StructType): Boolean = schema.forall { field =>
    Set[DataType](LongType, IntegerType, StringType).contains(field.dataType)
  }
  def requireSupported(schema: StructType): Unit = require(supported(schema),
    "Experimental Vortex supports only int/long/string; this combination is an explicit gap")
  def namedSchema(schema: Seq[Attribute]): StructType = StructType(
    schema.zipWithIndex.map { case (attribute, index) =>
      StructField("c" + index, attribute.dataType, attribute.nullable)
    })
  def selectedIndices(cached: Seq[Attribute], selected: Seq[Attribute]): Array[Int] =
    selected.map { attribute =>
      val index = cached.indexWhere(_.exprId == attribute.exprId)
      require(index >= 0, "Selected attribute missing from cache")
      index
    }.toArray

  private lazy val codecClass: Class[_] = {
    val stream = getClass.getResourceAsStream("/vortex-child-classpath.txt")
    require(stream != null, "Missing isolated Vortex classpath resource")
    val paths = withResource(stream)(owned =>
      new String(owned.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8).trim)
    val urls = paths.split(java.io.File.pathSeparator).map(path =>
      new java.io.File(path).toURI.toURL)
    val loader = new URLClassLoader(urls, ClassLoader.getPlatformClassLoader)
    loader.loadClass("cacheexploration.VortexIpcCodec")
  }
  private def invoke(method: java.lang.reflect.Method, target: AnyRef, args: AnyRef*): AnyRef = {
    try {
      method.invoke(target, args: _*)
    } catch {
      case error: java.lang.reflect.InvocationTargetException => throw error.getCause
    }
  }
  def encode(ipc: Array[Byte]): Array[Byte] = invoke(
    codecClass.getMethod("encode", classOf[Array[Byte]], java.lang.Integer.TYPE),
    null, ipc, Int.box(capBytes)).asInstanceOf[Array[Byte]]

  def decodedIpc(record: VortexCacheRecord, indices: Array[Int],
      legacy: Boolean, registerTask: Boolean = true): Iterator[Array[Byte]] with AutoCloseable =
    decodedUnits[Array[Byte]](record, indices, legacy, registerTask, "next")

  def decodedChunkedIpc(record: VortexCacheRecord,
      indices: Array[Int]): Iterator[Array[AnyRef]] with AutoCloseable =
    decodedUnits[Array[AnyRef]](record, indices, legacy = false,
      registerTask = true, nextName = "nextChunks")

  private def decodedUnits[A <: AnyRef](record: VortexCacheRecord, indices: Array[Int],
      legacy: Boolean, registerTask: Boolean, nextName: String): Iterator[A] with AutoCloseable = {
    var decoder = invoke(codecClass.getMethod("open", classOf[Array[Byte]],
      classOf[Array[Int]], java.lang.Boolean.TYPE, java.lang.Integer.TYPE),
      null, record.bytes, indices, Boolean.box(legacy), Int.box(capBytes))
    val expectedRows = record.numRows
    val nextMethod = decoder.getClass.getMethod(nextName)
    val closeMethod = decoder.getClass.getMethod("close")
    val rowsMethod = decoder.getClass.getMethod("rowsRead")
    new Iterator[A] with AutoCloseable {
      private var pending: A = null.asInstanceOf[A]
      private var closed = false
      if (registerTask) {
        Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => close()))
      }
      override def hasNext: Boolean = {
        if (!closed && pending == null) {
          try {
            pending = invoke(nextMethod, decoder).asInstanceOf[A]
            if (pending == null) {
              require(invoke(rowsMethod, decoder).asInstanceOf[Integer].intValue == expectedRows,
                "Vortex decoder row count")
              close()
            }
          } catch {
            case failure: Throwable => close(); throw failure
          }
        }
        pending != null
      }
      override def next(): A = {
        if (!hasNext) throw new NoSuchElementException
        val result = pending
        pending = null.asInstanceOf[A]
        result
      }
      override def close(): Unit = if (!closed) {
        closed = true
        pending = null.asInstanceOf[A]
        val owned = decoder
        decoder = null
        invoke(closeMethod, owned)
      }
    }
  }

  def directGpuBatches(record: VortexCacheRecord, indices: Array[Int],
      types: Array[DataType]): Iterator[ColumnarBatch] = {
    val decoded = decodedUnits[Array[AnyRef]](
      record, indices, legacy = false, registerTask = true, nextName = "nextBuffers")
    decoded.map { buffers =>
      try {
        // The decoder retains the Arrow root across all retry attempts. Do not advance it until
        // these synchronous uploads finish; task completion releases an abandoned decoder.
        GpuSemaphore.acquireIfNecessary(TaskContext.get())
        withRetryNoSplit[ColumnarBatch] {
          val rows = buffers(0).asInstanceOf[Integer].intValue()
          val widths = buffers(1).asInstanceOf[Array[Int]]
          val nulls = buffers(2).asInstanceOf[Array[Int]]
          val data = buffers(3).asInstanceOf[Array[ByteBuffer]]
          val validity = buffers(4).asInstanceOf[Array[ByteBuffer]]
          require(widths.length == types.length && nulls.length == types.length &&
            data.length == types.length && validity.length == types.length,
            "Direct buffer column count")
          val validBytes = (((rows.toLong + 7) / 8 + 63) / 64) * 64
          val bytes = widths.indices.map(i =>
            rows.toLong * widths(i) + (if (nulls(i) == 0) 0L else validBytes)).sum
          require(bytes <= capBytes, "Direct buffer upload exceeds hard cap")
          withResource(new scala.collection.mutable.ArrayBuffer[CudfColumn]()) { columns =>
            types.indices.foreach { i =>
              val dtype = types(i) match {
                case IntegerType => DType.INT32
                case LongType => DType.INT64
                case _ => throw new IllegalArgumentException("Direct buffer GPU type")
              }
              require(widths(i) == dtype.getSizeInBytes && nulls(i) >= 0 && nulls(i) <= rows,
                "Direct buffer type/null count")
              val host = closeOnExcept(
                new scala.collection.mutable.ArrayBuffer[HostMemoryBuffer]()) { allocated =>
                val hostData = HostMemoryBuffer.allocate(math.max(1L, rows.toLong * widths(i)), false)
                allocated += hostData
                hostData.asByteBuffer().put(data(i).duplicate())
                val hostValid = if (nulls(i) == 0) {
                  null
                } else {
                  val mask = HostMemoryBuffer.allocate(validBytes, false)
                  allocated += mask
                  mask.setMemory(0, validBytes, 0.toByte)
                  mask.asByteBuffer().put(validity(i).duplicate())
                  mask
                }
                val result = new HostColumnVector(
                  dtype, rows.toLong, Optional.of(java.lang.Long.valueOf(nulls(i).toLong)),
                  hostData, hostValid, null)
                // The host column owns these buffers after successful construction.
                allocated.clear()
                result
              }
              columns += withResource(host)(_.copyToDevice())
            }
            withResource(new Table(columns.toArray: _*)) { table =>
              require(table.getRowCount == rows, "Direct buffer GPU row count")
              GpuColumnVector.from(table, types)
            }
          }
        }
      } catch {
        case failure: Throwable =>
          try { withResource(decoded) { _ => () } } catch {
            case cleanup: Throwable => failure.addSuppressed(cleanup)
          }
          throw failure
      }
    }
  }

  def encodeRows(rows: Iterator[InternalRow], schema: StructType,
      zone: String): Iterator[CachedBatch] = {
    if (schema.isEmpty) {
      rows.grouped(10000).map(group => VortexCacheRecord(group.size, Array.emptyByteArray))
    } else {
      val projection = UnsafeProjection.create(schema)
      val unsafe = rows.map { row =>
        val projected = projection(row)
        require(projected.getSizeInBytes <= capBytes, "Oversized CPU input row")
        projected
      }
      val batches = ArrowConverters.toBatchWithSchemaIterator(
        unsafe, schema, 100000, targetBytes, zone, errorOnDuplicatedFieldNames = true)
      batches.map { ipc =>
        require(ipc.length <= capBytes, "CPU IPC unit exceeds hard cap")
        VortexCacheRecord(batches.rowCountInLastBatch.toInt, encode(ipc))
      }
    }
  }

  final class Sink extends OutputStream {
    private val stream = new ByteArrayOutputStream(65536)
    override def write(value: Int): Unit = { check(1); stream.write(value) }
    override def write(bytes: Array[Byte], offset: Int, length: Int): Unit = {
      check(length); stream.write(bytes, offset, length)
    }
    private def check(added: Int): Unit =
      require(stream.size().toLong + added <= capBytes, "GPU IPC unit exceeds hard cap")
    def result(): Array[Byte] = stream.toByteArray
  }
  def consumer(sink: Sink): HostBufferConsumer = new HostBufferConsumer {
    override def handleBuffer(buffer: HostMemoryBuffer, length: Long): Unit = {
      withResource(buffer) { owned =>
        val scratch = new Array[Byte](128 * 1024)
        var offset = 0L
        while (offset < length) {
          val count = math.min(scratch.length.toLong, length - offset).toInt
          owned.getBytes(scratch, 0, offset, count)
          sink.write(scratch, 0, count)
          offset += count
        }
      }
    }
  }
  def chunkProvider(chunks: Array[Array[Byte]], length: Int): HostBufferProvider = {
    require(length >= 0 && length <= capBytes, "Chunked IPC length exceeds hard cap")
    val chunkBytes = 64 * 1024
    val count = ((length.toLong + chunkBytes - 1) / chunkBytes).toInt
    require(chunks.length >= count, "Missing IPC chunks")
    (0 until count).foreach { i =>
      require(chunks(i) != null &&
        chunks(i).length >= math.min(chunkBytes, length - i * chunkBytes),
        "Truncated IPC chunk")
    }
    new HostBufferProvider {
      private var owned = chunks
      private var offset = 0
      override def readInto(buffer: HostMemoryBuffer, requested: Long): Long = {
        require(owned != null, "IPC provider is closed")
        require(requested >= 0, "Negative IPC read length")
        val amount = math.min(requested, length.toLong - offset).toInt
        var copied = 0
        while (copied < amount) {
          val index = offset / chunkBytes
          val within = offset % chunkBytes
          val take = math.min(amount - copied, chunkBytes - within)
          buffer.setBytes(copied, owned(index), within, take)
          offset += take
          copied += take
        }
        copied.toLong
      }
      override def close(): Unit = { owned = null }
    }
  }

  def gpuEncode(batch: ColumnarBatch): Iterator[CachedBatch] = {
    val parent = closeOnExcept(batch) { input =>
      SpillableColumnarBatch(input, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
    }
    new Iterator[CachedBatch] with AutoCloseable {
      private var offset = 0
      private var closed = false
      private val count = parent.numRows()
      private val names = parent.dataTypes.indices.map(i => "c" + i).toArray
      private val rowTarget = math.max(1,
        math.min(100000L, targetBytes.toLong * count / math.max(1L, parent.sizeInBytes)).toInt)
      Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => close()))
      override def hasNext: Boolean = {
        if (offset >= count) close()
        !closed
      }
      override def next(): CachedBatch = {
        if (!hasNext) throw new NoSuchElementException
        GpuSemaphore.acquireIfNecessary(TaskContext.get())
        val end = math.min(count, offset + rowTarget)
        try {
          val encoded = withRetryNoSplit {
            withResource(parent.getColumnarBatch()) { current =>
              withResource(new scala.collection.mutable.ArrayBuffer[CudfColumn]()) { columns =>
                (0 until current.numCols()).foreach { column =>
                  columns += (current.column(column).asInstanceOf[GpuColumnVector]
                    .getBase.subVector(offset, end))
                }
                withResource(new Table(columns.toArray: _*)) { table =>
                  // Verify actual slice size before whole-table host Arrow export.
                  require(table.getDeviceMemorySize <= capBytes,
                    "Oversized GPU slice: split estimate exceeded hard cap")
                  val sink = new Sink
                  val options = ArrowIPCWriterOptions.builder().withColumnNames(names: _*)
                    .withMaxChunkSize(end - offset).build()
                  withResource(Table.writeArrowIPCChunked(options, consumer(sink))) { writer =>
                    writer.write(table)
                  }
                  encode(sink.result())
                }
              }
            }
          }
          val result = VortexCacheRecord(end - offset, encoded)
          offset = end
          result
        } catch {
          case failure: Throwable => close(); throw failure
        }
      }
      override def close(): Unit = if (!closed) {
        closed = true
        withResource(parent) { _ => () }
      }
    }
  }

  def cpuRows(records: Iterator[VortexCacheRecord], indices: Array[Int],
      types: Array[DataType]): Iterator[InternalRow] = {
    new Iterator[InternalRow] with AutoCloseable {
      private var decoder: Iterator[Array[Byte]] with AutoCloseable = null
      private var reader: ArrowStreamReader = null
      private var rows: Iterator[InternalRow] = Iterator.empty
      private var allocator: org.apache.arrow.memory.BufferAllocator =
        ArrowUtils.rootAllocator.newChildAllocator("vortex-cpu-cache", 0, 4L * capBytes)
      private val projection = UnsafeProjection.create(types)
      private var closed = false
      Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => close()))

      private def closeReader(): Unit = {
        rows = Iterator.empty
        val previous = reader
        reader = null
        if (previous != null) withResource(previous) { _ => () }
      }

      override def hasNext: Boolean = {
        try {
          while (!closed && !rows.hasNext) {
            closeReader()
            if (decoder != null && decoder.hasNext) {
              val ipc = decoder.next()
              val opened = new ArrowStreamReader(new ByteArrayInputStream(ipc), allocator)
              reader = opened
              if (opened.loadNextBatch()) {
                val root = opened.getVectorSchemaRoot
                val columns = root.getFieldVectors.asScala.map(vector =>
                  new ArrowColumnVector(vector).asInstanceOf[ColumnVector]).toArray
                // The reader owns the vectors; this batch is only a row-access view.
                rows = new ColumnarBatch(columns, root.getRowCount).rowIterator().asScala
              }
            } else {
              if (decoder != null) { withResource(decoder) { _ => () }; decoder = null }
              if (records.hasNext) {
                decoder = decodedIpc(records.next(), indices, legacy = true, registerTask = false)
              } else {
                close()
              }
            }
          }
          !closed
        } catch {
          case failure: Throwable => close(); throw failure
        }
      }

      override def next(): InternalRow = {
        if (!hasNext) throw new NoSuchElementException
        projection(rows.next()).copy()
      }

      override def close(): Unit = if (!closed) {
        closed = true
        try {
          closeReader()
        } finally {
          try {
            val previous = decoder
            decoder = null
            if (previous != null) withResource(previous) { _ => () }
          } finally {
            val previous = allocator
            allocator = null
            if (previous != null) withResource(previous) { _ => () }
          }
        }
      }
    }
  }

}
