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

import java.nio.{ByteBuffer, ByteOrder}

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.vector.{FieldVector, VectorSchemaRoot}
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}

/** CPU adapter: Spark row conversion, then bounded packing and the shared chunk envelope. */
object CpuCachePath {
  import SparkNvcompSerializer.{ChunkBytes, RecordBytes}

  def supports(codec: String): Boolean =
    Set("uncompressed", "lz4", "snappy", "zstd").contains(codec)

  def requireCodec(codec: String): Unit = if (!supports(codec)) {
    throw SparkNvcompSerializer.cpuUnsupported(codec + " CPU codec")
  }

  private def arrowSchema(schema: StructType): StructType =
    StructType(schema.fields.zipWithIndex.map { case (field, i) => field.copy(name = "c" + i) })

  private def typeId(dataType: DataType): Int = dataType match {
    case IntegerType => 3
    case LongType => 4
    case StringType => 23
    case _ => throw new UnsupportedOperationException("Unsupported CPU cache type: " + dataType)
  }

  private def align(bytes: Long): Long = Math.multiplyExact((bytes + 63) / 64, 64)

  private case class Packed(bytes: Array[Byte], metadata: Array[Byte])

  private def pack(root: VectorSchemaRoot, types: Array[DataType]): Packed = {
    val rows = root.getRowCount
    val vectors = root.getFieldVectors.asScala.toArray
    val data = Array.fill[Long](types.length)(-1L)
    val masks = Array.fill[Long](types.length)(-1L)
    val offsets = Array.fill[Long](types.length)(-1L)
    val nulls = vectors.map(_.getNullCount)
    val copies = scala.collection.mutable.ArrayBuffer.empty[(ArrowBuf, Int, Int)]
    var size = 0L
    def add(buffer: ArrowBuf, length: Int): Long = {
      if (length == 0) -1L else {
        val start = size
        size = Math.addExact(size, align(length.toLong))
        require(size <= RecordBytes, "CPU packed record exceeds 8MiB")
        copies += ((buffer, start.toInt, length))
        start
      }
    }
    vectors.indices.foreach { i =>
      val vector = vectors(i)
      if (nulls(i) != 0) masks(i) = add(vector.getValidityBuffer, (rows + 7) / 8)
      types(i) match {
        case IntegerType => data(i) = add(vector.getDataBuffer, Math.multiplyExact(rows, 4))
        case LongType => data(i) = add(vector.getDataBuffer, Math.multiplyExact(rows, 8))
        case StringType =>
          offsets(i) = add(vector.getOffsetBuffer, Math.multiplyExact(rows + 1, 4))
          val chars = vector.getOffsetBuffer.getInt(rows.toLong * 4)
          require(chars >= 0, "Negative string payload length")
          data(i) = add(vector.getDataBuffer, chars)
        case _ => throw new UnsupportedOperationException("Unsupported CPU packed type")
      }
    }
    val bytes = new Array[Byte](Math.toIntExact(size))
    copies.foreach { case (buffer, start, length) => buffer.getBytes(0, bytes, start, length) }
    val metadata = HostPackedBridge.create(rows, types.map(typeId), nulls, data, masks, offsets, bytes)
    Packed(bytes, metadata)
  }

  def encodeRows(input: Iterator[InternalRow], schema: StructType,
      codec: String): Iterator[CachedBatch] = {
    requireCodec(codec)
    closeOnExcept(new CpuPackedCodec(codec, ChunkBytes.toInt)) { context =>
      val root = VectorSchemaRoot.create(
        ArrowUtils.toArrowSchema(arrowSchema(schema), "UTC", true), ArrowUtils.rootAllocator)
      closeOnExcept(root) { owned =>
        new Encoder(input, schema, codec, context, ArrowWriter.create(owned))
      }
    }
  }

  private class Encoder(input: Iterator[InternalRow], schema: StructType, codec: String,
      context: CpuPackedCodec, writer: ArrowWriter)
      extends Iterator[CachedBatch] with AutoCloseable {
    private val types = schema.fields.map(_.dataType)
    private val stringIndices = types.indices.filter(i => types(i) == StringType)
    private val fixedWidth = types.map {
      case IntegerType => 4L
      case LongType => 8L
      case _ => 0L
    }.sum
    private val fixedColumns = types.length - stringIndices.length
    private var leftover: InternalRow = null
    private var closed = false
    Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => close()))

    // Upper bound includes alignment and a validity bitmap for every column.
    private def packedUpperBound(rows: Long, chars: Long): Long =
      rows * fixedWidth + fixedColumns * 63L +
        stringIndices.length * ((rows + 1) * 4L + 126L) + chars +
        types.length * ((rows + 7) / 8 + 63L)

    override def hasNext: Boolean = {
      try {
        if (!closed && leftover == null && !input.hasNext) close()
        !closed
      } catch { case failure: Throwable => close(); throw failure }
    }

    override def next(): CachedBatch = {
      if (!hasNext) throw new NoSuchElementException
      try {
        writer.reset()
        var rows = 0
        var chars = 0L
        var full = false
        while (!full && (leftover != null || input.hasNext)) {
          val row = if (leftover != null) {
            val saved = leftover
            leftover = null
            saved
          } else input.next()
          val rowChars = stringIndices.map { i =>
            if (row.isNullAt(i)) 0L else row.getUTF8String(i).numBytes().toLong
          }.sum
          if (rows > 0 && packedUpperBound(rows.toLong + 1, chars + rowChars) > RecordBytes) {
            // Spark codegen may overwrite a row when its iterator advances.
            leftover = row.copy()
            full = true
          } else {
            require(packedUpperBound(rows.toLong + 1, chars + rowChars) <= RecordBytes,
              "One CPU input row exceeds the experimental 8MiB cache-record bound")
            writer.write(row) // Reuse Spark's schema-specific row-to-columnar conversion.
            rows += 1
            chars += rowChars
          }
        }
        writer.finish()
        if (types.isEmpty) {
          SparkNvcompSerializer.cpuWire(rows, Array.emptyByteArray, Array.emptyByteArray,
            Array.emptyByteArray, codec)
        } else {
          val packed = pack(writer.root, types)
          SparkNvcompSerializer.cpuWire(rows, packed.metadata, packed.bytes,
            context.encode(packed.bytes, 0, packed.bytes.length), codec)
        }
      } catch { case failure: Throwable => close(); throw failure }
    }

    override def close(): Unit = if (!closed) {
      closed = true
      leftover = null
      withResource(context) { _ => withResource(writer.root) { _ => () } }
    }
  }

  def decodeRows(input: Iterator[CachedBatch], schema: StructType, indices: Array[Int],
      codec: String): Iterator[InternalRow] = {
    requireCodec(codec)
    new RowDecoder(input, schema, indices, codec)
  }

  private class RowDecoder(input: Iterator[CachedBatch], schema: StructType,
      indices: Array[Int], codec: String) extends Iterator[InternalRow] with AutoCloseable {
    private val context = new CpuPackedCodec(codec, ChunkBytes.toInt)
    private val types = schema.fields.map(_.dataType)
    private val projection = UnsafeProjection.create(indices.map(types))
    private var root: VectorSchemaRoot = null
    private var rows: Iterator[InternalRow] = Iterator.empty
    private var closed = false
    Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => close()))

    private def releaseBatch(): Unit = {
      val owned = root
      root = null
      rows = Iterator.empty
      if (owned != null) withResource(owned) { _ => () }
    }

    private def decode(raw: CachedBatch): Unit = {
      val record = SparkNvcompSerializer.parse(raw, codec)
      if (indices.isEmpty) {
        rows = Iterator.fill(record.rows)(InternalRow.empty)
      } else {
        val bytes = if (record.lengths.length == 1) {
          context.decode(record.bytes, record.payloadOffsets(0), record.payloadLengths(0),
            Math.toIntExact(record.packedBytes))
        } else {
          val result = new Array[Byte](Math.toIntExact(record.packedBytes))
          record.lengths.indices.foreach { i =>
            val part = context.decode(record.bytes, record.payloadOffsets(i),
              record.payloadLengths(i), Math.toIntExact(record.lengths(i)))
            System.arraycopy(part, 0, result, Math.toIntExact(record.offsets(i)), part.length)
          }
          result
        }
        val descriptors = HostPackedBridge.parse(record.metadata, bytes)
        require(descriptors.length == types.length * 6, "CPU packed column count")
        root = VectorSchemaRoot.create(ArrowUtils.toArrowSchema(arrowSchema(schema), "UTC", true),
          ArrowUtils.rootAllocator)
        val vectors = root.getFieldVectors.asScala.toArray
        vectors.indices.foreach { i =>
          val base = i * 6
          require(descriptors(base) == typeId(types(i)) && descriptors(base + 1) == record.rows,
            "CPU packed column type/rows mismatch")
          val vector = vectors(i)
          vector.setInitialCapacity(record.rows)
          vector.allocateNew()
          def copy(offset: Long, length: Int, destination: ArrowBuf): Unit = {
            require(offset >= 0 && length >= 0 && offset <= bytes.length.toLong - length,
              "CPU packed buffer range")
            if (destination.capacity() < length) {
              throw new IllegalArgumentException("CPU decoded Arrow buffer too small")
            }
            destination.setBytes(0, bytes, offset.toInt, length)
          }
          val mask = descriptors(base + 4)
          if (mask < 0) {
            vector.getValidityBuffer.setOne(0, (record.rows.toLong + 7) / 8)
          } else {
            copy(mask, (record.rows + 7) / 8, vector.getValidityBuffer)
          }
          types(i) match {
            case IntegerType =>
              copy(descriptors(base + 3), Math.multiplyExact(record.rows, 4), vector.getDataBuffer)
            case LongType =>
              copy(descriptors(base + 3), Math.multiplyExact(record.rows, 8), vector.getDataBuffer)
            case StringType =>
              copy(descriptors(base + 5), Math.multiplyExact(record.rows + 1, 4),
                vector.getOffsetBuffer)
              val chars = vector.getOffsetBuffer.getInt(record.rows.toLong * 4)
              val charStart = descriptors(base + 3)
              require(chars >= 0 && (chars == 0 ||
                (charStart >= 0 && charStart <= bytes.length.toLong - chars)),
                "CPU decoded string payload range")
              val variable = vector.asInstanceOf[org.apache.arrow.vector.VarCharVector]
              while (variable.getDataBuffer.capacity() < chars) variable.reallocDataBuffer()
              if (chars != 0) copy(descriptors(base + 3), chars, variable.getDataBuffer)
              variable.setLastSet(record.rows - 1)
            case _ => throw new UnsupportedOperationException("Unsupported CPU decoded type")
          }
          vector.setValueCount(record.rows)
        }
        root.setRowCount(record.rows)
        val batch = new ColumnarBatch(indices.map(i =>
          new ArrowColumnVector(vectors(i)).asInstanceOf[ColumnVector]), record.rows)
        rows = batch.rowIterator().asScala
      }
    }

    override def hasNext: Boolean = {
      try {
        while (!closed && !rows.hasNext) {
          releaseBatch()
          if (input.hasNext) decode(input.next()) else close()
        }
        !closed
      } catch { case failure: Throwable => close(); throw failure }
    }
    override def next(): InternalRow = {
      if (!hasNext) throw new NoSuchElementException
      projection(rows.next())
    }
    override def close(): Unit = if (!closed) {
      closed = true
      withResource(context) { _ => releaseBatch() }
    }
  }
}
