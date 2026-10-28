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
import java.util.ArrayDeque
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{CloseableArray, ColumnVector => CudfColumn, ContiguousTable,
  Cuda, DefaultHostMemoryAllocator, DeviceMemoryBuffer, NvtxColor, NvtxRange, Table}
import cacheexploration.nvcomp.{PackedSegments, PackedTableCodec}
import com.nvidia.spark.ParquetCachedBatchSerializer
import com.nvidia.spark.rapids.{ConcatAndConsumeAll, GpuColumnVector, GpuSemaphore, RapidsConf,
  SpillableColumnarBatch, SpillPriorities}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.withRetryNoSplit

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.storage.StorageLevel

/** Spark owns these serializable heap bytes, including metadata and explicit framing. */
case class NvcompCacheRecord(numRows: Int, bytes: Array[Byte]) extends CachedBatch {
  override def sizeInBytes: Long = bytes.length.toLong
}

/** The subtype only satisfies the existing concrete GPU cache-scan gate; no PCBS codec is used. */
abstract class SparkNvcompSerializer extends ParquetCachedBatchSerializer {
  protected def codec: String

  override def supportsColumnarInput(schema: Seq[Attribute]): Boolean =
    SparkNvcompSerializer.supported(schema.toStructType)
  override def supportsColumnarOutput(schema: StructType): Boolean = false
  override def vectorTypes(attributes: Seq[Attribute], conf: SQLConf): Option[Seq[String]] = None
  override def buildFilter(predicates: Seq[Expression], cachedAttributes: Seq[Attribute]):
      (Int, Iterator[CachedBatch]) => Iterator[CachedBatch] = (_, input) => input

  override def convertInternalRowToCachedBatch(input: RDD[InternalRow], schema: Seq[Attribute],
      storageLevel: StorageLevel, conf: SQLConf): RDD[CachedBatch] =
    throw SparkNvcompSerializer.cpuUnsupported("row encoding")

  override def convertCachedBatchToInternalRow(input: RDD[CachedBatch], cached: Seq[Attribute],
      selected: Seq[Attribute], conf: SQLConf): RDD[InternalRow] =
    throw SparkNvcompSerializer.cpuUnsupported("row decoding")

  override def convertCachedBatchToColumnarBatch(input: RDD[CachedBatch], cached: Seq[Attribute],
      selected: Seq[Attribute], conf: SQLConf): RDD[ColumnarBatch] =
    throw SparkNvcompSerializer.cpuUnsupported("CPU columnar decoding")

  override def convertColumnarBatchToCachedBatch(input: RDD[ColumnarBatch],
      schema: Seq[Attribute], storageLevel: StorageLevel, conf: SQLConf): RDD[CachedBatch] = {
    SparkNvcompSerializer.requireSupported(schema.toStructType)
    val selectedCodec = codec
    input.mapPartitions(batches => new SparkNvcompSerializer.Encoder(batches, selectedCodec))
  }

  override def gpuConvertCachedBatchToColumnarBatch(input: RDD[CachedBatch], cached: Seq[Attribute],
      selected: Seq[Attribute], conf: SQLConf): RDD[ColumnarBatch] = {
    SparkNvcompSerializer.requireSupported(cached.toStructType)
    val indices = selected.map { attribute =>
      val index = cached.indexWhere(_.exprId == attribute.exprId)
      require(index >= 0, "Selected attribute is missing from the nvCOMP cache")
      index
    }.toArray
    val types = selected.map(_.dataType).toArray
    val selectedCodec = codec
    val targetBytes = new RapidsConf(conf).gpuTargetBatchSizeBytes
    input.mapPartitions { records =>
      if (indices.isEmpty) {
        records.map { raw =>
          val record = SparkNvcompSerializer.parse(raw, selectedCodec)
          new ColumnarBatch(Array.empty[ColumnVector], record.rows)
        }
      } else {
        new SparkNvcompSerializer.Decoder(records, selectedCodec, indices, types, targetBytes)
      }
    }
  }
}

class SparkNvcompUncompressedSerializer extends SparkNvcompSerializer {
  override protected def codec: String = "uncompressed"
}
class SparkNvcompLz4Serializer extends SparkNvcompSerializer {
  override protected def codec: String = "lz4"
}
class SparkNvcompZstdSerializer extends SparkNvcompSerializer {
  override protected def codec: String = "zstd"
}
class SparkNvcompSnappySerializer extends SparkNvcompSerializer {
  override protected def codec: String = "snappy"
}
class SparkNvcompAnsSerializer extends SparkNvcompSerializer {
  override protected def codec: String = "ans"
}
class SparkNvcompCascadedSerializer extends SparkNvcompSerializer {
  override protected def codec: String = "cascaded-typed"
}
class SparkNvcompCascadedByteSerializer extends SparkNvcompSerializer {
  override protected def codec: String = "cascaded8"
}

object SparkNvcompSerializer {
  private val RecordBytes = 8L * 1024 * 1024
  private val GroupBytes = 256L * 1024 * 1024
  // Leave space for codec expansion and wire framing; the actual heap group is checked as well.
  private val RawGroupBytes = GroupBytes - RecordBytes
  private val ChunkBytes = 64L * 1024
  private val HeaderBytes = 40
  private val DescriptorBytes = 32
  private val Magic = 0x4e564343
  private val encodedGroups = new AtomicLong
  private val decodedGroups = new AtomicLong
  private val nativeEncodeCalls = new AtomicLong
  private val nativeDecodeCalls = new AtomicLong
  private val concatenations = new AtomicLong
  private val outputBatches = new AtomicLong
  private val outputRows = new AtomicLong
  private val outputDeviceBytes = new AtomicLong
  private val maxOutputDeviceBytes = new AtomicLong

  /** Process-local observations, cumulative across tasks; these are not Spark storage statistics. */
  def snapshot(): Array[Long] = Array(encodedGroups.get(), decodedGroups.get(),
    nativeEncodeCalls.get(), nativeDecodeCalls.get(), concatenations.get(),
    outputBatches.get(), outputRows.get(), outputDeviceBytes.get(), maxOutputDeviceBytes.get())

  def supported(schema: StructType): Boolean = schema.forall { field =>
    Set[DataType](IntegerType, LongType, StringType).contains(field.dataType)
  }
  def requireSupported(schema: StructType): Unit = if (!supported(schema)) {
    throw new UnsupportedOperationException("Experimental nvCOMP cache supports only int/long/string")
  }
  def cpuUnsupported(route: String): UnsupportedOperationException =
    new UnsupportedOperationException("Experimental nvCOMP cache requires GPU execution; " +
      route + " and CPU fallback are disabled")

  private def codecId(codec: String): Int = codec match {
    case "uncompressed" => 0
    case "lz4" => 1
    case "zstd" => 2
    case "cascaded8" => 3
    case "cascaded-typed" => 4
    case "snappy" => 5
    case "ans" => 6
    case _ => throw new IllegalArgumentException("Unknown experimental nvCOMP codec")
  }

  private final case class Parsed(rows: Int, packedBytes: Long, metadata: Array[Byte],
      offsets: Array[Long], lengths: Array[Long], bits: Array[Int],
      payloadOffsets: Array[Int], payloadLengths: Array[Int], bytes: Array[Byte]) {
    def layout: Array[PackedSegments.Segment] = PackedTableCodec.layout(offsets, lengths, bits)
  }

  private def parse(raw: CachedBatch, codec: String): Parsed = {
    val record = raw match {
      case value: NvcompCacheRecord => value
      case _ => throw new IllegalArgumentException("Unexpected nvCOMP cached record type")
    }
    require(record.bytes.length >= HeaderBytes && record.bytes.length.toLong <= GroupBytes,
      "Invalid nvCOMP cache record byte length")
    val in = ByteBuffer.wrap(record.bytes).order(ByteOrder.LITTLE_ENDIAN)
    require(in.getInt() == Magic && in.getInt() == 1 && in.getInt() == codecId(codec),
      "nvCOMP cache format/codec mismatch")
    val rows = in.getInt()
    require(rows >= 0 && rows == record.numRows && in.getLong() == ChunkBytes,
      "nvCOMP cache row count/chunk mismatch")
    val packed = in.getLong()
    val metadataLength = in.getInt()
    val count = in.getInt()
    require(packed >= 0 && packed <= RecordBytes && metadataLength >= 0 && count >= 0 &&
      HeaderBytes.toLong + metadataLength + count.toLong * DescriptorBytes <= record.bytes.length,
      "Invalid nvCOMP cache layout lengths")
    val metadata = new Array[Byte](metadataLength)
    in.get(metadata)
    val offsets = new Array[Long](count)
    val lengths = new Array[Long](count)
    val bits = new Array[Int](count)
    val sizes = new Array[Int](count)
    var covered = 0L
    var payloadTotal = 0L
    (0 until count).foreach { i =>
      offsets(i) = in.getLong()
      lengths(i) = in.getLong()
      bits(i) = in.getInt()
      require(in.getInt() == 0, "Invalid nvCOMP cache descriptor flags")
      val encoded = in.getLong()
      require(offsets(i) == covered && offsets(i) % 8 == 0 && lengths(i) > 0 &&
        Set(8, 32, 64).contains(bits(i)) && lengths(i) % (bits(i) / 8) == 0 &&
        encoded > 0 && encoded <= Int.MaxValue, "Invalid nvCOMP cache segment")
      covered = Math.addExact(covered, lengths(i))
      payloadTotal = Math.addExact(payloadTotal, encoded)
      sizes(i) = encoded.toInt
    }
    require(covered == packed && in.position().toLong + payloadTotal == record.bytes.length,
      "Incomplete nvCOMP cache payload")
    val starts = new Array[Int](count)
    var start = in.position()
    sizes.indices.foreach { i => starts(i) = start; start = Math.addExact(start, sizes(i)) }
    Parsed(rows, packed, metadata, offsets, lengths, bits, starts, sizes, record.bytes)
  }

  private def wireSize(encoded: PackedTableCodec.EncodedTable): Long =
    HeaderBytes.toLong + encoded.metadata.length + encoded.segments.length.toLong * DescriptorBytes +
      encoded.payloads.map(_.getLength).sum

  private def wire(rows: Int, encoded: PackedTableCodec.EncodedTable, codec: String,
      stream: Cuda.Stream): NvcompCacheRecord = {
    val bytes = wireSize(encoded)
    require(bytes <= GroupBytes && bytes <= Int.MaxValue, "Encoded nvCOMP record exceeds heap bound")
    val result = new Array[Byte](bytes.toInt)
    val out = ByteBuffer.wrap(result).order(ByteOrder.LITTLE_ENDIAN)
    out.putInt(Magic).putInt(1).putInt(codecId(codec))
    out.putInt(rows).putLong(ChunkBytes).putLong(encoded.packedBytes)
      .putInt(encoded.metadata.length).putInt(encoded.segments.length)
    out.put(encoded.metadata)
    encoded.segments.indices.foreach { i =>
      val segment = encoded.segments(i)
      out.putLong(segment.offset).putLong(segment.length).putInt(segment.typeBits).putInt(0)
        .putLong(encoded.payloads(i).getLength)
    }
    encoded.payloads.foreach { payload =>
      val length = Math.toIntExact(payload.getLength)
      withResource(DefaultHostMemoryAllocator.get().allocate(length, true)) { host =>
        host.copyFromDeviceBuffer(payload, stream)
        host.getBytes(result, out.position(), 0, length)
      }
      out.position(out.position() + length)
    }
    NvcompCacheRecord(rows, result)
  }

  private final class Encoder(input: Iterator[ColumnarBatch], codec: String)
      extends Iterator[CachedBatch] with AutoCloseable {
    private var parent: SpillableColumnarBatch = null
    private var offset = 0
    private var rowTarget = 1
    private var closed = false
    private val pending = new ArrayDeque[NvcompCacheRecord]()
    Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => close()))

    private def nextParent(): Boolean = {
      while (parent == null && input.hasNext) {
        val batch = input.next()
        if (batch.numCols() == 0) {
          withResource(batch) { _ => () }
          throw cpuUnsupported("zero-column input encoding")
        }
        if (!(0 until batch.numCols()).forall(i => batch.column(i).isInstanceOf[GpuColumnVector])) {
          throw cpuUnsupported("non-GpuColumnVector input encoding")
        }
        parent = closeOnExcept(batch) { owned =>
          SpillableColumnarBatch(owned, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
        }
        offset = 0
        rowTarget = math.max(1, math.min(Int.MaxValue.toLong,
          RecordBytes * math.max(1, parent.numRows()) / math.max(1L, parent.sizeInBytes)).toInt)
        if (parent.numRows() == 0) { withResource(parent) { _ => () }; parent = null }
      }
      parent != null
    }

    private def pack(): ContiguousTable = {
      var take = math.min(rowTarget, parent.numRows() - offset)
      while (true) {
        val candidate = withRetryNoSplit[ContiguousTable] {
          withResource(parent.getColumnarBatch()) { batch =>
            withResource(new ArrayBuffer[CudfColumn]()) { columns =>
              (0 until batch.numCols()).foreach { i =>
                columns += batch.column(i).asInstanceOf[GpuColumnVector].getBase
                  .subVector(offset, offset + take)
              }
              withResource(new Table(columns.toArray: _*)) { table =>
                withResource(CloseableArray.wrap(table.contiguousSplit())) { packed =>
                  require(packed.size() == 1, "Expected one packed cache table")
                  packed.release()(0)
                }
              }
            }
          }
        }
        if (candidate.getBuffer.getLength <= RecordBytes) {
          offset += take
          rowTarget = take
          if (offset == parent.numRows()) { withResource(parent) { _ => () }; parent = null }
          return candidate
        }
        withResource(candidate) { _ => () }
        require(take > 1, "One input row exceeds the experimental 8MiB packed cache-record bound")
        take = math.max(1, take / 2)
      }
      throw new IllegalStateException("Unreachable packed-row loop")
    }

    private def fill(): Unit = {
      GpuSemaphore.acquireIfNecessary(TaskContext.get())
      withResource(new ArrayBuffer[ContiguousTable]()) { tables =>
        var bytes = 0L
        while (bytes + RecordBytes <= RawGroupBytes && nextParent()) {
          val table = pack()
          tables += table
          bytes += table.getBuffer.getLength
        }
        if (tables.nonEmpty) {
          val records = withRetryNoSplit {
            withResource(PackedTableCodec.compress(tables.toArray, codec, ChunkBytes,
                Cuda.DEFAULT_STREAM)) { compressed =>
              require(compressed.tables.length == tables.length, "nvCOMP encoded table count")
              require(compressed.tables.map(wireSize).sum <= GroupBytes,
                "Encoded nvCOMP group exceeds the 256MiB heap bound")
              val result = compressed.tables.zip(tables).map { case (encoded, table) =>
                wire(Math.toIntExact(table.getRowCount), encoded, codec, Cuda.DEFAULT_STREAM)
              }
              encodedGroups.incrementAndGet()
              nativeEncodeCalls.addAndGet(compressed.calls.length)
              result
            }
          }
          records.foreach(pending.addLast)
        }
      }
    }

    override def hasNext: Boolean = {
      try {
        if (!closed && pending.isEmpty) fill()
        if (pending.isEmpty) close()
        !closed
      } catch { case failure: Throwable => close(); throw failure }
    }
    override def next(): CachedBatch = {
      if (!hasNext) throw new NoSuchElementException
      pending.removeFirst()
    }
    override def close(): Unit = if (!closed) {
      closed = true
      pending.clear()
      val owned = parent
      parent = null
      if (owned != null) withResource(owned) { _ => () }
    }
  }

  private final class Decoder(input: Iterator[CachedBatch], codec: String,
      indices: Array[Int], types: Array[DataType], targetBytes: Long)
      extends Iterator[ColumnarBatch] with AutoCloseable {
    private val records = input.buffered
    private val pending = new ArrayDeque[SpillableColumnarBatch]()
    private var closed = false
    Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => close()))

    private def fill(): Unit = {
      val group = new ArrayBuffer[Parsed]()
      var bytes = 0L
      var rows = 0L
      var accepting = true
      while (accepting && records.hasNext) {
        val record = parse(records.head, codec)
        require(record.packedBytes > 0, "Nonempty GPU selection requires packed cache payload")
        if (group.nonEmpty && (bytes > targetBytes - record.packedBytes ||
            rows + record.rows > Int.MaxValue)) {
          accepting = false
        } else {
          records.next()
          group += record
          bytes += record.packedBytes
          rows += record.rows
        }
      }
      if (group.nonEmpty) {
        GpuSemaphore.acquireIfNecessary(TaskContext.get())
        var successfulCalls = 0
        val pieces = withRetryNoSplit[Array[SpillableColumnarBatch]] {
          closeOnExcept(new ArrayBuffer[SpillableColumnarBatch]()) { output =>
            withResource(new ArrayBuffer[DeviceMemoryBuffer]()) { uploads =>
              val inputBuffers = group.map { record =>
                record.payloadLengths.indices.map { i =>
                  val length = record.payloadLengths(i)
                  val buffer = closeOnExcept(DeviceMemoryBuffer.allocate(length, Cuda.DEFAULT_STREAM)) {
                      device =>
                    withResource(DefaultHostMemoryAllocator.get().allocate(length, true)) { host =>
                      host.setBytes(0, record.bytes, record.payloadOffsets(i), length)
                      device.copyFromHostBuffer(host, Cuda.DEFAULT_STREAM)
                    }
                    device
                  }
                  uploads += buffer
                  buffer
                }.toArray
              }.toArray
              withResource(PackedTableCodec.decompress(inputBuffers, group.map(_.layout).toArray,
                  group.map(_.packedBytes).toArray, codec, ChunkBytes, targetBytes,
                  Cuda.DEFAULT_STREAM)) { batch =>
                successfulCalls = batch.calls.length
                group.indices.foreach { i =>
                  withResource(Table.fromPackedTable(ByteBuffer.wrap(group(i).metadata),
                      batch.packedOutputs(i))) { table =>
                    require(table.getRowCount == group(i).rows, "nvCOMP decoded row count mismatch")
                    withResource(new Table(indices.map(table.getColumn): _*)) { projected =>
                      val result = GpuColumnVector.from(projected, types)
                      output += closeOnExcept(result) { owned =>
                        SpillableColumnarBatch(owned, SpillPriorities.ACTIVE_BATCHING_PRIORITY)
                      }
                    }
                  }
                }
              }
            }
            output.toArray
          }
        }
        // Decoder owners, compressed uploads and scratch are released; pieces remain spillable.
        val decoded = withRetryNoSplit(pieces.toSeq) { attempt =>
          withResource(new NvtxRange("nvcomp-cache-concatenate", NvtxColor.CYAN)) { _ =>
            val batches = attempt.safeMap(_.getColumnarBatch()).toArray
            val result = ConcatAndConsumeAll.buildNonEmptyBatchFromTypes(batches, types)
            closeOnExcept(result) { owned =>
              SpillableColumnarBatch(owned, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
            }
          }
        }
        pending.addLast(decoded)
        decodedGroups.incrementAndGet()
        nativeDecodeCalls.addAndGet(successfulCalls)
        if (group.length > 1) concatenations.incrementAndGet()
      }
    }

    override def hasNext: Boolean = {
      try {
        if (!closed && pending.isEmpty) fill()
        if (pending.isEmpty) close()
        !closed
      } catch { case failure: Throwable => close(); throw failure }
    }
    override def next(): ColumnarBatch = {
      if (!hasNext) throw new NoSuchElementException
      GpuSemaphore.acquireIfNecessary(TaskContext.get())
      val holder = pending.peekFirst()
      val result = withRetryNoSplit[ColumnarBatch] { holder.getColumnarBatch() }
      closeOnExcept(result) { owned =>
        pending.removeFirst()
        withResource(holder) { _ => () }
        val bytes = GpuColumnVector.getTotalDeviceMemoryUsed(owned)
        outputBatches.incrementAndGet()
        outputRows.addAndGet(owned.numRows().toLong)
        outputDeviceBytes.addAndGet(bytes)
        maxOutputDeviceBytes.accumulateAndGet(bytes, new java.util.function.LongBinaryOperator {
          override def applyAsLong(left: Long, right: Long): Long = math.max(left, right)
        })
        owned
      }
    }
    override def close(): Unit = if (!closed) {
      closed = true
      withResource(new ArrayBuffer[SpillableColumnarBatch]()) { owned =>
        while (!pending.isEmpty) owned += pending.removeFirst()
      }
    }
  }
}
