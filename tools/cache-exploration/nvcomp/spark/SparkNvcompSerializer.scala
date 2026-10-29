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

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{CloseableArray, Cuda, DefaultHostMemoryAllocator, DeviceMemoryBuffer,
  NvtxColor, NvtxRange, Table}
import cacheexploration.nvcomp.{PackedSegments, PackedTableCodec}
import com.nvidia.spark.ParquetCachedBatchSerializer
import com.nvidia.spark.rapids.{ConcatAndConsumeAll, GpuColumnVector, GpuSemaphore, RapidsConf,
  SpillableBuffer, SpillableColumnarBatch, SpillPriorities}
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
      storageLevel: StorageLevel, conf: SQLConf): RDD[CachedBatch] = {
    SparkNvcompSerializer.requireSupported(schema.toStructType)
    CpuCachePath.requireCodec(codec)
    val selectedCodec = codec
    val types = schema.toStructType
    input.mapPartitions(rows => CpuCachePath.encodeRows(rows, types, selectedCodec))
  }

  override def convertCachedBatchToInternalRow(input: RDD[CachedBatch], cached: Seq[Attribute],
      selected: Seq[Attribute], conf: SQLConf): RDD[InternalRow] = {
    SparkNvcompSerializer.requireSupported(cached.toStructType)
    CpuCachePath.requireCodec(codec)
    val indices = selected.map { attribute =>
      val index = cached.indexWhere(_.exprId == attribute.exprId)
      require(index >= 0, "Selected attribute is missing from the nvCOMP cache")
      index
    }.toArray
    val selectedCodec = codec
    val types = cached.toStructType
    input.mapPartitions(records => CpuCachePath.decodeRows(records, types, indices, selectedCodec))
  }

  override def convertCachedBatchToColumnarBatch(input: RDD[CachedBatch], cached: Seq[Attribute],
      selected: Seq[Attribute], conf: SQLConf): RDD[ColumnarBatch] =
    throw SparkNvcompSerializer.cpuUnsupported("CPU columnar decoding")

  override def convertColumnarBatchToCachedBatch(input: RDD[ColumnarBatch],
      schema: Seq[Attribute], storageLevel: StorageLevel, conf: SQLConf): RDD[CachedBatch] = {
    SparkNvcompSerializer.requireSupported(schema.toStructType)
    val selectedCodec = codec
    val types = schema.toStructType
    input.mapPartitions { batches =>
      if (!batches.hasNext) Iterator.empty
      else {
        val first = batches.next()
        val all = Iterator.single(first) ++ batches
        if (first.numCols() > 0 && first.column(0).isInstanceOf[GpuColumnVector]) {
          new SparkNvcompSerializer.Encoder(all, selectedCodec)
        } else {
          CpuCachePath.encodeRows(all.flatMap(_.rowIterator().asScala), types, selectedCodec)
        }
      }
    }
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
  private[cacheexploration] val RecordBytes = 8L * 1024 * 1024
  private val GroupBytes = 256L * 1024 * 1024
  // Leave space for codec expansion and wire framing; the actual heap group is checked as well.
  private val RawGroupBytes = GroupBytes - RecordBytes
  private[cacheexploration] val ChunkBytes = 64L * 1024
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
  private val cpuSelectedEncodedBytes = new AtomicLong
  private val cpuSelectedDecodedBytes = new AtomicLong
  private val cpuSelectedColumns = new AtomicLong
  private val gpuSelectedEncodedBytes = new AtomicLong
  private val gpuSelectedDecodedBytes = new AtomicLong
  private val gpuSelectedColumns = new AtomicLong

  private[cacheexploration] def recordCpuRead(columns: Array[ParsedColumn]): Unit = {
    cpuSelectedEncodedBytes.addAndGet(columns.map(_.payloadLengths.map(_.toLong).sum).sum)
    cpuSelectedDecodedBytes.addAndGet(columns.map(_.packedBytes).sum)
    cpuSelectedColumns.addAndGet(columns.length)
  }

  /** Process-local observations, cumulative across tasks; these are not Spark storage statistics. */
  def snapshot(): Array[Long] = Array(encodedGroups.get(), decodedGroups.get(),
    nativeEncodeCalls.get(), nativeDecodeCalls.get(), concatenations.get(),
    outputBatches.get(), outputRows.get(), outputDeviceBytes.get(), maxOutputDeviceBytes.get(),
    cpuSelectedEncodedBytes.get(), cpuSelectedDecodedBytes.get(), cpuSelectedColumns.get(),
    gpuSelectedEncodedBytes.get(), gpuSelectedDecodedBytes.get(), gpuSelectedColumns.get())

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

  private[cacheexploration] final case class ParsedColumn(metadata: Array[Byte],
      packedBytes: Long, offsets: Array[Long], lengths: Array[Long], bits: Array[Int],
      payloadOffsets: Array[Int], payloadLengths: Array[Int])

  private[cacheexploration] final case class Parsed(rows: Int, columns: Array[ParsedColumn],
      bytes: Array[Byte]) {
    def project(indices: Array[Int]): Projected = {
      require(indices.forall(i => i >= 0 && i < columns.length), "Invalid cached column ordinal")
      val selected = indices.map(columns)
      var base = 0L
      val offsets = selected.flatMap { column =>
        val result = column.offsets.map(_ + base)
        base = Math.addExact(base, column.packedBytes)
        result
      }
      val metadata = HostPackedBridge.project(selected.map(_.metadata), selected.map(_.packedBytes))
      Projected(rows, base, metadata, offsets, selected.flatMap(_.lengths),
        selected.flatMap(_.bits), selected.flatMap(_.payloadOffsets),
        selected.flatMap(_.payloadLengths), bytes)
    }
  }

  private[cacheexploration] final case class Projected(rows: Int, packedBytes: Long, metadata: Array[Byte],
      offsets: Array[Long], lengths: Array[Long], bits: Array[Int],
      payloadOffsets: Array[Int], payloadLengths: Array[Int], bytes: Array[Byte]) {
    def layout: Array[PackedSegments.Segment] = PackedTableCodec.layout(offsets, lengths, bits)
  }

  private[cacheexploration] def parse(raw: CachedBatch, codec: String): Parsed = {
    val record = raw match {
      case value: NvcompCacheRecord => value
      case _ => throw new IllegalArgumentException("Unexpected nvCOMP cached record type")
    }
    require(record.bytes.length >= HeaderBytes && record.bytes.length.toLong <= GroupBytes,
      "Invalid nvCOMP cache record byte length")
    val in = ByteBuffer.wrap(record.bytes).order(ByteOrder.LITTLE_ENDIAN)
    require(in.getInt() == Magic && in.getInt() == 2 && in.getInt() == codecId(codec),
      "nvCOMP cache format/codec mismatch")
    val rows = in.getInt()
    require(rows >= 0 && rows == record.numRows && in.getLong() == ChunkBytes,
      "nvCOMP cache row count/chunk mismatch")
    val packed = in.getLong()
    val count = in.getInt()
    require(in.getInt() == 0 && packed >= 0 && packed <= RecordBytes && count >= 0 &&
      count.toLong * 16 <= in.remaining(), "Invalid nvCOMP column directory")
    var totalRaw = 0L
    var totalPayload = 0L
    val columns = Array.fill(count) {
      require(in.remaining() >= 16, "Truncated nvCOMP column header")
      val rawBytes = in.getLong()
      val metadataLength = in.getInt()
      val segments = in.getInt()
      require(rawBytes > 0 && rawBytes <= RecordBytes && metadataLength > 0 && segments > 0 &&
        metadataLength.toLong + segments.toLong * DescriptorBytes <= in.remaining(),
        "Invalid nvCOMP column lengths")
      val metadata = new Array[Byte](metadataLength)
      in.get(metadata)
      val offsets = new Array[Long](segments)
      val lengths = new Array[Long](segments)
      val bits = new Array[Int](segments)
      val sizes = new Array[Int](segments)
      var covered = 0L
      (0 until segments).foreach { i =>
        offsets(i) = in.getLong()
        lengths(i) = in.getLong()
        bits(i) = in.getInt()
        require(in.getInt() == 0, "Invalid nvCOMP descriptor flags")
        val encoded = in.getLong()
        require(offsets(i) == covered && offsets(i) % 8 == 0 && lengths(i) > 0 &&
          Set(8, 32, 64).contains(bits(i)) && lengths(i) % (bits(i) / 8) == 0 &&
          encoded > 0 && encoded <= Int.MaxValue, "Invalid nvCOMP column segment")
        covered = Math.addExact(covered, lengths(i))
        totalPayload = Math.addExact(totalPayload, encoded)
        sizes(i) = encoded.toInt
      }
      require(covered == rawBytes, "Incomplete decoded column")
      totalRaw = Math.addExact(totalRaw, rawBytes)
      ParsedColumn(metadata, rawBytes, offsets, lengths, bits, new Array[Int](segments), sizes)
    }
    require(totalRaw == packed && in.position().toLong + totalPayload == record.bytes.length,
      "Incomplete nvCOMP cache payload")
    var start = in.position()
    columns.foreach { column =>
      column.payloadLengths.indices.foreach { i =>
        column.payloadOffsets(i) = start
        start = Math.addExact(start, column.payloadLengths(i))
      }
    }
    Parsed(rows, columns, record.bytes)
  }

  private final case class WireColumn(metadata: Array[Byte], packedBytes: Long,
      layout: Array[PackedSegments.Segment], payloadLengths: Array[Long])

  private final case class Frame(record: NvcompCacheRecord, payloadOffsets: Array[Int])

  private def frameSize(columns: Array[WireColumn]): Long =
    HeaderBytes.toLong + columns.map { column =>
      16L + column.metadata.length + column.layout.length.toLong * DescriptorBytes +
        column.payloadLengths.sum
    }.sum

  private def frame(rows: Int, columns: Array[WireColumn], codec: String): Frame = {
    val size = frameSize(columns)
    require(size <= GroupBytes && size <= Int.MaxValue, "Encoded cache record exceeds heap bound")
    val bytes = new Array[Byte](size.toInt)
    val out = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
    out.putInt(Magic).putInt(2).putInt(codecId(codec)).putInt(rows).putLong(ChunkBytes)
      .putLong(columns.map(_.packedBytes).sum).putInt(columns.length).putInt(0)
    columns.foreach { column =>
      require(column.layout.length == column.payloadLengths.length, "Column segment count")
      out.putLong(column.packedBytes).putInt(column.metadata.length).putInt(column.layout.length)
      out.put(column.metadata)
      column.layout.indices.foreach { i =>
        val segment = column.layout(i)
        out.putLong(segment.offset).putLong(segment.length).putInt(segment.typeBits).putInt(0)
          .putLong(column.payloadLengths(i))
      }
    }
    val starts = columns.flatMap(_.payloadLengths).map { length =>
      val start = out.position()
      out.position(Math.addExact(start, Math.toIntExact(length)))
      start
    }
    require(out.position() == bytes.length, "Encoded cache framing length")
    Frame(NvcompCacheRecord(rows, bytes), starts)
  }

  private final class StreamDrain(stream: Cuda.Stream) extends AutoCloseable {
    override def close(): Unit = stream.sync()
  }

  private def wireGroup(tables: Seq[PackedRecord], encoded: Array[PackedTableCodec.EncodedTable],
      codec: String, stream: Cuda.Stream): Array[NvcompCacheRecord] = {
    var next = 0
    val payloadGroups = new ArrayBuffer[Array[DeviceMemoryBuffer]]()
    val columnGroups = tables.map { table =>
      val columns = encoded.slice(next, next + table.metadata.length)
      require(columns.length == table.metadata.length, "Encoded column count")
      next += columns.length
      payloadGroups += columns.flatMap(_.payloads)
      columns.map { column =>
        WireColumn(column.metadata, column.packedBytes, column.segments,
          column.payloads.map(_.getLength))
      }
    }
    // Include the column directory before allocating any heap payload arrays.
    require(next == encoded.length && columnGroups.map(frameSize).sum <= GroupBytes,
      "Encoded nvCOMP group exceeds heap bound")
    val frames = tables.zip(columnGroups).map { case (table, columns) =>
      frame(table.rows, columns, codec)
    }
    val payloadBytes = payloadGroups.flatten.map(_.getLength).sum
    withResource(DefaultHostMemoryAllocator.get().allocate(payloadBytes, true)) { host =>
      // Keep staging storage and device owners alive until every enqueued copy has drained.
      withResource(new StreamDrain(stream)) { _ =>
        var offset = 0L
        payloadGroups.flatten.foreach { payload =>
          withResource(host.slice(offset, payload.getLength)) { slice =>
            slice.copyFromDeviceBufferAsync(payload, stream)
          }
          offset += payload.getLength
        }
      }
      var offset = 0L
      frames.zip(payloadGroups).foreach { case (destination, payloads) =>
        payloads.zip(destination.payloadOffsets).foreach { case (payload, position) =>
          host.getBytes(destination.record.bytes, position, offset, Math.toIntExact(payload.getLength))
          offset += payload.getLength
        }
      }
    }
    frames.map(_.record).toArray
  }

  private[cacheexploration] final case class CpuColumn(metadata: Array[Byte],
      packedBytes: Long, payload: Array[Byte])

  private[cacheexploration] def cpuWire(rows: Int, columns: Array[CpuColumn],
      codec: String): NvcompCacheRecord = {
    val result = frame(rows, columns.map { column =>
      WireColumn(column.metadata, column.packedBytes,
        PackedTableCodec.layout(Array(0L), Array(column.packedBytes), Array(8)),
        Array(column.payload.length.toLong))
    }, codec)
    columns.zip(result.payloadOffsets).foreach { case (column, offset) =>
      System.arraycopy(column.payload, 0, result.record.bytes, offset, column.payload.length)
    }
    result.record
  }

  private final case class PackedRecord(rows: Int, metadata: Array[Array[Byte]],
      spans: Array[Long], layout: Array[Array[PackedSegments.Segment]],
      buffer: SpillableBuffer) extends AutoCloseable {
    override def close(): Unit = withResource(buffer) { _ => () }
  }

  private def columnLayout(source: Array[PackedSegments.Segment], start: Long,
      length: Long): Array[PackedSegments.Segment] = {
    val parts = source.flatMap { segment =>
      val begin = math.max(start, segment.offset)
      val end = math.min(start + length, segment.offset + segment.length)
      if (end > begin) Some((begin - start, end - begin, segment.typeBits)) else None
    }
    PackedTableCodec.layout(parts.map(_._1), parts.map(_._2), parts.map(_._3))
  }

  private final class Encoder(input: Iterator[ColumnarBatch], codec: String)
      extends Iterator[CachedBatch] with AutoCloseable {
    private var parent: SpillableColumnarBatch = null
    private val packedRecords = new ArrayDeque[PackedRecord]()
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
        rowTarget = math.max(1, math.min(Int.MaxValue.toLong,
          RecordBytes * math.max(1, parent.numRows()) / math.max(1L, parent.sizeInBytes)).toInt)
        if (parent.numRows() == 0) { withResource(parent) { _ => () }; parent = null }
      }
      parent != null
    }

    private def packParent(): Unit = {
      var finished = false
      while (!finished) {
        val result = withRetryNoSplit[Option[Array[PackedRecord]]] {
          withResource(parent.getColumnarBatch()) { batch =>
            withResource(GpuColumnVector.from(batch)) { table =>
              val indices = Iterator.iterate(rowTarget.toLong)(_ + rowTarget)
                .takeWhile(_ < batch.numRows()).map(_.toInt).toArray
              withResource(new NvtxRange("nvcomp-cache-pack-splits", NvtxColor.CYAN)) { _ =>
                withResource(CloseableArray.wrap(table.contiguousSplit(indices: _*))) { packed =>
                  if ((0 until packed.size()).exists(i =>
                      packed.get(i).getBuffer.getLength > RecordBytes)) {
                    None
                  } else {
                    closeOnExcept(new ArrayBuffer[PackedRecord]()) { records =>
                      (0 until packed.size()).foreach { i =>
                        val part = packed.get(i)
                        val source = part.getMetadataDirectBuffer.duplicate()
                        val metadata = new Array[Byte](source.remaining())
                        source.get(metadata)
                        val layout = PackedSegments.describe(part, codec == "cascaded-typed")
                        val buffer = part.getBuffer
                        val index = HostPackedBridge.indexDevice(metadata,
                          buffer.getAddress, buffer.getLength)
                        val spans = index(0).asInstanceOf[Array[Long]]
                        val columnMetadata = index(1).asInstanceOf[Array[Array[Byte]]]
                        require(spans.length == columnMetadata.length * 2, "Packed column index")
                        val layouts = columnMetadata.indices.map { column =>
                          columnLayout(layout, spans(column * 2), spans(column * 2 + 1))
                        }.toArray
                        buffer.incRefCount()
                        val spillable = closeOnExcept(buffer) { owned =>
                          SpillableBuffer(owned, SpillPriorities.ACTIVE_BATCHING_PRIORITY)
                        }
                        closeOnExcept(spillable) { owned =>
                          records += PackedRecord(part.getRowCount.toInt,
                            columnMetadata, spans, layouts, owned)
                        }
                      }
                      Some(records.toArray)
                    }
                  }
                }
              }
            }
          }
        }
        result match {
          case Some(records) =>
            records.foreach(packedRecords.addLast)
            val owned = parent
            parent = null
            withResource(owned) { _ => () }
            finished = true
          case None =>
            require(rowTarget > 1,
              "One input row exceeds the experimental 8MiB packed cache-record bound")
            rowTarget = math.max(1, rowTarget / 2)
        }
      }
    }

    private def nextPacked(): Boolean = {
      if (packedRecords.isEmpty && nextParent()) packParent()
      !packedRecords.isEmpty
    }

    private def fill(): Unit = {
      GpuSemaphore.acquireIfNecessary(TaskContext.get())
      withResource(new ArrayBuffer[PackedRecord]()) { tables =>
        var bytes = 0L
        while (bytes + RecordBytes <= RawGroupBytes && nextPacked()) {
          val table = packedRecords.removeFirst()
          tables += table
          bytes += table.buffer.sizeInBytes
        }
        if (tables.nonEmpty) {
          val records = withRetryNoSplit {
            withResource(new ArrayBuffer[DeviceMemoryBuffer]()) { owners =>
              withResource(new ArrayBuffer[DeviceMemoryBuffer]()) { inputs =>
                tables.foreach { table =>
                  val parentBuffer = table.buffer.getDeviceBuffer()
                  owners += parentBuffer
                  table.metadata.indices.foreach { column =>
                    inputs += parentBuffer.slice(table.spans(column * 2), table.spans(column * 2 + 1))
                  }
                }
                withResource(PackedTableCodec.compress(inputs.toArray,
                    tables.flatMap(_.metadata).toArray, tables.flatMap(_.layout).toArray,
                    codec, ChunkBytes, Cuda.DEFAULT_STREAM)) { compressed =>
                  val result = wireGroup(tables.toSeq, compressed.tables, codec, Cuda.DEFAULT_STREAM)
                  encodedGroups.incrementAndGet()
                  nativeEncodeCalls.addAndGet(compressed.calls.length)
                  result
                }
              }
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
      withResource(new ArrayBuffer[PackedRecord]()) { owned =>
        while (!packedRecords.isEmpty) owned += packedRecords.removeFirst()
      }
      val owned = parent
      parent = null
      if (owned != null) withResource(owned) { _ => () }
    }
  }

  private final class Decoder(input: Iterator[CachedBatch], codec: String,
      indices: Array[Int], types: Array[DataType], targetBytes: Long)
      extends Iterator[ColumnarBatch] with AutoCloseable {
    private val records = input.buffered
    private val selected = indices.distinct
    private val projection = indices.map(selected.indexOf(_))
    private val pending = new ArrayDeque[SpillableColumnarBatch]()
    private var closed = false
    Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => close()))

    private def fill(): Unit = {
      val group = new ArrayBuffer[Projected]()
      var bytes = 0L
      var rows = 0L
      var accepting = true
      while (accepting && records.hasNext) {
        val record = parse(records.head, codec).project(selected)
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
              val payloadBytes = group.map(_.payloadLengths.map(_.toLong).sum).sum
              val inputBuffers = withResource(
                  DefaultHostMemoryAllocator.get().allocate(payloadBytes, true)) { host =>
                withResource(new StreamDrain(Cuda.DEFAULT_STREAM)) { _ =>
                  var offset = 0L
                  group.map { record =>
                    record.payloadLengths.indices.map { i =>
                      val length = record.payloadLengths(i)
                      host.setBytes(offset, record.bytes, record.payloadOffsets(i), length)
                      val buffer = DeviceMemoryBuffer.allocate(length, Cuda.DEFAULT_STREAM)
                      uploads += buffer
                      buffer.copyFromHostBufferAsync(0, host, offset, length, Cuda.DEFAULT_STREAM)
                      offset += length
                      buffer
                    }.toArray
                  }.toArray
                }
              }
              withResource(PackedTableCodec.decompress(inputBuffers, group.map(_.layout).toArray,
                  group.map(_.packedBytes).toArray, codec, ChunkBytes, targetBytes,
                  Cuda.DEFAULT_STREAM)) { batch =>
                successfulCalls = batch.calls.length
                group.indices.foreach { i =>
                  withResource(Table.fromPackedTable(ByteBuffer.wrap(group(i).metadata),
                      batch.packedOutputs(i))) { table =>
                    require(table.getRowCount == group(i).rows, "nvCOMP decoded row count mismatch")
                    withResource(new Table(projection.map(table.getColumn): _*)) { projected =>
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
        gpuSelectedEncodedBytes.addAndGet(group.map(_.payloadLengths.map(_.toLong).sum).sum)
        gpuSelectedDecodedBytes.addAndGet(group.map(_.packedBytes).sum)
        gpuSelectedColumns.addAndGet(group.length.toLong * selected.length)
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
