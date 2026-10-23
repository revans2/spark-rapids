/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nvidia.spark.rapids.parquet

import java.io.{ByteArrayOutputStream, EOFException, IOException}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.HostMemoryBuffer
import com.nvidia.spark.rapids.{GpuBatchUtils, HostAlloc, SpillableHostBuffer, SpillPriorities}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.withRetryNoSplit
import org.apache.parquet.bytes.BytesUtils
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.{BlockMetaData, ColumnChunkMetaData, ParquetMetadata}
import org.apache.parquet.io.{InputFile, SeekableInputStream}
import org.apache.parquet.schema.MessageType

import org.apache.spark.sql.types.StructType

/** Parquet framing over encoded cache data, with explicit ownership of native buffers. */
private[rapids] object ParquetCachedBatchFiles {
  private val Magic = "PAR1".getBytes(StandardCharsets.US_ASCII)
  private val FramingBytes = 12L

  /** Borrowed encoded bytes; callers keep all backing storage alive through iteration. */
  trait EncodedSource {
    def length: Long
    def readFully(offset: Long, destination: Array[Byte], start: Int, count: Int): Unit
  }

  private def checkRange(offset: Long, count: Long, size: Long): Unit = {
    require(offset >= 0 && count >= 0 && offset <= size && count <= size - offset,
      s"Invalid encoded range offset=$offset count=$count size=$size")
  }

  def fromBytes(bytes: Array[Byte]): EncodedSource = new EncodedSource {
    override val length: Long = bytes.length
    override def readFully(offset: Long, destination: Array[Byte], start: Int,
        count: Int): Unit = {
      checkRange(offset, count, length)
      checkRange(start, count, destination.length)
      System.arraycopy(bytes, offset.toInt, destination, start, count)
    }
  }

  /** Borrowed spillable regions; only the region being copied is materialized. */
  def fromSpillableRegions(regions: Seq[(SpillableHostBuffer, Long)]): EncodedSource = {
    val entries = regions.filter(_._2 != 0).map { case (buffer, used) =>
      require(used > 0 && used <= buffer.length, "Invalid spillable region used length")
      (buffer, used)
    }.toVector
    val starts = entries.scanLeft(0L) { case (offset, (_, used)) =>
      Math.addExact(offset, used)
    }
    new EncodedSource {
      override val length: Long = starts.last
      override def readFully(offset: Long, destination: Array[Byte], start: Int,
          count: Int): Unit = {
        checkRange(offset, count, length)
        checkRange(start, count, destination.length)
        var sourceOffset = offset
        var destinationOffset = start
        var remaining = count
        var index = 0
        while (remaining > 0) {
          while (starts(index + 1) <= sourceOffset) {
            index += 1
          }
          val withinRegion = sourceOffset - starts(index)
          val amount = math.min(remaining.toLong, entries(index)._2 - withinRegion).toInt
          withRetryNoSplit {
            withResource(entries(index)._1.getDataHostBuffer()) { buffer =>
              buffer.getBytes(destination, destinationOffset, withinRegion, amount)
            }
          }
          sourceOffset += amount
          destinationOffset += amount
          remaining -= amount
        }
      }
    }
  }

  private def inputFile(source: EncodedSource): InputFile = new InputFile {
    override def getLength: Long = source.length
    override def newStream(): SeekableInputStream = new SeekableInputStream {
      private var position = 0L
      override def getPos: Long = position
      override def seek(offset: Long): Unit = {
        checkRange(offset, 0, source.length)
        position = offset
      }
      override def read(): Int = {
        val one = new Array[Byte](1)
        if (read(one, 0, 1) < 0) -1 else one(0) & 0xff
      }
      override def read(bytes: Array[Byte], start: Int, count: Int): Int = {
        checkRange(start, count, bytes.length)
        if (count == 0) {
          0
        } else if (position == source.length) {
          -1
        } else {
          val amount = math.min(count.toLong, source.length - position).toInt
          source.readFully(position, bytes, start, amount)
          position += amount
          amount
        }
      }
      override def readFully(bytes: Array[Byte]): Unit = readFully(bytes, 0, bytes.length)
      override def readFully(bytes: Array[Byte], start: Int, count: Int): Unit = {
        if (count > source.length - position) {
          throw new EOFException("Truncated encoded Parquet source")
        }
        source.readFully(position, bytes, start, count)
        position += count
      }
      override def read(destination: ByteBuffer): Int = {
        if (!destination.hasRemaining) {
          0
        } else if (position == source.length) {
          -1
        } else {
          val amount = math.min(destination.remaining.toLong, source.length - position).toInt
          val scratch = new Array[Byte](math.min(8192, amount))
          var remaining = amount
          while (remaining > 0) {
            val next = math.min(remaining, scratch.length)
            readFully(scratch, 0, next)
            destination.put(scratch, 0, next)
            remaining -= next
          }
          amount
        }
      }
      override def readFully(destination: ByteBuffer): Unit = {
        if (destination.remaining > source.length - position) {
          throw new EOFException("Truncated encoded Parquet source")
        }
        read(destination)
      }
    }
  }

  @scala.annotation.nowarn("msg=method readFooter in class ParquetFileReader is deprecated")
  def readFooter(source: EncodedSource): ParquetMetadata = {
    require(source.length >= FramingBytes, "Parquet file is smaller than its framing")
    val head = new Array[Byte](4)
    val tail = new Array[Byte](4)
    source.readFully(0, head, 0, head.length)
    source.readFully(source.length - 4, tail, 0, tail.length)
    require(head.sameElements(Magic) && tail.sameElements(Magic),
      "Expected a complete unencrypted Parquet file")
    footerStart(source)
    ParquetFileReader.readFooter(inputFile(source), ParquetMetadataConverter.NO_FILTER)
  }

  private def footerStart(source: EncodedSource): Long = {
    val trailer = new Array[Byte](4)
    source.readFully(source.length - 8, trailer, 0, trailer.length)
    val footerLength = ByteBuffer.wrap(trailer).order(java.nio.ByteOrder.LITTLE_ENDIAN).getInt
    require(footerLength > 0 && footerLength.toLong <= source.length - FramingBytes,
      "Invalid Parquet footer length")
    source.length - 8 - footerLength
  }

  private def footerBytes(metadata: ParquetMetadata, blocks: Seq[BlockMetaData]): Array[Byte] = {
    val footer = new ParquetMetadata(metadata.getFileMetaData, blocks.asJava)
    val converted = new ParquetMetadataConverter().toParquetMetadata(1, footer)
    val output = new ByteArrayOutputStream()
    org.apache.parquet.format.Util.writeFileMetaData(converted, output)
    output.toByteArray
  }

  /**
   * Adapt the scan's column-range copy/offset rewrite without re-encoding any values.
   * Keep the original file metadata and distinct first-data/dictionary page offsets.
   */
  private def layout(block: BlockMetaData, startPosition: Long,
      footerStart: Long): (BlockMetaData, Vector[(Long, Int)]) = {
    var position = startPosition
    val ranges = ArrayBuffer.empty[(Long, Int)]
    val columns = block.getColumns.asScala.map { column =>
      val start = column.getStartingPos
      val bytes = column.getTotalSize
      checkRange(start, bytes, footerStart)
      require(start >= Magic.length && bytes <= Int.MaxValue, "Invalid column chunk extent")
      val adjustment = Math.subtractExact(position, start)
      val end = Math.addExact(position, bytes)
      val dataOffset = Math.addExact(column.getFirstDataPageOffset, adjustment)
      val dictionaryOffset = if (column.hasDictionaryPage) {
        Math.addExact(column.getDictionaryPageOffset, adjustment)
      } else {
        0L
      }
      require(dataOffset >= position && dataOffset < end,
        "First data page falls outside its column chunk")
      require(dictionaryOffset == 0 ||
        (dictionaryOffset >= position && dictionaryOffset < end),
        "Dictionary page falls outside its column chunk")
      val rewritten = ColumnChunkMetaData.get(column.getPath, column.getPrimitiveType,
        column.getCodec, column.getEncodingStats, column.getEncodings, column.getStatistics,
        dataOffset, dictionaryOffset, column.getValueCount, bytes,
        column.getTotalUncompressedSize)
      ranges += ((start, bytes.toInt))
      position = Math.addExact(position, bytes)
      rewritten
    }
    (GpuParquetUtils.newBlockMeta(block, columns.toSeq), ranges.toVector)
  }

  /**
   * Each next() copies only one independently valid row-group file into its own Java array.
   * This iterator borrows source storage, does not close it, and never collects all records.
   * The writer's row-group byte target is soft; records retain the existing Java array limit.
   */
  def split(source: EncodedSource): Iterator[ParquetCachedBatch] = {
    val metadata = readFooter(source)
    metadata.getBlocks.asScala.iterator.map { original =>
      require(original.getRowCount >= 0 && original.getRowCount <= Int.MaxValue,
        "A row group exceeds the cached-record row limit")
      val (rewritten, ranges) = layout(original, Magic.length, footerStart(source))
      val footer = footerBytes(metadata, Seq(rewritten))
      val bodySize = ranges.foldLeft(0L) { case (size, (_, bytes)) =>
        Math.addExact(size, bytes.toLong)
      }
      val size = Math.addExact(bodySize, FramingBytes + footer.length)
      if (size > Int.MaxValue) {
        throw new IOException(s"Indivisible Parquet row group needs $size bytes; " +
          s"maximum Java array length is ${Int.MaxValue}")
      }
      val output = new Array[Byte](size.toInt)
      System.arraycopy(Magic, 0, output, 0, Magic.length)
      var position = Magic.length
      ranges.foreach { case (offset, bytes) =>
        source.readFully(offset, output, position, bytes)
        position += bytes
      }
      System.arraycopy(footer, 0, output, position, footer.length)
      position += footer.length
      val trailer = new ByteArrayOutputStream(8)
      BytesUtils.writeIntLittleEndian(trailer, footer.length)
      trailer.write(Magic)
      System.arraycopy(trailer.toByteArray, 0, output, position, 8)
      ParquetCachedBatch(original.getRowCount.toInt, output)
    }
  }

  case class FileInfo(rows: Int, encodedBytes: Int, estimatedDecodedBytes: Long,
      metadata: ParquetMetadata, footerStart: Long) {
    def parquetSchema: MessageType =
      metadata.getFileMetaData.getSchema
    def rowGroups: Int = metadata.getBlocks.size
  }

  /** Parquet uncompressed bytes and schema-based GPU estimates are hints, not memory caps. */
  def describe(batch: ParquetCachedBatch, schema: StructType): FileInfo = {
    require(batch.numRows >= 0, "Cached row count must be nonnegative")
    val metadata = readFooter(fromBytes(batch.buffer))
    val rows = metadata.getBlocks.asScala.foldLeft(0L) { (total, block) =>
      require(block.getRowCount >= 0, "Parquet row count must be nonnegative")
      Math.addExact(total, block.getRowCount)
    }
    require(rows == batch.numRows, "Cached rows differ from Parquet footer")
    val uncompressed = metadata.getBlocks.asScala.foldLeft(0L) { (total, block) =>
      Math.addExact(total, block.getTotalByteSize)
    }
    val gpuEstimate = GpuBatchUtils.estimateGpuMemory(schema, rows)
    FileInfo(batch.numRows, batch.buffer.length, math.max(uncompressed, gpuEstimate), metadata,
      footerStart(fromBytes(batch.buffer)))
  }

  /** Owns its spillable file; every materialized buffer reference belongs to its caller. */
  final class StagedFile(val info: FileInfo, val buffer: SpillableHostBuffer)
      extends AutoCloseable {
    private var closed = false

    def getHostBuffer(): HostMemoryBuffer = synchronized {
      require(!closed, "Staged Parquet file is closed")
      buffer.getDataHostBuffer()
    }

    override def close(): Unit = synchronized {
      if (!closed) {
        closed = true
        buffer.close()
      }
    }
  }

  def stage(batch: ParquetCachedBatch, info: FileInfo): StagedFile = {
    require(batch.numRows == info.rows && batch.buffer.length == info.encodedBytes,
      "Staged record does not match its metadata")
    val spillable = withRetryNoSplit[SpillableHostBuffer] {
      closeOnExcept(HostAlloc.alloc(batch.buffer.length)) { buffer =>
        buffer.setBytes(0, batch.buffer, 0, batch.buffer.length)
        SpillableHostBuffer(buffer, batch.buffer.length, SpillPriorities.ACTIVE_BATCHING_PRIORITY)
      }
    }
    new StagedFile(info, spillable)
  }

  def stage(batch: ParquetCachedBatch, schema: StructType): StagedFile =
    stage(batch, describe(batch, schema))

  /**
   * One logical Parquet file. Owns acquired references, column slices and framing buffers;
   * the caller still owns the staged files. Keep this open through native reader consumption.
   */
  final class LogicalFile private[ParquetCachedBatchFiles] (
      val buffers: Array[HostMemoryBuffer],
      val schema: MessageType,
      val numRows: Long,
      resources: Seq[HostMemoryBuffer]) extends AutoCloseable {
    private var closed = false

    override def close(): Unit = synchronized {
      if (!closed) {
        closed = true
        resources.safeClose()
      }
    }
  }

  /**
   * Assemble column-chunk views and a rewritten footer without copying the encoded payload.
   * Input files may have multiple row groups. All files must have the same physical schema.
   */
  def logicalFile(files: Seq[StagedFile]): LogicalFile =
    logicalFile(files, _.getHostBuffer())

  private[parquet] def logicalFile(files: Seq[StagedFile],
      materialize: StagedFile => HostMemoryBuffer): LogicalFile = {
    require(files.nonEmpty, "At least one Parquet file is required")
    val schema = files.head.info.parquetSchema
    require(files.forall(_.info.parquetSchema == schema), "Parquet file schemas differ")
    val rows = files.foldLeft(0L)((count, file) => Math.addExact(count, file.info.rows.toLong))
    require(rows <= Int.MaxValue, "Combined cached rows exceed the columnar batch row limit")
    val blocks = ArrayBuffer.empty[BlockMetaData]
    var position = Magic.length.toLong
    val plans = files.map { file =>
      val ranges = file.info.metadata.getBlocks.asScala.flatMap { original =>
        val (rewritten, columnRanges) = layout(original, position, file.info.footerStart)
        columnRanges.foreach { case (_, bytes) =>
          position = Math.addExact(position, bytes.toLong)
        }
        blocks += rewritten
        columnRanges
      }.toVector
      (file, ranges)
    }
    val footer = footerBytes(files.head.info.metadata, blocks.toSeq)
    val trailer = new ByteArrayOutputStream()
    trailer.write(footer)
    BytesUtils.writeIntLittleEndian(trailer, footer.length)
    trailer.write(Magic)
    val footerAndTrailer = trailer.toByteArray
    // A retry must release all earlier parents and slices before waiting for memory.
    withRetryNoSplit[LogicalFile] {
      closeOnExcept(ArrayBuffer.empty[HostMemoryBuffer]) { resources =>
        // Allocate framing before holding any unspillable input references.
        val header = HostAlloc.alloc(Magic.length)
        resources += header
        header.setBytes(0, Magic, 0, Magic.length)
        val tail = HostAlloc.alloc(footerAndTrailer.length)
        resources += tail
        tail.setBytes(0, footerAndTrailer, 0, footerAndTrailer.length)
        val buffers = ArrayBuffer(header)
        plans.foreach { case (file, ranges) =>
          val parent = materialize(file)
          resources += parent
          ranges.foreach { case (offset, bytes) =>
            val slice = parent.slice(offset, bytes)
            resources += slice
            buffers += slice
          }
        }
        buffers += tail
        new LogicalFile(buffers.toArray, schema, rows, resources.toVector)
      }
    }
  }
}
