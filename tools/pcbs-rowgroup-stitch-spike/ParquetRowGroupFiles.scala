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

import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.types.StructType

/** Research-only encoded row-group files. Input and output buffers have explicit ownership. */
object ParquetRowGroupFiles {
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

  /** Regions are borrowed, and only their used prefixes participate in the encoded file. */
  def fromHostRegions(regions: Seq[(HostMemoryBuffer, Long)]): EncodedSource = {
    val entries = regions.filter(_._2 != 0).map { case (buffer, used) =>
      require(used > 0 && used <= buffer.getLength, "Invalid writer buffer used length")
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
          entries(index)._1.getBytes(destination, destinationOffset, withinRegion, amount)
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
    ParquetFileReader.readFooter(inputFile(source), ParquetMetadataConverter.NO_FILTER)
  }

  private def footerBytes(metadata: ParquetMetadata, block: BlockMetaData): Array[Byte] = {
    val footer = new ParquetMetadata(metadata.getFileMetaData, Seq(block).asJava)
    val converted = new ParquetMetadataConverter().toParquetMetadata(1, footer)
    val output = new ByteArrayOutputStream()
    org.apache.parquet.format.Util.writeFileMetaData(converted, output)
    output.toByteArray
  }

  /**
   * Adapt the scan's column-range copy/offset rewrite without re-encoding any values.
   * Keep the original file metadata and distinct first-data/dictionary page offsets.
   */
  private def layout(source: EncodedSource, metadata: ParquetMetadata,
      block: BlockMetaData): (BlockMetaData, Vector[(Long, Int)]) = {
    var position = Magic.length.toLong
    val ranges = ArrayBuffer.empty[(Long, Int)]
    val trailer = new Array[Byte](8)
    source.readFully(source.length - 8, trailer, 0, trailer.length)
    val footerLength = ByteBuffer.wrap(trailer, 0, 4)
      .order(java.nio.ByteOrder.LITTLE_ENDIAN).getInt
    require(footerLength > 0 && footerLength.toLong <= source.length - FramingBytes,
      "Invalid Parquet footer length")
    val footerStart = source.length - 8 - footerLength
    val columns = block.getColumns.asScala.map { column =>
      val start = column.getStartingPos
      val bytes = column.getTotalSize
      checkRange(start, bytes, footerStart)
      require(start >= Magic.length && bytes <= Int.MaxValue, "Invalid column chunk extent")
      val adjustment = position - start
      val dataOffset = column.getFirstDataPageOffset + adjustment
      val dictionaryOffset = if (column.hasDictionaryPage) {
        column.getDictionaryPageOffset + adjustment
      } else {
        0L
      }
      require(dataOffset >= position && dataOffset < position + bytes,
        "First data page falls outside its column chunk")
      require(dictionaryOffset == 0 ||
        (dictionaryOffset >= position && dictionaryOffset < position + bytes),
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
   * Each next() copies only one independently valid row-group file into a small Java array.
   * This iterator borrows source storage, does not close it, and never collects all records.
   * maxRecordBytes is a hard guard; the writer's row-group byte target is only a soft limit.
   */
  def split(source: EncodedSource, maxRecordBytes: Int): Iterator[ParquetCachedBatch] = {
    require(maxRecordBytes >= FramingBytes)
    val metadata = readFooter(source)
    metadata.getBlocks.asScala.iterator.map { original =>
      require(original.getRowCount >= 0 && original.getRowCount <= Int.MaxValue,
        "A row group exceeds the cached-record row limit")
      val (rewritten, ranges) = layout(source, metadata, original)
      val footer = footerBytes(metadata, rewritten)
      val bodySize = ranges.foldLeft(0L) { case (size, (_, bytes)) =>
        Math.addExact(size, bytes.toLong)
      }
      val size = Math.addExact(bodySize, FramingBytes + footer.length)
      if (size > maxRecordBytes) {
        throw new IOException(s"Indivisible Parquet row group needs $size bytes; " +
          s"record guard is $maxRecordBytes")
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
      parquetSchema: String, rowGroups: Int)

  /** Parquet uncompressed bytes and schema-based GPU estimates are hints, not memory caps. */
  def describe(batch: ParquetCachedBatch, schema: StructType): FileInfo = {
    val metadata = readFooter(fromBytes(batch.buffer))
    val rows = metadata.getBlocks.asScala.map(_.getRowCount).sum
    require(rows == batch.numRows, "Cached rows differ from Parquet footer")
    val uncompressed = metadata.getBlocks.asScala.map(_.getTotalByteSize).sum
    val gpuEstimate = GpuBatchUtils.estimateGpuMemory(schema, rows)
    FileInfo(batch.numRows, batch.buffer.length, math.max(uncompressed, gpuEstimate),
      metadata.getFileMetaData.getSchema.toString, metadata.getBlocks.size)
  }

  /** Owns its spillable file; every materialized buffer reference belongs to its caller. */
  case class StagedFile(info: FileInfo, buffer: SpillableHostBuffer) extends AutoCloseable {
    override def close(): Unit = buffer.close()
    def getHostBuffer(): HostMemoryBuffer = buffer.getDataHostBuffer()
  }

  /** Caller closes every returned file, including on task interruption or failed decode. */
  def stageNextGroup(input: BufferedIterator[CachedBatch], schema: StructType,
      targetDecodedBytes: Long, maxRecordBytes: Int): Array[StagedFile] = {
    require(targetDecodedBytes > 0 && maxRecordBytes >= FramingBytes)
    closeOnExcept(ArrayBuffer.empty[StagedFile]) { files =>
      var estimatedBytes = 0L
      var expectedSchema: Option[String] = None
      var finished = false
      while (!finished && input.hasNext) {
        val next = input.head match {
          case batch: ParquetCachedBatch => batch
          case other => throw new IllegalArgumentException("Expected ParquetCachedBatch: " + other)
        }
        require(next.buffer.length <= maxRecordBytes, "Encoded record exceeds its hard guard")
        val info = describe(next, schema)
        val sameSchema = expectedSchema.forall(_ == info.parquetSchema)
        val fits = info.estimatedDecodedBytes <= targetDecodedBytes - estimatedBytes
        if (files.nonEmpty && (!sameSchema || !fits)) {
          finished = true
        } else {
          input.next()
          val spillable = withRetryNoSplit[SpillableHostBuffer] {
            closeOnExcept(HostAlloc.alloc(next.buffer.length)) { buffer =>
              buffer.setBytes(0, next.buffer, 0, next.buffer.length)
              SpillableHostBuffer(buffer, next.buffer.length,
                SpillPriorities.ACTIVE_BATCHING_PRIORITY)
            }
          }
          files += StagedFile(info, spillable)
          expectedSchema = Some(info.parquetSchema)
          estimatedBytes = Math.addExact(estimatedBytes, info.estimatedDecodedBytes)
        }
      }
      files.toArray
    }
  }
}
