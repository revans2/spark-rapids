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

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{ColumnVector, CompressionType, HostBufferConsumer, HostMemoryBuffer,
  ParquetMultiFileReader, ParquetOptions, ParquetWriterOptions, Rmm, RmmAllocationMode, Table}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._

/** Compile-only until explicitly launched: GPU-writer files stay in memory throughout the gate. */
object ParquetRowGroupFilesGpuCheck {
  private val Rows = 262144
  private val PoolBytes = 15L << 30

  private class EncodedBuffers extends HostBufferConsumer with AutoCloseable {
    val regions = ArrayBuffer.empty[(HostMemoryBuffer, Long)]
    override def handleBuffer(buffer: HostMemoryBuffer, length: Long): Unit = {
      closeOnExcept(buffer) { owned =>
        require(length >= 0 && length <= owned.getLength)
        regions += ((owned, length))
      }
    }
    override def close(): Unit = {
      regions.map(_._1).safeClose()
      regions.clear()
    }
  }

  private def encode(table: Table, compression: CompressionType,
      small: Boolean): EncodedBuffers = {
    closeOnExcept(new EncodedBuffers) { output =>
      val options = ParquetWriterOptions.builder().withNonNullableColumns("id")
        .withNullableColumns("text", "value").withCompressionType(compression)
        .withRowGroupSizeRows(if (small) 65536 else 1000000)
        .withRowGroupSizeBytes(if (small) 8L << 20 else 128L << 20).build()
      withResource(Table.writeParquetChunked(options, output)) { writer =>
        writer.write(table)
      }
      output
    }
  }

  private def verify(table: Table, projected: Boolean): Unit = {
    require(table.getRowCount == Rows && table.getNumberOfColumns == (if (projected) 2 else 3))
    withResource(table.getColumn(0).copyToHost()) { ids =>
      withResource(table.getColumn(1).copyToHost()) { text =>
        (0 until Rows).foreach { row =>
          require(ids.getLong(row) == row.toLong)
          require(text.isNull(row) == (row % 5 == 0))
          if (row % 5 != 0) require(text.getJavaString(row) == "dictionary-" + row % 11)
        }
      }
    }
    if (!projected) {
      withResource(table.getColumn(2).copyToHost()) { values =>
        (0 until Rows).foreach { row =>
          require(values.isNull(row) == (row % 7 == 0))
          if (row % 7 != 0) require(values.getDouble(row) == row * 0.5)
        }
      }
    }
  }

  private def checkFiles(table: Table, compression: CompressionType, small: Boolean): Long = {
    withResource(encode(table, compression, small)) { encoded =>
      val source = ParquetRowGroupFiles.fromHostRegions(encoded.regions.toVector)
      val metadata = ParquetRowGroupFiles.readFooter(source)
      require(metadata.getBlocks.asScala.map(_.getRowCount).sum == Rows)
      if (small) require(metadata.getBlocks.size > 1, "Small row limit must create multiple groups")
      val dictionary = metadata.getBlocks.asScala.exists(_.getColumns.asScala.exists(
        column => column.hasDictionaryPage &&
          column.getFirstDataPageOffset > column.getDictionaryPageOffset))
      require(dictionary, "GPU writer fixture must exercise dictionary page offsets")
      val records = ParquetRowGroupFiles.split(source, 256 * 1024 * 1024).toArray
      require(records.length == metadata.getBlocks.size)
      require(records.map(_.numRows.toLong).sum == Rows)
      withResource(records.safeMap { record =>
        closeOnExcept(HostMemoryBuffer.allocate(record.buffer.length)) { buffer =>
          buffer.setBytes(0, record.buffer, 0, record.buffer.length)
          buffer
        }
      }) { files =>
        withResource(ParquetMultiFileReader.read(ParquetOptions.DEFAULT, files)) { decoded =>
          verify(decoded, projected = false)
        }
        val projection = ParquetOptions.builder().includeColumn("id", "text").build()
        withResource(ParquetMultiFileReader.read(projection, files)) { decoded =>
          verify(decoded, projected = true)
        }
      }
      val fragmentBytes = records.map(_.buffer.length.toLong).sum
      println(s"""ROWGROUP_GPU_FILE {"compression":"$compression","small":$small,""" +
        s""""rows":$Rows,"rowgroups":${records.length},""" +
        s""""original_encoded_bytes":${source.length},""" +
        s""""fragment_encoded_bytes":$fragmentBytes,"dictionary":true,"nulls":true,""" +
        """"projection":true,"encoded_in_memory":true}""")
      source.length
    }
  }

  def main(args: Array[String]): Unit = {
    require(args.isEmpty)
    Rmm.initialize(RmmAllocationMode.POOL, null, PoolBytes)
    try {
      val baseline = Rmm.getTotalBytesAllocated
      val ids = Array.tabulate[Long](Rows)(_.toLong)
      val text = Array.tabulate[String](Rows)(row =>
        if (row % 5 == 0) null else "dictionary-" + row % 11)
      val values = Array.tabulate[java.lang.Double](Rows)(row =>
        if (row % 7 == 0) null else java.lang.Double.valueOf(row * 0.5))
      withResource(ColumnVector.fromLongs(ids: _*)) { idColumn =>
        withResource(ColumnVector.fromStrings(text: _*)) { textColumn =>
          withResource(ColumnVector.fromBoxedDoubles(values: _*)) { valueColumn =>
            withResource(new Table(idColumn, textColumn, valueColumn)) { table =>
              Seq(CompressionType.NONE, CompressionType.SNAPPY).foreach { compression =>
                val before = Rmm.getTotalBytesAllocated
                val defaultBytes = checkFiles(table, compression, small = false)
                require(Rmm.getTotalBytesAllocated == before, "Default decode leaked device bytes")
                val smallBytes = checkFiles(table, compression, small = true)
                require(Rmm.getTotalBytesAllocated == before, "Small decode leaked device bytes")
                println(s"""ROWGROUP_GPU_RATIO {"compression":"$compression",""" +
                  s""""default_encoded_bytes":$defaultBytes,"small_encoded_bytes":$smallBytes,""" +
                  s""""small_to_default_ratio":${smallBytes.toDouble / defaultBytes}}""")
              }
            }
          }
        }
      }
      require(Rmm.getTotalBytesAllocated == baseline, "Gate leaked device bytes")
      println("ROWGROUP_GPU_CHECK PASS")
    } finally {
      Rmm.shutdown()
    }
  }
}
