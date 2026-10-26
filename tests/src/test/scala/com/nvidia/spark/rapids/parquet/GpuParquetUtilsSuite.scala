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

import java.util.Collections

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.GpuMetric
import com.nvidia.spark.rapids.jni.fileio.RapidsFileIO
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.column.Encoding
import org.apache.parquet.column.statistics.Statistics
import org.apache.parquet.hadoop.metadata.{BlockMetaData, ColumnChunkMetaData, ColumnPath, CompressionCodecName}
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.scalatest.funsuite.AnyFunSuite

class GpuParquetUtilsSuite extends AnyFunSuite {
  private object ScanMetadata extends ParquetPartitionReaderBase {
    override def fileIO: RapidsFileIO = throw new UnsupportedOperationException
    override def conf: Configuration = new Configuration(false)
    override def execMetrics: Map[String, GpuMetric] = Map.empty
    override def isSchemaCaseSensitive: Boolean = true
    override def compressCfg: CpuCompressionConfig = CpuCompressionConfig.disabled()

    def relocate(blocks: Seq[BlockMetaData], start: Long): Seq[BlockMetaData] =
      computeBlockMetaData(blocks, start)
  }

  private def column(name: String, data: Long, dictionary: Long,
      bytes: Long): ColumnChunkMetaData = {
    val primitive = new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT64, name)
    val encoding = if (dictionary > 0) Encoding.RLE_DICTIONARY else Encoding.PLAIN
    ColumnChunkMetaData.get(ColumnPath.get(name), primitive, CompressionCodecName.UNCOMPRESSED,
      null, Collections.singleton(encoding), Statistics.createStats(primitive),
      data, dictionary, 7L, bytes, bytes * 2)
  }

  private def block(columns: ColumnChunkMetaData*): BlockMetaData = {
    val result = new BlockMetaData
    result.setRowCount(7L)
    columns.foreach(result.addColumn)
    result.setTotalByteSize(columns.map(_.getTotalUncompressedSize).sum)
    result
  }

  test("scan relocation preserves separate dictionary and data offsets across blocks") {
    val dictionary = column("dictionary", 120L, 100L, 80L)
    val plain = column("plain", 500L, 0L, 40L)
    val other = column("other", 950L, 900L, 100L)
    val originals = Seq(block(dictionary, plain), block(other))
    // Row-index metadata exists in newer Parquet versions; exercise it where available.
    val rowIndexSetter = classOf[BlockMetaData].getMethods.find(_.getName == "setRowIndexOffset")
    rowIndexSetter.foreach { setter =>
      originals.zipWithIndex.foreach { case (metadata, index) =>
        setter.invoke(metadata, Long.box(index * 7L))
      }
    }
    val relocated = ScanMetadata.relocate(originals, 64L)
    val first = relocated.head.getColumns.asScala
    assert(first(0).getDictionaryPageOffset == 64L)
    assert(first(0).getFirstDataPageOffset == 84L)
    assert(first(0).getStartingPos == 64L)
    assert(first(1).getFirstDataPageOffset == 144L)
    assert(!first(1).hasDictionaryPage)
    val second = relocated(1).getColumns.get(0)
    assert(second.getDictionaryPageOffset == 184L)
    assert(second.getFirstDataPageOffset == 234L)
    relocated.zip(originals).foreach { case (output, original) =>
      assert(output.getRowCount == original.getRowCount)
      assert(output.getTotalByteSize == original.getTotalByteSize)
      classOf[BlockMetaData].getMethods.find(_.getName == "getRowIndexOffset").foreach { getter =>
        assert(getter.invoke(output) == getter.invoke(original))
      }
      output.getColumns.asScala.zip(original.getColumns.asScala).foreach { case (out, in) =>
        assert(out.getPrimitiveType == in.getPrimitiveType)
        assert(out.getCodec == in.getCodec)
        assert(out.getEncodings == in.getEncodings)
        assert(out.getStatistics == in.getStatistics)
        assert(out.getValueCount == in.getValueCount)
        assert(out.getTotalSize == in.getTotalSize)
        assert(out.getTotalUncompressedSize == in.getTotalUncompressedSize)
      }
    }
    assert(dictionary.getFirstDataPageOffset == 120L)
    assert(dictionary.getDictionaryPageOffset == 100L)
    assert(plain.getFirstDataPageOffset == 500L)
    assert(other.getDictionaryPageOffset == 900L)
  }

  test("relocation rejects offset overflow rather than wrapping") {
    val original = block(column("plain", 10L, 0L, 32L))
    intercept[ArithmeticException] {
      GpuParquetUtils.relocateBlock(original, Long.MaxValue - 8L)
    }
    assert(original.getColumns.get(0).getFirstDataPageOffset == 10L)
  }
}
