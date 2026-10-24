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

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{ColumnVector, CompressionType, HostBufferConsumer, HostMemoryBuffer,
  ParquetChunkedReader => CudfParquetChunkedReader, ParquetOptions, ParquetWriterOptions, Table}
import com.nvidia.spark.rapids.{RapidsConf, SparkQueryCompareTestSuite, SpillableHostBuffer,
  SpillPriorities}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.jni.{CpuRetryOOM, RmmSpark}
import com.nvidia.spark.rapids.spill.SpillFramework

import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

class ParquetCachedBatchFilesGpuSuite extends SparkQueryCompareTestSuite {
  private val rows = 32768
  private val schema = new StructType().add("id", LongType).add("text", StringType)

  private class EncodedOutput extends HostBufferConsumer with AutoCloseable {
    val regions = ArrayBuffer.empty[(SpillableHostBuffer, Long)]
    override def handleBuffer(buffer: HostMemoryBuffer, used: Long): Unit = {
      val region = SpillableHostBuffer(buffer, used, SpillPriorities.ACTIVE_BATCHING_PRIORITY)
      closeOnExcept(region) { owned => regions += ((owned, used)) }
    }
    override def close(): Unit = {
      regions.map(_._1).safeClose()
      regions.clear()
    }
  }

  private def check(table: Table, start: Int, projected: Boolean): Int = {
    assert(table.getNumberOfColumns == (if (projected) 1 else 2))
    if (!projected) {
      withResource(table.getColumn(0).copyToHost()) { ids =>
        (0 until table.getRowCount.toInt).foreach { index =>
          assert(ids.getLong(index) == (start + index).toLong)
        }
      }
    }
    withResource(table.getColumn(if (projected) 0 else 1).copyToHost()) { text =>
      (0 until table.getRowCount.toInt).foreach { index =>
        val row = start + index
        assert(text.isNull(index) == (row % 5 == 0))
        if (row % 5 != 0) assert(text.getJavaString(index) == "dictionary-" + row % 11)
      }
    }
    start + table.getRowCount.toInt
  }

  Seq(CompressionType.NONE, CompressionType.SNAPPY).foreach { compression =>
    test(s"GPU writer regions spill and logical column views decode with $compression") {
      withGpuSparkSession { _ =>
        val handles = SpillFramework.stores.hostStore.numHandles
        val ids = Array.tabulate[Long](rows)(_.toLong)
        val text = Array.tabulate[String](rows) { row =>
          if (row % 5 == 0) null else "dictionary-" + row % 11
        }
        withResource(ColumnVector.fromLongs(ids: _*)) { idColumn =>
          withResource(ColumnVector.fromStrings(text: _*)) { textColumn =>
            withResource(new Table(idColumn, textColumn)) { table =>
              withResource(new EncodedOutput) { encoded =>
                val options = ParquetWriterOptions.builder().withNonNullableColumns("id")
                  .withNullableColumns("text").withCompressionType(compression)
                  .withRowGroupSizeRows(1024).withRowGroupSizeBytes(8192).build()
                withResource(Table.writeParquetChunked(options, encoded)) { writer =>
                  writer.write(table)
                }
                assert(encoded.regions.nonEmpty)
                // Completed callbacks are no longer pinned by the writer or this test.
                assert(SpillFramework.stores.hostStore.spill(Long.MaxValue) > 0)
                val source = ParquetCachedBatchFiles.fromSpillableRegions(encoded.regions.toVector)
                val records = ParquetCachedBatchFiles.split(source).toVector
                assert(records.size > 1)
                assert(records.map(_.numRows.toLong).sum == rows)
                withResource(records.safeMap(record =>
                    ParquetCachedBatchFiles.stage(record, schema))) { staged =>
                  // Exercise rematerialization of the input records as well as writer regions.
                  assert(SpillFramework.stores.hostStore.spill(Long.MaxValue) > 0)
                  withResource(ParquetCachedBatchFiles.logicalFile(staged)) { logical =>
                    assert(logical.numRows == rows)
                    withResource(Table.readParquet(ParquetOptions.DEFAULT, logical.buffers: _*)) {
                      result => assert(check(result, 0, projected = false) == rows)
                    }
                    val projection = ParquetOptions.builder().includeColumn("text").build()
                    withResource(new CudfParquetChunkedReader(
                        64 * 1024, 0, projection, logical.buffers: _*)) { reader =>
                      var nextRow = 0
                      var chunks = 0
                      while (reader.hasNext) {
                        withResource(reader.readChunk()) { result =>
                          nextRow = check(result, nextRow, projected = true)
                        }
                        chunks += 1
                      }
                      assert(nextRow == rows)
                      assert(chunks > 1)
                    }
                    logical.close()
                  }
                  // logicalFile borrows these handles rather than retiring the staged records.
                  staged.foreach { file =>
                    withResource(file.getHostBuffer()) { buffer =>
                      assert(buffer.getLength == file.info.encodedBytes)
                    }
                  }
                }
              }
            }
          }
        }
        assert(SpillFramework.stores.hostStore.numHandles == handles)
      }
    }
  }

  test("cache writer byte target does not impose a small row limit on narrow data") {
    withGpuSparkSession { _ =>
      val handles = SpillFramework.stores.hostStore.numHandles
      val count = 200000
      val narrowSchema = new StructType().add("id", IntegerType, nullable = false)
      val options = new ParquetCachedBatchSerializer()
        .getParquetWriterOptions(useCompression = false, schema = narrowSchema,
          rowGroupSizeBytes = new RapidsConf(Map.empty[String, String]).parquetCacheRowGroupSizeBytes)
      assert(options.getRowGroupSizeRows == ParquetWriterOptions.builder().build()
        .getRowGroupSizeRows)
      withResource(ColumnVector.fromInts((0 until count).toArray: _*)) { ids =>
        withResource(new Table(ids)) { table =>
          withResource(new EncodedOutput) { encoded =>
            withResource(Table.writeParquetChunked(options, encoded))(_.write(table))
            val source = ParquetCachedBatchFiles.fromSpillableRegions(encoded.regions.toVector)
            val records = ParquetCachedBatchFiles.split(source).toVector
            assert(records.size == 1)
            assert(records.head.numRows == count)
            assert(records.head.numRows > 65536)
            withResource(records.safeMap(ParquetCachedBatchFiles.stage(_, narrowSchema))) {
              staged =>
                withResource(ParquetCachedBatchFiles.logicalFile(staged)) { logical =>
                  withResource(Table.readParquet(ParquetOptions.DEFAULT, logical.buffers: _*)) {
                    decoded =>
                      assert(decoded.getRowCount == count)
                      assert(decoded.getNumberOfColumns == 1)
                      withResource(decoded.getColumn(0).copyToHost()) { host =>
                        (0 until count).foreach(row => assert(host.getInt(row) == row))
                      }
                  }
                }
            }
          }
        }
      }
      assert(SpillFramework.stores.hostStore.numHandles == handles)
    }
  }

  test("a later-parent retry releases earlier logical-file references before retrying") {
    withGpuSparkSession { _ =>
      val initialHandles = SpillFramework.stores.hostStore.numHandles
      val record = withResource(new Table.TestBuilder()
          .column(Array[java.lang.Long](0L, 1L, 2L, 3L): _*)
          .column("a", null, "b", "c").build()) { table =>
        withResource(new EncodedOutput) { encoded =>
          val options = ParquetWriterOptions.builder().withNonNullableColumns("id")
            .withNullableColumns("text").withCompressionType(CompressionType.SNAPPY).build()
          withResource(Table.writeParquetChunked(options, encoded))(_.write(table))
          val source = ParquetCachedBatchFiles.fromSpillableRegions(encoded.regions.toVector)
          val bytes = new Array[Byte](source.length.toInt)
          source.readFully(0, bytes, 0, bytes.length)
          ParquetCachedBatch(4, bytes)
        }
      }
      withResource(Vector.fill(2)(record).safeMap(ParquetCachedBatchFiles.stage(_, schema))) {
        staged =>
        var firstParent: HostMemoryBuffer = null
        var firstAttempts = 0
        var injected = false
        var releasedBeforeRetry = false
        RmmSpark.currentThreadIsDedicatedToTask(1)
        try {
          val materialize: ParquetCachedBatchFiles.StagedFile => HostMemoryBuffer = file => {
            if (file eq staged.head) {
              firstAttempts += 1
              if (firstAttempts > 1) {
                // Only the staged handle owns its resident buffer between attempts.
                assert(firstParent.getRefCount == 1)
                releasedBeforeRetry = true
              }
              val parent = file.getHostBuffer()
              firstParent = parent
              parent
            } else if (!injected) {
              assert(firstParent.getRefCount > 1, "The first parent must already be borrowed")
              injected = true
              throw new CpuRetryOOM("later logical-file parent")
            } else {
              file.getHostBuffer()
            }
          }
          withResource(ParquetCachedBatchFiles.logicalFile(staged, materialize)) { logical =>
            assert(injected && releasedBeforeRetry && firstAttempts == 2)
            withResource(Table.readParquet(ParquetOptions.DEFAULT, logical.buffers: _*)) { table =>
              assert(table.getRowCount == 8)
              withResource(table.getColumn(0).copyToHost()) { ids =>
                assert((0 until 8).map(ids.getLong(_)) == Seq(0L, 1L, 2L, 3L, 0L, 1L, 2L, 3L))
              }
              withResource(table.getColumn(1).copyToHost()) { text =>
                val expected = Seq("a", null, "b", "c", "a", null, "b", "c")
                expected.indices.foreach { index =>
                  assert(text.isNull(index) == (expected(index) == null))
                  if (expected(index) != null) assert(text.getJavaString(index) == expected(index))
                }
              }
            }
          }
          assert(firstParent.getRefCount == 1)
        } finally {
          RmmSpark.removeAllCurrentThreadAssociation()
        }
      }
      assert(SpillFramework.stores.hostStore.numHandles == initialHandles)
    }
  }

}
