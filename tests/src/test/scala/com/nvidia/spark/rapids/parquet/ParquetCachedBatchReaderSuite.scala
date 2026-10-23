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

package com.nvidia.spark.rapids.parquet

import java.nio.file.Files

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{ParquetOptions, Rmm, RmmAllocationMode, RmmEventHandler, Table}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsConf
import com.nvidia.spark.rapids.jni.{GpuRetryOOM, RmmSpark}
import com.nvidia.spark.rapids.parquet.ParquetCachedBatchReader.TableProducer
import com.nvidia.spark.rapids.spill.SpillFramework
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.schema.MessageTypeParser
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.rapids.metrics.source.MockTaskContext
import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.apache.spark.util.TaskCompletionListener

class ParquetCachedBatchReaderSuite extends AnyFunSuite with BeforeAndAfterEach {
  private val schema = new StructType().add("id", LongType).add("label", StringType)
  private val parquetSchema = MessageTypeParser.parseMessageType(
    "message cached { required int64 id; optional binary label (UTF8); }")
  private val options = ParquetOptions.builder().includeColumn(Seq("id").asJava).build()
  private var ownsRmm = false

  override def beforeEach(): Unit = {
    super.beforeEach()
    ownsRmm = !Rmm.isInitialized
    if (ownsRmm) {
      Rmm.initialize(RmmAllocationMode.CUDA_DEFAULT, null, 512L * 1024 * 1024)
    }
    SpillFramework.initialize(new RapidsConf(new SparkConf))
    RmmSpark.setEventHandler(new RmmEventHandler {
      override def getAllocThresholds: Array[Long] = null
      override def getDeallocThresholds: Array[Long] = null
      override def onAllocThreshold(totalAllocSize: Long): Unit = {}
      override def onDeallocThreshold(totalAllocSize: Long): Unit = {}
      override def onAllocFailure(sizeRequested: Long, retryCount: Int): Boolean = false
    })
    RmmSpark.currentThreadIsDedicatedToTask(1)
  }

  override def afterEach(): Unit = {
    try {
      SpillFramework.shutdown()
      RmmSpark.removeAllCurrentThreadAssociation()
      RmmSpark.clearEventHandler()
      if (ownsRmm) {
        Rmm.shutdown()
      }
    } finally {
      super.afterEach()
    }
  }

  private class TemporaryDirectory extends AutoCloseable {
    val path: java.nio.file.Path = Files.createTempDirectory("cached-parquet-reader")
    override def close(): Unit = FileUtils.deleteDirectory(path.toFile)
  }

  private def encoded(start: Int, count: Int, dictionary: Boolean): ParquetCachedBatch = {
    withResource(new TemporaryDirectory) { directory =>
      val path = directory.path.resolve("input.parquet")
      val writer = ExampleParquetWriter.builder(new Path(path.toUri))
        .withConf(new Configuration()).withType(parquetSchema)
        .withRowGroupSize(1024L).withPageSize(128)
        .withDictionaryEncoding(dictionary).withCompressionCodec(CompressionCodecName.SNAPPY)
        .build()
      withResource(writer) { out =>
        val groups = new SimpleGroupFactory(parquetSchema)
        (start until start + count).foreach { value =>
          val group = groups.newGroup().append("id", value.toLong)
          if (value % 3 != 0) {
            group.append("label", "dictionary-" + value % 7)
          }
          out.write(group)
        }
      }
      ParquetCachedBatch(count, Files.readAllBytes(path))
    }
  }

  private def readValues(producer: TableProducer): Seq[Long] = {
    val values = ArrayBuffer.empty[Long]
    while (producer.hasNext) {
      withResource(producer.next) { table =>
        withResource(table.getColumn(0).copyToHost()) { host =>
          (0 until table.getRowCount.toInt).foreach(i => values += host.getLong(i))
        }
      }
    }
    values.toVector
  }

  test("combined records preserve old multi-row-group data and column projection") {
    val first = encoded(0, 400, dictionary = true)
    val second = encoded(400, 400, dictionary = false)
    assert(ParquetCachedBatchFiles.describe(first, schema).rowGroups > 1)
    withResource(new ParquetCachedBatchReader(
        Iterator[CachedBatch](first, second), schema, options, 1024 * 1024, 0)) { reader =>
      assert(reader.hasNext)
      withResource(reader.next()) { batch =>
        assert(batch.numRows == 800)
        val actual = ArrayBuffer.empty[Long]
        val producer = batch.producer.get
        while (producer.hasNext) {
          withResource(producer.next) { table =>
            assert(table.getNumberOfColumns == 1)
            withResource(table.getColumn(0).copyToHost()) { host =>
              (0 until table.getRowCount.toInt).foreach(i => actual += host.getLong(i))
            }
          }
        }
        assert(actual.toVector == (0L until 800L).toVector)
      }
      assert(!reader.hasNext)
    }
  }

  test("combined CPU-written row groups decode dictionary and plain labels with nulls") {
    val dictionary = encoded(0, 400, dictionary = true)
    val plain = encoded(400, 400, dictionary = false)
    val dictionaryInfo = ParquetCachedBatchFiles.describe(dictionary, schema)
    val plainInfo = ParquetCachedBatchFiles.describe(plain, schema)
    assert(dictionaryInfo.rowGroups > 1)
    assert(plainInfo.rowGroups > 1)
    val dictionaryLabels = dictionaryInfo.metadata.getBlocks.asScala
      .flatMap(_.getColumns.asScala).filter(_.getPath.toDotString == "label")
    val plainLabels = plainInfo.metadata.getBlocks.asScala
      .flatMap(_.getColumns.asScala).filter(_.getPath.toDotString == "label")
    assert(dictionaryLabels.nonEmpty && dictionaryLabels.exists(_.hasDictionaryPage))
    assert(plainLabels.nonEmpty && plainLabels.forall(!_.hasDictionaryPage))
    val allColumns = ParquetOptions.builder().build()
    withResource(new ParquetCachedBatchReader(
        Iterator[CachedBatch](dictionary, plain), schema, allColumns, 1024 * 1024, 0)) { reader =>
      withResource(reader.next()) { batch =>
        assert(batch.numRows == 800)
        val producer = batch.producer.get
        var expectedRow = 0
        while (producer.hasNext) {
          withResource(producer.next) { table =>
            assert(table.getNumberOfColumns == 2)
            withResource(table.getColumn(0).copyToHost()) { ids =>
              withResource(table.getColumn(1).copyToHost()) { labels =>
                (0 until table.getRowCount.toInt).foreach { row =>
                  assert(ids.getLong(row) == expectedRow.toLong)
                  assert(labels.isNull(row) == (expectedRow % 3 == 0))
                  if (expectedRow % 3 != 0) {
                    assert(labels.getJavaString(row) == "dictionary-" + expectedRow % 7)
                  }
                  expectedRow += 1
                }
              }
            }
          }
        }
        assert(expectedRow == 800)
      }
      assert(!reader.hasNext)
    }
  }

  test("oversize singleton progresses without consuming a following record") {
    var consumed = 0
    val record = encoded(0, 4, dictionary = true)
    val input = Iterator[CachedBatch](record, record).map { batch =>
      consumed += 1
      batch
    }
    withResource(new ParquetCachedBatchReader(input, schema, options, 1, 0)) { reader =>
      withResource(reader.next()) { batch =>
        assert(batch.numRows == 4)
        assert(batch.producer.nonEmpty)
        assert(consumed == 1)
      }
      withResource(reader.next()) { batch =>
        assert(batch.numRows == 4)
        assert(consumed == 2)
      }
      assert(!reader.hasNext)
    }
  }

  test("zero-byte records keep row counts and order without constructing a native reader") {
    val record = encoded(0, 4, dictionary = true)
    val input = Iterator[CachedBatch](
      ParquetCachedBatch(3, Array.emptyByteArray), record,
      ParquetCachedBatch(0, Array.emptyByteArray))
    withResource(new ParquetCachedBatchReader(input, schema, options, 1024 * 1024, 0)) { reader =>
      withResource(reader.next()) { batch =>
        assert(batch.numRows == 3)
        assert(batch.producer.isEmpty)
      }
      withResource(reader.next()) { batch =>
        assert(batch.numRows == 4)
        assert(batch.producer.nonEmpty)
      }
      withResource(reader.next()) { batch =>
        assert(batch.numRows == 0)
        assert(batch.producer.isEmpty)
      }
      assert(!reader.hasNext)
    }
  }

  test("bundle limits respect encoded hints, decoded hints and the integer row limit") {
    val info = ParquetCachedBatchFiles.describe(encoded(0, 1, dictionary = false), schema)
      .copy(rows = 1, encodedBytes = 2, estimatedDecodedBytes = 3)
    assert(ParquetCachedBatchReader.fits(1, 2, 3, info, 6))
    assert(!ParquetCachedBatchReader.fits(1, 5, 3, info, 6))
    assert(!ParquetCachedBatchReader.fits(1, 2, 4, info, 6))
    assert(!ParquetCachedBatchReader.fits(Int.MaxValue, 0, 0, info, Long.MaxValue))
    assert(!ParquetCachedBatchReader.fits(1, 0, 0,
      info.copy(rows = Int.MaxValue), Long.MaxValue))
  }

  test("retry after advancing native input replays only previously completed chunks") {
    var readersOpened = 0
    var readersClosed = 0
    var resourceCloses = 0
    var injected = false
    val resource = new AutoCloseable {
      override def close(): Unit = resourceCloses += 1
    }
    withResource(new TableProducer(() => {
      readersOpened += 1
      new ChunkedReader {
        private var index = 0
        override def hasNext: Boolean = index < 3
        override def next: Table = {
          val value = index
          index += 1
          if (value == 1 && !injected) {
            injected = true
            throw new GpuRetryOOM("after native progress")
          }
          new Table.TestBuilder().column(Array[java.lang.Long](value.toLong): _*).build()
        }
        override def close(): Unit = readersClosed += 1
      }
    }, Seq(resource))) { producer =>
      assert(readValues(producer) == Seq(0L, 1L, 2L))
    }
    assert(injected)
    assert(readersOpened == 2)
    assert(readersClosed == 2)
    assert(resourceCloses == 1)
  }

  test("hasNext allocation retry and early close release reader before its inputs") {
    var inject = true
    val closes = ArrayBuffer.empty[String]
    val input = new AutoCloseable {
      override def close(): Unit = closes += "input"
    }
    val producer = new TableProducer(() => new ChunkedReader {
      override def hasNext: Boolean = {
        if (inject) {
          inject = false
          throw new GpuRetryOOM("hasNext allocation")
        }
        true
      }
      override def next: Table = throw new UnsupportedOperationException()
      override def close(): Unit = closes += "reader"
    }, Seq(input))
    withResource(producer) { _ =>
      assert(producer.hasNext)
    }
    producer.close()
    assert(closes.toVector == Vector("reader", "reader", "input"))
  }

  test("task completion releases both an active logical file and staged lookahead") {
    val record = encoded(0, 4, dictionary = true)
    val info = ParquetCachedBatchFiles.describe(record, schema)
    val target = math.max(info.encodedBytes.toLong, info.estimatedDecodedBytes) + 1
    val context = new MockTaskContext(1, partitionId = 0)
    TrampolineUtil.setTaskContext(context)
    try {
      val reader = new ParquetCachedBatchReader(
        Iterator[CachedBatch](record, record), schema, options, target, 0)
      val batch = reader.next()
      assert(batch.producer.nonEmpty)
      assert(SpillFramework.stores.hostStore.numHandles == 2)
      context.markTaskComplete()
      assert(SpillFramework.stores.hostStore.numHandles == 0)
      batch.close()
      reader.close()
      assert(SpillFramework.stores.hostStore.numHandles == 0)
    } finally {
      TrampolineUtil.unsetTaskContext()
    }
  }

  test("an input failure closes records staged before the failed lookahead") {
    val record = encoded(0, 4, dictionary = true)
    var first = true
    val input = new Iterator[CachedBatch] {
      override def hasNext: Boolean = true
      override def next(): CachedBatch = {
        if (first) {
          first = false
          record
        } else {
          throw new IllegalStateException("failed input")
        }
      }
    }
    withResource(new ParquetCachedBatchReader(
        input, schema, options, 1024 * 1024, 0)) { reader =>
      val error = intercept[IllegalStateException](reader.next())
      assert(error.getMessage == "failed input")
      assert(SpillFramework.stores.hostStore.numHandles == 0)
    }
  }

  test("closed bundles do not accumulate task completion callbacks") {
    var registered = 0
    val context = new MockTaskContext(1, partitionId = 0) {
      override def addTaskCompletionListener(
          listener: TaskCompletionListener): org.apache.spark.TaskContext = {
        registered += 1
        super.addTaskCompletionListener(listener)
      }
    }
    TrampolineUtil.setTaskContext(context)
    try {
      val records = Iterator.fill[CachedBatch](1000)(
        ParquetCachedBatch(1, Array.emptyByteArray))
      withResource(new ParquetCachedBatchReader(
          records, schema, options, 1024 * 1024, 0)) { reader =>
        (0 until 1000).foreach { _ =>
          withResource(reader.next())(batch => assert(batch.numRows == 1))
        }
        assert(!reader.hasNext)
        assert(registered == 1)
        context.markTaskComplete()
        assert(SpillFramework.stores.hostStore.numHandles == 0)
      }
    } finally {
      TrampolineUtil.unsetTaskContext()
    }
  }
}
