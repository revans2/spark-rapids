/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shuffle

import java.io.File
import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._
import scala.io.Source

import com.nvidia.spark.rapids.{RapidsConf, RapidsShufflePartitioning}
import com.nvidia.spark.rapids.shuffle.RapidsShufflePartitioning.ShuffleBatch

import org.apache.spark._
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleWriteMetricsReporter}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.BlockManagerId

import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar

class RapidsShuffleMapOutputWriterSuite extends AnyFunSuite with BeforeAndAfterEach with MockitoSugar { // Renamed class

  private var tempDir: File = _ // This will represent a Spark local directory
  private var sparkConf: SparkConf = _
  private var rapidsConf: RapidsConf = _ // Still needed for other configs potentially
  private var mockTaskContext: TaskContext = _
  private var mockShuffleDep: ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = _
  private var mockPartitioner: Partitioner = _
  private var mockShuffleHandle: BaseShuffleHandle[Int, ColumnarBatch, ColumnarBatch] = _
  private var mockMetrics: ShuffleWriteMetricsReporter = _

  val shuffleId = 1
  val mapId = 0 // TaskContext.mapId() is Int
  val attemptNumber = 0 // TaskContext.attemptNumber() is Int
  val taskAttemptId = (attemptNumber.toLong << 32) | mapId.toLong // Simplified, real one is different

  override def beforeEach(): Unit = {
    super.beforeEach()
    // tempDir will simulate one of Spark's local dirs.
    tempDir = Files.createTempDirectory("rapids-shuffle-writer-suite-local-dir").toFile

    // Mock Spark's local directories to include our tempDir
    val mockBlockManager = mock[org.apache.spark.storage.BlockManager]
    val mockDiskBlockManager = mock[org.apache.spark.storage.DiskBlockManager]
    when(mockBlockManager.diskBlockManager).thenReturn(mockDiskBlockManager)
    when(mockDiskBlockManager.getLocalDirs).thenReturn(Array(tempDir))

    val mockEnv = mock[SparkEnv]
    when(mockEnv.blockManager).thenReturn(mockBlockManager)
    // Provide a BlockManagerId, as the writer's commitAllPartitions uses it for MapStatus
    when(mockBlockManager.blockManagerId).thenReturn(BlockManagerId("test-exec", "localhost", 12345, None))
    SparkEnv.set(mockEnv)

    // RapidsConf no longer needs SHUFFLE_SEEKY_ROOT_DIR
    sparkConf = new SparkConf(loadDefaults = false)
    rapidsConf = new RapidsConf(sparkConf)


    mockTaskContext = mock[TaskContext]
    when(mockTaskContext.mapId()).thenReturn(mapId)
    when(mockTaskContext.attemptNumber()).thenReturn(attemptNumber)
    // Note: TaskContext.taskAttemptId() is a long.
    // Let's use a simplified way to generate one for consistency if needed,
    // though the writer gets it from context.taskAttemptId() directly.
    // The writer uses context.taskAttemptId() which is long.
    when(mockTaskContext.taskAttemptId()).thenReturn(taskAttemptId)


    mockShuffleDep = mock[ShuffleDependency[Int, ColumnarBatch, ColumnarBatch]]
    mockPartitioner = new HashPartitioner(3) // 3 partitions for testing
    when(mockShuffleDep.shuffleId).thenReturn(shuffleId)
    when(mockShuffleDep.partitioner).thenReturn(mockPartitioner)

    // K, V, C types for ShuffleHandle and Writer
    mockShuffleHandle = new BaseShuffleHandle(shuffleId, mockShuffleDep)
    mockMetrics = mock[ShuffleWriteMetricsReporter]

    // SparkEnv.set(mockEnv) // Already set above
  }

  override def afterEach(): Unit = {
    try {
      if (tempDir != null) {
        // Clean up the contents of tempDir, but not tempDir itself if SparkEnv holds it
        tempDir.listFiles().foreach(org.apache.spark.util.Utils.deleteRecursively)
      }
    } finally {
      super.afterEach()
      SparkEnv.set(null) // Clean up SparkEnv
    }
  }

  test("write basic data and commit") {
    val writer = new RapidsShuffleMapOutputWriter( // Renamed class
      mockShuffleHandle,
      mockTaskContext,
      rapidsConf, // RapidsConf might not be strictly needed by writer now
      mockMetrics
    )

    // Create dummy ShuffleBatch data
    // partitionId is Int, batch is ColumnarBatch
    val batch0p0 = ShuffleBatch(0, new ColumnarBatch(Array(), 0)) // partition 0
    val batch1p0 = ShuffleBatch(0, new ColumnarBatch(Array(), 0)) // partition 0
    val batch0p1 = ShuffleBatch(1, new ColumnarBatch(Array(), 0)) // partition 1

    val records: Iterator[ShuffleBatch] = Seq(batch0p0, batch1p0, batch0p1).iterator
    // The writer expects Iterator[Product2[K,V]], but we made it take Iterator[ShuffleBatch]
    // So, we need to cast, or change writer signature (which would be a bigger change now)
    // For the test, we'll assume the cast inside the writer works or we adapt the input.
    // The writer's current implementation casts records.asInstanceOf[Iterator[ShuffleBatch]]

    writer.write(records.asInstanceOf[Iterator[Product2[Int, ColumnarBatch]]])
    val mapStatusOption = writer.commitAllPartitions()

    assert(mapStatusOption.isDefined)
    val mapStatus = mapStatusOption.get
    assert(mapStatus.location != null) // Should be the BlockManagerId we mocked

    // The writer now uses a placeholder path: tempDir / "shuffle_seeky_placeholder" / shuffleId / mapId
    val expectedShuffleDir = Paths.get(tempDir.getAbsolutePath,
      s"shuffle_seeky_placeholder/${shuffleId.toString}/${mapId.toString}").toFile
    assert(expectedShuffleDir.exists() && expectedShuffleDir.isDirectory)

    // Filenames remain the same structure within the attempt's directory
    val dataFile_b0 = Paths.get(expectedShuffleDir.getAbsolutePath, s"${taskAttemptId}_0_0.data").toFile
    val dataFile_b1 = Paths.get(expectedShuffleDir.getAbsolutePath, s"${taskAttemptId}_0_1.data").toFile
    val dataFile_b2 = Paths.get(expectedShuffleDir.getAbsolutePath, s"${taskAttemptId}_1_2.data").toFile

    assert(dataFile_b0.exists())
    assert(dataFile_b1.exists())
    assert(dataFile_b2.exists())

    val indexFile = Paths.get(expectedShuffleDir.getAbsolutePath, s"$taskAttemptId.index").toFile
    assert(indexFile.exists())

    // Verify index file content
    val source = Source.fromFile(indexFile)
    val lines = try source.getLines().toList finally source.close()
    assert(lines.length == 3)

    val expectedData0 = s"Serialized data for partition 0, batch 0\n".getBytes("UTF-8")
    val expectedData1 = s"Serialized data for partition 0, batch 1\n".getBytes("UTF-8")
    val expectedData2 = s"Serialized data for partition 1, batch 2\n".getBytes("UTF-8")

    val record0 = RapidsShuffleInternalUtils.parseIndexJsonLine(lines(0))
    assert(record0.filePath == dataFile_b0.getAbsolutePath)
    // Other assertions for record0, record1, record2 remain the same...
    assert(record0.partitionId == 0)
    assert(record0.offset == 0)
    assert(record0.length == expectedData0.length)

    val record1 = RapidsShuffleInternalUtils.parseIndexJsonLine(lines(1))
    assert(record1.partitionId == 0)
    assert(record1.filePath == dataFile_b1.getAbsolutePath)
    assert(record1.offset == 0)
    assert(record1.length == expectedData1.length)

    val record2 = RapidsShuffleInternalUtils.parseIndexJsonLine(lines(2))
    assert(record2.partitionId == 1)
    assert(record2.filePath == dataFile_b2.getAbsolutePath)
    assert(record2.offset == 0)
    assert(record2.length == expectedData2.length)


    val lengths = mapStatus.getSerializedSize(0)
    assert(lengths(0) == (expectedData0.length + expectedData1.length))
    assert(lengths(1) == expectedData2.length)
    assert(lengths(2) == 0)

    verify(mockMetrics, times(3)).incRecordsWritten(1)
  }

  test("stop with success=false cleans up files") {
    val writer = new RapidsShuffleMapOutputWriter( // Renamed class
      mockShuffleHandle,
      mockTaskContext,
      rapidsConf,
      mockMetrics
    )

    val batch0p0 = ShuffleBatch(0, new ColumnarBatch(Array(), 0))
    val records: Iterator[ShuffleBatch] = Seq(batch0p0).iterator
    writer.write(records.asInstanceOf[Iterator[Product2[Int, ColumnarBatch]]])

    val expectedShuffleDir = Paths.get(tempDir.getAbsolutePath,
      s"shuffle_seeky_placeholder/${shuffleId.toString}/${mapId.toString}").toFile
    val dataFile_b0 = Paths.get(expectedShuffleDir.getAbsolutePath, s"${taskAttemptId}_0_0.data").toFile
    assert(dataFile_b0.exists())

    writer.stop(success = false)

    assert(!dataFile_b0.exists())
    val indexFile = Paths.get(expectedShuffleDir.getAbsolutePath, s"$taskAttemptId.index").toFile
    assert(!indexFile.exists())
  }

  test("stop with success=true does not clean up files") {
    val writer = new RapidsShuffleMapOutputWriter( // Renamed class
      mockShuffleHandle,
      mockTaskContext,
      rapidsConf,
      mockMetrics
    )

    val batch0p0 = ShuffleBatch(0, new ColumnarBatch(Array(), 0))
    val records: Iterator[ShuffleBatch] = Seq(batch0p0).iterator
    writer.write(records.asInstanceOf[Iterator[Product2[Int, ColumnarBatch]]])
    writer.commitAllPartitions()

    val expectedShuffleDir = Paths.get(tempDir.getAbsolutePath,
      s"shuffle_seeky_placeholder/${shuffleId.toString}/${mapId.toString}").toFile
    val dataFile_b0 = Paths.get(expectedShuffleDir.getAbsolutePath, s"${taskAttemptId}_0_0.data").toFile
    val indexFile = Paths.get(expectedShuffleDir.getAbsolutePath, s"$taskAttemptId.index").toFile

    assert(dataFile_b0.exists())
    assert(indexFile.exists())

    writer.stop(success = true)

    assert(dataFile_b0.exists())
    assert(indexFile.exists())
  }

  test("handle empty input iterator") {
    val writer = new RapidsShuffleMapOutputWriter( // Renamed class
      mockShuffleHandle,
      mockTaskContext,
      rapidsConf,
      mockMetrics
    )

    val records: Iterator[ShuffleBatch] = Iterator.empty
    writer.write(records.asInstanceOf[Iterator[Product2[Int, ColumnarBatch]]])
    val mapStatusOption = writer.commitAllPartitions()

    assert(mapStatusOption.isDefined)
    val mapStatus = mapStatusOption.get

    val lengths = mapStatus.getSerializedSize(0)
    assert(lengths.sum == 0) // All partition lengths should be 0
    assert(lengths.length == mockPartitioner.numPartitions)

    val expectedShuffleDir = Paths.get(tempDir.getAbsolutePath, shuffleId.toString, mapId.toString).toFile
    val indexFile = Paths.get(expectedShuffleDir.getAbsolutePath, s"$taskAttemptId.index").toFile
    assert(indexFile.exists()) // Index file should be created

    val source = Source.fromFile(indexFile)
    val lines = try source.getLines().toList finally source.close()
    assert(lines.isEmpty) // Index file should be empty as no data was written

    verify(mockMetrics, never()).incBytesWritten(anyLong())
    verify(mockMetrics, never()).incRecordsWritten(anyLong())
  }
}

// Helper to parse index file lines if not using a proper JSON lib in tests
object RapidsShuffleInternalUtils { // Renamed to avoid conflict if such object exists
  case class IndexRecordTest(partitionId: Int, filePath: String, offset: Long, length: Long)

  // Very basic parser, make it robust or use a library for real tests
  def parseIndexJsonLine(line: String): IndexRecordTest = {
    // Example: {"partitionId":0,"filePath":"/path","offset":0,"length":123}
    val stripped = line.stripPrefix("{").stripSuffix("}")
    val parts = stripped.split(",").map(_.trim).toList
    val map = parts.map { part =>
      val kv = part.split(":", 2)
      val key = kv(0).trim.stripPrefix("\"").stripSuffix("\"")
      val value = kv(1).trim
      key -> value
    }.toMap

    IndexRecordTest(
      map("partitionId").toInt,
      map("filePath").stripPrefix("\"").stripSuffix("\""),
      map("offset").toLong,
      map("length").toLong
    )
  }
}
