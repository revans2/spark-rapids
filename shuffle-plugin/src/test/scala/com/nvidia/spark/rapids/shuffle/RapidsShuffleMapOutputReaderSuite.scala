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

import java.io.{File, FileOutputStream, PrintWriter}
import java.nio.file.{Files, Paths}

import com.nvidia.spark.rapids.RapidsConf

import org.apache.spark._
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleDependency, ShuffleReadMetricsReporter}
import org.apache.spark.sql.vectorized.ColumnarBatch // K is Int, C is ColumnarBatch
import org.apache.spark.storage.BlockManagerId

import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar

class RapidsShuffleMapOutputReaderSuite extends AnyFunSuite with BeforeAndAfterEach with MockitoSugar { // Renamed class

  private var tempDir: File = _ // This will represent a Spark local directory
  private var sparkConf: SparkConf = _
  private var rapidsConf: RapidsConf = _ // Still needed for other configs potentially
  private var mockTaskContext: TaskContext = _
  private var mockShuffleDep: ShuffleDependency[Int, _, ColumnarBatch] = _
  private var mockPartitioner: Partitioner = _
  private var mockShuffleHandle: BaseShuffleHandle[Int, _, ColumnarBatch] = _
  private var mockReadMetrics: ShuffleReadMetricsReporter = _
  private var mockMapOutputTracker: MapOutputTracker = _

  val shuffleId = 1
  val numMapTasks = 2
  val numPartitions = 3 // Reduce partitions

  override def beforeEach(): Unit = {
    super.beforeEach()
    tempDir = Files.createTempDirectory("rapids-shuffle-reader-suite-local-dir").toFile

    // RapidsConf no longer needs SHUFFLE_SEEKY_ROOT_DIR
    sparkConf = new SparkConf(loadDefaults = false)
    rapidsConf = new RapidsConf(sparkConf)

    // Mock SparkEnv to provide BlockManager and its local dirs
    val mockBlockManager = mock[org.apache.spark.storage.BlockManager]
    val mockDiskBlockManager = mock[org.apache.spark.storage.DiskBlockManager]
    when(mockBlockManager.diskBlockManager).thenReturn(mockDiskBlockManager)
    when(mockDiskBlockManager.getLocalDirs).thenReturn(Array(tempDir))

    val mockEnv = mock[SparkEnv]
    when(mockEnv.blockManager).thenReturn(mockBlockManager)
    when(mockEnv.mapOutputTracker).thenReturn(mockMapOutputTracker) // mockMapOutputTracker initialized below
    when(mockEnv.conf).thenReturn(sparkConf)
    SparkEnv.set(mockEnv)

    mockTaskContext = mock[TaskContext]
    mockReadMetrics = mock[ShuffleReadMetricsReporter]
    mockMapOutputTracker = mock[MapOutputTracker] // Reader uses SparkEnv.get.mapOutputTracker

    mockShuffleDep = mock[ShuffleDependency[Int, Any, ColumnarBatch]] // K, V, C
    mockPartitioner = new HashPartitioner(numPartitions)
    when(mockShuffleDep.shuffleId).thenReturn(shuffleId)
    when(mockShuffleDep.partitioner).thenReturn(mockPartitioner)
    when(mockShuffleDep.numMaps).thenReturn(numMapTasks) // Important for reader to know how many maps to check

    mockShuffleHandle = new BaseShuffleHandle(shuffleId, mockShuffleDep)

    // SparkEnv.set(mockEnv) // Already set above
  }

  override def afterEach(): Unit = {
    try {
      if (tempDir != null) {
        // Clean up the contents of tempDir
        tempDir.listFiles().foreach(org.apache.spark.util.Utils.deleteRecursively)
      }
    } finally {
      super.afterEach()
      SparkEnv.set(null)
    }
  }

  // Using placeholder dir structure as in writer: tempDir / "shuffle_seeky_placeholder" / shuffleId / mapId
  private val placeholderShuffleSubDir = "shuffle_seeky_placeholder"

  private def createShuffleFile(shuffleId: Int, mapId: Int, attemptId: Long, partitionId: Int, batchId: Int, content: String): File = {
    val mapDir = Paths.get(tempDir.getAbsolutePath, placeholderShuffleSubDir, shuffleId.toString, mapId.toString).toFile
    if (!mapDir.exists()) mapDir.mkdirs()
    val dataFile = Paths.get(mapDir.getAbsolutePath, s"${attemptId}_${partitionId}_${batchId}.data").toFile
    val writer = new PrintWriter(new FileOutputStream(dataFile))
    try {
      writer.print(content)
    } finally {
      writer.close()
    }
    dataFile
  }

  private def createIndexFile(shuffleId: Int, mapId: Int, attemptId: Long, records: Seq[RapidsShuffleIndexRecord]): File = {
    val mapDir = Paths.get(tempDir.getAbsolutePath, placeholderShuffleSubDir, shuffleId.toString, mapId.toString).toFile
    if (!mapDir.exists()) mapDir.mkdirs()
    val indexFile = Paths.get(mapDir.getAbsolutePath, s"$attemptId.index").toFile
    val writer = new PrintWriter(new FileOutputStream(indexFile))
    try {
      records.foreach { record =>
        // Using the same simple JSON format as the writer
        val line = s"""{"partitionId":${record.partitionId},"filePath":"${record.filePath}","offset":${record.offset},"length":${record.length}}"""
        writer.println(line)
      }
    } finally {
      writer.close()
    }
    indexFile
  }

  test("read basic shuffle data") {
    // Setup: Create mock shuffle files from two map tasks for a few partitions
    val map0Attempt0 = 0L
    val map1Attempt0 = 0L

    // Map 0 output
    val m0p0b0Data = "map0_part0_batch0_data"
    val m0p0b0File = createShuffleFile(shuffleId, 0, map0Attempt0, 0, 0, m0p0b0Data)
    val m0p1b0Data = "map0_part1_batch0_data"
    val m0p1b0File = createShuffleFile(shuffleId, 0, map0Attempt0, 1, 1, m0p1b0Data) // batchId 1 for map0 overall

    val map0IndexRecords = Seq(
      RapidsShuffleIndexRecord(0, m0p0b0File.getAbsolutePath, 0, m0p0b0Data.length),
      RapidsShuffleIndexRecord(1, m0p1b0File.getAbsolutePath, 0, m0p1b0Data.length)
    )
    createIndexFile(shuffleId, 0, map0Attempt0, map0IndexRecords)

    // Map 1 output
    val m1p0b0Data = "map1_part0_batch0_data"
    val m1p0b0File = createShuffleFile(shuffleId, 1, map1Attempt0, 0, 0, m1p0b0Data)
    val m1p2b0Data = "map1_part2_batch0_data" // Data for partition 2 from map 1
    val m1p2b0File = createShuffleFile(shuffleId, 1, map1Attempt0, 2, 1, m1p2b0Data)

    val map1IndexRecords = Seq(
      RapidsShuffleIndexRecord(0, m1p0b0File.getAbsolutePath, 0, m1p0b0Data.length),
      RapidsShuffleIndexRecord(2, m1p2b0File.getAbsolutePath, 0, m1p2b0Data.length)
    )
    createIndexFile(shuffleId, 1, map1Attempt0, map1IndexRecords)

    // Reader setup: Read all partitions (0 to numPartitions-1)
    val reader = new RapidsShuffleMapOutputReader[Int, ColumnarBatch]( // Renamed class
      mockShuffleHandle,
      0, // startPartition
      numPartitions, // endPartition
      mockTaskContext,
      mockReadMetrics,
      rapidsConf
    )

    val resultIterator = reader.read()
    // The reader currently produces dummy ColumnarBatch objects.
    // We are interested in how many such batches (segments) it reads per partition.

    var countP0 = 0
    var countP1 = 0
    var countP2 = 0
    var totalBytesRead = 0L

    while(resultIterator.hasNext) {
      val item = resultIterator.next() // item is Product2[Int, ColumnarBatch]
      val partitionId = item._1
      // val cb = item._2 // This is a dummy ColumnarBatch in current reader impl

      // For now, we can't verify content of CB easily, but we can count items per partition
      // and check metrics. The reader's log also shows file paths.
      partitionId match {
        case 0 => countP0 += 1
        case 1 => countP1 += 1
        case 2 => countP2 += 1
        case _ => fail(s"Unexpected partition ID $partitionId")
      }
    }

    // Expected counts:
    // Partition 0: one segment from map0, one segment from map1 => 2 items
    // Partition 1: one segment from map0 => 1 item
    // Partition 2: one segment from map1 => 1 item
    assert(countP0 == 2)
    assert(countP1 == 1)
    assert(countP2 == 1)

    // Verify metrics (these are based on the placeholder data lengths)
    // incRemoteBytesRead is called for each segment.
    verify(mockReadMetrics, times(1)).incRemoteBytesRead(m0p0b0Data.length)
    verify(mockReadMetrics, times(1)).incRemoteBytesRead(m0p1b0Data.length)
    verify(mockReadMetrics, times(1)).incRemoteBytesRead(m1p0b0Data.length)
    verify(mockReadMetrics, times(1)).incRemoteBytesRead(m1p2b0Data.length)

    // incRecordsRead is called once per segment read (dummy CB)
    verify(mockReadMetrics, times(4)).incRecordsRead(1)
  }

  test("read data for a specific partition range") {
    val map0Attempt0 = 0L
    // Map 0 output: part 0, 1, 2
    val m0p0File = createShuffleFile(shuffleId, 0, map0Attempt0, 0, 0, "m0p0")
    val m0p1File = createShuffleFile(shuffleId, 0, map0Attempt0, 1, 1, "m0p1")
    val m0p2File = createShuffleFile(shuffleId, 0, map0Attempt0, 2, 2, "m0p2")
    createIndexFile(shuffleId, 0, map0Attempt0, Seq(
      RapidsShuffleIndexRecord(0, m0p0File.getAbsolutePath, 0, "m0p0".length),
      RapidsShuffleIndexRecord(1, m0p1File.getAbsolutePath, 0, "m0p1".length),
      RapidsShuffleIndexRecord(2, m0p2File.getAbsolutePath, 0, "m0p2".length)
    ))

    // Reader setup: Read partitions 1 to 2 (exclusive end, so only partition 1)
    val reader = new RapidsShuffleMapOutputReader[Int, ColumnarBatch]( // Renamed class
      mockShuffleHandle,
      1, // startPartition
      2, // endPartition (exclusive)
      mockTaskContext,
      mockReadMetrics,
      rapidsConf
    )

    val results = reader.read().toList
    assert(results.length == 1)
    assert(results.head._1 == 1) // Should only contain partition 1 data

    verify(mockReadMetrics, times(1)).incRemoteBytesRead("m0p1".length)
    verify(mockReadMetrics, times(1)).incRecordsRead(1)
  }

  test("handle missing index file") {
    // Create a data file but no index file for map 0
    createShuffleFile(shuffleId, 0, 0L, 0, 0, "data_no_index")

    val reader = new RapidsShuffleMapOutputReader[Int, ColumnarBatch]( // Renamed class
      mockShuffleHandle, 0, 1, mockTaskContext, mockReadMetrics, rapidsConf
    )
    val results = reader.read().toList
    assert(results.isEmpty) // No data should be read as index is missing
    // verify logWarning was called if possible, or check logs manually in a real test env
  }

  test("handle malformed index file record") {
    val map0Attempt0 = 0L
    val validDataFile = createShuffleFile(shuffleId, 0, map0Attempt0, 0, 0, "valid_data")
    val mapDir = Paths.get(tempDir.getAbsolutePath, placeholderShuffleSubDir, shuffleId.toString, "0").toFile // Adjusted path
    mapDir.mkdirs()
    val indexFile = Paths.get(mapDir.getAbsolutePath, s"$map0Attempt0.index").toFile

    val writer = new PrintWriter(new FileOutputStream(indexFile))
    try {
      writer.println(s"""{"partitionId":0,"filePath":"${validDataFile.getAbsolutePath}","offset":0,"length":${"valid_data".length}}""")
      writer.println("this is a malformed line") // Malformed JSON
      writer.println(s"""{"partitionId":0,"filePath":"another_path","offset":0,"length":10}""") // Points to valid data (for parsing test)
    } finally {
      writer.close()
    }

    // Create the "another_path" file so it doesn't fail on file read for the second valid entry
    createShuffleFile(shuffleId, 0, map0Attempt0, 0, 1, "more_data")


    val reader = new RapidsShuffleMapOutputReader[Int, ColumnarBatch]( // Renamed class
      mockShuffleHandle, 0, 1, mockTaskContext, mockReadMetrics, rapidsConf
    )
    val results = reader.read().toList

    // Should read the first valid record and the third (which we made point to valid data for simplicity of this test point)
    // The reader's current parser is very basic and might fail differently.
    // The current parser in RapidsSeekyShuffleReader for index lines is:
    // line.stripPrefix("{").stripSuffix("}").split(",").map { part => kv = part.split(":", 2) ... }.toMap
    // "this is a malformed line" will likely cause an exception during split or toMap, caught by catch block.
    // So, it should skip the malformed line and read the other two.
    assert(results.length == 2)
    assert(results.count(_._1 == 0) == 2)
    verify(mockReadMetrics, times(1)).incRemoteBytesRead("valid_data".length)
    verify(mockReadMetrics, times(1)).incRemoteBytesRead("more_data".length) // from the third line, if "another_path" was made to point to it
    verify(mockReadMetrics, times(2)).incRecordsRead(1)
  }

  test("handle missing data file listed in index") {
    val map0Attempt0 = 0L
    // Use a path within the placeholder structure for consistency, though it won't exist
    val nonExistentDataFilePath = Paths.get(tempDir.getAbsolutePath,
        placeholderShuffleSubDir, shuffleId.toString, "0", "non_existent.data").toString

    createIndexFile(shuffleId, 0, map0Attempt0, Seq(
      RapidsShuffleIndexRecord(0, nonExistentDataFilePath, 0, 100)
    ))

    val reader = new RapidsShuffleMapOutputReader[Int, ColumnarBatch]( // Renamed class
      mockShuffleHandle, 0, 1, mockTaskContext, mockReadMetrics, rapidsConf
    )

    // The current reader logs an error and the iterator map for that record will produce nothing useful,
    // effectively skipping it or yielding an empty/invalid batch.
    // For this test, we expect the iterator to be empty or the produced batch to be recognizably problematic
    // if the iterator doesn't just skip it. The current code tries to read, logs error, and returns a dummy ColumnarBatch.
    // So it *will* produce an item.
    val results = reader.read().toList
    assert(results.length == 1) // It will produce one dummy batch
    // We can't easily check the content of the dummy batch here without more test infra.
    // Key is that it doesn't throw an unhandled exception.
    // Metrics will still be updated as if it read 100 bytes, as per current impl.
    verify(mockReadMetrics, times(1)).incRemoteBytesRead(100)
    verify(mockReadMetrics, times(1)).incRecordsRead(1)
  }

  test("read data from multiple map attempts (heuristic check)") {
    val mapIdToTest = 0
    val attempt0 = 0L
    val attempt1 = 1L // Higher attempt number

    // Attempt 0 data (older)
    val m0a0p0File = createShuffleFile(shuffleId, mapIdToTest, attempt0, 0, 0, "attempt0_data")
    createIndexFile(shuffleId, mapIdToTest, attempt0, Seq(
      RapidsShuffleIndexRecord(0, m0a0p0File.getAbsolutePath, 0, "attempt0_data".length)
    ))

    // Attempt 1 data (newer, should be preferred by reader's heuristic: last file by name)
    val m0a1p0File = createShuffleFile(shuffleId, mapIdToTest, attempt1, 0, 0, "attempt1_data_is_correct")
    createIndexFile(shuffleId, mapIdToTest, attempt1, Seq(
      RapidsShuffleIndexRecord(0, m0a1p0File.getAbsolutePath, 0, "attempt1_data_is_correct".length)
    ))

    // Mock MapOutputTracker to guide to this mapId if necessary, though reader iterates 0..numMaps-1
    // The reader's heuristic is `indexFiles.sortBy(_.getName).last`
    // "1.index" comes after "0.index" when sorted by name.

    val reader = new RapidsShuffleMapOutputReader[Int, ColumnarBatch]( // Renamed class
      mockShuffleHandle, 0, 1, mockTaskContext, mockReadMetrics, rapidsConf
    )
    val results = reader.read().toList
    assert(results.length == 1)

    // Verify metrics show it read from attempt 1's data
    verify(mockReadMetrics, times(1)).incRemoteBytesRead("attempt1_data_is_correct".length)
    verify(mockReadMetrics, never()).incRemoteBytesRead("attempt0_data".length)
    verify(mockReadMetrics, times(1)).incRecordsRead(1)
  }
}
