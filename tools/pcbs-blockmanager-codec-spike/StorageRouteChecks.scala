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
package org.apache.spark.storage

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import scala.reflect.ClassTag

import org.apache.spark.SparkEnv
import org.apache.spark.memory.MemoryMode
import org.apache.spark.serializer.CacheCodecClassTag
import org.apache.spark.sql.codeccachespike.{BulkBatch, LongCacheCodec}

object StorageRouteChecks {
  def run(): Unit = {
    val env = SparkEnv.get
    val manager = env.blockManager
    val store = manager.memoryStore
    implicit val tag: ClassTag[BulkBatch] =
      new CacheCodecClassTag(ClassTag(classOf[BulkBatch]), new LongCacheCodec(1))
    val original = Array.tabulate(512)(_.toLong)
    val block = RDDBlockId(900001, 0)
    require(manager.putIterator(block, Iterator(new BulkBatch(1, original)),
      StorageLevel.MEMORY_AND_DISK, tellMaster = true))
    require(store.contains(block) && !manager.diskStore.contains(block))
    require(manager.blockInfoManager.lockForWriting(block).nonEmpty)
    val values = store.getValues(block).get.map(_.asInstanceOf[BulkBatch]).toArray
    val level = manager.dropFromMemory[BulkBatch](block, () => Left(values))
    require(level.useDisk)
    manager.releaseLock(block)
    require(!store.contains(block) && manager.diskStore.contains(block))
    val restored = manager.getLocalValues(block).get
    require(restored.data.next().asInstanceOf[BulkBatch].values.sameElements(original))
    while (restored.data.hasNext) restored.data.next()
    println("CODEC_SPIKE EVICTION_DISK_RELOAD_PASS")

    val held = manager.getLocalValues(block).get
    require(held.data.hasNext)
    val started = new CountDownLatch(1)
    val finished = new CountDownLatch(1)
    val error = new AtomicReference[Throwable]()
    val thread = new Thread(new Runnable {
      override def run(): Unit = {
        started.countDown()
        try manager.removeRdd(block.rddId)
        catch { case t: Throwable => error.set(t) }
        finally finished.countDown()
      }
    })
    thread.start()
    require(started.await(5, TimeUnit.SECONDS))
    require(!finished.await(250, TimeUnit.MILLISECONDS), "removal bypassed held reader")
    while (held.data.hasNext) held.data.next()
    require(finished.await(5, TimeUnit.SECONDS), "removal did not finish after reader release")
    require(error.get() == null)
    require(manager.getStatus(block).isEmpty)
    println("CODEC_SPIKE BLOCKING_REMOVE_READER_PASS")

    val capacity = env.memoryManager.maxOnHeapStorageMemory
    val reserve = capacity - 2L * 1024 * 1024
    require(store.reserveUnrollMemoryForThisTask(RDDBlockId(900003, 0), reserve, MemoryMode.ON_HEAP))
    try {
      Seq(false, true).zipWithIndex.foreach { case (finishToStream, index) =>
        val partialId = RDDBlockId(900002, index)
        def input = Iterator.range(0, 32).map(i =>
          new BulkBatch(1, Array.fill(32768)(i.toLong)))
        val partial = store.putIteratorAsBytes(partialId, input, tag, MemoryMode.ON_HEAP) match {
          case Left(value) => value
          case Right(_) => throw new IllegalStateException("test failed to force partial unroll")
        }
        require(partial.getUnrolledChunkedByteBuffer.size > 0, "no serialized prefix")
        val records = if (finishToStream) {
          val output = new ByteArrayOutputStream()
          partial.finishWritingToStream(output)
          env.serializerManager.dataDeserializeStream(partialId,
            new ByteArrayInputStream(output.toByteArray))(tag).toVector
        } else partial.valuesIterator.toVector
        require(records.length == 32)
        records.zipWithIndex.foreach { case (batch, i) =>
          require(batch.values.length == 32768 && batch.values.forall(_ == i.toLong))
        }
        require(!store.contains(partialId))
        println(s"CODEC_SPIKE PARTIAL_UNROLL_PASS finishToStream=$finishToStream records=32")
      }
    } finally store.releaseUnrollMemoryForThisTask(MemoryMode.ON_HEAP, reserve)
  }
}
