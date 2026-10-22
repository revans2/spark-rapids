/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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
package org.apache.spark.storage.nativecachespike

import java.io._
import java.nio.ByteBuffer
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.reflect.ClassTag
import scala.collection.JavaConverters._
import java.nio.file.{Files, Paths}

import ai.rapids.cudf.HostMemoryBuffer
import com.nvidia.spark.rapids.{HostAlloc, RapidsConf, SpillableHostBuffer, SpillPriorities}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.spill.SpillFramework

import org.apache.spark.{SparkEnv, SparkConf}
import org.apache.spark.serializer._
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.{RDDBlockId, StorageLevel}

object NativeRuntime {
  private var initialized = false
  val created = new AtomicInteger()
  val closed = new AtomicInteger()
  val writes = new AtomicInteger()
  val reads = new AtomicInteger()
  val bytes = 32768L
  val limit = 64L * 1024 * 1024
  def init(): Unit = synchronized {
    if (!initialized) {
      val conf = new SparkConf(false)
        .set("spark.rapids.memory.host.offHeapLimit.enabled", "true")
        .set("spark.rapids.memory.host.offHeapLimit.size", limit.toString)
        .set("spark.rapids.memory.pinnedPool.size", "0")
        .set("spark.rapids.memory.host.spillStorageSize", "1m")
        .set("spark.rapids.memory.host.spillToDiskBounceBufferSize", "1m")
        .set("spark.rapids.memory.host.spillToDiskBounceBuffers", "1")
        .set("spark.rapids.sql.chunkedPack.bounceBufferSize", "1m")
        .set("spark.rapids.sql.chunkedPack.bounceBuffers", "1")
      HostAlloc.initialize(limit)
      SpillFramework.initialize(new RapidsConf(conf))
      initialized = true
      require(HostAlloc.tryAlloc(limit + 1, preferPinned = false).isEmpty)
      require(!SparkEnv.get.conf.getBoolean("spark.memory.offHeap.enabled", false))
      require(SparkEnv.get.memoryManager.offHeapStorageMemoryUsed == 0L)
      require(HostAlloc.getUsageRatio() <= 1.0)
    }
  }
  def make(seed: Int): NativeBatch = {
    init()
    val payload = closeOnExcept(HostAlloc.alloc(bytes, preferPinned = false)) { host =>
      var i = 0L
      while (i < bytes / 8) {
        host.setLong(i * 8, seed.toLong + i)
        i += 1
      }
      SpillableHostBuffer(host, bytes, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
    }
    new NativeBatch(seed, payload)
  }
  def assertClean(): Unit = {
    require(created.get == closed.get, s"owner leak ${created.get}/${closed.get}")
    require(SpillFramework.stores.hostStore.numHandles == 0)
    require(SpillFramework.stores.diskStore.numHandles == 0)
    require(HostAlloc.getUsageRatio() == 1.0 / 64)
    require(SparkEnv.get.memoryManager.offHeapStorageMemoryUsed == 0L)
  }
}

final class NativeBatch(val seed: Int, val payload: SpillableHostBuffer)
    extends AutoCloseable with Serializable {
  private var released = false
  NativeRuntime.created.incrementAndGet()
  override def close(): Unit = synchronized {
    require(!released, "owner closed twice")
    released = true
    payload.close()
    NativeRuntime.closed.incrementAndGet()
  }
  def verify(host: HostMemoryBuffer): Unit = {
    require(host.getLength == NativeRuntime.bytes)
    var i = 0L
    while (i < host.getLength / 8) {
      require(host.getLong(i * 8) == seed.toLong + i)
      i += 1
    }
  }
  def verify(): Unit = withResource(payload.getDataHostBuffer())(verify)
  private def writeObject(out: ObjectOutputStream): Unit =
    throw new NotSerializableException("NATIVE_BULK_JAVA_FORBIDDEN")
}

final class NativeCodec extends Serializer with Serializable {
  override def newInstance(): SerializerInstance = new SerializerInstance {
    override def serialize[T: ClassTag](v: T): ByteBuffer =
      throw new UnsupportedOperationException("stream only")
    override def deserialize[T: ClassTag](b: ByteBuffer): T =
      throw new UnsupportedOperationException("stream only")
    override def deserialize[T: ClassTag](b: ByteBuffer, c: ClassLoader): T =
      throw new UnsupportedOperationException("stream only")
    override def serializeStream(out: OutputStream): SerializationStream = new SerializationStream {
      val data = new DataOutputStream(out)
      override def writeObject[T: ClassTag](v: T): SerializationStream = {
        val b = v.asInstanceOf[NativeBatch]
        data.writeInt(0x6e617469)
        data.writeInt(b.seed)
        data.writeLong(b.payload.length)
        withResource(b.payload.getDataHostBuffer()) { host =>
          var i = 0L
          while (i < host.getLength / 8) { data.writeLong(host.getLong(i * 8)); i += 1 }
        }
        NativeRuntime.writes.incrementAndGet()
        this
      }
      override def flush(): Unit = data.flush()
      override def close(): Unit = data.close()
    }
    override def deserializeStream(in: InputStream): DeserializationStream = new DeserializationStream {
      val data = new DataInputStream(in)
      override def readObject[T: ClassTag](): T = {
        require(data.readInt() == 0x6e617469)
        val seed = data.readInt()
        require(data.readLong() == NativeRuntime.bytes)
        NativeRuntime.init()
        val payload = closeOnExcept(HostAlloc.alloc(NativeRuntime.bytes, preferPinned = false)) { host =>
          var i = 0L
          while (i < NativeRuntime.bytes / 8) { host.setLong(i * 8, data.readLong()); i += 1 }
          SpillableHostBuffer(host, NativeRuntime.bytes, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
        }
        NativeRuntime.reads.incrementAndGet()
        new NativeBatch(seed, payload).asInstanceOf[T]
      }
      override def close(): Unit = data.close()
    }
  }
}

object BlockManagerNativeSpike {
  implicit def tag: ClassTag[NativeBatch] =
    new CacheCodecClassTag[NativeBatch](ClassTag(classOf[NativeBatch]), new NativeCodec)
  def mark(s: String): Unit = println("NATIVE_SPIKE " + s)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("RAPIDS native BlockManager lifetime proof").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    NativeRuntime.init()
    val bm = SparkEnv.get.blockManager
    require(!spark.sparkContext.getConf.getBoolean("spark.memory.offHeap.enabled", false))
    require(SparkEnv.get.memoryManager.offHeapStorageMemoryUsed == 0L)
    val level = StorageLevel.MEMORY_AND_DISK
    val id = RDDBlockId(900001, 0)
    val batch = NativeRuntime.make(17)
    require(bm.putIterator(id, Iterator(batch), level))
    require(bm.memoryStore.contains(id))
    def recordAccounting(phase: String): Unit = {
      val status = bm.getStatus(id).get
      mark(s"ACCOUNTING phase=$phase memBytes=${status.memSize} " +
        s"diskBytes=${status.diskSize} useMemory=${status.storageLevel.useMemory} " +
        s"payloadBytes=${NativeRuntime.bytes}")
    }
    recordAccounting("resident")
    withResource(batch.payload.getDataHostBuffer())(batch.verify)
    mark("LOCAL_NATIVE_PASS")

    def allFiles(): Set[File] = {
      withResource(Files.walk(Paths.get(SparkEnv.get.conf.get("spark.local.dir")))) { paths =>
        paths.iterator().asScala.filter(Files.isRegularFile(_)).map(_.toFile).toSet
      }
    }
    val filesBeforeSpill = allFiles()
    val beforeSpill = SpillFramework.stores.diskStore.numHandles
    require(SpillFramework.stores.hostStore.spill(NativeRuntime.bytes) >= NativeRuntime.bytes)
    require(SpillFramework.stores.diskStore.numHandles > beforeSpill)
    val spillFiles = allFiles() -- filesBeforeSpill
    require(spillFiles.nonEmpty && spillFiles.forall(_.exists()))
    recordAccounting("rapids-spilled")
    batch.verify()
    mark("RAPIDS_SPILL_RESTORE_PASS")

    bm.blockInfoManager.lockForWriting(id).get
    try {
      bm.dropFromMemory[NativeBatch](id, () => Left(Array(batch)))
    } finally { bm.releaseLock(id) }
    require(!bm.memoryStore.contains(id) && bm.diskStore.contains(id))
    require(NativeRuntime.closed.get == 1)
    require(SpillFramework.stores.diskStore.numHandles == 0)
    require(spillFiles.forall(!_.exists()))
    val diskResult = bm.getLocalValues(id).get
    val restored = diskResult.data.next().asInstanceOf[NativeBatch]
    restored.verify()
    require(!diskResult.data.hasNext)
    require(bm.memoryStore.contains(id))
    require(NativeRuntime.reads.get == 1)
    mark("BLOCKMANAGER_DISK_RELOAD_PASS")

    val ready = new CountDownLatch(1)
    val letGo = new CountDownLatch(1)
    val removed = new CountDownLatch(1)
    val error = new AtomicReference[Throwable]()
    val independent = new AtomicReference[HostMemoryBuffer]()
    val seed = restored.seed
    val reader = new Thread(() => {
      try {
        val result = bm.getLocalValues(id).get
        val value = result.data.next().asInstanceOf[NativeBatch]
        val host = value.payload.getDataHostBuffer()
        independent.set(host)
        value.verify(host)
        ready.countDown()
        require(letGo.await(30, TimeUnit.SECONDS))
        require(!result.data.hasNext)
      } catch { case t: Throwable => error.set(t); ready.countDown() }
    }, "paused-native-reader")
    reader.start()
    require(ready.await(30, TimeUnit.SECONDS))
    Option(error.get).foreach(throw _)
    val remover = new Thread(() => {
      try { bm.master.removeRdd(900001, blocking = true) }
      catch { case t: Throwable => error.set(t) }
      finally { removed.countDown() }
    }, "blocking-native-removal")
    remover.start()
    require(!removed.await(250, TimeUnit.MILLISECONDS), "blocking removal ignored live reader")
    letGo.countDown()
    reader.join(30000)
    require(removed.await(30, TimeUnit.SECONDS))
    Option(error.get).foreach(throw _)
    withResource(independent.get) { host =>
      require(host.getLong(0) == seed.toLong)
      require(host.getLong(NativeRuntime.bytes - 8) == seed + NativeRuntime.bytes / 8 - 1)
    }
    require(NativeRuntime.closed.get == 2)
    require(!bm.memoryStore.contains(id) && !bm.diskStore.contains(id))
    NativeRuntime.assertClean()
    mark("BLOCKING_REMOVAL_AND_INDEPENDENT_REFERENCE_PASS")

    if (!spark.sparkContext.isLocal) {
      val remoteIds = spark.sparkContext.parallelize(0 until 2, 2).map { index =>
        NativeRuntime.init()
        val remoteBm = SparkEnv.get.blockManager
        val remoteId = RDDBlockId(900002, index)
        require(remoteBm.putIterator(remoteId, Iterator(NativeRuntime.make(100 + index)),
          StorageLevel.MEMORY_AND_DISK))
        (index, SparkEnv.get.executorId)
      }.collect()
      remoteIds.foreach { case (index, executor) =>
        require(executor != "driver")
        val remote = bm.getRemoteValues[NativeBatch](RDDBlockId(900002, index)).get
        val value = remote.data.next().asInstanceOf[NativeBatch]
        withResource(value) { b => b.verify(); require(!remote.data.hasNext) }
      }
      bm.master.removeRdd(900002, blocking = true)
      val cleaned = spark.sparkContext.parallelize(0 until 2, 2).map { _ =>
        NativeRuntime.init()
        Thread.sleep(1000)
        NativeRuntime.assertClean()
        (SparkEnv.get.executorId, NativeRuntime.created.get, NativeRuntime.closed.get,
          SpillFramework.stores.hostStore.numHandles,
          SpillFramework.stores.diskStore.numHandles,
          SparkEnv.get.memoryManager.offHeapStorageMemoryUsed,
          HostAlloc.getUsageRatio())
      }.collect()
      require(cleaned.map(_._1).toSet == remoteIds.map(_._2).toSet)
      cleaned.foreach(row => mark(s"EXECUTOR_CLEAN $row"))
      NativeRuntime.assertClean()
      mark("MULTI_JVM_REMOTE_NATIVE_PASS")
    }
    require(NativeRuntime.writes.get >= 1 && NativeRuntime.reads.get >= 1)
    mark(s"OWNERS created=${NativeRuntime.created.get} closed=${NativeRuntime.closed.get}")
    mark(s"HOST_LIMIT bytes=${NativeRuntime.limit} ratio=${HostAlloc.getUsageRatio()}")
    mark("LIFETIME_PATHS_PASS")
    SpillFramework.shutdown()
    require(HostAlloc.getUsageRatio() == 0.0)
    mark("FRAMEWORK_SHUTDOWN_HOST_ZERO_PASS")
    spark.stop()
  }
}
