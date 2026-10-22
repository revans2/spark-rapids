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
package org.apache.spark.sql.codecbenefit

import java.io._
import java.lang.management.{ManagementFactory, MemoryType}
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong
import javax.management.{Notification, NotificationEmitter, NotificationListener}
import javax.management.openmbean.CompositeData

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import com.sun.management.GarbageCollectionNotificationInfo
import org.apache.spark.SparkEnv
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.columnar._
import org.apache.spark.sql.execution.columnar.SqlCacheCodecProvider
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.{RDDBlockId, StorageLevel}

object Counters {
  val generated = new AtomicLong()
  val generatedBytes = new AtomicLong()
  val validated = new AtomicLong()
  val validatedBytes = new AtomicLong()
  val javaWrites = new AtomicLong()
  val javaReads = new AtomicLong()
  val codecWrites = new AtomicLong()
  val codecReads = new AtomicLong()
}

/** Both modes own identical heap arrays; generic Java serialization is permitted. */
final class HeapRecord(val seed: Long, val payload: Array[Byte])
    extends CachedBatch with Serializable {
  override def numRows: Int = 1
  override def sizeInBytes: Long = payload.length.toLong
  private def writeObject(out: ObjectOutputStream): Unit = {
    out.defaultWriteObject()
    Counters.javaWrites.incrementAndGet()
  }
  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()
    Counters.javaReads.incrementAndGet()
  }
}

final class BlobCodec extends Serializer with Serializable {
  override def newInstance(): SerializerInstance = new SerializerInstance {
    override def serialize[T: ClassTag](value: T): ByteBuffer =
      throw new UnsupportedOperationException("streaming only")
    override def deserialize[T: ClassTag](bytes: ByteBuffer): T =
      throw new UnsupportedOperationException("streaming only")
    override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T =
      throw new UnsupportedOperationException("streaming only")
    override def serializeStream(out: OutputStream): SerializationStream = new SerializationStream {
      private val data = new DataOutputStream(out)
      override def writeObject[T: ClassTag](value: T): SerializationStream = {
        val record = value.asInstanceOf[HeapRecord]
        data.writeLong(record.seed)
        data.writeInt(record.payload.length)
        data.write(record.payload)
        Counters.codecWrites.incrementAndGet()
        this
      }
      override def flush(): Unit = data.flush()
      override def close(): Unit = data.close()
    }
    override def deserializeStream(in: InputStream): DeserializationStream = new DeserializationStream {
      private val data = new DataInputStream(in)
      override def readObject[T: ClassTag](): T = {
        val seed = data.readLong()
        val length = data.readInt()
        require(length == SparkEnv.get.conf.getInt("spark.codecBenefit.recordBytes", 0))
        val payload = new Array[Byte](length)
        data.readFully(payload)
        Counters.codecReads.incrementAndGet()
        new HeapRecord(seed, payload).asInstanceOf[T]
      }
      override def close(): Unit = data.close()
    }
  }
}

class JavaBlobSerializer extends CachedBatchSerializer {
  override def supportsColumnarInput(schema: Seq[Attribute]): Boolean = false
  override def supportsColumnarOutput(schema: StructType): Boolean = false
  override def convertInternalRowToCachedBatch(input: RDD[InternalRow],
      schema: Seq[Attribute], level: StorageLevel, conf: SQLConf): RDD[CachedBatch] = {
    require(schema.size == 1 && schema.head.dataType == LongType)
    input.mapPartitions { rows =>
      val bytes = SparkEnv.get.conf.getInt("spark.codecBenefit.recordBytes", 0)
      rows.map { row =>
        val seed = row.getLong(0)
        val payload = new Array[Byte](bytes)
        java.util.Arrays.fill(payload, seed.toByte)
        Counters.generated.incrementAndGet()
        Counters.generatedBytes.addAndGet(bytes.toLong)
        new HeapRecord(seed, payload): CachedBatch
      }
    }
  }
  override def convertColumnarBatchToCachedBatch(input: RDD[ColumnarBatch],
      schema: Seq[Attribute], level: StorageLevel, conf: SQLConf): RDD[CachedBatch] =
    throw new UnsupportedOperationException
  override def buildFilter(predicates: Seq[Expression], attributes: Seq[Attribute])
      : (Int, Iterator[CachedBatch]) => Iterator[CachedBatch] = (_, it) => it
  override def convertCachedBatchToInternalRow(input: RDD[CachedBatch],
      cached: Seq[Attribute], selected: Seq[Attribute], conf: SQLConf): RDD[InternalRow] = {
    require(selected.size == 1)
    input.mapPartitions { records =>
      val projection = UnsafeProjection.create(Array[DataType](LongType))
      records.map { value =>
        val record = value.asInstanceOf[HeapRecord]
        val expected = record.seed.toByte
        var offset = 0
        while (offset < record.payload.length) {
          require(record.payload(offset) == expected, s"payload mismatch at $offset")
          offset += 1
        }
        Counters.validated.incrementAndGet()
        Counters.validatedBytes.addAndGet(record.payload.length.toLong)
        projection(new GenericInternalRow(Array[Any](record.seed))).copy()
      }
    }
  }
  override def convertCachedBatchToColumnarBatch(input: RDD[CachedBatch],
      cached: Seq[Attribute], selected: Seq[Attribute], conf: SQLConf): RDD[ColumnarBatch] =
    throw new UnsupportedOperationException
}

class CodecBlobSerializer extends JavaBlobSerializer with SqlCacheCodecProvider {
  override def cacheCodec(schema: Seq[Attribute]): Serializer = new BlobCodec
}

object CodecBenefitSpike {
  private val heap = ManagementFactory.getMemoryMXBean
  private val phasePeak = new AtomicLong()
  private val globalPeak = new AtomicLong()
  @volatile private var sampling = true
  @volatile private var phase = "setup"

  private def increase(target: AtomicLong, value: Long): Unit = {
    var previous = target.get()
    while (value > previous && !target.compareAndSet(previous, value)) previous = target.get()
  }
  private def jsonString(value: String): String =
    "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
  def event(kind: String, fields: (String, Any)*): Unit = {
    val encoded = (Seq("event" -> kind) ++ fields).map { case (key, value) =>
      val rendered = value match { case text: String => jsonString(text); case other => other.toString }
      jsonString(key) + ":" + rendered
    }.mkString("{", ",", "}")
    println("CODEC_BENEFIT " + encoded)
  }
  private def gcTime: Long = ManagementFactory.getGarbageCollectorMXBeans.asScala
    .map(_.getCollectionTime).filter(_ >= 0).sum
  private def gcCount: Long = ManagementFactory.getGarbageCollectorMXBeans.asScala
    .map(_.getCollectionCount).filter(_ >= 0).sum

  def main(args: Array[String]): Unit = {
    val mode = args(0)
    val reads = args(1).toInt
    val spark = SparkSession.builder().appName("Heap codec versus Java cache IO").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    val bytes = sc.getConf.getInt("spark.codecBenefit.recordBytes", 0)
    require(bytes > 0 && sc.master == "local[8]")
    require(sc.getConf.get("spark.serializer", "org.apache.spark.serializer.JavaSerializer") ==
      "org.apache.spark.serializer.JavaSerializer")
    require(!sc.getConf.getBoolean("spark.rdd.compress", false))
    require(!sc.getConf.getBoolean("spark.io.encryption.enabled", false))
    require(!sc.getConf.getBoolean("spark.memory.offHeap.enabled", false))
    require(!sc.getConf.contains("spark.memory.fraction"))
    require(!sc.getConf.contains("spark.storage.unrollMemoryCheckPeriod"))
    require(!sc.getConf.contains("spark.serializer.objectStreamReset"))
    val sampler = new Thread(() => {
      var nextProgress = System.nanoTime()
      while (sampling) {
        val used = heap.getHeapMemoryUsage.getUsed
        increase(phasePeak, used)
        increase(globalPeak, used)
        if (System.nanoTime() >= nextProgress) {
          event("heap_sample", "phase" -> phase, "heap_used_bytes" -> used,
            "phase_sampled_heap_peak_bytes" -> phasePeak.get(),
            "global_sampled_heap_peak_bytes" -> globalPeak.get(),
            "generated_records" -> Counters.generated.get(),
            "generated_payload_bytes" -> Counters.generatedBytes.get(),
            "validated_records" -> Counters.validated.get(),
            "validated_payload_bytes" -> Counters.validatedBytes.get(),
            "java_writes" -> Counters.javaWrites.get(), "java_reads" -> Counters.javaReads.get(),
            "codec_writes" -> Counters.codecWrites.get(), "codec_reads" -> Counters.codecReads.get())
          nextProgress = System.nanoTime() + 1000000000L
        }
        Thread.sleep(20)
      }
    }, "heap-occupancy-sampler")
    sampler.setDaemon(true)
    sampler.start()
    val heapPools = ManagementFactory.getMemoryPoolMXBeans.asScala
      .filter(_.getType == MemoryType.HEAP).map(_.getName).toSet
    ManagementFactory.getGarbageCollectorMXBeans.asScala.foreach { collector =>
      collector.asInstanceOf[NotificationEmitter].addNotificationListener(new NotificationListener {
        override def handleNotification(notification: Notification, handback: Any): Unit = {
          if (notification.getType == GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION) {
            val info = GarbageCollectionNotificationInfo.from(
              notification.getUserData.asInstanceOf[CompositeData])
            val after = info.getGcInfo.getMemoryUsageAfterGc.asScala
              .filter(entry => heapPools.contains(entry._1)).map(_._2.getUsed).sum
            event("gc_after", "phase" -> phase, "collector" -> info.getGcName,
              "action" -> info.getGcAction, "duration_ms" -> info.getGcInfo.getDuration,
              "after_heap_occupancy_bytes" -> after)
          }
        }
      }, null, null)
    }
    event("controls", "mode" -> mode, "record_bytes" -> bytes, "records_per_partition" -> 6,
      "partitions" -> 16, "task_threads" -> 8, "heap_max_bytes" -> heap.getHeapMemoryUsage.getMax,
      "collectors" -> ManagementFactory.getGarbageCollectorMXBeans.asScala.map(_.getName).mkString(","),
      "storage" -> "DISK_ONLY", "payload_kind" -> "synthetic encoded byte array, repeated seed byte")
    val frame = spark.range(0, 96, 1, 16).toDF("seed").persist(StorageLevel.DISK_ONLY)
    def run(label: String): Unit = {
      phase = label
      phasePeak.set(heap.getHeapMemoryUsage.getUsed)
      val beforeGc = gcTime
      val beforeCount = gcCount
      val beforeValidated = Counters.validated.get()
      val beforeValidatedBytes = Counters.validatedBytes.get()
      val started = System.nanoTime()
      event("phase_started", "phase" -> label)
      val values = frame.collect().map(_.getLong(0)).sorted
      require(values.toVector == (0L until 96L).toVector)
      val elapsed = (System.nanoTime() - started) / 1e9
      require(Counters.validated.get() - beforeValidated == 96)
      require(Counters.validatedBytes.get() - beforeValidatedBytes == 96L * bytes)
      event("phase_finished", "phase" -> label, "wall_s" -> elapsed,
        "gc_ms" -> (gcTime - beforeGc), "gc_count" -> (gcCount - beforeCount),
        "sampled_heap_peak_bytes" -> phasePeak.get(), "heap_used_end_bytes" ->
          heap.getHeapMemoryUsage.getUsed, "validated_records" -> 96,
        "validated_payload_bytes" -> (96L * bytes))
    }
    try {
      run("fill")
      val owner = spark.sharedState.cacheManager.lookupCachedData(frame).get
        .cachedRepresentation.cacheBuilder.cachedColumnBuffers
      require(owner.partitions.length == 16)
      require(owner.elementClassTag.isInstanceOf[CacheCodecClassTag[_]] == (mode == "codec"))
      val manager = SparkEnv.get.blockManager
      var totalDisk = 0L
      (0 until 16).foreach { index =>
        val id = RDDBlockId(owner.id, index)
        val status = manager.master.getLocationsAndStatus(id, manager.blockManagerId.host).get
        require(status.locations.nonEmpty && status.status.memSize == 0)
        totalDisk += status.status.diskSize
        event("block", "partition" -> index, "payload_records" -> 6,
          "payload_bytes" -> (6L * bytes), "disk_bytes" -> status.status.diskSize)
      }
      require(Counters.generated.get() == 96 && Counters.generatedBytes.get() == 96L * bytes)
      event("cache_filled", "generated_records" -> Counters.generated.get(),
        "generated_payload_bytes" -> Counters.generatedBytes.get(),
        "total_disk_bytes" -> totalDisk, "filled_partitions" -> 16)
      run("warmup")
      (1 to reads).foreach(index => run("read_" + index))
      require(Counters.generated.get() == 96)
      if (mode == "codec") {
        require(Counters.javaWrites.get() == 0 && Counters.javaReads.get() == 0)
        require(Counters.codecWrites.get() == 96)
        require(Counters.codecReads.get() == 96L * (reads + 2))
      } else {
        require(Counters.codecWrites.get() == 0 && Counters.codecReads.get() == 0)
        require(Counters.javaWrites.get() == 96)
        require(Counters.javaReads.get() == 96L * (reads + 2))
      }
      event("path_counts", "java_writes" -> Counters.javaWrites.get(),
        "java_reads" -> Counters.javaReads.get(), "codec_writes" -> Counters.codecWrites.get(),
        "codec_reads" -> Counters.codecReads.get(), "global_sampled_heap_peak_bytes" -> globalPeak.get())
      frame.unpersist(blocking = true)
      event("complete", "mode" -> mode)
    } finally {
      sampling = false
      sampler.join(1000)
      spark.stop()
    }
  }
}
