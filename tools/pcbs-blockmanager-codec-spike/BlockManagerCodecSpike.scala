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
package org.apache.spark.sql.codeccachespike

import java.io._
import java.nio.ByteBuffer

import scala.reflect.ClassTag

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.columnar._
import org.apache.spark.sql.execution.columnar.{InMemoryRelation, SqlCacheCodecProvider}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.{RDDBlockId, StorageLevel}

/** Deliberately rejects Java object serialization of a bulk cached record. */
final class BulkBatch(val columns: Int, val values: Array[Long])
    extends CachedBatch with Serializable with com.esotericsoftware.kryo.KryoSerializable {
  override def numRows: Int = values.length / columns
  override def sizeInBytes: Long = values.length.toLong * 8
  override def write(k: com.esotericsoftware.kryo.Kryo,
      out: com.esotericsoftware.kryo.io.Output): Unit =
    throw new IllegalStateException("GENERIC_KRYO_BULK_PATH_FORBIDDEN")
  override def read(k: com.esotericsoftware.kryo.Kryo,
      in: com.esotericsoftware.kryo.io.Input): Unit =
    throw new IllegalStateException("GENERIC_KRYO_BULK_PATH_FORBIDDEN")
  private def writeObject(out: ObjectOutputStream): Unit =
    throw new NotSerializableException("GENERIC_JAVA_BULK_PATH_FORBIDDEN")
}

final class LongCacheCodec(val columns: Int) extends Serializer with Serializable {
  override def newInstance(): SerializerInstance = new SerializerInstance {
    override def serialize[T: ClassTag](value: T): ByteBuffer =
      throw new UnsupportedOperationException("streaming only")
    override def deserialize[T: ClassTag](bytes: ByteBuffer): T =
      throw new UnsupportedOperationException("streaming only")
    override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T =
      throw new UnsupportedOperationException("streaming only")
    override def serializeStream(out: OutputStream): SerializationStream =
      new SerializationStream {
        private val data = new DataOutputStream(out)
        private var records = 0
        override def writeObject[T: ClassTag](value: T): SerializationStream = {
          val batch = value.asInstanceOf[BulkBatch]
          require(batch.columns == columns)
          data.writeInt(0x1abc0000 | columns)
          data.writeInt(batch.values.length)
          batch.values.foreach(data.writeLong)
          records += 1
          this
        }
        override def flush(): Unit = data.flush()
        override def close(): Unit = {
          data.close()
          println(s"CODEC_SPIKE WRITE executor=${SparkEnv.get.executorId} columns=$columns records=$records")
        }
      }
    override def deserializeStream(in: InputStream): DeserializationStream =
      new DeserializationStream {
        private val data = new DataInputStream(in)
        private var records = 0
        override def readObject[T: ClassTag](): T = {
          require(data.readInt() == (0x1abc0000 | columns), "wrong per-cache codec context")
          val length = data.readInt()
          require(length >= 0 && length % columns == 0 && length <= 1000000)
          val values = Array.fill(length)(data.readLong())
          records += 1
          new BulkBatch(columns, values).asInstanceOf[T]
        }
        override def close(): Unit = {
          data.close()
          println(s"CODEC_SPIKE READ executor=${SparkEnv.get.executorId} columns=$columns " +
            s"records=$records taskContext=${TaskContext.get() != null}")
        }
      }
  }
}

class ResearchCachedBatchSerializer extends CachedBatchSerializer with SqlCacheCodecProvider {
  override def cacheCodec(schema: Seq[Attribute]): Serializer = new LongCacheCodec(schema.length)
  override def supportsColumnarInput(schema: Seq[Attribute]): Boolean = false
  override def supportsColumnarOutput(schema: StructType): Boolean = false
  override def convertInternalRowToCachedBatch(input: RDD[InternalRow],
      schema: Seq[Attribute], level: StorageLevel, conf: SQLConf): RDD[CachedBatch] = {
    require(schema.nonEmpty && schema.forall(_.dataType == LongType))
    val columns = schema.length
    input.mapPartitions { rows =>
      rows.map(_.copy()).grouped(128).map { group =>
        new BulkBatch(columns, group.flatMap(row =>
          (0 until columns).map(row.getLong)).toArray): CachedBatch
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
    val indices = selected.map(a => cached.indexWhere(_.exprId == a.exprId)).toArray
    input.mapPartitions { batches =>
      val projection = UnsafeProjection.create(indices.map(_ => LongType: DataType))
      batches.flatMap { raw =>
        val batch = raw.asInstanceOf[BulkBatch]
        Iterator.range(0, batch.numRows).map { row =>
          projection(new GenericInternalRow(indices.map(i =>
            batch.values(row * batch.columns + i): Any))).copy()
        }
      }
    }
  }
  override def convertCachedBatchToColumnarBatch(input: RDD[CachedBatch],
      cached: Seq[Attribute], selected: Seq[Attribute], conf: SQLConf): RDD[ColumnarBatch] =
    throw new UnsupportedOperationException
}

object BlockManagerCodecSpike {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Disposable BlockManager codec proof").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._
    val manager = SparkEnv.get.blockManager
    val generic = sc.parallelize(0 until 1000, 2).persist(StorageLevel.DISK_ONLY)
    require(generic.sum() == 499500)
    require(generic.sum() == 499500)
    val plain = sc.parallelize(Seq(("alpha", 1), ("beta", 2)), 2)
      .persist(StorageLevel.DISK_ONLY)
    require(plain.collect().toSeq.sorted == Seq(("alpha", 1), ("beta", 2)))
    require(plain.collect().toSeq.sorted == Seq(("alpha", 1), ("beta", 2)))
    plain.unpersist(blocking = true)
    println("CODEC_SPIKE DEFAULT_RDD_PASS")
    Seq(classOf[SerializerManager],
      classOf[org.apache.spark.sql.execution.columnar.CachedRDDBuilder]).foreach { clazz =>
      val location = clazz.getProtectionDomain.getCodeSource.getLocation.toString
      require(location.endsWith("/blockmanager-codec-spike.jar"), s"wrong overlay: $location")
      println(s"CODEC_SPIKE OVERLAY class=${clazz.getName} location=$location")
    }
    val ordinary = org.apache.spark.sql.execution.columnar.CachedRDDBuilder(
      new org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer,
      StorageLevel.DISK_ONLY, spark.range(100).queryExecution.executedPlan, None)
    require(!ordinary.cachedColumnBuffers.elementClassTag.isInstanceOf[CacheCodecClassTag[_]])
    require(ordinary.cachedColumnBuffers.collect().map(_.numRows).sum == 100)
    require(ordinary.cachedColumnBuffers.collect().map(_.numRows).sum == 100)
    ordinary.clearCache(blocking = true)
    println("CODEC_SPIKE DEFAULT_SQL_WRAPPER_PASS")
    val levels = Seq(StorageLevel.MEMORY_ONLY_SER, StorageLevel.DISK_ONLY,
      StorageLevel.MEMORY_AND_DISK_2, StorageLevel.MEMORY_ONLY)
    levels.zipWithIndex.foreach { case (level, index) =>
      val frames = Seq(
        spark.range(10000, 11000, 1, 2).toDF("a"),
        spark.range(20000, 21000, 1, 2).selectExpr("id as a", "id * 3 as b"))
      frames.foreach(_.persist(level))
      frames.zipWithIndex.foreach { case (frame, colsIndex) =>
        val expected = if (colsIndex == 0) (10000L until 11000L).map(v => Seq(v)).toSet
          else (20000L until 21000L).map(v => Seq(v, v * 3)).toSet
        def check(): Unit = {
          val actual = frame.collect().map(row => (0 until row.length).map(row.getLong))
          require(actual.length == 1000 && actual.toSet == expected)
        }
        check()
        check()
        val relation = spark.sharedState.cacheManager.lookupCachedData(frame).get.cachedRepresentation
        val owner = relation.cacheBuilder.cachedColumnBuffers
        require(owner.elementClassTag.isInstanceOf[CacheCodecClassTag[_]])
        val tag = owner.elementClassTag
        val java = new JavaSerializer(sc.getConf).newInstance()
        val restored = java.deserialize[(StorageLevel, ClassTag[CachedBatch])](
          java.serialize((level, tag)))._2
        require(restored.isInstanceOf[CacheCodecClassTag[_]])
        require(restored.runtimeClass == tag.runtimeClass)
        require(restored.asInstanceOf[CacheCodecClassTag[_]].codec
          .asInstanceOf[LongCacheCodec].columns == colsIndex + 1)
        val decoded = manager.getRemoteValues[CachedBatch](RDDBlockId(owner.id, 0))(tag)
        if (!sc.isLocal) {
          require(decoded.nonEmpty, "driver must fetch from another executor JVM")
          val batches = decoded.get.data.toVector.map(_.asInstanceOf[BulkBatch])
          require(batches.map(_.numRows).sum == 500)
          require(batches.forall(_.columns == colsIndex + 1))
          val blockId = RDDBlockId(owner.id, 0)
          val location = manager.master.getLocations(blockId).head
          val wire = manager.blockTransferService.fetchBlockSync(
            location.host, location.port, location.executorId, blockId.name, null)
          try {
            val stream = wire.createInputStream()
            val wireRows = SparkEnv.get.serializerManager.dataDeserializeStream(
              blockId, stream)(tag).map(_.asInstanceOf[BulkBatch]).toVector
            require(wireRows.map(_.numRows).sum == 500)
            require(wireRows.forall(_.columns == colsIndex + 1))
            val first = if (colsIndex == 0) 10000L else 20000L
            val wireValues = wireRows.flatMap(batch =>
              batch.values.grouped(batch.columns).map(_.toSeq))
            val expectedWire = (first until first + 500).map { value =>
              if (colsIndex == 0) Seq(value) else Seq(value, value * 3)
            }
            require(wireValues == expectedWire)
          } finally wire.release()
          println(s"CODEC_SPIKE NETTY_PASS level=$index columns=${colsIndex + 1}")
          println(s"CODEC_SPIKE REMOTE_PASS level=$index columns=${colsIndex + 1}")
        }
        if (level.replication == 2 && !sc.isLocal) {
          (0 until 2).foreach { partition =>
            val locations = manager.master.getLocations(RDDBlockId(owner.id, partition))
            require(locations.size == 2, s"expected two replicas, got $locations")
          }
          println(s"CODEC_SPIKE REPLICAS_PASS columns=${colsIndex + 1}")
        }
        require(relation.cacheBuilder.sizeInBytesStats.value == 8000L * (colsIndex + 1))
        require(relation.cacheBuilder.rowCountStats.value == 1000L)
        println(s"CODEC_SPIKE SQL_PASS level=$index columns=${colsIndex + 1} owner=${owner.id}")
      }
      frames.foreach(_.unpersist(blocking = true))
    }
    generic.unpersist(blocking = true)
    org.apache.spark.storage.StorageRouteChecks.run()
    val tripwire = new BulkBatch(1, Array(1L))
    try {
      new JavaSerializer(sc.getConf).newInstance().serialize(tripwire)
      throw new IllegalStateException("tripwire unexpectedly serializable")
    } catch {
      case e: NotSerializableException => require(e.getMessage.contains("GENERIC_JAVA_BULK"))
    }
    try {
      new KryoSerializer(sc.getConf).newInstance().serialize(tripwire)
      throw new IllegalStateException("Kryo tripwire unexpectedly serializable")
    } catch {
      case e: Exception =>
        require(Iterator.iterate[Throwable](e)(_.getCause).takeWhile(_ != null)
          .exists(x => Option(x.getMessage).exists(_.contains("GENERIC_KRYO_BULK"))))
    }
    val empty = spark.range(0, 0, 1, 4).toDF("a").persist(StorageLevel.DISK_ONLY)
    require(empty.collect().isEmpty && empty.collect().isEmpty)
    empty.unpersist(blocking = true)
    println("CODEC_SPIKE EMPTY_PASS")
    println("CODEC_SPIKE TRIPWIRE_PASS")
    println("CODEC_SPIKE PRIMARY_PATHS_PASS")
    spark.stop()
  }
}
