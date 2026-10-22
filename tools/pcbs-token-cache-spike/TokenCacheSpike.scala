/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.tokencachespike

import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark.{Partition, ShuffleDependency, SparkEnv, TaskContext}
import org.apache.spark.rdd.{MapPartitionsRDD, RDD}
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, GenericInternalRow, UnsafeProjection}
import org.apache.spark.sql.columnar.{CachedBatch, CachedBatchSerializer}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.{RDDBlockId, StorageLevel, StorageUtils}

case class SchedulerToken(owner: Int, partition: Int, generation: Long) extends Serializable

// A mechanism fixture: raw Long bytes in direct memory, not Parquet, PCBS, GPU or spill.
final class NativeBatch(val bytes: ByteBuffer, val numRows: Int) extends CachedBatch {
  override def sizeInBytes: Long = numRows.toLong * 8L
}

object FixtureStore {
  val entries = new ConcurrentHashMap[(Int, Int), Vector[NativeBatch]]()
  val owners = new ConcurrentHashMap[Int, String]()
  val hits = new AtomicLong()
  val misses = new AtomicLong()
  val tokenOnlyMisses = new AtomicLong()
  val producerComputes = new AtomicLong()
  val upstreamComputes = new AtomicLong()
  val disposed = new AtomicLong()
  val generations = new AtomicLong()
  val fetchFailures = new AtomicLong()

  def closeBatches(batches: Vector[NativeBatch]): Unit = batches.foreach { b =>
    StorageUtils.dispose(b.bytes)
    disposed.incrementAndGet()
  }
  def remove(owner: Int, partition: Int): Unit = {
    Option(entries.remove((owner, partition))).foreach(closeBatches)
  }
  def clearOwner(owner: Int): Unit = {
    entries.keySet().asScala.filter(_._1 == owner).toVector.foreach { key =>
      remove(key._1, key._2)
    }
  }
}

class TokenProducer(previous: RDD[InternalRow]) extends RDD[CachedBatch](previous) {
  override protected def getPartitions: Array[Partition] = firstParent[InternalRow].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[CachedBatch] = {
    FixtureStore.producerComputes.incrementAndGet()
    var buffer = ByteBuffer.allocateDirect(256)
    var count = 0
    try {
      firstParent[InternalRow].iterator(split, context).foreach { row =>
        if (buffer.remaining() < 8) {
          val expanded = ByteBuffer.allocateDirect(buffer.capacity() * 2)
          buffer.flip()
          expanded.put(buffer)
          StorageUtils.dispose(buffer)
          buffer = expanded
        }
        buffer.putLong(row.getLong(0))
        count += 1
      }
      buffer.flip()
      Iterator(new NativeBatch(buffer, count))
    } catch {
      case t: Throwable => StorageUtils.dispose(buffer); throw t
    }
  }

  override private[spark] def mapPartitionsInternal[U: ClassTag](
      f: Iterator[CachedBatch] => Iterator[U],
      preservesPartitioning: Boolean = false): RDD[U] = withScope {
    new TokenOwnerRDD[U, CachedBatch](this,
      (_: TaskContext, _: Int, it: Iterator[CachedBatch]) => f(it),
      preservesPartitioning)
  }
}

class TokenOwnerRDD[U: ClassTag, T: ClassTag](
    parent: RDD[T],
    suppliedFunction: (TaskContext, Int, Iterator[T]) => Iterator[U],
    preservesPartitioning: Boolean)
  extends MapPartitionsRDD[U, T](parent, suppliedFunction, preservesPartitioning) {

  @volatile private var requestedLevel = StorageLevel.NONE

  override def getStorageLevel: StorageLevel = requestedLevel

  override def persist(level: StorageLevel): this.type = {
    require(requestedLevel == StorageLevel.NONE || requestedLevel == level)
    requestedLevel = level
    // Deliberately do not call super.persist: RDD.iterator must reach our compute.
    sparkContext.persistRDD(this)
    sparkContext.cleaner.foreach(_.registerRDDForCleanup(this))
    FixtureStore.owners.put(id, getClass.getName)
    println(s"TOKEN_SPIKE OWNER_PERSIST owner=$id requested=$requestedLevel")
    this
  }

  override def unpersist(blocking: Boolean = false): this.type = {
    sparkContext.unpersistRDD(id, blocking)
    FixtureStore.clearOwner(id)
    FixtureStore.owners.remove(id)
    requestedLevel = StorageLevel.NONE
    println(s"TOKEN_SPIKE OWNER_UNPERSIST owner=$id blocking=$blocking")
    this
  }

  override def compute(split: Partition, context: TaskContext): Iterator[U] = {
    val key = (id, split.index)
    val manager = SparkEnv.get.blockManager
    val block = RDDBlockId(id, split.index)
    Option(FixtureStore.entries.get(key)) match {
      case Some(cached) =>
        FixtureStore.hits.incrementAndGet()
        // The token is NEVER passed to the SQL serializer or treated as CachedBatch data.
        cached.iterator.asInstanceOf[Iterator[U]]
      case None =>
        FixtureStore.misses.incrementAndGet()
        if (manager.getStatus(block).nonEmpty) {
          FixtureStore.tokenOnlyMisses.incrementAndGet()
          manager.removeBlock(block, tellMaster = true)
        }
        val created = super.compute(split, context).toVector
        require(created.forall(_.isInstanceOf[NativeBatch]), "Fixture supports SQL batch wrapper only")
        val native = created.map(_.asInstanceOf[NativeBatch])
        val previous = FixtureStore.entries.put(key, native)
        if (previous != null) FixtureStore.closeBatches(previous)
        val token = SchedulerToken(id, split.index, FixtureStore.generations.incrementAndGet())
        require(manager.putSingle(block, token, StorageLevel.MEMORY_ONLY, tellMaster = true),
          s"Could not publish scheduler token $block")
        println(s"TOKEN_SPIKE TOKEN_PUBLISH owner=$id partition=${split.index} " +
          s"rows=${native.map(_.numRows.toLong).sum} bytes=${native.map(_.sizeInBytes).sum}")
        created.iterator
    }
  }
}

class TokenCachedBatchSerializer extends CachedBatchSerializer {
  override def supportsColumnarInput(schema: Seq[Attribute]): Boolean = false
  override def supportsColumnarOutput(schema: StructType): Boolean = false

  override def convertInternalRowToCachedBatch(input: RDD[InternalRow],
      schema: Seq[Attribute], level: StorageLevel, conf: SQLConf): RDD[CachedBatch] = {
    require(schema.length == 1 && schema.head.dataType == LongType)
    new TokenProducer(input)
  }

  override def convertColumnarBatchToCachedBatch(input: RDD[ColumnarBatch],
      schema: Seq[Attribute], level: StorageLevel, conf: SQLConf): RDD[CachedBatch] =
    throw new UnsupportedOperationException("Fixture row input only")

  override def buildFilter(predicates: Seq[Expression], attributes: Seq[Attribute])
      : (Int, Iterator[CachedBatch]) => Iterator[CachedBatch] = (_, it) => it

  override def convertCachedBatchToInternalRow(input: RDD[CachedBatch],
      cached: Seq[Attribute], selected: Seq[Attribute], conf: SQLConf): RDD[InternalRow] = {
    require(selected.length <= 1)
    val includeValue = selected.nonEmpty
    val types = selected.map(_.dataType).toArray
    input.mapPartitions { batches =>
      val projection = UnsafeProjection.create(types)
      batches.flatMap { batch =>
        require(batch.isInstanceOf[NativeBatch], s"Token used as data: ${batch.getClass}")
        val native = batch.asInstanceOf[NativeBatch]
        val buffer = native.bytes.duplicate()
        Iterator.range(0, native.numRows).map { _ =>
          val value = buffer.getLong()
          val row = if (includeValue) new GenericInternalRow(Array[Any](value))
          else new GenericInternalRow(Array.empty[Any])
          projection(row).copy()
        }
      }
    }
  }

  override def convertCachedBatchToColumnarBatch(input: RDD[CachedBatch],
      cached: Seq[Attribute], selected: Seq[Attribute], conf: SQLConf): RDD[ColumnarBatch] =
    throw new UnsupportedOperationException("Fixture row output only")
}

object TokenCacheSpike {
  def event(name: String, fields: (String, Any)*): Unit = {
    def quoted(value: String): String =
      "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
    val values = (Seq("event" -> name) ++ fields).map { case (key, value) =>
      val encoded = value match {
        case x: String => quoted(x)
        case other => other.toString
      }
      quoted(key) + ":" + encoded
    }
    println("TOKEN_SPIKE_JSON " + values.mkString("{", ",", "}"))
  }

  def shuffleIds(rdd: RDD[_]): Set[Int] = {
    val visited = scala.collection.mutable.Set[Int]()
    val found = scala.collection.mutable.Set[Int]()
    def visit(current: RDD[_]): Unit = if (visited.add(current.id)) {
      current.dependencies.foreach {
        case shuffle: ShuffleDependency[_, _, _] =>
          found += shuffle.shuffleId
          visit(shuffle.rdd)
        case other => visit(other.rdd)
      }
    }
    visit(rdd)
    found.toSet
  }

  def eraseShuffleOutputs(ids: Set[Int]): Unit = {
    require(ids.nonEmpty, "Test requires a real upstream shuffle")
    ids.foreach { shuffleId =>
      // Local-mode manager removes every registered map output file but leaves
      // driver tracker registration intact for the recomputation control.
      val tracker = SparkEnv.get.mapOutputTracker
        .asInstanceOf[org.apache.spark.MapOutputTrackerMaster]
      val disk = SparkEnv.get.blockManager.diskBlockManager
      def dataFiles = disk.getAllFiles().count(file =>
        file.getName.startsWith(s"shuffle_${shuffleId}_") && file.getName.endsWith(".data"))
      val filesBefore = dataFiles
      val outputsBefore = tracker.getNumAvailableOutputs(shuffleId)
      require(filesBefore > 0 && outputsBefore > 0)
      require(SparkEnv.get.shuffleManager.unregisterShuffle(shuffleId))
      tracker.unregisterAllMapAndMergeOutput(shuffleId)
      require(dataFiles == 0 && tracker.getNumAvailableOutputs(shuffleId) == 0)
      event("shuffle_erased", "shuffle" -> shuffleId,
        "data_files_before" -> filesBefore, "data_files_after" -> dataFiles,
        "outputs_before" -> outputsBefore,
        "outputs_after" -> tracker.getNumAvailableOutputs(shuffleId))
    }
    println(s"TOKEN_SPIKE SHUFFLE_OUTPUTS_ERASED ids=${ids.toSeq.sorted.mkString(",")}")
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Disposable scheduler token mechanism").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    sc.addSparkListener(new SparkListener {
      override def onTaskEnd(end: SparkListenerTaskEnd): Unit = {
        if (end.reason.toString.contains("FetchFailed")) FixtureStore.fetchFailures.incrementAndGet()
      }
    })
    try {
      for (adaptive <- Seq(false, true)) {
        spark.conf.set("spark.sql.adaptive.enabled", adaptive.toString)
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
        val total = 10000L
        val source = sc.parallelize(0L until total, 4).mapPartitions { values =>
          FixtureStore.upstreamComputes.incrementAndGet()
          values.map(value => Row(value))
        }
        val schema = StructType(Seq(StructField("value", LongType, nullable = false)))
        val frame = spark.createDataFrame(source, schema).repartition(6)
          .persist(StorageLevel.MEMORY_AND_DISK)
        val relation = CacheProbeCompatibility.cachedRelation(spark, frame)
        val owner = relation.cacheBuilder.cachedColumnBuffers
        require(owner.isInstanceOf[TokenOwnerRDD[_, _]], s"Wrong actual owner ${owner.getClass}")
        require(sc.getPersistentRDDs.get(owner.id).contains(owner))
        def exactRead(label: String): Unit = {
          val rows = frame.collect().map(_.getLong(0)).sorted
          require(rows.toSeq == (0L until total))
          val aggregate = frame.selectExpr("count(*) AS n", "sum(value) AS s").collect().head
          require(aggregate.getLong(0) == total && aggregate.getLong(1) == total * (total - 1) / 2)
          println(s"TOKEN_SPIKE READ_OK aqe=$adaptive label=$label rows=${rows.length}")
          event("read", "aqe" -> adaptive, "label" -> label, "rows" -> rows.length,
            "owner" -> owner.id, "hits" -> FixtureStore.hits.get(),
            "misses" -> FixtureStore.misses.get(),
            "upstream_computes" -> FixtureStore.upstreamComputes.get(),
            "producer_computes" -> FixtureStore.producerComputes.get())
        }
        exactRead("fill")
        require(relation.cacheBuilder.rowCountStats.value == total)
        require(relation.cacheBuilder.sizeInBytesStats.value == total * 8)
        event("initial_stats", "aqe" -> adaptive, "owner" -> owner.id,
          "owner_class" -> owner.getClass.getName,
          "cached_plan_class" -> relation.cachedPlan.getClass.getName,
          "row_count" -> relation.cacheBuilder.rowCountStats.value,
          "encoded_bytes" -> relation.cacheBuilder.sizeInBytesStats.value,
          "partitions" -> owner.partitions.length,
          "requested_level" -> owner.getStorageLevel.toString)
        val ids = shuffleIds(owner)
        require(ids.nonEmpty)
        val manager = SparkEnv.get.blockManager
        owner.partitions.indices.foreach { partition =>
          val block = RDDBlockId(owner.id, partition)
          require(manager.master.getLocations(block).nonEmpty)
          require(manager.getStatus(block).exists(_.memSize < 1024))
          val tokenData = manager.get[SchedulerToken](block).get.data.toVector
          require(tokenData.size == 1 && tokenData.head.asInstanceOf[SchedulerToken].owner == owner.id)
          val native = FixtureStore.entries.get((owner.id, partition))
          event("token", "aqe" -> adaptive, "owner" -> owner.id, "partition" -> partition,
            "token_class" -> tokenData.head.getClass.getName,
            "token_mem_bytes" -> manager.getStatus(block).get.memSize,
            "native_rows" -> native.map(_.numRows.toLong).sum,
            "native_bytes" -> native.map(_.sizeInBytes).sum,
            "locations" -> manager.master.getLocations(block).length)
        }
        println(s"TOKEN_SPIKE ACTUAL_OWNER_OK aqe=$adaptive owner=${owner.id} " +
          s"partitions=${owner.partitions.length} shuffleIds=${ids.mkString(",")}")
        val beforeHits = FixtureStore.hits.get()
        val beforeUpstream = FixtureStore.upstreamComputes.get()
        eraseShuffleOutputs(ids)
        exactRead("tokens-intact-shuffle-missing")
        require(FixtureStore.hits.get() > beforeHits)
        require(FixtureStore.upstreamComputes.get() == beforeUpstream,
          "Scheduler reran upstream despite intact token locations")
        println(s"TOKEN_SPIKE SCHEDULER_CACHE_BOUNDARY_PASS aqe=$adaptive upstream=$beforeUpstream")
        event("scheduler_boundary", "aqe" -> adaptive, "owner" -> owner.id,
          "upstream_before" -> beforeUpstream,
          "upstream_after" -> FixtureStore.upstreamComputes.get())

        val missing = 0
        manager.removeBlock(RDDBlockId(owner.id, missing), tellMaster = true)
        FixtureStore.remove(owner.id, missing)
        exactRead("token-and-payload-removed")
        require(FixtureStore.upstreamComputes.get() > beforeUpstream,
          "Token removal did not allow upstream shuffle recomputation")
        println(s"TOKEN_SPIKE TOKEN_MISS_RECOMPUTE_PASS aqe=$adaptive")

        val failuresBefore = FixtureStore.fetchFailures.get()
        val upstreamBeforePayloadMiss = FixtureStore.upstreamComputes.get()
        eraseShuffleOutputs(ids)
        FixtureStore.remove(owner.id, missing)
        require(manager.master.getLocations(RDDBlockId(owner.id, missing)).nonEmpty)
        exactRead("payload-missing-token-present")
        val fetchFailures = FixtureStore.fetchFailures.get() - failuresBefore
        require(FixtureStore.tokenOnlyMisses.get() > 0)
        require(FixtureStore.upstreamComputes.get() > upstreamBeforePayloadMiss)
        println(s"TOKEN_SPIKE PAYLOAD_ONLY_MISS_RECOVERED aqe=$adaptive " +
          s"fetchFailedTasks=$fetchFailures retryFallback=${fetchFailures > 0}")
        event("payload_only_miss", "aqe" -> adaptive, "fetch_failed_tasks" -> fetchFailures,
          "retry_fallback" -> (fetchFailures > 0), "owner" -> owner.id)

        frame.unpersist(blocking = true)
        require(!sc.getPersistentRDDs.contains(owner.id))
        require(!FixtureStore.entries.keySet().asScala.exists(_._1 == owner.id))
        require(owner.partitions.indices.forall(p =>
          manager.master.getLocations(RDDBlockId(owner.id, p)).isEmpty))
        println(s"TOKEN_SPIKE CLEANUP_PASS aqe=$adaptive owner=${owner.id}")
      }

      spark.conf.set("spark.sql.adaptive.enabled", "false")
      val tiny = spark.range(0, 2, 1, 6).selectExpr("id AS value")
        .persist(StorageLevel.MEMORY_AND_DISK)
      val tinyRelation = CacheProbeCompatibility.cachedRelation(spark, tiny)
      val tinyOwner = tinyRelation.cacheBuilder.cachedColumnBuffers
      require(tiny.collect().map(_.getLong(0)).sorted.sameElements(Array(0L, 1L)))
      val emptyPartitions = FixtureStore.entries.entrySet().asScala.count(e =>
        e.getKey._1 == tinyOwner.id && e.getValue.map(_.numRows).sum == 0)
      require(emptyPartitions >= 4)
      tiny.unpersist(blocking = true)
      require(FixtureStore.entries.isEmpty)
      println(s"TOKEN_SPIKE EMPTY_PARTITIONS_PASS count=$emptyPartitions")
      println("TOKEN_SPIKE ALL_MECHANISM_CHECKS_PASS")
    } finally {
      FixtureStore.entries.keySet().asScala.map(_._1).toSet.foreach(FixtureStore.clearOwner)
      spark.stop()
    }
  }
}
