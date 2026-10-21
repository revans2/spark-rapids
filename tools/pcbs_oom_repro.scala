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

// Manual heap-pressure reproduction; see docs/dev/pcbs-oom-investigation.md.
// Run in a dedicated spark-shell JVM. This script exits the JVM on completion.
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.storage.StorageLevel

try {
  val parts = sys.env.getOrElse("PCBS_PARTS", "8").toInt
  val rows = sys.env.getOrElse("PCBS_ROWS", "4000000").toLong
  val columns = sys.env.getOrElse("PCBS_COLUMNS", "16").toInt
  require(parts > 0 && rows > 0 && columns > 0)
  val df = spark.range(0L, Math.multiplyExact(rows, parts.toLong), 1L, parts)
    .selectExpr((0 until columns).map(i => s"rand($i) as c$i"): _*)
  df.persist(StorageLevel.MEMORY_AND_DISK)
  val relation = spark.sharedState.cacheManager.lookupCachedData(df).get.cachedRepresentation
  val buffers = relation.cacheBuilder.cachedColumnBuffers
  println("PCBS_PLAN " + relation.cacheBuilder.cachedPlan)
  println("PCBS_HEAP_MAX " + Runtime.getRuntime.maxMemory())
  Seq("spark.sql.cache.serializer", "spark.rapids.sql.batchSizeBytes",
    "spark.rapids.sql.concurrentGpuTasks", "spark.memory.fraction",
    "spark.serializer.objectStreamReset").foreach { key =>
    println(s"PCBS_CONFIG $key=${spark.conf.get(key, "unset")}")
  }
  // Populate one partition at a time, so creation pressure is distinct from reload pressure.
  for (p <- 0 until parts) {
    sc.runJob(buffers, (it: Iterator[CachedBatch]) => {
      it.foreach(b => println(s"PCBS_BATCH partition=$p rows=${b.numRows} " +
        s"bytes=${b.sizeInBytes} class=${b.getClass.getName}"))
    }, Seq(p))
    System.gc()
  }
  println("PCBS_POPULATED " + sc.getRDDStorageInfo.mkString(";"))
  System.gc()
  println("PCBS_RELOAD_START")
  // Read the real cached RDD directly, isolating deserialization from GPU decode allocations.
  val count = buffers.mapPartitions { it =>
    var n = 0L
    it.foreach(b => n += b.numRows)
    Iterator(n)
  }.collect().sum
  require(count == rows * parts, s"Unexpected row count: $count")
  println("PCBS_SUCCESS rows=" + count)
  spark.stop()
  System.exit(0)
} catch {
  case t: Throwable =>
    t.printStackTrace()
    System.exit(1)
}
