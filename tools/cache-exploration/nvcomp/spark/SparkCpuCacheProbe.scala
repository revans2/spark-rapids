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
package org.apache.spark.sql.cacheexploration

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/** Real Spark cache CPU/GPU interoperability prerequisites; no performance claims. */
object SparkCpuCacheProbe {
  private def check(frame: DataFrame, expected: Array[Row], gpu: Boolean, label: String): Unit = {
    require(frame.collect().sameElements(expected), label + " values/nulls/order")
    val plan = frame.queryExecution.executedPlan.toString()
    require(plan.contains("GpuInMemoryTableScan") == gpu, label + " execution mode: " + plan)
    require(plan.contains("InMemoryTableScan"), label + " missing cache read")
    println("PASS " + label)
  }

  def main(args: Array[String]): Unit = {
    val name = args.headOption.getOrElse("snappy")
    val serializer = name match {
      case "uncompressed" => classOf[SparkNvcompUncompressedSerializer]
      case "lz4" => classOf[SparkNvcompLz4Serializer]
      case "snappy" => classOf[SparkNvcompSnappySerializer]
      case "zstd" => classOf[SparkNvcompZstdSerializer]
      case _ => throw new IllegalArgumentException("Unsupported CPU probe codec")
    }
    val spark = SparkSession.builder().appName("CPU packed cache interoperability")
      .config("spark.sql.cache.serializer", serializer.getName)
      .config("spark.sql.adaptive.enabled", false)
      .config("spark.rapids.sql.enabled", false)
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try {
      for (writeGpu <- Seq(false, true)) {
        spark.conf.set("spark.rapids.sql.enabled", writeGpu)
        val expected = Array.tabulate[Row](1025) { i =>
          Row(i.toLong, if (i % 13 == 0) null else Int.box(i * 37),
            if (i % 17 == 0) null else if (i % 19 == 0) "" else "µ-雪-" + i,
            null, null, null)
        }
        val cached = spark.range(0, 1025, 1, 4).selectExpr("id",
          "cast(case when id % 13 = 0 then null else id * 37 end as int) as key",
          "case when id % 17 = 0 then cast(null as string) when id % 19 = 0 then '' " +
            "else concat('µ-雪-', cast(id as string)) end as payload",
          "cast(null as long) as empty_long", "cast(null as int) as empty_int",
          "cast(null as string) as empty_string").persist()
        try {
          require(cached.count() == 1025, "Population count")
          for (readGpu <- Seq(false, true)) {
            spark.conf.set("spark.rapids.sql.enabled", readGpu)
            val label = name + " writeGpu=" + writeGpu + " readGpu=" + readGpu
            check(cached.selectExpr("*"), expected, readGpu, label + " full")
            check(cached.selectExpr("payload", "key", "id", "id as again"),
              expected.map(r => Row(r.get(2), r.get(1), r.get(0), r.get(0))),
              readGpu, label + " projection/reorder/duplicate")
            check(cached.selectExpr("count(*) as n"), Array(Row(1025L)),
              readGpu, label + " count-only")
          }
        } finally { cached.unpersist(blocking = true) }
      }
      // CPU population must form multiple bounded records without a GPU split.
      spark.conf.set("spark.rapids.sql.enabled", false)
      val large = spark.range(0, 3000, 1, 1).selectExpr("id",
        "cast(id * 7 as int) as key",
        "case when id % 17 = 0 then cast(null as string) when id % 19 = 0 then '' " +
          "else repeat('雪', 2048) end as payload").persist()
      val expected = Array.tabulate[Row](3000) { i =>
        Row(i.toLong, i * 7,
          if (i % 17 == 0) null else if (i % 19 == 0) "" else "雪" * 2048)
      }
      try {
        require(large.count() == 3000, "Multi-record population")
        val relation = spark.sharedState.cacheManager.lookupCachedData(large).get.cachedRepresentation
        val records = relation.cacheBuilder.cachedColumnBuffers.collect()
          .map(_.asInstanceOf[NvcompCacheRecord])
        require(records.length > 1 && records.map(_.numRows).sum == 3000,
          "CPU input failed to create multiple records")
        require(records.forall(record => java.nio.ByteBuffer.wrap(record.bytes)
          .order(java.nio.ByteOrder.LITTLE_ENDIAN).getLong(24) <= 8L * 1024 * 1024),
          "CPU record exceeded packed bound")
        for (readGpu <- Seq(false, true)) {
          spark.conf.set("spark.rapids.sql.enabled", readGpu)
          check(large.selectExpr("*"), expected, readGpu, name + " multi-record readGpu=" + readGpu)
        }
        spark.conf.set("spark.rapids.sql.enabled", false)
        val before = org.apache.spark.sql.util.ArrowUtils.rootAllocator.getAllocatedMemory
        require(large.limit(1).collect().sameElements(expected.take(1)), "Partial CPU read")
        require(org.apache.spark.sql.util.ArrowUtils.rootAllocator.getAllocatedMemory == before,
          "CPU partial-read TaskEnd leaked Arrow allocation")
        println("PASS CPU partial-read TaskEnd Arrow cleanup")
      } finally { large.unpersist(blocking = true) }
      spark.conf.set("spark.rapids.sql.enabled", false)
      val duplicate = spark.range(0, 31, 1, 2).selectExpr(
        "id as duplicate", "cast(id + 7 as int) as duplicate").persist()
      try {
        val wanted = Array.tabulate[Row](31)(i => Row(i.toLong, i + 7))
        require(duplicate.count() == 31, "Duplicate-name CPU population")
        for (gpu <- Seq(false, true)) {
          spark.conf.set("spark.rapids.sql.enabled", gpu)
          check(duplicate.selectExpr("*"), wanted, gpu, name + " duplicate cached field names")
        }
      } finally { duplicate.unpersist(blocking = true) }
      spark.conf.set("spark.rapids.sql.enabled", false)
      val empty = spark.range(0).persist()
      try { require(empty.collect().isEmpty, "Empty CPU cache") }
      finally { empty.unpersist(blocking = true) }
      val noColumns = spark.range(0, 17, 1, 2).select().persist()
      try {
        require(noColumns.count() == 17, "Zero-column CPU population/count")
        require(noColumns.collect().length == 17, "Zero-column CPU cached row count")
      } finally { noColumns.unpersist(blocking = true) }
      println("PASS CPU empty and zero-column cache")
      println("PASS CPU Spark ArrowWriter -> packed/compressed -> CPU/GPU cache")
    } finally { spark.stop() }
  }
}
