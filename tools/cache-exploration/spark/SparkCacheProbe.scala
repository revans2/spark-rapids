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

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/** Small correctness/plan probe. It is deliberately not a performance or pressure benchmark. */
object SparkCacheProbe {
  def main(args: Array[String]): Unit = {
    val format = args.headOption.getOrElse("vortex")
    val population = if (args.length > 1) args(1) else "cpu"
    val fixture = if (args.length > 2) args(2) else "strings-low"
    val rows = if (args.length > 3) args(3).toInt else 1024
    val consumers = if (args.length > 4) args(4).split(",") else Array("cpu")
    require(Set("cpu", "gpu").contains(population))
    require(consumers.forall(Set("cpu", "gpu").contains))
    require(Set("fixed", "fixed-edge", "strings-low", "strings-wide").contains(fixture))
    require(rows > 0 && rows <= 100000, "Correctness probe is bounded to 100000 rows")
    val serializer = format match {
      case "vortex" => classOf[SparkVortexSerializer].getName
      case "vortex-direct" => classOf[SparkVortexDirectBufferSerializer].getName
      case "pcbs" => "com.nvidia.spark.ParquetCachedBatchSerializer"
      case "default" => "org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer"
      case _ => throw new IllegalArgumentException("format must be vortex/vortex-direct/pcbs/default")
    }
    val needsGpu = population == "gpu" || consumers.contains("gpu")
    val builder = SparkSession.builder().master("local[2]")
      .appName("Vortex cache correctness probe")
      .config("spark.sql.cache.serializer", serializer)
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.rapids.sql.enabled", (population == "gpu").toString)
      .config("spark.rapids.sql.explain", "ALL")
    if (needsGpu) builder.config("spark.plugins", "com.nvidia.spark.SQLPlugin")
    val spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try {
      val expressions = Seq(
        "id * 17L - 991L AS id",
        "CAST(CASE WHEN id % 13 = 0 THEN NULL ELSE (id * 37) % 1009 END AS INT) AS key") ++
        (if (fixture == "fixed-edge") Seq(
          "CAST(NULL AS LONG) AS all_null_long",
          "CAST(NULL AS INT) AS all_null_int",
          "id * 29L - 17L AS all_valid_long",
          "CAST(id * 11 - 23 AS INT) AS all_valid_int")
        else if (fixture == "fixed") Seq.empty else Seq(
          if (fixture == "strings-low")
            "CASE WHEN id % 17 = 0 THEN NULL ELSE concat('group-', id % 32) END AS payload"
          else "CASE WHEN id % 17 = 0 THEN NULL " +
            "ELSE concat('row-', id, '-varying-value') END AS payload"))
      val input = spark.range(0, rows, 1, 2).selectExpr(expressions: _*)
      val cached = input.persist(StorageLevel.MEMORY_AND_DISK)
      require(cached.count() == rows, "Cache population row count")
      println("POPULATION " + population + " FORMAT " + format + " PLAN\n" +
        input.queryExecution.executedPlan)
      consumers.foreach { mode =>
        spark.conf.set("spark.rapids.sql.enabled", mode == "gpu")
        val query = cached.select(cached.columns.head, cached.columns.tail: _*)
        val result = query.collect()
        require(result.length == rows, "Read row count")
        result.zipWithIndex.foreach { case (row, i) =>
          require(row.getLong(0) == i * 17L - 991L, "id/order " + i)
          require(row.isNullAt(1) == (i % 13 == 0), "key null " + i)
          if (!row.isNullAt(1)) require(row.getInt(1) == (i * 37) % 1009, "key " + i)
          if (fixture == "fixed-edge") {
            require(row.isNullAt(2) && row.isNullAt(3), "all-null columns " + i)
            require(row.getLong(4) == i * 29L - 17L, "all-valid long " + i)
            require(row.getInt(5) == i * 11 - 23, "all-valid int " + i)
          } else if (fixture != "fixed") {
            val expected = if (i % 17 == 0) null
              else if (fixture == "strings-low") "group-" + (i % 32)
              else "row-" + i + "-varying-value"
            require(row.getString(2) == expected, "payload " + i)
          }
        }
        val plan = query.queryExecution.executedPlan.toString()
        if (mode == "gpu" && format != "default") {
          require(plan.contains("GpuInMemoryTableScan"), "Direct GPU cache scan missing: " + plan)
        }
        if (mode == "cpu") {
          require(!plan.contains("GpuInMemoryTableScan"), "GPU cache scan in CPU-off path")
        }
        val narrow = cached.select("key", "id")
        val narrowRows = narrow.collect()
        require(narrowRows.length == rows, "Narrow row count")
        narrowRows.zipWithIndex.foreach { case (row, i) =>
          require(row.getLong(1) == i * 17L - 991L, "reordered id " + i)
          require(row.isNullAt(0) == (i % 13 == 0), "narrow null " + i)
          if (!row.isNullAt(0)) require(row.getInt(0) == (i * 37) % 1009, "narrow key " + i)
        }
        if (fixture == "fixed-edge") {
          val reversed = cached.select(cached.columns.reverse.head,
            cached.columns.reverse.tail: _*).collect()
          require(reversed.length == rows, "Reversed row count")
          reversed.zip(result).foreach { case (actual, original) =>
            require(actual.toSeq == original.toSeq.reverse, "Reversed field values/nulls")
          }
          val ids = cached.select("id").collect()
          require(ids.length == rows, "ID-only row count")
          ids.zipWithIndex.foreach { case (row, i) =>
            require(row.getLong(0) == i * 17L - 991L, "ID-only order " + i)
          }
        }
        println("CONSUMER " + mode + " ROWS " + result.length + " PLAN\n" + plan)
        println("NARROW REORDERED PLAN\n" + narrow.queryExecution.executedPlan)
      }
      cached.unpersist(blocking = true)
      println("PASS: same cache, all values/nulls/row order checked; no performance claim")
    } finally {
      spark.stop()
    }
  }
}
