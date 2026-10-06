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

/*** spark-rapids-shim-json-lines
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "359"}
{"spark": "400"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.iceberg.spark.source

import java.util.{Collections, HashMap => JHashMap}
import java.util.function.LongSupplier

import com.nvidia.spark.rapids.iceberg.IcebergProbeImpl
import org.apache.iceberg.SnapshotSummary
import org.apache.iceberg.spark.SparkReadOptions
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * The hand-off from the plugin to the root-level Iceberg table wrapper, and the pieces around
 * it, without Spark: no session, no catalog, no table.
 */
class IcebergSplitHandoffSuite extends AnyFunSuite with BeforeAndAfterEach {

  private val table = "cat.ns.events"

  override def afterEach(): Unit = IcebergSplitAdvisor.uninstall()

  private def options(entries: (String, String)*): CaseInsensitiveStringMap = {
    val map = new JHashMap[String, String]()
    entries.foreach { case (k, v) => map.put(k, v) }
    new CaseInsensitiveStringMap(map)
  }

  private def listed(bytes: Long): LongSupplier = new LongSupplier {
    override def getAsLong: Long = bytes
  }

  private val unread: LongSupplier = new LongSupplier {
    override def getAsLong: Long = fail("listed bytes read without an advisor decision")
  }

  test("advisor: no decision until installed, and again once removed") {
    assert(!IcebergSplitAdvisor.isInstalled)
    assert(IcebergSplitAdvisor.learnedSplitBytes(table, 1L) == IcebergSplitAdvisor.NO_DECISION)
    IcebergSplitAdvisor.install((t: String, l: Long) => if (t == table) l / 4 else -1L)
    assert(IcebergSplitAdvisor.isInstalled)
    assert(IcebergSplitAdvisor.learnedSplitBytes(table, 400L) == 100L)
    IcebergSplitAdvisor.uninstall()
    assert(!IcebergSplitAdvisor.isInstalled)
    assert(IcebergSplitAdvisor.learnedSplitBytes(table, 400L) == IcebergSplitAdvisor.NO_DECISION)
  }

  test("advisor: a throwing advisor means no decision, never a failed scan") {
    IcebergSplitAdvisor.install((_: String, _: Long) => throw new IllegalStateException("boom"))
    assert(IcebergSplitAdvisor.learnedSplitBytes(table, 1L) == IcebergSplitAdvisor.NO_DECISION)
    IcebergSplitAdvisor.install((_: String, _: Long) => throw new NoClassDefFoundError("gone"))
    assert(IcebergSplitAdvisor.learnedSplitBytes(table, 1L) == IcebergSplitAdvisor.NO_DECISION)
  }

  test("probe: a non-positive split from the plugin means no decision; None removes it") {
    val probe = new IcebergProbeImpl
    probe.installScanSplitAdvisor(Some((_, listedBytes) => listedBytes - 10L))
    assert(IcebergSplitAdvisor.learnedSplitBytes(table, 110L) == 100L)
    assert(IcebergSplitAdvisor.learnedSplitBytes(table, 10L) == IcebergSplitAdvisor.NO_DECISION)
    assert(IcebergSplitAdvisor.learnedSplitBytes(table, 5L) == IcebergSplitAdvisor.NO_DECISION)
    probe.installScanSplitAdvisor(None)
    assert(!IcebergSplitAdvisor.isInstalled)
  }

  test("table: no advisor leaves the options untouched and reads nothing") {
    val merged = options("other" -> "x")
    assert(RapidsSparkTable.withLearnedSplitSize(merged, options(), table, unread) eq merged)
  }

  test("table: an explicit DataFrame split size wins over the learned one") {
    IcebergSplitAdvisor.install((_: String, _: Long) => 100L)
    val caller = options(SparkReadOptions.SPLIT_SIZE -> "7")
    assert(RapidsSparkTable.withLearnedSplitSize(caller, caller, table, unread) eq caller)
  }

  test("table: the learned split outranks a session-level setting") {
    IcebergSplitAdvisor.install((t: String, l: Long) => if (t == table) l / 4 else -1L)
    // A split size the session settings merged in, which the caller did not ask for.
    val merged = options(SparkReadOptions.SPLIT_SIZE -> "7", "other" -> "x")
    val out = RapidsSparkTable.withLearnedSplitSize(merged, options(), table, listed(400L))
    assert(out.get(SparkReadOptions.SPLIT_SIZE) == "100")
    assert(out.get("other") == "x")
  }

  test("table: no learned split leaves every lower-precedence source in place") {
    IcebergSplitAdvisor.install((_: String, _: Long) => IcebergSplitAdvisor.NO_DECISION)
    val merged = options(SparkReadOptions.SPLIT_SIZE -> "7")
    assert(RapidsSparkTable.withLearnedSplitSize(merged, options(), table, listed(1L)) eq merged)
  }

  test("table: whole-table size from the snapshot summary, 0 when absent or malformed") {
    val key = SnapshotSummary.TOTAL_FILE_SIZE_PROP
    assert(RapidsSparkTable.totalFileSizeBytes(Collections.singletonMap(key, "12345")) == 12345L)
    assert(RapidsSparkTable.totalFileSizeBytes(Collections.singletonMap(key, " 9 ")) == 9L)
    assert(RapidsSparkTable.totalFileSizeBytes(Collections.singletonMap(key, "n/a")) == 0L)
    assert(RapidsSparkTable.totalFileSizeBytes(Collections.emptyMap[String, String]()) == 0L)
    assert(RapidsSparkTable.totalFileSizeBytes(null) == 0L)
  }
}
