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

package com.nvidia.spark.rapids.perf

import java.nio.file.{Files, Path, Paths}
import java.time.Clock

import com.nvidia.spark.history.{HistoryMetricCatalog, MetricStore, MetricStores}
import com.nvidia.spark.history.local.{LocalHistoryMetrics, LocalHistoryMetricsFactory,
  LocalProvenanceIdentity, LocalProvenanceSource, LocalSnapshotException}

import org.apache.spark.internal.Logging

/**
 * Owns the driver-side history metrics provider: open, install, persist, close. Knows nothing
 * about what is being measured.
 *
 * The provider holds everything in heap and never writes on its own, and each Spark application is
 * a fresh driver JVM, so what one application learns reaches the next only via an explicit save on
 * shutdown and restore on startup.
 *
 * The store is published through `MetricStores` because Spark builds plan nodes itself and they
 * cannot be handed a reference. Registration is non-owning and closes before the provider.
 */
class HistoryOwner private (
    private val owner: LocalHistoryMetrics,
    private val registration: AutoCloseable,
    private val snapshotPath: Path,
    private val policy: HistoryPolicy) extends Logging {

  def store: MetricStore = owner.store()

  /**
   * Detaches the store from the locator, drains admitted observations, publishes the snapshot,
   * then shuts the provider down. Deregistration comes first so nothing can record into a store
   * that is about to close.
   */
  def shutdown(): Unit = {
    try {
      registration.close()
    } catch {
      case t: Throwable if HistoryMetric.isContained(t) =>
        logDebug(s"History deregistration: $t")
    }
    try {
      if (!owner.drain(policy.drainBudget)) {
        logWarning("History drain incomplete; snapshot may omit the most recent observations")
      }
      try {
        Option(snapshotPath.getParent).foreach(Files.createDirectories(_))
        owner.save(snapshotPath, policy.saveBudget)
        logInfo(s"History snapshot written to $snapshotPath")
      } catch {
        case e: LocalSnapshotException =>
          logWarning(s"History snapshot not written (${e.reason}); this application's " +
            "observations will not carry over")
      }
    } catch {
      case t: Throwable if HistoryMetric.isContained(t) =>
        logWarning(s"History shutdown problem: $t")
    } finally {
      try {
        if (!owner.shutdown(policy.shutdownBudget)) {
          logWarning("History provider did not shut down within its budget")
        }
      } catch {
        case t: Throwable if HistoryMetric.isContained(t) => logDebug(s"History shutdown: $t")
      }
    }
  }
}

object HistoryOwner extends Logging {

  /**
   * Opens the provider at `pathStr`, restoring a previous snapshot when one is readable. Returns
   * None when history is unusable; nothing here may fail an application.
   */
  def open(
      pathStr: String,
      appId: String,
      attemptId: Option[String],
      policy: HistoryPolicy): Option[HistoryOwner] = {
    try {
      val path = Paths.get(pathStr)
      val catalog = HistoryMetricCatalog.production()
      // of() validates against Provenance's byte bounds. Building it here fails at open rather
      // than on the writer thread, where a rejection would be silent.
      val identity = LocalProvenanceIdentity.of(
        if (appId == null || appId.isEmpty) "unknown" else appId,
        attemptId.filter(_.nonEmpty).orNull,
        pluginVersion)
      val provenance = new LocalProvenanceSource {
        override def current(): LocalProvenanceIdentity = identity
      }
      val clock = Clock.systemUTC()

      val provider = restore(path, catalog, clock, provenance, policy).getOrElse {
        LocalHistoryMetricsFactory.open(catalog, clock, provenance, policy.planningMaxAge,
          policy.queue, policy.execution, policy.breaker)
      }
      try {
        Some(new HistoryOwner(provider, MetricStores.install(provider.store()), path, policy))
      } catch {
        case t: Throwable if HistoryMetric.isContained(t) =>
          // Installation validates API compatibility and permits one registration. Leave no
          // provider running if it failed.
          provider.shutdown(policy.shutdownBudget)
          throw t
      }
    } catch {
      case t: Throwable if HistoryMetric.isContained(t) =>
        logWarning(s"History unavailable, callers keep existing behavior: $t")
        None
    }
  }

  private def restore(
      path: Path,
      catalog: HistoryMetricCatalog,
      clock: Clock,
      provenance: LocalProvenanceSource,
      policy: HistoryPolicy): Option[LocalHistoryMetrics] = {
    if (!Files.isRegularFile(path)) {
      None
    } else {
      try {
        val restored = LocalHistoryMetricsFactory.openSnapshot(path, catalog, clock, provenance,
          policy.planningMaxAge, policy.queue, policy.execution, policy.breaker,
          policy.restoreBudget)
        logInfo(s"History restored from $path")
        Some(restored)
      } catch {
        case e: LocalSnapshotException =>
          // An unreadable snapshot is not a reason to fail. Start empty; shutdown republishes.
          logWarning(s"History snapshot at $path unusable (${e.reason}); starting empty")
          None
      }
    }
  }

  /** `getPackage` is null in a shaded jar, and Provenance rejects a null or empty version. */
  private def pluginVersion: String = {
    val v = try {
      Option(getClass.getPackage).map(_.getImplementationVersion).orNull
    } catch {
      case t: Throwable if HistoryMetric.isContained(t) => null
    }
    if (v == null || v.isEmpty) "unknown" else v
  }
}
