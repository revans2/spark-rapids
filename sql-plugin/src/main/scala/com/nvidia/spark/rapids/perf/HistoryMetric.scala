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

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.Duration
import java.util.Collections

import com.nvidia.spark.history.{DimensionSpec, DimValue, MetricSchema, MetricStore,
  MetricVersionId, Observation, Retention, SchemaStatus, Status, SummaryRequest}

import org.apache.spark.internal.Logging

/**
 * One metric family over any `MetricStore`: declare it, record into it, read the most recent value.
 *
 * Subclasses supply identity and a key recipe. The protocol is final so a heuristic cannot weaken
 * the abstain rules, and takes the store as a parameter so it needs no provider to run.
 */
abstract class HistoryMetric extends Logging {

  /** Governed id, allocated in `HistoryMetricCatalog`. */
  protected def metricId: Int

  /**
   * The single dimension. `limit(1)` requires every declared dimension to be bound, so a second
   * one would have to be bound on every read, and adding it is a version bump.
   */
  protected def dimension: String

  /** Family-scoped contract version. Bump only on an incompatible change. */
  protected def version: Int = 1

  final lazy val metric: MetricVersionId = new MetricVersionId(metricId, version)

  /**
   * Declares the schema. Recording under a version whose declaration was not accepted is not
   * allowed, so callers must treat `false` as "this family is off for the session".
   */
  final def declare(store: MetricStore, retention: Duration, budget: Duration): Boolean = {
    try {
      val schema = new MetricSchema(
        metric,
        Collections.singletonList(new DimensionSpec(dimension, DimValue.Kind.STRING)),
        new Retention(retention, retention))
      val statuses = store.declare(Collections.singletonList(schema), budget)
      if (statuses == null || statuses.size() != 1) {
        false
      } else {
        val status = statuses.get(0)
        val accepted = status != null && status.code() == SchemaStatus.Code.ACCEPTED
        if (!accepted) {
          logWarning(s"Schema for metric $metricId not accepted: " +
            s"${Option(status).map(_.code()).getOrElse("null")}")
        }
        accepted
      }
    } catch {
      case t: Throwable if HistoryMetric.isContained(t) =>
        logWarning(s"Declare failed for metric $metricId: $t")
        false
    }
  }

  /** Records one value. Non-blocking and unconfirmed: the provider may drop it. */
  final def record(store: MetricStore, label: String, value: Double, atMs: Long): Unit = {
    if (!isUsableLabel(label) || !isUsable(value)) {
      return
    }
    try {
      val dims = Collections.singletonMap(dimension, DimValue.of(label))
      store.record(Collections.singletonList(new Observation(metric, dims, value, atMs)))
    } catch {
      case t: Throwable if HistoryMetric.isContained(t) =>
        logDebug(s"Record skipped for metric $metricId: $t")
    }
  }

  /**
   * Returns the most recently observed value, or None to keep the existing static decision.
   *
   * None covers every outcome including the ordinary ones: key not seen, evidence too old,
   * provider unavailable, deadline exceeded, denied, malformed batch.
   */
  final def latest(
      store: MetricStore,
      label: String,
      nowMs: Long,
      maxAge: Duration,
      timeout: Duration): Option[Double] = {
    if (!isUsableLabel(label)) {
      return None
    }
    try {
      val fromMs = nowMs - maxAge.toMillis
      if (fromMs >= nowMs) {
        return None
      }
      val request = SummaryRequest.builder(metric)
        .bind(dimension, DimValue.of(label))
        .window(fromMs, nowMs + 1)
        .limit(1)
        .build()
      val responses = store.summarize(Collections.singletonList(request), timeout)
      if (responses == null || responses.size() != 1) {
        return None
      }
      val response = responses.get(0)
      if (response == null || response.status() == null ||
          response.status().code() != Status.Code.OK) {
        return None
      }
      val summary = response.summary()
      if (summary == null || summary.count() <= 0) {
        None // successful absence of evidence, not an error
      } else {
        // limit(1) selected one observation, so the mean is that observation.
        val value = summary.mean()
        if (isUsable(value)) Some(value) else None
      }
    } catch {
      case t: Throwable if HistoryMetric.isContained(t) =>
        logDebug(s"Lookup abstained for metric $metricId: $t")
        None
    }
  }

  /**
   * Hashes the parts to 128 bits: always within the dimension size cap, and keeps table paths and
   * other plan text out of the snapshot.
   */
  protected final def hashKey(parts: String*): String = {
    // Fixed separator count.
    val full = parts.mkString("|")
    val digest =
      MessageDigest.getInstance("SHA-256").digest(full.getBytes(StandardCharsets.UTF_8))
    val hex = new StringBuilder(32)
    digest.take(16).foreach(b => hex.append(f"${b & 0xff}%02x"))
    // The stored key is opaque, so record what produced it.
    logDebug(s"history key ${hex.toString} <- $full")
    hex.toString
  }

  /** Which values are worth storing. Override to admit values this one rejects. */
  protected def isUsable(value: Double): Boolean =
    !value.isNaN && !value.isInfinite && value > 0.0d

  private def isUsableLabel(label: String): Boolean =
    label != null && label.nonEmpty &&
      label.getBytes(StandardCharsets.UTF_8).length <= HistoryMetric.MAX_LABEL_BYTES
}

object HistoryMetric {

  /** DimValue's canonical cap less its kind tag and 2-byte length prefix. */
  private val MAX_LABEL_BYTES = DimValue.MAX_CANONICAL_BYTES - 3

  /** Contains RuntimeException and LinkageError; other Errors escape by design. */
  private[perf] def isContained(t: Throwable): Boolean = t match {
    case _: RuntimeException => true
    case _: LinkageError => true
    case _ => false
  }
}
