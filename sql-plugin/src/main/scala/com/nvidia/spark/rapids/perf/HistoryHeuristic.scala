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

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import com.nvidia.spark.history.{MetricStore, MetricStores}

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd

/**
 * One planning decision informed by history: read the families at planning time, observe what
 * actually happened when the query ends.
 *
 * `Ctx` is whatever the planning site has in hand, so heuristics do not share a widening parameter
 * list. `decide` and `register` are final: the fallback path and the execution-id scoping are not a
 * heuristic's business.
 */
abstract class HistoryHeuristic extends Logging {

  type Ctx
  type Decision

  def name: String

  /** Families this heuristic reads and writes. A formula may take several measured quantities. */
  def metrics: Seq[HistoryMetric]

  /** Set once every family is declared. Until then the static decision is the right answer. */
  @volatile private var active: Option[HistoryPolicy] = None

  final def enable(policy: HistoryPolicy): Unit = active = Some(policy)

  final def disable(): Unit = active = None

  final def isEnabled: Boolean = active.isDefined

  /** The dimension value this context maps to, per family. */
  protected def keyFor(metric: HistoryMetric, ctx: Ctx): String

  /** What planning would have chosen without history. */
  protected def staticDecision(ctx: Ctx): Decision

  /** The formula. Called only when `sufficient` holds. */
  protected def decideFrom(observed: Map[HistoryMetric, Double], ctx: Ctx): Decision

  /** Bounds on the formula's output. */
  protected def constrain(raw: Decision, ctx: Ctx): Decision = raw

  /** What this query actually did, read after it ran. */
  protected def observe(ctx: Ctx): Map[HistoryMetric, Double]

  /** Whether the evidence in hand is enough to decide. Default: every family answered. */
  protected def sufficient(observed: Map[HistoryMetric, Double]): Boolean =
    observed.size == metrics.size

  /** Whether this context is worth remembering for observation. */
  protected def shouldObserve(ctx: Ctx): Boolean = true

  private def store: MetricStore = MetricStores.current()

  /**
   * The planning decision. History or the static answer, never a blend: any family that abstains
   * is simply absent, and `sufficient` decides whether what remains is enough.
   */
  final def decide(ctx: Ctx, nowMs: Long): Decision = active match {
    case None => staticDecision(ctx)
    case Some(policy) =>
      val current = store
      val observed = metrics.flatMap { m =>
        m.latest(current, keyFor(m, ctx), nowMs, policy.planningMaxAge, policy.planningTimeout)
          .map(value => m -> value)
      }.toMap
      if (sufficient(observed)) constrain(decideFrom(observed, ctx), ctx)
      else staticDecision(ctx)
  }

  /**
   * Remembers this context so its query end can observe it. A context planned outside a SQL
   * execution has no id to drain against and is not tracked.
   */
  final def register(executionId: Option[Long], ctx: Ctx): Unit = {
    if (!isEnabled || !shouldObserve(ctx)) {
      return
    }
    executionId match {
      case None => logDebug(s"$name planned outside a SQL execution; not tracked for history")
      case Some(id) => HistoryObservations.register(id, () => recordAll(ctx))
    }
  }

  private def recordAll(ctx: Ctx): Unit = {
    val current = store
    val atMs = System.currentTimeMillis()
    observe(ctx).foreach { case (m, value) => m.record(current, keyFor(m, ctx), value, atMs) }
  }
}

/**
 * Holds planned contexts until their query ends, then lets each record what it observed.
 *
 * Values cannot be read at planning time: they come from accumulators that stay zero until Spark
 * merges task values back. Entries are keyed by SQL execution id so a query only ever reads its
 * own merged accumulators. One registry and one listener serve every heuristic.
 */
object HistoryObservations extends Logging {

  @volatile private var active: Boolean = false

  /**
   * Registered but not yet observed, keyed by SQL execution id. Entries are removed when their
   * execution ends, so this only grows if the listener bus drops an event.
   */
  private val pending = new ConcurrentHashMap[Long, java.util.List[() => Unit]]()

  def start(): Unit = active = true

  def shutdown(): Unit = {
    active = false
    pending.clear()
  }

  /** Registered once on the SparkContext, so every session and micro-batch is covered. */
  def listener: SparkListener = new ObservationListener

  private[perf] def register(executionId: Long, record: () => Unit): Unit = {
    if (!active) {
      return
    }
    val callbacks = pending.computeIfAbsent(executionId,
      new java.util.function.Function[Long, java.util.List[() => Unit]] {
        override def apply(key: Long): java.util.List[() => Unit] =
          java.util.Collections.synchronizedList(new java.util.ArrayList[() => Unit]())
      })
    callbacks.add(record)
  }

  /** Removes one execution's contexts without recording. Used when its query failed. */
  private def discard(executionId: Long): Unit = pending.remove(executionId)

  private def drain(executionId: Long): Unit = {
    val callbacks = pending.remove(executionId)
    if (callbacks == null || !active) {
      return
    }
    callbacks.asScala.foreach { record =>
      try {
        record()
      } catch {
        case t: Throwable if HistoryMetric.isContained(t) =>
          // No dimension values or provider text in diagnostics.
          logDebug(s"Observation skipped for execution $executionId: ${t.getClass.getName}")
      }
    }
  }

  private class ObservationListener extends SparkListener {
    override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
      case e: SparkListenerSQLExecutionEnd =>
        if (e.errorMessage.exists(_.nonEmpty)) {
          discard(e.executionId)
        } else {
          drain(e.executionId)
        }
      case _ => // not ours
    }
  }
}
