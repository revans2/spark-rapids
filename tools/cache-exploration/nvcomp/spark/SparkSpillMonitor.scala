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

import scala.collection.mutable
import scala.util.control.NonFatal

import com.nvidia.spark.rapids.RapidsConf
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart, SparkListenerTaskEnd}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.rapids.{NanoTime, SizeInBytes}

/** Local, action-scoped task observations; no allocation callbacks or forced spilling. */
object SparkSpillMonitor {
  private val SumMetrics = Seq(
    "gpuSpillToHostBytes", "gpuSpillToDiskBytes",
    "gpuSpillToHostTime", "gpuSpillToDiskTime",
    "gpuReadSpillFromHostTime", "gpuReadSpillFromDiskTime",
    "gpuRetryCount", "gpuSplitAndRetryCount", "gpuRetryBlockTime", "gpuRetryComputationTime",
    "gpuTime", "gpuSemaphoreWait")
  private val MaxMetrics = Seq(
    "gpuMaxConcurrentGpuTasks", "gpuMaxDeviceMemoryBytes", "gpuMaxHostMemoryBytes",
    "gpuMaxDiskMemoryBytes", "gpuMaxPageableMemoryBytes", "gpuMaxPinnedMemoryBytes")
  private val MetricNames = (SumMetrics ++ MaxMetrics).toSet

  def create(spark: SparkSession): SparkSpillMonitor = {
    val targetBytes = new RapidsConf(spark.sessionState.conf).gpuTargetBatchSizeBytes
    val monitor = new SparkSpillMonitor(spark.sparkContext, targetBytes)
    spark.sparkContext.addSparkListener(monitor)
    monitor
  }

  private def numeric(value: Any): Long = value match {
    case bytes: SizeInBytes => bytes.value.longValue()
    case nanos: NanoTime => nanos.value.longValue()
    case number: java.lang.Number => number.longValue()
    case other => throw new IllegalArgumentException(
      "Unexpected RAPIDS metric value type: " + other.getClass.getName)
  }
}

final class SparkSpillMonitor private (sc: SparkContext, val targetBatchBytes: Long)
    extends SparkListener {
  import SparkSpillMonitor._
  private implicit val formats: DefaultFormats.type = DefaultFormats

  private case class TaskObservation(
      stage_id: Int,
      stage_attempt: Int,
      task_id: Long,
      task_attempt: Int,
      successful: Boolean,
      task_metrics_available: Boolean,
      spark_memory_bytes_spilled: Option[Long],
      spark_disk_bytes_spilled: Option[Long],
      rapids: Map[String, Long])

  private val observations = mutable.Map.empty[String, mutable.ArrayBuffer[TaskObservation]]
  private val errors = mutable.ArrayBuffer.empty[String]
  private var active: Option[String] = None
  private var jobEvents = 0L
  private var taskEvents = 0L
  private var eventsBefore = (0L, 0L)

  override def onJobStart(event: SparkListenerJobStart): Unit = synchronized {
    jobEvents += 1
  }

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = synchronized {
    taskEvents += 1
    active.foreach { phase =>
      try {
        val metrics = Option(event.taskMetrics)
        // TaskInfo.accumulables discards zero updates. TaskMetrics retains the external
        // accumulator updates, including zero, so absent metrics remain distinguishable.
        val rapids = metrics.toSeq.flatMap(_.accumulators())
          .filter(acc => acc.metadata != null && acc.name.exists(MetricNames.contains))
          .groupBy(_.id).values.map(_.last)
          .map(acc => acc.name.get -> numeric(acc.value)).toSeq
          .groupBy(_._1).map { case (name, values) =>
            name -> (if (MaxMetrics.contains(name)) values.map(_._2).max
              else values.map(_._2).sum)
          }
        observations.getOrElseUpdate(phase, mutable.ArrayBuffer.empty) += TaskObservation(
          event.stageId, event.stageAttemptId, event.taskInfo.taskId,
          event.taskInfo.attemptNumber, event.taskInfo.successful, metrics.isDefined,
          metrics.map(_.memoryBytesSpilled), metrics.map(_.diskBytesSpilled), rapids)
      } catch {
        case NonFatal(error) => errors += error.toString
      }
    }
  }

  /** Call on the query-submitting thread before collect, outside its timer. */
  def begin(phase: String): Unit = {
    // This local benchmark submits one action at a time. Drain earlier jobs before attribution.
    sc.listenerBus.waitUntilEmpty(10000L)
    synchronized {
      require(active.isEmpty && !observations.contains(phase), "Duplicate/nested spill phase")
      active = Some(phase)
      eventsBefore = (jobEvents, taskEvents)
      observations(phase) = mutable.ArrayBuffer.empty
    }
  }

  /** Drain asynchronous TaskEnd delivery outside the collect/profile timers. */
  def end(phase: String): String = {
    synchronized {
      require(active.contains(phase), "Spill phase mismatch")
    }
    sc.listenerBus.waitUntilEmpty(10000L)
    synchronized {
      val tasks = observations.remove(phase).getOrElse(mutable.ArrayBuffer.empty).toSeq
      val summaries = (SumMetrics ++ MaxMetrics).map { name =>
        val values = tasks.flatMap(_.rapids.get(name))
        val value = if (values.isEmpty) None else Some(
          if (MaxMetrics.contains(name)) values.max else values.sum)
        name -> Map[String, Any](
          "value" -> value,
          "aggregation" -> (if (MaxMetrics.contains(name)) "max_task_value" else "sum_task_values"),
          "observed_task_attempts" -> values.size,
          "task_attempts_with_positive_value" -> values.count(_ > 0))
      }.toMap
      val response = Map[String, Any](
        "available" -> errors.isEmpty,
        "phase" -> phase,
        "job_events_in_window" -> (jobEvents - eventsBefore._1),
        "task_events_in_window" -> (taskEvents - eventsBefore._2),
        "task_attempts" -> tasks.size,
        "rapids_metrics_available" -> tasks.exists(_.rapids.nonEmpty),
        "task_attempts_with_rapids_metrics" -> tasks.count(_.rapids.nonEmpty),
        "successful_task_attempts" -> tasks.count(_.successful),
        "failed_task_attempts" -> tasks.count(!_.successful),
        "spark_metrics_observed_task_attempts" -> tasks.count(_.task_metrics_available),
        "spark_memory_bytes_spilled" -> (if (tasks.exists(_.task_metrics_available))
          Some(tasks.flatMap(_.spark_memory_bytes_spilled).sum) else None),
        "spark_disk_bytes_spilled" -> (if (tasks.exists(_.task_metrics_available))
          Some(tasks.flatMap(_.spark_disk_bytes_spilled).sum) else None),
        "rapids_metrics" -> summaries,
        "tasks" -> tasks,
        "errors" -> errors.toVector,
        "scope" -> ("TaskEnd updates for jobs observed in this serial local action window, " +
          "drained before begin and after collect, including failed attempts. " +
          "RAPIDS spill bytes are actual framework counters; Spark spill counters also include " +
          "CPU spills and may overlap RAPIDS counters. Do not add them. Missing RAPIDS values " +
          "are unavailable, not zero. A positive-task count is not a spill-event count. " +
          "Times are summed task nanoseconds, not elapsed time. Memory maxima are reported " +
          "task metric maxima, not isolated process peaks."))
      active = None
      errors.clear()
      write(response)
    }
  }

  def close(): Unit = sc.removeSparkListener(this)
}
