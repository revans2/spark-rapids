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

import java.time.Duration

import com.nvidia.spark.history.local.{LocalCircuitBreakerPolicy, LocalExecutionPolicy,
  LocalQueuePolicy}

/**
 * Every value the history provider requires, resolved in one place.
 *
 * The provider validates shape only - positive counts, rates in (0, 1] - and supplies no defaults,
 * so a consumer must choose all of them. Only the two an operator has a reason to turn are
 * configurable; the rest are placeholders picked inside those bounds, not fitted values.
 */
case class HistoryPolicy(
    planningMaxAge: Duration,
    planningTimeout: Duration,
    declareBudget: Duration,
    queue: LocalQueuePolicy,
    execution: LocalExecutionPolicy,
    breaker: LocalCircuitBreakerPolicy,
    drainBudget: Duration,
    saveBudget: Duration,
    restoreBudget: Duration,
    shutdownBudget: Duration)

object HistoryPolicy {

  private val QUEUE_CAPACITY_OBSERVATIONS = 4096
  private val QUEUE_MAX_BACKEND_BATCH = 64
  private val PLANNING_THREADS = 2
  private val PLANNING_QUEUE_CAPACITY = 64
  private val BREAKER_WINDOW = 32
  private val BREAKER_MIN_SAMPLES = 8
  private val BREAKER_FAILURE_RATE = 0.5d
  private val BREAKER_SLOW_RATE = 0.5d
  private val BREAKER_OPEN = Duration.ofSeconds(30)

  private val DECLARE_BUDGET = Duration.ofSeconds(5)
  private val DRAIN_BUDGET = Duration.ofSeconds(10)
  private val SAVE_BUDGET = Duration.ofSeconds(30)
  private val RESTORE_BUDGET = Duration.ofSeconds(30)
  private val SHUTDOWN_BUDGET = Duration.ofSeconds(15)

  val DEFAULT_MAX_AGE_DAYS: Int = 7
  val DEFAULT_PLANNING_TIMEOUT_MS: Int = 100

  /**
   * @param planningMaxAge how old an observation may be and still reach planning
   * @param planningTimeout budget for one read, and the breaker's slow-call threshold: the same
   *                        quantity, so declaring it once keeps the two from drifting
   */
  def of(planningMaxAge: Duration, planningTimeout: Duration): HistoryPolicy =
    HistoryPolicy(
      planningMaxAge = planningMaxAge,
      planningTimeout = planningTimeout,
      declareBudget = DECLARE_BUDGET,
      queue = LocalQueuePolicy.of(QUEUE_CAPACITY_OBSERVATIONS, QUEUE_MAX_BACKEND_BATCH),
      execution = LocalExecutionPolicy.of(PLANNING_THREADS, PLANNING_QUEUE_CAPACITY),
      breaker = LocalCircuitBreakerPolicy.of(BREAKER_WINDOW, BREAKER_MIN_SAMPLES,
        BREAKER_FAILURE_RATE, planningTimeout, BREAKER_SLOW_RATE, BREAKER_OPEN),
      drainBudget = DRAIN_BUDGET,
      saveBudget = SAVE_BUDGET,
      restoreBudget = RESTORE_BUDGET,
      shutdownBudget = SHUTDOWN_BUDGET)
}
