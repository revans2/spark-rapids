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

package com.nvidia.spark.rapids.iceberg.spark.source;

/**
 * Hand-off of the history-learned split size to {@link RapidsSparkTable}, which lives at the
 * distribution root and cannot reference shim-loaded classes. With no {@link Advisor} installed
 * every scan keeps its configured split.
 */
public final class IcebergSplitAdvisor {

  /** No learned split: the caller must leave the read options untouched. */
  public static final long NO_DECISION = -1L;

  public interface Advisor {
    /**
     * @param table Iceberg's fully qualified table name
     * @param listedBytes whole-table file bytes from the current snapshot, or 0 when unknown
     * @return the split size in bytes, or {@link #NO_DECISION}
     */
    long learnedSplitBytes(String table, long listedBytes);
  }

  private static volatile Advisor advisor;

  private IcebergSplitAdvisor() {
  }

  public static void install(Advisor next) {
    advisor = next;
  }

  public static void uninstall() {
    advisor = null;
  }

  public static boolean isInstalled() {
    return advisor != null;
  }

  /** Never throws. */
  public static long learnedSplitBytes(String table, long listedBytes) {
    Advisor current = advisor;
    if (current == null) {
      return NO_DECISION;
    }
    try {
      return current.learnedSplitBytes(table, listedBytes);
    } catch (Exception | LinkageError e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      return NO_DECISION;
    }
  }
}
