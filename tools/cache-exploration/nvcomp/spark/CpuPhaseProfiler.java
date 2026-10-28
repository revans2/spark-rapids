/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.cacheexploration;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import one.profiler.AsyncProfiler;
import one.profiler.AsyncProfilerLoader;

/** Action-scoped use of the same async-profiler loader packaged with the plugin. */
public final class CpuPhaseProfiler {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final String OPTIONS =
      "event=ctimer,interval=1ms,wall=5ms,jfr,threads,cstack=fp";
  private static AsyncProfiler profiler;
  private static Path activeFile;
  private static Map<String, Object> beforeCounters;
  private static long sampleStart;

  private CpuPhaseProfiler() {}

  public static synchronized String start(String filename) throws Exception {
    if (activeFile != null) throw new IllegalStateException("CPU profile already active");
    Path path = Path.of(filename);
    if (!path.isAbsolute() || Files.exists(path)) {
      throw new IllegalArgumentException("CPU profile requires a new absolute output path");
    }
    ThreadMXBean threads = ManagementFactory.getThreadMXBean();
    if (!threads.isThreadCpuTimeSupported()) {
      throw new UnsupportedOperationException("Per-thread CPU counters are unavailable");
    }
    if (!threads.isThreadCpuTimeEnabled()) threads.setThreadCpuTimeEnabled(true);
    if (profiler == null) {
      profiler = AsyncProfilerLoader.load();
      snapshot();  // Warm JMX/Jackson before the first sampled action.
    }
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("version", profiler.execute("version").trim());
    result.put("options", OPTIONS);
    result.put("status", profiler.execute("start," + OPTIONS + ",file=" + path));
    activeFile = path;
    return JSON.writeValueAsString(result);
  }

  public static synchronized String stop() throws Exception {
    if (activeFile == null) throw new IllegalStateException("CPU profile is not active");
    Path path = activeFile;
    try {
      String status = profiler.execute("stop");
      if (!Files.isRegularFile(path) || Files.size(path) == 0) {
        throw new IllegalStateException("CPU profiler produced no recording");
      }
      return status;
    } finally {
      activeFile = null;
      beforeCounters = null;
    }
  }

  public static long epochNanos() {
    Instant now = Instant.now();
    return Math.addExact(Math.multiplyExact(now.getEpochSecond(), 1000000000L), now.getNano());
  }

  public static String snapshot() throws Exception {
    return JSON.writeValueAsString(captureCounters());
  }

  public static synchronized void begin() {
    if (activeFile == null || beforeCounters != null) {
      throw new IllegalStateException("CPU profile action cannot begin");
    }
    beforeCounters = captureCounters();
    sampleStart = epochNanos();
  }

  public static synchronized String finish() throws Exception {
    long sampleEnd = epochNanos();
    if (beforeCounters == null) throw new IllegalStateException("No CPU action is active");
    Map<String, Object> after = captureCounters();
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("before", beforeCounters);
    result.put("after", after);
    result.put("sample_start_epoch_nanos", sampleStart);
    result.put("sample_end_epoch_nanos", sampleEnd);
    beforeCounters = null;
    // Return the large JSON only after both counter windows have closed.
    return JSON.writeValueAsString(result);
  }

  /** Counters are sequential observations; snapshot_duration_nanos exposes their skew. */
  private static Map<String, Object> captureCounters() {
    long started = System.nanoTime();
    Instant now = Instant.now();
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("epoch_nanos", Math.addExact(
        Math.multiplyExact(now.getEpochSecond(), 1000000000L), now.getNano()));
    com.sun.management.OperatingSystemMXBean os =
        (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    result.put("process_cpu_nanos", os.getProcessCpuTime());
    ThreadMXBean threads = ManagementFactory.getThreadMXBean();
    long[] ids = threads.getAllThreadIds();
    ThreadInfo[] infos = threads.getThreadInfo(ids);
    List<Map<String, Object>> values = new ArrayList<>();
    for (int i = 0; i < ids.length; i++) {
      if (infos[i] == null) continue;
      Map<String, Object> thread = new LinkedHashMap<>();
      thread.put("id", ids[i]);
      thread.put("name", infos[i].getThreadName());
      thread.put("cpu_nanos", threads.getThreadCpuTime(ids[i]));
      values.add(thread);
    }
    result.put("threads", values);
    List<Map<String, Object>> collectors = new ArrayList<>();
    for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
      Map<String, Object> value = new LinkedHashMap<>();
      value.put("name", gc.getName());
      value.put("count", gc.getCollectionCount());
      value.put("time_ms", gc.getCollectionTime());
      collectors.add(value);
    }
    result.put("gc", collectors);
    result.put("compilation_time_ms",
        ManagementFactory.getCompilationMXBean().getTotalCompilationTime());
    result.put("loaded_classes", ManagementFactory.getClassLoadingMXBean().getTotalLoadedClassCount());
    result.put("snapshot_duration_nanos", System.nanoTime() - started);
    return result;
  }
}
