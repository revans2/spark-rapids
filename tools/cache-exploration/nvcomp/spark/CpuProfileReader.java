/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.spark.sql.cacheexploration;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordedFrame;
import jdk.jfr.consumer.RecordedMethod;
import jdk.jfr.consumer.RecordedThread;
import jdk.jfr.consumer.RecordingFile;

/** Streams phase-filtered async-profiler 3 CPU/wall stacks without expanding JFR to JSON. */
public final class CpuProfileReader {
  private CpuProfileReader() {}

  private static String quote(String value) {
    if (value == null) return "null";
    StringBuilder result = new StringBuilder("\"");
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      switch (c) {
        case '"': result.append("\\\""); break;
        case '\\': result.append("\\\\"); break;
        case '\n': result.append("\\n"); break;
        case '\r': result.append("\\r"); break;
        case '\t': result.append("\\t"); break;
        default:
          if (c < 32) result.append(String.format("\\u%04x", (int) c));
          else result.append(c);
      }
    }
    return result.append('"').toString();
  }

  private static long epochNanos(Instant time) {
    return Math.addExact(Math.multiplyExact(time.getEpochSecond(), 1_000_000_000L), time.getNano());
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 3) throw new IllegalArgumentException("FILE START_EPOCH_NS END_EPOCH_NS");
    long start = Long.parseLong(args[1]);
    long end = Long.parseLong(args[2]);
    if (end <= start) throw new IllegalArgumentException("Empty/reversed phase window");
    Map<String, Long> stacks = new LinkedHashMap<>();
    long seen = 0;
    long included = 0;
    long before = 0;
    long after = 0;
    Map<String, Long> states = new LinkedHashMap<>();
    try (RecordingFile recording = new RecordingFile(Path.of(args[0]))) {
      while (recording.hasMoreEvents()) {
        RecordedEvent event = recording.readEvent();
        if (!event.getEventType().getName().equals("jdk.ExecutionSample")) continue;
        seen++;
        long timestamp = epochNanos(event.getStartTime());
        if (timestamp < start) { before++; continue; }
        if (timestamp >= end) { after++; continue; }
        String state = event.getString("state");
        String sampler;
        // AP3 CpuEngine uses THREAD_UNKNOWN; WallClock assigns RUNNING/SLEEPING.
        // This mapping requires the requested combined CPU + idle-thread wall sampling.
        switch (state) {
          case "STATE_DEFAULT": sampler = "cpu"; break;
          case "STATE_RUNNABLE":
          case "STATE_SLEEPING": sampler = "wall"; break;
          default: throw new IllegalArgumentException("Unknown async-profiler 3 state " + state);
        }
        included++;
        states.merge(state, 1L, Long::sum);
        RecordedThread thread = event.getThread("sampledThread");
        List<String> frames = new ArrayList<>();
        if (event.getStackTrace() != null) {
          for (RecordedFrame frame : event.getStackTrace().getFrames()) {
            RecordedMethod method = frame.getMethod();
            String type = method.getType().getName().replace('/', '.');
            frames.add(type + "." + method.getName());
          }
        }
        StringBuilder row = new StringBuilder("{\"type\":\"stack\",\"sampler\":")
            .append(quote(sampler)).append(",\"state\":").append(quote(state))
            .append(",\"java_id\":").append(thread == null ? 0 : thread.getJavaThreadId())
            .append(",\"os_id\":").append(thread == null ? 0 : thread.getOSThreadId())
            .append(",\"java_name\":").append(quote(thread == null ? null : thread.getJavaName()))
            .append(",\"os_name\":").append(quote(thread == null ? null : thread.getOSName()))
            .append(",\"frames\":[");
        for (int i = 0; i < frames.size(); i++) {
          if (i > 0) row.append(',');
          row.append(quote(frames.get(i)));
        }
        row.append("],\"truncated\":")
            .append(event.getStackTrace() != null && event.getStackTrace().isTruncated());
        stacks.merge(row.toString(), 1L, Long::sum);
      }
    }
    System.out.println("{\"type\":\"metadata\",\"execution_events_seen\":" + seen
        + ",\"included\":" + included + ",\"before_window\":" + before
        + ",\"after_window\":" + after + ",\"unique_stacks\":" + stacks.size() + "}");
    for (Map.Entry<String, Long> entry : stacks.entrySet()) {
      System.out.println(entry.getKey() + ",\"count\":" + entry.getValue() + "}");
    }
  }
}
