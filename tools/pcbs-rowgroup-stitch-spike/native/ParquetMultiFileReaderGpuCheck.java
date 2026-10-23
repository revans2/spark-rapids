/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations.
 */
package ai.rapids.cudf;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

/** Small native-ABI/ownership gate. Deliberately separate from performance measurements. */
public final class ParquetMultiFileReaderGpuCheck {
  private static final long POOL_BYTES = 15L << 30;

  private static void check(boolean value, String message) {
    if (!value) throw new AssertionError(message);
  }

  private static final class EncodedBuffers implements HostBufferConsumer, AutoCloseable {
    private final java.util.List<HostMemoryBuffer> buffers = new java.util.ArrayList<>();
    private final java.util.List<Long> lengths = new java.util.ArrayList<>();

    @Override
    public void handleBuffer(HostMemoryBuffer buffer, long length) {
      // The writer transfers ownership of every callback buffer to the consumer.
      try {
        check(length >= 0 && length <= buffer.getLength(), "writer used length");
        buffers.add(buffer);
        lengths.add(length);
      } catch (Throwable failure) {
        if (!buffers.contains(buffer)) {
          try (HostMemoryBuffer cleanup = buffer) {
            throw failure;
          }
        }
        throw failure;
      }
    }

    HostMemoryBuffer combine() {
      long total = 0;
      for (long length : lengths) total = Math.addExact(total, length);
      check(total > 0, "writer produced a complete file");
      HostMemoryBuffer result = HostMemoryBuffer.allocate(total);
      try {
        long offset = 0;
        for (int index = 0; index < buffers.size(); ++index) {
          long length = lengths.get(index);
          result.copyFromHostBuffer(offset, buffers.get(index), 0, length);
          offset += length;
        }
        return result;
      } catch (Throwable failure) {
        try (HostMemoryBuffer cleanup = result) {
          throw failure;
        }
      }
    }

    @Override
    public void close() {
      Throwable failure = null;
      for (HostMemoryBuffer buffer : buffers) {
        try (HostMemoryBuffer cleanup = buffer) {
          // Callback buffers are no longer needed after the off-heap copy.
        } catch (Throwable next) {
          if (failure == null) failure = next; else failure.addSuppressed(next);
        }
      }
      buffers.clear();
      lengths.clear();
      if (failure instanceof Error) throw (Error) failure;
      if (failure != null) throw new RuntimeException("Encoded buffer cleanup", failure);
    }
  }

  private static HostMemoryBuffer encode(int base) {
    try (EncodedBuffers output = new EncodedBuffers()) {
      try (ColumnVector id = ColumnVector.fromLongs(base, base + 1, base + 2);
           ColumnVector text = ColumnVector.fromStrings("file-" + base, null, "last-" + base);
           ColumnVector time = ColumnVector.timestampMicroSecondsFromBoxedLongs(
               1000000L + base * 1000L, null, 3000000L + base * 1000L);
           Table table = new Table(id, text, time);
           ParquetTableWriter writer = Table.writeParquetChunked(
               ParquetWriterOptions.builder().withNonNullableColumns("id")
                   .withNullableColumns("text")
                   .withNullableTimestampColumn("ts", false)
                   .withCompressionType(CompressionType.NONE).build(), output)) {
        writer.write(table);
      }
      // Closing the writer completes its footer before buffers are combined.
      return output.combine();
    }
  }

  private static void verify(Table table, int[] bases, boolean projected) {
    check(table.getRowCount() == bases.length * 3L, "row count");
    check(table.getNumberOfColumns() == (projected ? 2 : 3), "column count");
    try (HostColumnVector ids = table.getColumn(0).copyToHost();
         HostColumnVector times = table.getColumn(projected ? 1 : 2).copyToHost()) {
      check(times.getType().equals(projected ?
          DType.TIMESTAMP_MILLISECONDS : DType.TIMESTAMP_MICROSECONDS), "timestamp type");
      for (int part = 0; part < bases.length; ++part) {
        int base = bases[part];
        for (int row = 0; row < 3; ++row) {
          int index = part * 3 + row;
          check(ids.getLong(index) == base + row, "independent file order/id");
          check(times.isNull(index) == (row == 1), "timestamp null");
          if (row != 1) {
            long expected = (row == 0 ? 1000000L : 3000000L) + base * 1000L;
            check(times.getLong(index) == (projected ? expected / 1000 : expected),
                "timestamp value");
          }
        }
      }
    }
    if (!projected) {
      try (HostColumnVector text = table.getColumn(1).copyToHost()) {
        for (int part = 0; part < bases.length; ++part) {
          int index = part * 3;
          check(text.getJavaString(index).equals("file-" + bases[part]), "string first");
          check(text.isNull(index + 1), "string null");
          check(text.getJavaString(index + 2).equals("last-" + bases[part]), "string last");
        }
      }
    }
  }

  private static void baseline(long expected, String phase) {
    check(Rmm.getTotalBytesAllocated() == expected,
        phase + " outstanding bytes: " + Rmm.getTotalBytesAllocated() + " vs " + expected);
  }

  private static String fileSha(Path file) throws Exception {
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    try (InputStream input = Files.newInputStream(file)) {
      byte[] buffer = new byte[2 * 1024 * 1024];
      int count;
      while ((count = input.read(buffer)) >= 0) digest.update(buffer, 0, count);
    }
    StringBuilder result = new StringBuilder();
    for (byte value : digest.digest()) result.append(String.format("%02x", value & 0xff));
    return result.toString();
  }

  private static String mappedCudf(Path directory, String expectedSha) throws Exception {
    java.util.List<String> mappings = Files.readAllLines(Paths.get("/proc/self/maps"));
    Files.write(directory.resolve("proc-self-maps.txt"), mappings);
    Set<String> paths = new HashSet<>();
    Set<String> identities = new HashSet<>();
    java.util.List<String> inventory = new java.util.ArrayList<>();
    String result = null;
    for (String line : mappings) {
      String[] fields = line.trim().split("\\s+", 6);
      if (fields.length != 6 || !fields[5].startsWith("/")) continue;
      Path file = Paths.get(fields[5]);
      if (!paths.add(fields[5]) || !Files.isRegularFile(file)) continue;
      // Classify actual ELF SONAMEs, not loader-generated temporary filenames.
      Process inspection = new ProcessBuilder("readelf", "-d", file.toString())
          .redirectErrorStream(true).start();
      String dynamic = new String(inspection.getInputStream().readAllBytes(),
          StandardCharsets.UTF_8);
      int exit = inspection.waitFor();
      inventory.add(fields[3] + ":" + fields[4] + ":" + fields[5] +
          " readelf_exit=" + exit + "\n" + dynamic);
      if (exit == 0 && dynamic.lines().anyMatch(
          entry -> entry.contains("(SONAME)") && entry.contains("[libcudf.so]"))) {
        identities.add(fields[3] + ":" + fields[4]);
        check(fileSha(file).equals(expectedSha), "unexpected mapped libcudf binary: " + file);
        result = fields[5];
      }
    }
    Files.write(directory.resolve("mapped-library-inventory.txt"), inventory);
    check(identities.size() == 1, "expected one mapped libcudf identity: " + identities);
    return result;
  }

  public static void main(String[] arguments) throws Exception {
    check(arguments.length == 2, "expected fresh data directory and runtime SHA");
    Path directory = Files.createDirectory(Paths.get(arguments[0]));
    Rmm.initialize(RmmAllocationMode.POOL, null, POOL_BYTES);
    try {
      check(Rmm.getPoolSize() == POOL_BYTES, "15 GiB shared pool");
      long baseline = Rmm.getTotalBytesAllocated();
      long live;
      try (HostMemoryBuffer one = encode(10);
           HostMemoryBuffer two = encode(100);
           HostMemoryBuffer bad = HostMemoryBuffer.allocate(16)) {
        bad.setBytes(0, new byte[16], 0, 16);
        try (Table single = ParquetMultiFileReader.read(ParquetOptions.DEFAULT,
            new HostMemoryBuffer[]{one})) {
          verify(single, new int[]{10}, false);
        }
        baseline(baseline, "single close");
        try (Table combined = ParquetMultiFileReader.read(ParquetOptions.DEFAULT,
            new HostMemoryBuffer[]{one, two})) {
          live = Rmm.getTotalBytesAllocated();
          check(live > baseline, "native result must charge the existing Java RMM tracker");
          verify(combined, new int[]{10, 100}, false);
        }
        baseline(baseline, "combined close");
        ParquetOptions projection = ParquetOptions.builder().includeColumn("id", "ts")
            .withTimeUnit(DType.TIMESTAMP_MILLISECONDS).build();
        try (Table projected = ParquetMultiFileReader.read(projection,
            new HostMemoryBuffer[]{two, one})) {
          verify(projected, new int[]{100, 10}, true);
        }
        baseline(baseline, "projection close");
        boolean rejected = false;
        try (Table unexpected = ParquetMultiFileReader.read(ParquetOptions.DEFAULT,
            new HostMemoryBuffer[]{one, bad})) {
          throw new AssertionError("malformed independent file accepted");
        } catch (CudfException expected) {
          rejected = true;
        }
        check(rejected, "malformed independent file rejection");
        baseline(baseline, "malformed input cleanup");
        // Max equals initial size in Rmm.initialize(POOL); over-limit admission must fail.
        boolean limitRejected = false;
        try (DeviceMemoryBuffer unexpected = Rmm.alloc(POOL_BYTES + 256)) {
          throw new AssertionError("over-limit pool allocation accepted");
        } catch (OutOfMemoryError expected) {
          check(expected.getMessage().contains("Could not allocate native memory"),
              "must reject at the native pool, not Java heap allocation");
          limitRejected = true;
        }
        check(limitRejected, "pool limit rejection");
        baseline(baseline, "pool rejection cleanup");
      }
      baseline(baseline, "all inputs closed");
      String mapped = mappedCudf(directory, arguments[1]).replace("\\", "\\\\").replace("\"", "\\\"");
      System.out.println("NATIVE_GPU_CHECK {\"success\":true,\"independent_files\":2," +
          "\"rows\":6,\"encoded_in_memory\":true,\"projection\":true,\"nulls\":true,\"timestamp\":true," +
          "\"malformed_cleanup\":true,\"pool_limit_rejected\":true," +
          "\"pool_bytes\":" + POOL_BYTES + ",\"baseline_bytes\":" + baseline +
          ",\"live_result_bytes\":" + live + ",\"mapped_cudf_sha256\":\"" + arguments[1] +
          "\",\"mapped_cudf\":\"" + mapped + "\"}");
    } finally {
      Rmm.shutdown();
    }
  }
}
