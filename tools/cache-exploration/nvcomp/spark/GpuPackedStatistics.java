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
package org.apache.spark.sql.cacheexploration;

import java.io.File;
import java.util.ArrayList;

import ai.rapids.cudf.Cuda;
import ai.rapids.cudf.DefaultHostMemoryAllocator;
import ai.rapids.cudf.DeviceMemoryBuffer;
import ai.rapids.cudf.HostMemoryBuffer;

/**
 * Synchronous, batched statistics for borrowed packed INT32/INT64/STRING columns.
 * All device scratch uses RMM and host staging uses the managed host allocator.
 * The caller keeps column owners alive, acquires the GPU semaphore and supplies retry scope.
 * Calls stay on the calling thread, including every operation on a PTDS stream.
 */
public final class GpuPackedStatistics {
  private static final int TILE_ROWS = 4096;
  private static final int INPUT_WORDS = 8;
  private static final int DEVICE_WORDS = 10;
  private static final int RESULT_WORDS = 8;
  private static final int RESULT_BYTES = RESULT_WORDS * Long.BYTES;

  private GpuPackedStatistics() {}

  private static final class Library {
    static {
      String path = System.getProperty("cacheexploration.statistics.library");
      if (path == null || !new File(path).isAbsolute()) {
        throw new IllegalArgumentException("Set cacheexploration.statistics.library " +
            "to the absolute packed statistics library path");
      }
      System.load(path);
    }
    static void load() {}
  }

  private static final class Drain implements AutoCloseable {
    private final Cuda.Stream stream;
    Drain(Cuda.Stream stream) { this.stream = stream; }
    @Override
    public void close() { stream.sync(); }
  }

  private static void require(boolean condition, String message) {
    if (!condition) throw new IllegalArgumentException(message);
  }

  private static void span(long offset, long size, long length, int alignment) {
    require(offset >= 0 && size >= 0 && offset <= length && size <= length - offset &&
        offset % alignment == 0, "Invalid packed statistics buffer span");
  }

  /**
   * Eight descriptor longs per column: type, rows, declared null count, data offset,
   * mask offset, string INT32-offsets offset, raw span length, reserved.
   * Offsets are relative to the corresponding column buffer; -1 means absent.
   * The reserved word is ignored and replaced with the current borrowed device address.
   * No input references are consumed. All returned string arrays own their bytes.
   */
  public static PackedColumnStatistics[] compute(
      DeviceMemoryBuffer[] columns, long[] descriptors, Cuda.Stream stream) {
    require(columns != null && descriptors != null && stream != null,
        "Missing packed statistics inputs");
    require(descriptors.length == Math.multiplyExact(columns.length, INPUT_WORDS),
        "Packed statistics descriptor count");
    if (columns.length == 0) return new PackedColumnStatistics[0];
    long[] deviceDescriptors = new long[Math.multiplyExact(columns.length, DEVICE_WORDS)];
    long tileCount = 0;
    for (int i = 0; i < columns.length; i++) {
      require(columns[i] != null, "Null packed column buffer");
      int source = i * INPUT_WORDS;
      int dest = i * DEVICE_WORDS;
      System.arraycopy(descriptors, source, deviceDescriptors, dest, INPUT_WORDS);
      long type = descriptors[source];
      long rows = descriptors[source + 1];
      long nulls = descriptors[source + 2];
      long data = descriptors[source + 3];
      long mask = descriptors[source + 4];
      long offsets = descriptors[source + 5];
      long length = descriptors[source + 6];
      require((type == 3 || type == 4 || type == 23) && rows > 0 &&
          rows <= Integer.MAX_VALUE && nulls >= 0 && nulls <= rows &&
          length == columns[i].getLength(), "Invalid packed statistics descriptor");
      if (mask == -1) {
        require(nulls == 0, "Nullable packed column lacks validity mask");
      } else {
        span(mask, Math.multiplyExact((rows + 31) / 32, 4), length, 4);
      }
      if (type == 23) {
        span(offsets, Math.multiplyExact(rows + 1, 4), length, 4);
        require(data == -1 || (data >= 0 && data <= length), "Invalid string data offset");
      } else {
        require(offsets == -1, "Unexpected numeric offsets");
        span(data, Math.multiplyExact(rows, type == 3 ? 4 : 8), length, type == 3 ? 4 : 8);
      }
      require(columns[i].getAddress() != 0 &&
          columns[i].getAddress() % 8 == 0, "Unaligned packed statistics allocation");
      deviceDescriptors[dest + 7] = columns[i].getAddress();
      deviceDescriptors[dest + 8] = tileCount;
      long tiles = (rows + TILE_ROWS - 1) / TILE_ROWS;
      deviceDescriptors[dest + 9] = tiles;
      tileCount = Math.addExact(tileCount, tiles);
    }
    int tiles = Math.toIntExact(tileCount);
    long descriptorBytes = Math.multiplyExact((long) deviceDescriptors.length, Long.BYTES);
    long resultBytes = Math.multiplyExact((long) columns.length, RESULT_BYTES);
    long scratchBytes = Math.multiplyExact(tileCount, RESULT_BYTES);
    Library.load();
    int device = Cuda.getDevice();
    long[] results = new long[Math.multiplyExact(columns.length, RESULT_WORDS)];
    // Drain closes first on success/failure, while every upload/download and scratch owner lives.
    try (DeviceMemoryBuffer desc = DeviceMemoryBuffer.allocate(descriptorBytes, stream);
         DeviceMemoryBuffer scratch = DeviceMemoryBuffer.allocate(scratchBytes, stream);
         DeviceMemoryBuffer output = DeviceMemoryBuffer.allocate(resultBytes, stream);
         HostMemoryBuffer upload = DefaultHostMemoryAllocator.get().allocate(descriptorBytes, true);
         HostMemoryBuffer download = DefaultHostMemoryAllocator.get().allocate(resultBytes, true)) {
      try (Drain drain = new Drain(stream)) {
        upload.setLongs(0, deviceDescriptors, 0, deviceDescriptors.length);
        desc.copyFromHostBufferAsync(upload, stream);
        reduce(desc.getAddress(), columns.length, tiles, scratch.getAddress(),
            output.getAddress(), stream.getStream(), device);
        download.copyFromDeviceBufferAsync(output, stream);
      }
      download.getLongs(results, 0, 0, results.length);
    }
    long[] minStarts = new long[columns.length];
    long[] maxStarts = new long[columns.length];
    ArrayList<Long> copies = new ArrayList<>();
    long totalBytes = 0;
    for (int i = 0; i < columns.length; i++) {
      int r = i * RESULT_WORDS;
      int d = i * INPUT_WORDS;
      require(results[r + 7] == 0, "Malformed packed column encountered on GPU");
      require(results[r] == descriptors[d + 2], "GPU null count differs from packed metadata");
      if (descriptors[d] == 23 && results[r + 4] >= 0) {
        for (int bound = 0; bound < 2; bound++) {
          long offset = results[r + 3 + bound * 2];
          long length = results[r + 4 + bound * 2];
          span(offset, length, columns[i].getLength(), 1);
          Math.toIntExact(length);
          if (bound == 1 && offset == results[r + 3] && length == results[r + 4]) {
            maxStarts[i] = minStarts[i];
          } else {
            if (bound == 0) minStarts[i] = totalBytes;
            else maxStarts[i] = totalBytes;
            if (length != 0) {
              copies.add(columns[i].getAddress());
              copies.add(offset);
              copies.add(length);
              copies.add(totalBytes);
            }
            totalBytes = Math.addExact(totalBytes, length);
          }
        }
      }
    }
    // CUDA grid.x has a signed INT32 block-count limit on the supported target.
    require(totalBytes <= (long) Integer.MAX_VALUE * 256,
        "Packed string gather exceeds CUDA grid capacity");
    PackedColumnStatistics[] answer = new PackedColumnStatistics[columns.length];
    if (totalBytes == 0) {
      finish(answer, descriptors, results, minStarts, maxStarts, null);
    } else {
      long[] gatherDescriptors = new long[copies.size()];
      for (int i = 0; i < copies.size(); i++) gatherDescriptors[i] = copies.get(i);
      long bytes = Math.multiplyExact((long) gatherDescriptors.length, Long.BYTES);
      // Do not allocate per-bound GPU buffers or preserve a second aggregate heap byte array.
      try (DeviceMemoryBuffer desc = DeviceMemoryBuffer.allocate(bytes, stream);
           DeviceMemoryBuffer gathered = DeviceMemoryBuffer.allocate(totalBytes, stream);
           HostMemoryBuffer upload = DefaultHostMemoryAllocator.get().allocate(bytes, true);
           HostMemoryBuffer download = DefaultHostMemoryAllocator.get().allocate(totalBytes, true)) {
        try (Drain drain = new Drain(stream)) {
          upload.setLongs(0, gatherDescriptors, 0, gatherDescriptors.length);
          desc.copyFromHostBufferAsync(upload, stream);
          gather(desc.getAddress(), gatherDescriptors.length / 4, totalBytes,
              gathered.getAddress(), stream.getStream(), device);
          download.copyFromDeviceBufferAsync(gathered, stream);
        }
        finish(answer, descriptors, results, minStarts, maxStarts, download);
      }
    }
    return answer;
  }

  private static void finish(PackedColumnStatistics[] answer, long[] descriptors,
      long[] results, long[] minStarts, long[] maxStarts, HostMemoryBuffer strings) {
    for (int i = 0; i < answer.length; i++) {
      int d = i * INPUT_WORDS;
      int r = i * RESULT_WORDS;
      byte[] minimum = null;
      byte[] maximum = null;
      if (descriptors[d] == 23 && results[r + 4] >= 0) {
        minimum = new byte[Math.toIntExact(results[r + 4])];
        if (minimum.length > 0) strings.getBytes(minimum, 0, minStarts[i], minimum.length);
        if (results[r + 3] == results[r + 5] && results[r + 4] == results[r + 6]) {
          maximum = minimum;
        } else {
          maximum = new byte[Math.toIntExact(results[r + 6])];
          if (maximum.length > 0) strings.getBytes(maximum, 0, maxStarts[i], maximum.length);
        }
      }
      answer[i] = new PackedColumnStatistics(Math.toIntExact(descriptors[d]),
          Math.toIntExact(descriptors[d + 1]), Math.toIntExact(results[r]),
          results[r + 4] < 0 ? 0 : results[r + 1],
          results[r + 4] < 0 ? 0 : results[r + 2], minimum, maximum);
    }
  }

  private static native void reduce(long descriptors, int columns, int tiles, long scratch,
      long output, long stream, int device);
  private static native void gather(long descriptors, int copies, long bytes, long output,
      long stream, int device);
}
