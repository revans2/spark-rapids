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

/**
 * cuDF packed metadata adapter for the isolated int/long/string experiment.
 * Arrays/device addresses are borrowed for the call; metadata/descriptors are newly owned.
 * These methods inspect host metadata without GPU work or row-to-column conversion.
 * The pinned native build records cuDF dependencies.
 */
public final class HostPackedBridge {
  static {
    String library = System.getProperty("cacheexploration.hostPacked.library");
    if (library == null || library.isEmpty()) {
      throw new IllegalStateException("Set cacheexploration.hostPacked.library to the bridge library");
    }
    System.load(library);
  }

  private HostPackedBridge() {}

  /** Type IDs are cuDF INT32=3, INT64=4, STRING=23. Offsets use -1 for absent buffers. */
  public static native byte[] create(int rows, int[] typeIds, int[] nullCounts,
      long[] dataOffsets, long[] validityOffsets, long[] stringOffsets, byte[] packed);

  /**
   * Returns six longs per root column: type ID, rows, null count, data offset,
   * validity offset, string INT32-offsets offset (-1 for fixed-width columns).
   */
  public static native long[] parse(byte[] metadata, byte[] packed);

  /**
   * Scans borrowed host packed values for exact null counts and signed int/long or unsigned UTF8
   * bounds. Bounds exclude nulls; returned string arrays are newly owned and may share equal bounds.
   * Malformed metadata, offsets or declared null counts are rejected. No CUDA context is required.
   */
  public static native PackedColumnStatistics[] collectStatistics(byte[] metadata, byte[] packed);

  /**
   * Returns eight longs per one-column span: type ID, rows, declared null count, data offset,
   * validity offset, string-offsets offset, raw span length, reserved zero. Offsets are relative
   * to that span and use -1 for missing buffers. Only host metadata is read, so both CPU and GPU
   * packed buffer orders are accepted without accessing device data.
   */
  public static native long[] describeColumns(byte[][] columnMetadata, long[] rawLengths);

  /**
   * Returns Object[]{long[] interleavedStartsAndLengths, byte[][] rebasedColumnMetadata}.
   * The address borrows an intact GPU packed allocation; no device bytes are read.
   */
  public static native Object[] indexDevice(byte[] metadata, long address, long length);

  /** Same result as indexDevice, validating CPU-written buffers without column data copies. */
  public static native Object[] indexHost(byte[] metadata, byte[] packed);

  /**
   * Combines one-column metadata for spans concatenated in the requested column order.
   * Each raw length includes that span's padding; no packed data is accessed or copied.
   */
  public static native byte[] project(byte[][] columnMetadata, long[] rawLengths);
}
