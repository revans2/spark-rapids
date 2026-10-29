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
 * Host-only cuDF packed metadata adapter for the isolated int/long/string experiment.
 * Arrays are borrowed for the call; metadata/descriptors are newly owned. No GPU work or
 * row-to-column conversion occurs here. The pinned native build records cuDF dependencies.
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
}
