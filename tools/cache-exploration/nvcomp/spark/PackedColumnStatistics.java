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

/** Exact bounds for one packed int/long/string column; loading this DTO performs no GPU work. */
public final class PackedColumnStatistics {
  public final int typeId;
  public final int rowCount;
  public final int nullCount;
  /** Signed numeric bounds, meaningful for non-string columns having at least one valid row. */
  public final long minimum;
  public final long maximum;
  /** Owned UTF8 bytes, or null for an all-null/non-string column. Empty bounds are valid. */
  public final byte[] minimumUtf8;
  public final byte[] maximumUtf8;

  /** Arrays are transferred by the producer; equal string bounds may share the same array. */
  public PackedColumnStatistics(int typeId, int rowCount, int nullCount,
      long minimum, long maximum, byte[] minimumUtf8, byte[] maximumUtf8) {
    this.typeId = typeId;
    this.rowCount = rowCount;
    this.nullCount = nullCount;
    this.minimum = minimum;
    this.maximum = maximum;
    this.minimumUtf8 = minimumUtf8;
    this.maximumUtf8 = maximumUtf8;
  }
}
