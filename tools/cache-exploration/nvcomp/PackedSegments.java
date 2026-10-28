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
package cacheexploration.nvcomp;

import ai.rapids.cudf.BaseDeviceMemoryBuffer;
import ai.rapids.cudf.ColumnView;
import ai.rapids.cudf.ContiguousTable;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.Table;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Experimental description of the entire opaque packed payload. Integer data and string offsets
 * retain their widths for Cascaded; masks, chars and padding use bytes. No production format.
 */
public final class PackedSegments {
  /** Serialized descriptor: offset64, length64, type32, reserved32. */
  public static final int DESCRIPTOR_BYTES = 24;
  /** Maximum uncompressed bytes per segment and per codec submission, not a row-batch size. */
  public static final long MAX_SUBMISSION_BYTES = 256L * 1024 * 1024;

  public static final class Segment {
    public final long offset;
    public final long length;
    public final int typeBits;
    Segment(long offset, long length, int typeBits) {
      this.offset = offset;
      this.length = length;
      this.typeBits = typeBits;
    }
  }

  private PackedSegments() {}

  private static void addTyped(List<Segment> typed, BaseDeviceMemoryBuffer buffer,
      long base, long packedLength, int bits) {
    // getData/getOffsets return non-owning views; their parent table owns the allocation.
    if (buffer == null || buffer.getLength() == 0) return;
    long offset = buffer.getAddress() - base;
    long length = buffer.getLength();
    if (offset < 0 || length > packedLength - offset || offset % 8 != 0) {
      throw new IllegalArgumentException("Typed buffer lies outside aligned packed storage");
    }
    // Legacy Cascaded needs every segment's start aligned to 8B, including byte regions.
    // Leave an odd INT32 tail in the following byte segment along with padding.
    length -= length % 8;
    if (length > 0) typed.add(new Segment(offset, length, bits));
  }

  public static Segment[] describe(ContiguousTable packed, boolean typed) {
    long length = packed.getBuffer().getLength();
    if (length == 0) throw new IllegalArgumentException("Empty packed payload");
    if (!typed) return bounded(new Segment[] {new Segment(0, length, 8)});
    long base = packed.getBuffer().getAddress();
    Table table = packed.getTable();
    List<Segment> regions = new ArrayList<>();
    for (int c = 0; c < table.getNumberOfColumns(); c++) {
      ColumnView column = table.getColumn(c);
      DType type = column.getType();
      if (type.equals(DType.INT64)) {
        addTyped(regions, column.getData(), base, length, 64);
      } else if (type.equals(DType.INT32)) {
        addTyped(regions, column.getData(), base, length, 32);
      } else if (type.equals(DType.STRING)) {
        addTyped(regions, column.getOffsets(), base, length, 32);
      } else {
        throw new IllegalArgumentException("Type-aware experiment supports INT32/INT64/STRING: " + type);
      }
    }
    regions.sort(Comparator.comparingLong(s -> s.offset));
    List<Segment> result = new ArrayList<>();
    long next = 0;
    for (Segment region : regions) {
      if (region.offset < next) throw new IllegalArgumentException("Overlapping typed regions");
      if (region.offset > next) result.add(new Segment(next, region.offset - next, 8));
      result.add(region);
      next = Math.addExact(region.offset, region.length);
    }
    if (next < length) result.add(new Segment(next, length - next, 8));
    long covered = 0;
    for (Segment region : result) {
      if (region.offset != covered || region.offset % 8 != 0 || region.length <= 0
          || region.length % (region.typeBits / 8) != 0) {
        throw new IllegalStateException("Invalid packed segment coverage");
      }
      covered += region.length;
    }
    if (covered != length) throw new IllegalStateException("Incomplete packed segment coverage");
    return bounded(result.toArray(new Segment[0]));
  }

  /** Split full-layout regions without changing their order, type widths or packed offsets. */
  public static Segment[] bounded(Segment[] regions) {
    List<Segment> result = new ArrayList<>();
    long covered = 0;
    for (Segment region : regions) {
      if (region.offset != covered || region.offset % 8 != 0 || region.length <= 0
          || (region.typeBits != 8 && region.typeBits != 32 && region.typeBits != 64)
          || region.length % (region.typeBits / 8) != 0) {
        throw new IllegalArgumentException("Invalid full-layout region");
      }
      long remaining = region.length;
      long offset = region.offset;
      while (remaining > 0) {
        long take = Math.min(remaining, MAX_SUBMISSION_BYTES);
        result.add(new Segment(offset, take, region.typeBits));
        offset = Math.addExact(offset, take);
        remaining -= take;
      }
      covered = Math.addExact(covered, region.length);
    }
    return result.toArray(new Segment[0]);
  }
}
