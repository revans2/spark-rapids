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

import java.lang.ref.Reference;
import java.util.Objects;

/** Research-only binding: every buffer contains one complete, independent Parquet file. */
public final class ParquetMultiFileReader {
  private ParquetMultiFileReader() {}

  private static final class Native {
    static {
      NativeDepsLoader.loadNativeDeps();
      System.loadLibrary("pcbs_parquet_multifile");
    }

    private static native long[] readFiles(
        String[] columns, int timestampType, long[] addressesAndLengths);
  }

  /**
   * Decode the files together using the existing cuDF reader.
   * Callers own the input buffers and must not close them concurrently with this synchronous call.
   * The returned table owns the decoded columns and must be closed by its caller.
   */
  public static Table read(ParquetOptions options, HostMemoryBuffer[] files) {
    Objects.requireNonNull(options, "options");
    Objects.requireNonNull(files, "files");
    if (files.length == 0) {
      throw new IllegalArgumentException("At least one independent Parquet file is required");
    }
    HostMemoryBuffer[] retained = files.clone();
    long[] addressesAndLengths = new long[Math.multiplyExact(retained.length, 2)];
    for (int i = 0; i < retained.length; i++) {
      HostMemoryBuffer buffer = Objects.requireNonNull(retained[i], "file buffer");
      long address = buffer.getAddress();
      long length = buffer.getLength();
      if (address == 0 || length <= 0) {
        throw new IllegalArgumentException("Each file buffer must be open and nonempty");
      }
      addressesAndLengths[2 * i] = address;
      addressesAndLengths[2 * i + 1] = length;
    }
    try {
      return new Table(Native.readFiles(options.getIncludeColumnNames(),
          options.timeUnit().typeId.getNativeId(), addressesAndLengths));
    } finally {
      Reference.reachabilityFence(retained);
    }
  }
}
