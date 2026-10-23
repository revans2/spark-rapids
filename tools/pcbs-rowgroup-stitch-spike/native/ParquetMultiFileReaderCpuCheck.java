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

import java.nio.file.Files;
import java.nio.file.Paths;

/** Invalid arguments must fail before loading any native library or touching a GPU. */
public final class ParquetMultiFileReaderCpuCheck {
  private static void rejects(Class<? extends Throwable> expected, Runnable operation) {
    try {
      operation.run();
    } catch (Throwable failure) {
      if (expected.isInstance(failure)) {
        return;
      }
      throw new AssertionError("Unexpected exception", failure);
    }
    throw new AssertionError("Invalid input accepted");
  }

  public static void main(String[] arguments) throws Exception {
    rejects(NullPointerException.class, () -> ParquetMultiFileReader.read(null, null));
    rejects(NullPointerException.class,
        () -> ParquetMultiFileReader.read(ParquetOptions.DEFAULT, null));
    rejects(IllegalArgumentException.class,
        () -> ParquetMultiFileReader.read(ParquetOptions.DEFAULT, new HostMemoryBuffer[0]));
    rejects(NullPointerException.class,
        () -> ParquetMultiFileReader.read(ParquetOptions.DEFAULT, new HostMemoryBuffer[]{null}));
    String mappings = new String(Files.readAllBytes(Paths.get("/proc/self/maps")),
        java.nio.charset.StandardCharsets.UTF_8);
    if (mappings.contains("libcudf") || mappings.contains("libpcbs_parquet_multifile")) {
      throw new AssertionError("CPU validation unexpectedly loaded native libraries");
    }
    System.out.println("{\"cpu_argument_checks\":4,\"native_libraries_loaded\":false,\"success\":true}");
  }
}
