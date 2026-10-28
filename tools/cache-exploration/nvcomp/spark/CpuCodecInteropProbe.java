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
import ai.rapids.cudf.CloseableArray;
import ai.rapids.cudf.Cuda;
import ai.rapids.cudf.DeviceMemoryBuffer;
import ai.rapids.cudf.HostMemoryBuffer;
import ai.rapids.cudf.Rmm;
import ai.rapids.cudf.RmmAllocationMode;
import ai.rapids.cudf.nvcomp.BatchedCompressor;
import ai.rapids.cudf.nvcomp.BatchedDecompressor;
import ai.rapids.cudf.nvcomp.BatchedLZ4Compressor;
import ai.rapids.cudf.nvcomp.BatchedLZ4Decompressor;
import ai.rapids.cudf.nvcomp.BatchedZstdCompressor;
import ai.rapids.cudf.nvcomp.BatchedZstdDecompressor;
import java.util.Arrays;
import java.util.Random;

import org.apache.spark.sql.cacheexploration.CpuPackedCodec;

/** Byte compatibility prerequisites only; does not measure CPU table encoding or Spark caching. */
public final class CpuCodecInteropProbe {
  private static final int CHUNK_BYTES = 65536;

  private static BatchedCompressor compressor(String codec) {
    switch (codec) {
      case "lz4": return new BatchedLZ4Compressor(CHUNK_BYTES, 1L << 20);
      case "zstd": return new BatchedZstdCompressor(CHUNK_BYTES, 1L << 20);
      case "snappy": return ByteCodec.compressor(codec, CHUNK_BYTES, 1L << 20);
      default: throw new IllegalArgumentException(codec);
    }
  }

  private static BatchedDecompressor decompressor(String codec) {
    switch (codec) {
      case "lz4": return new BatchedLZ4Decompressor(CHUNK_BYTES);
      case "zstd": return new BatchedZstdDecompressor(CHUNK_BYTES);
      case "snappy": return ByteCodec.decompressor(codec, CHUNK_BYTES);
      default: throw new IllegalArgumentException(codec);
    }
  }

  private static byte[] download(DeviceMemoryBuffer input) {
    int length = Math.toIntExact(input.getLength());
    byte[] result = new byte[length];
    try (HostMemoryBuffer host = HostMemoryBuffer.allocate(length)) {
      host.copyFromDeviceBuffer(input, Cuda.DEFAULT_STREAM);
      host.getBytes(result, 0, 0, length);
    }
    return result;
  }

  private static void verify(String codec, CpuPackedCodec cpu, BatchedCompressor gpuEncoder,
      BatchedDecompressor gpuDecoder, int length, boolean random) throws Exception {
    byte[] expected = new byte[length];
    if (random) new Random(37L + length).nextBytes(expected);
    else for (int i = 0; i < length; i++) expected[i] = (byte) ((i / 257) % 5);
    try (HostMemoryBuffer host = HostMemoryBuffer.allocate(length);
         DeviceMemoryBuffer input = DeviceMemoryBuffer.allocate(length);
         DeviceMemoryBuffer decoded = DeviceMemoryBuffer.allocate(length)) {
      host.setBytes(0, expected, 0, length);
      input.copyFromHostBuffer(host, Cuda.DEFAULT_STREAM);
      input.incRefCount();
      try (CloseableArray<DeviceMemoryBuffer> encoded = CloseableArray.wrap(
          gpuEncoder.compress(new BaseDeviceMemoryBuffer[]{input}, Cuda.DEFAULT_STREAM))) {
        byte[] bytes = download(encoded.get(0));
        if (!Arrays.equals(expected, cpu.decode(bytes, 0, bytes.length, length))) {
          throw new AssertionError(codec + " GPU encode -> CPU decode");
        }
      }
      byte[] bytes = cpu.encode(expected, 0, length);
      try (HostMemoryBuffer encodedHost = HostMemoryBuffer.allocate(bytes.length);
           DeviceMemoryBuffer encoded = DeviceMemoryBuffer.allocate(bytes.length)) {
        encodedHost.setBytes(0, bytes, 0, bytes.length);
        encoded.copyFromHostBuffer(encodedHost, Cuda.DEFAULT_STREAM);
        encoded.incRefCount();
        try {
          gpuDecoder.decompressAsync(new BaseDeviceMemoryBuffer[]{encoded},
              new BaseDeviceMemoryBuffer[]{decoded}, Cuda.DEFAULT_STREAM);
          if (!Arrays.equals(expected, download(decoded))) {
            throw new AssertionError(codec + " CPU encode -> GPU decode");
          }
        } finally {
          // Drain even on failure before an asynchronous caller's borrowed buffers close.
          Cuda.DEFAULT_STREAM.sync();
        }
      }
    }
    if (Rmm.getTotalBytesAllocated() != 0) throw new AssertionError("GPU allocation leak");
    System.out.println("PASS interop codec=" + codec + " bytes=" + length + " random=" + random);
  }

  public static void main(String[] args) throws Exception {
    Rmm.initialize(RmmAllocationMode.CUDA_DEFAULT, null, 0);
    try {
      for (String codec : new String[]{"lz4", "zstd", "snappy"}) {
        // Factories can throw (for example while loading Snappy's JNI bridge).
        // Construct before any buffer reference is transferred to a codec.
        BatchedCompressor gpuEncoder = compressor(codec);
        BatchedDecompressor gpuDecoder = decompressor(codec);
        try (CpuPackedCodec cpu = new CpuPackedCodec(codec, CHUNK_BYTES)) {
          for (int length : new int[]{1, 7, 257, 65535, 65536, 65537, 3 * 65536 + 7}) {
            for (boolean random : new boolean[]{false, true}) verify(codec, cpu, gpuEncoder, gpuDecoder, length, random);
          }
        }
      }
      System.out.println("PASS CPU/GPU codec interop; no CPU table-layout or Spark performance claim");
    } finally {
      Rmm.shutdown();
    }
  }
}
