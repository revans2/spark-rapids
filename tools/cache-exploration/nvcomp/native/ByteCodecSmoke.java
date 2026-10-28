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
import java.util.Arrays;
import java.util.Random;

/** Small byte-exact GPU prerequisites, not performance measurements. */
public final class ByteCodecSmoke {
  private static int roundTrip(String codec, int length, boolean random) {
    final long chunk = 65536;
    byte[] expected = new byte[length];
    if (random) {
      new Random(37L + length).nextBytes(expected);
    } else {
      for (int i = 0; i < length; i++) expected[i] = (byte) ((i / 257) % 5);
    }
    try (HostMemoryBuffer host = HostMemoryBuffer.allocate(length);
         DeviceMemoryBuffer parent = DeviceMemoryBuffer.allocate(length + 8);
         DeviceMemoryBuffer input = parent.slice(8, length);
         DeviceMemoryBuffer output = DeviceMemoryBuffer.allocate(length)) {
      host.setBytes(0, expected, 0, length);
      input.copyFromHostBuffer(host, Cuda.DEFAULT_STREAM);
      input.incRefCount();
      try (CloseableArray<DeviceMemoryBuffer> compressed = CloseableArray.wrap(
          ByteCodec.compressor(codec, chunk, 1L << 20).compress(
              new BaseDeviceMemoryBuffer[]{input}, Cuda.DEFAULT_STREAM))) {
        int chunks = (int) ((length + chunk - 1) / chunk);
        int misaligned = 0;
        long[] compressedLengths = new long[chunks];
        try (HostMemoryBuffer bytes = HostMemoryBuffer.allocate(compressed.get(0).getLength())) {
          bytes.copyFromDeviceBuffer(compressed.get(0), Cuda.DEFAULT_STREAM);
          long offset = chunks * 8L;
          for (int i = 0; i < chunks; i++) {
            compressedLengths[i] = bytes.getLong(i * 8L);
            if (offset % 8 != 0) misaligned++;
            offset += compressedLengths[i];
          }
          if (offset != compressed.get(0).getLength()) {
            throw new AssertionError("Compressed framing length mismatch");
          }
        }
        compressed.get(0).incRefCount();
        ByteCodec.decompressor(codec, chunk).decompressAsync(
            new BaseDeviceMemoryBuffer[]{compressed.get(0)},
            new BaseDeviceMemoryBuffer[]{output}, Cuda.DEFAULT_STREAM);
        host.copyFromDeviceBuffer(output, Cuda.DEFAULT_STREAM);
        byte[] actual = new byte[length];
        host.getBytes(actual, 0, 0, length);
        if (!Arrays.equals(expected, actual)) throw new AssertionError("Byte round trip mismatch");
        if (chunks > 1 && random) {
          // Offset the entire frame so its first payload definitely requires ANS alignment repair.
          long encodedBytes = compressed.get(0).getLength();
          try (DeviceMemoryBuffer shiftedParent = DeviceMemoryBuffer.allocate(encodedBytes + 1);
               DeviceMemoryBuffer shifted = shiftedParent.slice(1, encodedBytes)) {
            if ((shifted.getAddress() + chunks * 8L) % 8 != 1) {
              throw new AssertionError("Forced compressed input is not misaligned");
            }
            shifted.copyFromDeviceBufferAsync(
                0, compressed.get(0), 0, encodedBytes, Cuda.DEFAULT_STREAM);
            shifted.incRefCount();
            ByteCodec.decompressor(codec, chunk).decompressAsync(
                new BaseDeviceMemoryBuffer[]{shifted},
                new BaseDeviceMemoryBuffer[]{output}, Cuda.DEFAULT_STREAM);
            host.copyFromDeviceBuffer(output, Cuda.DEFAULT_STREAM);
            host.getBytes(actual, 0, 0, length);
            if (!Arrays.equals(expected, actual)) {
              throw new AssertionError("Forced misaligned byte round trip mismatch");
            }
            System.out.println("PASS codec=" + codec + " forced frame offset=1 bytes=" + length);
          }
        }
        System.out.println("PASS codec=" + codec + " bytes=" + length + " random=" + random
            + " encodedBytes=" + compressed.get(0).getLength()
            + " chunkSizes=" + Arrays.toString(compressedLengths)
            + " misalignedStarts=" + misaligned);
        return misaligned;
      }
    }
  }

  public static void main(String[] args) {
    if (args.length != 1 || (!args[0].equals("snappy") && !args[0].equals("ans"))) {
      throw new IllegalArgumentException("Choose snappy or ans");
    }
    String codec = args[0];
    Rmm.initialize(RmmAllocationMode.CUDA_DEFAULT, null, 0);
    try {
      System.out.println(ByteCodec.nativeProvenance(codec));
      int misaligned = 0;
      for (int length : new int[]{1, 7, 257, 4099, 3 * 65536 + 7}) {
        for (boolean random : new boolean[]{false, true}) {
          misaligned += roundTrip(codec, length, random);
        }
      }
      System.out.println("PASS checked " + misaligned + " misaligned compressed chunk starts");
      if (Rmm.getTotalBytesAllocated() != 0) throw new AssertionError("Live RMM allocation leak");
      System.out.println("PASS all " + codec + " byte prerequisites, live RMM bytes=0");
    } finally {
      Rmm.shutdown();
    }
  }
}
