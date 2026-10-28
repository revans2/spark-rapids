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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/** Tiny GPU correctness check; run only after reserving the GPU. No performance claims. */
public final class CascadedSmoke {
  private static int roundTrip(int bits, int length) {
    return roundTrip(bits, length, 1024, false);
  }

  private static int roundTrip(int bits, int length, long chunk, boolean mixed) {
    byte[] expected = new byte[length];
    ByteBuffer data = ByteBuffer.wrap(expected).order(ByteOrder.LITTLE_ENDIAN);
    if (bits == 8) {
      for (int i = 0; i < length; i++) {
        expected[i] = chunk == 1024 ? (byte) ((i * 37) ^ (i >>> 3)) :
            (mixed ? (byte) ((i / 4096) % 5) : 0);
      }
    } else if (bits == 32) {
      int[] values = {Integer.MIN_VALUE, -1, 0, Integer.MAX_VALUE, 17, -37};
      for (int i = 0; data.remaining() > 0; i++) {
        int value = chunk == 1024 ? values[i % values.length] :
            values[(i / 8192) % values.length];
        if (mixed && (i / 257) % 3 == 0) value = i * 37;
        data.putInt(value);
      }
    } else {
      long[] values = {Long.MIN_VALUE, -1, 0, Long.MAX_VALUE, 17, -37};
      for (int i = 0; data.remaining() > 0; i++) {
        long value = chunk == 1024 ? values[i % values.length] :
            values[(i / 8192) % values.length];
        if (mixed && (i / 257) % 3 == 0) value = ((long) i << 33) - 37;
        data.putLong(value);
      }
    }
    try (HostMemoryBuffer host = HostMemoryBuffer.allocate(length);
         DeviceMemoryBuffer input = DeviceMemoryBuffer.allocate(length);
         DeviceMemoryBuffer output = DeviceMemoryBuffer.allocate(length)) {
      host.setBytes(0, expected, 0, length);
      input.copyFromHostBuffer(host, Cuda.DEFAULT_STREAM);
      input.incRefCount();
      try (CloseableArray<DeviceMemoryBuffer> compressed = CloseableArray.wrap(
          CascadedCodec.compressor(chunk, 1L << 20, bits).compress(
              new BaseDeviceMemoryBuffer[]{input}, Cuda.DEFAULT_STREAM))) {
        int chunks = (int) ((length + chunk - 1) / chunk);
        long[] compressedLengths = new long[chunks];
        int unaligned = 0;
        try (HostMemoryBuffer metadata = HostMemoryBuffer.allocate(compressed.get(0).getLength())) {
          metadata.copyFromDeviceBuffer(compressed.get(0), Cuda.DEFAULT_STREAM);
          long offset = chunks * 8L;
          for (int i = 0; i < chunks; i++) {
            compressedLengths[i] = metadata.getLong(i * 8L);
            if (offset % 8 != 0) unaligned++;
            offset += compressedLengths[i];
          }
        }
        compressed.get(0).incRefCount();
        CascadedCodec.decompressor(chunk).decompressAsync(
            new BaseDeviceMemoryBuffer[]{compressed.get(0)},
            new BaseDeviceMemoryBuffer[]{output}, Cuda.DEFAULT_STREAM);
        host.copyFromDeviceBuffer(output, Cuda.DEFAULT_STREAM);
        byte[] actual = new byte[length];
        host.getBytes(actual, 0, 0, length);
        if (!Arrays.equals(expected, actual)) throw new AssertionError("Byte round trip mismatch");
        System.out.println("PASS bits=" + bits + " bytes=" + length + " chunkBytes=" + chunk +
            " mixed=" + mixed + " chunks=" + chunks + " encodedBytes=" +
            compressed.get(0).getLength() +
            " sizes=" + Arrays.toString(compressedLengths) + " misaligned=" + unaligned);
        return unaligned;
      }
    }
  }

  public static void main(String[] args) {
    Rmm.initialize(RmmAllocationMode.CUDA_DEFAULT, null, 0);
    try {
      System.out.println(CascadedCodec.nativeProvenance());
      for (int n : new int[]{1, 7, 257, 4099}) roundTrip(8, n);
      for (int n : new int[]{4, 1028, 4100}) roundTrip(32, n);
      for (int n : new int[]{8, 1032, 4104}) roundTrip(64, n);
      // Exceed the SDK internal 4KiB unit and vary runs/deltas to avoid fallback-only coverage.
      int misalignedChunks = 0;
      for (int bits : new int[]{8, 32, 64}) {
        int tail = bits == 8 ? 7 : 3 * (bits / 8);
        misalignedChunks += roundTrip(bits, 3 * 65536 + tail, 65536, false);
        misalignedChunks += roundTrip(bits, 3 * 65536 + tail, 65536, true);
      }
      if (misalignedChunks == 0) throw new AssertionError("Alignment bounce was not exercised");
      System.out.println("PASS alignment bounce exercised on " + misalignedChunks + " chunks");
      try (DeviceMemoryBuffer invalid = DeviceMemoryBuffer.allocate(3)) {
        invalid.incRefCount();
        try {
          CascadedCodec.compressor(1024, 1L << 20, 32).compress(
              new BaseDeviceMemoryBuffer[]{invalid}, Cuda.DEFAULT_STREAM);
          throw new AssertionError("Invalid typed tail accepted");
        } catch (IllegalArgumentException expected) {
          System.out.println("PASS invalid typed tail rejected");
        }
      }
      if (Rmm.getTotalBytesAllocated() != 0) throw new AssertionError("Live device allocation leak");
      System.out.println("PASS all smoke cases, live RMM bytes=0");
    } finally {
      Rmm.shutdown();
    }
  }
}
