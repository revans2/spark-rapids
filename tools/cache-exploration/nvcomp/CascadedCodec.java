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
import ai.rapids.cudf.nvcomp.BatchedCompressor;
import ai.rapids.cudf.nvcomp.BatchedDecompressor;

/** Experimental legacy Cascaded adapter. Native callbacks synchronize to check every status. */
public final class CascadedCodec {
  private CascadedCodec() {}

  private static final class NativeLibrary {
    static {
      String path = System.getProperty("cacheexploration.cascaded.library");
      if (path == null || !new java.io.File(path).isAbsolute()) {
        throw new IllegalArgumentException(
            "Set cacheexploration.cascaded.library to the absolute bridge library path");
      }
      System.load(path);
    }
    static void load() {}
  }

  private static void validateChunk(long chunkBytes) {
    if (chunkBytes <= 0 || chunkBytes > (1L << 24) || chunkBytes % 8 != 0) {
      throw new IllegalArgumentException("Cascaded chunk size must be an 8-byte multiple <=16MiB");
    }
  }

  public static BatchedCompressor compressor(
      long chunkBytes, long maxIntermediateBytes, int typeBits) {
    validateChunk(chunkBytes);
    if (typeBits != 8 && typeBits != 32 && typeBits != 64) {
      throw new IllegalArgumentException("Cascaded type width must be 8, 32 or 64 bits");
    }
    if (maxIntermediateBytes <= 0) {
      throw new IllegalArgumentException("Intermediate buffer budget must be positive");
    }
    NativeLibrary.load();
    return new Compressor(chunkBytes, maxIntermediateBytes, typeBits);
  }

  public static BatchedDecompressor decompressor(long chunkBytes) {
    validateChunk(chunkBytes);
    NativeLibrary.load();
    return new Decompressor(chunkBytes);
  }

  /** Actual SDK shared object used by each explicitly resolved Cascaded entry point. */
  public static String nativeProvenance() {
    NativeLibrary.load();
    return provenance();
  }

  private static final class Compressor extends BatchedCompressor {
    private final int typeBits;

    Compressor(long chunkBytes, long maxIntermediateBytes, int typeBits) {
      super(chunkBytes, maxOutput(chunkBytes, typeBits), maxIntermediateBytes);
      this.typeBits = typeBits;
    }

    @Override
    public DeviceMemoryBuffer[] compress(BaseDeviceMemoryBuffer[] inputs, Cuda.Stream stream) {
      // The inherited method consumes its input references on both success and failure.
      try (CloseableArray<BaseDeviceMemoryBuffer> owned = CloseableArray.wrap(inputs)) {
        for (BaseDeviceMemoryBuffer input : inputs) {
          if (input.getAddress() % 8 != 0 || input.getLength() == 0 ||
              input.getLength() % (typeBits / 8) != 0) {
            throw new IllegalArgumentException("Cascaded input must be aligned, nonempty and " +
                "a multiple of the selected integer width");
          }
        }
        return super.compress(owned.release(), stream);
      }
    }

    @Override
    protected long batchedCompressGetTempSize(
        long batch, long chunk, long total, long stream) {
      return compressionWorkspace(batch, chunk, total, typeBits);
    }

    @Override
    protected void batchedCompressAsync(long inputs, long inputSizes, long chunk,
        long batch, long temp, long tempSize, long outputs, long outputSizes, long stream) {
      compressNative(inputs, inputSizes, chunk, batch, temp, tempSize,
          outputs, outputSizes, stream, typeBits);
    }
  }

  private static final class Decompressor extends BatchedDecompressor {
    Decompressor(long chunkBytes) {
      super(chunkBytes);
    }

    @Override
    protected long batchedDecompressGetTempSize(
        long batch, long chunk, long total, long stream) {
      return decompressionWorkspace(batch, chunk, total);
    }

    @Override
    protected void batchedDecompressAsync(long inputs, long inputSizes, long outputSizes,
        long batch, long temp, long tempSize, long outputs, long stream) {
      decompressNative(inputs, inputSizes, outputSizes, batch, temp, tempSize, outputs, stream);
    }
  }

  private static native long maxOutput(long chunk, int bits);
  private static native long compressionWorkspace(long batch, long chunk, long total, int bits);
  private static native long decompressionWorkspace(long batch, long chunk, long total);
  private static native void compressNative(long inputs, long inputSizes, long chunk,
      long batch, long temp, long tempSize, long outputs, long outputSizes, long stream, int bits);
  private static native void decompressNative(long inputs, long inputSizes, long outputSizes,
      long batch, long temp, long tempSize, long outputs, long stream);
  private static native String provenance();
}
