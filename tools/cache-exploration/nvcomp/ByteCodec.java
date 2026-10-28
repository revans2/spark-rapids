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

/** Experimental Snappy and generic-byte rANS adapters with checked native completion. */
public final class ByteCodec {
  private ByteCodec() {}

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

  private static int codecId(String codec) {
    switch (codec) {
      case "snappy": return 1;
      case "ans": return 2;
      default: throw new IllegalArgumentException("Unsupported byte codec: " + codec);
    }
  }

  private static void validateChunk(long chunkBytes) {
    if (chunkBytes <= 0 || chunkBytes > (1L << 24) || chunkBytes % 8 != 0) {
      throw new IllegalArgumentException("Codec chunk size must be an 8-byte multiple <=16MiB");
    }
  }

  public static BatchedCompressor compressor(
      String codec, long chunkBytes, long maxIntermediateBytes) {
    validateChunk(chunkBytes);
    int id = codecId(codec);
    if (maxIntermediateBytes <= 0) {
      throw new IllegalArgumentException("Intermediate buffer budget must be positive");
    }
    NativeLibrary.load();
    return new Compressor(chunkBytes, maxIntermediateBytes, id);
  }

  public static BatchedDecompressor decompressor(String codec, long chunkBytes) {
    validateChunk(chunkBytes);
    int id = codecId(codec);
    NativeLibrary.load();
    return new Decompressor(chunkBytes, id);
  }

  /** Exact SDK symbols/options used by this experimental codec. */
  public static String nativeProvenance(String codec) {
    int id = codecId(codec);
    NativeLibrary.load();
    return provenance(id);
  }

  private static final class Compressor extends BatchedCompressor {
    private final int codec;

    Compressor(long chunkBytes, long maxIntermediateBytes, int codec) {
      super(chunkBytes, maxOutput(chunkBytes, codec), maxIntermediateBytes);
      this.codec = codec;
    }

    @Override
    public DeviceMemoryBuffer[] compress(BaseDeviceMemoryBuffer[] inputs, Cuda.Stream stream) {
      // The inherited method consumes its input references on both success and failure.
      try (CloseableArray<BaseDeviceMemoryBuffer> owned = CloseableArray.wrap(inputs)) {
        for (BaseDeviceMemoryBuffer input : inputs) {
          if (input.getAddress() % 8 != 0 || input.getLength() == 0) {
            throw new IllegalArgumentException("Byte codec input must be aligned and nonempty");
          }
        }
        return super.compress(owned.release(), stream);
      }
    }

    @Override
    protected long batchedCompressGetTempSize(
        long batch, long chunk, long total, long stream) {
      return compressionWorkspace(batch, chunk, total, codec);
    }

    @Override
    protected void batchedCompressAsync(long inputs, long inputSizes, long chunk,
        long batch, long temp, long tempSize, long outputs, long outputSizes, long stream) {
      compressNative(inputs, inputSizes, chunk, batch, temp, tempSize,
          outputs, outputSizes, stream, codec);
    }
  }

  private static final class Decompressor extends BatchedDecompressor {
    private final long chunkBytes;
    private final int codec;

    Decompressor(long chunkBytes, int codec) {
      super(chunkBytes);
      this.chunkBytes = chunkBytes;
      this.codec = codec;
    }

    @Override
    protected long batchedDecompressGetTempSize(
        long batch, long chunk, long total, long stream) {
      return decompressionWorkspace(batch, chunk, total, codec);
    }

    @Override
    protected void batchedDecompressAsync(long inputs, long inputSizes, long outputSizes,
        long batch, long temp, long tempSize, long outputs, long stream) {
      decompressNative(inputs, inputSizes, outputSizes, batch, temp, tempSize, outputs,
          stream, chunkBytes, codec);
    }
  }

  private static native long maxOutput(long chunk, int codec);
  private static native long compressionWorkspace(long batch, long chunk, long total, int codec);
  private static native long decompressionWorkspace(long batch, long chunk, long total, int codec);
  private static native void compressNative(long inputs, long inputSizes, long chunk,
      long batch, long temp, long tempSize, long outputs, long outputSizes, long stream, int codec);
  private static native void decompressNative(long inputs, long inputSizes, long outputSizes,
      long batch, long temp, long tempSize, long outputs, long stream, long chunk, int codec);
  private static native String provenance(int codec);
}
