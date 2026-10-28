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

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdDecompressCtx;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;
import org.xerial.snappy.Snappy;

/**
 * Task-owned CPU codec for the experimental cuDF batched chunk envelope.
 * The owner is confined to its constructing thread and must close at task completion.
 * Input arrays are borrowed only during calls; returned arrays are independent owners.
 */
public final class CpuPackedCodec implements AutoCloseable {
  private final String codec;
  private final int chunkBytes;
  private final Thread owner = Thread.currentThread();
  private final Output output = new Output();
  private final LZ4Compressor lz4Compressor;
  private final LZ4SafeDecompressor lz4Decompressor;
  private ZstdCompressCtx zstdCompressor;
  private ZstdDecompressCtx zstdDecompressor;
  private byte[] compressedScratch;
  private boolean closed;

  private static final class Output extends ByteArrayOutputStream {
    void reserveHeader(int bytes) {
      reset();
      if (buf.length < bytes) buf = new byte[bytes];
      Arrays.fill(buf, 0, bytes, (byte) 0);
      count = bytes;
    }
    void putSize(int chunk, int size) {
      int offset = Math.multiplyExact(chunk, Long.BYTES);
      for (int i = 0; i < Long.BYTES; i++) {
        buf[offset + i] = (byte) ((long) size >>> (8 * i));
      }
    }
    void release() {
      reset();
      buf = new byte[0];
    }
  }

  public CpuPackedCodec(String codec, int chunkBytes) {
    this.codec = Objects.requireNonNull(codec, "codec");
    if (chunkBytes <= 0 || chunkBytes > 16 * 1024 * 1024) {
      throw new IllegalArgumentException("Invalid codec chunk size");
    }
    this.chunkBytes = chunkBytes;
    switch (codec) {
      case "uncompressed":
        lz4Compressor = null;
        lz4Decompressor = null;
        break;
      case "snappy":
        lz4Compressor = null;
        lz4Decompressor = null;
        break;
      case "lz4":
        LZ4Factory factory = LZ4Factory.fastestInstance();
        lz4Compressor = factory.fastCompressor();
        lz4Decompressor = factory.safeDecompressor();
        break;
      case "zstd":
        lz4Compressor = null;
        lz4Decompressor = null;
        // Native contexts are created lazily on the first operation of each direction.
        break;
      default:
        throw new UnsupportedOperationException("CPU packed codec is unavailable: " + codec);
    }
  }

  private void checkOpen() {
    if (Thread.currentThread() != owner) {
      throw new IllegalStateException("CPU codec used outside its owner thread");
    }
    if (closed) throw new IllegalStateException("CPU codec is closed");
  }

  private static void checkRange(byte[] bytes, int offset, int length) {
    Objects.requireNonNull(bytes, "bytes");
    if (offset < 0 || length < 0 || offset > bytes.length - length) {
      throw new IllegalArgumentException("Invalid byte range");
    }
  }

  private int chunks(int bytes) {
    return bytes == 0 ? 0 : (bytes - 1) / chunkBytes + 1;
  }

  private int maxCompressedLength(int bytes) {
    switch (codec) {
      case "lz4": return lz4Compressor.maxCompressedLength(bytes);
      case "snappy": return Snappy.maxCompressedLength(bytes);
      case "zstd": return Math.toIntExact(Zstd.compressBound(bytes));
      default: throw new IllegalStateException("No compressed codec");
    }
  }

  /** Encode with a little-endian size-vector prefix, followed by independent codec chunks. */
  public byte[] encode(byte[] input, int offset, int length) throws IOException {
    checkOpen();
    checkRange(input, offset, length);
    if (codec.equals("uncompressed")) return Arrays.copyOfRange(input, offset, offset + length);
    int count = chunks(length);
    int headerBytes = Math.multiplyExact(count, Long.BYTES);
    // Reject an impossible envelope before growing the reusable output storage.
    Math.toIntExact((long) headerBytes + (long) count * maxCompressedLength(chunkBytes));
    output.reserveHeader(headerBytes);
    int bound = maxCompressedLength(Math.min(length, chunkBytes));
    if (compressedScratch == null || compressedScratch.length < bound) {
      compressedScratch = new byte[bound];
    }
    for (int i = 0, consumed = 0; i < count; i++) {
      int raw = Math.min(chunkBytes, length - consumed);
      int encoded;
      switch (codec) {
        case "lz4":
          encoded = lz4Compressor.compress(input, offset + consumed, raw,
              compressedScratch, 0, compressedScratch.length);
          break;
        case "snappy":
          encoded = Snappy.compress(input, offset + consumed, raw, compressedScratch, 0);
          break;
        case "zstd":
          if (zstdCompressor == null) zstdCompressor = new ZstdCompressCtx();
          encoded = zstdCompressor.compressByteArray(compressedScratch, 0,
              compressedScratch.length, input, offset + consumed, raw);
          break;
        default: throw new IllegalStateException("No compressed codec");
      }
      if (encoded <= 0 || encoded > compressedScratch.length) {
        throw new IllegalArgumentException("Invalid compressed chunk size");
      }
      output.write(compressedScratch, 0, encoded);
      output.putSize(i, encoded);
      consumed += raw;
    }
    return output.toByteArray();
  }

  private static long getSize(byte[] input, int offset) {
    long size = 0;
    for (int i = 0; i < Long.BYTES; i++) {
      size |= (long) (input[offset + i] & 0xff) << (8 * i);
    }
    return size;
  }

  /** Decode exactly rawBytes; rejects truncated, extra or incorrectly sized chunks. */
  public byte[] decode(byte[] input, int offset, int length, int rawBytes) throws IOException {
    checkOpen();
    checkRange(input, offset, length);
    if (rawBytes < 0) throw new IllegalArgumentException("Negative raw byte length");
    if (codec.equals("uncompressed")) {
      if (length != rawBytes) throw new IllegalArgumentException("Uncompressed byte length mismatch");
      return Arrays.copyOfRange(input, offset, offset + length);
    }
    int count = chunks(rawBytes);
    int headerBytes = Math.multiplyExact(count, Long.BYTES);
    if (headerBytes > length) throw new IllegalArgumentException("Truncated chunk-size vector");
    long payloadBytes = 0;
    for (int i = 0; i < count; i++) {
      long size = getSize(input, offset + i * Long.BYTES);
      if (size <= 0 || size > Integer.MAX_VALUE) {
        throw new IllegalArgumentException("Invalid encoded chunk length");
      }
      payloadBytes = Math.addExact(payloadBytes, size);
      if (payloadBytes > length - headerBytes) {
        throw new IllegalArgumentException("Truncated chunk payload");
      }
    }
    if (payloadBytes != length - headerBytes) {
      throw new IllegalArgumentException("Extra chunk payload bytes");
    }
    byte[] result = new byte[rawBytes];
    int start = offset + headerBytes;
    for (int i = 0, produced = 0; i < count; i++) {
      int encoded = (int) getSize(input, offset + i * Long.BYTES);
      int raw = Math.min(chunkBytes, rawBytes - produced);
      int decoded;
      switch (codec) {
        case "lz4":
          decoded = lz4Decompressor.decompress(input, start, encoded, result, produced, raw);
          break;
        case "snappy":
          if (Snappy.uncompressedLength(input, start, encoded) != raw) {
            throw new IllegalArgumentException("Snappy chunk raw length mismatch");
          }
          decoded = Snappy.uncompress(input, start, encoded, result, produced);
          break;
        case "zstd":
          if (zstdDecompressor == null) zstdDecompressor = new ZstdDecompressCtx();
          decoded = zstdDecompressor.decompressByteArray(result, produced, raw,
              input, start, encoded);
          break;
        default: throw new IllegalStateException("No compressed codec");
      }
      if (decoded != raw) throw new IllegalArgumentException("Decoded chunk byte length mismatch");
      produced += raw;
      start += encoded;
    }
    return result;
  }

  @Override
  public void close() {
    if (Thread.currentThread() != owner) {
      throw new IllegalStateException("CPU codec closed outside its owner thread");
    }
    if (!closed) {
      closed = true;
      try {
        if (zstdCompressor != null) zstdCompressor.close();
      } finally {
        try {
          if (zstdDecompressor != null) zstdDecompressor.close();
        } finally {
          zstdCompressor = null;
          zstdDecompressor = null;
          compressedScratch = null;
          output.release();
        }
      }
    }
  }
}
