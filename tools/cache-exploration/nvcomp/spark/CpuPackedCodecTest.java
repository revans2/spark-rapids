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

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

/** CPU-only prerequisite checks; GPU interoperability is verified separately. */
public final class CpuPackedCodecTest {
  private static final int CHUNK = 64 * 1024;

  private interface CheckedAction {
    void run() throws Exception;
  }

  private static void require(boolean value, String message) {
    if (!value) throw new AssertionError(message);
  }

  private static void rejected(CheckedAction action) throws Exception {
    try {
      action.run();
    } catch (IllegalArgumentException | IllegalStateException | UnsupportedOperationException e) {
      return;
    }
    throw new AssertionError("Expected invalid operation rejection");
  }

  private static void decodeRejected(CheckedAction action) throws Exception {
    try {
      action.run();
    } catch (IOException | RuntimeException expected) {
      return;
    }
    throw new AssertionError("Expected corrupted data or decoded length rejection");
  }

  private static Object field(CpuPackedCodec codec, String name) throws Exception {
    Field value = CpuPackedCodec.class.getDeclaredField(name);
    value.setAccessible(true);
    return value.get(codec);
  }

  private static void roundTrip(CpuPackedCodec codec, byte[] original) throws Exception {
    byte[] source = new byte[original.length + 11];
    System.arraycopy(original, 0, source, 5, original.length);
    byte[] sourceBefore = source.clone();
    byte[] encoded = codec.encode(source, 5, original.length);
    byte[] wrapped = new byte[encoded.length + 17];
    System.arraycopy(encoded, 0, wrapped, 7, encoded.length);
    byte[] result = codec.decode(wrapped, 7, encoded.length, original.length);
    require(Arrays.equals(result, original), "CPU values differ");
    require(Arrays.equals(source, sourceBefore), "Encoding mutated borrowed input");
    byte[] saved = encoded.clone();
    codec.encode(new byte[99], 0, 99);
    require(Arrays.equals(encoded, saved), "Returned encoding aliases scratch storage");
    byte[] another = codec.encode(new byte[99], 0, 99);
    codec.decode(another, 0, another.length, 99);
    require(Arrays.equals(result, original), "Returned decoding aliases scratch storage");
  }

  private static void test(String name) throws Exception {
    Random random = new Random(459);
    try (CpuPackedCodec codec = new CpuPackedCodec(name, CHUNK)) {
      require(field(codec, "zstdCompressor") == null, "Eager Zstd compressor creation");
      require(field(codec, "zstdDecompressor") == null, "Eager Zstd decompressor creation");
      for (int length : new int[] {0, 1, 7, CHUNK - 1, CHUNK, CHUNK + 1, 3 * CHUNK + 19}) {
        byte[] bytes = new byte[length];
        roundTrip(codec, bytes);
        Arrays.fill(bytes, (byte) 42);
        roundTrip(codec, bytes);
        random.nextBytes(bytes);
        roundTrip(codec, bytes);
      }
      if (name.equals("zstd")) {
        Object compressor = field(codec, "zstdCompressor");
        Object decompressor = field(codec, "zstdDecompressor");
        require(compressor != null && decompressor != null, "Missing Zstd contexts");
        roundTrip(codec, new byte[CHUNK + 43]);
        require(compressor == field(codec, "zstdCompressor"), "Recreated Zstd compressor");
        require(decompressor == field(codec, "zstdDecompressor"), "Recreated Zstd decompressor");
      } else {
        require(field(codec, "zstdCompressor") == null, "Unneeded Zstd compressor");
        require(field(codec, "zstdDecompressor") == null, "Unneeded Zstd decompressor");
      }
      rejected(() -> codec.encode(new byte[3], -1, 1));
      rejected(() -> codec.encode(new byte[3], 0, 4));
      rejected(() -> codec.decode(new byte[3], 0, 4, 3));
      rejected(() -> codec.decode(new byte[3], 0, 3, -1));
      byte[] raw = new byte[CHUNK + 13];
      random.nextBytes(raw);
      byte[] encoded = codec.encode(raw, 0, raw.length);
      decodeRejected(() -> codec.decode(encoded, 0, encoded.length, raw.length - 1));
      decodeRejected(() -> codec.decode(encoded, 0, encoded.length, raw.length + 1));
      if (!name.equals("uncompressed")) {
        byte[] corrupt = encoded.clone();
        // Preserve the two-entry size vector, corrupt only the codec payload.
        Arrays.fill(corrupt, 2 * Long.BYTES, corrupt.length, (byte) 0xff);
        decodeRejected(() -> codec.decode(corrupt, 0, corrupt.length, raw.length));
      }
      rejected(() -> codec.decode(encoded, 0, encoded.length - 1, raw.length));
      byte[] extra = Arrays.copyOf(encoded, encoded.length + 1);
      rejected(() -> codec.decode(extra, 0, extra.length, raw.length));
      if (!name.equals("uncompressed")) {
        rejected(() -> codec.decode(encoded, 0, 7, raw.length));
        byte[] zero = encoded.clone();
        Arrays.fill(zero, 0, 8, (byte) 0);
        rejected(() -> codec.decode(zero, 0, zero.length, raw.length));
        byte[] negative = encoded.clone();
        Arrays.fill(negative, 0, 8, (byte) 0xff);
        rejected(() -> codec.decode(negative, 0, negative.length, raw.length));
        byte[] large = encoded.clone();
        large[7] = 1;
        rejected(() -> codec.decode(large, 0, large.length, raw.length));
        rejected(() -> codec.decode(new byte[0], 0, 0, 1));
        rejected(() -> codec.decode(encoded, 0, encoded.length, 0));
      }
      AtomicReference<Throwable> failure = new AtomicReference<>();
      Thread other = new Thread(() -> {
        try {
          codec.encode(new byte[1], 0, 1);
          failure.set(new AssertionError("Cross-thread access succeeded"));
        } catch (IllegalStateException expected) {
          // expected owner check
        } catch (Throwable unexpected) {
          failure.set(unexpected);
        }
      });
      other.start();
      other.join();
      require(failure.get() == null, "Cross-thread access check failed: " + failure.get());
    }
    CpuPackedCodec closed = new CpuPackedCodec(name, CHUNK);
    byte[] beforeClose = closed.encode(new byte[99], 0, 99);
    closed.decode(beforeClose, 0, beforeClose.length, 99);
    closed.close();
    closed.close();
    rejected(() -> closed.encode(new byte[1], 0, 1));
    rejected(() -> closed.decode(new byte[0], 0, 0, 0));
    require(field(closed, "zstdCompressor") == null, "Closed compressor retained");
    require(field(closed, "zstdDecompressor") == null, "Closed decompressor retained");
    System.out.println("PASS CPU codec " + name);
  }

  public static void main(String[] args) throws Exception {
    for (String name : new String[] {"uncompressed", "lz4", "snappy", "zstd"}) test(name);
    rejected(() -> new CpuPackedCodec("ans", CHUNK));
    rejected(() -> new CpuPackedCodec("cascaded-typed", CHUNK));
    rejected(() -> new CpuPackedCodec("snappy", 0));
    rejected(() -> new CpuPackedCodec("snappy", 16 * 1024 * 1024 + 1));
    System.out.println("PASS CPU codec prerequisite checks; GPU interoperability not tested here");
  }
}
