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

package cacheexploration;

import java.lang.ref.WeakReference;
import java.nio.channels.Channels;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

/** Correctness-only probe: force GC to detect encoded arrays pinned by native JNI references. */
public final class VortexLifecycleProbe {
  private static final int CAP = 4 * 1024 * 1024;

  private static byte[] input() throws Exception {
    try (BufferAllocator allocator = new RootAllocator(CAP);
         VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(
             List.of(Field.nullable("c0", new ArrowType.Int(64, true)))), allocator)) {
      BigIntVector column = (BigIntVector) root.getVector(0);
      column.allocateNew(512);
      for (int row = 0; row < 512; row++) column.set(row, row * 17L);
      root.setRowCount(512);
      VortexIpcCodec.Sink sink = new VortexIpcCodec.Sink(CAP);
      try (ArrowStreamWriter writer =
               new ArrowStreamWriter(root, null, Channels.newChannel(sink))) {
        writer.start();
        writer.writeBatch();
        writer.end();
      }
      return sink.result();
    }
  }

  private static WeakReference<byte[]> released(byte[] ipc, String mode) throws Exception {
    byte[] encoded = mode.equals("invalid") ? new byte[4] : VortexIpcCodec.encode(ipc, CAP);
    WeakReference<byte[]> reference = new WeakReference<>(encoded);
    if (mode.equals("invalid")) {
      boolean failed = false;
      try {
        VortexIpcCodec.open(encoded, new int[] {0}, true, CAP).close();
      } catch (RuntimeException expected) {
        failed = true;
      }
      if (!failed) throw new AssertionError("Invalid Vortex input unexpectedly opened");
    } else {
      VortexIpcCodec.Decoder decoder = VortexIpcCodec.open(encoded, new int[] {0}, true, CAP);
      try {
        if (mode.equals("exhaust")) {
          while (decoder.next() != null) {}
          if (decoder.rowsRead() != 512) throw new AssertionError("Lifecycle row count");
        } else if (mode.equals("partial")) {
          if (decoder.next() == null) throw new AssertionError("Expected first batch");
        }
      } finally {
        decoder.close();
        decoder.close();
      }
    }
    return reference;
  }

  public static void main(String[] ignored) throws Exception {
    byte[] ipc = input();
    for (String mode : List.of("exhaust", "partial", "unread", "invalid")) {
      WeakReference<byte[]> reference = released(ipc, mode);
      for (int attempt = 0; attempt < 100; attempt++) {
        System.gc();
        Thread.sleep(10);
        // refersTo avoids creating a strong local referent across the next GC.
        if (reference.refersTo(null)) break;
      }
      if (!reference.refersTo(null)) throw new AssertionError("Native reference retained " + mode);
      System.out.println("PASS encoded input released after " + mode);
    }
  }
}
