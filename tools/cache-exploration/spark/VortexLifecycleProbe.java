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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

/** Correctness-only probe: force GC to detect encoded arrays pinned by native JNI references. */
public final class VortexLifecycleProbe {
  private static final int CAP = 4 * 1024 * 1024;
  private static final int ROWS = 513;

  private static byte[] input() throws Exception {
    try (BufferAllocator allocator = new RootAllocator(CAP);
         VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(
             List.of(
                 Field.nullable("c0", new ArrowType.Int(64, true)),
                 Field.nullable("c1", new ArrowType.Int(32, true)),
                 Field.notNullable("c2", new ArrowType.Int(64, true)),
                 Field.nullable("c3", new ArrowType.Int(32, true)))), allocator)) {
      BigIntVector column = (BigIntVector) root.getVector(0);
      IntVector allNull = (IntVector) root.getVector(1);
      BigIntVector allValid = (BigIntVector) root.getVector(2);
      IntVector tail = (IntVector) root.getVector(3);
      root.getFieldVectors().forEach(vector -> vector.allocateNew());
      for (int row = 0; row < ROWS; row++) {
        if (row % 7 == 0) column.setNull(row); else column.setSafe(row, row * 17L);
        allNull.setNull(row);
        allValid.setSafe(row, row * 29L);
        if (row % 13 == 0) tail.setNull(row); else tail.setSafe(row, row * 37);
      }
      root.setRowCount(ROWS);
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

  private static int checkBuffers(Object[] batch, int start) {
    int rows = ((Integer) batch[0]).intValue();
    int[] widths = (int[]) batch[1];
    int[] nulls = (int[]) batch[2];
    ByteBuffer[] data = (ByteBuffer[]) batch[3];
    ByteBuffer[] valid = (ByteBuffer[]) batch[4];
    if (widths.length != 4) throw new AssertionError("Projected buffer column count");
    int[] expectedWidths = {4, 8, 4, 8};
    for (int c = 0; c < 4; c++) {
      if (widths[c] != expectedWidths[c]) throw new AssertionError("Buffer type order");
      int counted = 0;
      ByteBuffer values = data[c].duplicate().order(ByteOrder.LITTLE_ENDIAN);
      for (int row = 0; row < rows; row++) {
        int index = start + row;
        boolean expectedNull = c == 0 ? index % 13 == 0
            : c == 1 ? index % 7 == 0 : c == 2;
        boolean actualNull = valid[c] != null
            && (valid[c].get(row / 8) & (1 << (row % 8))) == 0;
        if (actualNull != expectedNull) throw new AssertionError("Borrowed validity/tail");
        if (actualNull) {
          counted++;
        } else {
          long actual = widths[c] == 4 ? values.getInt(row * 4) : values.getLong(row * 8);
          long expected = (long) index * (c == 0 ? 37 : c == 1 ? 17 : 29);
          if (actual != expected) throw new AssertionError("Borrowed data/order");
        }
      }
      if (counted != nulls[c]) throw new AssertionError("Exact borrowed null count");
    }
    return rows;
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
      boolean buffers = mode.startsWith("buffers-");
      VortexIpcCodec.Decoder decoder = VortexIpcCodec.open(
          encoded, buffers ? new int[] {3, 0, 1, 2} : new int[] {0}, !buffers, CAP);
      try {
        if (mode.equals("exhaust")) {
          while (decoder.next() != null) {}
          if (decoder.rowsRead() != ROWS) throw new AssertionError("Lifecycle row count");
        } else if (mode.equals("partial")) {
          if (decoder.next() == null) throw new AssertionError("Expected first batch");
        } else if (mode.equals("buffers-exhaust")) {
          int rows = 0;
          Object[] batch;
          while ((batch = decoder.nextBuffers()) != null) rows += checkBuffers(batch, rows);
          if (rows != ROWS || decoder.rowsRead() != ROWS) {
            throw new AssertionError("Borrowed buffer row count");
          }
        } else if (mode.equals("buffers-partial")) {
          Object[] batch = decoder.nextBuffers();
          if (batch == null) throw new AssertionError("Expected borrowed batch");
          checkBuffers(batch, 0);
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
    for (String mode : List.of("exhaust", "partial", "unread", "invalid",
        "buffers-exhaust", "buffers-partial", "buffers-unread")) {
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
