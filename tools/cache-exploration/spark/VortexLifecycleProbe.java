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
import java.io.InputStream;
import java.io.IOException;
import java.util.Arrays;
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
import org.apache.arrow.vector.ipc.ArrowStreamReader;
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

  private static final class ChunkInput extends InputStream {
    private byte[][] chunks;
    private final int length;
    private int position;
    ChunkInput(Object[] stream) {
      chunks = (byte[][]) stream[0];
      length = ((Integer) stream[1]).intValue();
    }
    @Override public int read() {
      if (position == length) return -1;
      int value = chunks[position / VortexIpcCodec.ChunkedSink.CHUNK_BYTES]
          [position % VortexIpcCodec.ChunkedSink.CHUNK_BYTES] & 255;
      position++;
      return value;
    }
    @Override public int read(byte[] target, int offset, int size) {
      java.util.Objects.checkFromIndexSize(offset, size, target.length);
      if (size == 0) return 0;
      if (position == length) return -1;
      int total = Math.min(size, length - position);
      int copied = 0;
      while (copied < total) {
        int within = position % VortexIpcCodec.ChunkedSink.CHUNK_BYTES;
        int count = Math.min(total - copied,
            VortexIpcCodec.ChunkedSink.CHUNK_BYTES - within);
        System.arraycopy(chunks[position / VortexIpcCodec.ChunkedSink.CHUNK_BYTES],
            within, target, offset + copied, count);
        copied += count;
        position += count;
      }
      return total;
    }
    @Override public void close() { chunks = null; }
  }

  private static void checkChunkSink() throws IOException {
    int cap = 2 * VortexIpcCodec.ChunkedSink.CHUNK_BYTES;
    byte[] expected = new byte[cap];
    for (int i = 0; i < cap; i++) expected[i] = (byte) (i * 37);
    VortexIpcCodec.ChunkedSink sink = new VortexIpcCodec.ChunkedSink(cap);
    sink.write(expected[0]);
    sink.write(expected, 1, VortexIpcCodec.ChunkedSink.CHUNK_BYTES - 2);
    sink.write(expected, VortexIpcCodec.ChunkedSink.CHUNK_BYTES - 1,
        VortexIpcCodec.ChunkedSink.CHUNK_BYTES + 1);
    boolean failed = false;
    try { sink.write(1); } catch (IllegalArgumentException limit) { failed = true; }
    if (!failed) throw new AssertionError("Chunk cap was not enforced");
    try (InputStream input = new ChunkInput(sink.result())) {
      if (!Arrays.equals(expected, input.readAllBytes())) {
        throw new AssertionError("Chunk boundary bytes/cap atomicity");
      }
    }
    System.out.println("PASS chunk sink boundary and hard cap");
  }

  private static int checkChunkIpc(Object[] stream, int start) throws Exception {
    try (BufferAllocator allocator = new RootAllocator(CAP);
         ArrowStreamReader reader = new ArrowStreamReader(new ChunkInput(stream), allocator)) {
      if (!reader.loadNextBatch()) throw new AssertionError("Expected chunked IPC batch");
      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      int rows = root.getRowCount();
      int[] widths = {4, 8, 4, 8};
      int[] nulls = new int[4];
      ByteBuffer[] data = new ByteBuffer[4];
      ByteBuffer[] validity = new ByteBuffer[4];
      for (int c = 0; c < 4; c++) {
        org.apache.arrow.vector.FieldVector vector = root.getVector(c);
        nulls[c] = vector.getNullCount();
        data[c] = vector.getDataBuffer().nioBuffer(0, rows * widths[c]);
        if (nulls[c] != 0) {
          validity[c] = vector.getValidityBuffer().nioBuffer(0, (rows + 7) / 8);
        }
      }
      checkBuffers(new Object[] {rows, widths, nulls, data, validity}, start);
      if (reader.loadNextBatch()) throw new AssertionError("More than one IPC batch per stream");
      return rows;
    } finally {
      Arrays.fill((byte[][]) stream[0], null);
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
      boolean buffers = mode.startsWith("buffers-");
      boolean chunks = mode.startsWith("chunks-");
      VortexIpcCodec.Decoder decoder = VortexIpcCodec.open(
          encoded, (buffers || chunks) ? new int[] {3, 0, 1, 2} : new int[] {0},
          !(buffers || chunks), CAP);
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
        } else if (mode.equals("chunks-exhaust")) {
          int rows = 0;
          Object[] batch;
          while ((batch = decoder.nextChunks()) != null) rows += checkChunkIpc(batch, rows);
          if (rows != ROWS || decoder.rowsRead() != ROWS) {
            throw new AssertionError("Chunked IPC row count");
          }
        } else if (mode.equals("chunks-partial")) {
          Object[] batch = decoder.nextChunks();
          if (batch == null) throw new AssertionError("Expected chunked IPC");
          checkChunkIpc(batch, 0);
        }
      } finally {
        decoder.close();
        decoder.close();
      }
    }
    return reference;
  }

  public static void main(String[] ignored) throws Exception {
    checkChunkSink();
    byte[] ipc = input();
    for (String mode : List.of("exhaust", "partial", "unread", "invalid",
        "buffers-exhaust", "buffers-partial", "buffers-unread",
        "chunks-exhaust", "chunks-partial", "chunks-unread")) {
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
