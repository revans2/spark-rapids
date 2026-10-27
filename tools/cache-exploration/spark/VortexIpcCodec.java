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

import dev.vortex.api.Session;
import dev.vortex.api.VortexWriter;
import dev.vortex.io.NativeReadable;
import dev.vortex.io.NativeWritable;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ViewVarCharVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

/** Loaded only in the isolated Arrow 19 loader; its boundary contains JDK values only. */
public final class VortexIpcCodec {
  private static final Session SESSION = Session.create();
  private static final AtomicLong IDS = new AtomicLong();

  static final class Sink extends OutputStream implements NativeWritable {
    private byte[] bytes = new byte[65536];
    private int size;
    private final int cap;
    Sink(int cap) { this.cap = cap; }
    private void reserve(int added) {
      long wanted = (long) size + added;
      if (wanted > cap) throw new IllegalArgumentException("IPC/cache record exceeds cap " + cap);
      if (wanted > bytes.length) {
        bytes = Arrays.copyOf(bytes, (int) Math.min(cap, Math.max(wanted, 2L * bytes.length)));
      }
    }
    @Override public void write(int value) { reserve(1); bytes[size++] = (byte) value; }
    @Override public void write(byte[] input, int offset, int length) {
      java.util.Objects.checkFromIndexSize(offset, length, input.length);
      reserve(length);
      System.arraycopy(input, offset, bytes, size, length);
      size += length;
    }
    @Override public void flush() {}
    @Override public void close() {}
    byte[] result() { return Arrays.copyOf(bytes, size); }
  }

  public static byte[] encode(byte[] ipc, int cap) throws Exception {
    if (ipc.length > cap) throw new IllegalArgumentException("IPC input exceeds cap");
    try (BufferAllocator allocator = new RootAllocator(4L * cap)) {
      Sink sink = new Sink(cap);
      try (ArrowStreamReader reader =
               new ArrowStreamReader(new ByteArrayInputStream(ipc), allocator);
           VortexWriter writer = VortexWriter.builder(SESSION, sink,
               reader.getVectorSchemaRoot().getSchema(), allocator).build()) {
        while (reader.loadNextBatch()) {
          try (ArrowArray array = ArrowArray.allocateNew(allocator);
               ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
            try {
              Data.exportVectorSchemaRoot(
                  allocator, reader.getVectorSchemaRoot(), null, array, schema);
              writer.writeBatch(array.memoryAddress(), schema.memoryAddress());
            } finally {
              try { array.release(); } finally { schema.release(); }
            }
          }
        }
        writer.finish();
      }
      if (allocator.getAllocatedMemory() != 0) {
        throw new AssertionError("Arrow encoder allocation leak");
      }
      return sink.result();
    }
  }

  /** Release the array even if canceled native tasks retain this callback temporarily. */
  private static final class Source implements NativeReadable {
    private final String name = "cache-" + IDS.incrementAndGet() + ".vortex";
    private final int length;
    private byte[] bytes;
    Source(byte[] bytes) { this.bytes = bytes; this.length = bytes.length; }
    @Override public String name() { return name; }
    @Override public long length() { return length; }
    @Override
    public synchronized void readFully(long position, ByteBuffer target) throws EOFException {
      byte[] input = bytes;
      int n = target.remaining();
      if (input == null || position < 0 || position > length - (long) n) {
        throw new EOFException("Closed or out-of-bounds " + name);
      }
      target.put(input, (int) position, n);
    }
    @Override public synchronized void close() { bytes = null; }
  }

  public static Decoder open(byte[] encoded, int[] projection, boolean legacy, int cap) {
    return new Decoder(encoded, projection, legacy, cap);
  }

  public static final class Decoder implements AutoCloseable {
    private final BufferAllocator allocator;
    private final boolean legacy;
    private final int cap;
    private Source readable;
    private long sessionPointer;
    private long sourcePointer;
    private long scanPointer;
    private ArrowReader reader;
    private boolean closed;
    private int rowsRead;

    Decoder(byte[] encoded, int[] projection, boolean legacy, int cap) {
      this.legacy = legacy;
      this.cap = cap;
      allocator = new RootAllocator(4L * cap);
      long root = 0;
      long packed = 0;
      long[] columns = new long[projection.length];
      try {
        sessionPointer = dev.vortex.jni.NativeSession.newSession();
        readable = new Source(encoded);
        sourcePointer = dev.vortex.jni.NativeDataSource.openFiles(sessionPointer,
            new Object[] {readable}, new String[] {readable.name()},
            new long[] {encoded.length}, 1);
        root = dev.vortex.jni.NativeExpression.root();
        String[] names = new String[projection.length];
        for (int i = 0; i < projection.length; i++) {
          columns[i] = dev.vortex.jni.NativeExpression.getItem("c" + projection[i], root);
          names[i] = "p" + i;
        }
        packed = dev.vortex.jni.NativeExpression.pack(names, columns, false);
        scanPointer = dev.vortex.jni.NativeScan.create(sourcePointer, packed, 0, 0, 0,
            null, null, (byte) 0, 0, true);
        if (sourcePointer == 0 || scanPointer == 0) {
          throw new IllegalStateException("Native source/scan creation returned zero");
        }
      } catch (Throwable failure) {
        try { close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
        throw failure;
      } finally {
        // Native expression builders and scan creation clone their input expressions.
        if (packed != 0) dev.vortex.jni.NativeExpression.free(packed);
        for (long column : columns) {
          if (column != 0) dev.vortex.jni.NativeExpression.free(column);
        }
        if (root != 0) dev.vortex.jni.NativeExpression.free(root);
      }
    }

    public int rowsRead() { return rowsRead; }

    /** Returns one complete IPC batch, or null at EOF; never materializes the whole partition. */
    public byte[] next() throws Exception {
      try {
        VectorSchemaRoot root = nextRoot();
        if (root == null) return null;
        Sink sink = new Sink(cap);
        if (legacy) {
          try (VectorSchemaRoot normalized = legacyRoot(root, allocator)) {
            writeIpc(normalized, sink);
          }
        } else {
          writeIpc(root, sink);
        }
        return sink.result();
      } catch (Throwable failure) {
        try { close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
        throw failure;
      }
    }

    /**
     * JDK-only borrowed view of one int/long batch. The buffers remain valid until the next
     * decoder operation or close; the caller must finish copying before advancing.
     */
    public Object[] nextBuffers() throws Exception {
      try {
        VectorSchemaRoot root = nextRoot();
        if (root == null) return null;
        int rows = root.getRowCount();
        int columns = root.getFieldVectors().size();
        int[] widths = new int[columns];
        int[] nulls = new int[columns];
        ByteBuffer[] data = new ByteBuffer[columns];
        ByteBuffer[] validity = new ByteBuffer[columns];
        long bytes = 0;
        for (int c = 0; c < columns; c++) {
          FieldVector vector = root.getVector(c);
          if (!(vector.getField().getType() instanceof ArrowType.Int integer)
              || !integer.getIsSigned()
              || (integer.getBitWidth() != 32 && integer.getBitWidth() != 64)) {
            throw new IllegalArgumentException("Direct buffer GPU decode supports signed int/long only");
          }
          widths[c] = integer.getBitWidth() / 8;
          int length = Math.toIntExact((long) rows * widths[c]);
          nulls[c] = vector.getNullCount();
          data[c] = vector.getDataBuffer().nioBuffer(0, length).asReadOnlyBuffer();
          int maskLength = Math.toIntExact(((long) rows + 7) / 8);
          if (nulls[c] != 0) {
            validity[c] = vector.getValidityBuffer().nioBuffer(0, maskLength).asReadOnlyBuffer();
          }
          bytes += length + (nulls[c] == 0 ? 0 : maskLength);
          if (bytes > cap) {
            throw new IllegalArgumentException("Direct buffer batch exceeds hard cap " + cap);
          }
        }
        return new Object[] {Integer.valueOf(rows), widths, nulls, data, validity};
      } catch (Throwable failure) {
        try { close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
        throw failure;
      }
    }

    private VectorSchemaRoot nextRoot() throws Exception {
      if (closed) return null;
      try {
        while (true) {
          if (reader != null && reader.loadNextBatch()) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            rowsRead = Math.addExact(rowsRead, root.getRowCount());
            return root;
          }
          if (reader != null) { reader.close(); reader = null; }
          long partition = dev.vortex.jni.NativeScan.nextPartition(scanPointer);
          if (partition == 0) { close(); return null; }
          org.apache.arrow.c.ArrowArrayStream stream = null;
          try {
            stream = org.apache.arrow.c.ArrowArrayStream.allocateNew(allocator);
            long consumed = partition;
            partition = 0;
            // Native consumes the partition even when this call throws.
            dev.vortex.jni.NativePartition.scanArrow(
                sessionPointer, consumed, stream.memoryAddress());
            // Import moves the stream into the reader; this original struct stays caller-owned.
            reader = Data.importArrayStream(allocator, stream, false);
          } finally {
            if (partition != 0) dev.vortex.jni.NativePartition.free(partition);
            if (stream != null) {
              try { stream.release(); } finally { stream.close(); }
            }
          }
        }
      } catch (Throwable failure) {
        try { close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
        throw failure;
      }
    }

    @Override public void close() throws Exception {
      if (!closed) {
        closed = true;
        try {
          if (reader != null) reader.close();
        } finally {
          reader = null;
          try {
            if (scanPointer != 0) dev.vortex.jni.NativeScan.free(scanPointer);
          } finally {
            scanPointer = 0;
            try {
              if (sourcePointer != 0) dev.vortex.jni.NativeDataSource.free(sourcePointer);
            } finally {
              sourcePointer = 0;
              try {
                if (sessionPointer != 0) dev.vortex.jni.NativeSession.free(sessionPointer);
              } finally {
                sessionPointer = 0;
                try {
                  if (readable != null) readable.close();
                } finally {
                  readable = null;
                  allocator.close();
                }
              }
            }
          }
        }
      }
    }
  }

  private static void writeIpc(VectorSchemaRoot root, Sink sink) throws Exception {
    try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(sink))) {
      writer.start();
      writer.writeBatch();
      writer.end();
    }
  }

  /** Arrow 12 cannot read Utf8View; normalize only on the CPU Spark decode route. */
  private static VectorSchemaRoot legacyRoot(VectorSchemaRoot source, BufferAllocator allocator) {
    List<Field> fields = new ArrayList<>();
    for (Field field : source.getSchema().getFields()) {
      fields.add(field.getType() instanceof ArrowType.Utf8View
          ? Field.nullable(field.getName(), new ArrowType.Utf8()) : field);
    }
    VectorSchemaRoot target = VectorSchemaRoot.create(new Schema(fields), allocator);
    try {
      int rows = source.getRowCount();
      for (int c = 0; c < fields.size(); c++) {
        FieldVector from = source.getVector(c);
        FieldVector to = target.getVector(c);
        if (from instanceof ViewVarCharVector view) {
          VarCharVector utf8 = (VarCharVector) to;
          utf8.allocateNew();
          for (int row = 0; row < rows; row++) {
            if (view.isNull(row)) utf8.setNull(row);
            else utf8.setSafe(row, view.get(row));
          }
          utf8.setValueCount(rows);
        } else {
          from.makeTransferPair(to).splitAndTransfer(0, rows);
        }
      }
      target.setRowCount(rows);
      return target;
    } catch (Throwable failure) {
      target.close();
      throw failure;
    }
  }
}
