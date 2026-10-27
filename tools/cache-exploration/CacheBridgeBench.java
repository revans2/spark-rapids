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

import ai.rapids.cudf.ArrowIPCOptions;
import ai.rapids.cudf.ArrowIPCWriterOptions;
import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.CompressionType;
import ai.rapids.cudf.Cuda;
import ai.rapids.cudf.HostBufferConsumer;
import ai.rapids.cudf.HostBufferProvider;
import ai.rapids.cudf.HostColumnVector;
import ai.rapids.cudf.HostMemoryBuffer;
import ai.rapids.cudf.ParquetOptions;
import ai.rapids.cudf.ParquetWriterOptions;
import ai.rapids.cudf.StreamedTableReader;
import ai.rapids.cudf.Table;
import ai.rapids.cudf.TableWriter;
import dev.vortex.api.DataSource;
import dev.vortex.api.Scan;
import dev.vortex.api.ScanOptions;
import dev.vortex.api.Session;
import dev.vortex.api.VortexWriter;
import dev.vortex.io.NativeReadable;
import dev.vortex.io.NativeWritable;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;

/** Isolated codec experiment. The Parquet leg is a cuDF codec proxy, not Spark PCBS. */
public final class CacheBridgeBench {
  private static final AtomicLong SOURCE_IDS = new AtomicLong();
  private static final String[] NAMES = {"id", "key", "payload"};

  /** Bounded growable sink; no output, IPC or encoded unit can exceed its explicit cap. */
  static final class CappedBytes extends OutputStream implements NativeWritable {
    private byte[] bytes = new byte[65536];
    private int size;
    private final int cap;

    CappedBytes(int cap) {
      if (cap < bytes.length) throw new IllegalArgumentException("cap must be >= 65536");
      this.cap = cap;
    }

    @Override
    public void write(int value) {
      reserve(1);
      bytes[size++] = (byte) value;
    }

    @Override
    public void write(byte[] source, int offset, int length) {
      Objects.checkFromIndexSize(offset, length, source.length);
      reserve(length);
      System.arraycopy(source, offset, bytes, size, length);
      size += length;
    }

    private void reserve(int length) {
      long wanted = (long) size + length;
      if (wanted > cap) {
        throw new IllegalStateException("unit exceeded byte cap " + cap + ": " + wanted);
      }
      if (wanted > bytes.length) {
        int grown = (int) Math.min(cap, Math.max(wanted, 2L * bytes.length));
        bytes = Arrays.copyOf(bytes, grown);
      }
    }

    ByteArrayInputStream input() {
      return new ByteArrayInputStream(bytes, 0, size);
    }

    @Override public void flush() {}
    @Override public void close() {}
  }

  static HostBufferConsumer consumer(CappedBytes sink) {
    return (buffer, length) -> {
      try (HostMemoryBuffer owned = buffer) {
        byte[] copy = new byte[128 * 1024];
        long offset = 0;
        while (offset < length) {
          int n = (int) Math.min(copy.length, length - offset);
          owned.getBytes(copy, 0, offset, n);
          sink.write(copy, 0, n);
          offset += n;
        }
      }
    };
  }

  static HostBufferProvider provider(CappedBytes source) {
    return new HostBufferProvider() {
      int offset;
      @Override public long readInto(HostMemoryBuffer buffer, long length) {
        int n = (int) Math.min(length, source.size - offset);
        if (n > 0) {
          buffer.setBytes(0, source.bytes, offset, n);
          offset += n;
        }
        return n;
      }
    };
  }

  static NativeReadable readable(CappedBytes source) {
    String name = "memory-" + SOURCE_IDS.incrementAndGet() + ".vortex";
    return new NativeReadable() {
      @Override public String name() { return name; }
      @Override public long length() { return source.size; }
      @Override public void readFully(long position, ByteBuffer target) throws EOFException {
        int n = target.remaining();
        if (position < 0 || position + n > source.size) throw new EOFException(name);
        target.put(source.bytes, (int) position, n);
      }
      @Override public void close() {}
    };
  }

  static String value(String fixture, int row) {
    if (row % 17 == 0) return null;
    if (fixture.equals("strings-low")) return "group-" + (row % 32);
    long mixed = (row * 0x9E3779B97F4A7C15L) ^ 0xBADC0FFEE0DDF00DL;
    return Long.toUnsignedString(mixed, 16) + "-value-" + row;
  }

  static Table fixture(String fixture, int rows) {
    long[] ids = new long[rows];
    Integer[] keys = new Integer[rows];
    for (int i = 0; i < rows; i++) {
      ids[i] = i * 17L - 991;
      keys[i] = i % 13 == 0 ? null : (i * 37) % 1009;
    }
    try (ColumnVector id = ColumnVector.fromLongs(ids);
         ColumnVector key = ColumnVector.fromBoxedInts(keys)) {
      if (fixture.equals("fixed")) return new Table(id, key);
      String[] strings = new String[rows];
      for (int i = 0; i < rows; i++) strings[i] = value(fixture, i);
      try (ColumnVector payload = ColumnVector.fromStrings(strings)) {
        return new Table(id, key, payload);
      }
    }
  }

  static void validate(Table table, String fixture, int firstRow) {
    if (table.getNumberOfColumns() != (fixture.equals("fixed") ? 2 : 3)) {
      throw new AssertionError("column count");
    }
    try (HostColumnVector id = table.getColumn(0).copyToHost();
         HostColumnVector key = table.getColumn(1).copyToHost()) {
      for (int i = 0; i < table.getRowCount(); i++) {
        int row = firstRow + i;
        if (id.isNull(i) || id.getLong(i) != row * 17L - 991) {
          throw new AssertionError("id/order at " + row);
        }
        if (key.isNull(i) != (row % 13 == 0)
            || (!key.isNull(i) && key.getInt(i) != (row * 37) % 1009)) {
          throw new AssertionError("key/null at " + row);
        }
      }
    }
    if (!fixture.equals("fixed")) {
      try (HostColumnVector payload = table.getColumn(2).copyToHost()) {
        for (int i = 0; i < table.getRowCount(); i++) {
          String expected = value(fixture, firstRow + i);
          String actual = payload.isNull(i) ? null : payload.getJavaString(i);
          if (!Objects.equals(expected, actual)) {
            throw new AssertionError("payload at " + (firstRow + i));
          }
        }
      }
    }
  }

  static final class Sample {
    long exportNs;
    long encodeNs;
    long decodeNs;
    long importNs;
    long validationNs;
    int encodedBytes;
    int decodedRows;
  }

  static CappedBytes encodeVortex(Table input, Session session, BufferAllocator allocator,
      int cap, Sample sample) throws Exception {
    long start = System.nanoTime();
    CappedBytes ipc = new CappedBytes(cap);
    String[] names = Arrays.copyOf(NAMES, input.getNumberOfColumns());
    ArrowIPCWriterOptions options = ArrowIPCWriterOptions.builder()
        .withColumnNames(names).withMaxChunkSize(input.getRowCount()).build();
    try (TableWriter writer = Table.writeArrowIPCChunked(options, consumer(ipc))) {
      writer.write(input);
    }
    Cuda.deviceSynchronize();
    sample.exportNs = System.nanoTime() - start;
    start = System.nanoTime();
    CappedBytes encoded = new CappedBytes(cap);
    try (ArrowStreamReader reader = new ArrowStreamReader(ipc.input(), allocator);
         VortexWriter writer = VortexWriter.builder(session, encoded,
             reader.getVectorSchemaRoot().getSchema(), allocator).build()) {
      while (reader.loadNextBatch()) {
        try (ArrowArray array = ArrowArray.allocateNew(allocator);
             ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
          try {
            Data.exportVectorSchemaRoot(allocator, reader.getVectorSchemaRoot(), null, array, schema);
            writer.writeBatch(array.memoryAddress(), schema.memoryAddress());
          } finally {
            // Native consumes the array but only borrows the schema; close frees the structs.
            try {
              array.release();
            } finally {
              schema.release();
            }
          }
        }
      }
      writer.finish();
    }
    sample.encodeNs = System.nanoTime() - start;
    sample.encodedBytes = encoded.size;
    return encoded;
  }

  static void decodeVortex(CappedBytes encoded, String fixture, int rows, Session session,
      BufferAllocator allocator, int cap, Sample sample) throws Exception {
    long start = System.nanoTime();
    DataSource source = DataSource.open(session, readable(encoded));
    Scan scan = source.scan(ScanOptions.builder().ordered(true).build());
    while (scan.hasNext()) {
      try (ArrowReader reader = scan.next().scanArrow(allocator)) {
        while (reader.loadNextBatch()) {
          CappedBytes ipc = new CappedBytes(cap);
          try (ArrowStreamWriter writer = new ArrowStreamWriter(reader.getVectorSchemaRoot(), null,
              Channels.newChannel(ipc))) {
            writer.start();
            writer.writeBatch();
            writer.end();
          }
          long importStart = System.nanoTime();
          try (StreamedTableReader gpuReader = Table.readArrowIPCChunked(
              ArrowIPCOptions.DEFAULT, provider(ipc))) {
            int batchRows = reader.getVectorSchemaRoot().getRowCount();
            try (Table decoded = gpuReader.getNextIfAvailable(batchRows)) {
              Cuda.deviceSynchronize();
              sample.importNs += System.nanoTime() - importStart;
              if (decoded == null || decoded.getRowCount() != batchRows) {
                throw new AssertionError("GPU IPC row count");
              }
              long validationStart = System.nanoTime();
              validate(decoded, fixture, sample.decodedRows);
              sample.validationNs += System.nanoTime() - validationStart;
              sample.decodedRows += batchRows;
            }
            try (Table extra = gpuReader.getNextIfAvailable()) {
              if (extra != null) throw new AssertionError("unexpected extra IPC batch");
            }
          }
        }
      }
    }
    sample.decodeNs = System.nanoTime() - start - sample.validationNs;
    if (sample.decodedRows != rows) throw new AssertionError("Vortex row count");
  }

  static void parquet(Table input, String fixture, int cap, Sample sample) throws Exception {
    CappedBytes encoded = new CappedBytes(cap);
    ParquetWriterOptions.Builder builder = ParquetWriterOptions.builder()
        .withCompressionType(CompressionType.SNAPPY).withRowGroupSizeBytes(8L * 1024 * 1024)
        .withStatisticsFrequency(ParquetWriterOptions.StatisticsFrequency.ROWGROUP);
    for (int i = 0; i < input.getNumberOfColumns(); i++) builder.withColumns(true, NAMES[i]);
    long start = System.nanoTime();
    try (TableWriter writer = Table.writeParquetChunked(builder.build(), consumer(encoded))) {
      writer.write(input);
    }
    Cuda.deviceSynchronize();
    sample.encodeNs = System.nanoTime() - start;
    sample.encodedBytes = encoded.size;
    start = System.nanoTime();
    try (Table decoded = Table.readParquet(ParquetOptions.DEFAULT, encoded.bytes, 0, encoded.size)) {
      Cuda.deviceSynchronize();
      if (decoded.getRowCount() != input.getRowCount()) throw new AssertionError("Parquet rows");
      long validationStart = System.nanoTime();
      validate(decoded, fixture, 0);
      sample.validationNs += System.nanoTime() - validationStart;
      sample.decodedRows = (int) decoded.getRowCount();
    }
    sample.decodeNs = System.nanoTime() - start - sample.validationNs;
  }

  public static void main(String[] args) throws Exception {
    String fixture = args.length > 0 ? args[0] : "fixed";
    int rows = args.length > 1 ? Integer.parseInt(args[1]) : 65536;
    int warmups = args.length > 2 ? Integer.parseInt(args[2]) : 2;
    int repeats = args.length > 3 ? Integer.parseInt(args[3]) : 5;
    int cap = args.length > 4 ? Integer.parseInt(args[4]) : 128 * 1024 * 1024;
    if (!List.of("fixed", "strings-low", "strings-wide").contains(fixture)
        || rows < 1 || rows > 2_000_000 || warmups < 0 || warmups > 20
        || repeats < 1 || repeats > 20 || cap < 65536 || cap > 512 * 1024 * 1024) {
      throw new IllegalArgumentException("fixture rows warmups repeats cap: invalid bounds");
    }
    Session session = Session.create();
    try (BufferAllocator allocator = new RootAllocator(2L * cap);
         Table input = fixture(fixture, rows)) {
      Cuda.deviceSynchronize();
      long gpuBytes = input.getDeviceMemorySize();
      if (gpuBytes > cap) throw new IllegalArgumentException("GPU input exceeds unit cap");
      System.out.println("codec,fixture,iteration,rows,input_gpu_bytes,encoded_bytes,"
          + "export_ns,encode_ns,decode_ns,gpu_import_ns,roundtrip_ns");
      for (int i = -warmups; i < repeats; i++) {
        // Alternate codec order to reduce systematic order bias.
        for (int j = 0; j < 2; j++) {
          boolean vortex = (j + Math.floorMod(i, 2)) % 2 == 0;
          Sample sample = new Sample();
          if (vortex) {
            CappedBytes encoded = encodeVortex(input, session, allocator, cap, sample);
            decodeVortex(encoded, fixture, rows, session, allocator, cap, sample);
          } else {
            parquet(input, fixture, cap, sample);
          }
          if (i >= 0) {
            long roundtrip = sample.exportNs + sample.encodeNs + sample.decodeNs;
            System.out.printf("%s,%s,%d,%d,%d,%d,%d,%d,%d,%d,%d%n",
                vortex ? "vortex-ipc-bridge" : "cudf-parquet-proxy", fixture, i, rows,
                gpuBytes, sample.encodedBytes, sample.exportNs, sample.encodeNs,
                sample.decodeNs, sample.importNs, roundtrip);
          }
        }
      }
      if (allocator.getAllocatedMemory() != 0) {
        throw new AssertionError("Arrow Java allocation remains live after iterations: "
            + allocator.getAllocatedMemory());
      }
    }
  }
}
