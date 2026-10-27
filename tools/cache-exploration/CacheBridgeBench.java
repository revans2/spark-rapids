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
import java.nio.charset.StandardCharsets;
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
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
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

  /** Canonical Arrow-layout estimate, independent of allocator padding and device representation. */
  static long logicalBytes(String fixture, int rows) {
    long validity = (rows + 7L) / 8;
    long size = rows * 12L + 2 * validity;
    if (!fixture.equals("fixed")) {
      size += validity + 4L * (rows + 1);
      for (int i = 0; i < rows; i++) {
        String text = value(fixture, i);
        if (text != null) size += text.getBytes(StandardCharsets.UTF_8).length;
      }
    }
    return size;
  }

  static Schema hostSchema(String fixture) {
    Field id = Field.nullable("id", new ArrowType.Int(64, true));
    Field key = Field.nullable("key", new ArrowType.Int(32, true));
    return new Schema(fixture.equals("fixed") ? List.of(id, key)
        : List.of(id, key, Field.nullable("payload", new ArrowType.Utf8())));
  }

  static void populateHost(VectorSchemaRoot root, String fixture, int rows) {
    root.allocateNew();
    BigIntVector id = (BigIntVector) root.getVector(0);
    IntVector key = (IntVector) root.getVector(1);
    VarCharVector payload = fixture.equals("fixed") ? null : (VarCharVector) root.getVector(2);
    for (int i = 0; i < rows; i++) {
      id.setSafe(i, i * 17L - 991);
      if (i % 13 == 0) key.setNull(i);
      else key.setSafe(i, (i * 37) % 1009);
      if (payload != null) {
        String text = value(fixture, i);
        if (text == null) payload.setNull(i);
        else payload.setSafe(i, text.getBytes(StandardCharsets.UTF_8));
      }
    }
    root.setRowCount(rows);
  }

  static void validateHost(VectorSchemaRoot root, String fixture, int firstRow) {
    if (root.getFieldVectors().size() != (fixture.equals("fixed") ? 2 : 3)) {
      throw new AssertionError("host column count");
    }
    BigIntVector id = (BigIntVector) root.getVector(0);
    IntVector key = (IntVector) root.getVector(1);
    for (int i = 0; i < root.getRowCount(); i++) {
      int row = firstRow + i;
      if (id.isNull(i) || id.get(i) != row * 17L - 991) {
        throw new AssertionError("host id/order at " + row);
      }
      if (key.isNull(i) != (row % 13 == 0)
          || (!key.isNull(i) && key.get(i) != (row * 37) % 1009)) {
        throw new AssertionError("host key/null at " + row);
      }
      if (!fixture.equals("fixed")) {
        Object actual = root.getVector(2).getObject(i);
        if (!Objects.equals(value(fixture, row), actual == null ? null : actual.toString())) {
          throw new AssertionError("host payload at " + row);
        }
      }
    }
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

  static void writeVortexBatch(VortexWriter writer, VectorSchemaRoot root,
      BufferAllocator allocator) throws Exception {
    try (ArrowArray array = ArrowArray.allocateNew(allocator);
         ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      try {
        Data.exportVectorSchemaRoot(allocator, root, null, array, schema);
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

  static CappedBytes encodeVortexHost(VectorSchemaRoot input, Session session,
      BufferAllocator allocator, int cap, Sample sample) throws Exception {
    long start = System.nanoTime();
    CappedBytes encoded = new CappedBytes(cap);
    try (VortexWriter writer = VortexWriter.builder(session, encoded,
        input.getSchema(), allocator).build()) {
      writeVortexBatch(writer, input, allocator);
      writer.finish();
    }
    sample.encodeNs = System.nanoTime() - start;
    sample.encodedBytes = encoded.size;
    return encoded;
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
        writeVortexBatch(writer, reader.getVectorSchemaRoot(), allocator);
      }
      writer.finish();
    }
    sample.encodeNs = System.nanoTime() - start;
    sample.encodedBytes = encoded.size;
    return encoded;
  }

  static void decodeVortex(CappedBytes encoded, String fixture, int rows, Session session,
      BufferAllocator allocator, int cap, boolean gpuOutput, Sample sample) throws Exception {
    long start = System.nanoTime();
    DataSource source = DataSource.open(session, readable(encoded));
    Scan scan = source.scan(ScanOptions.builder().ordered(true).build());
    while (scan.hasNext()) {
      try (ArrowReader reader = scan.next().scanArrow(allocator)) {
        while (reader.loadNextBatch()) {
          int batchRows = reader.getVectorSchemaRoot().getRowCount();
          if (!gpuOutput) {
            long validationStart = System.nanoTime();
            validateHost(reader.getVectorSchemaRoot(), fixture, sample.decodedRows);
            sample.validationNs += System.nanoTime() - validationStart;
            sample.decodedRows += batchRows;
            continue;
          }
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
    String mode = args.length > 5 ? args[5] : "gpu-gpu";
    String codec = args.length > 6 ? args[6] : "both";
    boolean crossRead = args.length > 7 && Boolean.parseBoolean(args[7]);
    if (!List.of("fixed", "strings-low", "strings-wide").contains(fixture)
        || !List.of("cpu-cpu", "cpu-gpu", "gpu-cpu", "gpu-gpu").contains(mode)
        || !List.of("vortex", "parquet-proxy", "both").contains(codec)
        || rows < 1 || rows > 2_000_000 || warmups < 0 || warmups > 20
        || repeats < 1 || repeats > 20 || cap < 65536 || cap > 512 * 1024 * 1024) {
      throw new IllegalArgumentException("fixture rows warmups repeats cap mode codec: invalid bounds");
    }
    if (!codec.equals("vortex") && !mode.equals("gpu-gpu")) {
      throw new IllegalArgumentException("Parquet proxy supports gpu-gpu only");
    }
    if (crossRead && mode.equals("cpu-cpu")) {
      throw new IllegalArgumentException("cpu-cpu must not initialize the GPU; use cpu-gpu to cross-check");
    }
    boolean gpuInput = mode.startsWith("gpu");
    boolean gpuOutput = mode.endsWith("gpu");
    long logicalBytes = logicalBytes(fixture, rows);
    if (logicalBytes > cap) throw new IllegalArgumentException("logical input estimate exceeds unit cap");
    if (gpuInput || gpuOutput) Cuda.deviceSynchronize();
    Session session = codec.equals("parquet-proxy") ? null : Session.create();
    try (BufferAllocator allocator = new RootAllocator(3L * cap);
         VectorSchemaRoot host = gpuInput ? null
             : VectorSchemaRoot.create(hostSchema(fixture), allocator);
         Table input = gpuInput ? fixture(fixture, rows) : null) {
      if (host != null) populateHost(host, fixture, rows);
      if (gpuInput) Cuda.deviceSynchronize();
      long gpuBytes = input == null ? 0 : input.getDeviceMemorySize();
      if (gpuBytes > cap) throw new IllegalArgumentException("GPU input exceeds unit cap");
      long fixtureAllocation = allocator.getAllocatedMemory();
      System.out.println("codec,mode,fixture,iteration,rows,logical_bytes,input_gpu_bytes,encoded_bytes,"
          + "encoded_bytes_per_row,logical_to_encoded,export_ns,encode_ns,decode_ns,"
          + "gpu_import_ns,roundtrip_ns,cross_read_checked");
      int codecCount = codec.equals("both") ? 2 : 1;
      for (int i = -warmups; i < repeats; i++) {
        // Alternate codec order when both are selected; individual processes can isolate codecs.
        for (int j = 0; j < codecCount; j++) {
          boolean vortex = codec.equals("vortex")
              || (codec.equals("both") && (j + Math.floorMod(i, 2)) % 2 == 0);
          Sample sample = new Sample();
          if (vortex) {
            CappedBytes encoded = gpuInput
                ? encodeVortex(input, session, allocator, cap, sample)
                : encodeVortexHost(host, session, allocator, cap, sample);
            decodeVortex(encoded, fixture, rows, session, allocator, cap, gpuOutput, sample);
            if (crossRead) {
              // Compatibility work is outside the selected mode's timing; disable it for memory runs.
              decodeVortex(encoded, fixture, rows, session, allocator, cap, !gpuOutput, new Sample());
            }
          } else {
            parquet(input, fixture, cap, sample);
          }
          if (allocator.getAllocatedMemory() != fixtureAllocation) {
            throw new AssertionError("Arrow allocation remains live after codec unit: "
                + (allocator.getAllocatedMemory() - fixtureAllocation));
          }
          if (i >= 0) {
            long roundtrip = sample.exportNs + sample.encodeNs + sample.decodeNs;
            System.out.printf(java.util.Locale.ROOT,
                "%s,%s,%s,%d,%d,%d,%d,%d,%.6f,%.6f,%d,%d,%d,%d,%d,%s%n",
                vortex ? "vortex" : "cudf-parquet-proxy", mode, fixture, i, rows,
                logicalBytes, gpuBytes, sample.encodedBytes, (double) sample.encodedBytes / rows,
                (double) logicalBytes / sample.encodedBytes, sample.exportNs, sample.encodeNs,
                sample.decodeNs, sample.importNs, roundtrip, crossRead && vortex);
          }
        }
      }
    }
    // Closing the root allocator checks zero after the reusable host fixture is also closed.
  }
}
