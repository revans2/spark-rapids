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

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.Cuda;
import ai.rapids.cudf.HostColumnVector;
import ai.rapids.cudf.ParquetOptions;
import ai.rapids.cudf.Rmm;
import ai.rapids.cudf.RmmAllocationMode;
import ai.rapids.cudf.RmmEventHandler;
import ai.rapids.cudf.Table;
import com.nvidia.spark.rapids.GpuColumnVector;
import com.nvidia.spark.rapids.RapidsConf;
import com.nvidia.spark.rapids.RetryableTableProducer;
import com.nvidia.spark.rapids.jni.RmmSpark;
import com.nvidia.spark.rapids.parquet.ParquetCachedBatch;
import com.nvidia.spark.rapids.parquet.ParquetCachedBatchReader;
import com.nvidia.spark.rapids.parquet.ParquetCachedBatchSerializer;
import com.nvidia.spark.rapids.spill.SpillFramework;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.columnar.CachedBatch;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import scala.collection.JavaConverters;

/** Actual branch PCBS GPU encode/framing/decode, without Spark storage or query execution. */
public final class PcbsCodecBench {
  // These fixtures and their logical-size definition match CacheBridgeBench. Kept independent
  // so this JVM can use Spark's dependencies without loading Vortex or Arrow Java 19.
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

  static long logicalBytes(String fixture, int rows) {
    long valid = (rows + 7L) / 8;
    long bytes = rows * 12L + (fixture.equals("fixed") ? 2 : 3) * valid;
    if (!fixture.equals("fixed")) {
      bytes += 4L * (rows + 1L);
      for (int i = 0; i < rows; i++) {
        String payload = value(fixture, i);
        if (payload != null) bytes += payload.getBytes(StandardCharsets.UTF_8).length;
      }
    }
    return bytes;
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

  static StructType schema(String fixture) {
    StructType result = new StructType().add("id", DataTypes.LongType, true)
        .add("key", DataTypes.IntegerType, true);
    return fixture.equals("fixed") ? result : result.add("payload", DataTypes.StringType, true);
  }

  static final class RuntimeState implements AutoCloseable {
    private final boolean ownsRmm;
    private boolean spillInitialized;
    private boolean handlerInstalled;

    RuntimeState() {
      ownsRmm = !Rmm.isInitialized();
      try {
        if (ownsRmm) Rmm.initialize(RmmAllocationMode.CUDA_DEFAULT, null, 512L * 1024 * 1024);
        SpillFramework.initialize(new RapidsConf(new SparkConf(false)));
        spillInitialized = true;
        RmmSpark.setEventHandler(new RmmEventHandler() {
          @Override public long[] getAllocThresholds() { return null; }
          @Override public long[] getDeallocThresholds() { return null; }
          @Override public void onAllocThreshold(long totalAllocSize) {}
          @Override public void onDeallocThreshold(long totalAllocSize) {}
          @Override public boolean onAllocFailure(long sizeRequested, int retryCount) { return false; }
        });
        handlerInstalled = true;
        RmmSpark.currentThreadIsDedicatedToTask(1);
      } catch (Throwable failure) {
        try {
          close();
        } catch (Throwable cleanup) {
          failure.addSuppressed(cleanup);
        }
        throw failure;
      }
    }

    @Override public void close() {
      try {
        if (spillInitialized) SpillFramework.shutdown();
      } finally {
        try {
          if (handlerInstalled) {
            RmmSpark.removeAllCurrentThreadAssociation();
            RmmSpark.clearEventHandler();
          }
        } finally {
          if (ownsRmm) Rmm.shutdown();
        }
      }
    }
  }

  static final class Sample {
    long encodeNs;
    long decodeNs;
    long encodedBytes;
    long maxRecordBytes;
    int recordCount;
    int decodedRows;
  }

  static Sample roundTrip(Table input, String fixture, StructType schema,
      long gpuTarget, long recordTarget, long readerMemoryTarget) throws Exception {
    DataType[] types = new DataType[schema.size()];
    for (int i = 0; i < types.length; i++) types[i] = schema.fields()[i].dataType();
    ParquetCachedBatchSerializer serializer = new ParquetCachedBatchSerializer();
    List<CachedBatch> records = new ArrayList<>();
    Sample sample = new Sample();
    long inputBudget = Math.min(gpuTarget - gpuTarget / 200,
        2L * 1024 * 1024 * 1024 - 10L * 1024 * 1024);
    long start = System.nanoTime();
    try (ColumnarBatch batch = GpuColumnVector.from(input, types)) {
      scala.collection.Iterator<ParquetCachedBatch> encoded =
          serializer.compressColumnarBatchWithParquet(batch, schema, schema, inputBudget,
              true, recordTarget);
      // PCBS returns an iterator that owns spillable output until drained or explicitly closed.
      try (AutoCloseable owned = (AutoCloseable) encoded) {
        while (encoded.hasNext()) {
          ParquetCachedBatch record = encoded.next();
          sample.encodedBytes += record.sizeInBytes();
          sample.maxRecordBytes = Math.max(sample.maxRecordBytes, record.sizeInBytes());
          sample.recordCount++;
          records.add(record);
        }
      }
    }
    Cuda.deviceSynchronize();
    sample.encodeNs = System.nanoTime() - start;

    long validationNs = 0;
    start = System.nanoTime();
    try (ParquetCachedBatchReader reader = new ParquetCachedBatchReader(
        JavaConverters.asScalaIteratorConverter(records.iterator()).asScala(), schema,
        ParquetOptions.DEFAULT, gpuTarget, readerMemoryTarget)) {
      while (reader.hasNext()) {
        try (ParquetCachedBatchReader.ReadBatch batch = reader.next()) {
          if (batch.producer().isEmpty()) throw new AssertionError("unexpected row-only batch");
          RetryableTableProducer producer = batch.producer().get();
          int bundleRows = 0;
          while (producer.hasNext()) {
            try (Table decoded = producer.next()) {
              Cuda.deviceSynchronize();
              long validationStart = System.nanoTime();
              validate(decoded, fixture, sample.decodedRows);
              validationNs += System.nanoTime() - validationStart;
              bundleRows += Math.toIntExact(decoded.getRowCount());
              sample.decodedRows += Math.toIntExact(decoded.getRowCount());
            }
          }
          if (bundleRows != batch.numRows()) throw new AssertionError("bundle row count");
        }
      }
    }
    Cuda.deviceSynchronize();
    sample.decodeNs = System.nanoTime() - start - validationNs;
    if (sample.decodedRows != input.getRowCount()) throw new AssertionError("PCBS row count");
    if (SpillFramework.stores().hostStore().numHandles() != 0) {
      throw new AssertionError("PCBS left spillable host handles live");
    }
    return sample;
  }

  public static void main(String[] args) throws Exception {
    String fixture = args.length > 0 ? args[0] : "fixed";
    int rows = args.length > 1 ? Integer.parseInt(args[1]) : 65536;
    int warmups = args.length > 2 ? Integer.parseInt(args[2]) : 2;
    int repeats = args.length > 3 ? Integer.parseInt(args[3]) : 5;
    long recordTarget = args.length > 4 ? Long.parseLong(args[4]) : 8L * 1024 * 1024;
    long gpuTarget = args.length > 5 ? Long.parseLong(args[5]) : 1024L * 1024 * 1024;
    RapidsConf conf = new RapidsConf(new SparkConf(false));
    long readerMemoryTarget = conf.limitChunkedReaderMemoryUsage()
        ? (long) (conf.chunkedReaderMemoryUsageRatio() * gpuTarget) : 0;
    if (!List.of("fixed", "strings-low", "strings-wide").contains(fixture)
        || rows < 1 || rows > 2_000_000 || warmups < 0 || warmups > 20
        || repeats < 1 || repeats > 20 || recordTarget < 1024 || recordTarget > 512L * 1024 * 1024
        || gpuTarget < 1024 || gpuTarget > 2L * 1024 * 1024 * 1024) {
      throw new IllegalArgumentException("fixture rows warmups repeats recordTarget gpuTarget: bounds");
    }
    StructType schema = schema(fixture);
    long logicalBytes = logicalBytes(fixture, rows);
    try (RuntimeState runtime = new RuntimeState(); Table input = fixture(fixture, rows)) {
      Cuda.deviceSynchronize();
      long gpuBytes = input.getDeviceMemorySize();
      System.out.println("codec,fixture,iteration,parent_rows,logical_input_bytes,input_gpu_bytes,"
          + "gpu_target_bytes,record_target_bytes,reader_memory_target_bytes,encoded_bytes,"
          + "record_count,max_record_bytes,"
          + "encode_ns,decode_ns,roundtrip_ns");
      for (int i = -warmups; i < repeats; i++) {
        Sample sample = roundTrip(input, fixture, schema, gpuTarget, recordTarget,
            readerMemoryTarget);
        if (i >= 0) {
          System.out.printf("branch-pcbs-gpu,%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d%n",
              fixture, i, rows, logicalBytes, gpuBytes, gpuTarget, recordTarget, readerMemoryTarget,
              sample.encodedBytes, sample.recordCount, sample.maxRecordBytes,
              sample.encodeNs, sample.decodeNs, sample.encodeNs + sample.decodeNs);
        }
      }
    }
  }
}
