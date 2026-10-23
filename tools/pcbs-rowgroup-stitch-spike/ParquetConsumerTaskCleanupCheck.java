/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations.
 */
package org.apache.spark;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.CompressionType;
import ai.rapids.cudf.HostBufferConsumer;
import ai.rapids.cudf.HostMemoryBuffer;
import ai.rapids.cudf.ParquetTableWriter;
import ai.rapids.cudf.ParquetWriterOptions;
import ai.rapids.cudf.Rmm;
import ai.rapids.cudf.RmmAllocationMode;
import ai.rapids.cudf.Table;
import com.nvidia.spark.rapids.ShimLoader;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Option;

/** Uses the real generated consumer and stock Spark completion listener implementation. */
public final class ParquetConsumerTaskCleanupCheck {
  private static void check(boolean condition, String message) {
    if (!condition) throw new AssertionError(message);
  }

  public static void main(String[] arguments) throws Exception {
    check(arguments.length == 1, "expected exact generated plugin path");
    final int rows = 262144;
    ClassLoader loader = ShimLoader.getShimClassLoader();
    Class<?> consumerClass = loader.loadClass(
        "com.nvidia.spark.rapids.parquet.ParquetBufferConsumer");
    check(consumerClass.getClassLoader() == loader, "must use actual shim consumer");
    String origin = consumerClass.getResource("ParquetBufferConsumer.class").toString();
    check(origin.contains(arguments[0]), "wrong consumer origin: " + origin);
    StructType schema = new StructType().add("id", DataTypes.LongType, false);
    Constructor<?> constructor = consumerClass.getDeclaredConstructor(int.class, StructType.class);
    constructor.setAccessible(true);
    Method records = consumerClass.getDeclaredMethod("records");
    records.setAccessible(true);
    TaskContextImpl context = TaskContext$.MODULE$.empty();
    check(TaskContext.get() == null, "requires fresh thread context");
    Rmm.initialize(RmmAllocationMode.POOL, null, 15L << 30);
    long baseline = Rmm.getTotalBytesAllocated();
    TaskContext$.MODULE$.setTaskContext(context);
    List<HostMemoryBuffer> observed = new ArrayList<>();
    try {
      Object instance = constructor.newInstance(rows, schema);
      try (AutoCloseable consumer = (AutoCloseable) instance) {
        HostBufferConsumer tracked = (buffer, length) -> {
          // Retain only a Java observer; ownership goes to the actual generated consumer.
          observed.add(buffer);
          ((HostBufferConsumer) instance).handleBuffer(buffer, length);
        };
        long[] values = new long[rows];
        for (int row = 0; row < rows; ++row) values[row] = row;
        try (ColumnVector ids = ColumnVector.fromLongs(values);
             Table table = new Table(ids);
             ParquetTableWriter writer = Table.writeParquetChunked(
                 ParquetWriterOptions.builder().withNonNullableColumns("id")
                     .withCompressionType(CompressionType.NONE)
                     .withRowGroupSizeRows(65536).withRowGroupSizeBytes(8L << 20).build(),
                 tracked)) {
          writer.write(table);
        }
        check(!observed.isEmpty(), "writer callbacks must be observed");
        for (HostMemoryBuffer buffer : observed) {
          check(buffer.getRefCount() > 0, "writer storage must still be owned before consumption");
        }
        scala.collection.Iterator<?> pieces =
            (scala.collection.Iterator<?>) records.invoke(instance);
        check(pieces.hasNext(), "expected first row group");
        Object first = pieces.next();
        check(pieces.hasNext(), "must leave at least one unconsumed row group");
        Method bufferMethod = first.getClass().getMethod("buffer");
        bufferMethod.setAccessible(true);
        byte[] copiedRecord = (byte[]) bufferMethod.invoke(first);
        check(copiedRecord.length > 12, "standalone first record must be valid");
        for (HostMemoryBuffer buffer : observed) {
          check(buffer.getRefCount() > 0, "partial consumer must retain writer storage");
        }
        check(!context.isCompleted(), "completion must be the cleanup trigger");
        context.markTaskCompleted(Option.<Throwable>empty());
        check(context.isCompleted(), "stock Spark task completion");
        for (HostMemoryBuffer buffer : observed) {
          check(buffer.getRefCount() == 0, "task completion must release each callback buffer");
        }
        // This closes a second time after the registered completion listener.
        consumer.close();
        for (HostMemoryBuffer buffer : observed) {
          check(buffer.getRefCount() == 0, "second close must be idempotent");
        }
        check(copiedRecord[0] == 'P' && copiedRecord[1] == 'A' &&
            copiedRecord[2] == 'R' && copiedRecord[3] == '1',
            "copied record must survive writer storage cleanup");
        check(Rmm.getTotalBytesAllocated() == baseline, "device allocations must return to baseline");
        String escapedOrigin = origin.replace("\\", "\\\\").replace("\"", "\\\"");
        System.out.println("OWNERSHIP_GPU_CHECK {\"success\":true,\"consumed_records\":1," +
            "\"more_records_remained\":true,\"callback_buffers\":" + observed.size() +
            ",\"task_completed\":true,\"callback_refcounts_zero\":true," +
            "\"second_close_idempotent\":true,\"encoded_in_memory\":true," +
            "\"rmm_baseline_bytes\":" + baseline + ",\"consumer_origin\":\"" +
            escapedOrigin + "\"}");
      }
    } finally {
      if (!context.isCompleted()) context.markTaskCompleted(Option.<Throwable>empty());
      TaskContext$.MODULE$.unset();
      Rmm.shutdown();
    }
  }
}
