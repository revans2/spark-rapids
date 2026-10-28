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

package cacheexploration.nvcomp;

import ai.rapids.cudf.BaseDeviceMemoryBuffer;
import ai.rapids.cudf.CloseableArray;
import ai.rapids.cudf.ContiguousTable;
import ai.rapids.cudf.Cuda;
import ai.rapids.cudf.DefaultHostMemoryAllocator;
import ai.rapids.cudf.DeviceMemoryBuffer;
import ai.rapids.cudf.HostMemoryBuffer;
import ai.rapids.cudf.PinnedMemoryPool;
import ai.rapids.cudf.Rmm;
import ai.rapids.cudf.RmmAllocationMode;
import ai.rapids.cudf.RmmPoolMemoryResource;
import ai.rapids.cudf.RmmTrackingResourceAdaptor;
import ai.rapids.cudf.Table;
import ai.rapids.cudf.nvcomp.BatchedCompressor;
import ai.rapids.cudf.nvcomp.BatchedDecompressor;
import ai.rapids.cudf.nvcomp.BatchedLZ4Compressor;
import ai.rapids.cudf.nvcomp.BatchedLZ4Decompressor;
import ai.rapids.cudf.nvcomp.BatchedZstdCompressor;
import ai.rapids.cudf.nvcomp.BatchedZstdDecompressor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nvidia.spark.rapids.HostAlloc;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.rapids.GpuTaskMetrics;

/**
 * Standalone GPU packed-codec experiment, not a Spark cache serializer.
 * Four compressed host records survive the encode barrier; only two work items execute at once.
 * Timers include allocation and stream completion but exclude fixture generation, correctness
 * validation and final resource cleanup. Whole-job peaks include all of those operations.
 */
public final class PackedCodecBench {
  private static final long GIB = 1024L * 1024 * 1024;
  private static final long PINNED_BYTES = 2 * GIB;
  private static final long NON_PINNED_BYTES = 30 * GIB;
  private static final long DEVICE_POOL_BYTES = 24 * GIB;
  private static final long INTERMEDIATE_BYTES = 64L * 1024 * 1024;
  private static final int HEADER_BYTES = 56;
  private static final int RETAINED_DESCRIPTOR_BYTES = PackedSegments.DESCRIPTOR_BYTES + 8;
  private static final int MAGIC = 0x50434F44;
  private static final int WORK_ITEMS = 4;
  private static final int SLOTS = 2;

  private PackedCodecBench() {}

  private static Map<String, Object> map() { return new LinkedHashMap<>(); }

  private static int codecId(String codec) {
    switch (codec) {
      case "uncompressed": return 0;
      case "lz4": return 1;
      case "zstd": return 2;
      case "cascaded8": return 3;
      case "cascaded-typed": return 4;
      default: throw new IllegalArgumentException("Codec must be uncompressed/lz4/zstd/cascaded8/cascaded-typed");
    }
  }

  private static BatchedCompressor compressor(String codec, long chunk) {
    switch (codec) {
      case "lz4": return new BatchedLZ4Compressor(chunk, INTERMEDIATE_BYTES);
      case "zstd": return new BatchedZstdCompressor(chunk, INTERMEDIATE_BYTES);
      case "cascaded8": return CascadedCodec.compressor(chunk, INTERMEDIATE_BYTES, 8);
      default: return null;
    }
  }

  private static BatchedDecompressor decompressor(String codec, long chunk) {
    switch (codec) {
      case "lz4": return new BatchedLZ4Decompressor(chunk);
      case "zstd": return new BatchedZstdDecompressor(chunk);
      case "cascaded8":
      case "cascaded-typed": return CascadedCodec.decompressor(chunk);
      default: return null;
    }
  }

  /**
   * Initialize once, before workers. Native host budgets exclude Java metadata and any allocations
   * outside HostAlloc; the separate runner must retain its sampled process-tree RSS early stop.
   * No custom event handler is installed and no spill/retry/OOM-resilience claim is made.
   */
  public static Map<String, Object> initialize() {
    if (Rmm.isInitialized() || PinnedMemoryPool.isInitialized()) {
      throw new IllegalStateException("Use a fresh JVM; allocators already initialized");
    }
    if (Runtime.getRuntime().maxMemory() > 16 * GIB) {
      throw new IllegalStateException("JVM heap exceeds authorized 16 GiB");
    }
    Cuda.setDevice(0);
    if (!Rmm.configureDefaultCudfPinnedPoolSize(0)) {
      throw new IllegalStateException("Cannot configure cuDF pinned allocator before first use");
    }
    Rmm.initialize(RmmAllocationMode.POOL, null, DEVICE_POOL_BYTES);
    Object resource = Rmm.getCurrentDeviceResource();
    if (resource instanceof RmmTrackingResourceAdaptor) {
      resource = ((RmmTrackingResourceAdaptor<?>) resource).getWrapped();
    }
    if (!(resource instanceof RmmPoolMemoryResource)
        || ((RmmPoolMemoryResource<?>) resource).getMaxSize() != DEVICE_POOL_BYTES) {
      throw new IllegalStateException("RMM pool maximum was not established at 24 GiB");
    }
    PinnedMemoryPool.initialize(PINNED_BYTES, 0, true);
    if (PinnedMemoryPool.getTotalPoolSizeBytes() != PINNED_BYTES) {
      throw new IllegalStateException("Pinned pool did not initialize at requested size");
    }
    HostAlloc.initialize(NON_PINNED_BYTES);
    if (!(DefaultHostMemoryAllocator.get() instanceof HostAlloc)) {
      throw new IllegalStateException("HostAlloc is not the installed default allocator");
    }
    HostAlloc allocator = (HostAlloc) DefaultHostMemoryAllocator.get();
    if (allocator.getTotalLimit() != PINNED_BYTES + NON_PINNED_BYTES) {
      throw new IllegalStateException("Managed host limit mismatch");
    }
    // Source returns a metrics object even without TaskContext. Fail explicitly if unavailable.
    if (GpuTaskMetrics.get() == null) throw new IllegalStateException("Standalone metrics unavailable");
    GpuTaskMetrics.get().getHostBytesAllocated();
    if (!Cuda.isPtdsEnabled()) {
      throw new IllegalStateException("Two independent GPU task slots require per-thread default streams");
    }
    Map<String, Object> result = map();
    result.put("heap_max_bytes", Runtime.getRuntime().maxMemory());
    result.put("pinned_pool_bytes", PinnedMemoryPool.getTotalPoolSizeBytes());
    result.put("non_pinned_limit_bytes", NON_PINNED_BYTES);
    result.put("managed_host_total_limit_bytes", allocator.getTotalLimit());
    result.put("device_pool_max_bytes", Rmm.getPoolSize());
    result.put("per_thread_default_streams", Cuda.isPtdsEnabled());
    result.put("host_allocator", DefaultHostMemoryAllocator.get().getClass().getName());
    result.put("gpu_task_metrics_outside_spark", true);
    return result;
  }

  /** Owns one host payload per ordered segment, opaque table metadata and explicit framing. */
  public static final class HostRecord implements AutoCloseable {
    public final String fixture;
    public final String codec;
    public final int rows;
    public final long firstRow;
    public final long chunkBytes;
    public final long packedBytes;
    public final byte[] metadata;
    public final byte[] header;
    public final byte[] descriptors;
    public final Map<String, Object> encode;
    private HostMemoryBuffer[] payloads;

    private HostRecord(String fixture, String codec, int rows, long firstRow, long chunkBytes,
        long packedBytes, byte[] metadata, PackedSegments.Segment[] segments,
        HostMemoryBuffer[] payloads, Map<String, Object> encode) {
      this.fixture = fixture;
      this.codec = codec;
      this.rows = rows;
      this.firstRow = firstRow;
      this.chunkBytes = chunkBytes;
      this.packedBytes = packedBytes;
      this.metadata = metadata;
      this.payloads = payloads;
      this.encode = encode;
      ByteBuffer descriptor = ByteBuffer.allocate(
          Math.multiplyExact(segments.length, RETAINED_DESCRIPTOR_BYTES)).order(ByteOrder.LITTLE_ENDIAN);
      for (int i = 0; i < segments.length; i++) {
        PackedSegments.Segment segment = segments[i];
        descriptor.putLong(segment.offset).putLong(segment.length).putInt(segment.typeBits)
            .putInt(0).putLong(payloads[i].getLength());
      }
      descriptors = descriptor.array();
      header = ByteBuffer.allocate(HEADER_BYTES).order(ByteOrder.LITTLE_ENDIAN)
          .putInt(MAGIC).putInt(1).putInt(codecId(codec)).putInt(metadata.length)
          .putLong(chunkBytes).putLong(packedBytes).putLong(payloadBytes()).putLong(rows)
          .putInt(segments.length).putInt(descriptors.length).array();
    }

    private PackedSegments.Segment[] readDescriptors() {
      if (payloads == null) throw new IllegalStateException("Retained record is closed");
      ByteBuffer h = ByteBuffer.wrap(header).order(ByteOrder.LITTLE_ENDIAN);
      if (h.getInt() != MAGIC || h.getInt() != 1 || h.getInt() != codecId(codec)
          || h.getInt() != metadata.length || h.getLong() != chunkBytes
          || h.getLong() != packedBytes || h.getLong() != payloadBytes()
          || h.getLong() != rows || h.getInt() != payloads.length
          || h.getInt() != descriptors.length) throw new IllegalStateException("Retained framing mismatch");
      ByteBuffer d = ByteBuffer.wrap(descriptors).order(ByteOrder.LITTLE_ENDIAN);
      if (descriptors.length != Math.multiplyExact(payloads.length, RETAINED_DESCRIPTOR_BYTES)) {
        throw new IllegalStateException("Retained descriptor size");
      }
      PackedSegments.Segment[] decoded = new PackedSegments.Segment[payloads.length];
      long covered = 0;
      for (int i = 0; i < decoded.length; i++) {
        long offset = d.getLong();
        long length = d.getLong();
        int bits = d.getInt();
        int reserved = d.getInt();
        long compressedLength = d.getLong();
        if (offset != covered || length <= 0 || length > PackedSegments.MAX_SUBMISSION_BYTES
            || offset % 8 != 0
            || (bits != 8 && bits != 32 && bits != 64) || length % (bits / 8) != 0
            || reserved != 0 || compressedLength != payloads[i].getLength()) {
          throw new IllegalStateException("Retained segment descriptor mismatch");
        }
        decoded[i] = new PackedSegments.Segment(offset, length, bits);
        covered = Math.addExact(covered, length);
      }
      if (covered != packedBytes) throw new IllegalStateException("Retained segment coverage");
      return decoded;
    }

    public long payloadBytes() {
      long bytes = 0;
      for (HostMemoryBuffer payload : payloads) bytes = Math.addExact(bytes, payload.getLength());
      return bytes;
    }
    public long retainedBytes() {
      return payloadBytes() + metadata.length + header.length + descriptors.length;
    }

    @Override public synchronized void close() {
      HostMemoryBuffer[] previous = payloads;
      payloads = null;
      if (previous != null) {
        try (CloseableArray<HostMemoryBuffer> owned = CloseableArray.wrap(previous)) {
          /* ownership ends here */
        }
      }
    }
  }

  /** One generated GPU input, owned across the generation-to-encode wave barrier. */
  public static final class PreparedInput implements AutoCloseable {
    public final String fixture;
    public final int rows;
    public final long firstRow;
    public final long generateNs;
    public final long generateStartedNs;
    private Table table;

    private PreparedInput(String fixture, int rows, long firstRow, long generateNs,
        long generateStartedNs, Table table) {
      this.fixture = fixture;
      this.rows = rows;
      this.firstRow = firstRow;
      this.generateNs = generateNs;
      this.generateStartedNs = generateStartedNs;
      this.table = table;
    }

    @Override public synchronized void close() {
      Table previous = table;
      table = null;
      if (previous != null) {
        try (Table owned = previous) { /* release generated input */ }
      }
    }
  }

  /** Timed decode output retained only until its two-item wave completes validation. */
  public static final class DecodedResult implements AutoCloseable {
    public final HostRecord record;
    public final Map<String, Object> timings;
    private Table table;

    private DecodedResult(HostRecord record, Map<String, Object> timings, Table table) {
      this.record = record;
      this.timings = timings;
      this.table = table;
    }

    @Override public synchronized void close() {
      Table previous = table;
      table = null;
      if (previous != null) {
        try (Table owned = previous) { /* release decoded output */ }
      }
    }
  }

  public static PreparedInput prepareWorkItem(String fixture, int rows, long firstRow) {
    Cuda.autoSetDevice();
    long started = System.nanoTime();
    try (CloseableArray<Table> tables = CloseableArray.wrap(new Table[1])) {
      tables.set(0, PackedFixtures.create(fixture, rows, firstRow));
      long elapsed = finish(Cuda.DEFAULT_STREAM, started);
      PreparedInput result = new PreparedInput(fixture, rows, firstRow, elapsed, started, tables.get(0));
      tables.release();
      return result;
    }
  }

  private static long finish(Cuda.Stream stream, long started) {
    stream.sync();
    return System.nanoTime() - started;
  }

  /** One actual native submission: homogeneous integer width and bounded input bytes. */
  private static final class CodecSubmission {
    final int typeBits;
    final int[] indices;
    final long inputBytes;
    final long blocks;
    final long blockBudget;
    final long workspaceInputUpperBoundBytes;
    CodecSubmission(int bits, List<Integer> indices, long bytes, long blocks, long chunk) {
      this.typeBits = bits;
      this.indices = indices.stream().mapToInt(Integer::intValue).toArray();
      this.inputBytes = bytes;
      this.blocks = blocks;
      this.blockBudget = PackedSegments.MAX_SUBMISSION_BYTES / chunk;
      this.workspaceInputUpperBoundBytes = Math.multiplyExact(blocks, chunk);
      if (bytes > PackedSegments.MAX_SUBMISSION_BYTES || blocks > blockBudget
          || workspaceInputUpperBoundBytes > PackedSegments.MAX_SUBMISSION_BYTES) {
        throw new IllegalArgumentException("Submission workspace input bound exceeded");
      }
    }
    Map<String, Object> describe() {
      Map<String, Object> result = map();
      result.put("type_bits", typeBits);
      result.put("segments", indices.length);
      result.put("uncompressed_bytes", inputBytes);
      result.put("blocks", blocks);
      result.put("block_budget", blockBudget);
      result.put("workspace_input_upper_bound_bytes", workspaceInputUpperBoundBytes);
      return result;
    }
  }

  private static List<CodecSubmission> submissions(
      String codec, long chunk, PackedSegments.Segment[] segments) {
    List<CodecSubmission> result = new ArrayList<>();
    if (chunk <= 0 || PackedSegments.MAX_SUBMISSION_BYTES % chunk != 0) {
      throw new IllegalArgumentException("Codec chunk must divide the submission byte bound");
    }
    long blockBudget = PackedSegments.MAX_SUBMISSION_BYTES / chunk;
    if (codec.equals("uncompressed")) return result;
    int[] groups = codec.equals("cascaded-typed") ? new int[] {8, 32, 64} : new int[] {8};
    for (int bits : groups) {
      List<Integer> indices = new ArrayList<>();
      long bytes = 0;
      long blocks = 0;
      for (int i = 0; i < segments.length; i++) {
        PackedSegments.Segment segment = segments[i];
        if (segment.length <= 0 || segment.length > PackedSegments.MAX_SUBMISSION_BYTES) {
          throw new IllegalArgumentException("Segment exceeds uncompressed submission bound");
        }
        if (segment.typeBits != bits) continue;
        long segmentBlocks = (segment.length + chunk - 1) / chunk;
        if (!indices.isEmpty() && (bytes + segment.length > PackedSegments.MAX_SUBMISSION_BYTES
            || blocks + segmentBlocks > blockBudget)) {
          result.add(new CodecSubmission(bits, indices, bytes, blocks, chunk));
          indices = new ArrayList<>();
          bytes = 0;
          blocks = 0;
        }
        indices.add(i);
        bytes += segment.length;
        blocks += segmentBlocks;
      }
      if (!indices.isEmpty()) result.add(new CodecSubmission(bits, indices, bytes, blocks, chunk));
    }
    int planned = result.stream().mapToInt(call -> call.indices.length).sum();
    if (planned != segments.length) throw new IllegalArgumentException("Submission plan coverage");
    return result;
  }

  private static List<Map<String, Object>> describeCalls(List<CodecSubmission> submissions) {
    List<Map<String, Object>> result = new ArrayList<>();
    for (CodecSubmission call : submissions) result.add(call.describe());
    return result;
  }

  /** CPU-only boundary checks: metadata plans do not allocate the synthetic payloads. */
  private static void checkSubmissionPlans() {
    long bound = PackedSegments.MAX_SUBMISSION_BYTES;
    for (long chunk : new long[] {64L * 1024, 256L * 1024}) {
      PackedSegments.Segment[] flat = PackedSegments.bounded(new PackedSegments.Segment[] {
          new PackedSegments.Segment(0, 2 * bound + 8, 8)});
      List<CodecSubmission> calls = submissions("lz4", chunk, flat);
      if (flat.length != 3 || calls.size() != 3
          || calls.get(0).inputBytes != bound || calls.get(0).blocks != bound / chunk
          || calls.get(2).inputBytes != 8 || calls.get(2).blocks != 1) {
        throw new AssertionError("Flat submission boundary/tail plan");
      }
      PackedSegments.Segment[] typed = PackedSegments.bounded(new PackedSegments.Segment[] {
          new PackedSegments.Segment(0, bound + 8, 8),
          new PackedSegments.Segment(bound + 8, bound + 8, 32),
          new PackedSegments.Segment(2 * (bound + 8), bound + 8, 64)});
      calls = submissions("cascaded-typed", chunk, typed);
      if (typed.length != 6 || calls.size() != 6) {
        throw new AssertionError("Typed split/group plan");
      }
      for (int i = 0; i < calls.size(); i++) {
        CodecSubmission call = calls.get(i);
        if (call.typeBits != new int[] {8, 32, 64}[i / 2]
            || call.inputBytes != (i % 2 == 0 ? bound : 8)) {
          throw new AssertionError("Typed submission width/byte bound");
        }
      }
      PackedSegments.Segment[] small = new PackedSegments.Segment[4];
      for (int i = 0; i < small.length; i++) {
        small[i] = new PackedSegments.Segment(i * bound / 4, bound / 4, 64);
      }
      calls = submissions("cascaded-typed", chunk, small);
      if (calls.size() != 1 || calls.get(0).indices.length != 4
          || calls.get(0).inputBytes != bound || calls.get(0).blocks != bound / chunk) {
        throw new AssertionError("Several segments must share one many-block call");
      }
    }
    PackedSegments.Segment[] tiny = new PackedSegments.Segment[1025];
    for (int i = 0; i < tiny.length; i++) {
      tiny[i] = new PackedSegments.Segment(8L * i, 8, 8);
    }
    List<CodecSubmission> rounded = submissions("cascaded-typed", 256L * 1024, tiny);
    if (rounded.size() != 2 || rounded.get(0).indices.length != 1024
        || rounded.get(0).inputBytes != 8192
        || rounded.get(0).workspaceInputUpperBoundBytes != bound
        || rounded.get(1).workspaceInputUpperBoundBytes != 256L * 1024) {
      throw new AssertionError("Tiny segments must obey rounded block workspace budget");
    }
  }

  /** Execute the same bounded submission plan used by the report, preserving descriptor order. */
  private static DeviceMemoryBuffer[] compressSegments(String codec, long chunk,
      DeviceMemoryBuffer input, PackedSegments.Segment[] segments,
      List<CodecSubmission> plan, Cuda.Stream stream) {
    try (CloseableArray<DeviceMemoryBuffer> output =
             CloseableArray.wrap(new DeviceMemoryBuffer[segments.length])) {
      if (codec.equals("uncompressed")) {
        for (int i = 0; i < segments.length; i++) {
          output.set(i, input.slice(segments[i].offset, segments[i].length));
        }
      } else {
        for (CodecSubmission call : plan) {
          BatchedCompressor encoder = codec.equals("cascaded-typed")
              ? CascadedCodec.compressor(chunk, INTERMEDIATE_BYTES, call.typeBits)
              : compressor(codec, chunk);
          try (CloseableArray<BaseDeviceMemoryBuffer> inputs =
                   CloseableArray.wrap(new BaseDeviceMemoryBuffer[call.indices.length])) {
            for (int i = 0; i < call.indices.length; i++) {
              PackedSegments.Segment segment = segments[call.indices[i]];
              inputs.set(i, input.slice(segment.offset, segment.length));
            }
            // The wrapper consumes the sliced input references on success and failure.
            try (CloseableArray<DeviceMemoryBuffer> compressed =
                     CloseableArray.wrap(encoder.compress(inputs.release(), stream))) {
              if (compressed.size() != call.indices.length) {
                throw new IllegalStateException("Codec output count");
              }
              for (int i = 0; i < call.indices.length; i++) {
                compressed.get(i).incRefCount();
                output.set(call.indices[i], compressed.get(i));
              }
            }
          }
        }
      }
      return output.release();
    }
  }

  /** Bounded homogeneous submissions reconstruct slices of one full packed allocation. */
  private static DeviceMemoryBuffer decompressSegments(String codec, long chunk,
      DeviceMemoryBuffer[] compressed, PackedSegments.Segment[] segments,
      List<CodecSubmission> plan, long packedBytes, Cuda.Stream stream) {
    if (compressed.length != segments.length) throw new IllegalArgumentException("Segment input count");
    try (CloseableArray<DeviceMemoryBuffer> output = CloseableArray.wrap(new DeviceMemoryBuffer[1])) {
      output.set(0, DeviceMemoryBuffer.allocate(packedBytes, stream));
      if (codec.equals("uncompressed")) {
        for (int i = 0; i < segments.length; i++) {
          if (compressed[i].getLength() != segments[i].length) {
            throw new IllegalArgumentException("Uncompressed payload shape");
          }
          output.get(0).copyFromDeviceBufferAsync(
              segments[i].offset, compressed[i], 0, segments[i].length, stream);
        }
      } else {
        BatchedDecompressor decoder = decompressor(codec, chunk);
        for (CodecSubmission call : plan) {
          try (CloseableArray<BaseDeviceMemoryBuffer> inputRefs =
                   CloseableArray.wrap(new BaseDeviceMemoryBuffer[call.indices.length]);
               CloseableArray<BaseDeviceMemoryBuffer> slices =
                   CloseableArray.wrap(new BaseDeviceMemoryBuffer[call.indices.length])) {
            for (int i = 0; i < call.indices.length; i++) {
              int index = call.indices[i];
              PackedSegments.Segment segment = segments[index];
              slices.set(i, output.get(0).slice(segment.offset, segment.length));
              compressed[index].incRefCount();
              inputRefs.set(i, compressed[index]);
            }
            // Installed SDK mixed-width failure requires homogeneous batches; each also obeys
            // the uncompressed-byte bound without falling back to per-block launches.
            decoder.decompressAsync(inputRefs.release(), slices.getArray(), stream);
          }
        }
      }
      return output.release()[0];
    }
  }

  private static HostMemoryBuffer[] toHost(DeviceMemoryBuffer[] input, Cuda.Stream stream) {
    try (CloseableArray<HostMemoryBuffer> host =
             CloseableArray.wrap(new HostMemoryBuffer[input.length])) {
      try {
        for (int i = 0; i < input.length; i++) {
          host.set(i, DefaultHostMemoryAllocator.get().allocate(input[i].getLength(), true));
          host.get(i).copyFromDeviceBufferAsync(input[i], stream);
        }
        stream.sync();
        return host.release();
      } catch (Throwable failure) {
        // Keep allocated host buffers alive until any already-enqueued DMA has finished.
        try { stream.sync(); } catch (Throwable sync) { failure.addSuppressed(sync); }
        throw failure;
      }
    }
  }

  private static DeviceMemoryBuffer[] toDevice(HostMemoryBuffer[] input, Cuda.Stream stream) {
    try (CloseableArray<DeviceMemoryBuffer> device =
             CloseableArray.wrap(new DeviceMemoryBuffer[input.length])) {
      try {
        for (int i = 0; i < input.length; i++) {
          device.set(i, DeviceMemoryBuffer.allocate(input[i].getLength(), stream));
          device.get(i).copyFromHostBufferAsync(input[i], stream);
        }
        stream.sync();
        return device.release();
      } catch (Throwable failure) {
        try { stream.sync(); } catch (Throwable sync) { failure.addSuppressed(sync); }
        throw failure;
      }
    }
  }

  public static HostRecord encodeWorkItem(PreparedInput prepared, String codec, long chunkBytes) {
    String fixture = prepared.fixture;
    int rows = prepared.rows;
    long firstRow = prepared.firstRow;
    Table input = prepared.table;
    if (input == null) throw new IllegalStateException("Prepared input is closed");
    codecId(codec);
    if (chunkBytes != 64L * 1024 && chunkBytes != 256L * 1024) {
      throw new IllegalArgumentException("First sweep supports only 64KiB or 256KiB codec chunks");
    }
    Cuda.autoSetDevice();
    Cuda.Stream stream = Cuda.DEFAULT_STREAM;
    {
      long generateNs = prepared.generateNs;
      stream.sync();
      long encodeStart = System.nanoTime();
      long packStart = System.nanoTime();
      try (CloseableArray<ContiguousTable> packed = CloseableArray.wrap(input.contiguousSplit())) {
        if (packed.size() != 1) throw new IllegalStateException("Expected one packed work item");
        ContiguousTable table = packed.get(0);
        ByteBuffer opaque = table.getMetadataDirectBuffer().duplicate();
        byte[] metadata = new byte[opaque.remaining()];
        opaque.get(metadata);
        long packedBytes = table.getBuffer().getLength();
        if (packedBytes <= 0) throw new IllegalArgumentException("Nonempty packed work item required");
        long descriptorStart = System.nanoTime();
        PackedSegments.Segment[] segments = PackedSegments.describe(
            table, codec.equals("cascaded-typed"));
        List<CodecSubmission> plan = submissions(codec, chunkBytes, segments);
        List<Map<String, Object>> calls = describeCalls(plan);
        long descriptorNs = System.nanoTime() - descriptorStart;
        long packNs = finish(stream, packStart);
        long compressStart = System.nanoTime();
        try (CloseableArray<DeviceMemoryBuffer> encoded = CloseableArray.wrap(
                 compressSegments(codec, chunkBytes, table.getBuffer(), segments, plan, stream))) {
          long compressionNs = finish(stream, compressStart);
          long d2hStart = System.nanoTime();
          try (CloseableArray<HostMemoryBuffer> retained =
                   CloseableArray.wrap(toHost(encoded.getArray(), stream))) {
            long d2hNs = System.nanoTime() - d2hStart;
            Map<String, Object> result = map();
            HostRecord record = new HostRecord(fixture, codec, rows, firstRow, chunkBytes,
                packedBytes, metadata, segments, retained.getArray(), result);
            long totalNs = finish(stream, encodeStart);
            long blocks = 0;
            List<Object> blockCounts = new ArrayList<>();
            List<Object> groupBits = new ArrayList<>();
            for (Map<String, Object> call : calls) {
              blocks += ((Number) call.get("blocks")).longValue();
              blockCounts.add(call.get("blocks"));
              groupBits.add(call.get("type_bits"));
            }
            long pinned = 0;
            long nonPinned = 0;
            long unknown = 0;
            long capacity = 0;
            List<Object> descriptors = new ArrayList<>();
            for (int i = 0; i < retained.size(); i++) {
              HostMemoryBuffer payload = retained.get(i);
              long length = payload.getLength();
              capacity += length;
              Object handler = payload.getEventHandler();
              String handlerClass = handler == null ? "" : handler.getClass().getName();
              Boolean isPinned = handlerClass.contains("HostAlloc$OnPinnedCloseCallback") ? Boolean.TRUE
                  : handlerClass.contains("HostAlloc$OnCloseCallback") ? Boolean.FALSE : null;
              if (isPinned == null) unknown += length;
              else if (isPinned) pinned += length;
              else nonPinned += length;
              Map<String, Object> descriptor = map();
              descriptor.put("offset", segments[i].offset);
              descriptor.put("uncompressed_length", segments[i].length);
              descriptor.put("type_bits", segments[i].typeBits);
              descriptor.put("compressed_length", length);
              descriptor.put("host_is_pinned", isPinned);
              descriptor.put("host_allocation_handler_class", handlerClass);
              descriptors.add(descriptor);
            }
            result.put("generate_ns", generateNs);
            result.put("generate_started_ns", prepared.generateStartedNs);
            result.put("generate_finished_ns", prepared.generateStartedNs + generateNs);
            result.put("encode_started_ns", encodeStart);
            result.put("encode_finished_ns", encodeStart + totalNs);
            result.put("pack_ns", packNs);
            result.put("descriptor_build_ns_subset_of_pack", descriptorNs);
            result.put("compress_ns", compressionNs);
            result.put("d2h_ns", d2hNs);
            result.put("encode_host_roundtrip_ns", totalNs);
            result.put("packed_bytes", packedBytes);
            result.put("canonical_logical_bytes",
                PackedFixtures.canonicalLogicalBytes(fixture, firstRow, rows));
            result.put("compressed_payload_including_codec_framing_bytes", record.payloadBytes());
            result.put("opaque_table_metadata_bytes", metadata.length);
            result.put("experiment_header_bytes", HEADER_BYTES);
            result.put("segment_descriptor_bytes", record.descriptors.length);
            result.put("segment_count", segments.length);
            result.put("segment_descriptors", descriptors);
            result.put("retained_host_used_bytes", record.retainedBytes());
            result.put("retained_host_payload_capacity_bytes", capacity);
            result.put("host_pinned_payload_bytes", pinned);
            result.put("host_non_pinned_payload_bytes", nonPinned);
            result.put("host_unknown_pinning_payload_bytes", unknown);
            result.put("host_payload_prefer_pinned", true);
            result.put("native_batched_calls", calls.size());
            result.put("compression_calls", calls);
            result.put("blocks_per_native_batched_call", blockCounts);
            result.put("type_bits_per_native_batched_call", groupBits);
            result.put("codec_chunk_framing_bytes_in_payload", blocks * 8L);
            result.put("payload_over_packed_ratio", (double) record.payloadBytes() / packedBytes);
            result.put("retained_over_logical_ratio", (double) record.retainedBytes()
                / PackedFixtures.canonicalLogicalBytes(fixture, firstRow, rows));
            result.put("host_usage_ratio_after_encode", HostAlloc.getUsageRatio());
            retained.release();
            return record;
          }
        }
      }
    }
  }

  public static DecodedResult decodeWorkItem(HostRecord record) {
    Cuda.autoSetDevice();
    Cuda.Stream stream = Cuda.DEFAULT_STREAM;
    stream.sync();
    long decodeStart = System.nanoTime();
    long framingStart = System.nanoTime();
    PackedSegments.Segment[] segments = record.readDescriptors();
    List<CodecSubmission> plan = submissions(record.codec, record.chunkBytes, segments);
    long framingNs = System.nanoTime() - framingStart;
    long h2dStart = System.nanoTime();
    try (CloseableArray<DeviceMemoryBuffer> compressed =
             CloseableArray.wrap(toDevice(record.payloads, stream))) {
      long h2dNs = System.nanoTime() - h2dStart;
      long decompressStart = System.nanoTime();
      try (DeviceMemoryBuffer decoded = decompressSegments(record.codec, record.chunkBytes,
               compressed.getArray(), segments, plan, record.packedBytes, stream)) {
        long decompressNs = finish(stream, decompressStart);
        long unpackStart = System.nanoTime();
        try (CloseableArray<Table> actual = CloseableArray.wrap(new Table[1])) {
          actual.set(0, Table.fromPackedTable(ByteBuffer.wrap(record.metadata), decoded));
          long unpackNs = finish(stream, unpackStart);
          long totalNs = finish(stream, decodeStart);
          List<Map<String, Object>> calls = describeCalls(plan);
          List<Object> blockCounts = new ArrayList<>();
          List<Object> typeBits = new ArrayList<>();
          List<Object> segmentCounts = new ArrayList<>();
          for (Map<String, Object> call : calls) {
            blockCounts.add(call.get("blocks"));
            typeBits.add(call.get("type_bits"));
            segmentCounts.add(call.get("segments"));
          }
          Map<String, Object> result = map();
          result.put("decode_started_ns", decodeStart);
          result.put("decode_finished_ns", decodeStart + totalNs);
          result.put("framing_decode_ns_subset_of_total", framingNs);
          result.put("h2d_ns", h2dNs);
          result.put("decompress_ns", decompressNs);
          result.put("uncompressed_segment_reassembly_bytes",
              record.codec.equals("uncompressed") ? record.packedBytes : 0L);
          result.put("unpack_ns", unpackNs);
          result.put("decode_host_roundtrip_ns", totalNs);
          result.put("full_packed_bytes_checked", false);
          result.put("native_batched_calls", calls.size());
          result.put("decompression_calls", calls);
          result.put("blocks_per_native_batched_call", blockCounts);
          result.put("type_bits_per_native_batched_call", typeBits);
          result.put("segments_per_native_batched_call", segmentCounts);
          DecodedResult output = new DecodedResult(record, result, actual.get(0));
          actual.release();
          return output;
        }
      }
    }
  }

  /** Called only after BOTH timed decodes in the wave have completed. */
  public static void validateWorkItem(DecodedResult decoded) {
    Cuda.autoSetDevice();
    long started = System.nanoTime();
    HostRecord record = decoded.record;
    try (Table expected = PackedFixtures.create(record.fixture, record.rows, record.firstRow)) {
      Cuda.DEFAULT_STREAM.sync();
      PackedFixtures.validate(expected, decoded.table);
    }
    long elapsed = finish(Cuda.DEFAULT_STREAM, started);
    decoded.timings.put("validation_excluded_ns", elapsed);
    decoded.timings.put("validation_started_ns", started);
    decoded.timings.put("validation_finished_ns", started + elapsed);
    decoded.timings.put("all_values_nulls_types_rows_order_checked", true);
  }

  /** Same original packed buffer remains alive: padding/null-slot bytes are never regenerated. */
  public static void validateSmallRoundTrip(String fixture, String codec, long chunk) {
    Cuda.Stream stream = Cuda.DEFAULT_STREAM;
    try (Table input = PackedFixtures.create(fixture, 513, 7);
         CloseableArray<ContiguousTable> packed = CloseableArray.wrap(input.contiguousSplit())) {
      stream.sync();
      ContiguousTable original = packed.get(0);
      PackedSegments.Segment[] segments = PackedSegments.describe(
          original, codec.equals("cascaded-typed"));
      List<CodecSubmission> plan = submissions(codec, chunk, segments);
      try (CloseableArray<DeviceMemoryBuffer> encoded = CloseableArray.wrap(
               compressSegments(codec, chunk, original.getBuffer(), segments, plan, stream));
           CloseableArray<HostMemoryBuffer> host =
               CloseableArray.wrap(toHost(encoded.getArray(), stream));
           CloseableArray<DeviceMemoryBuffer> uploaded =
               CloseableArray.wrap(toDevice(host.getArray(), stream));
           DeviceMemoryBuffer decoded = decompressSegments(codec, chunk, uploaded.getArray(),
               segments, plan, original.getBuffer().getLength(), stream)) {
        stream.sync();
        PackedFixtures.validatePacked(original.getBuffer(), decoded);
        try (Table restored = Table.fromPackedTable(original.getMetadataDirectBuffer(), decoded)) {
          PackedFixtures.validate(input, restored);
        }
      }
    }
    stream.sync();
  }

  private static <T> List<T> collect(List<Future<T>> futures) throws Exception {
    List<T> result = new ArrayList<>();
    Throwable failure = null;
    // Drain every task before cleanup, including after failure: retained records cannot be leaked.
    for (Future<T> future : futures) {
      try {
        result.add(future.get());
      } catch (ExecutionException error) {
        if (failure == null) failure = error.getCause(); else failure.addSuppressed(error.getCause());
      }
    }
    if (failure != null) {
      for (T value : result) {
        if (value instanceof AutoCloseable) {
          try (AutoCloseable owned = (AutoCloseable) value) { /* release successful sibling */ }
          catch (Exception cleanup) { failure.addSuppressed(cleanup); }
        }
      }
      throw new IllegalStateException("Work item failed; no larger case is authorized", failure);
    }
    return result;
  }

  /** Exact CLI: --fixture --codecs CSV --chunk-bytes --task-mib OR --rows, --tasks 4 --slots 2, --warmups --repeats --output. */
  public static void main(String[] args) throws Exception {
    Map<String, String> options = new LinkedHashMap<>();
    for (int i = 0; i < args.length; i += 2) {
      if (i + 1 == args.length || !args[i].startsWith("--")) {
        throw new IllegalArgumentException("Expected flag/value pairs");
      }
      if (options.put(args[i], args[i + 1]) != null) throw new IllegalArgumentException("Repeated flag");
    }
    for (String flag : options.keySet()) {
      if (!List.of("--fixture", "--codecs", "--chunk-bytes", "--task-mib", "--rows",
          "--tasks", "--slots", "--warmups", "--repeats", "--output").contains(flag)) {
        throw new IllegalArgumentException("Unknown flag " + flag);
      }
    }
    String fixture = options.getOrDefault("--fixture", "fixed-wide");
    String[] codecs = options.getOrDefault("--codecs", "uncompressed").split(",", -1);
    java.util.HashSet<String> uniqueCodecs = new java.util.HashSet<>();
    for (String codec : codecs) {
      codecId(codec);
      if (!uniqueCodecs.add(codec)) throw new IllegalArgumentException("Duplicate codec");
    }
    if (Integer.parseInt(options.getOrDefault("--tasks", "4")) != WORK_ITEMS
        || Integer.parseInt(options.getOrDefault("--slots", "2")) != SLOTS) {
      throw new IllegalArgumentException("Experiment requires four work items and two slots");
    }
    long chunk = Long.parseLong(options.getOrDefault("--chunk-bytes", "65536"));
    if (options.containsKey("--task-mib") && options.containsKey("--rows")) {
      throw new IllegalArgumentException("Choose task-mib or small rows smoke");
    }
    int taskMiB = Integer.parseInt(options.getOrDefault("--task-mib", "256"));
    if (!List.of(256, 512, 1024, 2048).contains(taskMiB)) {
      throw new IllegalArgumentException("Approved task MiB: 256/512/1024/2048");
    }
    int rows = options.containsKey("--rows") ? Integer.parseInt(options.get("--rows"))
        : Math.toIntExact(PackedFixtures.rowsForTask(fixture, taskMiB));
    if (rows <= 0 || (options.containsKey("--rows") && rows > 65536)) {
      throw new IllegalArgumentException("Small explicit rows smoke must be in 1..65536");
    }
    int warmups = Integer.parseInt(options.getOrDefault("--warmups", "1"));
    int repeats = Integer.parseInt(options.getOrDefault("--repeats", "2"));
    if (warmups < 0 || warmups > 2 || repeats < 1 || repeats > 5) {
      throw new IllegalArgumentException("Bounded warmups 0..2, repeats 1..5");
    }
    checkSubmissionPlans();
    Map<String, Object> report = map();
    report.put("submission_plan_cpu_boundary_checks_passed", true);
    report.put("resources", initialize());
    report.put("fixture", fixture);
    report.put("codecs", codecs);
    report.put("chunk_bytes", chunk);
    report.put("max_codec_submission_uncompressed_bytes", PackedSegments.MAX_SUBMISSION_BYTES);
    report.put("max_codec_submission_workspace_input_upper_bound_bytes",
        PackedSegments.MAX_SUBMISSION_BYTES);
    report.put("max_codec_submission_blocks", PackedSegments.MAX_SUBMISSION_BYTES / chunk);
    report.put("rows_per_work_item", rows);
    report.put("task_mib_target", options.containsKey("--rows") ? null : taskMiB);
    report.put("slots", SLOTS);
    report.put("work_items", WORK_ITEMS);
    report.put("small_bit_exact_prerequisite_rows", 513);
    report.put("first_large_sample_is_cold", false);
    report.put("phase_timestamp_clock", "System.nanoTime monotonic process clock, not epoch time");
    report.put("untimed_fixture_preparation_and_validation_execution", "serial within each two-item wave");
    report.put("wave_phase_barriers",
        "Generate inputs serially before encoding either; finish both concurrent timed decodes before serial reference "
        + "generation/validation. Wall clocks include task dispatch/join and cleanup; per-item timers exclude cleanup.");
    report.put("compression_stage_scope",
        "Batched wrapper allocation, native compression, length fetch and output stitching; not pure kernel time");
    report.put("scope", "GPU-only standalone pack/codec/host round trips; no Spark cache timing. "
        + "Each worker synchronizes its per-thread default CUDA stream. Generation, validation and "
        + "final cleanup excluded from encode/decode timers but included in whole-job memory peaks. "
        + "A small codec prerequisite precedes every first large sample; it is not a cold-start measurement. "
        + "Codec submissions bound both actual uncompressed input and rounded blocks*chunkBytes "
        + "workspace input upper bound to 256 MiB; layout segments are at most 256 MiB. "
        + "compressed payload may expand beyond that size. Uncompressed decode copies segment "
        + "uploads into one packed allocation and includes that device-copy work in decompress_ns. "
        + "Untimed preparation and reference validation are serialized to bound their temporary memory; "
        + "timed encode/decode still use two concurrent task slots. This is not a Spark input pipeline benchmark. "
        + "This submission/host-allocation cohort differs from earlier whole-table calls. "
        + "Compressed payload includes per-chunk codec framing. Ordered descriptors use 32 bytes "
        + "per segment including compressed lengths; metadata/header/descriptors are retained Java "
        + "arrays outside HostAlloc, counted explicitly; RSS requires an external early-stop sampler.");
    if (uniqueCodecs.contains("cascaded8") || uniqueCodecs.contains("cascaded-typed")) {
      report.put("cascaded_native", CascadedCodec.nativeProvenance());
    }
    List<Object> cases = new ArrayList<>();
    ExecutorService executor = Executors.newFixedThreadPool(SLOTS);
    try {
      for (String codec : codecs) {
        validateSmallRoundTrip(fixture, codec, chunk);
        List<Object> samples = new ArrayList<>();
        for (int iteration = -warmups; iteration < repeats; iteration++) {
          try (CloseableArray<HostRecord> retained =
                   CloseableArray.wrap(new HostRecord[WORK_ITEMS])) {
            List<Object> encodeWaves = new ArrayList<>();
            for (int base = 0; base < WORK_ITEMS; base += SLOTS) {
              Rmm.resetScopedMaximumBytesAllocated(Rmm.getTotalBytesAllocated());
              long generateStart = System.nanoTime();
              try (CloseableArray<PreparedInput> wave =
                       CloseableArray.wrap(new PreparedInput[SLOTS])) {
                // Untimed fixture scratch is serialized; both complete before timed work starts.
                for (int slot = 0; slot < SLOTS; slot++) {
                  final long firstRow = (long) (base + slot) * rows;
                  Future<PreparedInput> prepared = executor.submit(
                      () -> prepareWorkItem(fixture, rows, firstRow));
                  wave.set(slot, collect(List.of(prepared)).get(0));
                }
                long generateWallNs = System.nanoTime() - generateStart;
                long generateGpuPeak = Rmm.getScopedMaximumBytesAllocated();
                long encodeGpuBaseline = Rmm.getTotalBytesAllocated();
                Rmm.resetScopedMaximumBytesAllocated(encodeGpuBaseline);
                // Both streams have completed generation before either timed encode is submitted.
                long encodeStart = System.nanoTime();
                List<Future<HostRecord>> encodeTasks = new ArrayList<>();
                for (PreparedInput input : wave.getArray()) {
                  encodeTasks.add(executor.submit(() -> encodeWorkItem(input, codec, chunk)));
                }
                List<HostRecord> records = collect(encodeTasks);
                for (int slot = 0; slot < SLOTS; slot++) retained.set(base + slot, records.get(slot));
                long encodeWallNs = System.nanoTime() - encodeStart;
                Map<String, Object> phase = map();
                phase.put("wave", base / SLOTS);
                phase.put("generate_wall_ns", generateWallNs);
                phase.put("encode_wall_ns", encodeWallNs);
                phase.put("generate_gpu_live_peak_bytes", generateGpuPeak);
                phase.put("encode_gpu_live_baseline_bytes", encodeGpuBaseline);
                phase.put("encode_gpu_live_peak_bytes", Rmm.getScopedMaximumBytesAllocated());
                encodeWaves.add(phase);
              }
            }
            long retainedBytes = 0;
            long pinnedBytes = 0;
            long nonPinnedBytes = 0;
            for (HostRecord record : retained.getArray()) {
              retainedBytes += record.retainedBytes();
              pinnedBytes += ((Number) record.encode.get("host_pinned_payload_bytes")).longValue();
              nonPinnedBytes += ((Number) record.encode.get("host_non_pinned_payload_bytes")).longValue();
            }
            // All four host records stay owned until BOTH decode/validation waves finish.
            List<Object> decodeWaves = new ArrayList<>();
            List<Object> workResults = new ArrayList<>();
            for (int base = 0; base < WORK_ITEMS; base += SLOTS) {
              Rmm.resetScopedMaximumBytesAllocated(Rmm.getTotalBytesAllocated());
              long decodeStart = System.nanoTime();
              List<Future<DecodedResult>> decodeTasks = new ArrayList<>();
              for (int slot = 0; slot < SLOTS; slot++) {
                HostRecord record = retained.get(base + slot);
                decodeTasks.add(executor.submit(() -> decodeWorkItem(record)));
              }
              List<DecodedResult> outputs = collect(decodeTasks);
              try (CloseableArray<DecodedResult> wave =
                       CloseableArray.wrap(outputs.toArray(new DecodedResult[0]))) {
                long decodeWallNs = System.nanoTime() - decodeStart;
                long decodeGpuPeak = Rmm.getScopedMaximumBytesAllocated();
                long validationGpuBaseline = Rmm.getTotalBytesAllocated();
                Rmm.resetScopedMaximumBytesAllocated(validationGpuBaseline);
                // Neither reference generation nor validation starts until both timed decodes end.
                long validateStart = System.nanoTime();
                for (DecodedResult decoded : outputs) {
                  Future<Boolean> validation = executor.submit(() -> {
                    validateWorkItem(decoded);
                    return Boolean.TRUE;
                  });
                  collect(List.of(validation));
                }
                long validationWallNs = System.nanoTime() - validateStart;
                for (int slot = 0; slot < SLOTS; slot++) {
                  DecodedResult decoded = outputs.get(slot);
                  HostRecord record = decoded.record;
                  Map<String, Object> item = map();
                  item.put("work_item", base + slot);
                  item.put("first_row", record.firstRow);
                  item.put("rows", record.rows);
                  item.put("encode", record.encode);
                  item.put("decode", decoded.timings);
                  workResults.add(item);
                }
                Map<String, Object> phase = map();
                phase.put("wave", base / SLOTS);
                phase.put("decode_wall_ns", decodeWallNs);
                phase.put("validation_wall_ns", validationWallNs);
                phase.put("decode_gpu_live_peak_bytes", decodeGpuPeak);
                phase.put("validation_gpu_live_baseline_bytes", validationGpuBaseline);
                phase.put("validation_gpu_live_peak_bytes", Rmm.getScopedMaximumBytesAllocated());
                decodeWaves.add(phase);
              }
            }
            Map<String, Object> sample = map();
            sample.put("codec", codec);
            sample.put("warmup", iteration < 0);
            sample.put("iteration", iteration);
            sample.put("encode_waves", encodeWaves);
            sample.put("decode_waves", decodeWaves);
            sample.put("work_items", workResults);
            sample.put("four_records_retained_bytes_at_encode_barrier", retainedBytes);
            sample.put("four_records_pinned_payload_bytes_at_encode_barrier", pinnedBytes);
            sample.put("four_records_non_pinned_payload_bytes_at_encode_barrier", nonPinnedBytes);
            samples.add(sample);
            System.out.println(new ObjectMapper().writeValueAsString(sample));
            System.out.flush();
          }
        }
        Map<String, Object> caseResult = map();
        caseResult.put("codec", codec);
        caseResult.put("same_original_small_packed_bytes_checked", true);
        caseResult.put("samples", samples);
        cases.add(caseResult);
      }
      report.put("cases", cases);
      report.put("rmm_tracked_live_bytes_after_records_close", Rmm.getTotalBytesAllocated());
      report.put("rmm_tracked_cumulative_peak_bytes", Rmm.getMaximumTotalBytesAllocated());
      report.put("host_usage_ratio_after_records_close", HostAlloc.getUsageRatio());
      if (Rmm.getTotalBytesAllocated() != 0 || HostAlloc.getUsageRatio() != 0.0) {
        throw new IllegalStateException("Resources remain live after all records and validations");
      }
      report.put("status", "PASS");
      String output = options.get("--output");
      if (output != null) {
        Files.writeString(Path.of(output),
            new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(report) + "\n");
      } else {
        System.out.println(new ObjectMapper().writeValueAsString(report));
      }
    } finally {
      executor.shutdown();
      if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
        executor.shutdownNow();
        throw new IllegalStateException("Worker shutdown timed out");
      }
    }
  }
}
