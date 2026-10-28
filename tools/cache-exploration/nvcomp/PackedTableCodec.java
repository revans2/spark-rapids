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
import ai.rapids.cudf.DeviceMemoryBuffer;
import ai.rapids.cudf.nvcomp.BatchedCompressor;
import ai.rapids.cudf.nvcomp.BatchedDecompressor;
import ai.rapids.cudf.nvcomp.BatchedLZ4Compressor;
import ai.rapids.cudf.nvcomp.BatchedLZ4Decompressor;
import ai.rapids.cudf.nvcomp.BatchedZstdCompressor;
import ai.rapids.cudf.nvcomp.BatchedZstdDecompressor;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * GPU-only experimental multi-table codec. Inputs/stream are borrowed; returned batches own all
 * outputs. Callers make inputs ready on the borrowed stream. Calls finish that stream before
 * returning or completing exception cleanup; input tables and buffers are never consumed.
 * No allocator initialization, host data transfer, Spark fallback or production format is added.
 */
public final class PackedTableCodec {
  private static final long INTERMEDIATE_BYTES = 64L * 1024 * 1024;
  private PackedTableCodec() {}

  private static Map<String, Object> map() { return new LinkedHashMap<>(); }

  private static void checkCodec(String codec) {
    if (!List.of("uncompressed", "lz4", "zstd", "snappy", "ans", "cascaded8", "cascaded-typed").contains(codec)) {
      throw new IllegalArgumentException("Unsupported GPU packed codec: " + codec);
    }
  }

  /** One actual native submission: homogeneous integer width and bounded input bytes. */
  public static final class Call {
    public final int typeBits;
    public final int[] indices;
    public final long inputBytes;
    public final long blocks;
    public final long blockBudget;
    public final long workspaceInputUpperBoundBytes;
    Call(int bits, List<Integer> indices, long bytes, long blocks, long chunk) {
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
    public Map<String, Object> describe() {
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

  public static List<Call> plan(
      String codec, long chunk, PackedSegments.Segment[] segments) {
    checkCodec(codec);
    List<Call> result = new ArrayList<>();
    if (chunk <= 0 || chunk > (1L << 24) || chunk % 8 != 0
        || PackedSegments.MAX_SUBMISSION_BYTES % chunk != 0) {
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
          result.add(new Call(bits, indices, bytes, blocks, chunk));
          indices = new ArrayList<>();
          bytes = 0;
          blocks = 0;
        }
        indices.add(i);
        bytes += segment.length;
        blocks += segmentBlocks;
      }
      if (!indices.isEmpty()) result.add(new Call(bits, indices, bytes, blocks, chunk));
    }
    int planned = result.stream().mapToInt(call -> call.indices.length).sum();
    if (planned != segments.length) throw new IllegalArgumentException("Submission plan coverage");
    return result;
  }

  /** Build a layout from retained descriptors without exposing a package-private constructor. */
  public static PackedSegments.Segment[] layout(long[] offsets, long[] lengths, int[] bits) {
    if (offsets.length != lengths.length || offsets.length != bits.length) {
      throw new IllegalArgumentException("Layout descriptor shape");
    }
    PackedSegments.Segment[] result = new PackedSegments.Segment[offsets.length];
    for (int i = 0; i < result.length; i++) {
      result[i] = new PackedSegments.Segment(offsets[i], lengths[i], bits[i]);
    }
    return result;
  }

  private static final class Drain implements AutoCloseable {
    private final Cuda.Stream stream;
    private boolean finished;
    Drain(Cuda.Stream stream) { this.stream = stream; }
    void finish() { stream.sync(); finished = true; }
    @Override public void close() { if (!finished) stream.sync(); }
  }

  public static final class EncodedTable implements AutoCloseable {
    public final long packedBytes;
    public final byte[] metadata;
    public final PackedSegments.Segment[] segments;
    /** Borrowed from this owner until close. */
    public final DeviceMemoryBuffer[] payloads;
    private boolean closed;
    private EncodedTable(long bytes, byte[] metadata, PackedSegments.Segment[] segments,
        DeviceMemoryBuffer[] payloads) {
      this.packedBytes = bytes;
      this.metadata = metadata;
      this.segments = segments;
      this.payloads = payloads;
    }
    @Override public synchronized void close() {
      if (!closed) {
        closed = true;
        try (CloseableArray<DeviceMemoryBuffer> owned = CloseableArray.wrap(payloads)) {
          /* release owned outputs */
        } finally { Arrays.fill(payloads, null); }
      }
    }
  }

  public static final class EncodedBatch implements AutoCloseable {
    public final EncodedTable[] tables;
    public final Call[] calls;
    private boolean closed;
    private EncodedBatch(EncodedTable[] tables, Call[] calls) {
      this.tables = tables; this.calls = calls;
    }
    @Override public synchronized void close() {
      if (!closed) {
        closed = true;
        try (CloseableArray<EncodedTable> owned = CloseableArray.wrap(tables)) {
          /* release owned tables */
        } finally { Arrays.fill(tables, null); }
      }
    }
  }

  public static final class DecodedBatch implements AutoCloseable {
    /** One owned contiguous allocation per table; borrowed by callers until this batch closes. */
    public final DeviceMemoryBuffer[] packedOutputs;
    public final Call[] calls;
    private boolean closed;
    private DecodedBatch(DeviceMemoryBuffer[] outputs, Call[] calls) {
      this.packedOutputs = outputs; this.calls = calls;
    }
    @Override public synchronized void close() {
      if (!closed) {
        closed = true;
        try (CloseableArray<DeviceMemoryBuffer> owned = CloseableArray.wrap(packedOutputs)) {
          /* release owned allocations */
        } finally { Arrays.fill(packedOutputs, null); }
      }
    }
  }

  private static BatchedCompressor compressor(String codec, long chunk, int bits) {
    switch (codec) {
      case "lz4": return new BatchedLZ4Compressor(chunk, INTERMEDIATE_BYTES);
      case "zstd": return new BatchedZstdCompressor(chunk, INTERMEDIATE_BYTES);
      case "snappy":
      case "ans": return ByteCodec.compressor(codec, chunk, INTERMEDIATE_BYTES);
      case "cascaded8":
      case "cascaded-typed": return CascadedCodec.compressor(chunk, INTERMEDIATE_BYTES, bits);
      default: throw new IllegalArgumentException("No compressed codec for " + codec);
    }
  }

  private static BatchedDecompressor decompressor(String codec, long chunk) {
    switch (codec) {
      case "lz4": return new BatchedLZ4Decompressor(chunk);
      case "zstd": return new BatchedZstdDecompressor(chunk);
      case "snappy":
      case "ans": return ByteCodec.decompressor(codec, chunk);
      case "cascaded8":
      case "cascaded-typed": return CascadedCodec.decompressor(chunk);
      default: throw new IllegalArgumentException("No compressed codec for " + codec);
    }
  }

  private static int[] starts(PackedSegments.Segment[][] layouts, long[] bytes) {
    if (layouts.length != bytes.length) throw new IllegalArgumentException("Table layout count");
    int[] starts = new int[layouts.length + 1];
    long total = 0;
    for (int t = 0; t < layouts.length; t++) {
      long covered = 0;
      for (PackedSegments.Segment segment : layouts[t]) {
        if (segment.offset != covered || segment.offset % 8 != 0 || segment.length <= 0
            || segment.length > PackedSegments.MAX_SUBMISSION_BYTES
            || (segment.typeBits != 8 && segment.typeBits != 32 && segment.typeBits != 64)
            || segment.length % (segment.typeBits / 8) != 0) {
          throw new IllegalArgumentException("Invalid ordered packed layout");
        }
        covered = Math.addExact(covered, segment.length);
      }
      if (covered != bytes[t] || bytes[t] <= 0) {
        throw new IllegalArgumentException("Layout does not cover nonempty packed table");
      }
      total = Math.addExact(total, covered);
      starts[t + 1] = Math.addExact(starts[t], layouts[t].length);
    }
    if (total > PackedSegments.MAX_SUBMISSION_BYTES) {
      throw new IllegalArgumentException("Packed table batch exceeds 256 MiB");
    }
    return starts;
  }

  private static PackedSegments.Segment[] flatten(PackedSegments.Segment[][] layouts, int count) {
    PackedSegments.Segment[] result = new PackedSegments.Segment[count];
    int next = 0;
    for (PackedSegments.Segment[] layout : layouts) {
      System.arraycopy(layout, 0, result, next, layout.length);
      next += layout.length;
    }
    return result;
  }

  public static EncodedBatch compress(ContiguousTable[] borrowedTables, String codec,
      long chunkBytes, Cuda.Stream borrowedStream) {
    int tables = borrowedTables.length;
    long[] bytes = new long[tables];
    byte[][] metadata = new byte[tables][];
    PackedSegments.Segment[][] layouts = new PackedSegments.Segment[tables][];
    for (int t = 0; t < tables; t++) {
      bytes[t] = borrowedTables[t].getBuffer().getLength();
      ByteBuffer source = borrowedTables[t].getMetadataDirectBuffer().duplicate();
      metadata[t] = new byte[source.remaining()];
      source.get(metadata[t]);
      layouts[t] = PackedSegments.describe(borrowedTables[t], codec.equals("cascaded-typed"));
    }
    int[] starts = starts(layouts, bytes);
    PackedSegments.Segment[] flat = flatten(layouts, starts[tables]);
    List<Call> calls = plan(codec, chunkBytes, flat);
    DeviceMemoryBuffer[] sources = new DeviceMemoryBuffer[flat.length];
    for (int t = 0; t < tables; t++) {
      Arrays.fill(sources, starts[t], starts[t + 1], borrowedTables[t].getBuffer());
    }
    try (CloseableArray<EncodedTable> result = CloseableArray.wrap(new EncodedTable[tables]);
         CloseableArray<DeviceMemoryBuffer> outputs =
             CloseableArray.wrap(new DeviceMemoryBuffer[flat.length]);
         Drain drain = new Drain(borrowedStream)) {
      if (codec.equals("uncompressed")) {
        for (int i = 0; i < flat.length; i++) {
          outputs.set(i, sources[i].slice(flat[i].offset, flat[i].length));
        }
      } else {
        for (Call call : calls) {
          BatchedCompressor encoder = compressor(codec, chunkBytes, call.typeBits);
          try (CloseableArray<BaseDeviceMemoryBuffer> inputs =
                   CloseableArray.wrap(new BaseDeviceMemoryBuffer[call.indices.length])) {
            for (int i = 0; i < call.indices.length; i++) {
              int index = call.indices[i];
              inputs.set(i, sources[index].slice(flat[index].offset, flat[index].length));
            }
            try (CloseableArray<DeviceMemoryBuffer> encoded =
                     CloseableArray.wrap(encoder.compress(inputs.release(), borrowedStream));
                 Drain callDrain = new Drain(borrowedStream)) {
              if (encoded.size() != call.indices.length) {
                throw new IllegalStateException("Compressed output count");
              }
              for (int i = 0; i < call.indices.length; i++) {
                encoded.get(i).incRefCount();
                outputs.set(call.indices[i], encoded.get(i));
              }
              callDrain.finish();
            }
          }
        }
      }
      drain.finish();
      for (int t = 0; t < tables; t++) {
        try (CloseableArray<DeviceMemoryBuffer> payloads =
                 CloseableArray.wrap(new DeviceMemoryBuffer[layouts[t].length])) {
          for (int i = 0; i < layouts[t].length; i++) {
            outputs.get(starts[t] + i).incRefCount();
            payloads.set(i, outputs.get(starts[t] + i));
          }
          result.set(t, new EncodedTable(bytes[t], metadata[t], layouts[t], payloads.getArray()));
          payloads.release();
        }
      }
      EncodedBatch batch = new EncodedBatch(result.getArray(), calls.toArray(new Call[0]));
      result.release();
      return batch;
    }
  }

  public static DecodedBatch decompress(DeviceMemoryBuffer[][] borrowedInputs,
      PackedSegments.Segment[][] layouts, long[] packedBytes, String codec,
      long chunkBytes, Cuda.Stream borrowedStream) {
    if (borrowedInputs.length != layouts.length) throw new IllegalArgumentException("Input table count");
    int tables = layouts.length;
    int[] starts = starts(layouts, packedBytes);
    PackedSegments.Segment[] flat = flatten(layouts, starts[tables]);
    List<Call> calls = plan(codec, chunkBytes, flat);
    DeviceMemoryBuffer[] inputs = new DeviceMemoryBuffer[flat.length];
    int[] owners = new int[flat.length];
    for (int t = 0; t < tables; t++) {
      if (borrowedInputs[t].length != layouts[t].length) {
        throw new IllegalArgumentException("Input segment count");
      }
      System.arraycopy(borrowedInputs[t], 0, inputs, starts[t], borrowedInputs[t].length);
      Arrays.fill(owners, starts[t], starts[t + 1], t);
    }
    try (CloseableArray<DeviceMemoryBuffer> outputs =
             CloseableArray.wrap(new DeviceMemoryBuffer[tables]);
         Drain drain = new Drain(borrowedStream)) {
      for (int t = 0; t < tables; t++) {
        outputs.set(t, DeviceMemoryBuffer.allocate(packedBytes[t], borrowedStream));
      }
      if (codec.equals("uncompressed")) {
        for (int i = 0; i < flat.length; i++) {
          if (inputs[i].getLength() != flat[i].length) {
            throw new IllegalArgumentException("Uncompressed segment length");
          }
          outputs.get(owners[i]).copyFromDeviceBufferAsync(
              flat[i].offset, inputs[i], 0, flat[i].length, borrowedStream);
        }
      } else {
        BatchedDecompressor decoder = decompressor(codec, chunkBytes);
        for (Call call : calls) {
          try (CloseableArray<BaseDeviceMemoryBuffer> refs =
                   CloseableArray.wrap(new BaseDeviceMemoryBuffer[call.indices.length]);
               CloseableArray<BaseDeviceMemoryBuffer> slices =
                   CloseableArray.wrap(new BaseDeviceMemoryBuffer[call.indices.length]);
               Drain callDrain = new Drain(borrowedStream)) {
            for (int i = 0; i < call.indices.length; i++) {
              int index = call.indices[i];
              slices.set(i, outputs.get(owners[index]).slice(flat[index].offset, flat[index].length));
              inputs[index].incRefCount();
              refs.set(i, inputs[index]);
            }
            decoder.decompressAsync(refs.release(), slices.getArray(), borrowedStream);
            callDrain.finish();
          }
        }
      }
      drain.finish();
      DecodedBatch batch = new DecodedBatch(outputs.getArray(), calls.toArray(new Call[0]));
      outputs.release();
      return batch;
    }
  }
  /** CPU-only planner prerequisites: no allocator initialization or GPU operations. */
  public static void main(String[] ignored) {
    for (long chunk : new long[] {64L * 1024, 256L * 1024}) {
      PackedSegments.Segment[] tables = new PackedSegments.Segment[32];
      for (int i = 0; i < tables.length; i++) {
        // Independent table-relative offsets deliberately repeat at zero.
        tables[i] = new PackedSegments.Segment(0, 8L * 1024 * 1024, 8);
      }
      for (String codec : new String[] {"lz4", "zstd", "snappy", "ans"}) {
        List<Call> calls = plan(codec, chunk, tables);
        if (calls.size() != 1 || calls.get(0).indices.length != 32
            || calls.get(0).inputBytes != PackedSegments.MAX_SUBMISSION_BYTES
            || calls.get(0).blocks != PackedSegments.MAX_SUBMISSION_BYTES / chunk) {
          throw new AssertionError("Many small tables must share one many-block native submission");
        }
      }
    }
    PackedSegments.Segment[] tiny = new PackedSegments.Segment[1025];
    for (int i = 0; i < tiny.length; i++) tiny[i] = new PackedSegments.Segment(0, 8, 8);
    List<Call> calls = plan("cascaded-typed", 256L * 1024, tiny);
    if (calls.size() != 2 || calls.get(0).blocks != 1024 || calls.get(1).blocks != 1) {
      throw new AssertionError("Rounded workspace input budget");
    }
    calls = plan("cascaded-typed", 65536, new PackedSegments.Segment[] {
        new PackedSegments.Segment(0, 8, 64), new PackedSegments.Segment(0, 8, 8),
        new PackedSegments.Segment(0, 8, 32), new PackedSegments.Segment(0, 8, 64)});
    if (calls.size() != 3 || calls.get(0).indices[0] != 1 || calls.get(1).indices[0] != 2
        || !Arrays.equals(calls.get(2).indices, new int[] {0, 3})) {
      throw new AssertionError("Stable homogeneous width-group mapping");
    }
    System.out.println("PASS CPU submission plans; GPU round trips remain separate prerequisites");
  }

}
