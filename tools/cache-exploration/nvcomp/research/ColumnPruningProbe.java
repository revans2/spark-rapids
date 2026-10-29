/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cacheexploration.nvcomp;

import ai.rapids.cudf.CloseableArray;
import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.ContiguousTable;
import ai.rapids.cudf.Cuda;
import ai.rapids.cudf.DeviceMemoryBuffer;
import ai.rapids.cudf.HostColumnVector;
import ai.rapids.cudf.HostMemoryBuffer;
import ai.rapids.cudf.NvtxColor;
import ai.rapids.cudf.NvtxRange;
import ai.rapids.cudf.Rmm;
import ai.rapids.cudf.RmmAllocationMode;
import ai.rapids.cudf.Table;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.cacheexploration.CpuPackedCodec;
import org.apache.spark.sql.cacheexploration.HostPackedBridge;

/**
 * Isolated feasibility proof, not a Spark serializer or throughput benchmark.
 * Fixture generation and reference downloads are outside indexing/decode profiling ranges.
 */
public final class ColumnPruningProbe {
  private static final int ROWS = 32769;
  private static final int COLUMNS = 38;
  private static final int CHUNK = 65536;

  static {
    System.load(System.getProperty("cacheexploration.columnSlice.library"));
  }
  private static native Object[] describe(byte[] metadata, long address, long length);

  private static void require(boolean condition, String message) {
    if (!condition) throw new AssertionError(message);
  }

  private static int type(int column) {
    return column == 1 || column == 35 ? 3 : (column >= 32 && column <= 34 || column == 37) ? 23 : 4;
  }
  private static boolean isNull(int column, int row) {
    if (column == 33 || column == 35 || column == 36) return true;
    if (column == 0 || column == 34 || column == 37) return false;
    if (column == 32) return row % 17 == 0;
    return row % (11 + column % 5) == 0;
  }
  private static long number(int column, int row) {
    if (column == 0) return row * 17L - 991;
    if (column == 1) return (row * 37L) % 1009;
    return column % 3 == 0 ? row % (column + 7L) :
        column % 3 == 1 ? row * (column + 3L) :
        Long.rotateLeft(row * 0x9e3779b97f4a7c15L, column);
  }
  private static String text(int column, int row) {
    return column == 34 || column == 37 || row % 19 == 0 ? "" : "µ-雪-" + row;
  }

  private static Table fixture() {
    try (CloseableArray<ColumnVector> columns =
        CloseableArray.wrap(new ColumnVector[COLUMNS])) {
      for (int c = 0; c < COLUMNS; c++) {
        if (type(c) == 23) {
          String[] values = new String[ROWS];
          for (int r = 0; r < ROWS; r++) values[r] = isNull(c, r) ? null : text(c, r);
          columns.set(c, ColumnVector.fromStrings(values));
        } else if (type(c) == 3) {
          Integer[] values = new Integer[ROWS];
          for (int r = 0; r < ROWS; r++) values[r] = isNull(c, r) ? null : (int) number(c, r);
          columns.set(c, ColumnVector.fromBoxedInts(values));
        } else {
          Long[] values = new Long[ROWS];
          for (int r = 0; r < ROWS; r++) values[r] = isNull(c, r) ? null : number(c, r);
          columns.set(c, ColumnVector.fromBoxedLongs(values));
        }
      }
      return new Table(columns.getArray());
    }
  }

  private static byte[] download(DeviceMemoryBuffer input) {
    int length = Math.toIntExact(input.getLength());
    byte[] bytes = new byte[length];
    try (HostMemoryBuffer host = HostMemoryBuffer.allocate(length)) {
      host.copyFromDeviceBuffer(input, Cuda.DEFAULT_STREAM);
      host.getBytes(bytes, 0, 0, length);
    }
    return bytes;
  }

  private static DeviceMemoryBuffer upload(byte[] bytes) {
    try (CloseableArray<DeviceMemoryBuffer> owned =
        CloseableArray.wrap(new DeviceMemoryBuffer[1]);
         HostMemoryBuffer host = HostMemoryBuffer.allocate(bytes.length)) {
      host.setBytes(0, bytes, 0, bytes.length);
      owned.set(0, DeviceMemoryBuffer.allocate(bytes.length));
      owned.get(0).copyFromHostBuffer(host, Cuda.DEFAULT_STREAM);
      return owned.release()[0];
    }
  }

  private static PackedSegments.Segment[] intersect(
      PackedSegments.Segment[] source, long start, long length) {
    List<Long> offsets = new ArrayList<>();
    List<Long> sizes = new ArrayList<>();
    List<Integer> bits = new ArrayList<>();
    for (PackedSegments.Segment s : source) {
      long begin = Math.max(start, s.offset);
      long end = Math.min(start + length, s.offset + s.length);
      if (end > begin) {
        offsets.add(begin - start);
        sizes.add(end - begin);
        bits.add(s.typeBits);
      }
    }
    return PackedTableCodec.layout(offsets.stream().mapToLong(Long::longValue).toArray(),
        sizes.stream().mapToLong(Long::longValue).toArray(),
        bits.stream().mapToInt(Integer::intValue).toArray());
  }

  private static final class Cache {
    final byte[][] metadata = new byte[2 * COLUMNS][];
    final byte[][] raw = new byte[2 * COLUMNS][];
    final byte[][][] payloads = new byte[2 * COLUMNS][][];
    final PackedSegments.Segment[][] layouts = new PackedSegments.Segment[2 * COLUMNS][];
    final int[] rows = {16384, ROWS - 16384};
    long fullBytes;
    long fullRaw;
    int encodeCalls;
    long encodeBlocks;
    long wholeBytes;
    long wholeBlocks;
    int wholeCalls;
    long columnMetadataBytes;
    long wholeMetadataBytes;
  }

  private static Cache encode(String codec) {
    Cache result = new Cache();
    try (Table source = fixture();
         CloseableArray<ContiguousTable> parts = CloseableArray.wrap(source.contiguousSplit(16384));
         CloseableArray<DeviceMemoryBuffer> slices =
             CloseableArray.wrap(new DeviceMemoryBuffer[2 * COLUMNS])) {
      DeviceMemoryBuffer[] wholeBuffers = new DeviceMemoryBuffer[parts.size()];
      byte[][] wholeMetadata = new byte[parts.size()][];
      PackedSegments.Segment[][] wholeLayouts = new PackedSegments.Segment[parts.size()][];
      for (int p = 0; p < parts.size(); p++) {
        ContiguousTable packed = parts.get(p);
        wholeBuffers[p] = packed.getBuffer();
        ByteBuffer meta = packed.getMetadataDirectBuffer().duplicate();
        wholeMetadata[p] = new byte[meta.remaining()];
        meta.get(wholeMetadata[p]);
        result.wholeMetadataBytes += wholeMetadata[p].length;
        wholeLayouts[p] = codec.equals("cascaded-typed") ?
            PackedSegments.describe(packed, true) : PackedTableCodec.layout(
                new long[]{0}, new long[]{packed.getBuffer().getLength()}, new int[]{8});
      }
      try (NvtxRange range = new NvtxRange("whole-record-compress:" + codec, NvtxColor.CYAN);
           PackedTableCodec.EncodedBatch baseline = PackedTableCodec.compress(wholeBuffers,
               wholeMetadata, wholeLayouts, codec, CHUNK, Cuda.DEFAULT_STREAM)) {
        result.wholeCalls = baseline.calls.length;
        for (PackedTableCodec.Call call : baseline.calls) result.wholeBlocks += call.blocks;
        for (PackedTableCodec.EncodedTable table : baseline.tables) {
          for (DeviceMemoryBuffer buffer : table.payloads) result.wholeBytes += buffer.getLength();
        }
      }
      for (int p = 0; p < parts.size(); p++) {
        ContiguousTable packed = parts.get(p);
        PackedSegments.Segment[] whole = codec.equals("cascaded-typed") ?
            PackedSegments.describe(packed, true) : PackedTableCodec.layout(
                new long[]{0}, new long[]{packed.getBuffer().getLength()}, new int[]{8});
        try (NvtxRange range = new NvtxRange("column-index", NvtxColor.CYAN)) {
          Object[] index = describe(wholeMetadata[p],
              packed.getBuffer().getAddress(), packed.getBuffer().getLength());
          long[] spans = (long[]) index[0];
          byte[][] metadata = (byte[][]) index[1];
          require(spans.length == COLUMNS * 2 && metadata.length == COLUMNS, "Index shape");
          long total = 0;
          for (int c = 0; c < COLUMNS; c++) {
            int i = p * COLUMNS + c;
            long start = spans[c * 2], length = spans[c * 2 + 1];
            slices.set(i, packed.getBuffer().slice(start, length));
            require(slices.get(i).getAddress() == packed.getBuffer().getAddress() + start,
                "Slice must retain original device storage");
            result.metadata[i] = metadata[c];
            result.columnMetadataBytes += metadata[c].length;
            result.layouts[i] = intersect(whole, start, length);
            total += length;
          }
          require(total == packed.getBuffer().getLength(), "Spans must cover packed bytes");
        }
      }
      // Reference-only copies for byte-exact checks and CPU codec input. Not encoder requirements.
      try (NvtxRange range = new NvtxRange("reference-download", NvtxColor.CYAN)) {
        for (int i = 0; i < slices.size(); i++) result.raw[i] = download(slices.get(i));
      }
      try (NvtxRange range = new NvtxRange("batched-column-compress:" + codec, NvtxColor.CYAN);
           PackedTableCodec.EncodedBatch encoded = PackedTableCodec.compress(slices.getArray(),
               result.metadata, result.layouts, codec, CHUNK, Cuda.DEFAULT_STREAM)) {
        result.encodeCalls = encoded.calls.length;
        for (PackedTableCodec.Call call : encoded.calls) result.encodeBlocks += call.blocks;
        for (int i = 0; i < encoded.tables.length; i++) {
          DeviceMemoryBuffer[] buffers = encoded.tables[i].payloads;
          result.payloads[i] = new byte[buffers.length][];
          for (int j = 0; j < buffers.length; j++) {
            result.payloads[i][j] = download(buffers[j]);
            result.fullBytes += result.payloads[i][j].length;
          }
          result.fullRaw += result.raw[i].length;
        }
      }
    }
    require(Rmm.getTotalBytesAllocated() == 0, "Encoder leaked GPU allocation");
    return result;
  }

  private static int[] unique(int[] requested) {
    return Arrays.stream(requested).distinct().toArray();
  }

  private static void checkHost(HostColumnVector host, int column, int first, int count) {
    for (int r = 0; r < count; r++) {
      int row = first + r;
      require(host.isNull(r) == isNull(column, row), "GPU null/order mismatch");
      if (!isNull(column, row)) {
        if (type(column) == 23) require(host.getJavaString(r).equals(text(column, row)), "GPU UTF8");
        else if (type(column) == 3) require(host.getInt(r) == (int) number(column, row), "GPU int");
        else require(host.getLong(r) == number(column, row), "GPU long");
      }
    }
  }

  private static void gpuRead(Cache cache, String codec, int[] requested, boolean cpuEncoded) throws Exception {
    int[] selected = unique(requested);
    if (selected.length == 0) {
      require(cache.rows[0] + cache.rows[1] == ROWS, "Count-only rows");
      require(Rmm.getTotalBytesAllocated() == 0, "Count-only allocated data");
      System.out.println("{\"codec\":\"" + codec + "\",\"selection\":\"count-only\"," +
          "\"uploaded_bytes\":0,\"decoded_bytes\":0,\"native_calls\":0}");
      return;
    }
    int count = selected.length * 2;
    PackedSegments.Segment[][] layouts = new PackedSegments.Segment[count][];
    byte[][] metadata = new byte[count][];
    long[] lengths = new long[count];
    DeviceMemoryBuffer[][] inputs = new DeviceMemoryBuffer[count][];
    long uploaded = 0, decodedBytes = 0;
    List<byte[][]> hostPayloads = new ArrayList<>();
    int uploadCount = 0;
    for (int p = 0; p < 2; p++) {
      for (int c : selected) uploadCount += cpuEncoded ? 1 : cache.payloads[p * COLUMNS + c].length;
    }
    try (CpuPackedCodec cpu = cpuEncoded ? new CpuPackedCodec(codec, CHUNK) : null;
         CloseableArray<DeviceMemoryBuffer> uploads =
             CloseableArray.wrap(new DeviceMemoryBuffer[uploadCount])) {
      int nextUpload = 0;
      for (int p = 0; p < 2; p++) {
        for (int c = 0; c < selected.length; c++) {
          int index = p * COLUMNS + selected[c], out = p * selected.length + c;
          lengths[out] = cache.raw[index].length;
          metadata[out] = cache.metadata[index];
          layouts[out] = cpuEncoded ? PackedTableCodec.layout(
              new long[]{0}, new long[]{lengths[out]}, new int[]{8}) : cache.layouts[index];
          byte[][] payloads = cpuEncoded ?
              new byte[][]{cpu.encode(cache.raw[index], 0, cache.raw[index].length)} :
              cache.payloads[index];
          hostPayloads.add(payloads);
          uploaded += Arrays.stream(payloads).mapToLong(bytes -> bytes.length).sum();
          decodedBytes += lengths[out];
        }
      }
      PackedTableCodec.DecodedBatch decoded;
      try (NvtxRange range = new NvtxRange("selected-decode:" + codec + ":" +
          selected.length + ":cpuEncoded=" + cpuEncoded, NvtxColor.CYAN)) {
        for (int i = 0; i < count; i++) {
          byte[][] payloads = hostPayloads.get(i);
          inputs[i] = new DeviceMemoryBuffer[payloads.length];
          for (int j = 0; j < payloads.length; j++) {
            require(nextUpload < uploads.size(), "Probe upload owner capacity");
            uploads.set(nextUpload, upload(payloads[j]));
            inputs[i][j] = uploads.get(nextUpload++);
          }
        }
        decoded = PackedTableCodec.decompress(inputs, layouts,
            lengths, codec, CHUNK, 1024L * 1024 * 1024, Cuda.DEFAULT_STREAM);
      }
      try (PackedTableCodec.DecodedBatch output = decoded) {
          for (int p = 0; p < 2; p++) {
            try (CloseableArray<Table> columns = CloseableArray.wrap(new Table[selected.length])) {
              for (int c = 0; c < selected.length; c++) {
                int i = p * selected.length + c;
                require(Arrays.equals(cache.raw[p * COLUMNS + selected[c]],
                    download(output.packedOutputs[i])), "Packed byte mismatch");
                columns.set(c, Table.fromPackedTable(ByteBuffer.wrap(metadata[i]),
                    output.packedOutputs[i]));
              }
              ColumnVector[] projected = Arrays.stream(requested).mapToObj(column -> {
                int pos = 0;
                while (selected[pos] != column) pos++;
                return columns.get(pos).getColumn(0);
              }).toArray(ColumnVector[]::new);
              long before = Rmm.getTotalBytesAllocated();
              Table joined;
              try (NvtxRange assembled = new NvtxRange("assemble-projection", NvtxColor.CYAN)) {
                joined = new Table(projected);
              }
              try (Table table = joined) {
                require(Rmm.getTotalBytesAllocated() == before, "Assembly allocated GPU data");
                for (int c = 0; c < requested.length; c++) {
                  try (HostColumnVector host = table.getColumn(c).copyToHost()) {
                    checkHost(host, requested[c], p == 0 ? 0 : cache.rows[0], cache.rows[p]);
                  }
                }
              }
            }
          }
          System.out.println("{\"codec\":\"" + codec + "\",\"selected_unique\":" +
              selected.length + ",\"requested_ordinals\":" + Arrays.toString(requested) +
              ",\"requested_columns\":" + requested.length +
              ",\"cpu_encoded\":" + cpuEncoded + ",\"uploaded_bytes\":" + uploaded +
              ",\"decoded_bytes\":" + decodedBytes + ",\"full_encoded_bytes\":" +
              cache.fullBytes + ",\"full_raw_bytes\":" + cache.fullRaw +
              ",\"native_calls\":" + output.calls.length +
              ",\"encode_calls\":" + cache.encodeCalls + ",\"encode_blocks\":" +
              cache.encodeBlocks + ",\"whole_encoded_bytes\":" + cache.wholeBytes +
              ",\"whole_encode_calls\":" + cache.wholeCalls +
              ",\"whole_encode_blocks\":" + cache.wholeBlocks +
              ",\"column_metadata_bytes\":" + cache.columnMetadataBytes +
              ",\"whole_metadata_bytes\":" + cache.wholeMetadataBytes + "}");
      }
    }
    require(Rmm.getTotalBytesAllocated() == 0, "Reader leaked GPU allocation");
  }

  private static void cpuRead(Cache cache, String codec, int[] requested) throws Exception {
    try (CpuPackedCodec cpu = new CpuPackedCodec(codec, CHUNK)) {
      for (int p = 0; p < 2; p++) {
        for (int c : unique(requested)) {
          int i = p * COLUMNS + c;
          byte[] raw = new byte[cache.raw[i].length];
          for (int j = 0; j < cache.layouts[i].length; j++) {
            PackedSegments.Segment s = cache.layouts[i][j];
            byte[] piece = cpu.decode(cache.payloads[i][j], 0, cache.payloads[i][j].length,
                Math.toIntExact(s.length));
            System.arraycopy(piece, 0, raw, Math.toIntExact(s.offset), piece.length);
          }
          require(Arrays.equals(raw, cache.raw[i]), "CPU selected byte mismatch");
          long[] d = HostPackedBridge.parse(cache.metadata[i], raw);
          require(d.length == 6 && d[0] == type(c) && d[1] == cache.rows[p], "CPU metadata");
          ByteBuffer bytes = ByteBuffer.wrap(raw).order(ByteOrder.LITTLE_ENDIAN);
          for (int r = 0; r < cache.rows[p]; r++) {
            int row = (p == 0 ? 0 : cache.rows[0]) + r;
            boolean nil = d[4] >= 0 && (raw[(int) d[4] + r / 8] & (1 << (r % 8))) == 0;
            require(nil == isNull(c, row), "CPU null mismatch");
            if (!nil) {
              if (type(c) == 23) {
                int begin = bytes.getInt((int) d[5] + r * 4);
                int end = bytes.getInt((int) d[5] + (r + 1) * 4);
                String value = end == begin ? "" : new String(raw, (int) d[3] + begin,
                    end - begin, StandardCharsets.UTF_8);
                require(value.equals(text(c, row)), "CPU UTF8 mismatch");
              } else if (type(c) == 3) {
                require(bytes.getInt((int) d[3] + r * 4) == (int) number(c, row), "CPU int");
              } else require(bytes.getLong((int) d[3] + r * 8) == number(c, row), "CPU long");
            }
          }
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Rmm.initialize(RmmAllocationMode.CUDA_DEFAULT, null, 0);
    try {
      int[] all = java.util.stream.IntStream.range(0, COLUMNS).toArray();
      int[][] selections = {all, {0}, {32}, {32, 1, 0, 0, 33, 34, 35, 36}, {}};
      for (String codec : new String[]{"uncompressed", "snappy", "lz4", "zstd",
          "ans", "cascaded8", "cascaded-typed"}) {
        Cache cache = encode(codec);
        for (int[] requested : selections) {
          gpuRead(cache, codec, requested, false);
          if (Arrays.asList("uncompressed", "snappy", "lz4", "zstd").contains(codec)) {
            cpuRead(cache, codec, requested);
          }
        }
        if (Arrays.asList("uncompressed", "snappy", "lz4", "zstd").contains(codec)) {
          gpuRead(cache, codec, new int[]{32, 0, 33, 34, 35, 36, 0}, true);
        }
      }
      System.out.println("PASS column-slice feasibility; Spark serializer unchanged");
    } finally {
      try {
        require(Rmm.getTotalBytesAllocated() == 0, "Final RMM allocation leak");
      } finally {
        Rmm.shutdown();
      }
    }
  }
}
