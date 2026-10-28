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

import ai.rapids.cudf.BinaryOp;
import ai.rapids.cudf.CloseableArray;
import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.ColumnView;
import ai.rapids.cudf.ContiguousTable;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.DeviceMemoryBuffer;
import ai.rapids.cudf.HostColumnVector;
import ai.rapids.cudf.Scalar;
import ai.rapids.cudf.Table;
import com.nvidia.spark.rapids.jni.Hash;
import java.util.Optional;

/**
 * Deterministic GPU-generated inputs, not byte-identical replicas of the previous Spark fixtures.
 * Generation and validation belong outside codec timing. Their allocations still contribute to
 * whole-process/GPU high-water measurements. No generator uses a CPU array proportional to rows.
 */
public final class PackedFixtures {
  private static final long BYTE_CHECK_SLICE = 64L * 1024 * 1024;
  private static final int LOW_PERIOD = 32 * 17;
  private static final int[] LOW_PREFIX = new int[LOW_PERIOD + 1];
  static {
    for (int row = 0; row < LOW_PERIOD; row++) {
      LOW_PREFIX[row + 1] = LOW_PREFIX[row] + (row % 17 == 0 ? 0
          : "group-".length() + Integer.toString(row % 32).length());
    }
  }

  private PackedFixtures() {}

  private static void checkFixture(String fixture) {
    if (!fixture.equals("fixed-wide") && !fixture.equals("strings-low")
        && !fixture.equals("strings-wide")) {
      throw new IllegalArgumentException("Unknown fixture: " + fixture);
    }
  }

  private static ColumnVector op(ColumnView input, BinaryOp operation, long value) {
    try (Scalar scalar = Scalar.fromLong(value)) {
      return input.binaryOp(operation, scalar, DType.INT64);
    }
  }

  private static ColumnVector nullable(ColumnView values, ColumnView index, int modulus, int shift) {
    try (ColumnVector shifted = op(index, BinaryOp.ADD, shift);
         ColumnVector remainder = op(shifted, BinaryOp.MOD, modulus);
         Scalar zero = Scalar.fromLong(0);
         ColumnVector isNull = remainder.binaryOp(BinaryOp.EQUAL, zero, DType.BOOL8);
         Scalar missing = Scalar.fromNull(values.getType());
         ColumnVector masked = isNull.ifElse(missing, values)) {
      if (values.getType().equals(DType.STRING)) {
        // cuDF requires empty string payloads under nulls; ifElse already provides that layout.
        masked.incRefCount();
        return masked;
      }
      try (ColumnView deterministic = new ColumnView(values.getType(), values.getRowCount(),
               Optional.of(masked.getNullCount()), values.getData(), masked.getValid())) {
        // Keep deterministic fixed-width payloads under nulls. Otherwise allocator history
        // changes compression results despite equal values. Validity still marks the nulls.
        return deterministic.copyToColumnVector();
      }
    }
  }

  /**
   * Caller owns the returned table. Column order is id(long), key(int), then either 30 long
   * columns in monotonic/low-cardinality/hash triplets, or one nullable UTF-8 string column.
   * Row index x is firstRow + row; index must be nonnegative and fit the arithmetic below.
   * Hash triplet i is Spark JNI xxhash64(seed=1000+i, [x]), deliberately labeled as this fixture's
   * own formula rather than equating it to an older SQL expression with an extra seed column.
   */
  public static Table create(String fixture, int rows, long firstRow) {
    checkFixture(fixture);
    checkRange(firstRow, rows);
    int columns = fixture.equals("fixed-wide") ? 32 : 3;
    try (Scalar start = Scalar.fromLong(firstRow);
         Scalar step = Scalar.fromLong(1);
         ColumnVector index = ColumnVector.sequence(start, step, rows);
         CloseableArray<ColumnVector> output =
             CloseableArray.wrap(new ColumnVector[columns])) {
      try (ColumnVector multiplied = op(index, BinaryOp.MUL, 17)) {
        output.set(0, op(multiplied, BinaryOp.SUB, 991));
      }
      try (ColumnVector multiplied = op(index, BinaryOp.MUL, 37);
           ColumnVector remainder = op(multiplied, BinaryOp.MOD, 1009);
           ColumnVector ints = remainder.castTo(DType.INT32)) {
        output.set(1, nullable(ints, index, 13, 0));
      }
      if (fixture.equals("fixed-wide")) {
        for (int i = 0; i < 10; i++) {
          try (ColumnVector multiplied = op(index, BinaryOp.MUL, 3 + i);
               ColumnVector values = op(multiplied, BinaryOp.ADD, 101L * i)) {
            output.set(2 + 3 * i, nullable(values, index, 19, i));
          }
          try (ColumnVector shifted = op(index, BinaryOp.ADD, 7L * i);
               ColumnVector values = op(shifted, BinaryOp.MOD, 16 + 8L * i)) {
            output.set(3 + 3 * i, nullable(values, index, 23, i));
          }
          try (ColumnVector hashed = Hash.xxhash64(1000L + i, new ColumnView[] {index})) {
            output.set(4 + 3 * i, nullable(hashed, index, 29, i));
          }
        }
      } else {
        try (ColumnVector valueIndex = fixture.equals("strings-low")
                 ? op(index, BinaryOp.MOD, 32) : op(index, BinaryOp.ADD, 0);
             ColumnVector digits = valueIndex.asStrings();
             Scalar prefix = Scalar.fromString(fixture.equals("strings-low") ? "group-" : "row-");
             Scalar suffix = Scalar.fromString(
                 fixture.equals("strings-low") ? "" : "-varying-value");
             ColumnVector prefixes = ColumnVector.fromScalar(prefix, rows);
             ColumnVector suffixes = ColumnVector.fromScalar(suffix, rows);
             ColumnVector values = ColumnVector.stringConcatenate(
                 new ColumnView[] {prefixes, digits, suffixes})) {
          output.set(2, nullable(values, index, 17, 0));
        }
      }
      // Table increments each input column's reference; the local owners close on return.
      return new Table(output.getArray());
    }
  }

  private static void checkRange(long firstRow, long rows) {
    if (firstRow < 0 || rows < 0 || rows > Integer.MAX_VALUE
        || firstRow > Long.MAX_VALUE / 64 - rows) {
      throw new IllegalArgumentException("Fixture row range is negative or too large");
    }
  }

  /**
   * Canonical logical-layout ESTIMATE, not packed allocation/storage size: fixed-width slots
   * include null rows, plus ceil(rows/8) validity bytes for EVERY schema field, including id.
   * Strings add 4*(rows+1) offsets and exact non-null ASCII payload bytes, with no padding.
   * Thus fixed-wide is 252*rows + 32*ceil(rows/8), exactly 256 B/row for multiples of eight.
   */
  public static long canonicalLogicalBytes(String fixture, long firstRow, long rows) {
    checkFixture(fixture);
    checkRange(firstRow, rows);
    long validity = (rows + 7) / 8;
    if (fixture.equals("fixed-wide")) return Math.addExact(252 * rows, 32 * validity);
    long payload;
    long end = firstRow + rows;
    if (fixture.equals("strings-low")) {
      payload = lowPrefix(end) - lowPrefix(firstRow);
    } else {
      long nonNull = rows - multiples(firstRow, end, 17);
      payload = ("row-".length() + "-varying-value".length()) * nonNull;
      long lower = 0;
      long upper = 10;
      for (int digits = 1; lower < end; digits++) {
        long begin = Math.max(firstRow, lower);
        long stop = Math.min(end, upper);
        if (begin < stop) {
          payload += digits * (stop - begin - multiples(begin, stop, 17));
        }
        lower = upper;
        upper = upper > Long.MAX_VALUE / 10 ? Long.MAX_VALUE : upper * 10;
      }
    }
    return Math.addExact(Math.addExact(12 * rows, 3 * validity),
        Math.addExact(4 * (rows + 1), payload));
  }

  private static long multiples(long first, long end, long divisor) {
    if (first == end) return 0;
    return Math.floorDiv(end - 1, divisor) - Math.floorDiv(first - 1, divisor);
  }

  private static long lowPrefix(long end) {
    return (end / LOW_PERIOD) * LOW_PREFIX[LOW_PERIOD] + LOW_PREFIX[(int) (end % LOW_PERIOD)];
  }

  /**
   * Largest multiple of eight rows whose canonical estimate starting at zero fits the target.
   * The caller must record canonicalLogicalBytes at its actual firstRow: unique string indices
   * in later work items can add decimal digits, so actual represented bytes may differ slightly.
   * This model includes offsets even for an empty string column; packed bytes remain separate.
   */
  public static long rowsForTask(String fixture, int taskMiB) {
    checkFixture(fixture);
    if (taskMiB <= 0 || taskMiB > 2048) throw new IllegalArgumentException("Task MiB in 1..2048");
    long target = taskMiB * 1024L * 1024;
    long low = 0;
    long high = Integer.MAX_VALUE / 8;
    while (low < high) {
      long middle = (low + high + 1) / 2;
      if (canonicalLogicalBytes(fixture, 0, middle * 8) <= target) low = middle;
      else high = middle - 1;
    }
    return low * 8;
  }

  /** Exact GPU value/null/order comparison; boolean reduction synchronizes one scalar per column. */
  public static void validate(Table expected, Table actual) {
    if (expected.getRowCount() != actual.getRowCount()
        || expected.getNumberOfColumns() != actual.getNumberOfColumns()) {
      throw new AssertionError("Table shape mismatch");
    }
    for (int c = 0; c < expected.getNumberOfColumns(); c++) {
      ColumnView left = expected.getColumn(c);
      ColumnView right = actual.getColumn(c);
      if (!left.getType().equals(right.getType()) || left.getNullCount() != right.getNullCount()) {
        throw new AssertionError("Column type/null count mismatch at " + c);
      }
      if (expected.getRowCount() == 0) continue;
      try (ColumnVector equal = left.binaryOp(BinaryOp.NULL_EQUALS, right, DType.BOOL8);
           Scalar all = equal.all()) {
        if (!all.isValid() || !all.getBoolean()) {
          throw new AssertionError("Column value/null/order mismatch at " + c);
        }
      }
    }
  }

  /**
   * Exact GPU byte comparison, including padding, in at most 64 MiB slices. The expected and
   * actual packed buffers must stay alive; temporary comparison columns affect observed peaks.
   * No full payload is copied to host. Zero bytes is a valid equal payload.
   */
  public static void validatePacked(DeviceMemoryBuffer expected, DeviceMemoryBuffer actual) {
    if (expected.getLength() != actual.getLength()) throw new AssertionError("Packed length mismatch");
    for (long offset = 0; offset < expected.getLength(); offset += BYTE_CHECK_SLICE) {
      int length = (int) Math.min(BYTE_CHECK_SLICE, expected.getLength() - offset);
      try (DeviceMemoryBuffer left = expected.slice(offset, length);
           DeviceMemoryBuffer right = actual.slice(offset, length);
           ColumnView a = new ColumnView(DType.INT8, length, Optional.of(0L), left, null);
           ColumnView b = new ColumnView(DType.INT8, length, Optional.of(0L), right, null);
           ColumnVector same = a.binaryOp(BinaryOp.EQUAL, b, DType.BOOL8);
           Scalar all = same.all()) {
        if (!all.isValid() || !all.getBoolean()) {
          throw new AssertionError("Packed bytes differ in slice starting " + offset);
        }
      }
    }
  }

  /** Small correctness-only probe; host copies here are bounded to 513 rows and never timed. */
  public static void main(String[] ignored) {
    for (String fixture : new String[] {"fixed-wide", "strings-low", "strings-wide"}) {
      try (Table input = create(fixture, 513, 7);
           Table reference = create(fixture, 513, 7);
           CloseableArray<ContiguousTable> packed =
               CloseableArray.wrap(input.contiguousSplit())) {
        validate(reference, input);
        ContiguousTable contiguous = packed.get(0);
        try (Table unpacked = Table.fromPackedTable(
                 contiguous.getMetadataDirectBuffer(), contiguous.getBuffer());
             DeviceMemoryBuffer copy =
                 contiguous.getBuffer().sliceWithCopy(0, contiguous.getBuffer().getLength());
             HostColumnVector id = input.getColumn(0).copyToHost();
             HostColumnVector key = input.getColumn(1).copyToHost()) {
          validate(reference, unpacked);
          validatePacked(contiguous.getBuffer(), copy);
          for (int row = 0; row < 513; row++) {
            long index = 7L + row;
            if (id.getLong(row) != index * 17 - 991 || key.isNull(row) != (index % 13 == 0)
                || (!key.isNull(row) && key.getInt(row) != index * 37 % 1009)) {
              throw new AssertionError("Known id/key formula");
            }
          }
        }
        if (fixture.equals("fixed-wide")) {
          for (int i = 0; i < 10; i++) {
            for (int kind = 0; kind < 3; kind++) {
              try (HostColumnVector column = input.getColumn(2 + 3 * i + kind).copyToHost()) {
                for (int row = 0; row < 513; row++) {
                  long index = 7L + row;
                  int divisor = kind == 0 ? 19 : kind == 1 ? 23 : 29;
                  if (column.isNull(row) != ((index + i) % divisor == 0)) {
                    throw new AssertionError("Known wide null pattern");
                  }
                  if (!column.isNull(row) && kind != 2) {
                    long wanted = kind == 0 ? index * (3 + i) + 101L * i
                        : (index + 7L * i) % (16 + 8L * i);
                    if (column.getLong(row) != wanted) throw new AssertionError("Known wide value");
                  }
                }
              }
            }
          }
        } else {
          try (HostColumnVector strings = input.getColumn(2).copyToHost()) {
            long payload = 0;
            for (int row = 0; row < 513; row++) {
              long index = 7L + row;
              String wanted = index % 17 == 0 ? null : fixture.equals("strings-low")
                  ? "group-" + index % 32 : "row-" + index + "-varying-value";
              String found = strings.isNull(row) ? null : strings.getJavaString(row);
              if (!java.util.Objects.equals(wanted, found)) throw new AssertionError("Known strings");
              if (found != null) payload += found.length(); // Fixtures contain ASCII only.
            }
            long logical = 12L * 513 + 3 * ((513 + 7) / 8) + 4L * 514 + payload;
            if (logical != canonicalLogicalBytes(fixture, 7, 513)) {
              throw new AssertionError("Canonical string payload accounting");
            }
          }
        }
        System.out.println("PASS " + fixture + " rows=513 canonical="
            + canonicalLogicalBytes(fixture, 7, 513));
      }
    }
    try (ColumnVector nulls = ColumnVector.fromBoxedLongs(null, null, null);
         ColumnVector values = ColumnVector.fromLongs(1, 2, 3);
         Table input = new Table(nulls, values);
         ColumnVector nullSlice = nulls.subVector(1, 3);
         ColumnVector validSlice = values.subVector(1, 3);
         Table sliced = new Table(nullSlice, validSlice);
         ColumnVector expectedNulls = ColumnVector.fromBoxedLongs(null, null);
         ColumnVector expectedValues = ColumnVector.fromLongs(2, 3);
         Table expected = new Table(expectedNulls, expectedValues)) {
      validate(input, input);
      validate(expected, sliced);
    }
    if (rowsForTask("fixed-wide", 2048) != 8388608
        || canonicalLogicalBytes("fixed-wide", 0, 8388608) != 2147483648L) {
      throw new AssertionError("Canonical fixed-wide sizing");
    }
    System.out.println("PASS all-null/all-valid/sliced tails; no performance claim");
  }
}
