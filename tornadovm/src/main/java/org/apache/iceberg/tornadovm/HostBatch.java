/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.tornadovm;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.types.Types;
import uk.ac.manchester.tornado.api.types.arrays.ByteArray;
import uk.ac.manchester.tornado.api.types.arrays.DoubleArray;
import uk.ac.manchester.tornado.api.types.arrays.IntArray;
import uk.ac.manchester.tornado.api.types.arrays.LongArray;
import uk.ac.manchester.tornado.cudf.enums.CudfType;

/**
 * The live rows of one data file, copied back from the device: column-major, one typed buffer a
 * physical type, each column at element {@code slot * stride}.
 */
public final class HostBatch implements LiveRows {
  private final ScanLayout layout;
  private final Map<String, Integer> idByName;
  private final int rows;
  private final int stride;
  private final IntArray ints;
  private final LongArray longs;
  private final DoubleArray doubles;
  private final ByteArray valid;
  // Columns with no null in the batch, whose validity bytes were not copied back.
  private final boolean[] nullFree;
  private final int[] bases;
  private final CudfType[] types;
  private final StringColumns strings;

  /**
   * A batch's string columns as the device read them -- offsets and bytes of every row the batch
   * read -- and which of those rows are live, in order.
   */
  record StringColumns(IntArray compacted, IntArray offsets, byte[][] chars, int numericCount) {}

  HostBatch(
      ScanLayout layout,
      Map<String, Integer> idByName,
      int rows,
      int stride,
      IntArray ints,
      LongArray longs,
      DoubleArray doubles,
      ByteArray valid,
      boolean[] nullFree,
      StringColumns strings) {
    this.nullFree = nullFree;
    this.strings = strings;
    this.layout = layout;
    this.idByName = idByName;
    this.rows = rows;
    this.stride = stride;
    this.ints = ints;
    this.longs = longs;
    this.doubles = doubles;
    this.valid = valid;
    this.bases = new int[layout.fields().size()];
    this.types = new CudfType[bases.length];
    for (int c = 0; c < bases.length; c++) {
      int fieldId = layout.fields().get(c).fieldId();
      types[c] = layout.type(fieldId);
      bases[c] = layout.slot(fieldId) * stride;
    }
  }

  @Override
  public int rows() {
    return rows;
  }

  public List<Types.NestedField> columns() {
    return layout.fields();
  }

  /** The column's position in the read, which these accessors take. */
  @Override
  public int column(String name) {
    return layout.readIndex(idByName.get(name));
  }

  private int base(int column, CudfType expected) {
    if (types[column] != expected) {
      throw new IllegalArgumentException(
          layout.fields().get(column).name() + " is " + types[column] + ", not " + expected);
    }
    return bases[column];
  }

  @Override
  public boolean isNull(int column, int row) {
    return layout.anyOptional() && !nullFree[column] && valid.get(column * stride + row) == 0;
  }

  @Override
  public int getInt(int column, int row) {
    return ints.get(base(column, CudfType.INT32) + row);
  }

  @Override
  public long getLong(int column, int row) {
    return longs.get(base(column, CudfType.INT64) + row);
  }

  @Override
  public double getDouble(int column, int row) {
    return doubles.get(base(column, CudfType.FLOAT64) + row);
  }

  @Override
  public String getString(int column, int row) {
    int stringIndex = column - layout.numericCount();
    if (strings == null || stringIndex < 0) {
      throw new IllegalArgumentException(layout.fields().get(column).name() + " is not a string");
    }
    if (isNull(column, row)) {
      return null;
    }
    int source = strings.compacted().get(row);
    int from = strings.offsets().get(stringIndex * (stride + 1) + source);
    int to = strings.offsets().get(stringIndex * (stride + 1) + source + 1);
    return new String(strings.chars()[stringIndex], from, to - from, StandardCharsets.UTF_8);
  }

  // Bulk copies: one native-to-heap copy a column instead of a call a value.

  @Override
  public void copyInts(int column, int[] into) {
    MemorySegment.copy(
        ints.getSegment(),
        ValueLayout.JAVA_INT,
        (long) base(column, CudfType.INT32) * Integer.BYTES,
        into,
        0,
        rows);
  }

  @Override
  public void copyLongs(int column, long[] into) {
    MemorySegment.copy(
        longs.getSegment(),
        ValueLayout.JAVA_LONG,
        (long) base(column, CudfType.INT64) * Long.BYTES,
        into,
        0,
        rows);
  }

  @Override
  public void copyDoubles(int column, double[] into) {
    MemorySegment.copy(
        doubles.getSegment(),
        ValueLayout.JAVA_DOUBLE,
        (long) base(column, CudfType.FLOAT64) * Double.BYTES,
        into,
        0,
        rows);
  }

  @Override
  public boolean copyNulls(int column, boolean[] into) {
    if (!layout.anyOptional() || nullFree[column]) {
      return false;
    }
    byte[] flags = new byte[rows];
    MemorySegment.copy(
        valid.getSegment(), ValueLayout.JAVA_BYTE, (long) column * stride, flags, 0, rows);
    boolean any = false;
    for (int r = 0; r < rows; r++) {
      into[r] = flags[r] == 0;
      any |= into[r];
    }
    return any;
  }

  @Override
  public Utf8Column utf8Column(int column) {
    int stringIndex = column - layout.numericCount();
    if (strings == null || stringIndex < 0) {
      throw new IllegalArgumentException(layout.fields().get(column).name() + " is not a string");
    }
    int[] starts = new int[rows];
    int[] lengths = new int[rows];
    int[] compacted = new int[rows];
    MemorySegment.copy(
        strings.compacted().getSegment(), ValueLayout.JAVA_INT, 0, compacted, 0, rows);
    long base = (long) stringIndex * (stride + 1);
    for (int r = 0; r < rows; r++) {
      int from = strings.offsets().get((int) (base + compacted[r]));
      starts[r] = from;
      lengths[r] = strings.offsets().get((int) (base + compacted[r] + 1)) - from;
    }
    return new Utf8Column(strings.chars()[stringIndex], starts, lengths);
  }

  @Override
  public ByteBuffer getUtf8(int column, int row) {
    int stringIndex = column - layout.numericCount();
    if (strings == null || stringIndex < 0) {
      throw new IllegalArgumentException(layout.fields().get(column).name() + " is not a string");
    }
    if (isNull(column, row)) {
      return null;
    }
    int source = strings.compacted().get(row);
    int from = strings.offsets().get(stringIndex * (stride + 1) + source);
    int to = strings.offsets().get(stringIndex * (stride + 1) + source + 1);
    return ByteBuffer.wrap(strings.chars()[stringIndex], from, to - from);
  }
}
