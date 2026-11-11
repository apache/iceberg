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
package org.apache.iceberg.tornadovm.spark;

import org.apache.iceberg.tornadovm.LiveRows;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A read-only Spark column over heap arrays a device batch was copied into with one bulk copy a
 * column: ints (INT and DATE), longs (LONG and TIMESTAMP), doubles (DOUBLE, and FLOAT read
 * widened), or UTF-8 strings as one byte array with each row's start and length -- which {@link
 * #getUTF8String} wraps without copying, as Spark's own vectors do.
 */
final class HostColumnVector extends ColumnVector {
  private final int[] ints;
  private final long[] longs;
  private final double[] doubles;
  private final LiveRows.Utf8Column strings;
  private final boolean[] nulls;
  private final int numNulls;

  private HostColumnVector(
      DataType type,
      int[] ints,
      long[] longs,
      double[] doubles,
      LiveRows.Utf8Column strings,
      boolean[] nulls) {
    super(type);
    this.ints = ints;
    this.longs = longs;
    this.doubles = doubles;
    this.strings = strings;
    this.nulls = nulls;
    int count = 0;
    if (nulls != null) {
      for (boolean isNull : nulls) {
        count += isNull ? 1 : 0;
      }
    }
    this.numNulls = count;
  }

  static HostColumnVector ofInts(DataType type, int[] values, boolean[] nulls) {
    return new HostColumnVector(type, values, null, null, null, nulls);
  }

  static HostColumnVector ofLongs(DataType type, long[] values, boolean[] nulls) {
    return new HostColumnVector(type, null, values, null, null, nulls);
  }

  static HostColumnVector ofDoubles(DataType type, double[] values, boolean[] nulls) {
    return new HostColumnVector(type, null, null, values, null, nulls);
  }

  static HostColumnVector ofStrings(DataType type, LiveRows.Utf8Column values, boolean[] nulls) {
    return new HostColumnVector(type, null, null, null, values, nulls);
  }

  @Override
  public void close() {}

  @Override
  public boolean hasNull() {
    return numNulls > 0;
  }

  @Override
  public int numNulls() {
    return numNulls;
  }

  @Override
  public boolean isNullAt(int rowId) {
    return nulls != null && nulls[rowId];
  }

  @Override
  public int getInt(int rowId) {
    return ints[rowId];
  }

  @Override
  public long getLong(int rowId) {
    return longs[rowId];
  }

  @Override
  public float getFloat(int rowId) {
    return (float) doubles[rowId];
  }

  @Override
  public double getDouble(int rowId) {
    return doubles[rowId];
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    return isNullAt(rowId)
        ? null
        : UTF8String.fromBytes(strings.bytes(), strings.starts()[rowId], strings.lengths()[rowId]);
  }

  @Override
  public byte[] getBinary(int rowId) {
    throw new UnsupportedOperationException("Binary columns are not read on the device");
  }

  @Override
  public boolean getBoolean(int rowId) {
    throw new UnsupportedOperationException("Boolean columns are not read on the device");
  }

  @Override
  public byte getByte(int rowId) {
    throw new UnsupportedOperationException("Byte columns are not read on the device");
  }

  @Override
  public short getShort(int rowId) {
    throw new UnsupportedOperationException("Short columns are not read on the device");
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    // A decimal of up to 18 digits, held as its unscaled value at the column's scale.
    return isNullAt(rowId) ? null : Decimal.createUnsafe(longs[rowId], precision, scale);
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    throw new UnsupportedOperationException("Arrays are not read on the device");
  }

  @Override
  public ColumnarMap getMap(int ordinal) {
    throw new UnsupportedOperationException("Maps are not read on the device");
  }

  @Override
  public ColumnVector getChild(int ordinal) {
    throw new UnsupportedOperationException("Structs are not read on the device");
  }
}
