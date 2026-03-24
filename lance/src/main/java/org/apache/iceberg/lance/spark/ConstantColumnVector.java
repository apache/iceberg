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
package org.apache.iceberg.lance.spark;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A ColumnVector that returns a constant value for every row. Used for partition columns and
 * metadata columns that have a fixed value across an entire file/batch.
 */
class ConstantColumnVector extends ColumnVector {
  private final Object value;
  private final int numRows;
  private final boolean isNull;

  ConstantColumnVector(DataType sparkType, int numRows, Object value) {
    super(sparkType);
    this.numRows = numRows;
    this.value = value;
    this.isNull = (value == null);
  }

  @Override
  public void close() {}

  @Override
  public boolean hasNull() {
    return isNull;
  }

  @Override
  public int numNulls() {
    return isNull ? numRows : 0;
  }

  @Override
  public boolean isNullAt(int rowId) {
    return isNull;
  }

  @Override
  public boolean getBoolean(int rowId) {
    return (Boolean) value;
  }

  @Override
  public byte getByte(int rowId) {
    return ((Number) value).byteValue();
  }

  @Override
  public short getShort(int rowId) {
    return ((Number) value).shortValue();
  }

  @Override
  public int getInt(int rowId) {
    return ((Number) value).intValue();
  }

  @Override
  public long getLong(int rowId) {
    return ((Number) value).longValue();
  }

  @Override
  public float getFloat(int rowId) {
    return ((Number) value).floatValue();
  }

  @Override
  public double getDouble(int rowId) {
    return ((Number) value).doubleValue();
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    return Decimal.apply((java.math.BigDecimal) value, precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    return UTF8String.fromString(value.toString());
  }

  @Override
  public byte[] getBinary(int rowId) {
    if (value instanceof byte[]) {
      return (byte[]) value;
    }
    return null;
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    throw new UnsupportedOperationException("Array constants not supported");
  }

  @Override
  public ColumnarMap getMap(int rowId) {
    throw new UnsupportedOperationException("Map constants not supported");
  }

  @Override
  public ColumnVector getChild(int ordinal) {
    throw new UnsupportedOperationException("Struct constants not supported");
  }
}
