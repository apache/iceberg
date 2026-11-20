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
package org.apache.iceberg.spark.data.vectorized;

import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

class RowLineageColumnVector extends ColumnVector {
  private final long defaultValue;
  private final long batchOffsetInFile;
  private final boolean addRowPosition;
  private final int[] selected;
  private final ColumnVector storedValues;

  static RowLineageColumnVector rowId(
      long firstRowId, long batchOffsetInFile, int[] selected, ColumnVector storedValues) {
    return new RowLineageColumnVector(firstRowId, batchOffsetInFile, true, selected, storedValues);
  }

  static RowLineageColumnVector lastUpdatedSequence(
      long fileLastUpdated, ColumnVector storedValues) {
    return new RowLineageColumnVector(fileLastUpdated, 0, false, null, storedValues);
  }

  private RowLineageColumnVector(
      long defaultValue,
      long batchOffsetInFile,
      boolean addRowPosition,
      int[] selected,
      ColumnVector storedValues) {
    super(SparkSchemaUtil.convert(Types.LongType.get()));
    this.defaultValue = defaultValue;
    this.batchOffsetInFile = batchOffsetInFile;
    this.addRowPosition = addRowPosition;
    this.selected = selected;
    this.storedValues = storedValues;
  }

  @Override
  public long getLong(int rowId) {
    if (storedValues != null && !storedValues.isNullAt(rowId)) {
      return storedValues.getLong(rowId);
    }

    if (addRowPosition) {
      int rowInBatch = selected != null ? selected[rowId] : rowId;
      return defaultValue + batchOffsetInFile + rowInBatch;
    }

    return defaultValue;
  }

  @Override
  public void close() {}

  @Override
  public boolean hasNull() {
    return false;
  }

  @Override
  public int numNulls() {
    return 0;
  }

  @Override
  public boolean isNullAt(int rowId) {
    return false;
  }

  @Override
  public boolean getBoolean(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte getByte(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public short getShort(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getInt(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public float getFloat(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double getDouble(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ColumnarMap getMap(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    throw new UnsupportedOperationException();
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] getBinary(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ColumnVector getChild(int ordinal) {
    throw new UnsupportedOperationException();
  }
}
