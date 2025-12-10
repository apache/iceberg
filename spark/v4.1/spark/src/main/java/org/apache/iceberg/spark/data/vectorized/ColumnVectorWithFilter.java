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

import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A column vector implementation that applies row-level filtering.
 *
 * <p>This class wraps an existing column vector and uses a row ID mapping array to remap row
 * indices during data access. Each method that retrieves data for a specific row translates the
 * provided row index using the mapping array, effectively filtering the original data to only
 * expose the live subset of rows. This approach allows efficient row-level filtering without
 * modifying the underlying data.
 */
public class ColumnVectorWithFilter extends ColumnVector {
  private final ColumnVector delegate;
  private final int[] rowIdMapping;
  private volatile ColumnVectorWithFilter[] children = null;

  public ColumnVectorWithFilter(ColumnVector delegate, int[] rowIdMapping) {
    super(delegate.dataType());
    this.delegate = delegate;
    this.rowIdMapping = rowIdMapping;
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public void closeIfFreeable() {
    delegate.closeIfFreeable();
  }

  @Override
  public boolean hasNull() {
    return delegate.hasNull();
  }

  @Override
  public int numNulls() {
    // computing the actual number of nulls with rowIdMapping is expensive
    // it is OK to overestimate and return the number of nulls in the original vector
    return delegate.numNulls();
  }

  @Override
  public boolean isNullAt(int rowId) {
    return delegate.isNullAt(rowIdMapping[rowId]);
  }

  @Override
  public boolean getBoolean(int rowId) {
    return delegate.getBoolean(rowIdMapping[rowId]);
  }

  @Override
  public byte getByte(int rowId) {
    return delegate.getByte(rowIdMapping[rowId]);
  }

  @Override
  public short getShort(int rowId) {
    return delegate.getShort(rowIdMapping[rowId]);
  }

  @Override
  public int getInt(int rowId) {
    return delegate.getInt(rowIdMapping[rowId]);
  }

  @Override
  public long getLong(int rowId) {
    return delegate.getLong(rowIdMapping[rowId]);
  }

  @Override
  public float getFloat(int rowId) {
    return delegate.getFloat(rowIdMapping[rowId]);
  }

  @Override
  public double getDouble(int rowId) {
    return delegate.getDouble(rowIdMapping[rowId]);
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    return delegate.getArray(rowIdMapping[rowId]);
  }

  @Override
  public ColumnarMap getMap(int rowId) {
    return delegate.getMap(rowIdMapping[rowId]);
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    return delegate.getDecimal(rowIdMapping[rowId], precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    return delegate.getUTF8String(rowIdMapping[rowId]);
  }

  @Override
  public byte[] getBinary(int rowId) {
    return delegate.getBinary(rowIdMapping[rowId]);
  }

  @Override
  public ColumnVector getChild(int ordinal) {
    if (children == null) {
      synchronized (this) {
        if (children == null) {
          if (dataType() instanceof StructType) {
            StructType structType = (StructType) dataType();
            this.children = new ColumnVectorWithFilter[structType.length()];
            for (int index = 0; index < structType.length(); index++) {
              children[index] = new ColumnVectorWithFilter(delegate.getChild(index), rowIdMapping);
            }
          } else {
            throw new UnsupportedOperationException("Unsupported nested type: " + dataType());
          }
        }
      }
    }

    return children[ordinal];
  }
}
