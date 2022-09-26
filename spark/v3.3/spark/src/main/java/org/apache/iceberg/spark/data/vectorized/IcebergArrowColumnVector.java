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

import org.apache.iceberg.arrow.vectorized.ArrowVectorAccessor;
import org.apache.iceberg.arrow.vectorized.NullabilityHolder;
import org.apache.iceberg.arrow.vectorized.VectorHolder;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Implementation of Spark's {@link ColumnVector} interface. The code for this class is heavily
 * inspired from Spark's {@link ArrowColumnVector} The main difference is in how nullability checks
 * are made in this class by relying on {@link NullabilityHolder} instead of the validity vector in
 * the Arrow vector.
 */
public class IcebergArrowColumnVector extends ColumnVector {

  private final ArrowVectorAccessor<Decimal, UTF8String, ColumnarArray, ArrowColumnVector> accessor;
  private final NullabilityHolder nullabilityHolder;

  public IcebergArrowColumnVector(VectorHolder holder) {
    super(SparkSchemaUtil.convert(holder.icebergType()));
    this.nullabilityHolder = holder.nullabilityHolder();
    this.accessor = ArrowVectorAccessors.getVectorAccessor(holder);
  }

  protected ArrowVectorAccessor<Decimal, UTF8String, ColumnarArray, ArrowColumnVector> accessor() {
    return accessor;
  }

  protected NullabilityHolder nullabilityHolder() {
    return nullabilityHolder;
  }

  @Override
  public void close() {
    accessor.close();
  }

  @Override
  public boolean hasNull() {
    return nullabilityHolder.hasNulls();
  }

  @Override
  public int numNulls() {
    return nullabilityHolder.numNulls();
  }

  @Override
  public boolean isNullAt(int rowId) {
    return nullabilityHolder.isNullAt(rowId) == 1;
  }

  @Override
  public boolean getBoolean(int rowId) {
    return accessor.getBoolean(rowId);
  }

  @Override
  public byte getByte(int rowId) {
    throw new UnsupportedOperationException("Unsupported type - byte");
  }

  @Override
  public short getShort(int rowId) {
    throw new UnsupportedOperationException("Unsupported type - short");
  }

  @Override
  public int getInt(int rowId) {
    return accessor.getInt(rowId);
  }

  @Override
  public long getLong(int rowId) {
    return accessor.getLong(rowId);
  }

  @Override
  public float getFloat(int rowId) {
    return accessor.getFloat(rowId);
  }

  @Override
  public double getDouble(int rowId) {
    return accessor.getDouble(rowId);
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    if (isNullAt(rowId)) {
      return null;
    }
    return accessor.getArray(rowId);
  }

  @Override
  public ColumnarMap getMap(int rowId) {
    throw new UnsupportedOperationException("Unsupported type - map");
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    if (isNullAt(rowId)) {
      return null;
    }
    return accessor.getDecimal(rowId, precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    if (isNullAt(rowId)) {
      return null;
    }
    return accessor.getUTF8String(rowId);
  }

  @Override
  public byte[] getBinary(int rowId) {
    if (isNullAt(rowId)) {
      return null;
    }
    return accessor.getBinary(rowId);
  }

  @Override
  public ArrowColumnVector getChild(int ordinal) {
    return accessor.childColumn(ordinal);
  }

  public ArrowVectorAccessor<Decimal, UTF8String, ColumnarArray, ArrowColumnVector>
      vectorAccessor() {
    return accessor;
  }
}
