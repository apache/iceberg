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

import java.util.List;
import org.apache.iceberg.arrow.vectorized.NullabilityHolder;
import org.apache.iceberg.arrow.vectorized.VectorHolder;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

public class StructColumnVector extends ColumnVector {
  private final ColumnVector[] children;
  private final NullabilityHolder nullabilityHolder;

  StructColumnVector(VectorHolder.StructVectorHolder holder) {
    super(SparkSchemaUtil.convert(holder.icebergType()));
    List<VectorHolder> childHolders = holder.childHolders();
    int numRows = holder.numValues();
    ColumnVectorBuilder builder = new ColumnVectorBuilder();
    this.children = new ColumnVector[childHolders.size()];
    for (int idx = 0; idx < childHolders.size(); idx++) {
      children[idx] = builder.build(childHolders.get(idx), numRows);
    }

    this.nullabilityHolder = holder.nullabilityHolder();
  }

  @Override
  public void close() {
    for (ColumnVector child : children) {
      child.close();
    }
  }

  @Override
  public void closeIfFreeable() {
    // See SPARK-50235, SPARK-50463
  }

  @Override
  public boolean hasNull() {
    return nullabilityHolder != null && nullabilityHolder.hasNulls();
  }

  @Override
  public int numNulls() {
    return nullabilityHolder == null ? 0 : nullabilityHolder.numNulls();
  }

  @Override
  public boolean isNullAt(int rowId) {
    return nullabilityHolder != null && nullabilityHolder.isNullAt(rowId) == 1;
  }

  @Override
  public ColumnVector getChild(int ordinal) {
    return children[ordinal];
  }

  @Override
  public boolean getBoolean(int rowId) {
    throw unsupported();
  }

  @Override
  public byte getByte(int rowId) {
    throw unsupported();
  }

  @Override
  public short getShort(int rowId) {
    throw unsupported();
  }

  @Override
  public int getInt(int rowId) {
    throw unsupported();
  }

  @Override
  public long getLong(int rowId) {
    throw unsupported();
  }

  @Override
  public float getFloat(int rowId) {
    throw unsupported();
  }

  @Override
  public double getDouble(int rowId) {
    throw unsupported();
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    throw unsupported();
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    throw unsupported();
  }

  @Override
  public byte[] getBinary(int rowId) {
    throw unsupported();
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    throw unsupported();
  }

  @Override
  public ColumnarMap getMap(int rowId) {
    throw unsupported();
  }

  private UnsupportedOperationException unsupported() {
    return new UnsupportedOperationException("Struct column only supports getChild()");
  }
}
