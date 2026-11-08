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
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

class ConstantColumnVector extends ColumnVector {

  private final Type icebergType;
  private final Object constant;
  private final int batchSize;

  ConstantColumnVector(Type icebergType, int batchSize, Object constant) {
    // the type may be unknown for NULL vectors
    super(icebergType != null ? SparkSchemaUtil.convert(icebergType) : null);
    this.icebergType = icebergType;
    this.constant = constant;
    this.batchSize = batchSize;
  }

  @Override
  public void close() {}

  @Override
  public boolean hasNull() {
    return constant == null;
  }

  @Override
  public int numNulls() {
    return constant == null ? batchSize : 0;
  }

  @Override
  public boolean isNullAt(int rowId) {
    return constant == null;
  }

  @Override
  public boolean getBoolean(int rowId) {
    return (boolean) constant;
  }

  @Override
  public byte getByte(int rowId) {
    return (byte) constant;
  }

  @Override
  public short getShort(int rowId) {
    return (short) constant;
  }

  @Override
  public int getInt(int rowId) {
    return (int) constant;
  }

  @Override
  public long getLong(int rowId) {
    return (long) constant;
  }

  @Override
  public float getFloat(int rowId) {
    return (float) constant;
  }

  @Override
  public double getDouble(int rowId) {
    return (double) constant;
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    throw new UnsupportedOperationException(this.getClass() + " does not implement getArray");
  }

  @Override
  public ColumnarMap getMap(int ordinal) {
    throw new UnsupportedOperationException(this.getClass() + " does not implement getMap");
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    return (Decimal) constant;
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    return (UTF8String) constant;
  }

  @Override
  public byte[] getBinary(int rowId) {
    return (byte[]) constant;
  }

  @Override
  public ColumnVector getChild(int ordinal) {
    InternalRow constantAsRow = (InternalRow) constant;
    Object childConstant = constantAsRow.get(ordinal, childType(ordinal));
    return new ConstantColumnVector(childIcebergType(ordinal), batchSize, childConstant);
  }

  private Type childIcebergType(int ordinal) {
    Types.StructType icebergTypeAsStruct = (Types.StructType) icebergType;
    return icebergTypeAsStruct.fields().get(ordinal).type();
  }

  private DataType childType(int ordinal) {
    StructType typeAsStruct = (StructType) type;
    return typeAsStruct.fields()[ordinal].dataType();
  }
}
