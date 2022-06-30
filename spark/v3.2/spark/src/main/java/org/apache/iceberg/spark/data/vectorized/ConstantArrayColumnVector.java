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

import java.util.Arrays;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

public class ConstantArrayColumnVector extends ConstantColumnVector {

  private final Object[] constantArray;

  public ConstantArrayColumnVector(DataType type, int batchSize, Object[] constantArray) {
    super(type, batchSize, constantArray);
    this.constantArray = constantArray;
  }

  @Override
  public boolean getBoolean(int rowId) {
    return (boolean) constantArray[rowId];
  }

  @Override
  public byte getByte(int rowId) {
    return (byte) constantArray[rowId];
  }

  @Override
  public short getShort(int rowId) {
    return (short) constantArray[rowId];
  }

  @Override
  public int getInt(int rowId) {
    return (int) constantArray[rowId];
  }

  @Override
  public long getLong(int rowId) {
    return (long) constantArray[rowId];
  }

  @Override
  public float getFloat(int rowId) {
    return (float) constantArray[rowId];
  }

  @Override
  public double getDouble(int rowId) {
    return (double) constantArray[rowId];
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    return (Decimal) constantArray[rowId];
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    return (UTF8String) constantArray[rowId];
  }

  @Override
  public byte[] getBinary(int rowId) {
    return (byte[]) constantArray[rowId];
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    return new ColumnarArray(
        new ConstantArrayColumnVector(((ArrayType) type).elementType(), getBatchSize(),
            ((ArrayData) constantArray[rowId]).array()),
        0,
        ((ArrayData) constantArray[rowId]).numElements());
  }

  @Override
  public ColumnarMap getMap(int rowId) {
    ColumnVector keys = new ConstantArrayColumnVector(((MapType) type).keyType(), getBatchSize(),
        ((MapData) constantArray[rowId]).keyArray().array());
    ColumnVector values = new ConstantArrayColumnVector(((MapType) type).valueType(), getBatchSize(),
        ((MapData) constantArray[rowId]).valueArray().array());
    return new ColumnarMap(keys, values, 0, ((MapData) constantArray[rowId]).numElements());
  }

  @Override
  public ColumnVector getChild(int ordinal) {
    DataType fieldType = ((StructType) type).fields()[ordinal].dataType();
    return new ConstantArrayColumnVector(fieldType, getBatchSize(),
        Arrays.stream(constantArray).map(row -> ((InternalRow) row).get(ordinal, fieldType)).toArray());
  }
}
