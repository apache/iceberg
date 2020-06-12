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

import org.apache.arrow.vector.ValueVector;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.unsafe.types.UTF8String;

@SuppressWarnings("checkstyle:VisibilityModifier")
public abstract class ArrowVectorAccessor {

  private final ValueVector vector;
  private final ArrowColumnVector[] childColumns;

  ArrowVectorAccessor(ValueVector vector) {
    this.vector = vector;
    this.childColumns = new ArrowColumnVector[0];
  }

  ArrowVectorAccessor(ValueVector vector, ArrowColumnVector[] children) {
    this.vector = vector;
    this.childColumns = children;
  }

  final void close() {
    for (ArrowColumnVector column : childColumns) {
      // Closing an ArrowColumnVector is expected to not throw any exception
      column.close();
    }
    vector.close();
  }

  boolean getBoolean(int rowId) {
    throw new UnsupportedOperationException("Unsupported type: boolean");
  }

  int getInt(int rowId) {
    throw new UnsupportedOperationException("Unsupported type: int");
  }

  long getLong(int rowId) {
    throw new UnsupportedOperationException("Unsupported type: long");
  }

  float getFloat(int rowId) {
    throw new UnsupportedOperationException("Unsupported type: float");
  }

  double getDouble(int rowId) {
    throw new UnsupportedOperationException("Unsupported type: double");
  }

  Decimal getDecimal(int rowId, int precision, int scale) {
    throw new UnsupportedOperationException("Unsupported type: decimal");
  }

  UTF8String getUTF8String(int rowId) {
    throw new UnsupportedOperationException("Unsupported type: UTF8String");
  }

  byte[] getBinary(int rowId) {
    throw new UnsupportedOperationException("Unsupported type: binary");
  }

  ColumnarArray getArray(int rowId) {
    throw new UnsupportedOperationException("Unsupported type: array");
  }

  ArrowColumnVector childColumn(int pos) {
    return childColumns[pos];
  }

  public ValueVector getVector() {
    return vector;
  }
}
