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
package org.apache.iceberg.arrow.vectorized;

import org.apache.arrow.vector.ValueVector;

public class ArrowVectorAccessor<DecimalT, Utf8StringT, ArrayT, ChildVectorT extends AutoCloseable>
    implements AutoCloseable {
  private final ValueVector vector;
  private final ChildVectorT[] childColumns;

  protected ArrowVectorAccessor(ValueVector vector) {
    this(vector, null);
  }

  protected ArrowVectorAccessor(ValueVector vector, ChildVectorT[] children) {
    this.vector = vector;
    this.childColumns = children;
  }

  @Override
  public void close() {
    if (childColumns != null) {
      for (ChildVectorT column : childColumns) {
        try {
          // Closing an ArrowColumnVector is expected to not throw any exception
          column.close();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
    vector.close();
  }

  public boolean getBoolean(int rowId) {
    throw new UnsupportedOperationException("Unsupported type: boolean");
  }

  public int getInt(int rowId) {
    throw new UnsupportedOperationException("Unsupported type: int");
  }

  public long getLong(int rowId) {
    throw new UnsupportedOperationException("Unsupported type: long");
  }

  public float getFloat(int rowId) {
    throw new UnsupportedOperationException("Unsupported type: float");
  }

  public double getDouble(int rowId) {
    throw new UnsupportedOperationException("Unsupported type: double");
  }

  public byte[] getBinary(int rowId) {
    throw new UnsupportedOperationException("Unsupported type: binary");
  }

  public DecimalT getDecimal(int rowId, int precision, int scale) {
    throw new UnsupportedOperationException("Unsupported type: decimal");
  }

  public Utf8StringT getUTF8String(int rowId) {
    throw new UnsupportedOperationException("Unsupported type: UTF8String");
  }

  public ArrayT getArray(int rowId) {
    throw new UnsupportedOperationException("Unsupported type: array");
  }

  public ChildVectorT childColumn(int pos) {
    if (childColumns != null) {
      return childColumns[pos];
    } else {
      throw new IndexOutOfBoundsException("Child columns is null hence cannot find index: " + pos);
    }
  }

  public final ValueVector getVector() {
    return vector;
  }
}
