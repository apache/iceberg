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

package org.apache.iceberg.parquet;

@SuppressWarnings("checkstyle:VisibilityModifier")
public class ReusableArrayData {
  private static final Object[] EMPTY = new Object[0];

  protected Object[] values = EMPTY;
  protected int numElements = 0;

  public void grow() {
    if (values.length == 0) {
      this.values = new Object[20];
    } else {
      Object[] old = values;
      this.values = new Object[old.length << 1];
      // copy the old array in case it has values that can be reused
      System.arraycopy(old, 0, values, 0, old.length);
    }
  }

  public void setValue(int idx, Object value) {
    this.values[idx] = value;
  }

  public int capacity() {
    return values.length;
  }

  public void setNumElements(int numElements) {
    this.numElements = numElements;
  }

  public int size() {
    return numElements;
  }

  public boolean isNullAt(int ordinal) {
    return null == values[ordinal];
  }

  public boolean getBoolean(int ordinal) {
    return (boolean) values[ordinal];
  }

  public byte getByte(int ordinal) {
    return (byte) values[ordinal];
  }

  public short getShort(int ordinal) {
    return (short) values[ordinal];
  }

  public int getInt(int ordinal) {
    return (int) values[ordinal];
  }

  public long getLong(int ordinal) {
    return (long) values[ordinal];
  }

  public float getFloat(int ordinal) {
    return (float) values[ordinal];
  }

  public double getDouble(int ordinal) {
    return (double) values[ordinal];
  }

  @SuppressWarnings("unchecked")
  public <T> T getRaw(int pos) {
    return (T) values[pos];
  }

  public byte[] getBinary(int ordinal) {
    return (byte[]) values[ordinal];
  }
}
