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

import java.util.Arrays;

/**
 * Instances of this class simply track whether a value at an index is null. For simplicity and
 * performance, it is expected that various setter methods {@link #setNull(int)}, {@link
 * #setNulls(int, int, int)}, {@link #setNotNull(int, int)} and {@link #setNotNulls(int, int, int)} are invoked
 * with monotonically increasing values for the index parameter.
 */
public class NullabilityHolder {
  private final int[] definitionLevels;
  private final byte[] isNull;
  private int numNulls;
  private final byte[] nonNulls;
  private final byte[] nulls;

  public NullabilityHolder(int size) {
    this.isNull = new byte[size];
    this.nonNulls = new byte[size];
    Arrays.fill(nonNulls, (byte) 0);
    this.nulls = new byte[size];
    Arrays.fill(nulls, (byte) 1);
    this.definitionLevels = new int[size];
  }

  public int size() {
    return isNull.length;
  }

  public void setNull(int index) {
    isNull[index] = 1;
    numNulls++;
  }

  public void setNull(int index, int definitionLevel) {
    definitionLevels[index] = definitionLevel;
    isNull[index] = 1;
    numNulls++;
  }

  public void setNotNull(int index, int definitionLevel) {
    isNull[index] = 0;
    definitionLevels[index] = definitionLevel;
  }

  public void setNulls(int startIndex, int num, int definitionLevel) {
    Arrays.fill(definitionLevels, startIndex, startIndex + num, definitionLevel);
    System.arraycopy(nulls, 0, isNull, startIndex, num);
    numNulls += num;
  }

  public void setNotNulls(int startIndex, int num, int definitionLevel) {
    System.arraycopy(nonNulls, 0, isNull, startIndex, num);
    Arrays.fill(definitionLevels, startIndex, startIndex + num, definitionLevel);
  }

  /** Returns 1 if null, 0 otherwise. */
  public byte isNullAt(int index) {
    return isNull[index];
  }

  public int definitionLevelAt(int index) {
    return definitionLevels[index];
  }

  public boolean hasNulls() {
    return numNulls > 0;
  }

  public int numNulls() {
    return numNulls;
  }

  public void reset() {
    numNulls = 0;
  }
}
