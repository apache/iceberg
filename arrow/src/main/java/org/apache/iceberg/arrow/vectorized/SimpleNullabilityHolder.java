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

/** {@link NullabilityHolder} implementation for optional top-level columns */
public final class SimpleNullabilityHolder implements NullabilityHolder {
  private final byte[] isNull;
  private final byte[] nulls;
  private int numNulls;

  public SimpleNullabilityHolder(int size) {
    this.isNull = new byte[size];
    this.nulls = new byte[size];
    Arrays.fill(nulls, (byte) 1);
  }

  @Override
  public int size() {
    return isNull.length;
  }

  @Override
  public void setNull(int index, int definitionLevel) {
    isNull[index] = 1;
    numNulls++;
  }

  @Override
  public void setNotNull(int index, int definitionLevel) {
    isNull[index] = 0;
  }

  @Override
  public void setNulls(int startIndex, int num, int definitionLevel) {
    System.arraycopy(nulls, 0, isNull, startIndex, num);
    numNulls += num;
  }

  @Override
  public void setNotNulls(int startIndex, int num, int definitionLevel) {
    Arrays.fill(isNull, startIndex, startIndex + num, (byte) 0);
  }

  @Override
  public byte isNullAt(int index) {
    return isNull[index];
  }

  @Override
  public int definitionLevelAt(int index) {
    return 1 - isNull[index];
  }

  @Override
  public boolean hasNulls() {
    return numNulls > 0;
  }

  @Override
  public int numNulls() {
    return numNulls;
  }

  @Override
  public void reset() {
    numNulls = 0;
  }
}
