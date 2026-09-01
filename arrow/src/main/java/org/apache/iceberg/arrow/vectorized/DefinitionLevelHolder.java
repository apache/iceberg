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
 * {@link NullabilityHolder} implementation that stores per-slot Parquet definition levels
 */
final class DefinitionLevelHolder implements NullabilityHolder {
  private final int[] definitionLevels;
  private final int nullThreshold;
  private int numNulls;

  DefinitionLevelHolder(int size, int nullThreshold) {
    this.definitionLevels = new int[size];
    this.nullThreshold = nullThreshold;
  }

  @Override
  public int size() {
    return definitionLevels.length;
  }

  @Override
  public void setNull(int index, int definitionLevel) {
    definitionLevels[index] = definitionLevel;
    numNulls++;
  }

  @Override
  public void setNotNull(int index, int definitionLevel) {
    definitionLevels[index] = definitionLevel;
  }

  @Override
  public void setNulls(int startIndex, int num, int definitionLevel) {
    Arrays.fill(definitionLevels, startIndex, startIndex + num, definitionLevel);
    numNulls += num;
  }

  @Override
  public void setNotNulls(int startIndex, int num, int definitionLevel) {
    Arrays.fill(definitionLevels, startIndex, startIndex + num, definitionLevel);
  }

  @Override
  public byte isNullAt(int index) {
    return (byte) (definitionLevels[index] < nullThreshold ? 1 : 0);
  }

  @Override
  public int definitionLevelAt(int index) {
    return definitionLevels[index];
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
