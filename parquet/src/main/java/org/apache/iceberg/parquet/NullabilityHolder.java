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

public class NullabilityHolder {
  private final boolean[] isNull;
  private int numNulls;

  public NullabilityHolder(int batchSize) {
    this.isNull = new boolean[batchSize];
  }


  public void nullAt(int idx) {
    isNull[idx] =  true;
    numNulls++;
  }

  public void setNulls(int idx, int num) {
    int i = 0;
    while (i < num) {
      isNull[idx] = true;
      numNulls++;
      idx++;
      i++;
    }
  }

  public boolean isNullAt(int idx) {
    return isNull[idx];
  }

  public boolean hasNulls() {
    return numNulls > 0;
  }

  public int numNulls() {
    return numNulls;
  }
}
