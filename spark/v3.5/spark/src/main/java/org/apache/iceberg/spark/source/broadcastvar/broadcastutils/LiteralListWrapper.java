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
package org.apache.iceberg.spark.source.broadcastvar.broadcastutils;

import java.util.AbstractList;
import java.util.List;

public class LiteralListWrapper<T> extends AbstractList<T> {
  private final int keyIndex;
  private final List<T[]> underlyingList;
  private final int tupleLength;

  public LiteralListWrapper(int keyIndex, List<T[]> underlyingList, int tupleLength) {
    this.keyIndex = keyIndex;
    this.underlyingList = underlyingList;
    this.tupleLength = tupleLength;
  }

  @Override
  public T get(int index) {
    return this.underlyingList.get(index)[this.keyIndex];
  }

  @Override
  public int size() {
    return this.underlyingList.size();
  }

  public List<T[]> getUnderlyingList() {
    return this.underlyingList;
  }
}
