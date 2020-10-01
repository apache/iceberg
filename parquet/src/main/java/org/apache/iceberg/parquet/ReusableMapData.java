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

import java.util.Map;

@SuppressWarnings("checkstyle:VisibilityModifier")
public abstract class ReusableMapData {
  protected ReusableArrayData keys;
  protected ReusableArrayData values;

  private int numElements;

  public void grow() {
    keys.grow();
    values.grow();
  }

  public int capacity() {
    return keys.capacity();
  }

  public void setNumElements(int numElements) {
    this.numElements = numElements;
    keys.setNumElements(numElements);
    values.setNumElements(numElements);
  }

  public int size() {
    return numElements;
  }

  @SuppressWarnings("unchecked")
  public <K, V> Map.Entry<K, V> getRaw(
      int pos,
      ParquetValueReaders.ReusableEntry<K, V> reuse,
      ParquetValueReaders.ReusableEntry<K, V> def) {
    if (pos < capacity()) {
      reuse.set(keys.getRaw(pos), values.getRaw(pos));
      return reuse;
    }
    return def;
  }

  public void addPair(Object key, Object value) {
    if (numElements >= capacity()) {
      grow();
    }
    keys.setValue(numElements, key);
    values.setValue(numElements, value);
    numElements++;
  }

  public ReusableArrayData keyArray() {
    return keys;
  }

  public ReusableArrayData valueArray() {
    return values;
  }
}
