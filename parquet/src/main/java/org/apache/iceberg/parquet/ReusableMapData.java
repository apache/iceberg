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

public interface ReusableMapData {
  ReusableArrayData keys();
  ReusableArrayData values();

  default void grow() {
    keys().grow();
    values().grow();
  }

  default int capacity() {
    return keys().capacity();
  }

  default void setNumElements(int numElements) {
    keys().setNumElements(numElements);
    values().setNumElements(numElements);
  }

  int size();

  @SuppressWarnings("unchecked")
  default <K, V> Map.Entry<K, V> getRaw(
      int pos,
      ParquetValueReaders.ReusableEntry<K, V> reuse,
      ParquetValueReaders.ReusableEntry<K, V> def) {
    if (pos < capacity()) {
      reuse.set((K) keys().getObj(pos), (V) values().getObj(pos));
      return reuse;
    }
    return def;
  }

  default void addPair(Object key, Object value) {
    if (size() >= capacity()) {
      grow();
    }
    keys().update(size(), key);
    values().update(size(), value);
    setNumElements(size() + 1);
  }

  default ReusableArrayData keyArray() {
    return keys();
  }

  default ReusableArrayData valueArray() {
    return values();
  }
}
