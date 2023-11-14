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
package org.apache.iceberg.util;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class SerializableMap<K, V> implements Map<K, V>, Serializable {
  private static final long serialVersionUID = -3377238354349859240L;

  private final Map<K, V> copiedMap;
  private transient volatile Map<K, V> immutableMap;

  SerializableMap() {
    this.copiedMap = Maps.newHashMap();
  }

  private SerializableMap(Map<K, V> map) {
    this.copiedMap = Maps.newHashMap();
    this.copiedMap.putAll(map);
  }

  private SerializableMap(Map<K, V> map, Set<K> keys) {
    Map<K, V> filteredMap = Maps.newHashMapWithExpectedSize(keys.size());

    for (K key : keys) {
      if (map.containsKey(key)) {
        filteredMap.put(key, map.get(key));
      }
    }

    this.copiedMap = filteredMap;
  }

  public static <K, V> SerializableMap<K, V> copyOf(Map<K, V> map) {
    return map == null ? null : new SerializableMap<>(map);
  }

  public static <K, V> SerializableMap<K, V> filteredCopyOf(Map<K, V> map, Set<K> keys) {
    return map == null ? null : new SerializableMap<>(map, keys);
  }

  public Map<K, V> immutableMap() {
    if (immutableMap == null) {
      synchronized (this) {
        if (immutableMap == null) {
          immutableMap = Collections.unmodifiableMap(copiedMap);
        }
      }
    }

    return immutableMap;
  }

  @Override
  public int size() {
    return copiedMap.size();
  }

  @Override
  public boolean isEmpty() {
    return copiedMap.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return copiedMap.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return copiedMap.containsValue(value);
  }

  @Override
  public V get(Object key) {
    return copiedMap.get(key);
  }

  @Override
  public V put(K key, V value) {
    return copiedMap.put(key, value);
  }

  @Override
  public V remove(Object key) {
    return copiedMap.remove(key);
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    copiedMap.putAll(m);
  }

  @Override
  public void clear() {
    copiedMap.clear();
  }

  @Override
  public Set<K> keySet() {
    return copiedMap.keySet();
  }

  @Override
  public Collection<V> values() {
    return copiedMap.values();
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    return copiedMap.entrySet();
  }

  @Override
  public boolean equals(Object o) {
    return copiedMap.equals(o);
  }

  @Override
  public int hashCode() {
    return copiedMap.hashCode();
  }
}
