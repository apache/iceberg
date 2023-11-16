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
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

/**
 * A map that uses char sequences as keys.
 *
 * <p>This implementation wraps provided keys into {@link CharSequenceWrapper} for consistent
 * hashing and equals behavior. This ensures that objects of different types that represent the same
 * sequence of characters are treated as equal keys in the map.
 *
 * <p>Note: This map is not designed for concurrent modification by multiple threads. However, it
 * supports safe concurrent reads, assuming there are no concurrent writes.
 *
 * <p>Note: This map does not support null keys.
 *
 * @param <V> the type of values
 */
public class CharSequenceMap<V> implements Map<CharSequence, V>, Serializable {

  private static final long serialVersionUID = 1L;
  private static final ThreadLocal<CharSequenceWrapper> WRAPPERS =
      ThreadLocal.withInitial(() -> CharSequenceWrapper.wrap(null));

  private final Map<CharSequenceWrapper, V> wrapperMap;

  private CharSequenceMap() {
    this.wrapperMap = Maps.newHashMap();
  }

  public static <T> CharSequenceMap<T> create() {
    return new CharSequenceMap<>();
  }

  @Override
  public int size() {
    return wrapperMap.size();
  }

  @Override
  public boolean isEmpty() {
    return wrapperMap.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    if (key instanceof CharSequence) {
      CharSequenceWrapper wrapper = WRAPPERS.get();
      boolean result = wrapperMap.containsKey(wrapper.set((CharSequence) key));
      wrapper.set(null); // don't hold a reference to the key
      return result;
    }

    return false;
  }

  @Override
  public boolean containsValue(Object value) {
    return wrapperMap.containsValue(value);
  }

  @Override
  public V get(Object key) {
    if (key instanceof CharSequence) {
      CharSequenceWrapper wrapper = WRAPPERS.get();
      V result = wrapperMap.get(wrapper.set((CharSequence) key));
      wrapper.set(null); // don't hold a reference to the value
      return result;
    }

    return null;
  }

  @Override
  public V put(CharSequence key, V value) {
    return wrapperMap.put(CharSequenceWrapper.wrap(key), value);
  }

  @Override
  public V remove(Object key) {
    if (key instanceof CharSequence) {
      CharSequenceWrapper wrapper = WRAPPERS.get();
      V result = wrapperMap.remove(wrapper.set((CharSequence) key));
      wrapper.set(null); // don't hold a reference to the value
      return result;
    }

    return null;
  }

  @Override
  public void putAll(Map<? extends CharSequence, ? extends V> otherMap) {
    otherMap.forEach(this::put);
  }

  @Override
  public void clear() {
    wrapperMap.clear();
  }

  @Override
  public Set<CharSequence> keySet() {
    CharSequenceSet keySet = CharSequenceSet.empty();

    for (CharSequenceWrapper wrapper : wrapperMap.keySet()) {
      keySet.add(wrapper.get());
    }

    return keySet;
  }

  @Override
  public Collection<V> values() {
    return wrapperMap.values();
  }

  @Override
  public Set<Entry<CharSequence, V>> entrySet() {
    Set<Entry<CharSequence, V>> entrySet = Sets.newHashSet();

    for (Entry<CharSequenceWrapper, V> entry : wrapperMap.entrySet()) {
      entrySet.add(new CharSequenceEntry<>(entry));
    }

    return entrySet;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (other == null || getClass() != other.getClass()) {
      return false;
    }

    CharSequenceMap<?> that = (CharSequenceMap<?>) other;
    return Objects.equals(wrapperMap, that.wrapperMap);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(wrapperMap);
  }

  @Override
  public String toString() {
    return entrySet().stream().map(this::toString).collect(Collectors.joining(", ", "{", "}"));
  }

  private String toString(Entry<CharSequence, V> entry) {
    CharSequence key = entry.getKey();
    V value = entry.getValue();
    return key + "=" + (value == this ? "(this Map)" : value);
  }

  private static class CharSequenceEntry<V> implements Entry<CharSequence, V> {
    private final Entry<CharSequenceWrapper, V> inner;

    private CharSequenceEntry(Entry<CharSequenceWrapper, V> inner) {
      this.inner = inner;
    }

    @Override
    public CharSequence getKey() {
      return inner.getKey().get();
    }

    @Override
    public V getValue() {
      return inner.getValue();
    }

    @Override
    public int hashCode() {
      return inner.hashCode();
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      } else if (other == null || getClass() != other.getClass()) {
        return false;
      }

      CharSequenceEntry<?> that = (CharSequenceEntry<?>) other;
      return inner.equals(that.inner);
    }

    @Override
    public V setValue(V value) {
      throw new UnsupportedOperationException("Cannot set value");
    }
  }
}
