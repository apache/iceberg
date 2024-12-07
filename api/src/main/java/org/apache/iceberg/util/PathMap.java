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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * A map-like data structure that uses char sequences as keys and compares them by char sequence
 * value they represent.
 *
 * <p>This implementation wraps provided keys into {@link CharSequenceWrapper} for consistent
 * hashing and equals behavior. This ensures that objects of different types that represent the same
 * sequence of characters are treated as equal keys in the map.
 *
 * <p>Note: CharSequenceMap is not designed for concurrent modification by multiple threads.
 * However, it supports safe concurrent reads, assuming there are no concurrent writes.
 *
 * <p>Note: CharSequenceMap does not support null keys.
 *
 * @param <V> the type of values
 */
public class PathMap<V> implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final ThreadLocal<CharSequenceWrapper> WRAPPERS =
      ThreadLocal.withInitial(() -> CharSequenceWrapper.wrap(null));

  private final Map<CharSequenceWrapper, V> wrapperMap;

  private PathMap() {
    this.wrapperMap = Maps.newHashMap();
  }

  public static <T> PathMap<T> create() {
    return new PathMap<>();
  }

  public int size() {
    return wrapperMap.size();
  }

  public boolean isEmpty() {
    return wrapperMap.isEmpty();
  }

  public boolean containsKey(CharSequence key) {
    CharSequenceWrapper wrapper = WRAPPERS.get();
    boolean result = wrapperMap.containsKey(wrapper.set(key));
    wrapper.set(null); // don't hold a reference to the key
    return result;
  }

  public V get(CharSequence key) {
    CharSequenceWrapper wrapper = WRAPPERS.get();
    V result = wrapperMap.get(wrapper.set(key));
    wrapper.set(null); // don't hold a reference to the value
    return result;
  }

  public V getOrDefault(CharSequence filePath, V defaultValue) {
    return MoreObjects.firstNonNull(get(filePath), defaultValue);
  }

  public V put(CharSequence key, V value) {
    return wrapperMap.put(CharSequenceWrapper.wrap(key), value);
  }

  public V remove(CharSequence key) {
    CharSequenceWrapper wrapper = WRAPPERS.get();
    V result = wrapperMap.remove(wrapper.set(key));
    wrapper.set(null); // don't hold a reference to the value
    return result;
  }

  public void clear() {
    wrapperMap.clear();
  }

  public List<CharSequence> keys() {
    return wrapperMap.keySet().stream().map(CharSequenceWrapper::get).collect(Collectors.toList());
  }

  public Collection<V> values() {
    return wrapperMap.values();
  }

  public Stream<Entry<CharSequence, V>> entries() {
    return wrapperMap.entrySet().stream()
        .map(entry -> Maps.immutableEntry(entry.getKey().get(), entry.getValue()));
  }

  public V computeIfAbsent(CharSequence key, Supplier<V> valueSupplier) {
    return wrapperMap.computeIfAbsent(
        CharSequenceWrapper.wrap(key), ignored -> valueSupplier.get());
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (other == null || getClass() != other.getClass()) {
      return false;
    }

    PathMap<?> that = (PathMap<?>) other;
    return Objects.equals(wrapperMap, that.wrapperMap);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(wrapperMap);
  }

  @Override
  public String toString() {
    return entries().map(this::toString).collect(Collectors.joining(", ", "{", "}"));
  }

  private String toString(Entry<CharSequence, V> entry) {
    CharSequence key = entry.getKey();
    V value = entry.getValue();
    return key + "=" + (value == this ? "(this Map)" : value);
  }
}
