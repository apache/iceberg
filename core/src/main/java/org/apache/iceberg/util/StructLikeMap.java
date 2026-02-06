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

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;

public class StructLikeMap<T> extends AbstractMap<StructLike, T> implements Map<StructLike, T> {

  /**
   * Creates a new StructLikeMap with the specified type and comparator.
   *
   * @param type the struct type for the keys
   * @param comparator the comparator for comparing struct keys
   * @return a new StructLikeMap instance
   */
  public static <T> StructLikeMap<T> create(
      Types.StructType type, Comparator<StructLike> comparator) {
    return new StructLikeMap<>(type, comparator);
  }

  /**
   * Creates a new StructLikeMap with the specified type using the default comparator for the type.
   *
   * @param type the struct type for the keys
   * @return a new StructLikeMap instance
   */
  public static <T> StructLikeMap<T> create(Types.StructType type) {
    return create(type, Comparators.forType(type));
  }

  private final Types.StructType type;
  private final Map<StructLikeWrapper, T> wrapperMap;
  private final ThreadLocal<StructLikeWrapper> wrappers;

  private StructLikeMap(Types.StructType type, Comparator<StructLike> comparator) {
    this.type = type;
    this.wrapperMap = Maps.newHashMap();
    this.wrappers = ThreadLocal.withInitial(() -> StructLikeWrapper.forType(type, comparator));
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
    if (key instanceof StructLike || key == null) {
      StructLikeWrapper wrapper = wrappers.get();
      boolean result = wrapperMap.containsKey(wrapper.set((StructLike) key));
      wrapper.set(null); // don't hold a reference to the key.
      return result;
    }
    return false;
  }

  @Override
  public boolean containsValue(Object value) {
    return wrapperMap.containsValue(value);
  }

  @Override
  public T get(Object key) {
    if (key instanceof StructLike || key == null) {
      StructLikeWrapper wrapper = wrappers.get();
      T value = wrapperMap.get(wrapper.set((StructLike) key));
      wrapper.set(null); // don't hold a reference to the key.
      return value;
    }
    return null;
  }

  @Override
  public T put(StructLike key, T value) {
    return wrapperMap.put(wrappers.get().copyFor(key), value);
  }

  @Override
  public T remove(Object key) {
    if (key instanceof StructLike || key == null) {
      StructLikeWrapper wrapper = wrappers.get();
      T value = wrapperMap.remove(wrapper.set((StructLike) key));
      wrapper.set(null); // don't hold a reference to the key.
      return value;
    }
    return null;
  }

  @Override
  public void clear() {
    wrapperMap.clear();
  }

  @Override
  public Set<StructLike> keySet() {
    StructLikeSet keySet = StructLikeSet.create(type);
    for (StructLikeWrapper wrapper : wrapperMap.keySet()) {
      keySet.add(wrapper.get());
    }
    return keySet;
  }

  @Override
  public Collection<T> values() {
    return wrapperMap.values();
  }

  @Override
  public Set<Entry<StructLike, T>> entrySet() {
    Set<Entry<StructLike, T>> entrySet = Sets.newHashSet();
    for (Entry<StructLikeWrapper, T> entry : wrapperMap.entrySet()) {
      entrySet.add(new StructLikeEntry<>(entry));
    }
    return entrySet;
  }

  public T computeIfAbsent(StructLike struct, Supplier<T> valueSupplier) {
    return wrapperMap.computeIfAbsent(wrappers.get().copyFor(struct), key -> valueSupplier.get());
  }

  private static class StructLikeEntry<R> implements Entry<StructLike, R> {

    private final Entry<StructLikeWrapper, R> inner;

    private StructLikeEntry(Entry<StructLikeWrapper, R> inner) {
      this.inner = inner;
    }

    @Override
    public StructLike getKey() {
      return inner.getKey().get();
    }

    @Override
    public R getValue() {
      return inner.getValue();
    }

    @Override
    public int hashCode() {
      return inner.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (o == null || getClass() != o.getClass()) {
        return false;
      }

      StructLikeEntry<?> that = (StructLikeEntry<?>) o;
      return inner.equals(that.inner);
    }

    @Override
    public R setValue(R value) {
      throw new UnsupportedOperationException("Does not support setValue.");
    }
  }

  public <U> StructLikeMap<U> transformValues(Function<T, U> func) {
    StructLikeMap<U> result = create(type);
    wrapperMap.forEach((key, value) -> result.put(key.get(), func.apply(value)));
    return result;
  }
}
