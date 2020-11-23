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
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;

public class StructLikeMap<T> extends AbstractMap<StructLike, T> implements Map<StructLike, T> {

  public static <T> StructLikeMap<T> create(Types.StructType type) {
    return new StructLikeMap<>(type);
  }

  private final Types.StructType type;
  private final Map<StructLikeWrapper, T> wrapperMap;
  private final ThreadLocal<StructLikeWrapper> wrappers;

  private StructLikeMap(Types.StructType type) {
    this.type = type;
    this.wrapperMap = Maps.newHashMap();
    this.wrappers = ThreadLocal.withInitial(() -> StructLikeWrapper.forType(type));
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
    if (key instanceof StructLike) {
      StructLikeWrapper wrapper = wrappers.get();
      boolean result = wrapperMap.containsKey(wrapper.set((StructLike) key));
      wrapper.set(null);
      return result;
    }
    return false;
  }

  @Override
  public boolean containsValue(Object value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public T get(Object key) {
    if (key instanceof StructLike) {
      StructLikeWrapper wrapper = wrappers.get();
      T value = wrapperMap.get(wrapper.set((StructLike) key));
      wrapper.set(null);
      return value;
    }
    return null;
  }

  @Override
  public T put(StructLike key, T value) {
    return wrapperMap.put(StructLikeWrapper.forType(type).set(key), value);
  }

  @Override
  public T remove(Object key) {
    if (key instanceof StructLike) {
      StructLikeWrapper wrapper = wrappers.get();
      T value = wrapperMap.remove(wrapper.set((StructLike) key));
      wrapper.set(null); // don't hold a reference to the value.
      return value;
    }
    return null;
  }

  @Override
  public void putAll(Map<? extends StructLike, ? extends T> keyValues) {
    if (keyValues != null && !keyValues.isEmpty()) {
      for (Map.Entry<? extends StructLike, ? extends T> pair : keyValues.entrySet()) {
        wrapperMap.put(StructLikeWrapper.forType(type).set(pair.getKey()), pair.getValue());
      }
    }
  }

  @Override
  public void clear() {
    wrapperMap.clear();
  }

  @Override
  public Set<StructLike> keySet() {
    return wrapperMap.keySet().stream().map(StructLikeWrapper::get).collect(Collectors.toSet());
  }

  @Override
  public Collection<T> values() {
    return wrapperMap.values();
  }

  @Override
  public Set<Entry<StructLike, T>> entrySet() {
    throw new UnsupportedOperationException();
  }
}
