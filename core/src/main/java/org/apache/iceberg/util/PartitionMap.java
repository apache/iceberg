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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

/**
 * A map that uses a pair of spec ID and partition tuple as keys.
 *
 * <p>This implementation internally stores provided partition tuples in {@link StructLikeMap} for
 * consistent hashing and equals behavior. This ensures that objects of different types that
 * represent the same structs are treated as equal keys in the map.
 *
 * <p>Note: This map is not designed for concurrent modification by multiple threads. However, it
 * supports safe concurrent reads, assuming there are no concurrent writes.
 *
 * <p>Note: This map does not support null pairs but supports null as partition tuples.
 *
 * @param <V> the type of values
 */
public class PartitionMap<V> extends AbstractMap<Pair<Integer, StructLike>, V> {

  private final Map<Integer, PartitionSpec> specs;
  private final Map<Integer, Map<StructLike, V>> partitionMaps;

  private PartitionMap(Map<Integer, PartitionSpec> specs) {
    this.specs = specs;
    this.partitionMaps = Maps.newHashMap();
  }

  public static <T> PartitionMap<T> create(Map<Integer, PartitionSpec> specs) {
    return new PartitionMap<>(specs);
  }

  @Override
  public int size() {
    return partitionMaps.values().stream().mapToInt(Map::size).sum();
  }

  @Override
  public boolean isEmpty() {
    return partitionMaps.values().stream().allMatch(Map::isEmpty);
  }

  @Override
  public boolean containsKey(Object key) {
    return execute(key, this::containsKey, false /* default value */);
  }

  public boolean containsKey(int specId, StructLike struct) {
    Map<StructLike, V> partitionMap = partitionMaps.get(specId);
    return partitionMap != null && partitionMap.containsKey(struct);
  }

  @Override
  public boolean containsValue(Object value) {
    return partitionMaps.values().stream().anyMatch(map -> map.containsValue(value));
  }

  @Override
  public V get(Object key) {
    return execute(key, this::get, null /* default value */);
  }

  public V get(int specId, StructLike struct) {
    Map<StructLike, V> partitionMap = partitionMaps.get(specId);
    return partitionMap != null ? partitionMap.get(struct) : null;
  }

  @Override
  public V put(Pair<Integer, StructLike> key, V value) {
    return put(key.first(), key.second(), value);
  }

  public V put(int specId, StructLike struct, V value) {
    Map<StructLike, V> partitionMap = partitionMaps.computeIfAbsent(specId, this::newPartitionMap);
    return partitionMap.put(struct, value);
  }

  @Override
  public void putAll(Map<? extends Pair<Integer, StructLike>, ? extends V> otherMap) {
    otherMap.forEach(this::put);
  }

  @Override
  public V remove(Object key) {
    return execute(key, this::removeKey, null /* default value */);
  }

  public V removeKey(int specId, StructLike struct) {
    Map<StructLike, V> partitionMap = partitionMaps.get(specId);
    return partitionMap != null ? partitionMap.remove(struct) : null;
  }

  @Override
  public void clear() {
    partitionMaps.clear();
  }

  @Override
  public Set<Pair<Integer, StructLike>> keySet() {
    PartitionSet keySet = PartitionSet.create(specs);

    for (Entry<Integer, Map<StructLike, V>> specIdAndPartitionMap : partitionMaps.entrySet()) {
      int specId = specIdAndPartitionMap.getKey();
      Map<StructLike, V> partitionMap = specIdAndPartitionMap.getValue();
      for (StructLike partition : partitionMap.keySet()) {
        keySet.add(specId, partition);
      }
    }

    return Collections.unmodifiableSet(keySet);
  }

  @Override
  public Collection<V> values() {
    List<V> values = Lists.newArrayList();

    for (Map<StructLike, V> partitionMap : partitionMaps.values()) {
      values.addAll(partitionMap.values());
    }

    return Collections.unmodifiableCollection(values);
  }

  @Override
  public Set<Entry<Pair<Integer, StructLike>, V>> entrySet() {
    Set<Entry<Pair<Integer, StructLike>, V>> entrySet = Sets.newHashSet();

    for (Entry<Integer, Map<StructLike, V>> specIdAndPartitionMap : partitionMaps.entrySet()) {
      int specId = specIdAndPartitionMap.getKey();
      Map<StructLike, V> partitionMap = specIdAndPartitionMap.getValue();
      for (Entry<StructLike, V> structAndValue : partitionMap.entrySet()) {
        entrySet.add(new PartitionEntry<>(specId, structAndValue));
      }
    }

    return Collections.unmodifiableSet(entrySet);
  }

  public V computeIfAbsent(int specId, StructLike struct, Supplier<V> valueSupplier) {
    Map<StructLike, V> partitionMap = partitionMaps.computeIfAbsent(specId, this::newPartitionMap);
    return partitionMap.computeIfAbsent(struct, key -> valueSupplier.get());
  }

  private Map<StructLike, V> newPartitionMap(int specId) {
    PartitionSpec spec = specs.get(specId);
    Preconditions.checkNotNull(spec, "Cannot find spec with ID %s: %s", specId, specs);
    return StructLikeMap.create(spec.partitionType());
  }

  @Override
  public String toString() {
    return partitionMaps.entrySet().stream()
        .flatMap(this::toStrings)
        .collect(Collectors.joining(", ", "{", "}"));
  }

  private Stream<String> toStrings(Entry<Integer, Map<StructLike, V>> entry) {
    PartitionSpec spec = specs.get(entry.getKey());
    return entry.getValue().entrySet().stream().map(innerEntry -> toString(spec, innerEntry));
  }

  private String toString(PartitionSpec spec, Entry<StructLike, V> entry) {
    StructLike struct = entry.getKey();
    V value = entry.getValue();
    return spec.partitionToPath(struct) + " -> " + (value == this ? "(this Map)" : value);
  }

  private <R> R execute(Object key, BiFunction<Integer, StructLike, R> action, R defaultValue) {
    if (key instanceof Pair) {
      Object first = ((Pair<?, ?>) key).first();
      Object second = ((Pair<?, ?>) key).second();
      if (first instanceof Integer && (second == null || second instanceof StructLike)) {
        return action.apply((Integer) first, (StructLike) second);
      }
    } else if (key == null) {
      throw new NullPointerException(getClass().getName() + " does not support null keys");
    }

    return defaultValue;
  }

  private static class PartitionEntry<V> implements Entry<Pair<Integer, StructLike>, V> {
    private final int specId;
    private final Entry<StructLike, V> structAndValue;

    private PartitionEntry(int specId, Entry<StructLike, V> structAndValue) {
      this.specId = specId;
      this.structAndValue = structAndValue;
    }

    @Override
    public Pair<Integer, StructLike> getKey() {
      return Pair.of(specId, structAndValue.getKey());
    }

    @Override
    public V getValue() {
      return structAndValue.getValue();
    }

    @Override
    public int hashCode() {
      return Objects.hash(specId, structAndValue);
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      } else if (other == null || getClass() != other.getClass()) {
        return false;
      }

      PartitionEntry<?> that = (PartitionEntry<?>) other;
      return specId == that.specId && Objects.equals(structAndValue, that.structAndValue);
    }

    @Override
    public V setValue(V newValue) {
      throw new UnsupportedOperationException("Cannot set value");
    }
  }
}
