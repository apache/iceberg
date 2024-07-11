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

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Iterators;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;

public class PartitionSet extends AbstractSet<Pair<Integer, StructLike>> {
  public static PartitionSet create(Map<Integer, PartitionSpec> specsById) {
    return new PartitionSet(specsById);
  }

  private final Map<Integer, Types.StructType> partitionTypeById;
  private final Map<Integer, Set<StructLike>> partitionSetById;

  private PartitionSet(Map<Integer, PartitionSpec> specsById) {
    ImmutableMap.Builder<Integer, Types.StructType> builder = ImmutableMap.builder();
    specsById.forEach((specId, spec) -> builder.put(specId, spec.partitionType()));
    this.partitionTypeById = builder.build();
    this.partitionSetById = Maps.newHashMap();
  }

  @Override
  public int size() {
    return partitionSetById.values().stream().mapToInt(Set::size).sum();
  }

  @Override
  public boolean isEmpty() {
    return partitionSetById.values().stream().allMatch(Set::isEmpty);
  }

  @Override
  public boolean contains(Object o) {
    if (o instanceof Pair) {
      Object first = ((Pair<?, ?>) o).first();
      Object second = ((Pair<?, ?>) o).second();
      if (first instanceof Integer && (second == null || second instanceof StructLike)) {
        return contains((Integer) first, (StructLike) second);
      }
    }

    return false;
  }

  public boolean contains(int specId, StructLike struct) {
    Set<StructLike> partitionSet = partitionSetById.get(specId);
    if (partitionSet != null) {
      return partitionSet.contains(struct);
    }

    return false;
  }

  @Override
  public boolean add(Pair<Integer, StructLike> pair) {
    Preconditions.checkArgument(pair.first() != null, "Cannot track partition with null spec id");
    return add(pair.first(), pair.second());
  }

  public boolean add(int specId, StructLike struct) {
    Set<StructLike> partitionSet =
        partitionSetById.computeIfAbsent(
            specId, id -> StructLikeSet.create(partitionTypeById.get(id)));
    return partitionSet.add(struct);
  }

  @Override
  public boolean remove(Object o) {
    if (o instanceof Pair) {
      Object first = ((Pair<?, ?>) o).first();
      Object second = ((Pair<?, ?>) o).second();
      if (first instanceof Integer && (second == null || second instanceof StructLike)) {
        return remove((Integer) first, (StructLike) second);
      }
    }

    return false;
  }

  public boolean remove(int specId, StructLike struct) {
    Set<StructLike> partitionSet = partitionSetById.get(specId);
    if (partitionSet != null) {
      return partitionSet.remove(struct);
    }

    return false;
  }

  @Override
  public Iterator<Pair<Integer, StructLike>> iterator() {
    Iterable<Iterable<Pair<Integer, StructLike>>> setsAsPairs =
        Iterables.transform(
            partitionSetById.entrySet(),
            idAndSet ->
                Iterables.transform(
                    idAndSet.getValue(), struct -> Pair.of(idAndSet.getKey(), struct)));

    return Iterables.concat(setsAsPairs).iterator();
  }

  @Override
  public Object[] toArray() {
    return Iterators.toArray(iterator(), Pair.class);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T[] toArray(T[] destArray) {
    int size = size();
    if (destArray.length < size) {
      return (T[]) toArray();
    }

    Iterator<Pair<Integer, StructLike>> iter = iterator();
    int ind = 0;
    while (iter.hasNext()) {
      destArray[ind] = (T) iter.next();
      ind += 1;
    }

    if (destArray.length > size) {
      destArray[size] = null;
    }

    return destArray;
  }

  @Override
  public boolean containsAll(Collection<?> objects) {
    if (objects != null) {
      return Iterables.all(objects, this::contains);
    }
    return false;
  }

  @Override
  public boolean addAll(Collection<? extends Pair<Integer, StructLike>> pairs) {
    boolean changed = false;
    if (pairs != null) {
      for (Pair<Integer, StructLike> pair : pairs) {
        changed |= add(pair);
      }
    }
    return changed;
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException("retainAll is not supported");
  }

  @Override
  public boolean removeAll(Collection<?> objects) {
    boolean changed = false;
    if (objects != null) {
      for (Object object : objects) {
        changed |= remove(object);
      }
    }
    return changed;
  }

  @Override
  public String toString() {
    StringJoiner result = new StringJoiner(", ", "[", "]");
    for (Map.Entry<Integer, Set<StructLike>> e : partitionSetById.entrySet()) {
      StringJoiner partitionDataJoiner = new StringJoiner(", ");
      Types.StructType structType = partitionTypeById.get(e.getKey());
      for (StructLike s : e.getValue()) {
        for (int i = 0; i < structType.fields().size(); i++) {
          StringBuilder partitionStringBuilder = new StringBuilder();
          partitionStringBuilder.append(structType.fields().get(i).name());
          partitionStringBuilder.append("=");
          Object value = s.get(i, Object.class);
          partitionStringBuilder.append(value == null ? "null" : value.toString());
          partitionDataJoiner.add(partitionStringBuilder.toString());
        }
      }
      result.add(partitionDataJoiner.toString());
    }
    return result.toString();
  }

  @Override
  public void clear() {
    partitionSetById.clear();
  }
}
