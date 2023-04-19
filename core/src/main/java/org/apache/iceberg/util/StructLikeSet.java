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
import java.util.Objects;
import java.util.Set;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Iterators;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;

public class StructLikeSet extends AbstractSet<StructLike> implements Set<StructLike> {
  public static StructLikeSet create(Types.StructType type) {
    return new StructLikeSet(type);
  }

  private final Types.StructType type;
  private final Set<StructLikeWrapper> wrapperSet;
  private final ThreadLocal<StructLikeWrapper> wrappers;

  private StructLikeSet(Types.StructType type) {
    this.type = type;
    this.wrapperSet = Sets.newHashSet();
    this.wrappers = ThreadLocal.withInitial(() -> StructLikeWrapper.forType(type));
  }

  @Override
  public int size() {
    return wrapperSet.size();
  }

  @Override
  public boolean isEmpty() {
    return wrapperSet.isEmpty();
  }

  @Override
  public boolean contains(Object obj) {
    if (obj instanceof StructLike || obj == null) {
      StructLikeWrapper wrapper = wrappers.get();
      boolean result = wrapperSet.contains(wrapper.set((StructLike) obj));
      wrapper.set(null); // don't hold a reference to the value
      return result;
    }
    return false;
  }

  @Override
  public Iterator<StructLike> iterator() {
    return Iterators.transform(wrapperSet.iterator(), StructLikeWrapper::get);
  }

  @Override
  public Object[] toArray() {
    return Iterators.toArray(iterator(), StructLike.class);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T[] toArray(T[] destArray) {
    int size = wrapperSet.size();
    if (destArray.length < size) {
      return (T[]) toArray();
    }

    Iterator<StructLike> iter = iterator();
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
  public boolean add(StructLike struct) {
    return wrapperSet.add(wrappers.get().copyFor(struct));
  }

  @Override
  public boolean remove(Object obj) {
    if (obj instanceof StructLike || obj == null) {
      StructLikeWrapper wrapper = wrappers.get();
      boolean result = wrapperSet.remove(wrapper.set((StructLike) obj));
      wrapper.set(null); // don't hold a reference to the value
      return result;
    }
    return false;
  }

  @Override
  public boolean containsAll(Collection<?> objects) {
    if (objects != null) {
      return Iterables.all(objects, this::contains);
    }
    return false;
  }

  @Override
  public boolean addAll(Collection<? extends StructLike> structs) {
    if (structs != null) {
      return Iterables.addAll(
          wrapperSet, Iterables.transform(structs, struct -> wrappers.get().copyFor(struct)));
    }
    return false;
  }

  @Override
  public boolean retainAll(Collection<?> objects) {
    throw new UnsupportedOperationException("retailAll is not supported");
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
  public void clear() {
    wrapperSet.clear();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    StructLikeSet that = (StructLikeSet) o;
    if (!type.equals(that.type)) {
      return false;
    }

    if (wrapperSet.size() != that.wrapperSet.size()) {
      return false;
    }

    return containsAll(that);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(type) + wrapperSet.stream().mapToInt(StructLikeWrapper::hashCode).sum();
  }
}
