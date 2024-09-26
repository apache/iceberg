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
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Iterators;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

/**
 * A custom set for a {@link Wrapper} of the given type that maintains insertion order
 *
 * @param <T> The type to wrap in a {@link Wrapper} instance.
 */
abstract class WrapperSet<T> implements Set<T>, Serializable {
  private final Set<Wrapper<T>> set;

  protected WrapperSet(Iterable<Wrapper<T>> wrappers) {
    this.set = Sets.newLinkedHashSet(wrappers);
  }

  protected abstract Wrapper<T> wrapper();

  protected abstract Wrapper<T> wrap(T file);

  protected abstract boolean isInstance(Object obj);

  protected interface Wrapper<T> {
    T get();

    Wrapper<T> set(T object);
  }

  @Override
  public int size() {
    return set.size();
  }

  @Override
  public boolean isEmpty() {
    return set.isEmpty();
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean contains(Object obj) {
    if (isInstance(obj)) {
      Wrapper<T> wrapper = wrapper();
      boolean result = set.contains(wrapper.set((T) obj));
      wrapper.set(null); // don't hold a reference to the value
      return result;
    }

    return false;
  }

  @Override
  public Iterator<T> iterator() {
    return Iterators.transform(set.iterator(), Wrapper::get);
  }

  @Override
  public Object[] toArray() {
    return Lists.newArrayList(iterator()).toArray();
  }

  @Override
  public <X> X[] toArray(X[] destArray) {
    return Lists.newArrayList(iterator()).toArray(destArray);
  }

  @Override
  public boolean add(T obj) {
    return set.add(wrap(obj));
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean remove(Object obj) {
    if (isInstance(obj)) {
      Wrapper<T> wrapper = wrapper();
      boolean result = set.remove(wrapper.set((T) obj));
      wrapper.set(null); // don't hold a reference to the value
      return result;
    }

    return false;
  }

  @Override
  public boolean containsAll(Collection<?> collection) {
    if (null != collection) {
      return Iterables.all(collection, this::contains);
    }

    return false;
  }

  @Override
  public boolean addAll(Collection<? extends T> collection) {
    if (null != collection) {
      return Iterables.addAll(set, Iterables.transform(collection, this::wrap));
    }

    return false;
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean retainAll(Collection<?> collection) {
    if (null != collection) {
      Set<Wrapper<T>> toRetain =
          collection.stream()
              .filter(this::isInstance)
              .map(obj -> (T) obj)
              .map(this::wrap)
              .collect(Collectors.toSet());

      return Iterables.retainAll(set, toRetain);
    }

    return false;
  }

  @Override
  public boolean removeAll(Collection<?> collection) {
    if (null != collection) {
      return collection.stream().filter(this::remove).count() != 0;
    }

    return false;
  }

  @Override
  public void clear() {
    set.clear();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (!(other instanceof Set)) {
      return false;
    }

    Set<?> that = (Set<?>) other;

    if (size() != that.size()) {
      return false;
    }

    try {
      return containsAll(that);
    } catch (ClassCastException | NullPointerException unused) {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return set.stream().mapToInt(Object::hashCode).sum();
  }
}
