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
import java.util.Iterator;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

public class SerializableSet<E> implements Set<E>, Serializable {
  private final Set<E> copiedSet;
  private transient volatile Set<E> immutableSet;

  SerializableSet() {
    this.copiedSet = Sets.newHashSet();
  }

  private SerializableSet(Set<E> set) {
    this.copiedSet = Sets.newHashSet();
    this.copiedSet.addAll(set);
  }

  public static <E> SerializableSet<E> copyOf(Set<E> set) {
    return set == null ? null : new SerializableSet<>(set);
  }

  public Set<E> immutableSet() {
    if (null == immutableSet) {
      synchronized (this) {
        if (null == immutableSet) {
          immutableSet = Collections.unmodifiableSet(copiedSet);
        }
      }
    }

    return immutableSet;
  }

  @Override
  public int size() {
    return copiedSet.size();
  }

  @Override
  public boolean isEmpty() {
    return copiedSet.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return copiedSet.contains(o);
  }

  @Override
  public Iterator<E> iterator() {
    return copiedSet.iterator();
  }

  @Override
  public Object[] toArray() {
    return copiedSet.toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return copiedSet.toArray(a);
  }

  @Override
  public boolean add(E e) {
    return copiedSet.add(e);
  }

  @Override
  public boolean remove(Object o) {
    return copiedSet.remove(o);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return copiedSet.containsAll(c);
  }

  @Override
  public boolean addAll(Collection<? extends E> c) {
    return copiedSet.addAll(c);
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    return copiedSet.retainAll(c);
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    return copiedSet.removeAll(c);
  }

  @Override
  public void clear() {
    copiedSet.clear();
  }

  @Override
  public boolean equals(Object o) {
    return copiedSet.equals(o);
  }

  @Override
  public int hashCode() {
    return copiedSet.hashCode();
  }
}
