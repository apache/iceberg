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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Iterators;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

public class CharSequenceSet implements Set<CharSequence>, Serializable {
  private static final ThreadLocal<CharSequenceWrapper> wrappers = ThreadLocal.withInitial(
      () -> CharSequenceWrapper.wrap(null));

  public static CharSequenceSet of(Iterable<CharSequence> charSequences) {
    return new CharSequenceSet(charSequences);
  }

  public static CharSequenceSet empty() {
    return new CharSequenceSet(ImmutableList.of());
  }

  private final Set<CharSequenceWrapper> wrapperSet;

  private CharSequenceSet(Iterable<CharSequence> charSequences) {
    this.wrapperSet = Sets.newHashSet(Iterables.transform(charSequences, CharSequenceWrapper::wrap));
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
    if (obj instanceof CharSequence) {
      CharSequenceWrapper wrapper = wrappers.get();
      boolean result = wrapperSet.contains(wrapper.set((CharSequence) obj));
      wrapper.set(null); // don't hold a reference to the value
      return result;
    }
    return false;
  }

  @Override
  public Iterator<CharSequence> iterator() {
    return Iterators.transform(wrapperSet.iterator(), CharSequenceWrapper::get);
  }

  @Override
  public Object[] toArray() {
    return Iterators.toArray(iterator(), CharSequence.class);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T[] toArray(T[] destArray) {
    int size = wrapperSet.size();
    if (destArray.length < size) {
      return (T[]) toArray();
    }

    Iterator<CharSequence> iter = iterator();
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
  public boolean add(CharSequence charSequence) {
    return wrapperSet.add(CharSequenceWrapper.wrap(charSequence));
  }

  @Override
  public boolean remove(Object obj) {
    if (obj instanceof CharSequence) {
      CharSequenceWrapper wrapper = wrappers.get();
      boolean result = wrapperSet.remove(wrapper.set((CharSequence) obj));
      wrapper.set(null); // don't hold a reference to the value
      return result;
    }
    return false;
  }

  @Override
  @SuppressWarnings("CollectionUndefinedEquality")
  public boolean containsAll(Collection<?> objects) {
    if (objects != null) {
      return Iterables.all(objects, this::contains);
    }
    return false;
  }

  @Override
  public boolean addAll(Collection<? extends CharSequence> charSequences) {
    if (charSequences != null) {
      return Iterables.addAll(wrapperSet, Iterables.transform(charSequences, CharSequenceWrapper::wrap));
    }
    return false;
  }

  @Override
  public boolean retainAll(Collection<?> objects) {
    if (objects != null) {
      return Iterables.removeAll(wrapperSet, objects);
    }
    return false;
  }

  @Override
  public boolean removeAll(Collection<?> objects) {
    if (objects != null) {
      return Iterables.removeAll(wrapperSet, objects);
    }
    return false;
  }

  @Override
  public void clear() {
    wrapperSet.clear();
  }
}
