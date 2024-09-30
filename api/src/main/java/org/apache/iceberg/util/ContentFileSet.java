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
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Iterators;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

/**
 * A custom set for {@link ContentFile} that maintains insertion order
 *
 * @param <F> the concrete Java class of a ContentFile instance
 */
public class ContentFileSet<F extends ContentFile<F>> implements Set<F>, Serializable {
  private static final ThreadLocal<ContentFileWrapper<?>> WRAPPERS =
      ThreadLocal.withInitial(() -> ContentFileWrapper.wrap(null));

  private final Set<ContentFileWrapper<F>> set;

  private ContentFileSet(Set<ContentFileWrapper<F>> contentFiles) {
    this.set = contentFiles;
  }

  public static <T extends ContentFile<T>> ContentFileSet<T> empty() {
    return new ContentFileSet<>(Sets.newLinkedHashSet());
  }

  public static <T extends ContentFile<T>> ContentFileSet<T> of(Iterable<T> iterable) {
    return new ContentFileSet<>(
        Sets.newLinkedHashSet(Iterables.transform(iterable, ContentFileWrapper::wrap)));
  }

  @Override
  public int size() {
    return set.size();
  }

  @Override
  public boolean isEmpty() {
    return set.isEmpty();
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public boolean contains(Object obj) {
    if (obj instanceof ContentFile) {
      ContentFileWrapper<?> wrapper = WRAPPERS.get();
      boolean result = set.contains(wrapper.set((ContentFile) obj));
      wrapper.set(null); // don't hold a reference to the value
      return result;
    }

    return false;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Iterator<F> iterator() {
    return (Iterator<F>) Iterators.transform(set.iterator(), ContentFileWrapper::get);
  }

  @Override
  public Object[] toArray() {
    return Iterators.toArray(iterator(), ContentFile.class);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T[] toArray(T[] destArray) {
    int size = set.size();
    if (destArray.length < size) {
      return (T[]) toArray();
    }

    Iterator<F> iterator = iterator();
    int idx = 0;
    while (iterator.hasNext()) {
      destArray[idx] = (T) iterator.next();
      idx += 1;
    }

    if (destArray.length > size) {
      destArray[size] = null;
    }

    return destArray;
  }

  @Override
  public boolean add(F contentFile) {
    return set.add(ContentFileWrapper.wrap(contentFile));
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public boolean remove(Object obj) {
    if (obj instanceof ContentFile) {
      ContentFileWrapper<?> wrapper = WRAPPERS.get();
      boolean result = set.remove(wrapper.set((ContentFile) obj));
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
  public boolean addAll(Collection<? extends F> collection) {
    if (null != collection) {
      return Iterables.addAll(set, Iterables.transform(collection, ContentFileWrapper::wrap));
    }

    return false;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public boolean retainAll(Collection<?> collection) {
    if (null != collection) {
      Set<ContentFileWrapper<?>> toRetain = Sets.newLinkedHashSet();
      // using a stream here confuses the compiler
      for (Object obj : collection) {
        if (obj instanceof ContentFile) {
          ContentFile<?> file = (ContentFile) obj;
          toRetain.add(ContentFileWrapper.wrap(file));
        }
      }

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
    return set.stream().mapToInt(ContentFileWrapper::hashCode).sum();
  }

  @Override
  public String toString() {
    return set.stream()
        .map(ContentFileWrapper::toString)
        .collect(Collectors.joining("ContentFileSet({", ", ", "})"));
  }
}
