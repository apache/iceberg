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
package org.apache.iceberg.spark.source.broadcastvar.broadcastutils;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.function.Function;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.Literals;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

public class NavigableSetWrapper<T> implements NavigableSet<T> {
  private final int keyIndex;
  private final NavigableSet<Literal<T>[]> underlyingSet;

  private final ThreadLocal<Literal<T>[]> lookUpDummy;

  private final int tupleLength;

  private final Comparator<T> internalComparator;

  public NavigableSetWrapper(int keyIndex, Iterator<Literal<T>[]> transientLiterals) {
    this.keyIndex = keyIndex;
    Literal<T> firstNonNullEle = null;
    Literal<T>[] arrEleForFirstNonNull = null;
    while (firstNonNullEle == null && transientLiterals.hasNext()) {
      arrEleForFirstNonNull = transientLiterals.next();
      firstNonNullEle = arrEleForFirstNonNull[keyIndex];
    }
    if (firstNonNullEle != null) {
      this.internalComparator = firstNonNullEle.comparator();
      this.tupleLength = arrEleForFirstNonNull.length;
      Comparator<Literal<T>[]> comparator =
          (o1, o2) -> internalComparator.compare(o1[keyIndex].value(), o2[keyIndex].value());
      NavigableSet<Literal<T>[]> tempSet = Sets.newTreeSet(comparator);
      tempSet.add(arrEleForFirstNonNull);
      transientLiterals.forEachRemaining(
          x -> {
            if (x[keyIndex] != null) {
              tempSet.add(x);
            }
          });
      this.underlyingSet = tempSet;
      this.lookUpDummy = ThreadLocal.withInitial(() -> new Literal[tupleLength]);
    } else {
      this.underlyingSet = Collections.emptyNavigableSet();
      this.lookUpDummy = null;
      this.tupleLength = -1;
      this.internalComparator = null;
    }
  }

  public NavigableSetWrapper(
      int keyIndex,
      NavigableSet<Literal<T>[]> baseSet,
      int tupleLength,
      Comparator<T> internalComparator) {
    this.keyIndex = keyIndex;
    this.underlyingSet = baseSet;
    this.tupleLength = tupleLength;
    this.internalComparator = internalComparator;
    this.lookUpDummy = ThreadLocal.withInitial(() -> new Literal[tupleLength]);
  }

  @Override
  public T lower(T e) {
    return executeFunction(e, this.underlyingSet::lower);
  }

  @Override
  public T floor(T e) {
    return executeFunction(e, this.underlyingSet::floor);
  }

  @Override
  public T ceiling(T e) {
    return executeFunction(e, this.underlyingSet::ceiling);
  }

  @Override
  public T higher(T e) {
    return executeFunction(e, this.underlyingSet::higher);
  }

  @Override
  public T pollFirst() {
    return this.underlyingSet.pollFirst()[this.keyIndex].value();
  }

  @Override
  public T pollLast() {
    return this.underlyingSet.pollLast()[this.keyIndex].value();
  }

  @Override
  public int size() {
    return this.underlyingSet.size();
  }

  @Override
  public boolean isEmpty() {
    return this.underlyingSet.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return this.underlyingSet.contains(prepareKey((T) o));
  }

  @Override
  public Iterator<T> iterator() {
    return this.underlyingSet.stream().map(x -> x[this.keyIndex].value()).iterator();
  }

  @Override
  public Object[] toArray() {
    return this.underlyingSet.stream().map(x -> x[this.keyIndex].value()).toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public boolean add(T e) {
    Literal<T>[] arr = new Literal[this.tupleLength];
    arr[this.keyIndex] = Literals.from(e);
    return this.underlyingSet.add(arr);
  }

  @Override
  public boolean remove(Object o) {
    Literal<T>[] preparedKey = this.prepareKey((T) o);
    return this.underlyingSet.remove(preparedKey);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public boolean addAll(Collection<? extends T> c) {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void clear() {
    this.underlyingSet.clear();
  }

  @Override
  public NavigableSet<T> descendingSet() {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public Iterator<T> descendingIterator() {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public NavigableSet<T> subSet(
      T fromElement, boolean fromInclusive, T toElement, boolean toInclusive) {
    Literal<T>[] fromKey = new Literal[this.tupleLength];
    fromKey[this.keyIndex] = Literals.from(fromElement);
    Literal<T>[] toKey = new Literal[this.tupleLength];
    toKey[this.keyIndex] = Literals.from(toElement);
    return new NavigableSetWrapper<>(
        this.keyIndex,
        this.underlyingSet.subSet(fromKey, fromInclusive, toKey, toInclusive),
        this.tupleLength,
        this.internalComparator);
  }

  @Override
  public NavigableSet<T> headSet(T toElement, boolean inclusive) {
    Literal<T>[] toKey = new Literal[this.tupleLength];
    toKey[this.keyIndex] = Literals.from(toElement);
    return new NavigableSetWrapper<>(
        this.keyIndex,
        this.underlyingSet.headSet(toKey, inclusive),
        this.tupleLength,
        this.internalComparator);
  }

  @Override
  public NavigableSet<T> tailSet(T fromElement, boolean inclusive) {
    Literal<T>[] fromKey = new Literal[this.tupleLength];
    fromKey[this.keyIndex] = Literals.from(fromElement);
    return new NavigableSetWrapper<>(
        this.keyIndex,
        this.underlyingSet.tailSet(fromKey, inclusive),
        this.tupleLength,
        this.internalComparator);
  }

  @Override
  public Comparator<? super T> comparator() {
    return this.internalComparator;
  }

  @Override
  public SortedSet<T> subSet(T fromElement, T toElement) {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public SortedSet<T> headSet(T toElement) {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public SortedSet<T> tailSet(T fromElement) {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public T first() {
    return this.underlyingSet.first()[keyIndex].value();
  }

  @Override
  public T last() {
    return this.underlyingSet.last()[keyIndex].value();
  }

  private T executeFunction(T key, Function<Literal<T>[], Literal<T>[]> func) {
    Literal<T>[] preparedKey = prepareKey(key);
    Literal<T>[] litValArr = func.apply(preparedKey);
    return litValArr != null ? litValArr[this.keyIndex].value() : null;
  }

  private Literal<T>[] prepareKey(T key) {
    Literal<T>[] preparedKey = this.lookUpDummy.get();
    preparedKey[this.keyIndex] = Literals.from(key);
    return preparedKey;
  }
}
