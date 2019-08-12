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

package org.apache.iceberg.expressions;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Represents a set of literal values in an IN or NOT_IN predicate
 * @param <T> The Java type of the value, which can be wrapped by a {@link Literal}
 */
public class LiteralSet<T> implements Set<T>, Serializable {
  private final Set<T> literals;

  LiteralSet(Set<Literal<T>> lits) {
    Preconditions.checkArgument(lits == null || lits.size() > 1,
        "The input literal set must include more than 1 element.");
    literals = ImmutableSet.<T>builder().addAll(
        lits.stream().map(Literal::value).collect(Collectors.toSet())).build();
  }

  @Override
  public String toString() {
    Iterator<T> it = literals.iterator();
    if (!it.hasNext()) {
      return "{}";
    }

    StringBuilder sb = new StringBuilder();
    sb.append('{');
    while (true) {
      sb.append(it.next());
      if (!it.hasNext()) {
        return sb.append('}').toString();
      }
      sb.append(',').append(' ');
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    LiteralSet<T> that = (LiteralSet<T>) other;
    return literals.equals(that.literals);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(literals);
  }

  @Override
  public boolean contains(Object object) {
    return literals.contains(object);
  }

  @Override
  public int size() {
    return literals.size();
  }

  @Override
  public boolean isEmpty() {
    return literals.isEmpty();
  }

  @Override
  public Iterator<T> iterator() {
    return literals.iterator();
  }

  @Override
  public Object[] toArray() {
    return literals.toArray();
  }

  @Override
  public <X> X[] toArray(X[] a) {
    return literals.toArray(a);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return literals.containsAll(c);
  }

  @Override
  public boolean add(T t) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException();
  }


  @Override
  public boolean addAll(Collection<? extends T> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

}
