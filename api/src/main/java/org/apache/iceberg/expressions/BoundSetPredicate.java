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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import org.apache.iceberg.util.CharSequenceWrapper;

public class BoundSetPredicate<T> extends Predicate<BoundReference<T>> {
  private final LiteralSet<T> literalSet;

  @SuppressWarnings("unchecked")
  BoundSetPredicate(Operation op, BoundReference<T> ref, Set<Literal<T>> lits) {
    super(op, ref);
    Preconditions.checkArgument(op == Operation.IN || op == Operation.NOT_IN,
        "%s predicate does not support a set of literals", op);
    if (lits.iterator().next() instanceof Literals.StringLiteral) {
      this.literalSet = (LiteralSet<T>) new CharSeqLiteralSet((Set) lits);
    } else {
      this.literalSet = new LiteralSet<>(lits);
    }
  }

  BoundSetPredicate(Operation op, BoundReference<T> ref, LiteralSet<T> lits) {
    super(op, ref);
    Preconditions.checkArgument(op == Operation.IN || op == Operation.NOT_IN,
        "%s predicate does not support a literal set", op);
    this.literalSet = lits;
  }

  @Override
  public Expression negate() {
    return new BoundSetPredicate<>(op().negate(), ref(), literalSet);
  }

  public Set<T> literalSet() {
    return literalSet;
  }

  @Override
  String literalString() {
    return literalSet.toString();
  }

  /**
   * Represents a set of literal values in an IN or NOT_IN predicate
   * @param <T> The Java type of the value, which can be wrapped by a {@link Literal}
   */
  private static class LiteralSet<T> implements Set<T>, Serializable {
    private final Set<T> values;

    LiteralSet(Iterator<T> vals) {
      this.values = ImmutableSet.<T>builder().addAll(vals).build();
    }

    LiteralSet(Set<Literal<T>> lits) {
      Preconditions.checkArgument(lits == null || lits.size() > 1,
          "The input literal set must include more than 1 element.");
      values = ImmutableSet.<T>builder().addAll(
          lits.stream().map(Literal::value).iterator()).build();
    }

    Set<T> values() {
      return values;
    }

    @Override
    public String toString() {
      return Joiner.on(", ").join(values);
    }

    @Override
    public boolean contains(Object object) {
      return values.contains(object);
    }

    @Override
    public int size() {
      return values.size();
    }

    @Override
    public boolean isEmpty() {
      return values.isEmpty();
    }

    @Override
    public Iterator<T> iterator() {
      return values.iterator();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
      throw new UnsupportedOperationException(
          "LiteralSet currently only supports checking if a single item is contained in it.");
    }

    @Override
    public Object[] toArray() {
      throw new UnsupportedOperationException(
          "Please use iterator() to visit the elements in the set.");
    }

    @Override
    public <X> X[] toArray(X[] a) {
      throw new UnsupportedOperationException(
          "Please use iterator() to visit the elements in the set.");
    }

    @Override
    public boolean add(T t) {
      throw new UnsupportedOperationException(
          "The set is immutable and cannot add an element.");
    }

    @Override
    public boolean remove(Object o) {
      throw new UnsupportedOperationException(
          "The set is immutable and cannot remove an element.");
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
      throw new UnsupportedOperationException(
          "The set is immutable and cannot add elements.");
    }

    @Override
    public boolean retainAll(Collection<?> c) {
      throw new UnsupportedOperationException(
          "The set is immutable and cannot be modified.");
    }

    @Override
    public boolean removeAll(Collection<?> c) {
      throw new UnsupportedOperationException(
          "The set is immutable and cannot remove elements.");
    }

    @Override
    public void clear() {
      throw new UnsupportedOperationException(
          "The set is immutable and cannot be modified.");
    }
  }

  private static class CharSeqLiteralSet extends LiteralSet<CharSequence> {
    private static final ThreadLocal<CharSequenceWrapper> wrapper =
        ThreadLocal.withInitial(() -> CharSequenceWrapper.wrap(null));

    CharSeqLiteralSet(Set<Literal<CharSequence>> lits) {
      super(lits.stream()
          .map(lit -> (CharSequence) CharSequenceWrapper.wrap(lit.value()))
          .iterator());
    }

    @Override
    public Iterator<CharSequence> iterator() {
      return values().stream().map(val -> ((CharSequenceWrapper) val).get()).iterator();
    }

    @Override
    public boolean contains(Object object) {
      if (object instanceof CharSequence) {
        return super.contains(wrapper.get().set((CharSequence) object));
      }
      return false;
    }
  }
}
