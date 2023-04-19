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

import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class BoundSetPredicate<T> extends BoundPredicate<T> {
  private static final Joiner COMMA = Joiner.on(", ");
  private final Set<T> literalSet;

  BoundSetPredicate(Operation op, BoundTerm<T> term, Set<T> lits) {
    super(op, term);
    Preconditions.checkArgument(
        op == Operation.IN || op == Operation.NOT_IN,
        "%s predicate does not support a literal set",
        op);
    this.literalSet = lits;
  }

  @Override
  public Expression negate() {
    return new BoundSetPredicate<>(op().negate(), term(), literalSet);
  }

  @Override
  public boolean isSetPredicate() {
    return true;
  }

  @Override
  public BoundSetPredicate<T> asSetPredicate() {
    return this;
  }

  public Set<T> literalSet() {
    return literalSet;
  }

  @Override
  public boolean test(T value) {
    switch (op()) {
      case IN:
        return literalSet.contains(value);
      case NOT_IN:
        return !literalSet.contains(value);
      default:
        throw new IllegalStateException("Invalid operation for BoundSetPredicate: " + op());
    }
  }

  @Override
  public boolean isEquivalentTo(Expression other) {
    // only check bound set predicate; binding will convert sets of a single item to a literal
    // predicate
    if (op() == other.op()) {
      BoundSetPredicate<?> pred = (BoundSetPredicate<?>) other;
      return literalSet().equals(pred.literalSet());
    }

    return false;
  }

  @Override
  public String toString() {
    switch (op()) {
      case IN:
        return term() + " in (" + COMMA.join(literalSet) + ")";
      case NOT_IN:
        return term() + " not in (" + COMMA.join(literalSet) + ")";
      default:
        return "Invalid unary predicate: operation = " + op();
    }
  }
}
