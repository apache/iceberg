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

import org.apache.iceberg.util.NaNUtil;

public class BoundTermPredicate<T> extends BoundPredicate<T> {

  BoundTermPredicate(Operation op, BoundTerm<T> term, BoundTerm<T> rightTerm) {
    super(op, term, rightTerm);
  }

  @Override
  public Expression negate() {
    return new BoundTermPredicate<>(op().negate(), term(), rightTerm());
  }

  @Override
  public boolean isTermPredicate() {
    return true;
  }

  @Override
  public BoundTermPredicate<T> asTermPredicate() {
    return this;
  }

  @Override
  public boolean test(T value) { // TODO
    switch (op()) {
      case IS_NULL:
        return value == null;
      case NOT_NULL:
        return value != null;
      case IS_NAN:
        return NaNUtil.isNaN(value);
      case NOT_NAN:
        return !NaNUtil.isNaN(value);
      default:
        throw new IllegalStateException("Invalid operation for BoundTermPredicate: " + op());
    }
  }

  @Override
  public boolean isEquivalentTo(Expression other) {
    if (op() == other.op()) {
      return term().isEquivalentTo(((BoundTermPredicate<?>) other).term());
    }

    return false;
  }

  @Override
  public String toString() {
    switch (op()) {
      case LT:
        return term() + " < " + rightTerm();
      case LT_EQ:
        return term() + " <= " + rightTerm();
      case GT:
        return term() + " > " + rightTerm();
      case GT_EQ:
        return term() + " >= " + rightTerm();
      case EQ:
        return term() + " == " + rightTerm();
      case NOT_EQ:
        return term() + " != " + rightTerm();
      default:
        return "Invalid term predicate: operation = " + op();
    }
  }
}
