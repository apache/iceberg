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
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * A bound predicate whose operands are bound value expressions, such as function applications or
 * references on both sides of a comparison.
 *
 * @param <T> the Java type of values compared by this predicate
 */
public class BoundValuePredicate<T> implements BoundExpression<Boolean> {
  private final Operation op;
  private final BoundExpression<T> left;
  private final BoundExpression<T> right;
  private final Set<T> literalSet;

  BoundValuePredicate(Operation op, BoundExpression<T> child) {
    this(op, child, null, null);
    Preconditions.checkArgument(
        UnboundValuePredicate.isUnaryTest(op), "Invalid operation for unary test: %s", op);
  }

  BoundValuePredicate(Operation op, BoundExpression<T> left, BoundExpression<T> right) {
    this(op, left, right, null);
    Preconditions.checkArgument(
        UnboundValuePredicate.isComparison(op), "Invalid operation for comparison: %s", op);
    Preconditions.checkArgument(right != null, "Invalid right operand: null");
  }

  BoundValuePredicate(Operation op, BoundExpression<T> child, Set<T> literalSet) {
    this(op, child, null, literalSet);
    Preconditions.checkArgument(
        UnboundValuePredicate.isSetTest(op), "Invalid operation for set test: %s", op);
    Preconditions.checkArgument(literalSet != null, "Invalid literal set: null");
  }

  private BoundValuePredicate(
      Operation op, BoundExpression<T> left, BoundExpression<T> right, Set<T> literalSet) {
    Preconditions.checkArgument(left != null, "Invalid operand: null");
    this.op = op;
    this.left = left;
    this.right = right;
    this.literalSet = literalSet;
  }

  @Override
  public Operation op() {
    return op;
  }

  /**
   * Returns the operand of a unary or set predicate.
   *
   * @throws IllegalArgumentException if this predicate is a comparison
   */
  public BoundExpression<T> child() {
    Preconditions.checkArgument(
        !UnboundValuePredicate.isComparison(op), "%s predicate does not have a child", op);
    return left;
  }

  /**
   * Returns the left operand of a comparison.
   *
   * @throws IllegalArgumentException if this predicate is not a comparison
   */
  public BoundExpression<T> left() {
    Preconditions.checkArgument(
        UnboundValuePredicate.isComparison(op), "%s predicate is not a comparison", op);
    return left;
  }

  /**
   * Returns the right operand of a comparison.
   *
   * @throws IllegalArgumentException if this predicate is not a comparison
   */
  public BoundExpression<T> right() {
    Preconditions.checkArgument(
        UnboundValuePredicate.isComparison(op), "%s predicate is not a comparison", op);
    return right;
  }

  /**
   * Returns the set of values tested by a set predicate.
   *
   * @throws IllegalArgumentException if this predicate is not a set test
   */
  public Set<T> literalSet() {
    Preconditions.checkArgument(
        UnboundValuePredicate.isSetTest(op), "%s predicate does not have a literal set", op);
    return literalSet;
  }

  @Override
  public Expression negate() {
    return new BoundValuePredicate<>(op.negate(), left, right, literalSet);
  }

  /**
   * Evaluating value predicates is not supported.
   *
   * @throws UnsupportedOperationException always
   */
  @Override
  public Boolean eval(StructLike struct) {
    throw new UnsupportedOperationException("Cannot evaluate value predicate: " + this);
  }

  @Override
  public String toString() {
    return UnboundValuePredicate.toString(op, left, right, literalSet);
  }
}
