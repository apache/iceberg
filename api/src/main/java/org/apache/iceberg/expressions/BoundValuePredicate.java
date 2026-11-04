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

import java.util.Comparator;
import java.util.Set;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.util.NaNUtil;

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
  private transient volatile Comparator<T> comparator = null;

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
   * Evaluates this predicate against a struct.
   *
   * @throws UnsupportedOperationException if an operand cannot be evaluated
   */
  @Override
  public Boolean eval(StructLike struct) {
    T leftValue = left.eval(struct);
    return switch (op) {
      case IS_NULL -> leftValue == null;
      case NOT_NULL -> leftValue != null;
      case IS_NAN -> NaNUtil.isNaN(leftValue);
      case NOT_NAN -> !NaNUtil.isNaN(leftValue);
      case IN -> leftValue != null && literalSet.contains(leftValue);
      case NOT_IN -> leftValue == null || !literalSet.contains(leftValue);
      case STARTS_WITH -> startsWith(leftValue, right.eval(struct));
      case NOT_STARTS_WITH -> !startsWith(leftValue, right.eval(struct));
      default -> compare(leftValue, right.eval(struct));
    };
  }

  private static boolean startsWith(Object value, Object prefix) {
    return value != null && prefix != null && value.toString().startsWith(prefix.toString());
  }

  private boolean compare(T leftValue, T rightValue) {
    int cmp = comparator().compare(leftValue, rightValue);
    return switch (op) {
      case LT -> cmp < 0;
      case LT_EQ -> cmp <= 0;
      case GT -> cmp > 0;
      case GT_EQ -> cmp >= 0;
      case EQ -> cmp == 0;
      case NOT_EQ -> cmp != 0;
      default -> throw new IllegalStateException("Invalid operation for comparison: " + op);
    };
  }

  private Comparator<T> comparator() {
    if (comparator == null) {
      this.comparator = Comparators.<T>nullsFirst().thenComparing(valueComparator());
    }

    return comparator;
  }

  private Comparator<T> valueComparator() {
    Type type = typeOf(left) != null ? typeOf(left) : typeOf(right);
    if (type != null) {
      return Comparators.forType(type.asPrimitiveType());
    } else if (left instanceof Literal<T> literal) {
      return literal.comparator();
    } else if (right instanceof Literal<T> literal) {
      return literal.comparator();
    }

    throw new UnsupportedOperationException("Cannot compare untyped operands: " + this);
  }

  private static Type typeOf(BoundExpression<?> expr) {
    return expr instanceof BoundValueExpression<?> typed ? typed.type() : null;
  }

  @Override
  public String toString() {
    return UnboundValuePredicate.toString(op, left, right, literalSet);
  }
}
