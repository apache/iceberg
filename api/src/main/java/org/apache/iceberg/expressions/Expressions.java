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
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.iceberg.expressions.Expression.Operation;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.util.NaNUtil;

/** Factory methods for creating {@link Expression expressions}. */
public class Expressions {
  private Expressions() {}

  public static Expression and(Expression left, Expression right) {
    Preconditions.checkNotNull(left, "Left expression cannot be null.");
    Preconditions.checkNotNull(right, "Right expression cannot be null.");
    if (left == alwaysFalse() || right == alwaysFalse()) {
      return alwaysFalse();
    } else if (left == alwaysTrue()) {
      return right;
    } else if (right == alwaysTrue()) {
      return left;
    } else if (isContradiction(left, right)) {
      return alwaysFalse();
    }
    return new And(left, right);
  }

  public static Expression and(Expression left, Expression right, Expression... expressions) {
    return Stream.of(expressions).reduce(and(left, right), Expressions::and);
  }

  public static Expression or(Expression left, Expression right) {
    Preconditions.checkNotNull(left, "Left expression cannot be null.");
    Preconditions.checkNotNull(right, "Right expression cannot be null.");
    if (left == alwaysTrue() || right == alwaysTrue()) {
      return alwaysTrue();
    } else if (left == alwaysFalse()) {
      return right;
    } else if (right == alwaysFalse()) {
      return left;
    }
    return new Or(left, right);
  }

  public static Expression not(Expression child) {
    Preconditions.checkNotNull(child, "Child expression cannot be null.");
    if (child == alwaysTrue()) {
      return alwaysFalse();
    } else if (child == alwaysFalse()) {
      return alwaysTrue();
    } else if (child instanceof Not) {
      return ((Not) child).child();
    }
    return new Not(child);
  }

  @SuppressWarnings("unchecked")
  public static <T> UnboundTerm<T> bucket(String name, int numBuckets) {
    Transform<?, T> transform = (Transform<?, T>) Transforms.bucket(numBuckets);
    return new UnboundTransform<>(ref(name), transform);
  }

  @SuppressWarnings("unchecked")
  public static <T> UnboundTerm<T> year(String name) {
    return new UnboundTransform<>(ref(name), (Transform<?, T>) Transforms.year());
  }

  @SuppressWarnings("unchecked")
  public static <T> UnboundTerm<T> month(String name) {
    return new UnboundTransform<>(ref(name), (Transform<?, T>) Transforms.month());
  }

  @SuppressWarnings("unchecked")
  public static <T> UnboundTerm<T> day(String name) {
    return new UnboundTransform<>(ref(name), (Transform<?, T>) Transforms.day());
  }

  @SuppressWarnings("unchecked")
  public static <T> UnboundTerm<T> hour(String name) {
    return new UnboundTransform<>(ref(name), (Transform<?, T>) Transforms.hour());
  }

  public static <T> UnboundTerm<T> truncate(String name, int width) {
    return new UnboundTransform<>(ref(name), Transforms.truncate(width));
  }

  public static <T> UnboundTerm<T> extract(String name, String path, String type) {
    return new UnboundExtract<>(ref(name), path, type);
  }

  public static <T> UnboundPredicate<T> isNull(String name) {
    return new UnboundPredicate<>(Expression.Operation.IS_NULL, ref(name));
  }

  public static <T> UnboundPredicate<T> isNull(UnboundTerm<T> expr) {
    return new UnboundPredicate<>(Expression.Operation.IS_NULL, expr);
  }

  public static <T> UnboundPredicate<T> notNull(String name) {
    return new UnboundPredicate<>(Expression.Operation.NOT_NULL, ref(name));
  }

  public static <T> UnboundPredicate<T> notNull(UnboundTerm<T> expr) {
    return new UnboundPredicate<>(Expression.Operation.NOT_NULL, expr);
  }

  public static <T> UnboundPredicate<T> isNaN(String name) {
    return new UnboundPredicate<>(Expression.Operation.IS_NAN, ref(name));
  }

  public static <T> UnboundPredicate<T> isNaN(UnboundTerm<T> expr) {
    return new UnboundPredicate<>(Expression.Operation.IS_NAN, expr);
  }

  public static <T> UnboundPredicate<T> notNaN(String name) {
    return new UnboundPredicate<>(Expression.Operation.NOT_NAN, ref(name));
  }

  public static <T> UnboundPredicate<T> notNaN(UnboundTerm<T> expr) {
    return new UnboundPredicate<>(Expression.Operation.NOT_NAN, expr);
  }

  public static <T> UnboundPredicate<T> lessThan(String name, T value) {
    return new UnboundPredicate<>(Expression.Operation.LT, ref(name), value);
  }

  public static <T> UnboundPredicate<T> lessThan(UnboundTerm<T> expr, T value) {
    return new UnboundPredicate<>(Expression.Operation.LT, expr, value);
  }

  public static <T> UnboundPredicate<T> lessThanOrEqual(String name, T value) {
    return new UnboundPredicate<>(Expression.Operation.LT_EQ, ref(name), value);
  }

  public static <T> UnboundPredicate<T> lessThanOrEqual(UnboundTerm<T> expr, T value) {
    return new UnboundPredicate<>(Expression.Operation.LT_EQ, expr, value);
  }

  public static <T> UnboundPredicate<T> greaterThan(String name, T value) {
    return new UnboundPredicate<>(Expression.Operation.GT, ref(name), value);
  }

  public static <T> UnboundPredicate<T> greaterThan(UnboundTerm<T> expr, T value) {
    return new UnboundPredicate<>(Expression.Operation.GT, expr, value);
  }

  public static <T> UnboundPredicate<T> greaterThanOrEqual(String name, T value) {
    return new UnboundPredicate<>(Expression.Operation.GT_EQ, ref(name), value);
  }

  public static <T> UnboundPredicate<T> greaterThanOrEqual(UnboundTerm<T> expr, T value) {
    return new UnboundPredicate<>(Expression.Operation.GT_EQ, expr, value);
  }

  public static <T> UnboundPredicate<T> equal(String name, T value) {
    return new UnboundPredicate<>(Expression.Operation.EQ, ref(name), value);
  }

  public static <T> UnboundPredicate<T> equal(UnboundTerm<T> expr, T value) {
    return new UnboundPredicate<>(Expression.Operation.EQ, expr, value);
  }

  public static <T> UnboundPredicate<T> notEqual(String name, T value) {
    return new UnboundPredicate<>(Expression.Operation.NOT_EQ, ref(name), value);
  }

  public static <T> UnboundPredicate<T> notEqual(UnboundTerm<T> expr, T value) {
    return new UnboundPredicate<>(Expression.Operation.NOT_EQ, expr, value);
  }

  public static UnboundPredicate<String> startsWith(String name, String value) {
    return new UnboundPredicate<>(Expression.Operation.STARTS_WITH, ref(name), value);
  }

  public static UnboundPredicate<String> startsWith(UnboundTerm<String> expr, String value) {
    return new UnboundPredicate<>(Expression.Operation.STARTS_WITH, expr, value);
  }

  public static UnboundPredicate<String> notStartsWith(String name, String value) {
    return new UnboundPredicate<>(Expression.Operation.NOT_STARTS_WITH, ref(name), value);
  }

  public static UnboundPredicate<String> notStartsWith(UnboundTerm<String> expr, String value) {
    return new UnboundPredicate<>(Expression.Operation.NOT_STARTS_WITH, expr, value);
  }

  public static <T> UnboundPredicate<T> in(String name, T... values) {
    return predicate(Operation.IN, name, Lists.newArrayList(values));
  }

  public static <T> UnboundPredicate<T> in(UnboundTerm<T> expr, T... values) {
    return predicate(Operation.IN, expr, Lists.newArrayList(values));
  }

  public static <T> UnboundPredicate<T> in(String name, Iterable<T> values) {
    Preconditions.checkNotNull(values, "Values cannot be null for IN predicate.");
    return predicate(Operation.IN, ref(name), values);
  }

  public static <T> UnboundPredicate<T> in(UnboundTerm<T> expr, Iterable<T> values) {
    Preconditions.checkNotNull(values, "Values cannot be null for IN predicate.");
    return predicate(Operation.IN, expr, values);
  }

  public static <T> UnboundPredicate<T> notIn(String name, T... values) {
    return predicate(Operation.NOT_IN, name, Lists.newArrayList(values));
  }

  public static <T> UnboundPredicate<T> notIn(UnboundTerm<T> expr, T... values) {
    return predicate(Operation.NOT_IN, expr, Lists.newArrayList(values));
  }

  public static <T> UnboundPredicate<T> notIn(String name, Iterable<T> values) {
    Preconditions.checkNotNull(values, "Values cannot be null for NOT_IN predicate.");
    return predicate(Operation.NOT_IN, name, values);
  }

  public static <T> UnboundPredicate<T> notIn(UnboundTerm<T> expr, Iterable<T> values) {
    Preconditions.checkNotNull(values, "Values cannot be null for NOT_IN predicate.");
    return predicate(Operation.NOT_IN, expr, values);
  }

  public static <T> UnboundPredicate<T> predicate(Operation op, String name, T value) {
    return predicate(op, name, Literals.from(value));
  }

  public static <T> UnboundPredicate<T> predicate(Operation op, String name, Literal<T> lit) {
    Preconditions.checkArgument(
        op != Operation.IS_NULL
            && op != Operation.NOT_NULL
            && op != Operation.IS_NAN
            && op != Operation.NOT_NAN,
        "Cannot create %s predicate inclusive a value",
        op);
    Preconditions.checkArgument(
        !NaNUtil.isNaN(lit.value()),
        "Invalid expression literal: NaN, use isNaN or notNaN instead");
    return new UnboundPredicate<T>(op, ref(name), lit);
  }

  public static <T> UnboundPredicate<T> predicate(Operation op, String name, Iterable<T> values) {
    return predicate(op, ref(name), values);
  }

  public static <T> UnboundPredicate<T> predicate(Operation op, String name) {
    Preconditions.checkArgument(
        op == Operation.IS_NULL
            || op == Operation.NOT_NULL
            || op == Operation.IS_NAN
            || op == Operation.NOT_NAN,
        "Cannot create %s predicate without a value",
        op);
    return new UnboundPredicate<>(op, ref(name));
  }

  public static <T> UnboundPredicate<T> predicate(
      Operation op, UnboundTerm<T> expr, Iterable<T> values) {
    return new UnboundPredicate<>(op, expr, values);
  }

  public static <T> UnboundPredicate<T> predicate(Operation op, UnboundTerm<T> expr) {
    return new UnboundPredicate<>(op, expr);
  }

  /**
   * Checks whether two expressions form a logical contradiction (always false when ANDed).
   *
   * <p>Detects patterns like {@code col == 'a' AND col != 'a'} or {@code col == 'a' AND col ==
   * 'b'} that can never be satisfied. This enables early pruning of scans that would otherwise plan
   * file tasks only to discard every row.
   */
  private static boolean isContradiction(Expression left, Expression right) {
    // Both are simple predicates
    if (left instanceof Predicate && right instanceof Predicate) {
      return arePredicatesContradictory(left, right);
    }

    // Left is And, right is a predicate: contradiction if right contradicts any leaf of left
    if (left instanceof And && right instanceof Predicate) {
      And and = (And) left;
      return isContradiction(and.left(), right) || isContradiction(and.right(), right);
    }

    // Right is And, left is a predicate: symmetric case
    if (right instanceof And && left instanceof Predicate) {
      And and = (And) right;
      return isContradiction(left, and.left()) || isContradiction(left, and.right());
    }

    return false;
  }

  private static boolean arePredicatesContradictory(Expression left, Expression right) {
    // Bound literal vs bound literal (post-binding path)
    if (left instanceof BoundLiteralPredicate && right instanceof BoundLiteralPredicate) {
      return areBoundLiteralsContradictory(
          (BoundLiteralPredicate<?>) left, (BoundLiteralPredicate<?>) right);
    }

    // Bound literal vs bound set (e.g., EQ vs NOT_IN)
    if (left instanceof BoundLiteralPredicate && right instanceof BoundSetPredicate) {
      return isBoundLiteralSetContradiction(
          (BoundLiteralPredicate<?>) left, (BoundSetPredicate<?>) right);
    }

    if (left instanceof BoundSetPredicate && right instanceof BoundLiteralPredicate) {
      return isBoundLiteralSetContradiction(
          (BoundLiteralPredicate<?>) right, (BoundSetPredicate<?>) left);
    }

    // Unbound predicates (pre-binding path, used by SparkScanBuilder)
    if (left instanceof UnboundPredicate && right instanceof UnboundPredicate) {
      return areUnboundContradictory(
          (UnboundPredicate<?>) left, (UnboundPredicate<?>) right);
    }

    return false;
  }

  @SuppressWarnings("unchecked")
  private static boolean areBoundLiteralsContradictory(
      BoundLiteralPredicate<?> left, BoundLiteralPredicate<?> right) {
    if (left.ref().fieldId() != right.ref().fieldId()) {
      return false;
    }

    Comparator<Object> cmp = (Comparator<Object>) left.literal().comparator();
    int comparison = cmp.compare(left.literal().value(), right.literal().value());

    Operation leftOp = left.op();
    Operation rightOp = right.op();

    // EQ(col, a) AND NOT_EQ(col, a) with same value
    if (comparison == 0) {
      return (leftOp == Operation.EQ && rightOp == Operation.NOT_EQ)
          || (leftOp == Operation.NOT_EQ && rightOp == Operation.EQ);
    }

    // EQ(col, a) AND EQ(col, b) with different values
    return leftOp == Operation.EQ && rightOp == Operation.EQ;
  }

  @SuppressWarnings("unchecked")
  private static boolean isBoundLiteralSetContradiction(
      BoundLiteralPredicate<?> literal, BoundSetPredicate<?> set) {
    if (literal.ref().fieldId() != set.ref().fieldId()) {
      return false;
    }

    Object value = literal.literal().value();

    // EQ(col, val) AND NOT_IN(col, {val, ...}) => false
    if (literal.op() == Operation.EQ && set.op() == Operation.NOT_IN) {
      return set.literalSet().contains(value);
    }

    // EQ(col, val) AND IN(col, set) where val not in set => false
    if (literal.op() == Operation.EQ && set.op() == Operation.IN) {
      return !set.literalSet().contains(value);
    }

    return false;
  }

  private static boolean areUnboundContradictory(
      UnboundPredicate<?> left, UnboundPredicate<?> right) {
    // Only handle simple column references, not transforms
    if (!(left.term() instanceof NamedReference) || !(right.term() instanceof NamedReference)) {
      return false;
    }

    String leftName = ((NamedReference<?>) left.term()).name();
    String rightName = ((NamedReference<?>) right.term()).name();
    if (!leftName.equals(rightName)) {
      return false;
    }

    // Both must have literals
    if (left.literals() == null || right.literals() == null) {
      return false;
    }

    Operation leftOp = left.op();
    Operation rightOp = right.op();
    boolean leftIsSingle = leftOp != Operation.IN && leftOp != Operation.NOT_IN;
    boolean rightIsSingle = rightOp != Operation.IN && rightOp != Operation.NOT_IN;

    if (leftIsSingle && rightIsSingle) {
      Object leftVal = left.literal().value();
      Object rightVal = right.literal().value();
      boolean sameValue = Objects.equals(leftVal, rightVal);

      // EQ AND NOT_EQ with same value
      if (sameValue
          && ((leftOp == Operation.EQ && rightOp == Operation.NOT_EQ)
              || (leftOp == Operation.NOT_EQ && rightOp == Operation.EQ))) {
        return true;
      }

      // EQ AND EQ with different values
      if (!sameValue && leftOp == Operation.EQ && rightOp == Operation.EQ) {
        return true;
      }
    }

    return false;
  }

  public static True alwaysTrue() {
    return True.INSTANCE;
  }

  public static False alwaysFalse() {
    return False.INSTANCE;
  }

  public static Expression rewriteNot(Expression expr) {
    return ExpressionVisitors.visit(expr, RewriteNot.get());
  }

  /**
   * Constructs a reference for a given column.
   *
   * <p>The following are equivalent: equals("a", 5) and equals(ref("a"), 5).
   *
   * @param name a column name
   * @param <T> the Java type of this reference
   * @return a named reference
   */
  public static <T> NamedReference<T> ref(String name) {
    return new NamedReference<>(name);
  }

  /**
   * Constructs a transform expression for a given column.
   *
   * @param name a column name
   * @param transform a transform function
   * @param <T> the Java type of this term
   * @return an unbound transform expression
   */
  public static <T> UnboundTerm<T> transform(String name, Transform<?, T> transform) {
    return new UnboundTransform<>(ref(name), transform);
  }

  /**
   * Create a {@link Literal} from an Object.
   *
   * @param value a value
   * @param <T> Java type of value
   * @return a Literal for the given value
   * @throws IllegalArgumentException if the value has no literal implementation
   */
  public static <T> Literal<T> lit(T value) {
    return Literals.from(value);
  }

  /**
   * Create a {@link Literal} from a microsecond value.
   *
   * @param micros a timestamp in microseconds from the unix epoch
   * @return a literal representing the timestamp
   */
  public static Literal<Long> micros(long micros) {
    return new Literals.TimestampLiteral(micros);
  }

  /**
   * Create a {@link Literal} from a millisecond value.
   *
   * @param millis a timestamp in milliseconds from the unix epoch
   * @return a literal representing the timestamp
   */
  public static Literal<Long> millis(long millis) {
    return new Literals.TimestampLiteral(millis * 1000);
  }

  /**
   * Create a {@link Literal} from a nanosecond value.
   *
   * @param nanos a timestamp in nanoseconds from the unix epoch
   * @return a literal representing the timestamp
   */
  public static Literal<Long> nanos(long nanos) {
    return new Literals.TimestampNanoLiteral(nanos);
  }

  public static <T> UnboundAggregate<T> count(String name) {
    return new UnboundAggregate<>(Operation.COUNT, ref(name));
  }

  public static <T> UnboundAggregate<T> countNull(String name) {
    return new UnboundAggregate<>(Operation.COUNT_NULL, ref(name));
  }

  public static <T> UnboundAggregate<T> countStar() {
    return new UnboundAggregate<>(Operation.COUNT_STAR, null);
  }

  public static <T> UnboundAggregate<T> max(String name) {
    return new UnboundAggregate<>(Operation.MAX, ref(name));
  }

  public static <T> UnboundAggregate<T> min(String name) {
    return new UnboundAggregate<>(Operation.MIN, ref(name));
  }
}
