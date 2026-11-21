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

import java.util.List;
import java.util.stream.Stream;
import org.apache.iceberg.catalog.CatalogObjectIdentifier;
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
   * Creates a predicate that tests a value expression, like {@code is_null(f(x))}.
   *
   * <p>A term, like a named reference or a transform, produces the same predicate as {@link
   * #predicate(Operation, UnboundTerm)}.
   *
   * @param op a unary test: {@code IS_NULL}, {@code NOT_NULL}, {@code IS_NAN}, or {@code NOT_NAN}
   * @param child the value expression to test
   * @param <T> the Java type of the tested value
   * @return an unbound predicate
   * @throws IllegalArgumentException if the operation is not a unary test
   */
  public static <T> UnboundPredicate<T> predicate(Operation op, ValueExpression<T> child) {
    Preconditions.checkArgument(
        UnboundPredicate.isUnaryTest(op), "Invalid operation for unary test: %s", op);
    if (child instanceof UnboundTerm<T> term) {
      return new UnboundPredicate<>(op, term);
    }

    return new UnboundPredicate<>(op, child);
  }

  /**
   * Creates a predicate that compares two value expressions, like {@code a < b}.
   *
   * <p>A term, like a named reference or a transform, compared with a literal produces the same
   * predicate as the term and literal factories, such as {@link #lessThan(UnboundTerm, Object)}.
   *
   * @param op a comparison: {@code LT}, {@code LT_EQ}, {@code GT}, {@code GT_EQ}, {@code EQ},
   *     {@code NOT_EQ}, {@code STARTS_WITH}, or {@code NOT_STARTS_WITH}
   * @param left the left value expression
   * @param right the right value expression
   * @param <T> the Java type of the compared values
   * @return an unbound predicate
   * @throws IllegalArgumentException if the operation is not a comparison or a literal is NaN
   */
  public static <T> UnboundPredicate<T> predicate(
      Operation op, ValueExpression<T> left, ValueExpression<T> right) {
    Preconditions.checkArgument(
        UnboundPredicate.isComparison(op), "Invalid operation for comparison: %s", op);
    Preconditions.checkArgument(
        !isNaNLiteral(left) && !isNaNLiteral(right),
        "Invalid expression literal: NaN, use isNaN or notNaN instead");
    if (left instanceof UnboundTerm<T> term && right instanceof Literal<T> lit) {
      return new UnboundPredicate<>(op, term, lit);
    }

    return new UnboundPredicate<>(op, left, right);
  }

  /**
   * Creates a predicate that tests whether a value expression is in a set of values, like {@code
   * f(x) IN (1, 2, 3)}.
   *
   * <p>A term, like a named reference or a transform, produces the same predicate as {@link
   * #predicate(Operation, UnboundTerm, Iterable)}.
   *
   * @param op a set test: {@code IN} or {@code NOT_IN}
   * @param child the value expression to test
   * @param values the set of values
   * @param <T> the Java type of the tested value
   * @return an unbound predicate
   * @throws IllegalArgumentException if the operation is not a set test or a value is invalid
   */
  public static <T> UnboundPredicate<T> predicate(
      Operation op, ValueExpression<T> child, Iterable<T> values) {
    Preconditions.checkArgument(
        UnboundPredicate.isSetTest(op), "Invalid operation for set test: %s", op);
    Preconditions.checkArgument(values != null, "Invalid values: null");
    if (child instanceof UnboundTerm<T> term) {
      return new UnboundPredicate<>(op, term, values);
    }

    List<Literal<T>> literals = Lists.newArrayList();
    for (T value : values) {
      literals.add(Literals.from(value));
    }

    return new UnboundPredicate<>(op, child, literals);
  }

  private static boolean isNaNLiteral(ValueExpression<?> expr) {
    return expr instanceof Literal<?> lit && NaNUtil.isNaN(lit.value());
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
   * Constructs a reference to a field by field ID.
   *
   * @param fieldId a field ID
   * @param <T> the Java type of this reference
   * @return an ID reference
   */
  public static <T> IdReference<T> ref(int fieldId) {
    return new IdReference<>(fieldId);
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
   * Creates a reference to a function in the catalog of the object that stores the expression.
   *
   * @param identifier zero or more namespace levels followed by the function name
   * @return a function reference
   * @throws IllegalArgumentException if the identifier is null or has no function name
   */
  public static FunctionReference function(CatalogObjectIdentifier identifier) {
    return new FunctionReference(null, identifier);
  }

  /**
   * Creates a reference to a function in a catalog.
   *
   * @param catalog a catalog name or a reserved name, like {@code iceberg_functions}; null refers
   *     to the catalog of the object that stores the expression
   * @param identifier zero or more namespace levels followed by the function name
   * @return a function reference
   * @throws IllegalArgumentException if the identifier is null or has no function name
   */
  public static FunctionReference function(String catalog, CatalogObjectIdentifier identifier) {
    return new FunctionReference(catalog, identifier);
  }

  /**
   * Constructs a function call.
   *
   * @param function a function reference
   * @param arguments value expressions or predicates passed to the function
   * @param <T> the Java type produced by the function
   * @return an unbound function call
   */
  public static <T> UnboundApply<T> apply(FunctionReference function, Expression... arguments) {
    return new UnboundApply<>(function, Lists.newArrayList(arguments));
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
   * @return a {@code timestamp} literal representing the timestamp
   */
  public static Literal<Long> micros(long micros) {
    return new Literals.TimestampLiteral(micros);
  }

  /**
   * Create a {@link Literal} from a millisecond value.
   *
   * @param millis a timestamp in milliseconds from the unix epoch
   * @return a {@code timestamp} literal representing the timestamp
   */
  public static Literal<Long> millis(long millis) {
    return new Literals.TimestampLiteral(millis * 1000);
  }

  /**
   * Create a {@link Literal} from a nanosecond value.
   *
   * @param nanos a timestamp in nanoseconds from the unix epoch
   * @return a {@code timestamp_ns} literal representing the timestamp
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
