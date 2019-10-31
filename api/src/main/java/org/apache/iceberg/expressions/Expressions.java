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
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.expressions.Expression.Operation;

/**
 * Factory methods for creating {@link Expression expressions}.
 */
public class Expressions {
  private Expressions() {
  }

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
    return Stream.of(expressions)
      .reduce(and(left, right), Expressions::and);
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

  public static <T> UnboundPredicate<T> isNull(String name) {
    return new UnboundPredicate<>(Expression.Operation.IS_NULL, ref(name));
  }

  public static <T> UnboundPredicate<T> notNull(String name) {
    return new UnboundPredicate<>(Expression.Operation.NOT_NULL, ref(name));
  }

  public static <T> UnboundPredicate<T> lessThan(String name, T value) {
    return new UnboundPredicate<>(Expression.Operation.LT, ref(name), value);
  }

  public static <T> UnboundPredicate<T> lessThanOrEqual(String name, T value) {
    return new UnboundPredicate<>(Expression.Operation.LT_EQ, ref(name), value);
  }

  public static <T> UnboundPredicate<T> greaterThan(String name, T value) {
    return new UnboundPredicate<>(Expression.Operation.GT, ref(name), value);
  }

  public static <T> UnboundPredicate<T> greaterThanOrEqual(String name, T value) {
    return new UnboundPredicate<>(Expression.Operation.GT_EQ, ref(name), value);
  }

  public static <T> UnboundPredicate<T> equal(String name, T value) {
    return new UnboundPredicate<>(Expression.Operation.EQ, ref(name), value);
  }

  public static <T> UnboundPredicate<T> notEqual(String name, T value) {
    return new UnboundPredicate<>(Expression.Operation.NOT_EQ, ref(name), value);
  }

  public static UnboundPredicate<String> startsWith(String name, String value) {
    return new UnboundPredicate<>(Expression.Operation.STARTS_WITH, ref(name), value);
  }

  public static <T> UnboundPredicate<T> in(String name, T... values) {
    return predicate(Operation.IN, name,
        Stream.of(values).map(Literals::from).collect(Collectors.toSet()));
  }

  public static <T> UnboundPredicate<T> in(String name, Collection<T> values) {
    Preconditions.checkNotNull(values, "Values cannot be null for IN predicate.");
    return predicate(Operation.IN, name,
        values.stream().map(Literals::from).collect(Collectors.toSet()));
  }

  public static <T> UnboundPredicate<T> notIn(String name, T... values) {
    return predicate(Operation.NOT_IN, name,
        Stream.of(values).map(Literals::from).collect(Collectors.toSet()));
  }

  public static <T> UnboundPredicate<T> notIn(String name, Collection<T> values) {
    Preconditions.checkNotNull(values, "Values cannot be null for NOT_IN predicate.");
    return predicate(Operation.NOT_IN, name,
        values.stream().map(Literals::from).collect(Collectors.toSet()));
  }

  public static <T> UnboundPredicate<T> predicate(Operation op, String name, T value) {
    return predicate(op, name, Literals.from(value));
  }

  public static <T> UnboundPredicate<T> predicate(Operation op, String name, Literal<T> lit) {
    Preconditions.checkArgument(op != Operation.IS_NULL && op != Operation.NOT_NULL,
        "Cannot create %s predicate inclusive a value", op);
    return predicate(op, name, Collections.singleton(lit));
  }

  private static <T> UnboundPredicate<T> predicate(Operation op, String name, Collection<Literal<T>> lits) {
    return new UnboundPredicate<>(op, ref(name), lits);
  }

  public static <T> UnboundPredicate<T> predicate(Operation op, String name) {
    Preconditions.checkArgument(op == Operation.IS_NULL || op == Operation.NOT_NULL,
        "Cannot create %s predicate without a value", op);
    return new UnboundPredicate<>(op, ref(name));
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

  static NamedReference ref(String name) {
    return new NamedReference(name);
  }
}
