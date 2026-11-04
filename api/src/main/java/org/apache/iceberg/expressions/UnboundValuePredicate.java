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
import java.util.Set;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;

/**
 * A predicate whose operands are value expressions, such as function applications or references on
 * both sides of a comparison.
 *
 * @param <T> the Java type of values compared by this predicate
 */
public class UnboundValuePredicate<T> implements Expression {
  private static final Joiner COMMA = Joiner.on(", ");

  private final Operation op;
  private final ValueExpression<T> left;
  private final ValueExpression<T> right;
  private final List<Literal<T>> literals;

  UnboundValuePredicate(Operation op, ValueExpression<T> child) {
    this(op, child, null, null);
    Preconditions.checkArgument(isUnaryTest(op), "Invalid operation for unary test: %s", op);
  }

  UnboundValuePredicate(Operation op, ValueExpression<T> left, ValueExpression<T> right) {
    this(op, left, right, null);
    Preconditions.checkArgument(isComparison(op), "Invalid operation for comparison: %s", op);
    Preconditions.checkArgument(right != null, "Invalid right operand: null");
  }

  UnboundValuePredicate(Operation op, ValueExpression<T> child, List<Literal<T>> values) {
    this(op, child, null, List.copyOf(values));
    Preconditions.checkArgument(isSetTest(op), "Invalid operation for set test: %s", op);
  }

  private UnboundValuePredicate(
      Operation op, ValueExpression<T> left, ValueExpression<T> right, List<Literal<T>> literals) {
    Preconditions.checkArgument(left != null, "Invalid operand: null");
    this.op = op;
    this.left = left;
    this.right = right;
    this.literals = literals;
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
  public ValueExpression<T> child() {
    Preconditions.checkArgument(!isComparison(op), "%s predicate does not have a child", op);
    return left;
  }

  /**
   * Returns the left operand of a comparison.
   *
   * @throws IllegalArgumentException if this predicate is not a comparison
   */
  public ValueExpression<T> left() {
    Preconditions.checkArgument(isComparison(op), "%s predicate is not a comparison", op);
    return left;
  }

  /**
   * Returns the right operand of a comparison.
   *
   * @throws IllegalArgumentException if this predicate is not a comparison
   */
  public ValueExpression<T> right() {
    Preconditions.checkArgument(isComparison(op), "%s predicate is not a comparison", op);
    return right;
  }

  /**
   * Returns the literals tested by a set predicate.
   *
   * @throws IllegalArgumentException if this predicate is not a set test
   */
  public List<Literal<T>> literals() {
    Preconditions.checkArgument(isSetTest(op), "%s predicate does not have literals", op);
    return literals;
  }

  @Override
  public Expression negate() {
    return new UnboundValuePredicate<>(op.negate(), left, right, literals);
  }

  /**
   * Binds this predicate to a struct, resolving function applications with a function binder.
   *
   * <p>Predicates that test a term against literals bind to the equivalent {@link BoundPredicate}.
   *
   * @param struct the struct type used to resolve references by name
   * @param caseSensitive whether references are resolved case sensitively
   * @param functionBinder a function binder, or null if the predicate has no function applications
   * @return a bound expression
   * @throws ValidationException if an operand cannot be bound or a literal cannot be converted
   */
  Expression bind(StructType struct, boolean caseSensitive, FunctionBinder functionBinder) {
    UnboundPredicate<T> termPredicate = asTermPredicate();
    if (termPredicate != null) {
      return termPredicate.bind(struct, caseSensitive);
    }

    BoundExpression<T> boundLeft = bindOperand(left, struct, caseSensitive, functionBinder);
    if (isUnaryTest(op)) {
      return bindUnary(boundLeft);
    } else if (isSetTest(op)) {
      return bindSet(boundLeft);
    }

    BoundExpression<T> boundRight = bindOperand(right, struct, caseSensitive, functionBinder);
    return bindComparison(convert(boundLeft, boundRight), convert(boundRight, boundLeft));
  }

  private UnboundPredicate<T> asTermPredicate() {
    if (!(left instanceof UnboundTerm<T> term)) {
      return null;
    }

    if (isUnaryTest(op)) {
      return new UnboundPredicate<>(op, term);
    } else if (isSetTest(op)) {
      return new UnboundPredicate<>(op, term, Lists.transform(literals, Literal::value));
    } else if (right instanceof Literal<T> literal) {
      return new UnboundPredicate<>(op, term, literal);
    }

    return null;
  }

  private Expression bindUnary(BoundExpression<T> child) {
    if (op == Operation.IS_NAN || op == Operation.NOT_NAN) {
      Type type = typeOf(child);
      ValidationException.check(
          type == null || type.equals(Types.FloatType.get()) || type.equals(Types.DoubleType.get()),
          "%s cannot be used with a non-floating-point expression: %s",
          op,
          child);
    }

    return new BoundValuePredicate<>(op, child);
  }

  private Expression bindSet(BoundExpression<T> child) {
    Type type = typeOf(child);
    List<Literal<T>> converted =
        type != null
            ? Lists.newArrayList(Lists.transform(literals, lit -> toType(lit, type)))
            : literals;
    if (converted.isEmpty()) {
      return op == Operation.IN ? Expressions.alwaysFalse() : Expressions.alwaysTrue();
    }

    Set<T> literalSet = UnboundPredicate.setOf(converted);
    if (literalSet.size() == 1) {
      Operation eqOp = op == Operation.IN ? Operation.EQ : Operation.NOT_EQ;
      return new BoundValuePredicate<>(eqOp, child, Iterables.getOnlyElement(converted));
    }

    return new BoundValuePredicate<>(op, child, literalSet);
  }

  private Expression bindComparison(BoundExpression<T> boundLeft, BoundExpression<T> boundRight) {
    if (op == Operation.STARTS_WITH || op == Operation.NOT_STARTS_WITH) {
      Type type = typeOf(boundLeft);
      ValidationException.check(
          type == null || type.equals(Types.StringType.get()),
          "%s must be applied to a string expression: %s",
          op,
          boundLeft);
    }

    return new BoundValuePredicate<>(op, boundLeft, boundRight);
  }

  private static <X> BoundExpression<X> bindOperand(
      ValueExpression<X> operand,
      StructType struct,
      boolean caseSensitive,
      FunctionBinder functionBinder) {
    if (operand instanceof Literal<X> literal) {
      return literal;
    } else if (operand instanceof UnboundTerm<X> term) {
      return term.bind(struct, caseSensitive);
    } else if (operand instanceof UnboundApply<X> apply) {
      ValidationException.check(
          functionBinder != null, "Cannot bind function without a function binder: %s", apply);
      return apply.bind(functionBinder, struct, caseSensitive);
    }

    throw new ValidationException("Cannot bind value expression: %s", operand);
  }

  private static <X> BoundExpression<X> convert(
      BoundExpression<X> operand, BoundExpression<X> other) {
    Type type = typeOf(other);
    if (operand instanceof Literal<X> literal && type != null) {
      return toType(literal, type);
    }

    return operand;
  }

  private static <X> Literal<X> toType(Literal<X> literal, Type type) {
    Literal<X> converted = literal.to(type);
    ValidationException.check(
        converted != null
            && converted != Literals.<X>aboveMax()
            && converted != Literals.<X>belowMin(),
        "Invalid value for conversion to type %s: %s",
        type,
        literal);
    return converted;
  }

  private static Type typeOf(BoundExpression<?> expr) {
    return expr instanceof BoundValueExpression<?> typed ? typed.type() : null;
  }

  @Override
  public String toString() {
    return toString(op, left, right, literals);
  }

  static String toString(Operation op, Object left, Object right, Iterable<?> literals) {
    return switch (op) {
      case IS_NULL -> "is_null(" + left + ")";
      case NOT_NULL -> "not_null(" + left + ")";
      case IS_NAN -> "is_nan(" + left + ")";
      case NOT_NAN -> "not_nan(" + left + ")";
      case LT -> left + " < " + right;
      case LT_EQ -> left + " <= " + right;
      case GT -> left + " > " + right;
      case GT_EQ -> left + " >= " + right;
      case EQ -> left + " == " + right;
      case NOT_EQ -> left + " != " + right;
      case STARTS_WITH -> left + " startsWith " + right;
      case NOT_STARTS_WITH -> left + " notStartsWith " + right;
      case IN -> left + " in (" + COMMA.join(literals) + ")";
      case NOT_IN -> left + " not in (" + COMMA.join(literals) + ")";
      default -> "Invalid value predicate: operation = " + op;
    };
  }

  static boolean isUnaryTest(Operation op) {
    return switch (op) {
      case IS_NULL, NOT_NULL, IS_NAN, NOT_NAN -> true;
      default -> false;
    };
  }

  static boolean isComparison(Operation op) {
    return switch (op) {
      case LT, LT_EQ, GT, GT_EQ, EQ, NOT_EQ, STARTS_WITH, NOT_STARTS_WITH -> true;
      default -> false;
    };
  }

  static boolean isSetTest(Operation op) {
    return op == Operation.IN || op == Operation.NOT_IN;
  }
}
