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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.NaNUtil;
import org.apache.iceberg.util.SerializableFunction;

/**
 * Evaluates bound predicates and value expressions using the semantics of the Iceberg expressions
 * spec.
 *
 * <p>Predicates produce true or false. Comparisons are null-safe: {@code a = b} is true when both
 * operands are null, and {@code a < b} is false when either operand is null. NaN values follow the
 * same rules as null. Floating point values use IEEE 754 order, so {@code -0.0 = 0.0}, and UUIDs
 * use unsigned byte order.
 *
 * <p>Operands with different types are aligned using type promotion, and a literal operand is
 * converted to the other operand's type when its value converts exactly. Types that cannot be
 * aligned are rejected when the evaluator is created.
 *
 * <p>Function calls are evaluated using the implementation attached to each bound function
 * reference. {@code iceberg_functions.if_else} evaluates only the selected branch.
 */
public class ExpressionEvaluator implements Serializable {
  private final Expression expr;
  private final boolean isPredicate;
  private transient volatile RowFunction compiled = null;

  /**
   * Creates an evaluator for a bound predicate or a bound value expression.
   *
   * @param expr a bound expression
   * @throws ValidationException if operand types cannot be aligned or are not valid for an
   *     operation
   * @throws UnsupportedOperationException if the expression is unbound or calls a function that has
   *     no implementation
   */
  public ExpressionEvaluator(Expression expr) {
    Preconditions.checkArgument(expr != null, "Invalid expression: null");
    this.expr = expr;
    this.isPredicate = ExpressionUtil.isPredicate(expr);
    this.compiled = compile(expr);
  }

  /**
   * Tests whether a row satisfies the predicate.
   *
   * @param row a row
   * @return true if the row satisfies the predicate, false otherwise
   * @throws IllegalStateException if the expression is a value expression
   */
  public boolean test(StructLike row) {
    Preconditions.checkState(isPredicate, "Cannot test a value expression: %s", expr);
    return isTrue(compiled().eval(row));
  }

  /**
   * Evaluates the expression for a row.
   *
   * @param row a row
   * @return the value produced by a value expression, or a {@link Boolean} for a predicate
   */
  public Object evaluate(StructLike row) {
    return compiled().eval(row);
  }

  private RowFunction compiled() {
    if (compiled == null) {
      this.compiled = compile(expr);
    }

    return compiled;
  }

  private static RowFunction compile(Expression expr) {
    return BoundExpressionTreeVisitor.visit(expr, new Compiler());
  }

  @FunctionalInterface
  private interface RowFunction {
    Object eval(StructLike row);
  }

  private record Aligned(Type type, RowFunction left, RowFunction right) {}

  private static boolean isTrue(Object value) {
    return Boolean.TRUE.equals(value);
  }

  private static class Compiler extends BoundExpressionTreeVisitor<RowFunction> {
    @Override
    public RowFunction alwaysTrue() {
      return row -> Boolean.TRUE;
    }

    @Override
    public RowFunction alwaysFalse() {
      return row -> Boolean.FALSE;
    }

    @Override
    public RowFunction not(Supplier<RowFunction> child) {
      RowFunction childFunc = child.get();
      return row -> !isTrue(childFunc.eval(row));
    }

    @Override
    public RowFunction and(Supplier<RowFunction> left, Supplier<RowFunction> right) {
      RowFunction leftFunc = left.get();
      RowFunction rightFunc = right.get();
      return row -> isTrue(leftFunc.eval(row)) && isTrue(rightFunc.eval(row));
    }

    @Override
    public RowFunction or(Supplier<RowFunction> left, Supplier<RowFunction> right) {
      RowFunction leftFunc = left.get();
      RowFunction rightFunc = right.get();
      return row -> isTrue(leftFunc.eval(row)) || isTrue(rightFunc.eval(row));
    }

    @Override
    public <T> RowFunction test(BoundPredicate<T> pred, Supplier<RowFunction> child) {
      RowFunction value = child.get();
      Type type = type(pred.asUnaryPredicate().child());
      return switch (pred.op()) {
        case IS_NULL -> row -> value.eval(row) == null;
        case NOT_NULL -> row -> value.eval(row) != null;
        case IS_NAN -> {
          checkFloatingPoint(type, pred);
          yield row -> NaNUtil.isNaN(value.eval(row));
        }
        case NOT_NAN -> {
          checkFloatingPoint(type, pred);
          yield row -> !NaNUtil.isNaN(value.eval(row));
        }
        default -> throw new UnsupportedOperationException("Unsupported test: " + pred);
      };
    }

    @Override
    public <T> RowFunction comparison(
        BoundPredicate<T> pred, Supplier<RowFunction> left, Supplier<RowFunction> right) {
      BoundLiteralPredicate<T> cmp = pred.asLiteralPredicate();
      Aligned aligned = align(cmp.left(), left.get(), cmp.right(), right.get(), pred);
      RowFunction leftFunc = aligned.left();
      RowFunction rightFunc = aligned.right();
      if (pred.op() == Expression.Operation.STARTS_WITH
          || pred.op() == Expression.Operation.NOT_STARTS_WITH) {
        ValidationException.check(
            aligned.type().typeId() == Type.TypeID.STRING,
            "Invalid comparison %s: starts with requires strings, got %s",
            pred,
            aligned.type());
        RowFunction startsWith = row -> startsWith(leftFunc.eval(row), rightFunc.eval(row));
        return pred.op() == Expression.Operation.STARTS_WITH
            ? startsWith
            : row -> !isTrue(startsWith.eval(row));
      }

      Comparator<Object> comparator = comparator(aligned.type(), pred);
      return switch (pred.op()) {
        case EQ -> row -> isEqual(comparator, leftFunc.eval(row), rightFunc.eval(row));
        case NOT_EQ -> row -> !isEqual(comparator, leftFunc.eval(row), rightFunc.eval(row));
        case LT -> row -> isLess(comparator, leftFunc.eval(row), rightFunc.eval(row));
        case GT -> row -> isLess(comparator, rightFunc.eval(row), leftFunc.eval(row));
        case LT_EQ ->
            row -> {
              Object leftValue = leftFunc.eval(row);
              Object rightValue = rightFunc.eval(row);
              return isEqual(comparator, leftValue, rightValue)
                  || isLess(comparator, leftValue, rightValue);
            };
        case GT_EQ ->
            row -> {
              Object leftValue = leftFunc.eval(row);
              Object rightValue = rightFunc.eval(row);
              return isEqual(comparator, leftValue, rightValue)
                  || isLess(comparator, rightValue, leftValue);
            };
        default -> throw new UnsupportedOperationException("Unsupported comparison: " + pred);
      };
    }

    @Override
    public <T> RowFunction setTest(BoundPredicate<T> pred, Supplier<RowFunction> child) {
      BoundSetPredicate<T> setPred = pred.asSetPredicate();
      Type childType = type(setPred.child());
      RowFunction value = child.get();
      Type setType = childType;
      Collection<?> values = setPred.literalSet();

      List<Literal<T>> literals = setPred.literals();
      if (literals != null && !literals.isEmpty() && !isUnknown(childType)) {
        List<Object> converted = convertExactly(literals, childType);
        if (converted != null) {
          values = converted;
        } else {
          Type literalType = literals.get(0).type();
          ValidationException.check(
              literals.stream().allMatch(lit -> literalType.equals(lit.type()))
                  && isPromotionAllowed(childType, literalType),
              "Invalid set test %s: cannot align %s with the set values",
              pred,
              childType);
          setType = literalType;
          value = promote(value, childType);
        }
      }

      NavigableSet<Object> set = new TreeSet<>(comparator(setType, pred));
      set.addAll(values);
      RowFunction finalValue = value;
      RowFunction in =
          row -> {
            Object val = finalValue.eval(row);
            return val != null && !NaNUtil.isNaN(val) && set.contains(val);
          };
      return switch (pred.op()) {
        case IN -> in;
        case NOT_IN -> row -> !isTrue(in.eval(row));
        default -> throw new UnsupportedOperationException("Unsupported set test: " + pred);
      };
    }

    @Override
    public <T> RowFunction literal(Literal<T> literal) {
      Object value = literal.value();
      return row -> value;
    }

    @Override
    public <T> RowFunction reference(BoundReference<T> ref) {
      return ref::eval;
    }

    @Override
    public <S, T> RowFunction transform(
        BoundTransform<S, T> transform, Supplier<RowFunction> child) {
      return transform::eval;
    }

    @Override
    public <T> RowFunction extract(BoundExtract<T> extract, Supplier<RowFunction> child) {
      return extract::eval;
    }

    @Override
    public <T> RowFunction apply(BoundApply<T> apply, List<Supplier<RowFunction>> arguments) {
      List<RowFunction> args = arguments.stream().map(Supplier::get).toList();
      if (IcebergFunctions.isIfElse(apply.function())) {
        RowFunction condition = args.get(0);
        RowFunction whenTrue = args.get(1);
        RowFunction whenFalse = args.get(2);
        return row -> isTrue(condition.eval(row)) ? whenTrue.eval(row) : whenFalse.eval(row);
      }

      SerializableFunction<List<Object>, ?> implementation = apply.function().implementation();
      if (implementation == null) {
        throw new UnsupportedOperationException(
            "Cannot evaluate function without an implementation: " + apply.function());
      }

      return row -> {
        Object[] values = new Object[args.size()];
        for (int i = 0; i < values.length; i += 1) {
          values[i] = args.get(i).eval(row);
        }

        return implementation.apply(Arrays.asList(values));
      };
    }
  }

  private static Aligned align(
      BoundExpression<?> leftExpr,
      RowFunction left,
      BoundExpression<?> rightExpr,
      RowFunction right,
      Expression pred) {
    Type leftType = type(leftExpr);
    Type rightType = type(rightExpr);
    if (leftType.equals(rightType) || isUnknown(rightType) || bothDecimal(leftType, rightType)) {
      return new Aligned(leftType, left, right);
    } else if (isUnknown(leftType)) {
      return new Aligned(rightType, left, right);
    }

    Object convertedRight =
        rightExpr instanceof Literal<?> lit ? convertExactly(lit, leftType) : null;
    if (convertedRight != null) {
      return new Aligned(leftType, left, row -> convertedRight);
    }

    Object convertedLeft =
        leftExpr instanceof Literal<?> lit ? convertExactly(lit, rightType) : null;
    if (convertedLeft != null) {
      return new Aligned(rightType, row -> convertedLeft, right);
    } else if (isPromotionAllowed(leftType, rightType)) {
      return new Aligned(rightType, promote(left, leftType), right);
    } else if (isPromotionAllowed(rightType, leftType)) {
      return new Aligned(leftType, left, promote(right, rightType));
    }

    throw new ValidationException(
        "Invalid comparison %s: cannot align %s and %s", pred, leftType, rightType);
  }

  private static Object convertExactly(Literal<?> literal, Type type) {
    Literal<?> converted = literal.to(type);
    if (converted == null
        || converted instanceof Literals.AboveMax
        || converted instanceof Literals.BelowMin) {
      return null;
    }

    return converted.value();
  }

  private static <T> List<Object> convertExactly(List<Literal<T>> literals, Type type) {
    List<Object> values = Lists.newArrayListWithExpectedSize(literals.size());
    for (Literal<T> literal : literals) {
      Object value = convertExactly(literal, type);
      if (value == null) {
        return null;
      }

      values.add(value);
    }

    return values;
  }

  private static boolean isPromotionAllowed(Type from, Type to) {
    return to.isPrimitiveType() && TypeUtil.isPromotionAllowed(from, to.asPrimitiveType());
  }

  private static RowFunction promote(RowFunction func, Type from) {
    return switch (from.typeId()) {
      case INTEGER -> row -> func.eval(row) instanceof Integer value ? value.longValue() : null;
      case FLOAT -> row -> func.eval(row) instanceof Float value ? value.doubleValue() : null;
      default -> func;
    };
  }

  private static boolean isUnknown(Type type) {
    return type.typeId() == Type.TypeID.UNKNOWN;
  }

  private static boolean bothDecimal(Type left, Type right) {
    return left.typeId() == Type.TypeID.DECIMAL && right.typeId() == Type.TypeID.DECIMAL;
  }

  private static Type type(BoundExpression<?> expr) {
    if (expr instanceof BoundValueExpression<?> value) {
      return value.type();
    }

    throw new ValidationException("Cannot determine the type of operand: %s", expr);
  }

  private static void checkFloatingPoint(Type type, Expression pred) {
    ValidationException.check(
        type.typeId() == Type.TypeID.FLOAT || type.typeId() == Type.TypeID.DOUBLE,
        "Invalid test %s: NaN tests require float or double, got %s",
        pred,
        type);
  }

  @SuppressWarnings("unchecked")
  private static Comparator<Object> comparator(Type type, Expression pred) {
    switch (type.typeId()) {
      case FLOAT:
      case DOUBLE:
        return ExpressionEvaluator::compareFloatingPoint;
      case UUID:
        return ExpressionEvaluator::compareUnsigned;
      case GEOMETRY:
      case GEOGRAPHY:
      case VARIANT:
        throw new ValidationException("Invalid predicate %s: cannot compare %s values", pred, type);
      default:
        ValidationException.check(
            type.isPrimitiveType(), "Invalid predicate %s: cannot compare %s values", pred, type);
        return (Comparator<Object>) Comparators.forType(type.asPrimitiveType());
    }
  }

  // NaN values are handled before comparing
  private static int compareFloatingPoint(Object left, Object right) {
    double leftValue = ((Number) left).doubleValue();
    double rightValue = ((Number) right).doubleValue();
    if (leftValue < rightValue) {
      return -1;
    } else if (leftValue > rightValue) {
      return 1;
    }

    return 0;
  }

  private static int compareUnsigned(Object left, Object right) {
    UUID leftValue = (UUID) left;
    UUID rightValue = (UUID) right;
    int cmp =
        Long.compareUnsigned(
            leftValue.getMostSignificantBits(), rightValue.getMostSignificantBits());
    if (cmp != 0) {
      return cmp;
    }

    return Long.compareUnsigned(
        leftValue.getLeastSignificantBits(), rightValue.getLeastSignificantBits());
  }

  private static boolean isEqual(Comparator<Object> comparator, Object left, Object right) {
    if (left == null || right == null) {
      return left == null && right == null;
    }

    boolean leftIsNaN = NaNUtil.isNaN(left);
    boolean rightIsNaN = NaNUtil.isNaN(right);
    if (leftIsNaN || rightIsNaN) {
      return leftIsNaN && rightIsNaN;
    }

    return comparator.compare(left, right) == 0;
  }

  private static boolean isLess(Comparator<Object> comparator, Object left, Object right) {
    if (left == null || right == null || NaNUtil.isNaN(left) || NaNUtil.isNaN(right)) {
      return false;
    }

    return comparator.compare(left, right) < 0;
  }

  private static boolean startsWith(Object value, Object prefix) {
    if (value == null || prefix == null) {
      return false;
    }

    CharSequence str = (CharSequence) value;
    CharSequence pre = (CharSequence) prefix;
    if (pre.length() > str.length()) {
      return false;
    }

    for (int i = 0; i < pre.length(); i += 1) {
      if (str.charAt(i) != pre.charAt(i)) {
        return false;
      }
    }

    return true;
  }
}
