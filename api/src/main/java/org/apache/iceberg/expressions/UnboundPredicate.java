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
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.CharSequenceSet;

public class UnboundPredicate<T> extends Predicate<T, UnboundTerm<T>>
    implements Unbound<T, Expression> {
  private static final Joiner COMMA = Joiner.on(", ");

  private final List<Literal<T>> literals;
  private final ValueExpression<T> left;
  private final ValueExpression<T> right;

  UnboundPredicate(Operation op, UnboundTerm<T> term, T value) {
    this(op, term, Literals.from(value));
  }

  UnboundPredicate(Operation op, UnboundTerm<T> term) {
    super(op, term);
    this.literals = null;
    this.left = null;
    this.right = null;
  }

  UnboundPredicate(Operation op, UnboundTerm<T> term, Literal<T> lit) {
    super(op, term);
    this.literals = Lists.newArrayList(lit);
    this.left = null;
    this.right = null;
  }

  UnboundPredicate(Operation op, UnboundTerm<T> term, Iterable<T> values) {
    super(op, term);
    this.literals = Lists.newArrayList(Iterables.transform(values, Literals::from));
    this.left = null;
    this.right = null;
  }

  private UnboundPredicate(Operation op, UnboundTerm<T> term, List<Literal<T>> literals) {
    super(op, term);
    this.literals = literals;
    this.left = null;
    this.right = null;
  }

  UnboundPredicate(Operation op, ValueExpression<T> child) {
    super(op);
    Preconditions.checkArgument(isUnaryTest(op), "Invalid operation for unary test: %s", op);
    Preconditions.checkNotNull(child, "Child expression cannot be null");
    this.literals = null;
    this.left = child;
    this.right = null;
  }

  UnboundPredicate(Operation op, ValueExpression<T> left, ValueExpression<T> right) {
    super(op);
    Preconditions.checkArgument(isComparison(op), "Invalid operation for comparison: %s", op);
    Preconditions.checkNotNull(left, "Left expression cannot be null");
    Preconditions.checkNotNull(right, "Right expression cannot be null");
    this.literals = null;
    this.left = left;
    this.right = right;
  }

  UnboundPredicate(Operation op, ValueExpression<T> child, List<Literal<T>> values) {
    super(op);
    Preconditions.checkArgument(isSetTest(op), "Invalid operation for set test: %s", op);
    Preconditions.checkNotNull(child, "Child expression cannot be null");
    Preconditions.checkNotNull(values, "Values cannot be null");
    this.literals = Lists.newArrayList(values);
    this.left = child;
    this.right = null;
  }

  @Override
  public NamedReference<?> ref() {
    return requireTerm().ref();
  }

  @Override
  public Expression negate() {
    if (term() == null) {
      return super.negate();
    }

    return new UnboundPredicate<>(op().negate(), term(), literals);
  }

  public Literal<T> literal() {
    Preconditions.checkArgument(
        op() != Operation.IN && op() != Operation.NOT_IN,
        "%s predicate cannot return a literal",
        op());
    return literals == null ? null : Iterables.getOnlyElement(literals);
  }

  public List<Literal<T>> literals() {
    return literals;
  }

  /**
   * Returns the value expression tested by a unary or set predicate.
   *
   * @return the child value expression
   * @throws IllegalArgumentException if this predicate is a comparison
   */
  public ValueExpression<T> child() {
    Preconditions.checkArgument(
        isUnaryTest(op()) || isSetTest(op()), "%s predicate does not have a child", op());
    return term() != null ? term() : left;
  }

  /**
   * Returns the left operand of a comparison.
   *
   * @return the left value expression
   * @throws IllegalArgumentException if this predicate is not a comparison
   */
  public ValueExpression<T> left() {
    Preconditions.checkArgument(isComparison(op()), "%s predicate is not a comparison", op());
    return term() != null ? term() : left;
  }

  /**
   * Returns the right operand of a comparison.
   *
   * @return the right value expression
   * @throws IllegalArgumentException if this predicate is not a comparison
   */
  public ValueExpression<T> right() {
    Preconditions.checkArgument(isComparison(op()), "%s predicate is not a comparison", op());
    return term() != null ? literal() : right;
  }

  /**
   * Bind this UnboundPredicate, defaulting to case sensitive mode.
   *
   * <p>Access modifier is package-private, to only allow use from existing tests.
   *
   * @param struct The {@link StructType struct type} to resolve references by name.
   * @return an {@link Expression}
   * @throws ValidationException if literals do not match bound references, or if comparison on
   *     expression is invalid
   */
  Expression bind(StructType struct) {
    return bind(struct, true);
  }

  /**
   * Bind this UnboundPredicate.
   *
   * @param struct The {@link StructType struct type} to resolve references by name.
   * @param caseSensitive A boolean flag to control whether the bind should enforce case
   *     sensitivity.
   * @return an {@link Expression}
   * @throws ValidationException if literals do not match bound references, or if comparison on
   *     expression is invalid
   * @throws UnsupportedOperationException if this predicate does not have a term
   */
  @Override
  public Expression bind(StructType struct, boolean caseSensitive) {
    if (term() == null) {
      throw new UnsupportedOperationException(
          "Cannot bind predicate without a function binder: " + this);
    }

    BoundTerm<T> bound = term().bind(struct, caseSensitive);

    if (literals == null) {
      return bindUnaryOperation(struct, bound);
    }

    if (op() == Operation.IN || op() == Operation.NOT_IN) {
      return bindInOperation(bound);
    }

    return bindLiteralOperation(bound);
  }

  /**
   * Bind this UnboundPredicate, using a function binder to resolve function applications.
   *
   * @param struct The {@link StructType struct type} to resolve references by name.
   * @param caseSensitive A boolean flag to control whether the bind should enforce case
   *     sensitivity.
   * @param functionBinder a {@link FunctionBinder} used to resolve functions
   * @return an {@link Expression}
   * @throws ValidationException if literals do not match bound references, or if comparison on
   *     expression is invalid
   */
  public Expression bind(StructType struct, boolean caseSensitive, FunctionBinder functionBinder) {
    Preconditions.checkNotNull(functionBinder, "Function binder cannot be null");
    if (term() != null) {
      return bind(struct, caseSensitive);
    }

    BoundExpression<T> boundLeft = bindOperand(left, struct, caseSensitive, functionBinder);
    if (right != null) {
      return new BoundLiteralPredicate<>(
          op(), boundLeft, bindOperand(right, struct, caseSensitive, functionBinder));
    } else if (literals != null) {
      Set<T> literalSet = literals.isEmpty() ? Sets.newHashSet() : setOf(literals);
      return new BoundSetPredicate<>(op(), boundLeft, literalSet);
    } else {
      return new BoundUnaryPredicate<>(op(), boundLeft);
    }
  }

  private static <X> BoundExpression<X> bindOperand(
      ValueExpression<X> expr,
      StructType struct,
      boolean caseSensitive,
      FunctionBinder functionBinder) {
    if (expr instanceof Literal<X> literal) {
      return literal;
    } else if (expr instanceof UnboundTerm<X> term) {
      return term.bind(struct, caseSensitive);
    } else if (expr instanceof UnboundApply<X> apply) {
      return apply.bind(functionBinder, struct, caseSensitive);
    } else {
      throw new UnsupportedOperationException("Cannot bind value expression: " + expr);
    }
  }

  private Expression bindUnaryOperation(StructType struct, BoundTerm<T> boundTerm) {
    switch (op()) {
      case IS_NULL:
        if (!boundTerm.producesNull()
            && allAncestorFieldsAreRequired(struct, boundTerm.ref().fieldId())) {
          return Expressions.alwaysFalse();
        } else if (boundTerm.type().equals(Types.UnknownType.get())) {
          return Expressions.alwaysTrue();
        }
        return new BoundUnaryPredicate<>(Operation.IS_NULL, boundTerm);
      case NOT_NULL:
        if (!boundTerm.producesNull()
            && allAncestorFieldsAreRequired(struct, boundTerm.ref().fieldId())) {
          return Expressions.alwaysTrue();
        } else if (boundTerm.type().equals(Types.UnknownType.get())) {
          return Expressions.alwaysFalse();
        }
        return new BoundUnaryPredicate<>(Operation.NOT_NULL, boundTerm);
      case IS_NAN:
        if (floatingType(boundTerm.type().typeId())) {
          return new BoundUnaryPredicate<>(Operation.IS_NAN, boundTerm);
        } else {
          throw new ValidationException("IsNaN cannot be used with a non-floating-point column");
        }
      case NOT_NAN:
        if (floatingType(boundTerm.type().typeId())) {
          return new BoundUnaryPredicate<>(Operation.NOT_NAN, boundTerm);
        } else {
          throw new ValidationException("NotNaN cannot be used with a non-floating-point column");
        }
      default:
        throw new ValidationException("Operation must be IS_NULL, NOT_NULL, IS_NAN, or NOT_NAN");
    }
  }

  private boolean allAncestorFieldsAreRequired(StructType struct, int fieldId) {
    return TypeUtil.ancestorFields(struct.asSchema(), fieldId).stream()
        .allMatch(Types.NestedField::isRequired);
  }

  private boolean floatingType(Type.TypeID typeID) {
    return Type.TypeID.DOUBLE.equals(typeID) || Type.TypeID.FLOAT.equals(typeID);
  }

  private Expression bindLiteralOperation(BoundTerm<T> boundTerm) {
    if (op() == Operation.STARTS_WITH || op() == Operation.NOT_STARTS_WITH) {
      ValidationException.check(
          boundTerm.type().equals(Types.StringType.get()),
          "Term for STARTS_WITH or NOT_STARTS_WITH must produce a string: %s: %s",
          boundTerm,
          boundTerm.type());
    }

    Literal<T> lit = literal().to(boundTerm.type());

    if (lit == null) {
      throw new ValidationException(
          "Invalid value for conversion to type %s: %s (%s)",
          boundTerm.type(), literal().value(), literal().value().getClass().getName());

    } else if (lit == Literals.aboveMax()) {
      switch (op()) {
        case LT:
        case LT_EQ:
        case NOT_EQ:
          return Expressions.alwaysTrue();
        case GT:
        case GT_EQ:
        case EQ:
          return Expressions.alwaysFalse();
      }
    } else if (lit == Literals.belowMin()) {
      switch (op()) {
        case GT:
        case GT_EQ:
        case NOT_EQ:
          return Expressions.alwaysTrue();
        case LT:
        case LT_EQ:
        case EQ:
          return Expressions.alwaysFalse();
      }
    }

    // TODO: translate truncate(col) == value to startsWith(value)
    return new BoundLiteralPredicate<>(op(), boundTerm, lit);
  }

  private Expression bindInOperation(BoundTerm<T> boundTerm) {
    List<Literal<T>> convertedLiterals =
        Lists.newArrayList(
            Iterables.filter(
                Lists.transform(
                    literals,
                    lit -> {
                      Literal<T> converted = lit.to(boundTerm.type());
                      ValidationException.check(
                          converted != null,
                          "Invalid value for conversion to type %s: %s (%s)",
                          boundTerm.type(),
                          lit,
                          lit.getClass().getName());
                      return converted;
                    }),
                lit -> lit != Literals.aboveMax() && lit != Literals.belowMin()));

    if (convertedLiterals.isEmpty()) {
      switch (op()) {
        case IN:
          return Expressions.alwaysFalse();
        case NOT_IN:
          return Expressions.alwaysTrue();
        default:
          throw new ValidationException("Operation must be IN or NOT_IN");
      }
    }

    Set<T> literalSet = setOf(convertedLiterals);
    if (literalSet.size() == 1) {
      switch (op()) {
        case IN:
          return new BoundLiteralPredicate<>(
              Operation.EQ, boundTerm, Iterables.get(convertedLiterals, 0));
        case NOT_IN:
          return new BoundLiteralPredicate<>(
              Operation.NOT_EQ, boundTerm, Iterables.get(convertedLiterals, 0));
        default:
          throw new ValidationException("Operation must be IN or NOT_IN");
      }
    }

    return new BoundSetPredicate<>(op(), boundTerm, literalSet);
  }

  @Override
  public String toString() {
    switch (op()) {
      case IS_NULL:
        return "is_null(" + child() + ")";
      case NOT_NULL:
        return "not_null(" + child() + ")";
      case IS_NAN:
        return "is_nan(" + child() + ")";
      case NOT_NAN:
        return "not_nan(" + child() + ")";
      case LT:
        return left() + " < " + right();
      case LT_EQ:
        return left() + " <= " + right();
      case GT:
        return left() + " > " + right();
      case GT_EQ:
        return left() + " >= " + right();
      case EQ:
        return left() + " == " + right();
      case NOT_EQ:
        return left() + " != " + right();
      case STARTS_WITH:
        return left() + " startsWith \"" + right() + "\"";
      case NOT_STARTS_WITH:
        return left() + " notStartsWith \"" + right() + "\"";
      case IN:
        return child() + " in (" + COMMA.join(literals()) + ")";
      case NOT_IN:
        return child() + " not in (" + COMMA.join(literals()) + ")";
      default:
        return "Invalid predicate: operation = " + op();
    }
  }

  private static boolean isUnaryTest(Operation op) {
    return switch (op) {
      case IS_NULL, NOT_NULL, IS_NAN, NOT_NAN -> true;
      default -> false;
    };
  }

  private static boolean isComparison(Operation op) {
    return switch (op) {
      case LT, LT_EQ, GT, GT_EQ, EQ, NOT_EQ, STARTS_WITH, NOT_STARTS_WITH -> true;
      default -> false;
    };
  }

  private static boolean isSetTest(Operation op) {
    return op == Operation.IN || op == Operation.NOT_IN;
  }

  @SuppressWarnings("unchecked")
  static <T> Set<T> setOf(Iterable<Literal<T>> literals) {
    Literal<T> lit = Iterables.get(literals, 0);
    if (lit instanceof Literals.StringLiteral) {
      Iterable<T> values = Iterables.transform(literals, Literal::value);
      Iterable<CharSequence> charSeqs = Iterables.transform(values, val -> (CharSequence) val);
      return (Set<T>) CharSequenceSet.of(charSeqs);
    } else {
      return Sets.newHashSet(Iterables.transform(literals, Literal::value));
    }
  }
}
