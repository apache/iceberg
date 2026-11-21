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
import java.util.function.Supplier;

/**
 * A visitor for bound predicates and bound value expressions.
 *
 * <p>Children are passed as suppliers that visit the child when called, so implementations control
 * whether and in what order children are visited. For example, an evaluator can skip the right side
 * of {@code and} when the left side is false.
 *
 * <p>Predicates are visited by category: tests, comparisons, and set tests. The predicate passed to
 * each method provides its operation, operands, and set values.
 *
 * <p>Methods that are not overridden throw {@link UnsupportedOperationException}.
 *
 * @param <R> the type of result produced by visiting an expression
 */
public abstract class BoundExpressionTreeVisitor<R> {

  /**
   * Visits a bound expression.
   *
   * @param expr a bound predicate or bound value expression
   * @param visitor a visitor
   * @param <R> the type of result produced by the visitor
   * @return the result of visiting the expression
   * @throws UnsupportedOperationException if the expression is unbound or not supported
   */
  public static <R> R visit(Expression expr, BoundExpressionTreeVisitor<R> visitor) {
    if (expr instanceof Unbound) {
      throw new UnsupportedOperationException("Cannot visit unbound expression: " + expr);
    } else if (expr instanceof BoundValueExpression<?> value) {
      return visitValue(value, visitor);
    } else if (expr instanceof BoundPredicate<?> pred) {
      return visitPredicate(pred, visitor);
    }

    return visitLogical(expr, visitor);
  }

  private static <R> R visitValue(
      BoundValueExpression<?> expr, BoundExpressionTreeVisitor<R> visitor) {
    if (expr instanceof Literal<?> literal) {
      return visitor.literal(literal);
    } else if (expr instanceof BoundReference<?> ref) {
      return visitor.reference(ref);
    } else if (expr instanceof BoundTransform<?, ?> transform) {
      return visitor.transform(transform, () -> visit(transform.ref(), visitor));
    } else if (expr instanceof BoundExtract<?> extract) {
      return visitor.extract(extract, () -> visit(extract.ref(), visitor));
    } else if (expr instanceof BoundApply<?> apply) {
      List<Supplier<R>> arguments =
          apply.arguments().stream().map(arg -> (Supplier<R>) () -> visit(arg, visitor)).toList();
      return visitor.apply(apply, arguments);
    }

    throw new UnsupportedOperationException("Cannot visit unsupported expression: " + expr);
  }

  private static <R> R visitLogical(Expression expr, BoundExpressionTreeVisitor<R> visitor) {
    if (expr instanceof True) {
      return visitor.alwaysTrue();
    } else if (expr instanceof False) {
      return visitor.alwaysFalse();
    } else if (expr instanceof Not not) {
      return visitor.not(() -> visit(not.child(), visitor));
    } else if (expr instanceof And and) {
      return visitor.and(() -> visit(and.left(), visitor), () -> visit(and.right(), visitor));
    } else if (expr instanceof Or or) {
      return visitor.or(() -> visit(or.left(), visitor), () -> visit(or.right(), visitor));
    }

    throw new UnsupportedOperationException("Cannot visit unsupported expression: " + expr);
  }

  private static <R, T> R visitPredicate(
      BoundPredicate<T> pred, BoundExpressionTreeVisitor<R> visitor) {
    if (pred.isUnaryPredicate()) {
      return visitor.test(pred, () -> visit(pred.asUnaryPredicate().child(), visitor));
    } else if (pred.isLiteralPredicate()) {
      BoundLiteralPredicate<T> comparison = pred.asLiteralPredicate();
      return visitor.comparison(
          pred, () -> visit(comparison.left(), visitor), () -> visit(comparison.right(), visitor));
    } else if (pred.isSetPredicate()) {
      return visitor.setTest(pred, () -> visit(pred.asSetPredicate().child(), visitor));
    }

    throw new UnsupportedOperationException("Cannot visit unsupported predicate: " + pred);
  }

  /**
   * Visits the always-true predicate.
   *
   * @return the result for {@code true}
   */
  public R alwaysTrue() {
    throw unsupported("true");
  }

  /**
   * Visits the always-false predicate.
   *
   * @return the result for {@code false}
   */
  public R alwaysFalse() {
    throw unsupported("false");
  }

  /**
   * Visits the negation of a predicate.
   *
   * @param child a supplier that visits the negated predicate
   * @return the result for {@code not}
   */
  public R not(Supplier<R> child) {
    throw unsupported("not");
  }

  /**
   * Visits the conjunction of two predicates.
   *
   * @param left a supplier that visits the left predicate
   * @param right a supplier that visits the right predicate
   * @return the result for {@code and}
   */
  public R and(Supplier<R> left, Supplier<R> right) {
    throw unsupported("and");
  }

  /**
   * Visits the disjunction of two predicates.
   *
   * @param left a supplier that visits the left predicate
   * @param right a supplier that visits the right predicate
   * @return the result for {@code or}
   */
  public R or(Supplier<R> left, Supplier<R> right) {
    throw unsupported("or");
  }

  /**
   * Visits a test of a value expression: {@code IS_NULL}, {@code NOT_NULL}, {@code IS_NAN}, or
   * {@code NOT_NAN}.
   *
   * @param pred the test predicate
   * @param child a supplier that visits the tested value expression
   * @param <T> the Java type of the tested value
   * @return the result for the test
   */
  public <T> R test(BoundPredicate<T> pred, Supplier<R> child) {
    throw unsupported("tests");
  }

  /**
   * Visits a comparison of two value expressions: {@code LT}, {@code LT_EQ}, {@code GT}, {@code
   * GT_EQ}, {@code EQ}, {@code NOT_EQ}, {@code STARTS_WITH}, or {@code NOT_STARTS_WITH}.
   *
   * @param pred the comparison predicate
   * @param left a supplier that visits the left value expression
   * @param right a supplier that visits the right value expression
   * @param <T> the Java type of the compared values
   * @return the result for the comparison
   */
  public <T> R comparison(BoundPredicate<T> pred, Supplier<R> left, Supplier<R> right) {
    throw unsupported("comparisons");
  }

  /**
   * Visits a set test of a value expression: {@code IN} or {@code NOT_IN}.
   *
   * <p>The set of values is available from {@link BoundSetPredicate#literalSet()}.
   *
   * @param pred the set predicate
   * @param child a supplier that visits the tested value expression
   * @param <T> the Java type of the tested value
   * @return the result for the set test
   */
  public <T> R setTest(BoundPredicate<T> pred, Supplier<R> child) {
    throw unsupported("set tests");
  }

  /**
   * Visits a literal.
   *
   * @param literal a literal
   * @param <T> the Java type of the literal's value
   * @return the result for the literal
   */
  public <T> R literal(Literal<T> literal) {
    throw unsupported("literals");
  }

  /**
   * Visits a bound reference.
   *
   * @param ref a bound reference
   * @param <T> the Java type of the referenced field
   * @return the result for the reference
   */
  public <T> R reference(BoundReference<T> ref) {
    throw unsupported("references");
  }

  /**
   * Visits a bound function call.
   *
   * <p>Arguments may be value expressions or predicates.
   *
   * @param apply a bound function call
   * @param arguments suppliers that visit each argument, in order
   * @param <T> the Java type produced by the function
   * @return the result for the function call
   */
  public <T> R apply(BoundApply<T> apply, List<Supplier<R>> arguments) {
    throw unsupported("function calls");
  }

  /**
   * Visits a bound partition transform of a field.
   *
   * @param transform a bound transform
   * @param child a supplier that visits the transformed field's reference
   * @param <S> the Java type of the transformed field
   * @param <T> the Java type produced by the transform
   * @return the result for the transform
   */
  public <S, T> R transform(BoundTransform<S, T> transform, Supplier<R> child) {
    throw unsupported("transforms");
  }

  /**
   * Visits a bound extract of a value from a variant field.
   *
   * @param extract a bound extract
   * @param child a supplier that visits the variant field's reference
   * @param <T> the Java type of the extracted value
   * @return the result for the extract
   */
  public <T> R extract(BoundExtract<T> extract, Supplier<R> child) {
    throw unsupported("extracts");
  }

  private UnsupportedOperationException unsupported(String nodes) {
    return new UnsupportedOperationException(
        getClass().getSimpleName() + " does not support " + nodes);
  }
}
