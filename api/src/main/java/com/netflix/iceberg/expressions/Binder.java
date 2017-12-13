/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg.expressions;

import com.netflix.iceberg.exceptions.ValidationException;
import com.netflix.iceberg.expressions.ExpressionVisitors.ExpressionVisitor;
import com.netflix.iceberg.types.Type;
import com.netflix.iceberg.types.Types.StructType;

/**
 * Rewrites {@link Expression expressions} by replacing unbound named references with references to
 * fields in a struct schema.
 */
public class Binder {
  private Binder() {
  }

  /**
   * Replaces all unbound/named references with bound references to fields in the given struct.
   * <p>
   * When a reference is resolved, any literal used in a predicate for that field is converted to
   * the field's type using {@link Literal#to(Type)}. If automatic conversion to that type isn't
   * allowed, a {@link ValidationException validation exception} is thrown.
   * <p>
   * The result expression may be simplified when constructed. For example, {@code isNull("a")} is
   * replaced with {@code alwaysFalse()} when {@code "a"} is resolved to a required field.
   * <p>
   * The expression cannot contain references that are already bound, or an
   * {@link IllegalStateException} will be thrown.
   *
   * @param struct The {@link StructType struct type} to resolve references by name.
   * @param expr An {@link Expression expression} to rewrite with bound references.
   * @return the expression rewritten with bound references
   * @throws ValidationException if literals do not match bound references
   * @throws IllegalStateException if any references are already bound
   */
  public static Expression bind(StructType struct,
                                Expression expr) {
    return ExpressionVisitors.visit(expr, new BindVisitor(struct));
  }

  private static class BindVisitor extends ExpressionVisitor<Expression> {
    private final StructType struct;

    private BindVisitor(StructType struct) {
      this.struct = struct;
    }

    @Override
    public Expression alwaysTrue() {
      return Expressions.alwaysTrue();
    }

    @Override
    public Expression alwaysFalse() {
      return Expressions.alwaysFalse();
    }

    @Override
    public Expression not(Expression result) {
      return Expressions.not(result);
    }

    @Override
    public Expression and(Expression leftResult, Expression rightResult) {
      return Expressions.and(leftResult, rightResult);
    }

    @Override
    public Expression or(Expression leftResult, Expression rightResult) {
      return Expressions.or(leftResult, rightResult);
    }

    @Override
    public <T> Expression predicate(BoundPredicate<T> pred) {
      throw new IllegalStateException("Found already bound predicate: " + pred);
    }

    @Override
    public <T> Expression predicate(UnboundPredicate<T> pred) {
      return pred.bind(struct);
    }
  }
}
