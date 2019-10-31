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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.ExpressionVisitors.ExpressionVisitor;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.StructType;

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
   * @param caseSensitive A boolean flag to control whether the bind should enforce case sensitivity.
   * @return the expression rewritten with bound references
   * @throws ValidationException if literals do not match bound references
   * @throws IllegalStateException if any references are already bound
   */
  public static Expression bind(StructType struct,
                                Expression expr,
                                boolean caseSensitive) {
    return ExpressionVisitors.visit(expr, new BindVisitor(struct, caseSensitive));
  }

  /**
   * Replaces all unbound/named references with bound references to fields in the given struct,
   * defaulting to case sensitive mode.
   *
   * Access modifier is package-private, to only allow use from existing tests.
   *
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
   *
   * @throws IllegalStateException if any references are already bound
   */
  static Expression bind(StructType struct,
                         Expression expr) {
    return Binder.bind(struct, expr, true);
  }

  public static Set<Integer> boundReferences(StructType struct, List<Expression> exprs, boolean caseSensitive) {
    if (exprs == null) {
      return ImmutableSet.of();
    }
    ReferenceVisitor visitor = new ReferenceVisitor();
    for (Expression expr : exprs) {
      ExpressionVisitors.visit(bind(struct, expr, caseSensitive), visitor);
    }
    return visitor.references;
  }

  private static class BindVisitor extends ExpressionVisitor<Expression> {
    private final StructType struct;
    private final boolean caseSensitive;

    private BindVisitor(StructType struct, boolean caseSensitive) {
      this.struct = struct;
      this.caseSensitive = caseSensitive;
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
    public <T> Expression predicate(BoundSetPredicate<T> pred) {
      throw new IllegalStateException("Found already bound set predicate: " + pred);
    }

    @Override
    public <T> Expression predicate(UnboundPredicate<T> pred) {
      return pred.bind(struct, caseSensitive);
    }
  }

  private static class ReferenceVisitor extends ExpressionVisitor<Set<Integer>> {
    private final Set<Integer> references = Sets.newHashSet();

    @Override
    public Set<Integer> alwaysTrue() {
      return references;
    }

    @Override
    public Set<Integer> alwaysFalse() {
      return references;
    }

    @Override
    public Set<Integer> not(Set<Integer> result) {
      return references;
    }

    @Override
    public Set<Integer> and(Set<Integer> leftResult, Set<Integer> rightResult) {
      return references;
    }

    @Override
    public Set<Integer> or(Set<Integer> leftResult, Set<Integer> rightResult) {
      return references;
    }

    @Override
    public <T> Set<Integer> predicate(BoundPredicate<T> pred) {
      references.add(pred.ref().fieldId());
      return references;
    }

    @Override
    public <T> Set<Integer> predicate(BoundSetPredicate<T> pred) {
      references.add(pred.ref().fieldId());
      return references;
    }
  }
}
