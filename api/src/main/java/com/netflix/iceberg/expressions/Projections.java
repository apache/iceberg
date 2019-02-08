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

package com.netflix.iceberg.expressions;

import com.netflix.iceberg.expressions.ExpressionVisitors.ExpressionVisitor;
import com.netflix.iceberg.PartitionField;
import com.netflix.iceberg.PartitionSpec;
import com.netflix.iceberg.transforms.Transform;

/**
 * Utils to project expressions on rows to expressions on partitions.
 */
public class Projections {
  private Projections() {
  }

  /**
   * A class that projects expressions for a table's data rows into expressions on the table's
   * partition values, for a table's {@link PartitionSpec partition spec}.
   * <p>
   * There are two types of projections: inclusive and strict.
   * <p>
   * An inclusive projection guarantees that if an expression matches a row, the projected
   * expression will match the row's partition.
   * <p>
   * A strict projection guarantees that if a partition matches a projected expression, then all
   * rows in that partition will match the original expression.
   */
  public static abstract class ProjectionEvaluator extends ExpressionVisitor<Expression> {
    /**
     * Project the given row expression to a partition expression.
     *
     * @param expr an expression on data rows
     * @return an expression on partition data (depends on the projection)
     */
    public abstract Expression project(Expression expr);
  }

  /**
   * Creates an inclusive {@code ProjectionEvaluator} for the {@link PartitionSpec spec}.
   * <p>
   * An evaluator is used to project expressions for a table's data rows into expressions on the
   * table's partition values. The evaluator returned by this function is inclusive and will build
   * expressions with the following guarantee: if the original expression matches a row, then the
   * projected expression will match that row's partition.
   * <p>
   * Each predicate in the expression is projected using
   * {@link Transform#project(String, BoundPredicate)}.
   *
   * @param spec a partition spec
   * @return an inclusive projection evaluator for the partition spec
   * @see Transform#project(String, BoundPredicate) Inclusive transform used for each predicate
   */
  public static ProjectionEvaluator inclusive(PartitionSpec spec) {
    return new InclusiveProjection(spec);
  }

  /**
   * Creates a strict {@code ProjectionEvaluator} for the {@link PartitionSpec spec}.
   * <p>
   * An evaluator is used to project expressions for a table's data rows into expressions on the
   * table's partition values. The evaluator returned by this function is strict and will build
   * expressions with the following guarantee: if the projected expression matches a partition,
   * then the original expression will match all rows in that partition.
   * <p>
   * Each predicate in the expression is projected using
   * {@link Transform#projectStrict(String, BoundPredicate)}.
   *
   * @param spec a partition spec
   * @return a strict projection evaluator for the partition spec
   * @see Transform#projectStrict(String, BoundPredicate) Strict transform used for each predicate
   */
  public static ProjectionEvaluator strict(PartitionSpec spec) {
    return new StrictProjection(spec);
  }

  private static class BaseProjectionEvaluator extends ProjectionEvaluator {
    final PartitionSpec spec;

    private BaseProjectionEvaluator(PartitionSpec spec) {
      this.spec = spec;
    }

    @Override
    public Expression project(Expression expr) {
      // projections assume that there are no NOT nodes in the expression tree. to ensure that this
      // is the case, the expression is rewritten to push all NOT nodes down to the expression
      // leaf nodes.
      // this is necessary to ensure that the default expression returned when a predicate can't be
      // projected is correct.
      return ExpressionVisitors.visit(ExpressionVisitors.visit(expr, RewriteNot.get()), this);
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
      throw new UnsupportedOperationException("[BUG] project called on expression with a not");
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
    public <T> Expression predicate(UnboundPredicate<T> pred) {
      Expression bound = pred.bind(spec.schema().asStruct(), true);

      if (bound instanceof BoundPredicate) {
        return predicate((BoundPredicate<?>) bound);
      }

      return bound;
    }
  }

  private static class InclusiveProjection extends BaseProjectionEvaluator {
    private InclusiveProjection(PartitionSpec spec) {
      super(spec);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Expression predicate(BoundPredicate<T> pred) {
      PartitionField part = spec.getFieldBySourceId(pred.ref().fieldId());
      if (part == null) {
        // the predicate has no partition column
        return alwaysTrue();
      }

      UnboundPredicate<?> result = ((Transform<T, ?>) part.transform()).project(part.name(), pred);

      if (result != null) {
        return result;
      }

      // if the predicate could not be projected, it always matches
      return alwaysTrue();
    }
  }

  private static class StrictProjection extends BaseProjectionEvaluator {
    private StrictProjection(PartitionSpec spec) {
      super(spec);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Expression predicate(BoundPredicate<T> pred) {
      PartitionField part = spec.getFieldBySourceId(pred.ref().fieldId());
      if (part == null) {
        // the predicate has no partition column
        return alwaysFalse();
      }

      UnboundPredicate<?> result = ((Transform<T, ?>) part.transform())
          .projectStrict(part.name(), pred);

      if (result != null) {
        return result;
      }

      // if the predicate could not be projected, it never matches
      return alwaysFalse();
    }
  }
}
