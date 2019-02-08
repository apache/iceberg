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

import com.netflix.iceberg.StructLike;
import com.netflix.iceberg.PartitionField;
import com.netflix.iceberg.PartitionSpec;
import com.netflix.iceberg.transforms.Transform;
import java.io.Serializable;
import java.util.Comparator;

/**
 * Finds the residuals for an {@link Expression} the partitions in the given {@link PartitionSpec}.
 * <p>
 * A residual expression is made by partially evaluating an expression using partition values. For
 * example, if a table is partitioned by day(utc_timestamp) and is read with a filter expression
 * utc_timestamp &gt;= a and utc_timestamp &lt;= b, then there are 4 possible residuals expressions
 * for the partition data, d:
 * <ul>
 * <li>If d &gt; day(a) and d &lt; day(b), the residual is always true</li>
 * <li>If d == day(a) and d != day(b), the residual is utc_timestamp &gt;= a</li>
 * <li>if d == day(b) and d != day(a), the residual is utc_timestamp &lt;= b</li>
 * <li>If d == day(a) == day(b), the residual is utc_timestamp &gt;= a and utc_timestamp &lt;= b
 * </li>
 * </ul>
 * <p>
 * Partition data is passed using {@link StructLike}. Residuals are returned by
 * {@link #residualFor(StructLike)}.
 * <p>
 * This class is thread-safe.
 */
public class ResidualEvaluator implements Serializable {
  private final PartitionSpec spec;
  private final Expression expr;
  private transient ThreadLocal<ResidualVisitor> visitors = null;

  private ResidualVisitor visitor() {
    if (visitors == null) {
      this.visitors = ThreadLocal.withInitial(ResidualVisitor::new);
    }
    return visitors.get();
  }

  public ResidualEvaluator(PartitionSpec spec, Expression expr) {
    this.spec = spec;
    this.expr = expr;
  }

  /**
   * Returns a residual expression for the given partition values.
   *
   * @param partitionData partition data values
   * @return the residual of this evaluator's expression from the partition values
   */
  public Expression residualFor(StructLike partitionData) {
    return visitor().eval(partitionData);
  }

  private class ResidualVisitor extends ExpressionVisitors.BoundExpressionVisitor<Expression> {
    private StructLike struct;

    private Expression eval(StructLike struct) {
      this.struct = struct;
      return ExpressionVisitors.visit(expr, this);
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
    public <T> Expression isNull(BoundReference<T> ref) {
      return (ref.get(struct) == null) ? alwaysTrue() : alwaysFalse();
    }

    @Override
    public <T> Expression notNull(BoundReference<T> ref) {
      return (ref.get(struct) != null) ? alwaysTrue() : alwaysFalse();
    }

    @Override
    public <T> Expression lt(BoundReference<T> ref, Literal<T> lit) {
      Comparator<T> cmp = lit.comparator();
      return (cmp.compare(ref.get(struct), lit.value()) < 0) ? alwaysTrue() : alwaysFalse();
    }

    @Override
    public <T> Expression ltEq(BoundReference<T> ref, Literal<T> lit) {
      Comparator<T> cmp = lit.comparator();
      return (cmp.compare(ref.get(struct), lit.value()) <= 0) ? alwaysTrue() : alwaysFalse();
    }

    @Override
    public <T> Expression gt(BoundReference<T> ref, Literal<T> lit) {
      Comparator<T> cmp = lit.comparator();
      return (cmp.compare(ref.get(struct), lit.value()) > 0) ? alwaysTrue() : alwaysFalse();
    }

    @Override
    public <T> Expression gtEq(BoundReference<T> ref, Literal<T> lit) {
      Comparator<T> cmp = lit.comparator();
      return (cmp.compare(ref.get(struct), lit.value()) >= 0) ? alwaysTrue() : alwaysFalse();
    }

    @Override
    public <T> Expression eq(BoundReference<T> ref, Literal<T> lit) {
      Comparator<T> cmp = lit.comparator();
      return (cmp.compare(ref.get(struct), lit.value()) == 0) ? alwaysTrue() : alwaysFalse();
    }

    @Override
    public <T> Expression notEq(BoundReference<T> ref, Literal<T> lit) {
      Comparator<T> cmp = lit.comparator();
      return (cmp.compare(ref.get(struct), lit.value()) != 0) ? alwaysTrue() : alwaysFalse();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Expression predicate(BoundPredicate<T> pred) {
      // Get the strict projection of this predicate in partition data, then use it to determine
      // whether to return the original predicate. The strict projection returns true iff the
      // original predicate would have returned true, so the predicate can be eliminated if the
      // strict projection evaluates to true.
      //
      // If there is no strict projection or if it evaluates to false, then return the predicate.
      PartitionField part = spec.getFieldBySourceId(pred.ref().fieldId());
      if (part == null) {
        return pred; // not associated inclusive a partition field, can't be evaluated
      }

      UnboundPredicate<?> strictProjection = ((Transform<T, ?>) part.transform())
          .projectStrict(part.name(), pred);

      if (strictProjection != null) {
        Expression bound = strictProjection.bind(spec.partitionType(), true);
        if (bound instanceof BoundPredicate) {
          // the predicate methods will evaluate and return alwaysTrue or alwaysFalse
          return super.predicate((BoundPredicate<?>) bound);
        }
        return bound; // use the non-predicate residual (e.g. alwaysTrue)
      }

      // if the predicate could not be projected, it must be in the residual
      return pred;
    }

    @Override
    public <T> Expression predicate(UnboundPredicate<T> pred) {
      Expression bound = pred.bind(spec.schema().asStruct(), true);

      if (bound instanceof BoundPredicate) {
        Expression boundResidual = predicate((BoundPredicate<?>) bound);
        if (boundResidual instanceof Predicate) {
          return pred; // replace inclusive original unbound predicate
        }
        return boundResidual; // use the non-predicate residual (e.g. alwaysTrue)
      }

      // if binding didn't result in a Predicate, return the expression
      return bound;
    }
  }
}
