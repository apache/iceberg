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

package com.netflix.iceberg.spark;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.expressions.Binder;
import com.netflix.iceberg.expressions.BoundReference;
import com.netflix.iceberg.expressions.Expression;
import com.netflix.iceberg.expressions.Expression.Operation;
import com.netflix.iceberg.expressions.ExpressionVisitors;
import com.netflix.iceberg.expressions.Literal;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.catalyst.expressions.And$;
import org.apache.spark.sql.catalyst.expressions.Not$;
import org.apache.spark.sql.catalyst.expressions.Or$;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.functions$;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualNullSafe;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.IsNotNull;
import org.apache.spark.sql.sources.IsNull;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.sources.Not;
import org.apache.spark.sql.sources.Or;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Map;

import static com.netflix.iceberg.expressions.ExpressionVisitors.visit;
import static com.netflix.iceberg.expressions.Expressions.alwaysFalse;
import static com.netflix.iceberg.expressions.Expressions.and;
import static com.netflix.iceberg.expressions.Expressions.equal;
import static com.netflix.iceberg.expressions.Expressions.greaterThan;
import static com.netflix.iceberg.expressions.Expressions.greaterThanOrEqual;
import static com.netflix.iceberg.expressions.Expressions.isNull;
import static com.netflix.iceberg.expressions.Expressions.lessThan;
import static com.netflix.iceberg.expressions.Expressions.lessThanOrEqual;
import static com.netflix.iceberg.expressions.Expressions.not;
import static com.netflix.iceberg.expressions.Expressions.notNull;
import static com.netflix.iceberg.expressions.Expressions.or;

public class SparkFilters {
  private SparkFilters() {
  }

  private static final Map<Class<? extends Filter>, Operation> FILTERS = ImmutableMap
      .<Class<? extends Filter>, Operation>builder()
      .put(EqualTo.class, Operation.EQ)
      .put(EqualNullSafe.class, Operation.EQ)
      .put(GreaterThan.class, Operation.GT)
      .put(GreaterThanOrEqual.class, Operation.GT_EQ)
      .put(LessThan.class, Operation.LT)
      .put(LessThanOrEqual.class, Operation.LT_EQ)
      .put(In.class, Operation.IN)
      .put(IsNull.class, Operation.IS_NULL)
      .put(IsNotNull.class, Operation.NOT_NULL)
      .put(And.class, Operation.AND)
      .put(Or.class, Operation.OR)
      .put(Not.class, Operation.NOT)
      .build();

  public static Expression convert(Filter filter) {
    // avoid using a chain of if instanceof statements by mapping to the expression enum.
    Operation op = FILTERS.get(filter.getClass());
    if (op != null) {
      switch (op) {
        case IS_NULL:
          IsNull isNullFilter = (IsNull) filter;
          return isNull(isNullFilter.attribute());

        case NOT_NULL:
          IsNotNull notNullFilter = (IsNotNull) filter;
          return notNull(notNullFilter.attribute());

        case LT:
          LessThan lt = (LessThan) filter;
          return lessThan(lt.attribute(), convertLiteral(lt.value()));

        case LT_EQ:
          LessThanOrEqual ltEq = (LessThanOrEqual) filter;
          return lessThanOrEqual(ltEq.attribute(), convertLiteral(ltEq.value()));

        case GT:
          GreaterThan gt = (GreaterThan) filter;
          return greaterThan(gt.attribute(), convertLiteral(gt.value()));

        case GT_EQ:
          GreaterThanOrEqual gtEq = (GreaterThanOrEqual) filter;
          return greaterThanOrEqual(gtEq.attribute(), convertLiteral(gtEq.value()));

        case EQ: // used for both eq and null-safe-eq
          if (filter instanceof EqualTo) {
            EqualTo eq = (EqualTo) filter;
            // comparison with null in normal equality is always null. this is probably a mistake.
            Preconditions.checkNotNull(eq.value(),
                "Expression is always false (eq is not null-safe): " + filter);
            return equal(eq.attribute(), convertLiteral(eq.value()));
          } else {
            EqualNullSafe eq = (EqualNullSafe) filter;
            if (eq.value() == null) {
              return isNull(eq.attribute());
            } else {
              return equal(eq.attribute(), convertLiteral(eq.value()));
            }
          }

        case IN:
          In inFilter = (In) filter;
          Expression in = alwaysFalse();
          for (Object value : inFilter.values()) {
            in = or(in, equal(inFilter.attribute(), convertLiteral(value)));
          }
          return in;

        case NOT:
          Not notFilter = (Not) filter;
          Expression child = convert(notFilter.child());
          if (child != null) {
            return not(child);
          }
          return null;

        case AND: {
          And andFilter = (And) filter;
          Expression left = convert(andFilter.left());
          Expression right = convert(andFilter.right());
          if (left != null && right != null) {
            return and(left, right);
          }
          return null;
        }

        case OR: {
          Or orFilter = (Or) filter;
          Expression left = convert(orFilter.left());
          Expression right = convert(orFilter.right());
          if (left != null && right != null) {
            return or(left, right);
          }
          return null;
        }
      }
    }

    return null;
  }

  private static Object convertLiteral(Object value) {
    if (value instanceof Timestamp) {
      return DateTimeUtils.fromJavaTimestamp((Timestamp) value);
    } else if (value instanceof Date) {
      return DateTimeUtils.fromJavaDate((Date) value);
    }
    return value;
  }

  public static org.apache.spark.sql.catalyst.expressions.Expression convert(Expression filter,
                                                                             Schema schema) {
    return visit(Binder.bind(schema.asStruct(), filter), new ExpressionToSpark(schema));
  }

  private static class ExpressionToSpark extends ExpressionVisitors.
      BoundExpressionVisitor<org.apache.spark.sql.catalyst.expressions.Expression> {
    private final Schema schema;

    public ExpressionToSpark(Schema schema) {
      this.schema = schema;
    }

    @Override
    public org.apache.spark.sql.catalyst.expressions.Expression alwaysTrue() {
      return functions$.MODULE$.lit(true).expr();
    }

    @Override
    public org.apache.spark.sql.catalyst.expressions.Expression alwaysFalse() {
      return functions$.MODULE$.lit(false).expr();
    }

    @Override
    public org.apache.spark.sql.catalyst.expressions.Expression not(
        org.apache.spark.sql.catalyst.expressions.Expression child) {
      return Not$.MODULE$.apply(child);
    }

    @Override
    public org.apache.spark.sql.catalyst.expressions.Expression and(
        org.apache.spark.sql.catalyst.expressions.Expression left,
        org.apache.spark.sql.catalyst.expressions.Expression right) {
      return And$.MODULE$.apply(left, right);
    }

    @Override
    public org.apache.spark.sql.catalyst.expressions.Expression or(
        org.apache.spark.sql.catalyst.expressions.Expression left,
        org.apache.spark.sql.catalyst.expressions.Expression right) {
      return Or$.MODULE$.apply(left, right);
    }

    @Override
    public <T> org.apache.spark.sql.catalyst.expressions.Expression isNull(
        BoundReference<T> ref) {
      return column(ref).isNull().expr();
    }

    @Override
    public <T> org.apache.spark.sql.catalyst.expressions.Expression notNull(
        BoundReference<T> ref) {
      return column(ref).isNotNull().expr();
    }

    @Override
    public <T> org.apache.spark.sql.catalyst.expressions.Expression lt(
        BoundReference<T> ref, Literal<T> lit) {
      return column(ref).lt(lit.value()).expr();
    }

    @Override
    public <T> org.apache.spark.sql.catalyst.expressions.Expression ltEq(
        BoundReference<T> ref, Literal<T> lit) {
      return column(ref).leq(lit.value()).expr();
    }

    @Override
    public <T> org.apache.spark.sql.catalyst.expressions.Expression gt(
        BoundReference<T> ref, Literal<T> lit) {
      return column(ref).gt(lit.value()).expr();
    }

    @Override
    public <T> org.apache.spark.sql.catalyst.expressions.Expression gtEq(
        BoundReference<T> ref, Literal<T> lit) {
      return column(ref).geq(lit.value()).expr();
    }

    @Override
    public <T> org.apache.spark.sql.catalyst.expressions.Expression eq(
        BoundReference<T> ref, Literal<T> lit) {
      return column(ref).equalTo(lit.value()).expr();
    }

    @Override
    public <T> org.apache.spark.sql.catalyst.expressions.Expression notEq(
        BoundReference<T> ref, Literal<T> lit) {
      return column(ref).notEqual(lit.value()).expr();
    }

    @Override
    public <T> org.apache.spark.sql.catalyst.expressions.Expression in(
        BoundReference<T> ref, Literal<T> lit) {
      throw new UnsupportedOperationException("Not implemented: in");
    }

    @Override
    public <T> org.apache.spark.sql.catalyst.expressions.Expression notIn(
        BoundReference<T> ref, Literal<T> lit) {
      throw new UnsupportedOperationException("Not implemented: notIn");
    }

    private Column column(BoundReference ref) {
      return functions$.MODULE$.column(schema.findColumnName(ref.fieldId()));
    }
  }
}
