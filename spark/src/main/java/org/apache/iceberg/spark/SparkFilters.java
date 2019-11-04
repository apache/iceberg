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

package org.apache.iceberg.spark;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.sql.Date;
import java.sql.Timestamp;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expression.Operation;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
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
import org.apache.spark.sql.sources.StringStartsWith;

import static org.apache.iceberg.expressions.Expressions.alwaysFalse;
import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.isNull;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.not;
import static org.apache.iceberg.expressions.Expressions.notNull;
import static org.apache.iceberg.expressions.Expressions.or;
import static org.apache.iceberg.expressions.Expressions.startsWith;

public class SparkFilters {
  private SparkFilters() {
  }

  private static final ImmutableMap<Class<? extends Filter>, Operation> FILTERS = ImmutableMap
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
      .put(StringStartsWith.class, Operation.STARTS_WITH)
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
                "Expression is always false (eq is not null-safe): %s", filter);
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

        case STARTS_WITH: {
          StringStartsWith stringStartsWith = (StringStartsWith) filter;
          return startsWith(stringStartsWith.attribute(), stringStartsWith.value());
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
}
