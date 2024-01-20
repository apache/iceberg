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

import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.in;
import static org.apache.iceberg.expressions.Expressions.isNaN;
import static org.apache.iceberg.expressions.Expressions.isNull;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.not;
import static org.apache.iceberg.expressions.Expressions.notIn;
import static org.apache.iceberg.expressions.Expressions.notNull;
import static org.apache.iceberg.expressions.Expressions.or;
import static org.apache.iceberg.expressions.Expressions.startsWith;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expression.Operation;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.source.Tuple;
import org.apache.iceberg.spark.source.UncomparableLiteralException;
import org.apache.iceberg.spark.source.broadcastvar.BroadcastHRUnboundPredicate;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.util.NaNUtil;
import org.apache.spark.sql.catalyst.bcvar.BroadcastedJoinKeysWrapper;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.sources.AlwaysFalse;
import org.apache.spark.sql.sources.AlwaysFalse$;
import org.apache.spark.sql.sources.AlwaysTrue;
import org.apache.spark.sql.sources.AlwaysTrue$;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkFilters {

  private static final Logger LOG = LoggerFactory.getLogger(SparkFilters.class);

  private static final Pattern BACKTICKS_PATTERN = Pattern.compile("([`])(.|$)");

  private SparkFilters() {}

  private static final Map<Class<? extends Filter>, Operation> FILTERS =
      ImmutableMap.<Class<? extends Filter>, Operation>builder()
          .put(AlwaysTrue.class, Operation.TRUE)
          .put(AlwaysTrue$.class, Operation.TRUE)
          .put(AlwaysFalse$.class, Operation.FALSE)
          .put(AlwaysFalse.class, Operation.FALSE)
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
          .buildOrThrow();

  public static Tuple<Expression, Expression> convert(Filter[] filters) {
    Expression expressionForRangeIn = Expressions.alwaysTrue();
    Expression expressionForOthers = Expressions.alwaysTrue();
    for (Filter filter : filters) {
      Tuple<Boolean, Expression> converted = convert(filter);
      Preconditions.checkArgument(
          converted != null, "Cannot convert filter to Iceberg: %s", filter);
      if (converted.getElement1()) {
        expressionForRangeIn = Expressions.and(expressionForRangeIn, converted.getElement2());
      } else {
        expressionForOthers = Expressions.and(expressionForOthers, converted.getElement2());
      }
    }
    return new Tuple<>(expressionForRangeIn, expressionForOthers);
  }

  public static Tuple<Boolean, Expression> convert(Filter filter, Schema schema) {
    // avoid using a chain of if instanceof statements by mapping to the expression enum.
    Operation op = FILTERS.get(filter.getClass());
    // if boolean is true, it is a range in filter
    if (op != null) {
      switch (op) {
        case TRUE:
          return new Tuple<>(false, Expressions.alwaysTrue());

        case FALSE:
          return new Tuple<>(false, Expressions.alwaysFalse());

        case IS_NULL:
          IsNull isNullFilter = (IsNull) filter;
          return new Tuple<>(false, isNull(unquote(isNullFilter.attribute())));

        case NOT_NULL:
          IsNotNull notNullFilter = (IsNotNull) filter;
          return new Tuple<>(false, notNull(unquote(notNullFilter.attribute())));

        case LT:
          LessThan lt = (LessThan) filter;
          return new Tuple<>(false, lessThan(unquote(lt.attribute()), convertLiteral(lt.value())));

        case LT_EQ:
          LessThanOrEqual ltEq = (LessThanOrEqual) filter;
          return new Tuple<>(
              false, lessThanOrEqual(unquote(ltEq.attribute()), convertLiteral(ltEq.value())));

        case GT:
          GreaterThan gt = (GreaterThan) filter;
          return new Tuple<>(
              false, greaterThan(unquote(gt.attribute()), convertLiteral(gt.value())));

        case GT_EQ:
          GreaterThanOrEqual gtEq = (GreaterThanOrEqual) filter;
          return new Tuple<>(
              false, greaterThanOrEqual(unquote(gtEq.attribute()), convertLiteral(gtEq.value())));

        case EQ: // used for both eq and null-safe-eq
          if (filter instanceof EqualTo) {
            EqualTo eq = (EqualTo) filter;
            // comparison with null in normal equality is always null. this is probably a mistake.
            Preconditions.checkNotNull(
                eq.value(), "Expression is always false (eq is not null-safe): %s", filter);
            return new Tuple<>(false, handleEqual(unquote(eq.attribute()), eq.value()));
          } else {
            EqualNullSafe eq = (EqualNullSafe) filter;
            if (eq.value() == null) {
              return new Tuple<>(false, isNull(unquote(eq.attribute())));
            } else {
              return new Tuple<>(false, handleEqual(unquote(eq.attribute()), eq.value()));
            }
          }

        case IN:
          return handleInFilter((In) filter, schema);

        case NOT:
          Not notFilter = (Not) filter;
          Filter childFilter = notFilter.child();
          Operation childOp = FILTERS.get(childFilter.getClass());
          if (childOp == Operation.IN) {
            // infer an extra notNull predicate for Spark NOT IN filters
            // as Iceberg expressions don't follow the 3-value SQL boolean logic
            // col NOT IN (1, 2) in Spark is equivalent to notNull(col) && notIn(col, 1, 2) in
            // Iceberg
            In childInFilter = (In) childFilter;
            Expression notIn =
                notIn(
                    unquote(childInFilter.attribute()),
                    Stream.of(childInFilter.values())
                        .map(SparkFilters::convertLiteral)
                        .collect(Collectors.toList()));
            return new Tuple<>(false, and(notNull(childInFilter.attribute()), notIn));
          } else if (hasNoInFilter(childFilter)) {
            Tuple<Boolean, Expression> child = convert(childFilter);
            if (child != null) {
              return new Tuple<>(false, not(child.getElement2()));
            }
          }
          return null;

        case AND:
          {
            And andFilter = (And) filter;
            Tuple<Boolean, Expression> left = convert(andFilter.left());
            Tuple<Boolean, Expression> right = convert(andFilter.right());
            if (left != null && right != null) {
              return new Tuple<>(false, and(left.getElement2(), right.getElement2()));
            }
            return null;
          }

        case OR:
          {
            Or orFilter = (Or) filter;
            Tuple<Boolean, Expression> left = convert(orFilter.left());
            Tuple<Boolean, Expression> right = convert(orFilter.right());
            if (left != null && right != null) {
              return new Tuple<>(false, or(left.getElement2(), right.getElement2()));
            }
            return null;
          }

        case STARTS_WITH:
          {
            StringStartsWith stringStartsWith = (StringStartsWith) filter;
            return new Tuple<>(
                false, startsWith(unquote(stringStartsWith.attribute()), stringStartsWith.value()));
          }
      }
    }
    return null;
  }

  private static Tuple<Boolean, Expression> handleInFilter(In inFilter, Schema schema) {
    // TODO :Asif find a more graceful logic to identify rangeIn case
    if (inFilter.values()[0] instanceof BroadcastedJoinKeysWrapper) {
      String unquotedName = unquote(inFilter.attribute());
      Type internalType = schema.findType(unquotedName);
      // we have a BroadCast variable here. so get elements out of it here itself
      BroadcastedJoinKeysWrapper bc = (BroadcastedJoinKeysWrapper) inFilter.values()[0];
      try {
        return new Tuple<>(true, new BroadcastHRUnboundPredicate<>(unquotedName, bc, internalType));
      } catch (UncomparableLiteralException ule) {
        // for now jst log
        LOG.error("Literals involved in RangeIn pred not comparable", ule);
        return null;
      }
    } else {
      return new Tuple<>(
          false,
          in(
              unquote(inFilter.attribute()),
              Stream.of(inFilter.values())
                  .filter(Objects::nonNull)
                  .map(SparkFilters::convertLiteral)
                  .collect(Collectors.toList())));
    }
  }

  public static Tuple<Boolean, Expression> convert(Filter filter) {
    return convert(filter, null);
  }

  public static Object convertLiteral(Object value) {
    if (value instanceof Timestamp) {
      return DateTimeUtils.fromJavaTimestamp((Timestamp) value);
    } else if (value instanceof Date) {
      return DateTimeUtils.fromJavaDate((Date) value);
    } else if (value instanceof Instant) {
      return DateTimeUtils.instantToMicros((Instant) value);
    } else if (value instanceof LocalDateTime) {
      return DateTimeUtils.localDateTimeToMicros((LocalDateTime) value);
    } else if (value instanceof LocalDate) {
      return DateTimeUtils.localDateToDays((LocalDate) value);
    }
    return value;
  }

  private static Expression handleEqual(String attribute, Object value) {
    if (NaNUtil.isNaN(value)) {
      return isNaN(attribute);
    } else {
      return equal(attribute, convertLiteral(value));
    }
  }

  private static String unquote(String attributeName) {
    Matcher matcher = BACKTICKS_PATTERN.matcher(attributeName);
    return matcher.replaceAll("$2");
  }

  private static boolean hasNoInFilter(Filter filter) {
    Operation op = FILTERS.get(filter.getClass());

    if (op != null) {
      switch (op) {
        case AND:
          And andFilter = (And) filter;
          return hasNoInFilter(andFilter.left()) && hasNoInFilter(andFilter.right());
        case OR:
          Or orFilter = (Or) filter;
          return hasNoInFilter(orFilter.left()) && hasNoInFilter(orFilter.right());
        case NOT:
          Not notFilter = (Not) filter;
          return hasNoInFilter(notFilter.child());
        case IN:
          return false;
        default:
          return true;
      }
    }

    return false;
  }
}
