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
package org.apache.iceberg.flink;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expression.Operation;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.NaNUtil;

public class FlinkFilters {
  private FlinkFilters() {}

  private static final Pattern STARTS_WITH_PATTERN = Pattern.compile("([^%]+)%");

  private static final Map<FunctionDefinition, Operation> FILTERS =
      ImmutableMap.<FunctionDefinition, Operation>builder()
          .put(BuiltInFunctionDefinitions.EQUALS, Operation.EQ)
          .put(BuiltInFunctionDefinitions.NOT_EQUALS, Operation.NOT_EQ)
          .put(BuiltInFunctionDefinitions.GREATER_THAN, Operation.GT)
          .put(BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL, Operation.GT_EQ)
          .put(BuiltInFunctionDefinitions.LESS_THAN, Operation.LT)
          .put(BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL, Operation.LT_EQ)
          .put(BuiltInFunctionDefinitions.IS_NULL, Operation.IS_NULL)
          .put(BuiltInFunctionDefinitions.IS_NOT_NULL, Operation.NOT_NULL)
          .put(BuiltInFunctionDefinitions.AND, Operation.AND)
          .put(BuiltInFunctionDefinitions.OR, Operation.OR)
          .put(BuiltInFunctionDefinitions.NOT, Operation.NOT)
          .put(BuiltInFunctionDefinitions.LIKE, Operation.STARTS_WITH)
          .buildOrThrow();

  /**
   * Convert flink expression to iceberg expression.
   *
   * <p>the BETWEEN, NOT_BETWEEN, IN expression will be converted by flink automatically. the
   * BETWEEN will be converted to (GT_EQ AND LT_EQ), the NOT_BETWEEN will be converted to (LT_EQ OR
   * GT_EQ), the IN will be converted to OR, so we do not add the conversion here
   *
   * @param flinkExpression the flink expression
   * @return the iceberg expression
   */
  public static Optional<Expression> convert(
      org.apache.flink.table.expressions.Expression flinkExpression) {
    if (!(flinkExpression instanceof CallExpression)) {
      return Optional.empty();
    }

    CallExpression call = (CallExpression) flinkExpression;
    Operation op = FILTERS.get(call.getFunctionDefinition());
    if (op != null) {
      switch (op) {
        case IS_NULL:
          return onlyChildAs(call, FieldReferenceExpression.class)
              .map(FieldReferenceExpression::getName)
              .map(Expressions::isNull);

        case NOT_NULL:
          return onlyChildAs(call, FieldReferenceExpression.class)
              .map(FieldReferenceExpression::getName)
              .map(Expressions::notNull);

        case LT:
          return convertFieldAndLiteral(Expressions::lessThan, Expressions::greaterThan, call);

        case LT_EQ:
          return convertFieldAndLiteral(
              Expressions::lessThanOrEqual, Expressions::greaterThanOrEqual, call);

        case GT:
          return convertFieldAndLiteral(Expressions::greaterThan, Expressions::lessThan, call);

        case GT_EQ:
          return convertFieldAndLiteral(
              Expressions::greaterThanOrEqual, Expressions::lessThanOrEqual, call);

        case EQ:
          return convertFieldAndLiteral(
              (ref, lit) -> {
                if (NaNUtil.isNaN(lit)) {
                  return Expressions.isNaN(ref);
                } else {
                  return Expressions.equal(ref, lit);
                }
              },
              call);

        case NOT_EQ:
          return convertFieldAndLiteral(
              (ref, lit) -> {
                if (NaNUtil.isNaN(lit)) {
                  return Expressions.notNaN(ref);
                } else {
                  return Expressions.notEqual(ref, lit);
                }
              },
              call);

        case NOT:
          return onlyChildAs(call, CallExpression.class)
              .flatMap(FlinkFilters::convert)
              .map(Expressions::not);

        case AND:
          return convertLogicExpression(Expressions::and, call);

        case OR:
          return convertLogicExpression(Expressions::or, call);

        case STARTS_WITH:
          return convertLike(call);
      }
    }

    return Optional.empty();
  }

  private static <T extends ResolvedExpression> Optional<T> onlyChildAs(
      CallExpression call, Class<T> expectedChildClass) {
    List<ResolvedExpression> children = call.getResolvedChildren();
    if (children.size() != 1) {
      return Optional.empty();
    }

    ResolvedExpression child = children.get(0);
    if (!expectedChildClass.isInstance(child)) {
      return Optional.empty();
    }

    return Optional.of(expectedChildClass.cast(child));
  }

  private static Optional<Expression> convertLike(CallExpression call) {
    List<ResolvedExpression> args = call.getResolvedChildren();
    if (args.size() != 2) {
      return Optional.empty();
    }

    org.apache.flink.table.expressions.Expression left = args.get(0);
    org.apache.flink.table.expressions.Expression right = args.get(1);

    if (left instanceof FieldReferenceExpression && right instanceof ValueLiteralExpression) {
      String name = ((FieldReferenceExpression) left).getName();
      return convertLiteral((ValueLiteralExpression) right)
          .flatMap(
              lit -> {
                if (lit instanceof String) {
                  String pattern = (String) lit;
                  Matcher matcher = STARTS_WITH_PATTERN.matcher(pattern);
                  // exclude special char of LIKE
                  // '_' is the wildcard of the SQL LIKE
                  if (!pattern.contains("_") && matcher.matches()) {
                    return Optional.of(Expressions.startsWith(name, matcher.group(1)));
                  }
                }

                return Optional.empty();
              });
    }

    return Optional.empty();
  }

  private static Optional<Expression> convertLogicExpression(
      BiFunction<Expression, Expression, Expression> function, CallExpression call) {
    List<ResolvedExpression> args = call.getResolvedChildren();
    if (args == null || args.size() != 2) {
      return Optional.empty();
    }

    Optional<Expression> left = convert(args.get(0));
    Optional<Expression> right = convert(args.get(1));
    if (left.isPresent() && right.isPresent()) {
      return Optional.of(function.apply(left.get(), right.get()));
    }

    return Optional.empty();
  }

  private static Optional<Object> convertLiteral(ValueLiteralExpression expression) {
    Optional<?> value =
        expression.getValueAs(
            expression.getOutputDataType().getLogicalType().getDefaultConversion());
    return value.map(
        o -> {
          if (o instanceof LocalDateTime) {
            return DateTimeUtil.microsFromTimestamp((LocalDateTime) o);
          } else if (o instanceof Instant) {
            return DateTimeUtil.microsFromInstant((Instant) o);
          } else if (o instanceof LocalTime) {
            return DateTimeUtil.microsFromTime((LocalTime) o);
          } else if (o instanceof LocalDate) {
            return DateTimeUtil.daysFromDate((LocalDate) o);
          }

          return o;
        });
  }

  private static Optional<Expression> convertFieldAndLiteral(
      BiFunction<String, Object, Expression> expr, CallExpression call) {
    return convertFieldAndLiteral(expr, expr, call);
  }

  private static Optional<Expression> convertFieldAndLiteral(
      BiFunction<String, Object, Expression> convertLR,
      BiFunction<String, Object, Expression> convertRL,
      CallExpression call) {
    List<ResolvedExpression> args = call.getResolvedChildren();
    if (args.size() != 2) {
      return Optional.empty();
    }

    org.apache.flink.table.expressions.Expression left = args.get(0);
    org.apache.flink.table.expressions.Expression right = args.get(1);

    if (left instanceof FieldReferenceExpression && right instanceof ValueLiteralExpression) {
      String name = ((FieldReferenceExpression) left).getName();
      Optional<Object> lit = convertLiteral((ValueLiteralExpression) right);
      if (lit.isPresent()) {
        return Optional.of(convertLR.apply(name, lit.get()));
      }
    } else if (left instanceof ValueLiteralExpression
        && right instanceof FieldReferenceExpression) {
      Optional<Object> lit = convertLiteral((ValueLiteralExpression) left);
      String name = ((FieldReferenceExpression) right).getName();
      if (lit.isPresent()) {
        return Optional.of(convertRL.apply(name, lit.get()));
      }
    }

    return Optional.empty();
  }
}
