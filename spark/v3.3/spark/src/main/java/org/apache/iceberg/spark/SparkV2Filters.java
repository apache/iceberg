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

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.NaNUtil;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.And;
import org.apache.spark.sql.connector.expressions.filter.Not;
import org.apache.spark.sql.connector.expressions.filter.Or;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.unsafe.types.UTF8String;

public class SparkV2Filters {

  private static final Pattern BACKTICKS_PATTERN = Pattern.compile("([`])(.|$)");

  private SparkV2Filters() {}

  private static final String TRUE = "ALWAYS_TRUE";
  private static final String FALSE = "ALWAYS_FALSE";
  private static final String EQ = "=";
  private static final String EQ_NULL_SAFE = "<=>";
  private static final String GT = ">";
  private static final String GT_EQ = ">=";
  private static final String LT = "<";
  private static final String LT_EQ = "<=";
  private static final String IN = "IN";
  private static final String IS_NULL = "IS_NULL";
  private static final String NOT_NULL = "IS_NOT_NULL";
  private static final String AND = "AND";
  private static final String OR = "OR";
  private static final String NOT = "NOT";
  private static final String STARTS_WITH = "STARTS_WITH";

  private static final Map<String, Expression.Operation> FILTERS =
      ImmutableMap.<String, Expression.Operation>builder()
          .put(TRUE, Expression.Operation.TRUE)
          .put(FALSE, Expression.Operation.FALSE)
          .put(EQ, Expression.Operation.EQ)
          .put(EQ_NULL_SAFE, Expression.Operation.EQ)
          .put(GT, Expression.Operation.GT)
          .put(GT_EQ, Expression.Operation.GT_EQ)
          .put(LT, Expression.Operation.LT)
          .put(LT_EQ, Expression.Operation.LT_EQ)
          .put(IN, Expression.Operation.IN)
          .put(IS_NULL, Expression.Operation.IS_NULL)
          .put(NOT_NULL, Expression.Operation.NOT_NULL)
          .put(AND, Expression.Operation.AND)
          .put(OR, Expression.Operation.OR)
          .put(NOT, Expression.Operation.NOT)
          .put(STARTS_WITH, Expression.Operation.STARTS_WITH)
          .build();

  private static final int FIRST_ORDINAL = 0;
  private static final int SECOND_ORDINAL = 1;

  public static Expression convert(Predicate[] predicates) {
    Expression expression = Expressions.alwaysTrue();
    for (Predicate predicate : predicates) {
      Expression converted = convert(predicate);
      Preconditions.checkArgument(
          converted != null, "Cannot convert predicate to Iceberg: %s", predicate);
      expression = Expressions.and(expression, converted);
    }
    return expression;
  }

  @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:MethodLength"})
  public static Expression convert(Predicate predicate) {
    if (!valid(predicate)) {
      return null;
    }

    Expression.Operation op = FILTERS.get(predicate.name());
    if (op != null) {
      switch (op) {
        case TRUE:
          return Expressions.alwaysTrue();

        case FALSE:
          return Expressions.alwaysFalse();

        case IS_NULL:
          return isNull(unquote(predicate.children()[FIRST_ORDINAL].toString()));

        case NOT_NULL:
          return notNull(unquote(predicate.children()[FIRST_ORDINAL].toString()));

        case LT:
          if (predicate.children()[SECOND_ORDINAL] instanceof LiteralValue) {
            return lessThan(
                unquote(predicate.children()[FIRST_ORDINAL].toString()),
                convertUTF8StringIfNecessary(((LiteralValue) predicate.children()[1]).value()));
          } else {
            return greaterThan(
                unquote(predicate.children()[SECOND_ORDINAL].toString()),
                convertUTF8StringIfNecessary(
                    ((LiteralValue) predicate.children()[FIRST_ORDINAL]).value()));
          }

        case LT_EQ:
          if (predicate.children()[SECOND_ORDINAL] instanceof LiteralValue) {
            return lessThanOrEqual(
                unquote(predicate.children()[FIRST_ORDINAL].toString()),
                convertUTF8StringIfNecessary(((LiteralValue) predicate.children()[1]).value()));
          } else {
            return greaterThanOrEqual(
                unquote(predicate.children()[SECOND_ORDINAL].toString()),
                convertUTF8StringIfNecessary(
                    ((LiteralValue) predicate.children()[FIRST_ORDINAL]).value()));
          }

        case GT:
          if (predicate.children()[SECOND_ORDINAL] instanceof LiteralValue) {
            return greaterThan(
                unquote(predicate.children()[FIRST_ORDINAL].toString()),
                convertUTF8StringIfNecessary(
                    ((LiteralValue) predicate.children()[SECOND_ORDINAL]).value()));
          } else {
            return lessThan(
                unquote(predicate.children()[SECOND_ORDINAL].toString()),
                convertUTF8StringIfNecessary(
                    ((LiteralValue) predicate.children()[FIRST_ORDINAL]).value()));
          }

        case GT_EQ:
          if (predicate.children()[SECOND_ORDINAL] instanceof LiteralValue) {
            return greaterThanOrEqual(
                unquote(predicate.children()[FIRST_ORDINAL].toString()),
                convertUTF8StringIfNecessary(((LiteralValue) predicate.children()[1]).value()));
          } else {
            return lessThanOrEqual(
                unquote(predicate.children()[SECOND_ORDINAL].toString()),
                convertUTF8StringIfNecessary(
                    ((LiteralValue) predicate.children()[FIRST_ORDINAL]).value()));
          }

        case EQ: // used for both eq and null-safe-eq
          Object value;
          String attributeName;
          if (predicate.children()[SECOND_ORDINAL] instanceof LiteralValue) {
            attributeName = predicate.children()[FIRST_ORDINAL].toString();
            value = convertUTF8StringIfNecessary(((LiteralValue) predicate.children()[1]).value());
          } else {
            attributeName = predicate.children()[SECOND_ORDINAL].toString();
            value =
                convertUTF8StringIfNecessary(
                    ((LiteralValue) predicate.children()[FIRST_ORDINAL]).value());
          }

          if (predicate.name().equals("=")) {
            // comparison with null in normal equality is always null. this is probably a mistake.
            Preconditions.checkNotNull(
                value, "Expression is always false (eq is not null-safe): %s", predicate);
            return handleEqual(unquote(attributeName), value);
          } else { // "<=>"
            if (value == null) {
              return isNull(unquote(attributeName));
            } else {
              return handleEqual(unquote(attributeName), value);
            }
          }

        case IN:
          return in(
              unquote(predicate.children()[FIRST_ORDINAL].toString()),
              Arrays.stream(predicate.children())
                  .skip(1)
                  .map(val -> convertUTF8StringIfNecessary(((LiteralValue) val).value()))
                  .filter(Objects::nonNull)
                  .collect(Collectors.toList()));

        case NOT:
          Not notFilter = (Not) predicate;
          Predicate childFilter = notFilter.child();
          if (childFilter.name().equals("IN")) {
            // infer an extra notNull predicate for Spark NOT IN filters
            // as Iceberg expressions don't follow the 3-value SQL boolean logic
            // col NOT IN (1, 2) in Spark is equivalent to notNull(col) && notIn(col, 1, 2) in
            // Iceberg
            Expression notIn =
                notIn(
                    unquote(childFilter.children()[FIRST_ORDINAL].toString()),
                    Arrays.stream(childFilter.children())
                        .skip(1)
                        .map(val -> convertUTF8StringIfNecessary(((LiteralValue) val).value()))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList()));
            return and(notNull(unquote(childFilter.children()[FIRST_ORDINAL].toString())), notIn);
          } else if (hasNoInFilter(childFilter)) {
            Expression child = convert(childFilter);
            if (child != null) {
              return not(child);
            }
          }
          return null;

        case AND:
          {
            And andPredicate = (And) predicate;
            Expression left = convert(andPredicate.left());
            Expression right = convert(andPredicate.right());
            if (left != null && right != null) {
              return and(left, right);
            }
            return null;
          }

        case OR:
          {
            Or orPredicate = (Or) predicate;
            Expression left = convert(orPredicate.left());
            Expression right = convert(orPredicate.right());
            if (left != null && right != null) {
              return or(left, right);
            }
            return null;
          }

        case STARTS_WITH:
          {
            return startsWith(
                unquote(predicate.children()[FIRST_ORDINAL].toString()),
                convertUTF8StringIfNecessary(
                        ((LiteralValue) predicate.children()[SECOND_ORDINAL]).value())
                    .toString());
          }
      }
    }

    return null;
  }

  private static Object convertUTF8StringIfNecessary(Object value) {
    if (value instanceof UTF8String) {
      return ((UTF8String) value).toString();
    }
    return value;
  }

  private static Expression handleEqual(String attribute, Object value) {
    if (NaNUtil.isNaN(value)) {
      return isNaN(attribute);
    } else {
      return equal(attribute, value);
    }
  }

  private static String unquote(String attributeName) {
    Matcher matcher = BACKTICKS_PATTERN.matcher(attributeName);
    return matcher.replaceAll("$2");
  }

  private static boolean hasNoInFilter(Predicate predicate) {
    Expression.Operation op = FILTERS.get(predicate.name());

    if (op != null) {
      switch (op) {
        case AND:
          And andPredicate = (And) predicate;
          return hasNoInFilter(andPredicate.left()) && hasNoInFilter(andPredicate.right());
        case OR:
          Or orPredicate = (Or) predicate;
          return hasNoInFilter(orPredicate.left()) && hasNoInFilter(orPredicate.right());
        case NOT:
          Not notPredicate = (Not) predicate;
          return hasNoInFilter(notPredicate.child());
        case IN:
          return false;
        default:
          return true;
      }
    }

    return false;
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private static boolean valid(Predicate predicate) {
    Expression.Operation op = FILTERS.get(predicate.name());
    if (op != null) {
      switch (op) {
        case IS_NULL:
        case NOT_NULL:
          return predicate.children()[FIRST_ORDINAL] instanceof NamedReference;

        case LT:
        case LT_EQ:
        case GT:
        case GT_EQ:
        case EQ:
          if (predicate.children().length != 2) {
            return false;
          }
          return namedRefCompLit(predicate) || litCompNamedRef(predicate);

        case IN:
          if (!(predicate.children()[FIRST_ORDINAL] instanceof NamedReference)) {
            return false;
          } else {
            return Arrays.stream(predicate.children())
                .skip(1)
                .allMatch(val -> val instanceof LiteralValue);
          }

        case NOT:
          Not notFilter = (Not) predicate;
          return valid(notFilter.child());

        case AND:
          And andFilter = (And) predicate;
          return valid(andFilter.left()) && valid(andFilter.right());

        case OR:
          Or orFilter = (Or) predicate;
          return valid(orFilter.left()) && valid(orFilter.right());

        case STARTS_WITH:
          if (predicate.children().length != 2) {
            return false;
          }
          return namedRefCompLit(predicate)
              && ((LiteralValue<?>) predicate.children()[SECOND_ORDINAL]).value() instanceof String;

        case TRUE:
        case FALSE:
          return true;
      }
    }

    return false;
  }

  private static boolean namedRefCompLit(Predicate predicate) {
    return predicate.children()[FIRST_ORDINAL] instanceof NamedReference
        && predicate.children()[SECOND_ORDINAL] instanceof LiteralValue;
  }

  private static boolean litCompNamedRef(Predicate predicate) {
    return predicate.children()[FIRST_ORDINAL] instanceof LiteralValue
        && predicate.children()[SECOND_ORDINAL] instanceof NamedReference;
  }
}
