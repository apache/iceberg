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
import java.util.stream.Collectors;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expression.Operation;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.NaNUtil;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.And;
import org.apache.spark.sql.connector.expressions.filter.Not;
import org.apache.spark.sql.connector.expressions.filter.Or;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.unsafe.types.UTF8String;

public class SparkV2Filters {

  private static final Joiner DOT = Joiner.on(".");

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

  private static final Map<String, Operation> FILTERS =
      ImmutableMap.<String, Operation>builder()
          .put(TRUE, Operation.TRUE)
          .put(FALSE, Operation.FALSE)
          .put(EQ, Operation.EQ)
          .put(EQ_NULL_SAFE, Operation.EQ)
          .put(GT, Operation.GT)
          .put(GT_EQ, Operation.GT_EQ)
          .put(LT, Operation.LT)
          .put(LT_EQ, Operation.LT_EQ)
          .put(IN, Operation.IN)
          .put(IS_NULL, Operation.IS_NULL)
          .put(NOT_NULL, Operation.NOT_NULL)
          .put(AND, Operation.AND)
          .put(OR, Operation.OR)
          .put(NOT, Operation.NOT)
          .put(STARTS_WITH, Operation.STARTS_WITH)
          .build();

  private SparkV2Filters() {}

  @SuppressWarnings("unchecked")
  private static <T> T child(Predicate predicate) {
    org.apache.spark.sql.connector.expressions.Expression[] children = predicate.children();
    Preconditions.checkArgument(children.length == 1, "%s should have one child", predicate);
    return (T) children[0];
  }

  @SuppressWarnings("unchecked")
  private static <T> T leftChild(Predicate predicate) {
    org.apache.spark.sql.connector.expressions.Expression[] children = predicate.children();
    Preconditions.checkArgument(children.length == 2, "%s should have two children", predicate);
    return (T) children[0];
  }

  @SuppressWarnings("unchecked")
  private static <T> T rightChild(Predicate predicate) {
    org.apache.spark.sql.connector.expressions.Expression[] children = predicate.children();
    Preconditions.checkArgument(children.length == 2, "%s should have two children", predicate);
    return (T) children[1];
  }

  private static String toColumnName(NamedReference ref) {
    return DOT.join(ref.fieldNames());
  }

  @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:MethodLength"})
  public static Expression convert(Predicate predicate) {
    if (!valid(predicate)) {
      return null;
    }

    Operation op = FILTERS.get(predicate.name());
    if (op != null) {
      switch (op) {
        case TRUE:
          return Expressions.alwaysTrue();

        case FALSE:
          return Expressions.alwaysFalse();

        case IS_NULL:
          return isNull(toColumnName(child(predicate)));

        case NOT_NULL:
          return notNull(toColumnName(child(predicate)));

        case LT:
          if (rightChild(predicate) instanceof Literal) {
            return lessThan(
                toColumnName(leftChild(predicate)),
                convertUTF8StringIfNecessary(rightChild(predicate)));
          } else {
            return greaterThan(
                toColumnName(rightChild(predicate)),
                convertUTF8StringIfNecessary(leftChild(predicate)));
          }

        case LT_EQ:
          if (rightChild(predicate) instanceof Literal) {
            return lessThanOrEqual(
                toColumnName(leftChild(predicate)),
                convertUTF8StringIfNecessary(rightChild(predicate)));
          } else {
            return greaterThanOrEqual(
                toColumnName(rightChild(predicate)),
                convertUTF8StringIfNecessary(leftChild(predicate)));
          }

        case GT:
          if (rightChild(predicate) instanceof Literal) {
            return greaterThan(
                toColumnName(leftChild(predicate)),
                convertUTF8StringIfNecessary(rightChild(predicate)));
          } else {
            return lessThan(
                toColumnName(rightChild(predicate)),
                convertUTF8StringIfNecessary(leftChild(predicate)));
          }

        case GT_EQ:
          if (rightChild(predicate) instanceof Literal) {
            return greaterThanOrEqual(
                toColumnName(leftChild(predicate)),
                convertUTF8StringIfNecessary(rightChild(predicate)));
          } else {
            return lessThanOrEqual(
                toColumnName(rightChild(predicate)),
                convertUTF8StringIfNecessary(leftChild(predicate)));
          }

        case EQ: // used for both eq and null-safe-eq
          Object value;
          String attributeName;
          if (rightChild(predicate) instanceof Literal) {
            attributeName = toColumnName(leftChild(predicate));
            value = convertUTF8StringIfNecessary(rightChild(predicate));
          } else {
            attributeName = toColumnName(rightChild(predicate));
            value = convertUTF8StringIfNecessary(leftChild(predicate));
          }

          if (predicate.name().equals("=")) {
            // comparison with null in normal equality is always null. this is probably a mistake.
            Preconditions.checkNotNull(
                value, "Expression is always false (eq is not null-safe): %s", predicate);
            return handleEqual(attributeName, value);
          } else { // "<=>"
            if (value == null) {
              return isNull(attributeName);
            } else {
              return handleEqual(attributeName, value);
            }
          }

        case IN:
          return in(
              toColumnName(leftChild(predicate)),
              Arrays.stream(predicate.children())
                  .skip(1)
                  .map(val -> convertUTF8StringIfNecessary(((Literal) val)))
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
                    toColumnName(leftChild(childFilter)),
                    Arrays.stream(childFilter.children())
                        .skip(1)
                        .map(val -> convertUTF8StringIfNecessary(((Literal) val)))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList()));
            return and(notNull(toColumnName(leftChild(childFilter))), notIn);
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
          return startsWith(
              toColumnName(leftChild(predicate)),
              convertUTF8StringIfNecessary(rightChild(predicate)).toString());
      }
    }

    return null;
  }

  private static Object convertUTF8StringIfNecessary(Literal literal) {
    if (literal.value() instanceof UTF8String) {
      return ((UTF8String) literal.value()).toString();
    }
    return literal.value();
  }

  private static Expression handleEqual(String attribute, Object value) {
    if (NaNUtil.isNaN(value)) {
      return isNaN(attribute);
    } else {
      return equal(attribute, value);
    }
  }

  private static boolean hasNoInFilter(Predicate predicate) {
    Operation op = FILTERS.get(predicate.name());

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
    Operation op = FILTERS.get(predicate.name());
    if (op != null) {
      switch (op) {
        case IS_NULL:
        case NOT_NULL:
          return child(predicate) instanceof NamedReference;

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
          if (!(leftChild(predicate) instanceof NamedReference)) {
            return false;
          } else {
            return Arrays.stream(predicate.children())
                .skip(1)
                .allMatch(val -> val instanceof Literal);
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
              && ((Literal<?>) rightChild(predicate)).value() instanceof String;

        case TRUE:
        case FALSE:
          return true;
      }
    }

    return false;
  }

  private static boolean namedRefCompLit(Predicate predicate) {
    return leftChild(predicate) instanceof NamedReference
        && rightChild(predicate) instanceof Literal;
  }

  private static boolean litCompNamedRef(Predicate predicate) {
    return leftChild(predicate) instanceof Literal
        && rightChild(predicate) instanceof NamedReference;
  }
}
