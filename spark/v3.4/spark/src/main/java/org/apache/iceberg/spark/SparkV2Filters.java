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
import static org.apache.iceberg.expressions.Expressions.notEqual;
import static org.apache.iceberg.expressions.Expressions.notIn;
import static org.apache.iceberg.expressions.Expressions.notNaN;
import static org.apache.iceberg.expressions.Expressions.notNull;
import static org.apache.iceberg.expressions.Expressions.or;
import static org.apache.iceberg.expressions.Expressions.ref;
import static org.apache.iceberg.expressions.Expressions.startsWith;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expression.Operation;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.expressions.UnboundTerm;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.NaNUtil;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.And;
import org.apache.spark.sql.connector.expressions.filter.Not;
import org.apache.spark.sql.connector.expressions.filter.Or;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.unsafe.types.UTF8String;

public class SparkV2Filters {

  private static final String TRUE = "ALWAYS_TRUE";
  private static final String FALSE = "ALWAYS_FALSE";
  private static final String EQ = "=";
  private static final String EQ_NULL_SAFE = "<=>";
  private static final String NOT_EQ = "<>";
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
          .put(NOT_EQ, Operation.NOT_EQ)
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
          .buildOrThrow();

  private SparkV2Filters() {}

  @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:MethodLength"})
  public static Expression convert(Predicate predicate) {
    Operation op = FILTERS.get(predicate.name());
    if (op != null) {
      switch (op) {
        case TRUE:
          return Expressions.alwaysTrue();

        case FALSE:
          return Expressions.alwaysFalse();

        case IS_NULL:
          return isRef(child(predicate)) ? isNull(SparkUtil.toColumnName(child(predicate))) : null;

        case NOT_NULL:
          return isRef(child(predicate)) ? notNull(SparkUtil.toColumnName(child(predicate))) : null;

        case LT:
          if (isRef(leftChild(predicate)) && isLiteral(rightChild(predicate))) {
            String columnName = SparkUtil.toColumnName(leftChild(predicate));
            return lessThan(columnName, convertLiteral(rightChild(predicate)));
          } else if (isRef(rightChild(predicate)) && isLiteral(leftChild(predicate))) {
            String columnName = SparkUtil.toColumnName(rightChild(predicate));
            return greaterThan(columnName, convertLiteral(leftChild(predicate)));
          } else {
            return null;
          }

        case LT_EQ:
          if (isRef(leftChild(predicate)) && isLiteral(rightChild(predicate))) {
            String columnName = SparkUtil.toColumnName(leftChild(predicate));
            return lessThanOrEqual(columnName, convertLiteral(rightChild(predicate)));
          } else if (isRef(rightChild(predicate)) && isLiteral(leftChild(predicate))) {
            String columnName = SparkUtil.toColumnName(rightChild(predicate));
            return greaterThanOrEqual(columnName, convertLiteral(leftChild(predicate)));
          } else {
            return null;
          }

        case GT:
          if (isRef(leftChild(predicate)) && isLiteral(rightChild(predicate))) {
            String columnName = SparkUtil.toColumnName(leftChild(predicate));
            return greaterThan(columnName, convertLiteral(rightChild(predicate)));
          } else if (isRef(rightChild(predicate)) && isLiteral(leftChild(predicate))) {
            String columnName = SparkUtil.toColumnName(rightChild(predicate));
            return lessThan(columnName, convertLiteral(leftChild(predicate)));
          } else {
            return null;
          }

        case GT_EQ:
          if (isRef(leftChild(predicate)) && isLiteral(rightChild(predicate))) {
            String columnName = SparkUtil.toColumnName(leftChild(predicate));
            return greaterThanOrEqual(columnName, convertLiteral(rightChild(predicate)));
          } else if (isRef(rightChild(predicate)) && isLiteral(leftChild(predicate))) {
            String columnName = SparkUtil.toColumnName(rightChild(predicate));
            return lessThanOrEqual(columnName, convertLiteral(leftChild(predicate)));
          } else {
            return null;
          }

        case EQ: // used for both eq and null-safe-eq
          Pair<UnboundTerm<Object>, Object> eqChildren = predicateChildren(predicate);
          if (eqChildren == null) {
            return null;
          }

          if (predicate.name().equals(EQ)) {
            // comparison with null in normal equality is always null. this is probably a mistake.
            Preconditions.checkNotNull(
                eqChildren.second(),
                "Expression is always false (eq is not null-safe): %s",
                predicate);
          }

          return handleEqual(eqChildren.first(), eqChildren.second());

        case NOT_EQ:
          Pair<UnboundTerm<Object>, Object> notEqChildren = predicateChildren(predicate);
          if (notEqChildren == null) {
            return null;
          }

          // comparison with null in normal equality is always null. this is probably a mistake.
          Preconditions.checkNotNull(
              notEqChildren.second(),
              "Expression is always false (notEq is not null-safe): %s",
              predicate);

          return handleNotEqual(notEqChildren.first(), notEqChildren.second());

        case IN:
          if (isSupportedInPredicate(predicate)) {
            return in(
                SparkUtil.toColumnName(childAtIndex(predicate, 0)),
                Arrays.stream(predicate.children())
                    .skip(1)
                    .map(val -> convertLiteral(((Literal<?>) val)))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList()));
          } else {
            return null;
          }

        case NOT:
          Not notPredicate = (Not) predicate;
          Predicate childPredicate = notPredicate.child();
          if (childPredicate.name().equals(IN) && isSupportedInPredicate(childPredicate)) {
            // infer an extra notNull predicate for Spark NOT IN filters
            // as Iceberg expressions don't follow the 3-value SQL boolean logic
            // col NOT IN (1, 2) in Spark is equal to notNull(col) && notIn(col, 1, 2) in Iceberg
            Expression notIn =
                notIn(
                    SparkUtil.toColumnName(childAtIndex(childPredicate, 0)),
                    Arrays.stream(childPredicate.children())
                        .skip(1)
                        .map(val -> convertLiteral(((Literal<?>) val)))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList()));
            return and(notNull(SparkUtil.toColumnName(childAtIndex(childPredicate, 0))), notIn);
          } else if (hasNoInFilter(childPredicate)) {
            Expression child = convert(childPredicate);
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
          String colName = SparkUtil.toColumnName(leftChild(predicate));
          return startsWith(colName, convertLiteral(rightChild(predicate)).toString());
      }
    }

    return null;
  }

  private static Pair<UnboundTerm<Object>, Object> predicateChildren(Predicate predicate) {
    if (isRef(leftChild(predicate)) && isLiteral(rightChild(predicate))) {
      UnboundTerm<Object> term = ref(SparkUtil.toColumnName(leftChild(predicate)));
      Object value = convertLiteral(rightChild(predicate));
      return Pair.of(term, value);

    } else if (isRef(rightChild(predicate)) && isLiteral(leftChild(predicate))) {
      UnboundTerm<Object> term = ref(SparkUtil.toColumnName(rightChild(predicate)));
      Object value = convertLiteral(leftChild(predicate));
      return Pair.of(term, value);

    } else {
      return null;
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> T child(Predicate predicate) {
    org.apache.spark.sql.connector.expressions.Expression[] children = predicate.children();
    Preconditions.checkArgument(
        children.length == 1, "Predicate should have one child: %s", predicate);
    return (T) children[0];
  }

  @SuppressWarnings("unchecked")
  private static <T> T leftChild(Predicate predicate) {
    org.apache.spark.sql.connector.expressions.Expression[] children = predicate.children();
    Preconditions.checkArgument(
        children.length == 2, "Predicate should have two children: %s", predicate);
    return (T) children[0];
  }

  @SuppressWarnings("unchecked")
  private static <T> T rightChild(Predicate predicate) {
    org.apache.spark.sql.connector.expressions.Expression[] children = predicate.children();
    Preconditions.checkArgument(
        children.length == 2, "Predicate should have two children: %s", predicate);
    return (T) children[1];
  }

  @SuppressWarnings("unchecked")
  private static <T> T childAtIndex(Predicate predicate, int index) {
    return (T) predicate.children()[index];
  }

  private static boolean isRef(org.apache.spark.sql.connector.expressions.Expression expr) {
    return expr instanceof NamedReference;
  }

  private static boolean isLiteral(org.apache.spark.sql.connector.expressions.Expression expr) {
    return expr instanceof Literal;
  }

  private static Object convertLiteral(Literal<?> literal) {
    if (literal.value() instanceof UTF8String) {
      return ((UTF8String) literal.value()).toString();
    }
    return literal.value();
  }

  private static UnboundPredicate<Object> handleEqual(UnboundTerm<Object> term, Object value) {
    if (value == null) {
      return isNull(term);
    } else if (NaNUtil.isNaN(value)) {
      return isNaN(term);
    } else {
      return equal(term, value);
    }
  }

  private static UnboundPredicate<Object> handleNotEqual(UnboundTerm<Object> term, Object value) {
    if (NaNUtil.isNaN(value)) {
      return notNaN(term);
    } else {
      return notEqual(term, value);
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

  private static boolean isSupportedInPredicate(Predicate predicate) {
    if (!isRef(childAtIndex(predicate, 0))) {
      return false;
    } else {
      return Arrays.stream(predicate.children()).skip(1).allMatch(SparkV2Filters::isLiteral);
    }
  }
}
