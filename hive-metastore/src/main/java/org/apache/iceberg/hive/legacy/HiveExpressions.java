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

package org.apache.iceberg.hive.legacy;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.expressions.UnboundTerm;


class HiveExpressions {

  private HiveExpressions() {
  }

  /**
   * Simplifies the {@link Expression} so that it fits the restrictions of the expression that can be passed
   * to the Hive metastore. For details about the simplification, please see {@link RemoveNonPartitionPredicates} and
   * {@link RewriteUnsupportedOperators}
   * @param expr The {@link Expression} to be simplified
   * @param partitionColumnNames The set of partition column names
   * @return TRUE if the simplified expression results in an always true expression or if there are no predicates on
   *            partition columns in the simplified expression,
   *         FALSE if the simplified expression results in an always false expression,
   *         otherwise returns the simplified expression
   */
  static Expression simplifyPartitionFilter(Expression expr, Set<String> partitionColumnNames) {
    try {
      Expression partitionPredicatesOnly = ExpressionVisitors.visit(expr,
          new RemoveNonPartitionPredicates(partitionColumnNames));
      return ExpressionVisitors.visit(partitionPredicatesOnly, new RewriteUnsupportedOperators());
    } catch (Exception e) {
      throw new RuntimeException("Error while processing expression: " + expr, e);
    }
  }

  /**
   * Converts an {@link Expression} into a filter string which can be passed to the Hive metastore
   *
   * It is expected that caller handles TRUE and FALSE expressions before calling this method. The given
   * {@link Expressions} must also be passed through {@link #simplifyPartitionFilter(Expression, Set)} first to
   * remove any unsupported predicates.
   * @param expr The {@link Expression} to be converted into a filter string
   * @return a filter string equivalent to the given {@link Expression} which can be passed to the Hive metastore
   */
  static String toPartitionFilterString(Expression expr) {
    return ExpressionVisitors.visit(expr, ExpressionToPartitionFilterString.get());
  }

  /**
   * Removes any predicates on non-partition columns from the given {@link Expression}
   */
  private static class RemoveNonPartitionPredicates extends ExpressionVisitors.ExpressionVisitor<Expression> {

    private final Set<String> partitionColumnNamesLowerCase;

    RemoveNonPartitionPredicates(Set<String> partitionColumnNames) {
      this.partitionColumnNamesLowerCase =
          partitionColumnNames.stream().map(String::toLowerCase).collect(Collectors.toSet());
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
      throw new IllegalStateException("Bound predicate not expected: " + pred.getClass().getName());
    }

    @Override
    public <T> Expression predicate(UnboundPredicate<T> pred) {
      return (partitionColumnNamesLowerCase.contains(pred.ref().name().toLowerCase())) ? pred
          : Expressions.alwaysTrue();
    }
  }

  /**
   * Rewrites the {@link Expression} so that it fits the restrictions of the expression that can be passed
   * to the Hive metastore.
   *
   * This visitor assumes that all predicates are on partition columns. Predicates on non-partition columns should be
   * removed using {@link RemoveNonPartitionPredicates} before calling this visitor. It performs the following changes:
   * 1. Rewrites NOT operators by inverting binary operators, negating unary literals and using De Morgan's laws
   *    e.g. NOT(value > 0 AND TRUE) => value <= 0 OR FALSE
   *         NOT(value < 0 OR value > 10) => value >= 0 AND value <= 10
   * 2. Removes IS NULL and IS NOT NULL predicates (Replaced with FALSE and TRUE respectively as partition column values
   *    are always non null for Hive)
   *    e.g. partitionColumn IS NULL => FALSE
   *         partitionColumn IS NOT NULL => TRUE
   * 3. Expands IN and NOT IN operators into ORs of EQUAL operations and ANDs of NOT EQUAL operations respectively
   *    e.g. value IN (1, 2, 3) => value = 1 OR value = 2 OR value = 3
   *         value NOT IN (1, 2, 3) => value != 1 AND value != 2 AND value != 3
   * 4. Removes any children TRUE and FALSE expressions. The checks to remove these are happening inside
   *    {@link Expressions#and(Expression, Expression)} and {@link Expressions#or(Expression, Expression)}
   *    (Note that the rewritten expression still can be TRUE and FALSE at the root and will have to be handled
   *    appropriately by the caller)
   *
   * For examples take a look at the tests in {@code TestHiveExpressions}
   */
  private static class RewriteUnsupportedOperators extends ExpressionVisitors.ExpressionVisitor<Expression> {

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
      return result.negate();
    }

    @Override
    public Expression and(Expression leftResult, Expression rightResult) {
      return Expressions.and(leftResult, rightResult);
    }

    @Override
    public Expression or(Expression leftResult, Expression rightResult) {
      return Expressions.or(leftResult, rightResult);
    }

    <T> Expression in(UnboundTerm<T> term, List<Literal<T>> literals) {
      Expression in = alwaysFalse();
      for (Literal<T> literal : literals) {
        in = Expressions.or(in, Expressions.equal(term, literal.value()));
      }
      return in;
    }

    <T> Expression notIn(UnboundTerm<T> term, List<Literal<T>> literals) {
      Expression notIn = alwaysTrue();
      for (Literal<T> literal : literals) {
        notIn = Expressions.and(notIn, Expressions.notEqual(term, literal.value()));
      }
      return notIn;
    }

    @Override
    public <T> Expression predicate(BoundPredicate<T> pred) {
      throw new IllegalStateException("Bound predicate not expected: " + pred.getClass().getName());
    }

    @Override
    public <T> Expression predicate(UnboundPredicate<T> pred) {
      switch (pred.op()) {
        case LT:
        case LT_EQ:
        case GT:
        case GT_EQ:
        case EQ:
        case NOT_EQ:
          return pred;
        case IS_NULL:
          return Expressions.alwaysFalse();
        case NOT_NULL:
          return Expressions.alwaysTrue();
        case IN:
          return in(pred.term(), pred.literals());
        case NOT_IN:
          return notIn(pred.term(), pred.literals());
        case STARTS_WITH:
          throw new UnsupportedOperationException("STARTS_WITH predicate not supported in partition filter " +
              "expression. Please use a combination of greater than AND less than predicates instead.");
        default:
          throw new IllegalStateException("Unexpected predicate: " + pred.op());
      }
    }
  }

  private static class ExpressionToPartitionFilterString extends ExpressionVisitors.ExpressionVisitor<String> {
    private static final ExpressionToPartitionFilterString INSTANCE = new ExpressionToPartitionFilterString();

    private ExpressionToPartitionFilterString() {
    }

    static ExpressionToPartitionFilterString get() {
      return INSTANCE;
    }

    @Override
    public String alwaysTrue() {
      throw new IllegalStateException("TRUE literal not allowed in Hive partition filter string");
    }

    @Override
    public String alwaysFalse() {
      throw new IllegalStateException("FALSE literal not allowed in Hive partition filter string");
    }

    @Override
    public String not(String result) {
      throw new IllegalStateException("NOT operator not allowed in Hive partition filter string");
    }

    @Override
    public String and(String leftResult, String rightResult) {
      return String.format("((%s) AND (%s))", leftResult, rightResult);
    }

    @Override
    public String or(String leftResult, String rightResult) {
      return String.format("((%s) OR (%s))", leftResult, rightResult);
    }

    @Override
    public <T> String predicate(BoundPredicate<T> pred) {
      throw new IllegalStateException("Bound predicate not expected: " + pred.getClass().getName());
    }

    @Override
    public <T> String predicate(UnboundPredicate<T> pred) {
      switch (pred.op()) {
        case LT:
        case LT_EQ:
        case GT:
        case GT_EQ:
        case EQ:
        case NOT_EQ:
          return getBinaryExpressionString(pred.ref().name(), pred.op(), pred.literal());
        default:
          throw new IllegalStateException("Unexpected operator in Hive partition filter string: " + pred.op());
      }
    }

    private <T> String getBinaryExpressionString(String columnName, Expression.Operation op, Literal<T> lit) {
      return String.format("( %s %s %s )", columnName, getOperationString(op), getLiteralValue(lit));
    }

    private String getOperationString(Expression.Operation op) {
      switch (op) {
        case LT:
          return "<";
        case LT_EQ:
          return "<=";
        case GT:
          return ">";
        case GT_EQ:
          return ">=";
        case EQ:
          return "=";
        case NOT_EQ:
          return "!=";
        default:
          throw new IllegalStateException("Unexpected operator in Hive partition filter string: " + op);
      }
    }

    private <T> String getLiteralValue(Literal<T> lit) {
      Object value = lit.value();
      if (value instanceof String) {
        String escapedString = ((String) value).replace("'", "\\'");
        return String.format("'%s'", escapedString);
      } else {
        return String.valueOf(value);
      }
    }
  }
}
