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

package org.apache.iceberg.mr.hive;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import org.apache.hadoop.hive.ql.io.sarg.ExpressionTree;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;

import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.in;
import static org.apache.iceberg.expressions.Expressions.isNull;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.not;
import static org.apache.iceberg.expressions.Expressions.or;


public class HiveIcebergFilterFactory {

  private static final int MICROS_PER_SECOND = 1000000;
  private static final int NANOSECS_PER_MICROSEC = 1000;

  private HiveIcebergFilterFactory() {}

  public static Expression generateFilterExpression(SearchArgument sarg) {
    return translate(sarg.getExpression(), sarg.getLeaves());
  }

  /**
   * Recursive method to traverse down the ExpressionTree to evaluate each expression and its leaf nodes.
   * @param tree Current ExpressionTree where the 'top' node is being evaluated.
   * @param leaves List of all leaf nodes within the tree.
   * @return Expression that is translated from the Hive SearchArgument.
   */
  private static Expression translate(ExpressionTree tree, List<PredicateLeaf> leaves) {
    List<ExpressionTree> childNodes = tree.getChildren();
    switch (tree.getOperator()) {
      case OR:
        Expression orResult = Expressions.alwaysFalse();
        for (ExpressionTree child : childNodes) {
          orResult = or(orResult, translate(child, leaves));
        }
        return orResult;
      case AND:
        Expression result = Expressions.alwaysTrue();
        for (ExpressionTree child : childNodes) {
          result = and(result, translate(child, leaves));
        }
        return result;
      case NOT:
        return not(translate(childNodes.get(0), leaves));
      case LEAF:
        return translateLeaf(leaves.get(tree.getLeaf()));
      case CONSTANT:
        throw new UnsupportedOperationException("CONSTANT operator is not supported");
      default:
        throw new IllegalStateException("Unknown operator: " + tree.getOperator());
    }
  }

  /**
   * Translate leaf nodes from Hive operator to Iceberg operator.
   * @param leaf Leaf node
   * @return Expression fully translated from Hive PredicateLeaf
   */
  private static Expression translateLeaf(PredicateLeaf leaf) {
    String column = leaf.getColumnName();
    switch (leaf.getOperator()) {
      case EQUALS:
        return equal(column, leafToLiteral(leaf));
      case LESS_THAN:
        return lessThan(column, leafToLiteral(leaf));
      case LESS_THAN_EQUALS:
        return lessThanOrEqual(column, leafToLiteral(leaf));
      case IN:
        return in(column, leafToLiteralList(leaf));
      case BETWEEN:
        List<Object> icebergLiterals = leafToLiteralList(leaf);
        return and(greaterThanOrEqual(column, icebergLiterals.get(0)),
                lessThanOrEqual(column, icebergLiterals.get(1)));
      case IS_NULL:
        return isNull(column);
      default:
        throw new IllegalStateException("Unknown operator: " + leaf.getOperator());
    }
  }

  private static Object leafToLiteral(PredicateLeaf leaf) {
    switch (leaf.getType()) {
      case LONG:
      case BOOLEAN:
      case STRING:
      case FLOAT:
        return leaf.getLiteral();
      case DATE:
        //Hive converts a Date type to a Timestamp internally when retrieving literal
        return ((Timestamp) leaf.getLiteral()).toLocalDateTime().toLocalDate().toEpochDay();
      case DECIMAL:
        return BigDecimal.valueOf(((HiveDecimalWritable) leaf.getLiteral()).doubleValue());
      case TIMESTAMP:
        Timestamp timestamp = (Timestamp) leaf.getLiteral();
        return timestamp.toInstant().getEpochSecond() * MICROS_PER_SECOND +
                timestamp.getNanos() / NANOSECS_PER_MICROSEC;
      default:
        throw new IllegalStateException("Unknown type: " + leaf.getType());
    }
  }

  private static List<Object> leafToLiteralList(PredicateLeaf leaf) {
    switch (leaf.getType()) {
      case LONG:
      case BOOLEAN:
      case FLOAT:
      case STRING:
        return leaf.getLiteralList();
      case DATE:
        List<Object> dateValues = leaf.getLiteralList();
        dateValues.replaceAll(value -> ((Date) value).toLocalDate().toEpochDay());
        return dateValues;
      case DECIMAL:
        List<Object> decimalValues = leaf.getLiteralList();
        decimalValues.replaceAll(value -> BigDecimal.valueOf(((HiveDecimalWritable) value).doubleValue()));
        return decimalValues;
      case TIMESTAMP:
        List<Object> timestampValues = leaf.getLiteralList();
        timestampValues.replaceAll(value -> ((Timestamp) value).toInstant().getEpochSecond() * MICROS_PER_SECOND +
                ((Timestamp) value).getNanos() / NANOSECS_PER_MICROSEC);
        return timestampValues;
      default:
        throw new IllegalStateException("Unknown type: " + leaf.getType());
    }
  }
}
