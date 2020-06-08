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

package org.apache.iceberg.mr.mapred;

import java.util.List;
import org.apache.hadoop.hive.ql.io.sarg.ExpressionTree;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
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
import static org.apache.iceberg.expressions.Expressions.notNull;
import static org.apache.iceberg.expressions.Expressions.or;

public class IcebergFilterFactory {

  private IcebergFilterFactory() {
  }

  public static Expression generateFilterExpression(SearchArgument sarg) {
    List<PredicateLeaf> leaves = sarg.getLeaves();
    List<ExpressionTree> childNodes = sarg.getExpression().getChildren();

    switch (sarg.getExpression().getOperator()) {
      case OR:
        ExpressionTree orLeft = childNodes.get(0);
        ExpressionTree orRight = childNodes.get(1);
        return or(translate(orLeft, leaves), translate(orRight, leaves));
      case AND:
        ExpressionTree andLeft = childNodes.get(0);
        ExpressionTree andRight = childNodes.get(1);
        if (childNodes.size() > 2) {
          Expression[] evaluatedChildren = getLeftoverLeaves(childNodes, leaves);
          return and(
                  translate(andLeft, leaves), translate(andRight, leaves), evaluatedChildren);
        } else {
          return and(translate(andLeft, leaves), translate(andRight, leaves));
        }
      case NOT:
        return not(translateLeaf(sarg.getLeaves().get(0)));
      case LEAF:
        return translateLeaf(sarg.getLeaves().get(0));
      case CONSTANT:
        return null;
      default:
        throw new IllegalStateException("Unknown operator: " + sarg.getExpression().getOperator());
    }
  }

  /**
   * Remove first 2 nodes already evaluated and return an array of the evaluated leftover nodes.
   * @param allChildNodes All child nodes to be evaluated for the AND expression.
   * @param leaves All instances of the leaf nodes.
   * @return Array of leftover evaluated nodes.
   */
  private static Expression[] getLeftoverLeaves(List<ExpressionTree> allChildNodes, List<PredicateLeaf> leaves) {
    allChildNodes.remove(0);
    allChildNodes.remove(0);

    Expression[] evaluatedLeaves = new Expression[allChildNodes.size()];
    for (int i = 0; i < allChildNodes.size(); i++) {
      Expression filter = translate(allChildNodes.get(i), leaves);
      evaluatedLeaves[i] = filter;
    }
    return evaluatedLeaves;
  }

  /**
   * Recursive method to traverse down the ExpressionTree to evaluate each expression and its leaf nodes.
   * @param tree Current ExpressionTree where the 'top' node is being evaluated.
   * @param leaves List of all leaf nodes within the tree.
   * @return Expression that is translated from the Hive SearchArgument.
   */
  private static Expression translate(ExpressionTree tree, List<PredicateLeaf> leaves) {
    switch (tree.getOperator()) {
      case OR:
        return or(translate(tree.getChildren().get(0), leaves),
                translate(tree.getChildren().get(1), leaves));
      case AND:
        if (tree.getChildren().size() > 2) {
          Expression[] evaluatedChildren = getLeftoverLeaves(tree.getChildren(), leaves);
          return and(translate(tree.getChildren().get(0), leaves),
                  translate(tree.getChildren().get(1), leaves), evaluatedChildren);
        } else {
          return and(translate(tree.getChildren().get(0), leaves),
                  translate(tree.getChildren().get(1), leaves));
        }
      case NOT:
        return not(translate(tree.getChildren().get(0), leaves));
      case LEAF:
        return translateLeaf(leaves.get(tree.getLeaf()));
      case CONSTANT:
        //We are unsure of how the CONSTANT case works, so using the approach of:
        //https://github.com/apache/hive/blob/master/ql/src/java/org/apache/hadoop/hive/ql/io/parquet/read/
        // ParquetFilterPredicateConverter.java#L116
        return null;
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
    if (column.equals("snapshot__id")) {
      return Expressions.alwaysTrue();
    }
    switch (leaf.getOperator()) {
      case EQUALS:
        return equal(column, leaf.getLiteral());
      case NULL_SAFE_EQUALS:
        return equal(notNull(column).ref().name(), leaf.getLiteral()); //TODO: Unsure..
      case LESS_THAN:
        return lessThan(column, leaf.getLiteral());
      case LESS_THAN_EQUALS:
        return lessThanOrEqual(column, leaf.getLiteral());
      case IN:
        return in(column, leaf.getLiteralList());
      case BETWEEN:
        return and(greaterThanOrEqual(column, leaf.getLiteralList().get(0)),
                lessThanOrEqual(column, leaf.getLiteralList().get(1)));
      case IS_NULL:
        return isNull(column);
      default:
        throw new IllegalStateException("Unknown operator: " + leaf.getOperator());
    }
  }

}
