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

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expression.Operation;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

public class FlinkFilters {
  private FlinkFilters() {
  }

  private static final Map<BuiltInFunctionDefinition, Operation> FILTERS = ImmutableMap
      .<BuiltInFunctionDefinition, Operation>builder()
      .put(BuiltInFunctionDefinitions.EQUALS, Operation.EQ)
      .put(BuiltInFunctionDefinitions.NOT_EQUALS, Operation.NOT_EQ)
      .put(BuiltInFunctionDefinitions.GREATER_THAN, Operation.GT)
      .put(BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL, Operation.GT_EQ)
      .put(BuiltInFunctionDefinitions.LESS_THAN, Operation.LT)
      .put(BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL, Operation.LT_EQ)
      .put(BuiltInFunctionDefinitions.IN, Operation.IN)
      .put(BuiltInFunctionDefinitions.IS_NULL, Operation.IS_NULL)
      .put(BuiltInFunctionDefinitions.IS_NOT_NULL, Operation.NOT_NULL)
      .put(BuiltInFunctionDefinitions.AND, Operation.AND)
      .put(BuiltInFunctionDefinitions.OR, Operation.OR)
      .put(BuiltInFunctionDefinitions.NOT, Operation.NOT)
      .build();


  public static Expression convert(org.apache.flink.table.expressions.Expression flinkExpression) {
    if (!(flinkExpression instanceof CallExpression)) {
      return null;
    }

    CallExpression call = (CallExpression) flinkExpression;
    Operation op = FILTERS.get(call.getFunctionDefinition());
    if (op != null) {
      switch (op) {
        case IS_NULL:
          FieldReferenceExpression isNullFilter = (FieldReferenceExpression) call.getResolvedChildren().get(0);
          return Expressions.isNull(isNullFilter.getName());

        case NOT_NULL:
          FieldReferenceExpression notNullExpression = (FieldReferenceExpression) call.getResolvedChildren().get(0);
          return Expressions.notNull(notNullExpression.getName());

        case LT:
          return convertComparisonExpression(Expressions::lessThan, call);

        case LT_EQ:
          return convertComparisonExpression(Expressions::lessThanOrEqual, call);

        case GT:
          return convertComparisonExpression(Expressions::greaterThan, call);

        case GT_EQ:
          return convertComparisonExpression(Expressions::greaterThanOrEqual, call);

        case EQ:
          return convertComparisonExpression(Expressions::equal, call);

        case NOT_EQ:
          return convertComparisonExpression(Expressions::notEqual, call);

        case IN:
          List<ResolvedExpression> args = call.getResolvedChildren();
          FieldReferenceExpression field = (FieldReferenceExpression) args.get(0);
          List<ResolvedExpression> values = args.subList(1, args.size());

          List<Object> expressions = values.stream().filter(
              expression -> {
                if (expression instanceof ValueLiteralExpression) {
                  return !((ValueLiteralExpression) flinkExpression).isNull();
                }

                return false;
              }
          ).map(expression -> {
            ValueLiteralExpression valueLiteralExpression = (ValueLiteralExpression) expression;
            return valueLiteralExpression.getValueAs(valueLiteralExpression.getOutputDataType().getConversionClass())
                .get();
          }).collect(Collectors.toList());
          return Expressions.in(field.getName(), expressions);

        case NOT:
          Expression child = convert(call.getResolvedChildren().get(0));
          if (child != null) {
            return Expressions.not(child);
          }

          return null;

        case AND: {
          return convertLogicExpression(Expressions::and, call);
        }

        case OR: {
          return convertLogicExpression(Expressions::or, call);
        }
      }
    }

    return null;
  }

  private static Expression convertLogicExpression(BiFunction<Expression, Expression, Expression> function,
                                                   CallExpression call) {
    List<ResolvedExpression> args = call.getResolvedChildren();
    Expression left = convert(args.get(0));
    Expression right = convert(args.get(1));
    if (left != null && right != null) {
      return function.apply(left, right);
    }

    return null;

  }

  private static Expression convertComparisonExpression(BiFunction<String, Object, Expression> function,
                                                        CallExpression call) {
    List<ResolvedExpression> args = call.getResolvedChildren();
    FieldReferenceExpression fieldReferenceExpression;
    ValueLiteralExpression valueLiteralExpression;
    if (literalOnRight(args)) {
      fieldReferenceExpression = (FieldReferenceExpression) args.get(0);
      valueLiteralExpression = (ValueLiteralExpression) args.get(1);
    } else {
      fieldReferenceExpression = (FieldReferenceExpression) args.get(1);
      valueLiteralExpression = (ValueLiteralExpression) args.get(0);
    }

    String name = fieldReferenceExpression.getName();
    Class clazz = valueLiteralExpression.getOutputDataType().getConversionClass();
    return function.apply(name, valueLiteralExpression.getValueAs(clazz).get());
  }

  private static boolean literalOnRight(List<ResolvedExpression> args) {
    return args.get(0) instanceof FieldReferenceExpression && args.get(1) instanceof ValueLiteralExpression ? true :
        false;
  }
}
