/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.aws.glue.util;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods for constructing the string representation of query expressions used by Catalog service
 */
public final class ExpressionHelper {

  private static final String HIVE_STRING_TYPE_NAME = "string";
  private static final String HIVE_IN_OPERATOR = "IN";
  private static final String HIVE_NOT_IN_OPERATOR = "NOT IN";
  private static final String HIVE_NOT_OPERATOR = "not";

  // TODO "hook" into Hive logging (hive or hive.metastore)
  private static final Logger LOG = LoggerFactory.getLogger(ExpressionHelper.class);

  private static final List<String> QUOTED_TYPES = ImmutableList.of(
      "string", "char", "varchar", "date", "datetime", "timestamp");
  private static final Joiner JOINER = Joiner.on(" AND ");

  private ExpressionHelper() {
  }

  private static String rewriteExpressionForNotIn(String expressionInput, Set<String> columnsInNotInExpression) {
    String expression = expressionInput;
    for (String columnName : columnsInNotInExpression) {
      if (columnName != null) {
        String hiveExpression = getHiveCompatibleNotInExpression(columnName);
        hiveExpression = escapeParentheses(hiveExpression);
        String catalogExpression = getCatalogCompatibleNotInExpression(columnName);
        catalogExpression = escapeParentheses(catalogExpression);
        expression = expression.replaceAll(hiveExpression, catalogExpression);
      }
    }
    return expression;
  }

  // return "not (<columnName>) IN ("
  private static String getHiveCompatibleNotInExpression(String columnName) {
    return String.format("%s (%s) %s (", HIVE_NOT_OPERATOR, columnName, HIVE_IN_OPERATOR);
  }

  // return "(<columnName>) NOT IN ("
  private static String getCatalogCompatibleNotInExpression(String columnName) {
    return String.format("(%s) %s (", columnName, HIVE_NOT_IN_OPERATOR);
  }

  /*
   * Escape the parentheses so that they are considered literally and not as part of regular expression. In the updated
   * expression , we need "\\(" as the output. So, the first four '\' generate '\\' and the last two '\' generate a '('
   */
  private static String escapeParentheses(String expressionInput) {
    String expression = expressionInput;
    expression = expression.replaceAll("\\(", "\\\\\\(");
    expression = expression.replaceAll("\\)", "\\\\\\)");
    return expression;
  }

  public static String buildExpressionFromPartialSpecification(org.apache.hadoop.hive.metastore.api.Table table,
          List<String> partitionValues) throws MetaException {

    List<org.apache.hadoop.hive.metastore.api.FieldSchema> partitionKeys = table.getPartitionKeys();

    if (partitionValues == null || partitionValues.isEmpty()) {
      return null;
    }

    if (partitionKeys == null || partitionValues.size() > partitionKeys.size()) {
      throw new MetaException("Incorrect number of partition values: " + partitionValues);
    }

    partitionKeys = partitionKeys.subList(0, partitionValues.size());
    List<String> predicates = new LinkedList<>();
    for (int i = 0; i < partitionValues.size(); i++) {
      if (!Strings.isNullOrEmpty(partitionValues.get(i))) {
        predicates.add(buildPredicate(partitionKeys.get(i), partitionValues.get(i)));
      }
    }

    return JOINER.join(predicates);
  }

  private static String buildPredicate(org.apache.hadoop.hive.metastore.api.FieldSchema schema, String value) {
    if (isQuotedType(schema.getType())) {
      return String.format("(%s='%s')", schema.getName(), escapeSingleQuotes(value));
    } else {
      return String.format("(%s=%s)", schema.getName(), value);
    }
  }

  private static String escapeSingleQuotes(String str) {
    return str.replaceAll("'", "\\\\'");
  }

  private static boolean isQuotedType(String type) {
    return QUOTED_TYPES.contains(type);
  }

  public static String replaceDoubleQuoteWithSingleQuotes(String str) {
    return str.replaceAll("\"", "\'");
  }

}
