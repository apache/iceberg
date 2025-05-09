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
package org.apache.iceberg.spark.source;

import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation;
import scala.PartialFunction;
import scala.collection.JavaConverters;
import scala.collection.Seq;

public class PlanUtils {
  private PlanUtils() {}

  public static List<org.apache.iceberg.expressions.Expression> collectPushDownFilters(
      LogicalPlan logicalPlan) {
    return JavaConverters.asJavaCollection(logicalPlan.collectLeaves()).stream()
        .flatMap(
            plan -> {
              if (!(plan instanceof DataSourceV2ScanRelation)) {
                return Stream.empty();
              }

              DataSourceV2ScanRelation scanRelation = (DataSourceV2ScanRelation) plan;
              if (!(scanRelation.scan() instanceof SparkBatchQueryScan)) {
                return Stream.empty();
              }

              SparkBatchQueryScan batchQueryScan = (SparkBatchQueryScan) scanRelation.scan();
              return batchQueryScan.filterExpressions().stream();
            })
        .collect(Collectors.toList());
  }

  public static List<Expression> collectSparkExpressions(
      LogicalPlan logicalPlan, Predicate<Expression> predicate) {
    Seq<List<Expression>> list =
        logicalPlan.collect(
            new PartialFunction<LogicalPlan, List<Expression>>() {

              @Override
              public List<Expression> apply(LogicalPlan plan) {
                return JavaConverters.asJavaCollection(plan.expressions()).stream()
                    .flatMap(expr -> collectSparkExpressions(expr, predicate).stream())
                    .collect(Collectors.toList());
              }

              @Override
              public boolean isDefinedAt(LogicalPlan plan) {
                return true;
              }
            });

    return JavaConverters.asJavaCollection(list).stream()
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }

  private static List<Expression> collectSparkExpressions(
      Expression expression, Predicate<Expression> predicate) {
    Seq<Expression> list =
        expression.collect(
            new PartialFunction<Expression, Expression>() {
              @Override
              public Expression apply(Expression expr) {
                return expr;
              }

              @Override
              public boolean isDefinedAt(Expression expr) {
                return predicate.test(expr);
              }
            });

    return Lists.newArrayList(JavaConverters.asJavaCollection(list));
  }
}
