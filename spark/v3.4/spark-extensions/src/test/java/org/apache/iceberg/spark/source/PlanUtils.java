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
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.execution.CommandResultExec;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec;
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation;
import org.assertj.core.api.Assertions;
import scala.PartialFunction;
import scala.collection.JavaConverters;
import scala.collection.Seq;

public class PlanUtils {
  private PlanUtils() {}

  public static List<Expression> getScanPushDownFilters(LogicalPlan logicalPlan) {
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

  public static List<Expression> getCopyOnWritePushDownFilters(SparkPlan sparkPlan) {
    return collectCommandScans(sparkPlan).stream()
        .flatMap(
            scan -> {
              if (!(scan instanceof SparkCopyOnWriteScan)) {
                return Stream.empty();
              }

              SparkCopyOnWriteScan copyOnWriteScan = (SparkCopyOnWriteScan) scan;
              return copyOnWriteScan.filterExpressions().stream();
            })
        .collect(Collectors.toList());
  }

  public static List<Expression> getMergeOnReadPushDownFilters(SparkPlan sparkPlan) {
    return collectCommandScans(sparkPlan).stream()
        .flatMap(
            scan -> {
              if (!(scan instanceof SparkBatchQueryScan)) {
                return Stream.empty();
              }

              SparkBatchQueryScan batchQueryScan = (SparkBatchQueryScan) scan;
              return batchQueryScan.filterExpressions().stream();
            })
        .collect(Collectors.toList());
  }

  private static List<Scan> collectCommandScans(SparkPlan sparkPlan) {
    Assertions.assertThat(sparkPlan).isInstanceOf(CommandResultExec.class);
    CommandResultExec commandResultExec = (CommandResultExec) sparkPlan;
    return collectLevies(commandResultExec.commandPhysicalPlan()).stream()
        .flatMap(
            plan -> {
              if (!(plan instanceof BatchScanExec)) {
                return Stream.empty();
              }

              BatchScanExec batchScanExec = (BatchScanExec) plan;
              Scan scan = batchScanExec.scan();
              return Stream.of(scan);
            })
        .collect(Collectors.toList());
  }

  private static List<SparkPlan> collectLevies(SparkPlan sparkPlan) {
    Seq<List<SparkPlan>> leaves =
        sparkPlan.collect(
            new PartialFunction<SparkPlan, List<SparkPlan>>() {
              @Override
              public boolean isDefinedAt(SparkPlan plan) {
                return plan.children().isEmpty();
              }

              @Override
              public List<SparkPlan> apply(SparkPlan plan) {
                if (plan instanceof AdaptiveSparkPlanExec) {
                  AdaptiveSparkPlanExec adaptiveSparkPlanExec = (AdaptiveSparkPlanExec) plan;
                  return collectLevies(adaptiveSparkPlanExec.inputPlan());
                } else {
                  return Lists.newArrayList(plan);
                }
              }
            });

    return JavaConverters.asJavaCollection(leaves).stream()
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }

  public static List<org.apache.spark.sql.catalyst.expressions.Expression> collectSparkExpressions(
      LogicalPlan logicalPlan,
      Function<org.apache.spark.sql.catalyst.expressions.Expression, Boolean> filterFunction) {
    Seq<List<org.apache.spark.sql.catalyst.expressions.Expression>> list =
        logicalPlan.collect(
            new PartialFunction<
                LogicalPlan, List<org.apache.spark.sql.catalyst.expressions.Expression>>() {

              @Override
              public List<org.apache.spark.sql.catalyst.expressions.Expression> apply(
                  LogicalPlan plan) {
                return JavaConverters.asJavaCollection(plan.expressions()).stream()
                    .flatMap(expr -> collectSparkExpressions(expr, filterFunction).stream())
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

  private static List<org.apache.spark.sql.catalyst.expressions.Expression> collectSparkExpressions(
      org.apache.spark.sql.catalyst.expressions.Expression expression,
      Function<org.apache.spark.sql.catalyst.expressions.Expression, Boolean> filterFunction) {
    Seq<org.apache.spark.sql.catalyst.expressions.Expression> list =
        expression.collect(
            new PartialFunction<
                org.apache.spark.sql.catalyst.expressions.Expression,
                org.apache.spark.sql.catalyst.expressions.Expression>() {
              @Override
              public org.apache.spark.sql.catalyst.expressions.Expression apply(
                  org.apache.spark.sql.catalyst.expressions.Expression expr) {
                return expr;
              }

              @Override
              public boolean isDefinedAt(
                  org.apache.spark.sql.catalyst.expressions.Expression expr) {
                return filterFunction.apply(expr);
              }
            });

    return Lists.newArrayList(JavaConverters.asJavaCollection(list));
  }
}
