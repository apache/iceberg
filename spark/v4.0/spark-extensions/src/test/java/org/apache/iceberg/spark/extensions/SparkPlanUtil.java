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
package org.apache.iceberg.spark.extensions;

import static scala.collection.JavaConverters.seqAsJavaListConverter;

import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.execution.CommandResultExec;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper;
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec;
import scala.PartialFunction;
import scala.collection.Seq;

public class SparkPlanUtil {

  private static final AdaptiveSparkPlanHelper SPARK_HELPER = new AdaptiveSparkPlanHelper() {};

  private SparkPlanUtil() {}

  public static List<SparkPlan> collectLeaves(SparkPlan plan) {
    return toJavaList(SPARK_HELPER.collectLeaves(actualPlan(plan)));
  }

  public static List<SparkPlan> collectBatchScans(SparkPlan plan) {
    List<SparkPlan> leaves = collectLeaves(plan);
    return leaves.stream()
        .filter(scan -> scan instanceof BatchScanExec)
        .collect(Collectors.toList());
  }

  private static SparkPlan actualPlan(SparkPlan plan) {
    if (plan instanceof CommandResultExec) {
      return ((CommandResultExec) plan).commandPhysicalPlan();
    } else {
      return plan;
    }
  }

  public static List<Expression> collectExprs(
      SparkPlan sparkPlan, Predicate<Expression> predicate) {
    Seq<List<Expression>> seq =
        SPARK_HELPER.collect(
            sparkPlan,
            new PartialFunction<SparkPlan, List<Expression>>() {
              @Override
              public List<Expression> apply(SparkPlan plan) {
                List<Expression> exprs = Lists.newArrayList();

                for (Expression expr : toJavaList(plan.expressions())) {
                  exprs.addAll(collectExprs(expr, predicate));
                }

                return exprs;
              }

              @Override
              public boolean isDefinedAt(SparkPlan plan) {
                return true;
              }
            });
    return toJavaList(seq).stream().flatMap(Collection::stream).collect(Collectors.toList());
  }

  private static List<Expression> collectExprs(
      Expression expression, Predicate<Expression> predicate) {
    Seq<Expression> seq =
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
    return toJavaList(seq);
  }

  private static <T> List<T> toJavaList(Seq<T> seq) {
    return seqAsJavaListConverter(seq).asJava();
  }
}
