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
import org.apache.flink.table.expressions.AggregateExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;

/**
 * Converts a Flink {@link AggregateExpression} to an Iceberg {@link Expression} that {@link
 * org.apache.iceberg.expressions.AggregateEvaluator} can evaluate from file-level metrics alone,
 * without reading any data files.
 *
 * <p>Only {@code COUNT(*)}, {@code COUNT(col)}, {@code MAX(col)} and {@code MIN(col)} can be
 * derived from file metrics; {@code SUM} and {@code AVG} are not tracked by Iceberg manifests and
 * are never converted.
 */
public class FlinkAggregates {

  private static final String FLINK_AGG_FUNCTIONS_PACKAGE =
      "org.apache.flink.table.planner.functions.aggfunctions.";

  private FlinkAggregates() {}

  public static Expression convert(AggregateExpression aggregate) {
    if (aggregate.isDistinct()
        || aggregate.isApproximate()
        || aggregate.getFilterExpression().isPresent()) {
      return null;
    }

    FunctionDefinition function = aggregate.getFunctionDefinition();
    List<FieldReferenceExpression> args = aggregate.getArgs();

    // The planner hands over the instantiated aggregate function implementation (e.g.
    // MaxAggFunction.IntMaxAggFunction). Those classes live in flink-table-planner and are hidden
    // from connector code behind flink-table-planner-loader, so an instanceof check would throw
    // NoClassDefFoundError in a stock distribution. Reflect on the runtime class already loaded by
    // the planner instead of resolving it here, and gate on the declaring package so a user-defined
    // aggregate whose name resembles a built-in one is not mistaken for it.
    Class<?> functionClass = function.getClass();
    if (!functionClass.getName().startsWith(FLINK_AGG_FUNCTIONS_PACKAGE)) {
      return null;
    }

    String functionName = functionClass.getSimpleName();
    if ("Count1AggFunction".equals(functionName)) {
      return Expressions.countStar();
    } else if ("CountAggFunction".equals(functionName)) {
      return args.size() == 1 ? Expressions.count(args.get(0).getName()) : null;
    } else if (functionName.endsWith("MaxAggFunction")) {
      return args.size() == 1 ? Expressions.max(args.get(0).getName()) : null;
    } else if (functionName.endsWith("MinAggFunction")) {
      return args.size() == 1 ? Expressions.min(args.get(0).getName()) : null;
    }

    return null;
  }
}
