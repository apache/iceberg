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
package org.apache.iceberg.flink.source;

import java.util.List;
import org.apache.flink.table.expressions.AggregateExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.functions.aggfunctions.Count1AggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.CountAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.MaxAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.MinAggFunction;
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
  private FlinkAggregates() {}

  public static Expression convert(AggregateExpression aggregate) {
    if (aggregate.isDistinct()
        || aggregate.isApproximate()
        || aggregate.getFilterExpression().isPresent()) {
      return null;
    }

    FunctionDefinition function = aggregate.getFunctionDefinition();
    List<FieldReferenceExpression> args = aggregate.getArgs();

    if (function instanceof Count1AggFunction) {
      return Expressions.countStar();
    } else if (function instanceof CountAggFunction) {
      return args.size() == 1 ? Expressions.count(args.get(0).getName()) : null;
    } else if (function instanceof MaxAggFunction) {
      return args.size() == 1 ? Expressions.max(args.get(0).getName()) : null;
    } else if (function instanceof MinAggFunction) {
      return args.size() == 1 ? Expressions.min(args.get(0).getName()) : null;
    }

    return null;
  }
}
