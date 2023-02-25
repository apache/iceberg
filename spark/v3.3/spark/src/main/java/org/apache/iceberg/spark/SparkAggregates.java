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
package org.apache.iceberg.spark;

import java.util.Map;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expression.Operation;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc;
import org.apache.spark.sql.connector.expressions.aggregate.Count;
import org.apache.spark.sql.connector.expressions.aggregate.CountStar;
import org.apache.spark.sql.connector.expressions.aggregate.Max;
import org.apache.spark.sql.connector.expressions.aggregate.Min;

public class SparkAggregates {
  private SparkAggregates() {}

  private static final Map<Class<? extends AggregateFunc>, Operation> AGGREGATES =
      ImmutableMap.<Class<? extends AggregateFunc>, Operation>builder()
          .put(Count.class, Operation.COUNT)
          .put(CountStar.class, Operation.COUNT_STAR)
          .put(Max.class, Operation.MAX)
          .put(Min.class, Operation.MIN)
          .buildOrThrow();

  public static Expression convert(AggregateFunc aggregate) {
    Operation op = AGGREGATES.get(aggregate.getClass());
    if (op != null) {
      switch (op) {
        case COUNT:
          Count countAgg = (Count) aggregate;
          if (countAgg.isDistinct()) {
            // manifest file doesn't have count distinct so this can't be pushed down
            return null;
          }

          if (countAgg.column() instanceof NamedReference) {
            return Expressions.count(SparkUtil.toColumnName((NamedReference) countAgg.column()));
          } else {
            return null;
          }

        case COUNT_STAR:
          return Expressions.countStar();

        case MAX:
          Max maxAgg = (Max) aggregate;
          if (maxAgg.column() instanceof NamedReference) {
            return Expressions.max(SparkUtil.toColumnName((NamedReference) maxAgg.column()));
          } else {
            return null;
          }

        case MIN:
          Min minAgg = (Min) aggregate;
          if (minAgg.column() instanceof NamedReference) {
            return Expressions.min(SparkUtil.toColumnName((NamedReference) minAgg.column()));
          } else {
            return null;
          }
      }
    }

    return null;
  }
}
