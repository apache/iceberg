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

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expression.Operation;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc;
import org.apache.spark.sql.connector.expressions.aggregate.Count;
import org.apache.spark.sql.connector.expressions.aggregate.CountStar;
import org.apache.spark.sql.connector.expressions.aggregate.Max;
import org.apache.spark.sql.connector.expressions.aggregate.Min;

public class SparkAggregates {

  private static final Pattern BACKTICKS_PATTERN = Pattern.compile("([`])(.|$)");

  private SparkAggregates() {}

  private static final Map<Class<? extends AggregateFunc>, Operation> AGGREGATES =
      ImmutableMap.<Class<? extends AggregateFunc>, Operation>builder()
          .put(Count.class, Operation.COUNT)
          .put(CountStar.class, Operation.COUNTSTAR)
          .put(Max.class, Operation.MAX)
          .put(Min.class, Operation.MIN)
          .build();

  public static Expression convert(AggregateFunc aggregate) {
    Operation op = AGGREGATES.get(aggregate.getClass());
    if (op != null) {
      switch (op) {
        case COUNT:
          Count countAgg = (Count) aggregate;
          return Expressions.count(unquote(countAgg.column().describe()));
        case COUNTSTAR:
          return Expressions.countStar();
        case MAX:
          Max maxAgg = (Max) aggregate;
          return Expressions.max(unquote(maxAgg.column().describe()));
        case MIN:
          Min minAgg = (Min) aggregate;
          return Expressions.min(unquote(minAgg.column().describe()));
      }
    }

    return null;
  }

  private static String unquote(String attributeName) {
    Matcher matcher = BACKTICKS_PATTERN.matcher(attributeName);
    return matcher.replaceAll("$2");
  }
}
