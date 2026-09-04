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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.expressions.AggregateExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.functions.aggfunctions.Count1AggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.CountAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.MaxAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.MinAggFunction;
import org.apache.flink.table.types.DataType;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expression.Operation;
import org.apache.iceberg.expressions.UnboundAggregate;
import org.junit.jupiter.api.Test;

public class TestFlinkAggregates {

  private static final DataType INT_TYPE = DataTypes.INT();

  private static FieldReferenceExpression field(String name) {
    return new FieldReferenceExpression(name, INT_TYPE, 0, 0);
  }

  private static AggregateExpression aggregate(
      FunctionDefinition function, List<FieldReferenceExpression> args) {
    return new AggregateExpression(function, args, null, INT_TYPE, false, false, false);
  }

  @Test
  public void countStar() {
    Expression converted = FlinkAggregates.convert(aggregate(new Count1AggFunction(), List.of()));
    assertThat(converted).isInstanceOf(UnboundAggregate.class);
    assertThat(converted.op()).isEqualTo(Operation.COUNT_STAR);
  }

  @Test
  public void countColumn() {
    Expression converted =
        FlinkAggregates.convert(aggregate(new CountAggFunction(), List.of(field("id"))));
    assertThat(converted).isInstanceOf(UnboundAggregate.class);
    UnboundAggregate<?> aggregate = (UnboundAggregate<?>) converted;
    assertThat(aggregate.op()).isEqualTo(Operation.COUNT);
    assertThat(aggregate.ref().name()).isEqualTo("id");
  }

  @Test
  public void max() {
    Expression converted =
        FlinkAggregates.convert(
            aggregate(new MaxAggFunction.IntMaxAggFunction(), List.of(field("id"))));
    assertThat(converted).isInstanceOf(UnboundAggregate.class);
    UnboundAggregate<?> aggregate = (UnboundAggregate<?>) converted;
    assertThat(aggregate.op()).isEqualTo(Operation.MAX);
    assertThat(aggregate.ref().name()).isEqualTo("id");
  }

  @Test
  public void min() {
    Expression converted =
        FlinkAggregates.convert(
            aggregate(new MinAggFunction.IntMinAggFunction(), List.of(field("id"))));
    assertThat(converted).isInstanceOf(UnboundAggregate.class);
    UnboundAggregate<?> aggregate = (UnboundAggregate<?>) converted;
    assertThat(aggregate.op()).isEqualTo(Operation.MIN);
    assertThat(aggregate.ref().name()).isEqualTo("id");
  }

  @Test
  public void countDistinctIsNotPushedDown() {
    AggregateExpression distinctCount =
        new AggregateExpression(
            new CountAggFunction(), List.of(field("id")), null, INT_TYPE, true, false, false);
    assertThat(FlinkAggregates.convert(distinctCount)).isNull();
  }

  @Test
  public void approximateAggregateIsNotPushedDown() {
    AggregateExpression approxCount =
        new AggregateExpression(
            new CountAggFunction(), List.of(field("id")), null, INT_TYPE, false, true, false);
    assertThat(FlinkAggregates.convert(approxCount)).isNull();
  }

  @Test
  public void unsupportedFunctionIsNotPushedDown() {
    Expression converted =
        FlinkAggregates.convert(
            aggregate(BuiltInFunctionDefinitions.SUM, List.of(field("amount"))));
    assertThat(converted).isNull();
  }

  @Test
  public void countColumnWithoutArgIsNotPushedDown() {
    assertThat(FlinkAggregates.convert(aggregate(new CountAggFunction(), List.of()))).isNull();
  }
}
