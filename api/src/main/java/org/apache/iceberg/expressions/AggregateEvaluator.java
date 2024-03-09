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
package org.apache.iceberg.expressions;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.expressions.BoundAggregate.Aggregator;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;

/**
 * A class for evaluating aggregates. It evaluates each of the aggregates and updates the aggregated
 * value. The final aggregated result can be returned by {@link #result()}.
 */
public class AggregateEvaluator {

  public static AggregateEvaluator create(Schema schema, List<Expression> aggregates) {
    return create(schema.asStruct(), aggregates);
  }

  public static AggregateEvaluator create(List<BoundAggregate<?, ?>> aggregates) {
    return new AggregateEvaluator(aggregates);
  }

  private static AggregateEvaluator create(Types.StructType struct, List<Expression> aggregates) {
    List<BoundAggregate<?, ?>> boundAggregates =
        aggregates.stream()
            .map(expr -> Binder.bind(struct, expr))
            .map(bound -> (BoundAggregate<?, ?>) bound)
            .collect(Collectors.toList());

    return new AggregateEvaluator(boundAggregates);
  }

  private final List<Aggregator<?>> aggregators;
  private final Types.StructType resultType;
  private final List<BoundAggregate<?, ?>> aggregates;

  private AggregateEvaluator(List<BoundAggregate<?, ?>> aggregates) {
    ImmutableList.Builder<Aggregator<?>> aggregatorsBuilder = ImmutableList.builder();
    List<Types.NestedField> resultFields = Lists.newArrayList();

    for (int pos = 0; pos < aggregates.size(); pos += 1) {
      BoundAggregate<?, ?> aggregate = aggregates.get(pos);
      aggregatorsBuilder.add(aggregate.newAggregator());
      resultFields.add(Types.NestedField.optional(pos, aggregate.describe(), aggregate.type()));
    }

    this.aggregators = aggregatorsBuilder.build();
    this.resultType = Types.StructType.of(resultFields);
    this.aggregates = aggregates;
  }

  public void update(StructLike struct) {
    for (Aggregator<?> aggregator : aggregators) {
      aggregator.update(struct);
    }
  }

  public void update(DataFile file) {
    for (Aggregator<?> aggregator : aggregators) {
      aggregator.update(file);
    }
  }

  public Types.StructType resultType() {
    return resultType;
  }

  public boolean allAggregatorsValid() {
    return aggregators.stream().allMatch(BoundAggregate.Aggregator::isValid);
  }

  public StructLike result() {
    Object[] results =
        aggregators.stream().map(BoundAggregate.Aggregator::result).toArray(Object[]::new);
    return new ArrayStructLike(results);
  }

  public List<BoundAggregate<?, ?>> aggregates() {
    return aggregates;
  }

  private static class ArrayStructLike implements StructLike {
    private final Object[] values;

    private ArrayStructLike(Object[] values) {
      this.values = values;
    }

    @Override
    public int size() {
      return values.length;
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      return javaClass.cast(values[pos]);
    }

    @Override
    public <T> void set(int pos, T value) {
      values[pos] = value;
    }
  }
}
