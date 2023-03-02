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

import java.util.Arrays;
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

  public static AggregateEvaluator create(
      Schema schema, List<Expression> aggregates, List<Expression> groupBys) {
    return create(schema.asStruct(), aggregates, groupBys);
  }

  public static AggregateEvaluator create(
      List<BoundAggregate<?, ?>> aggregates, List<BoundGroupBy<?, ?>> groupBys) {
    return new AggregateEvaluator(aggregates, groupBys);
  }

  private static AggregateEvaluator create(
      Types.StructType struct, List<Expression> aggregates, List<Expression> groupBys) {
    List<BoundAggregate<?, ?>> boundAggregates =
        aggregates.stream()
            .map(expr -> Binder.bind(struct, expr))
            .map(bound -> (BoundAggregate<?, ?>) bound)
            .collect(Collectors.toList());

    List<BoundGroupBy<?, ?>> boundGroupBys =
        groupBys.stream()
            .map(expr -> Binder.bind(struct, expr))
            .map(bound -> (BoundGroupBy<?, ?>) bound)
            .collect(Collectors.toList());

    return new AggregateEvaluator(boundAggregates, boundGroupBys);
  }

  private final List<Aggregator<?>> aggregators;
  private final Types.StructType resultType;
  private final List<BoundAggregate<?, ?>> aggregates;
  private final List<BoundGroupBy<?, ?>> groupBys;

  private AggregateEvaluator(
      List<BoundAggregate<?, ?>> aggregates, List<BoundGroupBy<?, ?>> groupBys) {
    ImmutableList.Builder<Aggregator<?>> aggregatorsBuilder = ImmutableList.builder();
    List<Types.NestedField> resultFields = Lists.newArrayList();

    for (int pos = 0; pos < groupBys.size(); pos += 1) {
      BoundGroupBy<?, ?> groupBy = groupBys.get(pos);
      Types.NestedField field = groupBy.ref().field();
      resultFields.add(Types.NestedField.optional(pos, field.name(), field.type()));
    }

    for (int pos = groupBys.size(); pos < aggregates.size() + groupBys.size(); pos += 1) {
      BoundAggregate<?, ?> aggregate = aggregates.get(pos - groupBys.size());
      aggregatorsBuilder.add(aggregate.newAggregator(groupBys));
      resultFields.add(Types.NestedField.optional(pos, aggregate.describe(), aggregate.type()));
    }

    this.aggregators = aggregatorsBuilder.build();
    this.resultType = Types.StructType.of(resultFields);
    this.aggregates = aggregates;
    this.groupBys = groupBys;
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

  public StructLike[] result() {
    if (groupBys.isEmpty()) {
      ArrayStructLike[] result = new ArrayStructLike[1];
      result[0] =
          new ArrayStructLike(
              aggregators.stream().map(BoundAggregate.Aggregator::result).toArray(Object[]::new));
      return result;
    }

    int numRows = aggregators.get(0).numOfPartitions();
    int numCols = groupBys.size() + aggregators.size();

    Object[][] res = new Object[numRows][numCols];

    Object[] keys = aggregators.get(0).partitionResult().keySet().toArray();
    if (!groupBys.isEmpty()) {
      for (int i = 0; i < numRows; i++) {
        for (int j = 0; j < groupBys.size(); j++) {
          res[i][j] = ((StructLike) keys[i]).get(j, Object.class);
        }
      }
    }

    for (int i = 0; i < numRows; i++) {
      for (int j = groupBys.size(); j < numCols; j++) {
        res[i][j] = aggregators.get(j - groupBys.size()).partitionResult().get(keys[i]);
      }
    }

    StructLike[] arrayStructLikeArray = new StructLike[numRows];
    for (int i = 0; i < numRows; i++) {
      arrayStructLikeArray[i] = new ArrayStructLike(res[i]);
    }

    return arrayStructLikeArray;
  }

  public List<BoundAggregate<?, ?>> aggregates() {
    return aggregates;
  }

  public static class ArrayStructLike implements StructLike {
    private final Object[] values;

    public ArrayStructLike(Object[] values) {
      this.values = values;
    }

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

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      ArrayStructLike that = (ArrayStructLike) o;
      return Arrays.equals(values, that.values);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(values);
    }
  }
}
