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

import static org.apache.iceberg.types.Conversions.toByteBuffer;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.TestHelpers.TestDataFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.StringType;
import org.junit.jupiter.api.Test;

public class TestAggregateEvaluator {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", IntegerType.get()),
          optional(2, "no_stats", IntegerType.get()),
          optional(3, "all_nulls", StringType.get()),
          optional(4, "some_nulls", StringType.get()));

  private static final DataFile FILE =
      new TestDataFile(
          "file.avro",
          Row.of(),
          50,
          // any value counts, including nulls
          ImmutableMap.of(1, 50L, 3, 50L, 4, 50L),
          // null value counts
          ImmutableMap.of(1, 10L, 3, 50L, 4, 10L),
          // nan value counts
          null,
          // lower bounds
          ImmutableMap.of(1, toByteBuffer(IntegerType.get(), 33)),
          // upper bounds
          ImmutableMap.of(1, toByteBuffer(IntegerType.get(), 2345)));

  private static final DataFile MISSING_SOME_NULLS_STATS_1 =
      new TestDataFile(
          "file_2.avro",
          Row.of(),
          20,
          // any value counts, including nulls
          ImmutableMap.of(1, 20L, 3, 20L),
          // null value counts
          ImmutableMap.of(1, 0L, 3, 20L),
          // nan value counts
          null,
          // lower bounds
          ImmutableMap.of(1, toByteBuffer(IntegerType.get(), 33)),
          // upper bounds
          ImmutableMap.of(1, toByteBuffer(IntegerType.get(), 100)));

  private static final DataFile MISSING_SOME_NULLS_STATS_2 =
      new TestDataFile(
          "file_3.avro",
          Row.of(),
          20,
          // any value counts, including nulls
          ImmutableMap.of(1, 20L, 3, 20L),
          // null value counts
          ImmutableMap.of(1, 20L, 3, 20L),
          // nan value counts
          null,
          // lower bounds
          ImmutableMap.of(1, toByteBuffer(IntegerType.get(), -33)),
          // upper bounds
          ImmutableMap.of(1, toByteBuffer(IntegerType.get(), 3333)));

  private static final DataFile[] dataFiles = {
    FILE, MISSING_SOME_NULLS_STATS_1, MISSING_SOME_NULLS_STATS_2
  };

  @Test
  public void testIntAggregate() {
    List<Expression> list =
        ImmutableList.of(
            Expressions.countStar(),
            Expressions.count("id"),
            Expressions.max("id"),
            Expressions.min("id"));
    AggregateEvaluator aggregateEvaluator = AggregateEvaluator.create(SCHEMA, list);

    for (DataFile dataFile : dataFiles) {
      aggregateEvaluator.update(dataFile, dataFile.partition());
    }

    assertThat(aggregateEvaluator.allAggregatorsValid()).isTrue();
    StructLike result = aggregateEvaluator.result();
    Object[] expected = {90L, 60L, 3333, -33};
    assertEvaluatorResult(result, expected);
  }

  @Test
  public void testAllNulls() {
    List<Expression> list =
        ImmutableList.of(
            Expressions.countStar(),
            Expressions.count("all_nulls"),
            Expressions.max("all_nulls"),
            Expressions.min("all_nulls"));
    AggregateEvaluator aggregateEvaluator = AggregateEvaluator.create(SCHEMA, list);

    for (DataFile dataFile : dataFiles) {
      aggregateEvaluator.update(dataFile, dataFile.partition());
    }

    assertThat(aggregateEvaluator.allAggregatorsValid()).isTrue();
    StructLike result = aggregateEvaluator.result();
    Object[] expected = {90L, 0L, null, null};
    assertEvaluatorResult(result, expected);
  }

  @Test
  public void testSomeNulls() {
    List<Expression> list =
        ImmutableList.of(
            Expressions.countStar(),
            Expressions.count("some_nulls"),
            Expressions.max("some_nulls"),
            Expressions.min("some_nulls"));
    AggregateEvaluator aggregateEvaluator = AggregateEvaluator.create(SCHEMA, list);
    for (DataFile dataFile : dataFiles) {
      aggregateEvaluator.update(dataFile, dataFile.partition());
    }

    assertThat(aggregateEvaluator.allAggregatorsValid()).isFalse();
    StructLike result = aggregateEvaluator.result();
    Object[] expected = {90L, null, null, null};
    assertEvaluatorResult(result, expected);
  }

  @Test
  public void testNoStats() {
    List<Expression> list =
        ImmutableList.of(
            Expressions.countStar(),
            Expressions.count("no_stats"),
            Expressions.max("no_stats"),
            Expressions.min("no_stats"));
    AggregateEvaluator aggregateEvaluator = AggregateEvaluator.create(SCHEMA, list);
    for (DataFile dataFile : dataFiles) {
      aggregateEvaluator.update(dataFile, dataFile.partition());
    }

    assertThat(aggregateEvaluator.allAggregatorsValid()).isFalse();
    StructLike result = aggregateEvaluator.result();
    Object[] expected = {90L, null, null, null};
    assertEvaluatorResult(result, expected);
  }

  private void assertEvaluatorResult(StructLike result, Object[] expected) {
    Object[] actual = new Object[result.size()];
    for (int i = 0; i < result.size(); i++) {
      actual[i] = result.get(i, Object.class);
    }

    assertThat(actual).as("equals").isEqualTo(expected);
  }
}
