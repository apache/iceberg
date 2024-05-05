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

import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.TestHelpers.TestDataFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types.IntegerType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.iceberg.types.Conversions.toByteBuffer;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAggregateEvaluatorAllStatsOptional {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", IntegerType.get()),
          optional(2, "optional_col", IntegerType.get()));

  private static final DataFile MISSING_ALL_OPTIONAL_STATS =
          new TestDataFile(
                  "file_3.avro",
                  Row.of(),
                  20,
                  // any value counts, including nulls
                  null,
                  // null value counts
                  null,
                  // nan value counts
                  null,
                  // lower bounds
                  null,
                  // upper bounds
                  null);


  private static final DataFile[] dataFiles = {
          MISSING_ALL_OPTIONAL_STATS
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
      aggregateEvaluator.update(dataFile);
    }

    assertThat(aggregateEvaluator.allAggregatorsValid()).isFalse();
    StructLike result = aggregateEvaluator.result();
    Object[] expected = {20L, null, null, null};
    assertEvaluatorResult(result, expected);
  }

  @Test
  public void testOptionalCol() {
    List<Expression> list =
        ImmutableList.of(
            Expressions.countStar(),
            Expressions.count("optional_col"),
            Expressions.max("optional_col"),
            Expressions.min("optional_col"));
    AggregateEvaluator aggregateEvaluator = AggregateEvaluator.create(SCHEMA, list);

    for (DataFile dataFile : dataFiles) {
      aggregateEvaluator.update(dataFile);
    }

    assertThat(aggregateEvaluator.allAggregatorsValid()).isFalse();
    StructLike result = aggregateEvaluator.result();
    Object[] expected = {20L, null, null, null};
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
