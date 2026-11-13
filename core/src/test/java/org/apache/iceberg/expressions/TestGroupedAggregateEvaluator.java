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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestGroupedAggregateEvaluator {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          optional(2, "category", Types.StringType.get()),
          optional(3, "region", Types.StringType.get()));

  private static final PartitionSpec CATEGORY_SPEC =
      PartitionSpec.builderFor(SCHEMA).withSpecId(0).identity("category").build();
  private static final PartitionSpec BUCKET_AND_CATEGORY_SPEC =
      PartitionSpec.builderFor(SCHEMA).withSpecId(1).bucket("id", 4).identity("category").build();
  private static final PartitionSpec CATEGORY_AND_REGION_SPEC =
      PartitionSpec.builderFor(SCHEMA)
          .withSpecId(2)
          .identity("category")
          .identity("region")
          .build();
  private static final PartitionSpec BUCKET_CATEGORY_SPEC =
      PartitionSpec.builderFor(SCHEMA).withSpecId(3).bucket("category", 4).build();
  private static final PartitionSpec UNPARTITIONED_SPEC =
      PartitionSpec.builderFor(SCHEMA).withSpecId(4).build();

  private static final Map<Integer, PartitionSpec> SPECS =
      ImmutableMap.of(
          CATEGORY_SPEC.specId(), CATEGORY_SPEC,
          BUCKET_AND_CATEGORY_SPEC.specId(), BUCKET_AND_CATEGORY_SPEC,
          CATEGORY_AND_REGION_SPEC.specId(), CATEGORY_AND_REGION_SPEC,
          BUCKET_CATEGORY_SPEC.specId(), BUCKET_CATEGORY_SPEC,
          UNPARTITIONED_SPEC.specId(), UNPARTITIONED_SPEC);

  private static final List<BoundAggregate<?, ?>> AGGREGATES =
      ImmutableList.of(
          bind(Expressions.countStar()), bind(Expressions.min("id")), bind(Expressions.max("id")));

  @Test
  void groupByIdentityPartition() {
    GroupedAggregateEvaluator evaluator = create(2);
    evaluator.update(file(CATEGORY_SPEC, Row.of("a"), 10, 1, 20));
    evaluator.update(file(CATEGORY_SPEC, Row.of("a"), 5, 15, 30));
    evaluator.update(file(CATEGORY_SPEC, Row.of("b"), 7, 40, 50));

    assertThat(evaluator.allAggregatorsValid()).isTrue();
    assertThat(rows(evaluator))
        .containsExactlyInAnyOrder(Arrays.asList("a", 15L, 1, 30), Arrays.asList("b", 7L, 40, 50));
  }

  @Test
  void groupByMultipleFields() {
    GroupedAggregateEvaluator evaluator = create(3, 2);
    evaluator.update(file(CATEGORY_AND_REGION_SPEC, Row.of("a", "us"), 10, 1, 20));
    evaluator.update(file(CATEGORY_AND_REGION_SPEC, Row.of("a", "eu"), 5, 15, 30));
    evaluator.update(file(CATEGORY_AND_REGION_SPEC, Row.of("a", "us"), 7, 40, 50));

    assertThat(evaluator.allAggregatorsValid()).isTrue();
    assertThat(rows(evaluator))
        .containsExactlyInAnyOrder(
            Arrays.asList("us", "a", 17L, 1, 50), Arrays.asList("eu", "a", 5L, 15, 30));
  }

  @Test
  void groupAcrossSpecs() {
    GroupedAggregateEvaluator evaluator = create(2);
    evaluator.update(file(CATEGORY_SPEC, Row.of("a"), 10, 1, 20));
    evaluator.update(file(BUCKET_AND_CATEGORY_SPEC, Row.of(3, "a"), 5, 15, 30));

    assertThat(evaluator.allAggregatorsValid()).isTrue();
    assertThat(rows(evaluator)).containsExactly(Arrays.asList("a", 15L, 1, 30));
  }

  @Test
  void groupByNullPartitionValue() {
    GroupedAggregateEvaluator evaluator = create(2);
    evaluator.update(file(CATEGORY_SPEC, Row.of((Object) null), 10, 1, 20));
    evaluator.update(file(CATEGORY_SPEC, Row.of((Object) null), 5, 15, 30));

    assertThat(evaluator.allAggregatorsValid()).isTrue();
    assertThat(rows(evaluator)).containsExactly(Arrays.asList(null, 15L, 1, 30));
  }

  @Test
  void fileSpecWithoutGroupByField() {
    GroupedAggregateEvaluator evaluator = create(2);
    evaluator.update(file(CATEGORY_SPEC, Row.of("a"), 10, 1, 20));
    evaluator.update(file(UNPARTITIONED_SPEC, null, 5, 15, 30));

    assertThat(evaluator.allAggregatorsValid()).isFalse();
  }

  @Test
  void nonIdentityPartitionTransform() {
    GroupedAggregateEvaluator evaluator = create(2);
    evaluator.update(file(BUCKET_CATEGORY_SPEC, Row.of(1), 10, 1, 20));

    assertThat(evaluator.allAggregatorsValid()).isFalse();
  }

  @Test
  void missingStats() {
    GroupedAggregateEvaluator evaluator = create(2);
    evaluator.update(file(CATEGORY_SPEC, Row.of("a"), 10, 1, 20));
    evaluator.update(
        DataFiles.builder(CATEGORY_SPEC)
            .withPath("/path/to/no-stats.parquet")
            .withFileSizeInBytes(10)
            .withPartition(Row.of("b"))
            .withRecordCount(5)
            .build());

    assertThat(evaluator.allAggregatorsValid()).isFalse();
  }

  @Test
  void resultType() {
    GroupedAggregateEvaluator evaluator = create(3, 2);

    List<String> expectedNames =
        ImmutableList.<String>builder()
            .add(SCHEMA.findColumnName(3), SCHEMA.findColumnName(2))
            .addAll(AGGREGATES.stream().map(BoundAggregate::describe).collect(Collectors.toList()))
            .build();
    List<Types.NestedField> fields = evaluator.resultType().fields();
    assertThat(fields).extracting(Types.NestedField::name).isEqualTo(expectedNames);
    assertThat(fields).allMatch(Types.NestedField::isOptional);
    assertThat(fields.get(0).type()).isEqualTo(SCHEMA.findType(3));
    assertThat(fields.get(2).type()).isEqualTo(AGGREGATES.get(0).type());
  }

  @Test
  void unknownGroupByField() {
    assertThatThrownBy(
            () -> GroupedAggregateEvaluator.create(SCHEMA, SPECS, ImmutableList.of(99), AGGREGATES))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find group by field with ID 99");
  }

  @Test
  void emptyGroupByFields() {
    assertThatThrownBy(
            () -> GroupedAggregateEvaluator.create(SCHEMA, SPECS, ImmutableList.of(), AGGREGATES))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid group by fields: empty");
  }

  private static GroupedAggregateEvaluator create(Integer... groupByFieldIds) {
    return GroupedAggregateEvaluator.create(
        SCHEMA, SPECS, ImmutableList.copyOf(groupByFieldIds), AGGREGATES);
  }

  private static BoundAggregate<?, ?> bind(Expression aggregate) {
    return (BoundAggregate<?, ?>) Binder.bind(SCHEMA.asStruct(), aggregate);
  }

  private static DataFile file(
      PartitionSpec spec, StructLike partition, long recordCount, int minId, int maxId) {
    DataFiles.Builder builder =
        DataFiles.builder(spec)
            .withPath("/path/to/data-" + minId + ".parquet")
            .withFileSizeInBytes(10)
            .withMetrics(
                new Metrics(
                    recordCount,
                    null,
                    ImmutableMap.of(1, recordCount),
                    ImmutableMap.of(1, 0L),
                    null,
                    ImmutableMap.of(1, Conversions.toByteBuffer(Types.IntegerType.get(), minId)),
                    ImmutableMap.of(1, Conversions.toByteBuffer(Types.IntegerType.get(), maxId))));
    if (partition != null) {
      builder.withPartition(partition);
    }

    return builder.build();
  }

  private static List<List<Object>> rows(GroupedAggregateEvaluator evaluator) {
    int size = evaluator.resultType().fields().size();
    return evaluator.results().stream()
        .map(
            row -> {
              Object[] values = new Object[size];
              for (int pos = 0; pos < size; pos += 1) {
                values[pos] = row.get(pos, Object.class);
              }

              return Arrays.asList(values);
            })
        .collect(Collectors.toList());
  }
}
