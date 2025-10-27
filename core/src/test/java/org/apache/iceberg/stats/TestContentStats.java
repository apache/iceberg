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
package org.apache.iceberg.stats;

import static org.apache.iceberg.stats.FieldStatistic.AVG_VALUE_SIZE;
import static org.apache.iceberg.stats.FieldStatistic.LOWER_BOUND;
import static org.apache.iceberg.stats.FieldStatistic.MAX_VALUE_SIZE;
import static org.apache.iceberg.stats.FieldStatistic.NAN_VALUE_COUNT;
import static org.apache.iceberg.stats.FieldStatistic.NULL_VALUE_COUNT;
import static org.apache.iceberg.stats.FieldStatistic.UPPER_BOUND;
import static org.apache.iceberg.stats.FieldStatistic.VALUE_COUNT;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestContentStats {

  @Test
  public void emptyContentStats() {
    BaseContentStats stats = BaseContentStats.builder().build();
    assertThat(stats).isNotNull();
    assertThat(stats.fieldStats()).isEmpty();
  }

  @Test
  public void validContentStats() {
    BaseFieldStats<?> fieldStatsOne = BaseFieldStats.builder().fieldId(1).build();
    BaseFieldStats<?> fieldStatsTwo = BaseFieldStats.builder().fieldId(2).build();
    BaseContentStats stats =
        BaseContentStats.builder()
            .withFieldStats(fieldStatsOne)
            .withFieldStats(fieldStatsTwo)
            .build();

    assertThat(stats.fieldStats()).containsExactly(fieldStatsOne, fieldStatsTwo);
    assertThat(stats.size()).isEqualTo(stats.fieldStats().size()).isEqualTo(2);
  }

  @Test
  public void buildFromExistingStats() {
    BaseFieldStats<?> fieldStatsOne = BaseFieldStats.builder().fieldId(1).build();
    BaseFieldStats<?> fieldStatsTwo = BaseFieldStats.builder().fieldId(2).build();
    BaseFieldStats<?> fieldStatsThree = BaseFieldStats.builder().fieldId(3).build();

    BaseContentStats stats =
        BaseContentStats.buildFrom(
                BaseContentStats.builder()
                    .withFieldStats(fieldStatsOne)
                    .withFieldStats(fieldStatsTwo)
                    .build())
            .withFieldStats(fieldStatsThree)
            .build();
    assertThat(stats.fieldStats()).containsExactly(fieldStatsOne, fieldStatsTwo, fieldStatsThree);
  }

  @Test
  public void buildFromExistingStatsWithRequestedIds() {
    BaseFieldStats<?> fieldStatsOne = BaseFieldStats.builder().fieldId(1).build();
    BaseFieldStats<?> fieldStatsTwo = BaseFieldStats.builder().fieldId(2).build();
    BaseFieldStats<?> fieldStatsThree = BaseFieldStats.builder().fieldId(3).build();

    BaseContentStats stats =
        BaseContentStats.builder()
            .withFieldStats(fieldStatsOne)
            .withFieldStats(fieldStatsTwo)
            .withFieldStats(fieldStatsThree)
            .build();

    assertThat(BaseContentStats.buildFrom(stats, null).build()).isEqualTo(stats);
    assertThat(BaseContentStats.buildFrom(stats, ImmutableSet.of(1, 3)).build().fieldStats())
        .containsExactly(fieldStatsOne, fieldStatsThree);
    assertThat(BaseContentStats.buildFrom(stats, ImmutableSet.of(2)).build().fieldStats())
        .containsExactly(fieldStatsTwo);
    assertThat(
            BaseContentStats.buildFrom(stats, ImmutableSet.of(2, 5, 10, 12)).build().fieldStats())
        .containsExactly(fieldStatsTwo);
    assertThat(BaseContentStats.buildFrom(stats, ImmutableSet.of(5, 10, 12)).build().fieldStats())
        .isEmpty();
  }

  @Test
  public void retrievalByPosition() {
    BaseFieldStats<?> fieldStatsOne = BaseFieldStats.builder().fieldId(1).build();
    BaseFieldStats<?> fieldStatsTwo = BaseFieldStats.builder().fieldId(2).build();
    BaseContentStats stats =
        BaseContentStats.builder()
            .withFieldStats(fieldStatsOne)
            .withFieldStats(fieldStatsTwo)
            .build();

    assertThat(stats.get(0, FieldStats.class)).isEqualTo(fieldStatsOne);
    assertThat(stats.get(1, FieldStats.class)).isEqualTo(fieldStatsTwo);
    assertThat(stats.get(2, FieldStats.class)).isNull();
    assertThat(stats.get(10, FieldStats.class)).isNull();

    assertThatThrownBy(() -> stats.get(0, Long.class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Wrong class, expected java.lang.Long but was org.apache.iceberg.stats.BaseFieldStats for object:");
  }

  @Test
  public void setByPosition() {
    Schema tableSchema = new Schema(required(1, "id", Types.IntegerType.get()));
    Types.StructType rootStatsStruct = StatsUtil.contentStatsFor(tableSchema).type().asStructType();
    Types.StructType statsStructForIdField = rootStatsStruct.fields().get(0).type().asStructType();

    GenericRecord record = GenericRecord.create(statsStructForIdField);
    BaseFieldStats<Integer> fieldStats =
        BaseFieldStats.<Integer>builder()
            .type(Types.IntegerType.get())
            .fieldId(1)
            .valueCount(10L)
            .nullValueCount(2L)
            .nanValueCount(3L)
            .avgValueSize(30)
            .maxValueSize(70)
            .lowerBound(5)
            .upperBound(20)
            .build();

    record.set(VALUE_COUNT.offset(), fieldStats.valueCount());
    record.set(NULL_VALUE_COUNT.offset(), fieldStats.nullValueCount());
    record.set(NAN_VALUE_COUNT.offset(), fieldStats.nanValueCount());
    record.set(AVG_VALUE_SIZE.offset(), fieldStats.avgValueSize());
    record.set(MAX_VALUE_SIZE.offset(), fieldStats.maxValueSize());
    record.set(LOWER_BOUND.offset(), fieldStats.lowerBound());
    record.set(UPPER_BOUND.offset(), fieldStats.upperBound());

    // this is typically called by Avro reflection code
    BaseContentStats stats = new BaseContentStats(rootStatsStruct);
    stats.set(0, record);
    assertThat(stats.fieldStats()).containsExactly(fieldStats);
  }

  @Test
  public void setByPositionWithInvalidLowerAndUpperBound() {
    Schema tableSchema = new Schema(required(1, "id", Types.IntegerType.get()));
    Types.StructType rootStatsStruct = StatsUtil.contentStatsFor(tableSchema).type().asStructType();
    Types.StructType statsStructForIdField = rootStatsStruct.fields().get(0).type().asStructType();

    GenericRecord record = GenericRecord.create(statsStructForIdField);
    // this is typically called by Avro reflection code
    BaseContentStats stats = new BaseContentStats(rootStatsStruct);

    // invalid lower bound
    record.set(LOWER_BOUND.offset(), 5.0);
    assertThatThrownBy(() -> stats.set(0, record))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid lower bound type, expected a subtype of class java.lang.Integer: java.lang.Double");

    // set valid lower bound so that upper bound is evaluated
    record.set(LOWER_BOUND.offset(), 5);

    // invalid upper bound
    record.set(UPPER_BOUND.offset(), "20");
    assertThatThrownBy(() -> stats.set(0, record))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid upper bound type, expected a subtype of class java.lang.Integer: java.lang.String");
  }
}
