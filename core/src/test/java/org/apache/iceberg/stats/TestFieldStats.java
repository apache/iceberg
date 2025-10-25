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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestFieldStats {

  @Test
  public void empty() {
    BaseFieldStats<?> empty = BaseFieldStats.builder().build();
    assertThat(empty.fieldId()).isEqualTo(0);
    assertThat(empty.type()).isNull();
    assertThat(empty.valueCount()).isNull();
    assertThat(empty.nullValueCount()).isNull();
    assertThat(empty.nanValueCount()).isNull();
    assertThat(empty.avgValueSize()).isNull();
    assertThat(empty.maxValueSize()).isNull();
    assertThat(empty.lowerBound()).isNull();
    assertThat(empty.upperBound()).isNull();
  }

  @Test
  public void validIndividualValues() {
    BaseFieldStats<Integer> fieldStats =
        BaseFieldStats.<Integer>builder()
            .type(Types.IntegerType.get())
            .fieldId(23)
            .valueCount(10L)
            .nullValueCount(2L)
            .nanValueCount(3L)
            .avgValueSize(30)
            .maxValueSize(70)
            .lowerBound(5)
            .upperBound(20)
            .build();

    assertThat(fieldStats.type()).isEqualTo(Types.IntegerType.get());
    assertThat(fieldStats.fieldId()).isEqualTo(23);
    assertThat(fieldStats.valueCount()).isEqualTo(10L);
    assertThat(fieldStats.nullValueCount()).isEqualTo(2L);
    assertThat(fieldStats.nanValueCount()).isEqualTo(3L);
    assertThat(fieldStats.avgValueSize()).isEqualTo(30);
    assertThat(fieldStats.maxValueSize()).isEqualTo(70);
    assertThat(fieldStats.lowerBound()).isEqualTo(5);
    assertThat(fieldStats.upperBound()).isEqualTo(20);
  }

  @Test
  public void buildFromExistingStats() {
    BaseFieldStats<Integer> fieldStats =
        BaseFieldStats.buildFrom(
                BaseFieldStats.<Integer>builder()
                    .type(Types.IntegerType.get())
                    .fieldId(23)
                    .valueCount(10L)
                    .nullValueCount(2L)
                    .nanValueCount(3L)
                    .avgValueSize(30)
                    .maxValueSize(70)
                    .lowerBound(5)
                    .upperBound(20)
                    .build())
            .lowerBound(2)
            .upperBound(50)
            .maxValueSize(90)
            .build();
    assertThat(fieldStats.type()).isEqualTo(Types.IntegerType.get());
    assertThat(fieldStats.fieldId()).isEqualTo(23);
    assertThat(fieldStats.valueCount()).isEqualTo(10L);
    assertThat(fieldStats.nullValueCount()).isEqualTo(2L);
    assertThat(fieldStats.nanValueCount()).isEqualTo(3L);
    assertThat(fieldStats.avgValueSize()).isEqualTo(30);
    assertThat(fieldStats.maxValueSize()).isEqualTo(90);
    assertThat(fieldStats.lowerBound()).isEqualTo(2);
    assertThat(fieldStats.upperBound()).isEqualTo(50);
  }

  @Test
  public void validFieldStats() {
    assertThat(BaseFieldStats.builder().build()).isNotNull();
    assertThat(BaseFieldStats.builder().fieldId(1).build()).isNotNull();
    assertThat(BaseFieldStats.builder().valueCount(3L).build()).isNotNull();
    assertThat(BaseFieldStats.builder().nullValueCount(3L).build()).isNotNull();
    assertThat(BaseFieldStats.builder().nanValueCount(3L).build()).isNotNull();
    assertThat(BaseFieldStats.builder().type(Types.IntegerType.get()).build()).isNotNull();
    assertThat(BaseFieldStats.builder().avgValueSize(3).build()).isNotNull();
    assertThat(BaseFieldStats.builder().maxValueSize(3).build()).isNotNull();

    assertThat(BaseFieldStats.builder().type(Types.LongType.get()).lowerBound(3L).build())
        .isNotNull();
    assertThat(BaseFieldStats.builder().type(Types.LongType.get()).upperBound(10L).build())
        .isNotNull();
    assertThat(
            BaseFieldStats.builder()
                .type(Types.LongType.get())
                .lowerBound(3L)
                .upperBound(10L)
                .build())
        .isNotNull();
    assertThat(
            BaseFieldStats.<Long>builder()
                .type(Types.LongType.get())
                .lowerBound(3L)
                .upperBound(10L)
                .build())
        .isNotNull();
  }

  @Test
  public void missingTypeWithUpperOrLowerBound() {
    assertThatThrownBy(() -> BaseFieldStats.builder().lowerBound(3).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid type (required when lower bound is set): null");
    assertThatThrownBy(() -> BaseFieldStats.builder().upperBound(3).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid type (required when lower bound is set): null");
  }

  @Test
  public void invalidType() {
    assertThatThrownBy(
            () -> BaseFieldStats.builder().type(Types.LongType.get()).lowerBound(3).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid lower bound type, expected a subtype of java.lang.Long: java.lang.Integer");
    assertThatThrownBy(
            () ->
                BaseFieldStats.<Integer>builder().type(Types.LongType.get()).lowerBound(3).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid lower bound type, expected a subtype of java.lang.Long: java.lang.Integer");

    assertThatThrownBy(
            () -> BaseFieldStats.builder().type(Types.LongType.get()).upperBound(3).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid upper bound type, expected a subtype of java.lang.Long: java.lang.Integer");
    assertThatThrownBy(
            () ->
                BaseFieldStats.<Integer>builder().type(Types.LongType.get()).upperBound(3).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid upper bound type, expected a subtype of java.lang.Long: java.lang.Integer");
  }

  @Test
  public void retrievalByPosition() {
    BaseFieldStats<Integer> fieldStats =
        BaseFieldStats.<Integer>builder()
            .type(Types.IntegerType.get())
            .fieldId(23)
            .valueCount(10L)
            .nullValueCount(2L)
            .nanValueCount(3L)
            .avgValueSize(30)
            .maxValueSize(70)
            .lowerBound(5)
            .upperBound(20)
            .build();

    assertThat(fieldStats.get(StatsUtil.VALUE_COUNT_OFFSET, Long.class)).isEqualTo(10L);
    assertThat(fieldStats.get(StatsUtil.NULL_VALUE_COUNT_OFFSET, Long.class)).isEqualTo(2L);
    assertThat(fieldStats.get(StatsUtil.NAN_VALUE_COUNT_OFFSET, Long.class)).isEqualTo(3L);
    assertThat(fieldStats.get(StatsUtil.AVG_VALUE_SIZE_OFFSET, Integer.class)).isEqualTo(30);
    assertThat(fieldStats.get(StatsUtil.MAX_VALUE_SIZE_OFFSET, Integer.class)).isEqualTo(70);
    assertThat(fieldStats.get(StatsUtil.LOWER_BOUND_OFFSET, Integer.class)).isEqualTo(5);
    assertThat(fieldStats.get(StatsUtil.UPPER_BOUND_OFFSET, Integer.class)).isEqualTo(20);

    assertThatThrownBy(() -> assertThat(fieldStats.get(10, Long.class)))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Unknown field ordinal: 10");
    assertThatThrownBy(() -> assertThat(fieldStats.get(StatsUtil.VALUE_COUNT_OFFSET, Double.class)))
        .isInstanceOf(ClassCastException.class)
        .hasMessage("Cannot cast java.lang.Long to java.lang.Double");
    assertThatThrownBy(
            () -> assertThat(fieldStats.get(StatsUtil.AVG_VALUE_SIZE_OFFSET, Long.class)))
        .isInstanceOf(ClassCastException.class)
        .hasMessage("Cannot cast java.lang.Integer to java.lang.Long");
  }
}
