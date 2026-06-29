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
package org.apache.iceberg;

import static org.apache.iceberg.FieldStatistic.AVG_VALUE_SIZE_IN_BYTES;
import static org.apache.iceberg.FieldStatistic.LOWER_BOUND;
import static org.apache.iceberg.FieldStatistic.NAN_VALUE_COUNT;
import static org.apache.iceberg.FieldStatistic.NULL_VALUE_COUNT;
import static org.apache.iceberg.FieldStatistic.TIGHT_BOUNDS;
import static org.apache.iceberg.FieldStatistic.UPPER_BOUND;
import static org.apache.iceberg.FieldStatistic.VALUE_COUNT;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.Variants;
import org.junit.jupiter.api.Test;

public class TestContentStats {

  @Test
  public void contentStatsWithoutStatsStruct() {
    assertThatThrownBy(() -> BaseContentStats.builder().build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Either stats struct or table schema must be set");

    assertThatThrownBy(
            () ->
                BaseContentStats.builder()
                    .withTableSchema(new Schema())
                    .withStatsStruct(new Schema().asStruct())
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot set stats struct and table schema");
  }

  @Test
  public void emptyContentStats() {
    BaseContentStats stats = BaseContentStats.builder().withTableSchema(new Schema()).build();
    assertThat(stats).isNotNull();
    assertThat(stats.fieldStats()).isEmpty();
  }

  @Test
  public void validContentStats() {
    BaseFieldStats<?> fieldStatsOne = BaseFieldStats.builder().fieldId(1).build();
    BaseFieldStats<?> fieldStatsTwo = BaseFieldStats.builder().fieldId(2).build();
    BaseContentStats stats =
        BaseContentStats.builder()
            .withTableSchema(
                new Schema(
                    optional(1, "id", Types.IntegerType.get()),
                    optional(2, "id2", Types.IntegerType.get())))
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
                    .withTableSchema(
                        new Schema(
                            optional(1, "id", Types.IntegerType.get()),
                            optional(2, "id2", Types.IntegerType.get()),
                            optional(3, "id3", Types.IntegerType.get())))
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
            .withTableSchema(
                new Schema(
                    optional(1, "id", Types.IntegerType.get()),
                    optional(2, "id2", Types.IntegerType.get()),
                    optional(3, "id3", Types.IntegerType.get())))
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
            .withTableSchema(
                new Schema(
                    optional(1, "id", Types.IntegerType.get()),
                    optional(2, "id2", Types.IntegerType.get())))
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
            "Wrong class, expected java.lang.Long but was org.apache.iceberg.BaseFieldStats for object:");
  }

  @Test
  public void retrievalByFieldId() {
    Schema schema =
        new Schema(
            required(1, "id", Types.IntegerType.get()),
            required(2, "id2", Types.StringType.get()),
            required(3, "id3", Types.DoubleType.get()),
            required(4, "id4", Types.LongType.get()),
            required(5, "id5", Types.FloatType.get()));

    BaseFieldStats<Object> fieldStatsTwo =
        BaseFieldStats.builder()
            .fieldId(2)
            .type(Types.StringType.get())
            .lowerBound("aaa")
            .upperBound("zzz")
            .build();
    BaseFieldStats<Object> fieldStatsFive =
        BaseFieldStats.builder()
            .fieldId(5)
            .type(Types.FloatType.get())
            .lowerBound(1.0f)
            .upperBound(5.0f)
            .build();

    // table schema has 5 columns, but we only have stats for field IDs 2 and 5 and hold the stats
    // in an inverse order
    BaseContentStats stats =
        BaseContentStats.builder()
            .withTableSchema(schema)
            .withFieldStats(fieldStatsFive)
            .withFieldStats(fieldStatsTwo)
            .build();

    assertThat(stats.statsFor(1)).isNull();
    assertThat(stats.statsFor(2)).isEqualTo(fieldStatsTwo);
    assertThat(stats.statsFor(3)).isNull();
    assertThat(stats.statsFor(4)).isNull();
    assertThat(stats.statsFor(5)).isEqualTo(fieldStatsFive);
    assertThat(stats.statsFor(100)).isNull();
  }

  @Test
  public void retrievalByPositionWithPartialStats() {
    Schema schema =
        new Schema(
            required(1, "id", Types.IntegerType.get()),
            required(2, "id2", Types.StringType.get()),
            required(3, "id3", Types.DoubleType.get()),
            required(4, "id4", Types.LongType.get()),
            required(5, "id5", Types.FloatType.get()));

    BaseFieldStats<Object> fieldStatsTwo =
        BaseFieldStats.builder()
            .fieldId(2)
            .type(Types.StringType.get())
            .lowerBound("aaa")
            .upperBound("zzz")
            .build();
    BaseFieldStats<Object> fieldStatsFive =
        BaseFieldStats.builder()
            .fieldId(5)
            .type(Types.FloatType.get())
            .lowerBound(1.0f)
            .upperBound(5.0f)
            .build();

    // table schema has 5 columns, but we only have stats for field IDs 2 and 5 and hold the stats
    // in an inverse order
    BaseContentStats stats =
        BaseContentStats.builder()
            .withTableSchema(schema)
            .withFieldStats(fieldStatsFive)
            .withFieldStats(fieldStatsTwo)
            .build();

    assertThat(stats.get(0, FieldStats.class)).isNull();
    assertThat(stats.get(1, FieldStats.class)).isEqualTo(fieldStatsTwo);
    assertThat(stats.get(2, FieldStats.class)).isNull();
    assertThat(stats.get(3, FieldStats.class)).isNull();
    assertThat(stats.get(4, FieldStats.class)).isEqualTo(fieldStatsFive);
  }

  @Test
  public void setByPositionOptionalString() {
    Schema tableSchema = new Schema(optional(1, "s", Types.StringType.get()));
    Types.StructType rootStatsStruct = StatsUtil.contentStatsFor(tableSchema).type().asStructType();
    Types.StructType statsStructForFieldId = rootStatsStruct.fields().get(0).type().asStructType();
    assertThat(statsStructForFieldId.fields()).hasSize(6);

    GenericRecord record = GenericRecord.create(statsStructForFieldId);
    BaseFieldStats<String> fieldStats =
        BaseFieldStats.<String>builder()
            .type(Types.StringType.get())
            .fieldId(1)
            .valueCount(10L)
            .nullValueCount(2L)
            .avgValueSizeInBytes(3)
            .lowerBound("aa")
            .upperBound("zzz")
            .tightBounds()
            .build();

    record.setField(LOWER_BOUND.fieldName(), fieldStats.lowerBound());
    record.setField(UPPER_BOUND.fieldName(), fieldStats.upperBound());
    record.setField(TIGHT_BOUNDS.fieldName(), fieldStats.tightBounds());
    record.setField(VALUE_COUNT.fieldName(), fieldStats.valueCount());
    record.setField(NULL_VALUE_COUNT.fieldName(), fieldStats.nullValueCount());
    record.setField(AVG_VALUE_SIZE_IN_BYTES.fieldName(), fieldStats.avgValueSizeInBytes());

    BaseContentStats stats = new BaseContentStats(rootStatsStruct);
    stats.set(0, record);
    assertThat(stats.fieldStats()).containsExactly(fieldStats);
  }

  @Test
  public void setByPositionOptionalDouble() {
    Schema tableSchema = new Schema(optional(1, "d", Types.DoubleType.get()));
    Types.StructType rootStatsStruct = StatsUtil.contentStatsFor(tableSchema).type().asStructType();
    Types.StructType statsStructForFieldId = rootStatsStruct.fields().get(0).type().asStructType();
    assertThat(statsStructForFieldId.fields()).hasSize(6);

    GenericRecord record = GenericRecord.create(statsStructForFieldId);
    BaseFieldStats<Double> fieldStats =
        BaseFieldStats.<Double>builder()
            .type(Types.DoubleType.get())
            .fieldId(1)
            .valueCount(10L)
            .nullValueCount(2L)
            .nanValueCount(3L)
            .lowerBound(5.0)
            .upperBound(20.0)
            .tightBounds()
            .build();

    record.setField(LOWER_BOUND.fieldName(), fieldStats.lowerBound());
    record.setField(UPPER_BOUND.fieldName(), fieldStats.upperBound());
    record.setField(TIGHT_BOUNDS.fieldName(), fieldStats.tightBounds());
    record.setField(VALUE_COUNT.fieldName(), fieldStats.valueCount());
    record.setField(NULL_VALUE_COUNT.fieldName(), fieldStats.nullValueCount());
    record.setField(NAN_VALUE_COUNT.fieldName(), fieldStats.nanValueCount());

    BaseContentStats stats = new BaseContentStats(rootStatsStruct);
    stats.set(0, record);
    assertThat(stats.fieldStats()).containsExactly(fieldStats);
  }

  @Test
  public void setByPositionRequiredInteger() {
    Schema tableSchema = new Schema(required(1, "id", Types.IntegerType.get()));
    Types.StructType rootStatsStruct = StatsUtil.contentStatsFor(tableSchema).type().asStructType();
    Types.StructType statsStructForFieldId = rootStatsStruct.fields().get(0).type().asStructType();
    assertThat(statsStructForFieldId.fields()).hasSize(4);

    GenericRecord record = GenericRecord.create(statsStructForFieldId);
    BaseFieldStats<Integer> fieldStats =
        BaseFieldStats.<Integer>builder()
            .type(Types.IntegerType.get())
            .fieldId(1)
            .valueCount(10L)
            .lowerBound(5)
            .upperBound(20)
            .tightBounds()
            .build();

    record.setField(LOWER_BOUND.fieldName(), fieldStats.lowerBound());
    record.setField(UPPER_BOUND.fieldName(), fieldStats.upperBound());
    record.setField(TIGHT_BOUNDS.fieldName(), fieldStats.tightBounds());
    record.setField(VALUE_COUNT.fieldName(), fieldStats.valueCount());

    // this is typically called by Avro reflection code
    BaseContentStats stats = new BaseContentStats(rootStatsStruct);
    stats.set(0, record);
    assertThat(stats.fieldStats()).containsExactly(fieldStats);
  }

  @Test
  public void setByPositionOptionalGeometry() {
    Schema tableSchema = new Schema(optional(1, "g", Types.GeometryType.crs84()));
    Types.StructType rootStatsStruct = StatsUtil.contentStatsFor(tableSchema).type().asStructType();
    Types.StructType statsStructForFieldId = rootStatsStruct.fields().get(0).type().asStructType();
    // lower_bound, upper_bound, value_count, null_value_count
    assertThat(statsStructForFieldId.fields()).hasSize(4);

    GenericRecord lower =
        GenericRecord.create(
            statsStructForFieldId.field(LOWER_BOUND.fieldName()).type().asStructType());
    lower.setField("x", -122.4);
    lower.setField("y", 37.7);
    lower.setField("z", null);
    lower.setField("m", null);

    GenericRecord upper =
        GenericRecord.create(
            statsStructForFieldId.field(UPPER_BOUND.fieldName()).type().asStructType());
    upper.setField("x", -122.0);
    upper.setField("y", 38.0);
    upper.setField("z", null);
    upper.setField("m", null);

    GenericRecord record = GenericRecord.create(statsStructForFieldId);
    record.setField(LOWER_BOUND.fieldName(), lower);
    record.setField(UPPER_BOUND.fieldName(), upper);
    record.setField(VALUE_COUNT.fieldName(), 100L);
    record.setField(NULL_VALUE_COUNT.fieldName(), 5L);

    ContentStats stats = new BaseContentStats(rootStatsStruct);
    stats.set(0, record);

    FieldStats<?> result = stats.fieldStats().get(0);
    assertThat(result.valueCount()).isEqualTo(100L);
    assertThat(result.nullValueCount()).isEqualTo(5L);
    assertThat(result.tightBounds()).isFalse();
    assertThat(result.lowerBound()).isEqualTo(lower);
    assertThat(result.upperBound()).isEqualTo(upper);
  }

  @Test
  public void setByPositionOptionalGeography() {
    Schema tableSchema = new Schema(optional(1, "g", Types.GeographyType.crs84()));
    Types.StructType rootStatsStruct = StatsUtil.contentStatsFor(tableSchema).type().asStructType();
    Types.StructType statsStructForFieldId = rootStatsStruct.fields().get(0).type().asStructType();
    // lower_bound, upper_bound, value_count, null_value_count
    assertThat(statsStructForFieldId.fields()).hasSize(4);

    GenericRecord lower =
        GenericRecord.create(
            statsStructForFieldId.field(LOWER_BOUND.fieldName()).type().asStructType());
    lower.setField("x", 10.0);
    lower.setField("y", 20.0);
    lower.setField("z", 0.0);
    lower.setField("m", null);

    GenericRecord upper =
        GenericRecord.create(
            statsStructForFieldId.field(UPPER_BOUND.fieldName()).type().asStructType());
    upper.setField("x", 30.0);
    upper.setField("y", 40.0);
    upper.setField("z", 100.0);
    upper.setField("m", null);

    GenericRecord record = GenericRecord.create(statsStructForFieldId);
    record.setField(LOWER_BOUND.fieldName(), lower);
    record.setField(UPPER_BOUND.fieldName(), upper);
    record.setField(VALUE_COUNT.fieldName(), 200L);
    record.setField(NULL_VALUE_COUNT.fieldName(), 10L);

    ContentStats stats = new BaseContentStats(rootStatsStruct);
    stats.set(0, record);

    FieldStats<?> result = stats.fieldStats().get(0);
    assertThat(result.valueCount()).isEqualTo(200L);
    assertThat(result.nullValueCount()).isEqualTo(10L);
    assertThat(result.tightBounds()).isFalse();
    assertThat(result.lowerBound()).isEqualTo(lower);
    assertThat(result.upperBound()).isEqualTo(upper);
  }

  @Test
  public void setByPositionRequiredVariant() {
    Schema tableSchema = new Schema(required(1, "v", Types.VariantType.get()));
    Types.StructType rootStatsStruct = StatsUtil.contentStatsFor(tableSchema).type().asStructType();
    Types.StructType statsStructForFieldId = rootStatsStruct.fields().get(0).type().asStructType();
    // lower_bound, upper_bound, value_count, avg_value_size_in_bytes
    assertThat(statsStructForFieldId.fields()).hasSize(4);

    VariantMetadata metadata = Variants.metadata("$['name']", "$['score']");
    ShreddedObject lower = Variants.object(metadata);
    lower.put("$['name']", Variants.of("alice"));
    lower.put("$['score']", Variants.of(1));
    Variant lowerVariant = Variant.of(metadata, lower);

    ShreddedObject upper = Variants.object(metadata);
    upper.put("$['name']", Variants.of("zara"));
    upper.put("$['score']", Variants.of(100));
    Variant upperVariant = Variant.of(metadata, upper);

    GenericRecord record = GenericRecord.create(statsStructForFieldId);
    record.setField(LOWER_BOUND.fieldName(), lowerVariant);
    record.setField(UPPER_BOUND.fieldName(), upperVariant);
    record.setField(VALUE_COUNT.fieldName(), 50L);
    record.setField(AVG_VALUE_SIZE_IN_BYTES.fieldName(), 128);

    ContentStats stats = new BaseContentStats(rootStatsStruct);
    stats.set(0, record);

    FieldStats<?> result = stats.fieldStats().get(0);
    assertThat(result.valueCount()).isEqualTo(50L);
    assertThat(result.avgValueSizeInBytes()).isEqualTo(128);
    assertThat(result.tightBounds()).isFalse();
    assertThat(result.lowerBound()).isEqualTo(lowerVariant);
    assertThat(result.upperBound()).isEqualTo(upperVariant);
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
    record.setField(LOWER_BOUND.fieldName(), 5.0);
    assertThatThrownBy(() -> stats.set(0, record))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid lower bound type, expected a subtype of class java.lang.Integer: java.lang.Double");

    // set valid lower bound so that upper bound is evaluated
    record.setField(LOWER_BOUND.fieldName(), 5);

    // invalid upper bound
    record.setField(UPPER_BOUND.fieldName(), "20");
    assertThatThrownBy(() -> stats.set(0, record))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid upper bound type, expected a subtype of class java.lang.Integer: java.lang.String");
  }
}
