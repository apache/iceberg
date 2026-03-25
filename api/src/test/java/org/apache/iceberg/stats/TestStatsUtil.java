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
import static org.apache.iceberg.stats.FieldStatistic.EXACT_BOUNDS;
import static org.apache.iceberg.stats.FieldStatistic.LOWER_BOUND;
import static org.apache.iceberg.stats.FieldStatistic.MAX_VALUE_SIZE;
import static org.apache.iceberg.stats.FieldStatistic.NAN_VALUE_COUNT;
import static org.apache.iceberg.stats.FieldStatistic.NULL_VALUE_COUNT;
import static org.apache.iceberg.stats.FieldStatistic.UPPER_BOUND;
import static org.apache.iceberg.stats.FieldStatistic.VALUE_COUNT;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestStatsUtil {

  @Test
  public void statsIdsForTableColumns() {
    int offset = 0;
    for (int id = 0; id < StatsUtil.MAX_DATA_FIELD_ID; id++) {
      int statsFieldId = StatsUtil.statsFieldIdForField(id);
      int expected = StatsUtil.STATS_SPACE_FIELD_ID_START_FOR_DATA_FIELDS + offset;
      assertThat(statsFieldId).as("at pos %s", id).isEqualTo(expected);
      offset += StatsUtil.NUM_SUPPORTED_STATS_PER_COLUMN;
      assertThat(StatsUtil.fieldIdForStatsField(statsFieldId)).as("at pos %s", id).isEqualTo(id);
    }

    // also verify hardcoded field IDs from docs
    int fieldId = 0;
    int statsFieldId = 10_000;
    assertThat(StatsUtil.statsFieldIdForField(fieldId)).isEqualTo(statsFieldId);
    assertThat(StatsUtil.fieldIdForStatsField(statsFieldId)).isEqualTo(fieldId);

    fieldId = 1;
    statsFieldId = 10_200;
    assertThat(StatsUtil.statsFieldIdForField(fieldId)).isEqualTo(statsFieldId);
    assertThat(StatsUtil.fieldIdForStatsField(statsFieldId)).isEqualTo(fieldId);

    fieldId = 2;
    statsFieldId = 10_400;
    assertThat(StatsUtil.statsFieldIdForField(fieldId)).isEqualTo(statsFieldId);
    assertThat(StatsUtil.fieldIdForStatsField(statsFieldId)).isEqualTo(fieldId);

    fieldId = 5;
    statsFieldId = 11_000;
    assertThat(StatsUtil.statsFieldIdForField(fieldId)).isEqualTo(statsFieldId);
    assertThat(StatsUtil.fieldIdForStatsField(statsFieldId)).isEqualTo(fieldId);

    fieldId = 100;
    statsFieldId = 30_000;
    assertThat(StatsUtil.statsFieldIdForField(fieldId)).isEqualTo(statsFieldId);
    assertThat(StatsUtil.fieldIdForStatsField(statsFieldId)).isEqualTo(fieldId);

    fieldId = StatsUtil.MAX_DATA_FIELD_ID;
    statsFieldId = StatsUtil.MAX_DATA_STATS_FIELD_ID;
    assertThat(StatsUtil.statsFieldIdForField(fieldId)).isEqualTo(statsFieldId);
    assertThat(StatsUtil.fieldIdForStatsField(statsFieldId)).isEqualTo(fieldId);

    fieldId = -1;
    statsFieldId = -1;
    assertThat(StatsUtil.statsFieldIdForField(fieldId)).isEqualTo(statsFieldId);
    assertThat(StatsUtil.fieldIdForStatsField(statsFieldId)).isEqualTo(fieldId);
  }

  @Test
  public void statsIdsOverflowForTableColumns() {
    // pick 100 random IDs that are > MAX_FIELD_ID and < METADATA_SPACE_FIELD_ID_START as going over
    // the entire ID range takes too long
    int invalidFieldId = -1;
    for (int i = 0; i < 100; i++) {
      int id =
          ThreadLocalRandom.current()
              .nextInt(
                  StatsUtil.MAX_DATA_FIELD_ID + 1,
                  StatsUtil.STATS_SPACE_FIELD_ID_START_FOR_METADATA_FIELDS);
      assertThat(StatsUtil.statsFieldIdForField(id)).as("at pos %s", id).isEqualTo(invalidFieldId);
    }

    assertThat(StatsUtil.fieldIdForStatsField(-1)).isEqualTo(invalidFieldId);
    assertThat(StatsUtil.fieldIdForStatsField(5_000)).isEqualTo(invalidFieldId);
    assertThat(StatsUtil.fieldIdForStatsField(10_001)).isEqualTo(invalidFieldId);
    assertThat(StatsUtil.fieldIdForStatsField(10_201)).isEqualTo(invalidFieldId);
    assertThat(StatsUtil.fieldIdForStatsField(10_500)).isEqualTo(invalidFieldId);
    assertThat(StatsUtil.fieldIdForStatsField(10_900)).isEqualTo(invalidFieldId);
  }

  @Test
  public void statsIdsForReservedColumns() {
    int offset = 0;
    for (int id = StatsUtil.RESERVED_FIELD_IDS_START; id < Integer.MAX_VALUE; id++) {
      int statsFieldId = StatsUtil.statsFieldIdForField(id);
      int expected = StatsUtil.STATS_SPACE_FIELD_ID_START_FOR_METADATA_FIELDS + offset;
      assertThat(statsFieldId).as("at pos %s", id).isEqualTo(expected);
      offset = offset + StatsUtil.NUM_SUPPORTED_STATS_PER_COLUMN;
      assertThat(StatsUtil.fieldIdForStatsField(statsFieldId)).as("at pos %s", id).isEqualTo(id);
    }

    // also verify hardcoded IDs that are mentioned in the docs
    int fieldId = 2_147_483_447;
    int statsFieldId = 2_147_000_000;
    assertThat(StatsUtil.statsFieldIdForField(fieldId)).isEqualTo(statsFieldId);
    assertThat(StatsUtil.fieldIdForStatsField(statsFieldId)).isEqualTo(fieldId);

    fieldId = 2_147_483_448;
    statsFieldId = 2_147_000_200;
    assertThat(StatsUtil.statsFieldIdForField(fieldId)).isEqualTo(statsFieldId);
    assertThat(StatsUtil.fieldIdForStatsField(statsFieldId)).isEqualTo(fieldId);

    fieldId = 2_147_483_541;
    statsFieldId = 2_147_018_800;
    assertThat(StatsUtil.statsFieldIdForField(fieldId)).isEqualTo(statsFieldId);
    assertThat(StatsUtil.fieldIdForStatsField(statsFieldId)).isEqualTo(fieldId);

    fieldId = 2_147_483_645;
    statsFieldId = 2_147_039_600;
    assertThat(StatsUtil.statsFieldIdForField(fieldId)).isEqualTo(statsFieldId);
    assertThat(StatsUtil.fieldIdForStatsField(statsFieldId)).isEqualTo(fieldId);

    fieldId = 2_147_483_646;
    statsFieldId = 2_147_039_800;
    assertThat(StatsUtil.statsFieldIdForField(fieldId)).isEqualTo(statsFieldId);
    assertThat(StatsUtil.fieldIdForStatsField(statsFieldId)).isEqualTo(fieldId);
  }

  @Test
  public void contentStatsForSimpleSchema() {
    Types.NestedField intField = required(0, "i", Types.IntegerType.get());
    Types.NestedField floatField = required(2, "f", Types.FloatType.get());
    Types.NestedField stringField = required(4, "s", Types.StringType.get());
    Types.NestedField booleanField = required(6, "b", Types.BooleanType.get());
    Types.NestedField uuidField = required(1_000_000, "u", Types.UUIDType.get());
    Schema schema = new Schema(intField, floatField, stringField, booleanField, uuidField);
    Schema expectedStatsSchema =
        new Schema(
            optional(
                146,
                "content_stats",
                Types.StructType.of(
                    optional(10000, "0", FieldStatistic.fieldStatsFor(intField, 10000)),
                    optional(10400, "2", FieldStatistic.fieldStatsFor(floatField, 10400)),
                    optional(10800, "4", FieldStatistic.fieldStatsFor(stringField, 10800)),
                    optional(11200, "6", FieldStatistic.fieldStatsFor(booleanField, 11200)),
                    optional(
                        200010000,
                        "1000000",
                        FieldStatistic.fieldStatsFor(uuidField, 200010000)))));
    Schema statsSchema = new Schema(StatsUtil.contentStatsFor(schema));
    assertThat(statsSchema.asStruct()).isEqualTo(expectedStatsSchema.asStruct());
  }

  @Test
  public void contentStatsForComplexSchema() {
    Types.NestedField listElement = optional(3, "element", Types.IntegerType.get());
    Types.NestedField structInt = optional(7, "int", Types.IntegerType.get());
    Types.NestedField structString = optional(8, "string", Types.StringType.get());
    Types.NestedField mapKey = required(22, "key", Types.IntegerType.get());
    Types.NestedField mapValue = optional(24, "value", Types.StringType.get());
    Types.NestedField uuidField = required(100_000, "u", Types.UUIDType.get());
    Schema schema =
        new Schema(
            required(0, "i", Types.IntegerType.get()),
            required(2, "list", Types.ListType.ofOptional(3, Types.IntegerType.get())),
            required(
                6,
                "simple_struct",
                Types.StructType.of(
                    optional(7, "int", Types.IntegerType.get()),
                    optional(8, "string", Types.StringType.get()))),
            required(
                20,
                "b",
                Types.MapType.ofOptional(22, 24, Types.IntegerType.get(), Types.StringType.get())),
            required(30, "variant", Types.VariantType.get()),
            uuidField);
    Schema expectedStatsSchema =
        new Schema(
            optional(
                146,
                "content_stats",
                Types.StructType.of(
                    optional(
                        10000,
                        "0",
                        FieldStatistic.fieldStatsFor(
                            required(0, "i", Types.IntegerType.get()), 10000)),
                    optional(10600, "3", FieldStatistic.fieldStatsFor(listElement, 10600)),
                    optional(11400, "7", FieldStatistic.fieldStatsFor(structInt, 11400)),
                    optional(11600, "8", FieldStatistic.fieldStatsFor(structString, 11600)),
                    optional(14400, "22", FieldStatistic.fieldStatsFor(mapKey, 14400)),
                    optional(14800, "24", FieldStatistic.fieldStatsFor(mapValue, 14800)),
                    optional(
                        20010000, "100000", FieldStatistic.fieldStatsFor(uuidField, 20010000)))));
    Schema statsSchema = new Schema(StatsUtil.contentStatsFor(schema));
    assertThat(statsSchema.asStruct()).isEqualTo(expectedStatsSchema.asStruct());
  }

  @Test
  public void conditionalFieldInclusionForInteger() {
    assertThat(
            fieldStatsNames(
                FieldStatistic.fieldStatsFor(required(1, "x", Types.IntegerType.get()), 10000)))
        .containsExactly(
            VALUE_COUNT.fieldName(),
            LOWER_BOUND.fieldName(),
            UPPER_BOUND.fieldName(),
            EXACT_BOUNDS.fieldName())
        .doesNotContain(
            NULL_VALUE_COUNT.fieldName(),
            NAN_VALUE_COUNT.fieldName(),
            AVG_VALUE_SIZE.fieldName(),
            MAX_VALUE_SIZE.fieldName());

    assertThat(
            fieldStatsNames(
                FieldStatistic.fieldStatsFor(optional(1, "x", Types.IntegerType.get()), 10000)))
        .containsExactly(
            VALUE_COUNT.fieldName(),
            NULL_VALUE_COUNT.fieldName(),
            LOWER_BOUND.fieldName(),
            UPPER_BOUND.fieldName(),
            EXACT_BOUNDS.fieldName())
        .doesNotContain(
            NAN_VALUE_COUNT.fieldName(), AVG_VALUE_SIZE.fieldName(), MAX_VALUE_SIZE.fieldName());
  }

  @Test
  public void conditionalFieldInclusionForFloatAndDouble() {
    assertThat(
            fieldStatsNames(
                FieldStatistic.fieldStatsFor(required(1, "x", Types.FloatType.get()), 10000)))
        .containsExactly(
            VALUE_COUNT.fieldName(),
            NAN_VALUE_COUNT.fieldName(),
            LOWER_BOUND.fieldName(),
            UPPER_BOUND.fieldName(),
            EXACT_BOUNDS.fieldName())
        .doesNotContain(
            NULL_VALUE_COUNT.fieldName(), AVG_VALUE_SIZE.fieldName(), MAX_VALUE_SIZE.fieldName());

    assertThat(
            fieldStatsNames(
                FieldStatistic.fieldStatsFor(optional(1, "x", Types.DoubleType.get()), 10000)))
        .containsExactly(
            VALUE_COUNT.fieldName(),
            NULL_VALUE_COUNT.fieldName(),
            NAN_VALUE_COUNT.fieldName(),
            LOWER_BOUND.fieldName(),
            UPPER_BOUND.fieldName(),
            EXACT_BOUNDS.fieldName());
  }

  @Test
  public void conditionalFieldInclusionForString() {
    assertThat(
            fieldStatsNames(
                FieldStatistic.fieldStatsFor(required(1, "x", Types.StringType.get()), 10000)))
        .containsExactly(
            VALUE_COUNT.fieldName(),
            AVG_VALUE_SIZE.fieldName(),
            MAX_VALUE_SIZE.fieldName(),
            LOWER_BOUND.fieldName(),
            UPPER_BOUND.fieldName(),
            EXACT_BOUNDS.fieldName())
        .doesNotContain(NULL_VALUE_COUNT.fieldName(), NAN_VALUE_COUNT.fieldName());

    assertThat(
            fieldStatsNames(
                FieldStatistic.fieldStatsFor(optional(1, "x", Types.StringType.get()), 10000)))
        .containsExactly(
            VALUE_COUNT.fieldName(),
            NULL_VALUE_COUNT.fieldName(),
            AVG_VALUE_SIZE.fieldName(),
            MAX_VALUE_SIZE.fieldName(),
            LOWER_BOUND.fieldName(),
            UPPER_BOUND.fieldName(),
            EXACT_BOUNDS.fieldName());
  }

  @Test
  public void conditionalFieldInclusionForBinary() {
    assertThat(
            fieldStatsNames(
                FieldStatistic.fieldStatsFor(optional(1, "x", Types.BinaryType.get()), 10000)))
        .containsExactly(
            VALUE_COUNT.fieldName(),
            NULL_VALUE_COUNT.fieldName(),
            AVG_VALUE_SIZE.fieldName(),
            MAX_VALUE_SIZE.fieldName(),
            LOWER_BOUND.fieldName(),
            UPPER_BOUND.fieldName(),
            EXACT_BOUNDS.fieldName())
        .doesNotContain(NAN_VALUE_COUNT.fieldName());

    assertThat(
            fieldStatsNames(
                FieldStatistic.fieldStatsFor(required(1, "x", Types.BinaryType.get()), 10000)))
        .containsExactly(
            VALUE_COUNT.fieldName(),
            AVG_VALUE_SIZE.fieldName(),
            MAX_VALUE_SIZE.fieldName(),
            LOWER_BOUND.fieldName(),
            UPPER_BOUND.fieldName(),
            EXACT_BOUNDS.fieldName())
        .doesNotContain(NULL_VALUE_COUNT.fieldName(), NAN_VALUE_COUNT.fieldName());
  }

  private List<String> fieldStatsNames(Types.StructType structType) {
    return structType.fields().stream().map(Types.NestedField::name).toList();
  }
}
