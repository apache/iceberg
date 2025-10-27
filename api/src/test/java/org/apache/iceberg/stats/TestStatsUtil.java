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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.ThreadLocalRandom;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestStatsUtil {

  @Test
  public void statsIdsForTableColumns() {
    int offset = 0;
    for (int id = 0; id < StatsUtil.MAX_DATA_FIELD_ID; id++) {
      int statsFieldId = StatsUtil.statsFieldIdForField(id);
      int expected = StatsUtil.DATA_SPACE_FIELD_ID_START + offset;
      assertThat(statsFieldId).as("at pos %s", id).isEqualTo(expected);
      offset += StatsUtil.NUM_STATS_PER_COLUMN;
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
              .nextInt(StatsUtil.MAX_DATA_FIELD_ID + 1, StatsUtil.METADATA_SPACE_FIELD_ID_START);
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
      int expected = StatsUtil.METADATA_SPACE_FIELD_ID_START + offset;
      assertThat(statsFieldId).as("at pos %s", id).isEqualTo(expected);
      offset = offset + StatsUtil.NUM_STATS_PER_COLUMN;
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
    Schema schema =
        new Schema(
            required(0, "i", Types.IntegerType.get()),
            required(2, "f", Types.FloatType.get()),
            required(4, "s", Types.StringType.get()),
            required(6, "b", Types.BooleanType.get()),
            required(1_000_000, "u", Types.UUIDType.get()));
    Schema expectedStatsSchema =
        new Schema(
            optional(
                146,
                "content_stats",
                Types.StructType.of(
                    optional(10000, "0", fieldStatsFor(Types.IntegerType.get(), 10001)),
                    optional(10400, "2", fieldStatsFor(Types.FloatType.get(), 10401)),
                    optional(10800, "4", fieldStatsFor(Types.StringType.get(), 10801)),
                    optional(11200, "6", fieldStatsFor(Types.BooleanType.get(), 11201)),
                    optional(
                        200010000, "1000000", fieldStatsFor(Types.UUIDType.get(), 200010001)))));
    Schema statsSchema = new Schema(StatsUtil.contentStatsFor(schema));
    assertThat(statsSchema.asStruct()).isEqualTo(expectedStatsSchema.asStruct());
  }

  @Test
  public void contentStatsForComplexSchema() {
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
            required(100_000, "u", Types.UUIDType.get()));
    Schema expectedStatsSchema =
        new Schema(
            optional(
                146,
                "content_stats",
                Types.StructType.of(
                    optional(10000, "0", fieldStatsFor(Types.IntegerType.get(), 10001)),
                    optional(10600, "3", fieldStatsFor(Types.IntegerType.get(), 10601)),
                    optional(11400, "7", fieldStatsFor(Types.IntegerType.get(), 11401)),
                    optional(11600, "8", fieldStatsFor(Types.StringType.get(), 11601)),
                    optional(14400, "22", fieldStatsFor(Types.IntegerType.get(), 14401)),
                    optional(14800, "24", fieldStatsFor(Types.StringType.get(), 14801)),
                    optional(20010000, "100000", fieldStatsFor(Types.UUIDType.get(), 20010001)))));
    Schema statsSchema = new Schema(StatsUtil.contentStatsFor(schema));
    assertThat(statsSchema.asStruct()).isEqualTo(expectedStatsSchema.asStruct());
  }

  private Type fieldStatsFor(Type type, int id) {
    int fieldId = id;
    return Types.StructType.of(
        optional(
            fieldId++,
            "value_count",
            Types.LongType.get(),
            "Total value count, including null and NaN"),
        optional(fieldId++, "null_value_count", Types.LongType.get(), "Total null value count"),
        optional(fieldId++, "nan_value_count", Types.LongType.get(), "Total NaN value count"),
        optional(
            fieldId++,
            "avg_value_size",
            Types.IntegerType.get(),
            "Avg value size of variable-length types (String, Binary)"),
        optional(
            fieldId++,
            "max_value_size",
            Types.IntegerType.get(),
            "Max value size of variable-length types (String, Binary)"),
        optional(fieldId++, "lower_bound", type, "Lower bound"),
        optional(fieldId++, "upper_bound", type, "Upper bound"),
        optional(
            fieldId, "is_exact", Types.BooleanType.get(), "Whether the statistic is exact or not"));
  }
}
