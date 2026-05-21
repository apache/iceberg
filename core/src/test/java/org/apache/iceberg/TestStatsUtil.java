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

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestStatsUtil {
  // the reserved field IDs from the reserved field ID space as defined in
  // https://iceberg.apache.org/spec/#reserved-field-ids
  private static final int RESERVED_FIELD_IDS_START = Integer.MAX_VALUE - 200;

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
    // pick 100 random IDs that are > MAX_FIELD_ID and < RESERVED_FIELD_IDS_START as going over
    // the entire ID range takes too long
    int invalidFieldId = -1;
    for (int i = 0; i < 100; i++) {
      int id =
          ThreadLocalRandom.current()
              .nextInt(StatsUtil.MAX_DATA_FIELD_ID + 1, RESERVED_FIELD_IDS_START);
      assertThat(StatsUtil.statsFieldIdForField(id)).as("at pos %s", id).isEqualTo(invalidFieldId);
    }

    assertThat(StatsUtil.fieldIdForStatsField(-1)).isEqualTo(invalidFieldId);
    assertThat(StatsUtil.fieldIdForStatsField(0)).isEqualTo(invalidFieldId);
    assertThat(StatsUtil.fieldIdForStatsField(200)).isEqualTo(invalidFieldId);
    assertThat(StatsUtil.fieldIdForStatsField(5_000)).isEqualTo(invalidFieldId);
    assertThat(StatsUtil.fieldIdForStatsField(8_600)).isEqualTo(invalidFieldId);
    assertThat(StatsUtil.fieldIdForStatsField(10_001)).isEqualTo(invalidFieldId);
    assertThat(StatsUtil.fieldIdForStatsField(10_201)).isEqualTo(invalidFieldId);
    assertThat(StatsUtil.fieldIdForStatsField(10_500)).isEqualTo(invalidFieldId);
    assertThat(StatsUtil.fieldIdForStatsField(10_900)).isEqualTo(invalidFieldId);

    // stats field IDs at or above the exclusive upper bound are invalid
    assertThat(StatsUtil.fieldIdForStatsField(StatsUtil.STATS_SPACE_FIELD_ID_END))
        .isEqualTo(invalidFieldId);
    assertThat(StatsUtil.fieldIdForStatsField(StatsUtil.STATS_SPACE_FIELD_ID_END + 200))
        .isEqualTo(invalidFieldId);
    assertThat(StatsUtil.fieldIdForStatsField(Integer.MAX_VALUE)).isEqualTo(invalidFieldId);

    // field ID just past MAX_DATA_FIELD_ID is invalid
    assertThat(StatsUtil.statsFieldIdForField(StatsUtil.MAX_DATA_FIELD_ID + 1))
        .isEqualTo(invalidFieldId);
  }

  @Test
  public void statsIdsForMetadataColumns() {
    int fieldId = MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId();
    int statsFieldId = 9_000;
    assertThat(StatsUtil.statsFieldIdForField(fieldId)).isEqualTo(statsFieldId);
    assertThat(StatsUtil.fieldIdForStatsField(statsFieldId)).isEqualTo(fieldId);

    fieldId = MetadataColumns.ROW_ID.fieldId();
    statsFieldId = 9_200;
    assertThat(StatsUtil.statsFieldIdForField(fieldId)).isEqualTo(statsFieldId);
    assertThat(StatsUtil.fieldIdForStatsField(statsFieldId)).isEqualTo(fieldId);

    // reserved metadata fields with IDs below/above the reserved stats range have no stats
    int invalidFieldId = -1;
    assertThat(StatsUtil.fieldIdForStatsField(8_800)).isEqualTo(invalidFieldId);
    assertThat(StatsUtil.fieldIdForStatsField(9_400)).isEqualTo(invalidFieldId);
    assertThat(StatsUtil.fieldIdForStatsField(9_600)).isEqualTo(invalidFieldId);
    assertThat(StatsUtil.fieldIdForStatsField(9_800)).isEqualTo(invalidFieldId);
    assertThat(IntStream.range(RESERVED_FIELD_IDS_START, Integer.MAX_VALUE))
        .filteredOn(id -> !StatsUtil.SUPPORTED_METADATA_FIELD_IDS.contains(id))
        .allSatisfy(
            id ->
                assertThat(StatsUtil.statsFieldIdForField(id))
                    .as("at pos %s", id)
                    .isEqualTo(invalidFieldId));
  }

  @Test
  public void contentStatsForSimpleSchema() {
    Types.NestedField intField = required(0, "i", Types.IntegerType.get());
    Types.NestedField floatField = required(2, "f", Types.FloatType.get());
    Types.NestedField stringField = required(4, "s", Types.StringType.get());
    Types.NestedField booleanField = required(6, "b", Types.BooleanType.get());
    Types.NestedField uuidField = required(StatsUtil.MAX_DATA_FIELD_ID, "u", Types.UUIDType.get());
    Schema schema = new Schema(intField, floatField, stringField, booleanField, uuidField);
    Schema expectedStatsSchema =
        new Schema(
            optional(
                146,
                "content_stats",
                Types.StructType.of(
                    optional(10000, "i", FieldStatistic.fieldStatsFor(intField, 10000)),
                    optional(10400, "f", FieldStatistic.fieldStatsFor(floatField, 10400)),
                    optional(10800, "s", FieldStatistic.fieldStatsFor(stringField, 10800)),
                    optional(11200, "b", FieldStatistic.fieldStatsFor(booleanField, 11200)),
                    optional(
                        StatsUtil.MAX_DATA_STATS_FIELD_ID,
                        "u",
                        FieldStatistic.fieldStatsFor(
                            uuidField, StatsUtil.MAX_DATA_STATS_FIELD_ID)))));
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
    Types.NestedField variantField = required(30, "variant", Types.VariantType.get());
    Types.NestedField uuidField = required(StatsUtil.MAX_DATA_FIELD_ID, "u", Types.UUIDType.get());
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
            variantField,
            uuidField);
    Schema expectedStatsSchema =
        new Schema(
            optional(
                146,
                "content_stats",
                Types.StructType.of(
                    optional(
                        10000,
                        "i",
                        FieldStatistic.fieldStatsFor(
                            required(0, "i", Types.IntegerType.get()), 10000)),
                    optional(
                        10600, "list.element", FieldStatistic.fieldStatsFor(listElement, 10600)),
                    optional(
                        11400, "simple_struct.int", FieldStatistic.fieldStatsFor(structInt, 11400)),
                    optional(
                        11600,
                        "simple_struct.string",
                        FieldStatistic.fieldStatsFor(structString, 11600)),
                    optional(14400, "b.key", FieldStatistic.fieldStatsFor(mapKey, 14400)),
                    optional(14800, "b.value", FieldStatistic.fieldStatsFor(mapValue, 14800)),
                    optional(16000, "variant", FieldStatistic.fieldStatsFor(variantField, 16000)),
                    optional(
                        StatsUtil.MAX_DATA_STATS_FIELD_ID,
                        "u",
                        FieldStatistic.fieldStatsFor(
                            uuidField, StatsUtil.MAX_DATA_STATS_FIELD_ID)))));
    Schema statsSchema = new Schema(StatsUtil.contentStatsFor(schema));
    assertThat(statsSchema.asStruct()).isEqualTo(expectedStatsSchema.asStruct());
  }

  @Test
  public void contentStatsChildNamesAreUnique() {
    Schema schema =
        new Schema(
            required(1, "a", Types.StructType.of(required(2, "x", Types.IntegerType.get()))),
            required(3, "b", Types.StructType.of(required(4, "x", Types.IntegerType.get()))));

    Types.StructType contentStats = StatsUtil.contentStatsFor(schema).type().asStructType();

    assertThat(contentStats.fields().stream().map(Types.NestedField::name).toList())
        .doesNotHaveDuplicates()
        .containsExactly("a.x", "b.x");
  }

  @Test
  public void contentStatsSkipsFieldsOutsideStatsRange() {
    Types.NestedField validField = required(0, "i", Types.IntegerType.get());
    Types.NestedField outOfRangeField =
        required(StatsUtil.MAX_DATA_FIELD_ID + 1, "out_of_range", Types.IntegerType.get());
    Schema schema = new Schema(validField, outOfRangeField);
    Schema expectedStatsSchema =
        new Schema(
            optional(
                146,
                "content_stats",
                Types.StructType.of(
                    optional(10000, "i", FieldStatistic.fieldStatsFor(validField, 10000)))));
    Schema statsSchema = new Schema(StatsUtil.contentStatsFor(schema));
    assertThat(statsSchema.asStruct()).isEqualTo(expectedStatsSchema.asStruct());
  }

  @Test
  public void conditionalFieldInclusionForInteger() {
    assertThat(
            fieldStatsNames(
                FieldStatistic.fieldStatsFor(required(1, "x", Types.IntegerType.get()), 10000)))
        .containsExactly(
            LOWER_BOUND.fieldName(),
            UPPER_BOUND.fieldName(),
            TIGHT_BOUNDS.fieldName(),
            VALUE_COUNT.fieldName())
        .doesNotContain(
            NULL_VALUE_COUNT.fieldName(),
            NAN_VALUE_COUNT.fieldName(),
            AVG_VALUE_SIZE_IN_BYTES.fieldName());

    assertThat(
            fieldStatsNames(
                FieldStatistic.fieldStatsFor(optional(1, "x", Types.IntegerType.get()), 10000)))
        .containsExactly(
            LOWER_BOUND.fieldName(),
            UPPER_BOUND.fieldName(),
            TIGHT_BOUNDS.fieldName(),
            VALUE_COUNT.fieldName(),
            NULL_VALUE_COUNT.fieldName())
        .doesNotContain(NAN_VALUE_COUNT.fieldName(), AVG_VALUE_SIZE_IN_BYTES.fieldName());
  }

  @Test
  public void conditionalFieldInclusionForFloatAndDouble() {
    assertThat(
            fieldStatsNames(
                FieldStatistic.fieldStatsFor(required(1, "x", Types.FloatType.get()), 10000)))
        .containsExactly(
            LOWER_BOUND.fieldName(),
            UPPER_BOUND.fieldName(),
            TIGHT_BOUNDS.fieldName(),
            VALUE_COUNT.fieldName(),
            NAN_VALUE_COUNT.fieldName())
        .doesNotContain(NULL_VALUE_COUNT.fieldName(), AVG_VALUE_SIZE_IN_BYTES.fieldName());

    assertThat(
            fieldStatsNames(
                FieldStatistic.fieldStatsFor(optional(1, "x", Types.DoubleType.get()), 10000)))
        .containsExactly(
            LOWER_BOUND.fieldName(),
            UPPER_BOUND.fieldName(),
            TIGHT_BOUNDS.fieldName(),
            VALUE_COUNT.fieldName(),
            NULL_VALUE_COUNT.fieldName(),
            NAN_VALUE_COUNT.fieldName());
  }

  @Test
  public void conditionalFieldInclusionForString() {
    assertThat(
            fieldStatsNames(
                FieldStatistic.fieldStatsFor(required(1, "x", Types.StringType.get()), 10000)))
        .containsExactly(
            LOWER_BOUND.fieldName(),
            UPPER_BOUND.fieldName(),
            TIGHT_BOUNDS.fieldName(),
            VALUE_COUNT.fieldName(),
            AVG_VALUE_SIZE_IN_BYTES.fieldName())
        .doesNotContain(NULL_VALUE_COUNT.fieldName(), NAN_VALUE_COUNT.fieldName());

    assertThat(
            fieldStatsNames(
                FieldStatistic.fieldStatsFor(optional(1, "x", Types.StringType.get()), 10000)))
        .containsExactly(
            LOWER_BOUND.fieldName(),
            UPPER_BOUND.fieldName(),
            TIGHT_BOUNDS.fieldName(),
            VALUE_COUNT.fieldName(),
            NULL_VALUE_COUNT.fieldName(),
            AVG_VALUE_SIZE_IN_BYTES.fieldName());
  }

  @Test
  public void conditionalFieldInclusionForBinary() {
    assertThat(
            fieldStatsNames(
                FieldStatistic.fieldStatsFor(optional(1, "x", Types.BinaryType.get()), 10000)))
        .containsExactly(
            LOWER_BOUND.fieldName(),
            UPPER_BOUND.fieldName(),
            TIGHT_BOUNDS.fieldName(),
            VALUE_COUNT.fieldName(),
            NULL_VALUE_COUNT.fieldName(),
            AVG_VALUE_SIZE_IN_BYTES.fieldName())
        .doesNotContain(NAN_VALUE_COUNT.fieldName());

    assertThat(
            fieldStatsNames(
                FieldStatistic.fieldStatsFor(required(1, "x", Types.BinaryType.get()), 10000)))
        .containsExactly(
            LOWER_BOUND.fieldName(),
            UPPER_BOUND.fieldName(),
            TIGHT_BOUNDS.fieldName(),
            VALUE_COUNT.fieldName(),
            AVG_VALUE_SIZE_IN_BYTES.fieldName())
        .doesNotContain(NULL_VALUE_COUNT.fieldName(), NAN_VALUE_COUNT.fieldName());
  }

  @Test
  public void conditionalFieldInclusionForGeometry() {
    Types.StructType requiredStats =
        FieldStatistic.fieldStatsFor(required(1, "x", Types.GeometryType.crs84()), 10000);
    assertThat(fieldStatsNames(requiredStats))
        .containsExactly(LOWER_BOUND.fieldName(), UPPER_BOUND.fieldName(), VALUE_COUNT.fieldName())
        .doesNotContain(
            TIGHT_BOUNDS.fieldName(),
            NULL_VALUE_COUNT.fieldName(),
            NAN_VALUE_COUNT.fieldName(),
            AVG_VALUE_SIZE_IN_BYTES.fieldName());
    assertGeoBoundStructs(requiredStats, 10000);

    Types.StructType optionalStats =
        FieldStatistic.fieldStatsFor(optional(1, "x", Types.GeometryType.crs84()), 10000);
    assertThat(fieldStatsNames(optionalStats))
        .containsExactly(
            LOWER_BOUND.fieldName(),
            UPPER_BOUND.fieldName(),
            VALUE_COUNT.fieldName(),
            NULL_VALUE_COUNT.fieldName())
        .doesNotContain(
            TIGHT_BOUNDS.fieldName(),
            NAN_VALUE_COUNT.fieldName(),
            AVG_VALUE_SIZE_IN_BYTES.fieldName());
    assertGeoBoundStructs(optionalStats, 10000);
  }

  @Test
  public void conditionalFieldInclusionForGeography() {
    Types.StructType requiredStats =
        FieldStatistic.fieldStatsFor(required(1, "x", Types.GeographyType.crs84()), 10000);
    assertThat(fieldStatsNames(requiredStats))
        .containsExactly(LOWER_BOUND.fieldName(), UPPER_BOUND.fieldName(), VALUE_COUNT.fieldName())
        .doesNotContain(
            TIGHT_BOUNDS.fieldName(),
            NULL_VALUE_COUNT.fieldName(),
            NAN_VALUE_COUNT.fieldName(),
            AVG_VALUE_SIZE_IN_BYTES.fieldName());
    assertGeoBoundStructs(requiredStats, 10000);

    Types.StructType optionalStats =
        FieldStatistic.fieldStatsFor(optional(1, "x", Types.GeographyType.crs84()), 10000);
    assertThat(fieldStatsNames(optionalStats))
        .containsExactly(
            LOWER_BOUND.fieldName(),
            UPPER_BOUND.fieldName(),
            VALUE_COUNT.fieldName(),
            NULL_VALUE_COUNT.fieldName())
        .doesNotContain(
            TIGHT_BOUNDS.fieldName(),
            NAN_VALUE_COUNT.fieldName(),
            AVG_VALUE_SIZE_IN_BYTES.fieldName());
    assertGeoBoundStructs(optionalStats, 10000);
  }

  @Test
  public void conditionalFieldInclusionForVariant() {
    Types.StructType requiredStats =
        FieldStatistic.fieldStatsFor(required(1, "x", Types.VariantType.get()), 10000);
    assertThat(fieldStatsNames(requiredStats))
        .containsExactly(
            LOWER_BOUND.fieldName(),
            UPPER_BOUND.fieldName(),
            VALUE_COUNT.fieldName(),
            AVG_VALUE_SIZE_IN_BYTES.fieldName())
        .doesNotContain(
            TIGHT_BOUNDS.fieldName(), NULL_VALUE_COUNT.fieldName(), NAN_VALUE_COUNT.fieldName());
    assertVariantBoundTypes(requiredStats);

    Types.StructType optionalStats =
        FieldStatistic.fieldStatsFor(optional(1, "x", Types.VariantType.get()), 10000);
    assertThat(fieldStatsNames(optionalStats))
        .containsExactly(
            LOWER_BOUND.fieldName(),
            UPPER_BOUND.fieldName(),
            VALUE_COUNT.fieldName(),
            NULL_VALUE_COUNT.fieldName(),
            AVG_VALUE_SIZE_IN_BYTES.fieldName())
        .doesNotContain(TIGHT_BOUNDS.fieldName(), NAN_VALUE_COUNT.fieldName());
    assertVariantBoundTypes(optionalStats);
  }

  private void assertGeoBoundStructs(Types.StructType stats, int baseFieldId) {
    Types.NestedField lowerBound = stats.field(LOWER_BOUND.fieldName());
    assertThat(lowerBound.type().isStructType()).isTrue();
    Types.StructType geoLower = lowerBound.type().asStructType();
    assertThat(geoLower.fields()).hasSize(4);
    assertThat(geoLower.field("x"))
        .satisfies(
            f -> {
              assertThat(f.fieldId()).isEqualTo(baseFieldId + 10);
              assertThat(f.type()).isEqualTo(Types.DoubleType.get());
              assertThat(f.isRequired()).isTrue();
            });
    assertThat(geoLower.field("y"))
        .satisfies(
            f -> {
              assertThat(f.fieldId()).isEqualTo(baseFieldId + 11);
              assertThat(f.type()).isEqualTo(Types.DoubleType.get());
              assertThat(f.isRequired()).isTrue();
            });
    assertThat(geoLower.field("z"))
        .satisfies(
            f -> {
              assertThat(f.fieldId()).isEqualTo(baseFieldId + 12);
              assertThat(f.type()).isEqualTo(Types.DoubleType.get());
              assertThat(f.isOptional()).isTrue();
            });
    assertThat(geoLower.field("m"))
        .satisfies(
            f -> {
              assertThat(f.fieldId()).isEqualTo(baseFieldId + 13);
              assertThat(f.type()).isEqualTo(Types.DoubleType.get());
              assertThat(f.isOptional()).isTrue();
            });

    Types.NestedField upperBound = stats.field(UPPER_BOUND.fieldName());
    assertThat(upperBound.type().isStructType()).isTrue();
    Types.StructType geoUpper = upperBound.type().asStructType();
    assertThat(geoUpper.fields()).hasSize(4);
    assertThat(geoUpper.field("x"))
        .satisfies(
            f -> {
              assertThat(f.fieldId()).isEqualTo(baseFieldId + 14);
              assertThat(f.type()).isEqualTo(Types.DoubleType.get());
              assertThat(f.isRequired()).isTrue();
            });
    assertThat(geoUpper.field("y"))
        .satisfies(
            f -> {
              assertThat(f.fieldId()).isEqualTo(baseFieldId + 15);
              assertThat(f.type()).isEqualTo(Types.DoubleType.get());
              assertThat(f.isRequired()).isTrue();
            });
    assertThat(geoUpper.field("z"))
        .satisfies(
            f -> {
              assertThat(f.fieldId()).isEqualTo(baseFieldId + 16);
              assertThat(f.type()).isEqualTo(Types.DoubleType.get());
              assertThat(f.isOptional()).isTrue();
            });
    assertThat(geoUpper.field("m"))
        .satisfies(
            f -> {
              assertThat(f.fieldId()).isEqualTo(baseFieldId + 17);
              assertThat(f.type()).isEqualTo(Types.DoubleType.get());
              assertThat(f.isOptional()).isTrue();
            });
  }

  private void assertVariantBoundTypes(Types.StructType stats) {
    assertThat(stats.field(LOWER_BOUND.fieldName()).type()).isEqualTo(Types.VariantType.get());
    assertThat(stats.field(UPPER_BOUND.fieldName()).type()).isEqualTo(Types.VariantType.get());
  }

  private List<String> fieldStatsNames(Types.StructType structType) {
    return structType.fields().stream().map(Types.NestedField::name).toList();
  }
}
