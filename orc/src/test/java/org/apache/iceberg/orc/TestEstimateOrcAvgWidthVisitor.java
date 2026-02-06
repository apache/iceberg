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
package org.apache.iceberg.orc;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.orc.TypeDescription;
import org.junit.jupiter.api.Test;

public class TestEstimateOrcAvgWidthVisitor {

  // all supported fields
  protected static final Types.NestedField ID_FIELD = required(1, "id", Types.IntegerType.get());
  protected static final Types.NestedField DATA_FIELD = optional(2, "data", Types.StringType.get());
  protected static final Types.NestedField FLOAT_FIELD =
      required(3, "float", Types.FloatType.get());
  protected static final Types.NestedField DOUBLE_FIELD =
      optional(4, "double", Types.DoubleType.get());
  protected static final Types.NestedField DECIMAL_FIELD =
      optional(5, "decimal", Types.DecimalType.of(5, 3));
  protected static final Types.NestedField FIXED_FIELD =
      optional(7, "fixed", Types.FixedType.ofLength(4));
  protected static final Types.NestedField BINARY_FIELD =
      optional(8, "binary", Types.BinaryType.get());
  protected static final Types.NestedField FLOAT_LIST_FIELD =
      optional(9, "floatList", Types.ListType.ofRequired(10, Types.FloatType.get()));
  protected static final Types.NestedField LONG_FIELD = optional(11, "long", Types.LongType.get());
  protected static final Types.NestedField BOOLEAN_FIELD =
      optional(12, "boolean", Types.BooleanType.get());
  protected static final Types.NestedField TIMESTAMP_ZONE_FIELD =
      optional(13, "timestampZone", Types.TimestampType.withZone());
  protected static final Types.NestedField TIMESTAMP_FIELD =
      optional(14, "timestamp", Types.TimestampType.withoutZone());
  protected static final Types.NestedField DATE_FIELD = optional(15, "date", Types.DateType.get());
  protected static final Types.NestedField UUID_FIELD = required(16, "uuid", Types.UUIDType.get());

  protected static final Types.NestedField MAP_FIELD_1 =
      optional(
          17,
          "map1",
          Types.MapType.ofOptional(18, 19, Types.FloatType.get(), Types.StringType.get()));
  protected static final Types.NestedField MAP_FIELD_2 =
      optional(
          20,
          "map2",
          Types.MapType.ofOptional(21, 22, Types.IntegerType.get(), Types.DoubleType.get()));
  protected static final Types.NestedField STRUCT_FIELD =
      optional(
          23,
          "struct",
          Types.StructType.of(
              required(24, "booleanField", Types.BooleanType.get()),
              optional(25, "date", Types.DateType.get()),
              optional(27, "timestamp", Types.TimestampType.withZone())));

  @Test
  public void testEstimateIntegerWidth() {
    Schema integerSchema = new Schema(ID_FIELD);
    TypeDescription integerOrcSchema = ORCSchemaUtil.convert(integerSchema);
    long estimateLength = getEstimateLength(integerOrcSchema);
    assertThat(estimateLength).as("Estimated average length of integer must be 8.").isEqualTo(8);
  }

  @Test
  public void testEstimateStringWidth() {
    Schema stringSchema = new Schema(DATA_FIELD);
    TypeDescription stringOrcSchema = ORCSchemaUtil.convert(stringSchema);
    long estimateLength = getEstimateLength(stringOrcSchema);
    assertThat(estimateLength).as("Estimated average length of string must be 128.").isEqualTo(128);
  }

  @Test
  public void testEstimateFloatWidth() {
    Schema floatSchema = new Schema(FLOAT_FIELD);
    TypeDescription floatOrcSchema = ORCSchemaUtil.convert(floatSchema);
    long estimateLength = getEstimateLength(floatOrcSchema);
    assertThat(estimateLength).as("Estimated average length of float must be 8.").isEqualTo(8);
  }

  @Test
  public void testEstimateDoubleWidth() {
    Schema doubleSchema = new Schema(DOUBLE_FIELD);
    TypeDescription doubleOrcSchema = ORCSchemaUtil.convert(doubleSchema);
    long estimateLength = getEstimateLength(doubleOrcSchema);
    assertThat(estimateLength).as("Estimated average length of double must be 8.").isEqualTo(8);
  }

  @Test
  public void testEstimateDecimalWidth() {
    Schema decimalSchema = new Schema(DECIMAL_FIELD);
    TypeDescription decimalOrcSchema = ORCSchemaUtil.convert(decimalSchema);
    long estimateLength = getEstimateLength(decimalOrcSchema);
    assertThat(estimateLength).as("Estimated average length of decimal must be 7.").isEqualTo(7);
  }

  @Test
  public void testEstimateFixedWidth() {
    Schema fixedSchema = new Schema(FIXED_FIELD);
    TypeDescription fixedOrcSchema = ORCSchemaUtil.convert(fixedSchema);
    long estimateLength = getEstimateLength(fixedOrcSchema);
    assertThat(estimateLength).as("Estimated average length of fixed must be 128.").isEqualTo(128);
  }

  @Test
  public void testEstimateBinaryWidth() {
    Schema binarySchema = new Schema(BINARY_FIELD);
    TypeDescription binaryOrcSchema = ORCSchemaUtil.convert(binarySchema);
    long estimateLength = getEstimateLength(binaryOrcSchema);
    assertThat(estimateLength).as("Estimated average length of binary must be 128.").isEqualTo(128);
  }

  @Test
  public void testEstimateListWidth() {
    Schema listSchema = new Schema(FLOAT_LIST_FIELD);
    TypeDescription listOrcSchema = ORCSchemaUtil.convert(listSchema);
    long estimateLength = getEstimateLength(listOrcSchema);
    assertThat(estimateLength).as("Estimated average length of list must be 8.").isEqualTo(8);
  }

  @Test
  public void testEstimateLongWidth() {
    Schema longSchema = new Schema(LONG_FIELD);
    TypeDescription longOrcSchema = ORCSchemaUtil.convert(longSchema);
    long estimateLength = getEstimateLength(longOrcSchema);
    assertThat(estimateLength).as("Estimated average length of long must be 8.").isEqualTo(8);
  }

  @Test
  public void testEstimateBooleanWidth() {
    Schema booleanSchema = new Schema(BOOLEAN_FIELD);
    TypeDescription booleanOrcSchema = ORCSchemaUtil.convert(booleanSchema);
    long estimateLength = getEstimateLength(booleanOrcSchema);
    assertThat(estimateLength).as("Estimated average length of boolean must be 8.").isEqualTo(8);
  }

  @Test
  public void testEstimateTimestampWidth() {
    Schema timestampZoneSchema = new Schema(TIMESTAMP_ZONE_FIELD);
    TypeDescription timestampZoneOrcSchema = ORCSchemaUtil.convert(timestampZoneSchema);
    long estimateLength = getEstimateLength(timestampZoneOrcSchema);
    assertThat(estimateLength)
        .as("Estimated average length of timestamps with zone must be 12.")
        .isEqualTo(12);

    Schema timestampSchema = new Schema(TIMESTAMP_FIELD);
    TypeDescription timestampOrcSchema = ORCSchemaUtil.convert(timestampSchema);
    estimateLength = getEstimateLength(timestampOrcSchema);
    assertThat(estimateLength)
        .as("Estimated average length of timestamp must be 12.")
        .isEqualTo(12);
  }

  @Test
  public void testEstimateDateWidth() {
    Schema dateSchema = new Schema(DATE_FIELD);
    TypeDescription dateOrcSchema = ORCSchemaUtil.convert(dateSchema);
    long estimateLength = getEstimateLength(dateOrcSchema);
    assertThat(estimateLength).as("Estimated average length of date must be 8.").isEqualTo(8);
  }

  @Test
  public void testEstimateUUIDWidth() {
    Schema uuidSchema = new Schema(UUID_FIELD);
    TypeDescription uuidOrcSchema = ORCSchemaUtil.convert(uuidSchema);
    long estimateLength = getEstimateLength(uuidOrcSchema);
    assertThat(estimateLength).as("Estimated average length of uuid must be 128.").isEqualTo(128);
  }

  @Test
  public void testEstimateMapWidth() {
    Schema mapSchema = new Schema(MAP_FIELD_1);
    TypeDescription mapOrcSchema = ORCSchemaUtil.convert(mapSchema);
    long estimateLength = getEstimateLength(mapOrcSchema);
    assertThat(estimateLength).as("Estimated average length of map must be 136.").isEqualTo(136);
  }

  @Test
  public void testEstimateStructWidth() {
    Schema structSchema = new Schema(STRUCT_FIELD);
    TypeDescription structOrcSchema = ORCSchemaUtil.convert(structSchema);
    long estimateLength = getEstimateLength(structOrcSchema);
    assertThat(estimateLength).as("Estimated average length of struct must be 28.").isEqualTo(28);
  }

  @Test
  public void testEstimateFullWidth() {
    Schema fullSchema =
        new Schema(
            ID_FIELD,
            DATA_FIELD,
            FLOAT_FIELD,
            DOUBLE_FIELD,
            DECIMAL_FIELD,
            FIXED_FIELD,
            BINARY_FIELD,
            FLOAT_LIST_FIELD,
            LONG_FIELD,
            MAP_FIELD_1,
            MAP_FIELD_2,
            STRUCT_FIELD);
    TypeDescription fullOrcSchema = ORCSchemaUtil.convert(fullSchema);
    long estimateLength = getEstimateLength(fullOrcSchema);
    assertThat(estimateLength)
        .as("Estimated average length of the row must be 611.")
        .isEqualTo(611);
  }

  private Integer getEstimateLength(TypeDescription orcSchemaWithDate) {
    return OrcSchemaVisitor.visitSchema(orcSchemaWithDate, new EstimateOrcAvgWidthVisitor())
        .stream()
        .reduce(Integer::sum)
        .orElse(0);
  }
}
