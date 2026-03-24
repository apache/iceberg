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
package org.apache.iceberg.lance;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/** Tests for bidirectional Iceberg-Arrow schema conversion. */
class TestLanceSchemaConversion {

  @Test
  void testPrimitiveTypesRoundTrip() {
    org.apache.iceberg.Schema icebergSchema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "value", Types.DoubleType.get()),
            Types.NestedField.optional(4, "flag", Types.BooleanType.get()),
            Types.NestedField.optional(5, "count", Types.LongType.get()),
            Types.NestedField.optional(6, "score", Types.FloatType.get()),
            Types.NestedField.optional(7, "data", Types.BinaryType.get()),
            Types.NestedField.optional(8, "event_date", Types.DateType.get()),
            Types.NestedField.optional(9, "event_time", Types.TimeType.get()));

    Schema arrowSchema = LanceSchemaUtil.icebergToArrow(icebergSchema);

    assertThat(arrowSchema.getFields()).hasSize(9);
    assertThat(arrowSchema.getFields().get(0).getName()).isEqualTo("id");
    assertThat(arrowSchema.getFields().get(0).isNullable()).isFalse();
    assertThat(arrowSchema.getFields().get(1).getName()).isEqualTo("name");
    assertThat(arrowSchema.getFields().get(1).isNullable()).isTrue();

    org.apache.iceberg.Schema recovered = LanceSchemaUtil.arrowToIceberg(arrowSchema);
    assertThat(recovered.columns()).hasSize(9);
    assertThat(recovered.findField("id").type().typeId())
        .isEqualTo(Types.IntegerType.get().typeId());
    assertThat(recovered.findField("name").type().typeId())
        .isEqualTo(Types.StringType.get().typeId());
    assertThat(recovered.findField("value").type().typeId())
        .isEqualTo(Types.DoubleType.get().typeId());
  }

  @Test
  void testDecimalType() {
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "amount", Types.DecimalType.of(10, 2)));

    Schema arrowSchema = LanceSchemaUtil.icebergToArrow(schema);
    org.apache.iceberg.Schema recovered = LanceSchemaUtil.arrowToIceberg(arrowSchema);

    Types.DecimalType decimal = (Types.DecimalType) recovered.findField("amount").type();
    assertThat(decimal.precision()).isEqualTo(10);
    assertThat(decimal.scale()).isEqualTo(2);
  }

  @Test
  void testTimestampTypes() {
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(
            Types.NestedField.optional(1, "ts_with_tz", Types.TimestampType.withZone()),
            Types.NestedField.optional(2, "ts_no_tz", Types.TimestampType.withoutZone()));

    Schema arrowSchema = LanceSchemaUtil.icebergToArrow(schema);
    assertThat(arrowSchema.getFields()).hasSize(2);

    org.apache.iceberg.Schema recovered = LanceSchemaUtil.arrowToIceberg(arrowSchema);
    assertThat(recovered.findField("ts_with_tz").type()).isEqualTo(Types.TimestampType.withZone());
    assertThat(recovered.findField("ts_no_tz").type()).isEqualTo(Types.TimestampType.withoutZone());
  }

  @Test
  void testFixedAndUuidTypes() {
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(
            Types.NestedField.optional(1, "uuid_col", Types.UUIDType.get()),
            Types.NestedField.optional(2, "fixed_col", Types.FixedType.ofLength(8)));

    Schema arrowSchema = LanceSchemaUtil.icebergToArrow(schema);
    assertThat(arrowSchema.getFields()).hasSize(2);
  }

  @Test
  void testFieldIdPreservation() {
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(42, "special_id", Types.IntegerType.get()),
            Types.NestedField.optional(99, "name", Types.StringType.get()));

    Schema arrowSchema = LanceSchemaUtil.icebergToArrow(schema);
    org.apache.iceberg.Schema recovered = LanceSchemaUtil.arrowToIceberg(arrowSchema);

    assertThat(recovered.findField("special_id").fieldId()).isEqualTo(42);
    assertThat(recovered.findField("name").fieldId()).isEqualTo(99);
  }

  @Test
  void testColumnNameExtraction() {
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "col_a", Types.IntegerType.get()),
            Types.NestedField.optional(2, "col_b", Types.StringType.get()),
            Types.NestedField.optional(3, "col_c", Types.DoubleType.get()));

    List<String> names = LanceSchemaUtil.columnNames(schema);
    assertThat(names).containsExactly("col_a", "col_b", "col_c");
  }

  @Test
  void testStructType() {
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(
            Types.NestedField.optional(
                1,
                "address",
                Types.StructType.of(
                    Types.NestedField.required(2, "street", Types.StringType.get()),
                    Types.NestedField.optional(3, "city", Types.StringType.get()))));

    Schema arrowSchema = LanceSchemaUtil.icebergToArrow(schema);
    assertThat(arrowSchema.getFields()).hasSize(1);
    assertThat(arrowSchema.getFields().get(0).getChildren()).hasSize(2);

    org.apache.iceberg.Schema recovered = LanceSchemaUtil.arrowToIceberg(arrowSchema);
    Types.NestedField addressField = recovered.findField("address");
    assertThat(addressField.type().isStructType()).isTrue();
    Types.StructType structType = addressField.type().asStructType();
    assertThat(structType.fields()).hasSize(2);
    assertThat(structType.field("street").type().typeId())
        .isEqualTo(Types.StringType.get().typeId());
    assertThat(structType.field("city").type().typeId()).isEqualTo(Types.StringType.get().typeId());
  }

  @Test
  void testListType() {
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(
            Types.NestedField.optional(
                1, "tags", Types.ListType.ofOptional(2, Types.StringType.get())));

    Schema arrowSchema = LanceSchemaUtil.icebergToArrow(schema);
    assertThat(arrowSchema.getFields()).hasSize(1);
    assertThat(arrowSchema.getFields().get(0).getChildren()).hasSize(1);

    org.apache.iceberg.Schema recovered = LanceSchemaUtil.arrowToIceberg(arrowSchema);
    Types.NestedField tagsField = recovered.findField("tags");
    assertThat(tagsField.type().isListType()).isTrue();
    Types.ListType listType = tagsField.type().asListType();
    assertThat(listType.elementType().typeId()).isEqualTo(Types.StringType.get().typeId());
  }
}
