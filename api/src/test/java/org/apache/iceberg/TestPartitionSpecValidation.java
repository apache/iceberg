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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TestPartitionSpecValidation {
  private static final Schema SCHEMA =
      new Schema(
          NestedField.required(1, "id", Types.LongType.get()),
          NestedField.required(2, "ts", Types.TimestampType.withZone()),
          NestedField.required(3, "another_ts", Types.TimestampType.withZone()),
          NestedField.required(4, "d", Types.TimestampType.withZone()),
          NestedField.required(5, "another_d", Types.TimestampType.withZone()),
          NestedField.required(6, "s", Types.StringType.get()),
          NestedField.required(7, "v", Types.VariantType.get()),
          NestedField.required(8, "geom", Types.GeometryType.crs84()),
          NestedField.required(9, "geog", Types.GeographyType.crs84()),
          NestedField.optional(10, "u", Types.UnknownType.get()));

  @Test
  public void testMultipleTimestampPartitions() {
    assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA).year("ts").year("ts").build())
        .hasMessageContaining("Cannot use partition name more than once")
        .isInstanceOf(IllegalArgumentException.class);

    assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA).year("ts").month("ts").build())
        .hasMessageContaining("Cannot add redundant partition")
        .isInstanceOf(IllegalArgumentException.class);

    assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA).year("ts").day("ts").build())
        .hasMessageContaining("Cannot add redundant partition")
        .isInstanceOf(IllegalArgumentException.class);

    assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA).year("ts").hour("ts").build())
        .hasMessageContaining("Cannot add redundant partition")
        .isInstanceOf(IllegalArgumentException.class);

    assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA).month("ts").month("ts").build())
        .hasMessageContaining("Cannot use partition name more than once")
        .isInstanceOf(IllegalArgumentException.class);

    assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA).month("ts").day("ts").build())
        .hasMessageContaining("Cannot add redundant partition")
        .isInstanceOf(IllegalArgumentException.class);

    assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA).month("ts").hour("ts").build())
        .hasMessageContaining("Cannot add redundant partition")
        .isInstanceOf(IllegalArgumentException.class);

    assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA).day("ts").day("ts").build())
        .hasMessageContaining("Cannot use partition name more than once")
        .isInstanceOf(IllegalArgumentException.class);

    assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA).day("ts").hour("ts").build())
        .hasMessageContaining("Cannot add redundant partition")
        .isInstanceOf(IllegalArgumentException.class);

    assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA).hour("ts").hour("ts").build())
        .hasMessageContaining("Cannot use partition name more than once")
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testMultipleDatePartitions() {
    assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA).year("d").year("d").build())
        .hasMessageContaining("Cannot use partition name more than once")
        .isInstanceOf(IllegalArgumentException.class);

    assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA).year("d").month("d").build())
        .hasMessageContaining("Cannot add redundant partition")
        .isInstanceOf(IllegalArgumentException.class);

    assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA).year("d").day("d").build())
        .hasMessageContaining("Cannot add redundant partition")
        .isInstanceOf(IllegalArgumentException.class);

    assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA).month("d").month("d").build())
        .hasMessageContaining("Cannot use partition name more than once")
        .isInstanceOf(IllegalArgumentException.class);

    assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA).month("d").day("d").build())
        .hasMessageContaining("Cannot add redundant partition")
        .isInstanceOf(IllegalArgumentException.class);

    assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA).day("d").day("d").build())
        .hasMessageContaining("Cannot use partition name more than once")
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testMultipleTimestampPartitionsWithDifferentSourceColumns() {
    PartitionSpec.builderFor(SCHEMA).year("ts").year("another_ts").build();
    PartitionSpec.builderFor(SCHEMA).year("ts").month("another_ts").build();
    PartitionSpec.builderFor(SCHEMA).year("ts").day("another_ts").build();
    PartitionSpec.builderFor(SCHEMA).year("ts").hour("another_ts").build();
    PartitionSpec.builderFor(SCHEMA).month("ts").month("another_ts").build();
    PartitionSpec.builderFor(SCHEMA).month("ts").day("another_ts").build();
    PartitionSpec.builderFor(SCHEMA).month("ts").hour("another_ts").build();
    PartitionSpec.builderFor(SCHEMA).day("ts").day("another_ts").build();
    PartitionSpec.builderFor(SCHEMA).day("ts").hour("another_ts").build();
    PartitionSpec.builderFor(SCHEMA).hour("ts").hour("another_ts").build();
  }

  @Test
  public void testMultipleDatePartitionsWithDifferentSourceColumns() {
    PartitionSpec.builderFor(SCHEMA).year("d").year("another_d").build();
    PartitionSpec.builderFor(SCHEMA).year("d").month("another_d").build();
    PartitionSpec.builderFor(SCHEMA).year("d").day("another_d").build();
    PartitionSpec.builderFor(SCHEMA).year("d").hour("another_d").build();
    PartitionSpec.builderFor(SCHEMA).month("d").month("another_d").build();
    PartitionSpec.builderFor(SCHEMA).month("d").day("another_d").build();
    PartitionSpec.builderFor(SCHEMA).month("d").hour("another_d").build();
    PartitionSpec.builderFor(SCHEMA).day("d").day("another_d").build();
    PartitionSpec.builderFor(SCHEMA).day("d").hour("another_d").build();
    PartitionSpec.builderFor(SCHEMA).hour("d").hour("another_d").build();
  }

  @Test
  public void testMultipleIdentityPartitions() {
    PartitionSpec.builderFor(SCHEMA).year("d").identity("id").identity("d").identity("s").build();
    assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA).identity("id").identity("id").build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot use partition name more than once");

    assertThatThrownBy(
            () -> PartitionSpec.builderFor(SCHEMA).identity("id").identity("id", "test-id").build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot add redundant partition");

    assertThatThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA)
                    .identity("id", "test-id")
                    .identity("d", "test-id")
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot use partition name more than once");
  }

  @Test
  public void testSettingPartitionTransformsWithCustomTargetNames() {
    assertThat(
            PartitionSpec.builderFor(SCHEMA)
                .year("ts", "custom_year")
                .build()
                .fields()
                .get(0)
                .name())
        .isEqualTo("custom_year");
    assertThat(
            PartitionSpec.builderFor(SCHEMA)
                .month("ts", "custom_month")
                .build()
                .fields()
                .get(0)
                .name())
        .isEqualTo("custom_month");
    assertThat(
            PartitionSpec.builderFor(SCHEMA).day("ts", "custom_day").build().fields().get(0).name())
        .isEqualTo("custom_day");
    assertThat(
            PartitionSpec.builderFor(SCHEMA)
                .hour("ts", "custom_hour")
                .build()
                .fields()
                .get(0)
                .name())
        .isEqualTo("custom_hour");
    assertThat(
            PartitionSpec.builderFor(SCHEMA)
                .bucket("ts", 4, "custom_bucket")
                .build()
                .fields()
                .get(0)
                .name())
        .isEqualTo("custom_bucket");
    assertThat(
            PartitionSpec.builderFor(SCHEMA)
                .truncate("s", 1, "custom_truncate")
                .build()
                .fields()
                .get(0)
                .name())
        .isEqualTo("custom_truncate");
  }

  @Test
  public void testSettingPartitionTransformsWithCustomTargetNamesThatAlreadyExist() {
    assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA).year("ts", "another_ts"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot create partition from name that exists in schema: another_ts");

    assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA).month("ts", "another_ts"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot create partition from name that exists in schema: another_ts");

    assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA).day("ts", "another_ts"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot create partition from name that exists in schema: another_ts");

    assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA).hour("ts", "another_ts"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot create partition from name that exists in schema: another_ts");

    assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA).truncate("ts", 2, "another_ts"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot create partition from name that exists in schema: another_ts");

    assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA).bucket("ts", 4, "another_ts"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot create partition from name that exists in schema: another_ts");

    assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA).identity("ts", "another_ts"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot create identity partition sourced from different field in schema: another_ts");
  }

  @Test
  public void testMissingSourceColumn() {
    assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA).year("missing").build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find source column: missing");

    assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA).month("missing").build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find source column: missing");

    assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA).day("missing").build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find source column: missing");

    assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA).hour("missing").build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find source column: missing");

    assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA).bucket("missing", 4).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find source column: missing");

    assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA).truncate("missing", 5).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find source column: missing");

    assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA).identity("missing").build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find source column: missing");
  }

  @Test
  public void testAutoSettingPartitionFieldIds() {
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA)
            .year("ts", "custom_year")
            .bucket("ts", 4, "custom_bucket")
            .add(1, "id_partition2", Transforms.bucket(4))
            .truncate("s", 1, "custom_truncate")
            .build();

    assertThat(spec.fields().get(0).fieldId()).isEqualTo(1000);
    assertThat(spec.fields().get(1).fieldId()).isEqualTo(1001);
    assertThat(spec.fields().get(2).fieldId()).isEqualTo(1002);
    assertThat(spec.fields().get(3).fieldId()).isEqualTo(1003);
    assertThat(spec.lastAssignedFieldId()).isEqualTo(1003);
  }

  @Test
  public void testAddPartitionFieldsWithFieldIds() {
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA)
            .add(1, 1005, "id_partition1", Transforms.bucket(4))
            .add(1, 1006, "id_partition2", Transforms.bucket(5))
            .add(1, 1002, "id_partition3", Transforms.bucket(6))
            .build();

    assertThat(spec.fields().get(0).fieldId()).isEqualTo(1005);
    assertThat(spec.fields().get(1).fieldId()).isEqualTo(1006);
    assertThat(spec.fields().get(2).fieldId()).isEqualTo(1002);
    assertThat(spec.lastAssignedFieldId()).isEqualTo(1006);
  }

  @Test
  public void testAddPartitionFieldsWithAndWithoutFieldIds() {
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA)
            .add(1, "id_partition2", Transforms.bucket(5))
            .add(1, 1005, "id_partition1", Transforms.bucket(4))
            .truncate("s", 1, "custom_truncate")
            .build();

    assertThat(spec.fields().get(0).fieldId()).isEqualTo(1000);
    assertThat(spec.fields().get(1).fieldId()).isEqualTo(1005);
    assertThat(spec.fields().get(2).fieldId()).isEqualTo(1006);
    assertThat(spec.lastAssignedFieldId()).isEqualTo(1006);
  }

  @ParameterizedTest
  @MethodSource("unsupportedFieldsProvider")
  public void testUnsupported(int fieldId, String partitionName, String expectedErrorMessage) {
    assertThatThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA)
                    .add(fieldId, 1005, partitionName, Transforms.bucket(5))
                    .build())
        .isInstanceOf(ValidationException.class)
        .hasMessage(expectedErrorMessage);
  }

  private static Object[][] unsupportedFieldsProvider() {
    return new Object[][] {
      {7, "variant_partition1", "Cannot partition by non-primitive source field: variant"},
      {8, "geom_partition1", "Invalid source type geometry for transform: bucket[5]"},
      {9, "geog_partition1", "Invalid source type geography for transform: bucket[5]"},
      {10, "unknown_partition1", "Invalid source type unknown for transform: bucket[5]"}
    };
  }

  @Test
  void testSourceIdNotFound() {
    assertThatThrownBy(
            () ->
                PartitionSpec.builderFor(SCHEMA)
                    .add(99, 1000, "Test", Transforms.identity())
                    .build())
        .isInstanceOf(ValidationException.class)
        .hasMessage("Cannot find source column for partition field: 1000: Test: identity(99)");
  }

  @Test
  void testPartitionFieldInStruct() {
    final Schema schema =
        new Schema(
            NestedField.required(SCHEMA.highestFieldId() + 1, "MyStruct", SCHEMA.asStruct()));
    PartitionSpec.builderFor(schema).identity("MyStruct.id").build();
  }

  @Test
  void testPartitionFieldInStructInStruct() {
    final Schema schema =
        new Schema(
            NestedField.optional(
                SCHEMA.highestFieldId() + 2,
                "Outer",
                Types.StructType.of(
                    NestedField.required(
                        SCHEMA.highestFieldId() + 1, "Inner", SCHEMA.asStruct()))));
    PartitionSpec.builderFor(schema).identity("Outer.Inner.id").build();
  }

  @Test
  void testPartitionFieldInList() {
    final Schema schema =
        new Schema(
            NestedField.required(
                2, "MyList", Types.ListType.ofRequired(1, Types.IntegerType.get())));
    assertThatThrownBy(() -> PartitionSpec.builderFor(schema).identity("MyList.element").build())
        .isInstanceOf(ValidationException.class)
        .hasMessage("Invalid partition field parent: list<int>");
  }

  @Test
  void testPartitionFieldInStructInList() {
    final Schema schema =
        new Schema(
            NestedField.required(
                3,
                "MyList",
                Types.ListType.ofRequired(
                    2,
                    Types.StructType.of(NestedField.optional(1, "Foo", Types.IntegerType.get())))));
    assertThatThrownBy(
            () -> PartitionSpec.builderFor(schema).identity("MyList.element.Foo").build())
        .isInstanceOf(ValidationException.class)
        .hasMessage("Invalid partition field parent: list<struct<1: Foo: optional int>>");
  }

  @Test
  void testPartitionFieldInMap() {
    final Schema schema =
        new Schema(
            NestedField.required(
                3,
                "MyMap",
                Types.MapType.ofRequired(1, 2, Types.IntegerType.get(), Types.IntegerType.get())));

    assertThatThrownBy(() -> PartitionSpec.builderFor(schema).identity("MyMap.key").build())
        .isInstanceOf(ValidationException.class)
        .hasMessage("Invalid partition field parent: map<int, int>");

    assertThatThrownBy(() -> PartitionSpec.builderFor(schema).identity("MyMap.value").build())
        .isInstanceOf(ValidationException.class)
        .hasMessage("Invalid partition field parent: map<int, int>");
  }

  @Test
  void testPartitionFieldInStructInMap() {
    final Schema schema =
        new Schema(
            NestedField.required(
                5,
                "MyMap",
                Types.MapType.ofRequired(
                    3,
                    4,
                    Types.StructType.of(NestedField.optional(1, "Foo", Types.IntegerType.get())),
                    Types.StructType.of(NestedField.optional(2, "Bar", Types.IntegerType.get())))));

    assertThatThrownBy(() -> PartitionSpec.builderFor(schema).identity("MyMap.key.Foo").build())
        .isInstanceOf(ValidationException.class)
        .hasMessage(
            "Invalid partition field parent: map<struct<1: Foo: optional int>, struct<2: Bar: optional int>>");

    assertThatThrownBy(() -> PartitionSpec.builderFor(schema).identity("MyMap.value.Bar").build())
        .isInstanceOf(ValidationException.class)
        .hasMessage(
            "Invalid partition field parent: map<struct<1: Foo: optional int>, struct<2: Bar: optional int>>");
  }
}
