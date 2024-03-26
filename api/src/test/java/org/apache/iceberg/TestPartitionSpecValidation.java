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

import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPartitionSpecValidation {
  private static final Schema SCHEMA =
      new Schema(
          NestedField.required(1, "id", Types.LongType.get()),
          NestedField.required(2, "ts", Types.TimestampType.withZone()),
          NestedField.required(3, "another_ts", Types.TimestampType.withZone()),
          NestedField.required(4, "d", Types.TimestampType.withZone()),
          NestedField.required(5, "another_d", Types.TimestampType.withZone()),
          NestedField.required(6, "s", Types.StringType.get()));

  @Test
  public void testMultipleTimestampPartitions() {
    Assertions.assertThatThrownBy(
            () -> PartitionSpec.builderFor(SCHEMA).year("ts").year("ts").build())
        .hasMessageContaining("Cannot use partition name more than once")
        .isInstanceOf(IllegalArgumentException.class);

    Assertions.assertThatThrownBy(
            () -> PartitionSpec.builderFor(SCHEMA).year("ts").month("ts").build())
        .hasMessageContaining("Cannot add redundant partition")
        .isInstanceOf(IllegalArgumentException.class);

    Assertions.assertThatThrownBy(
            () -> PartitionSpec.builderFor(SCHEMA).year("ts").day("ts").build())
        .hasMessageContaining("Cannot add redundant partition")
        .isInstanceOf(IllegalArgumentException.class);

    Assertions.assertThatThrownBy(
            () -> PartitionSpec.builderFor(SCHEMA).year("ts").hour("ts").build())
        .hasMessageContaining("Cannot add redundant partition")
        .isInstanceOf(IllegalArgumentException.class);

    Assertions.assertThatThrownBy(
            () -> PartitionSpec.builderFor(SCHEMA).month("ts").month("ts").build())
        .hasMessageContaining("Cannot use partition name more than once")
        .isInstanceOf(IllegalArgumentException.class);

    Assertions.assertThatThrownBy(
            () -> PartitionSpec.builderFor(SCHEMA).month("ts").day("ts").build())
        .hasMessageContaining("Cannot add redundant partition")
        .isInstanceOf(IllegalArgumentException.class);

    Assertions.assertThatThrownBy(
            () -> PartitionSpec.builderFor(SCHEMA).month("ts").hour("ts").build())
        .hasMessageContaining("Cannot add redundant partition")
        .isInstanceOf(IllegalArgumentException.class);

    Assertions.assertThatThrownBy(
            () -> PartitionSpec.builderFor(SCHEMA).day("ts").day("ts").build())
        .hasMessageContaining("Cannot use partition name more than once")
        .isInstanceOf(IllegalArgumentException.class);

    Assertions.assertThatThrownBy(
            () -> PartitionSpec.builderFor(SCHEMA).day("ts").hour("ts").build())
        .hasMessageContaining("Cannot add redundant partition")
        .isInstanceOf(IllegalArgumentException.class);

    Assertions.assertThatThrownBy(
            () -> PartitionSpec.builderFor(SCHEMA).hour("ts").hour("ts").build())
        .hasMessageContaining("Cannot use partition name more than once")
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testMultipleDatePartitions() {
    Assertions.assertThatThrownBy(
            () -> PartitionSpec.builderFor(SCHEMA).year("d").year("d").build())
        .hasMessageContaining("Cannot use partition name more than once")
        .isInstanceOf(IllegalArgumentException.class);

    Assertions.assertThatThrownBy(
            () -> PartitionSpec.builderFor(SCHEMA).year("d").month("d").build())
        .hasMessageContaining("Cannot add redundant partition")
        .isInstanceOf(IllegalArgumentException.class);

    Assertions.assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA).year("d").day("d").build())
        .hasMessageContaining("Cannot add redundant partition")
        .isInstanceOf(IllegalArgumentException.class);

    Assertions.assertThatThrownBy(
            () -> PartitionSpec.builderFor(SCHEMA).month("d").month("d").build())
        .hasMessageContaining("Cannot use partition name more than once")
        .isInstanceOf(IllegalArgumentException.class);

    Assertions.assertThatThrownBy(
            () -> PartitionSpec.builderFor(SCHEMA).month("d").day("d").build())
        .hasMessageContaining("Cannot add redundant partition")
        .isInstanceOf(IllegalArgumentException.class);

    Assertions.assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA).day("d").day("d").build())
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
    Assertions.assertThatThrownBy(
            () -> PartitionSpec.builderFor(SCHEMA).identity("id").identity("id").build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot use partition name more than once");

    Assertions.assertThatThrownBy(
            () -> PartitionSpec.builderFor(SCHEMA).identity("id").identity("id", "test-id").build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot add redundant partition");

    Assertions.assertThatThrownBy(
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
    Assertions.assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA).year("ts", "another_ts"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot create partition from name that exists in schema: another_ts");

    Assertions.assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA).month("ts", "another_ts"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot create partition from name that exists in schema: another_ts");

    Assertions.assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA).day("ts", "another_ts"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot create partition from name that exists in schema: another_ts");

    Assertions.assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA).hour("ts", "another_ts"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot create partition from name that exists in schema: another_ts");

    Assertions.assertThatThrownBy(
            () -> PartitionSpec.builderFor(SCHEMA).truncate("ts", 2, "another_ts"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot create partition from name that exists in schema: another_ts");

    Assertions.assertThatThrownBy(
            () -> PartitionSpec.builderFor(SCHEMA).bucket("ts", 4, "another_ts"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot create partition from name that exists in schema: another_ts");

    Assertions.assertThatThrownBy(
            () -> PartitionSpec.builderFor(SCHEMA).identity("ts", "another_ts"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot create identity partition sourced from different field in schema: another_ts");
  }

  @Test
  public void testMissingSourceColumn() {
    Assertions.assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA).year("missing").build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find source column: missing");

    Assertions.assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA).month("missing").build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find source column: missing");

    Assertions.assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA).day("missing").build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find source column: missing");

    Assertions.assertThatThrownBy(() -> PartitionSpec.builderFor(SCHEMA).hour("missing").build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find source column: missing");

    Assertions.assertThatThrownBy(
            () -> PartitionSpec.builderFor(SCHEMA).bucket("missing", 4).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find source column: missing");

    Assertions.assertThatThrownBy(
            () -> PartitionSpec.builderFor(SCHEMA).truncate("missing", 5).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find source column: missing");

    Assertions.assertThatThrownBy(
            () -> PartitionSpec.builderFor(SCHEMA).identity("missing").build())
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
}
