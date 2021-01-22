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

import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.junit.Assert;
import org.junit.Test;

public class TestPartitionSpecValidation {
  private static final Schema SCHEMA = new Schema(
      NestedField.required(1, "id", Types.LongType.get()),
      NestedField.required(2, "ts", Types.TimestampType.withZone()),
      NestedField.required(3, "another_ts", Types.TimestampType.withZone()),
      NestedField.required(4, "d", Types.TimestampType.withZone()),
      NestedField.required(5, "another_d", Types.TimestampType.withZone()),
      NestedField.required(6, "s", Types.StringType.get())
  );

  @Test
  public void testMultipleTimestampPartitions() {
    AssertHelpers.assertThrows("Should not allow year(ts) and year(ts)",
        IllegalArgumentException.class, "Cannot use partition name more than once",
        () -> PartitionSpec.builderFor(SCHEMA).year("ts").year("ts").build());
    AssertHelpers.assertThrows("Should not allow year(ts) and month(ts)",
        IllegalArgumentException.class, "Cannot add redundant partition",
        () -> PartitionSpec.builderFor(SCHEMA).year("ts").month("ts").build());
    AssertHelpers.assertThrows("Should not allow year(ts) and day(ts)",
        IllegalArgumentException.class, "Cannot add redundant partition",
        () -> PartitionSpec.builderFor(SCHEMA).year("ts").day("ts").build());
    AssertHelpers.assertThrows("Should not allow year(ts) and hour(ts)",
        IllegalArgumentException.class, "Cannot add redundant partition",
        () -> PartitionSpec.builderFor(SCHEMA).year("ts").hour("ts").build());

    AssertHelpers.assertThrows("Should not allow month(ts) and month(ts)",
        IllegalArgumentException.class, "Cannot use partition name more than once",
        () -> PartitionSpec.builderFor(SCHEMA).month("ts").month("ts").build());
    AssertHelpers.assertThrows("Should not allow month(ts) and day(ts)",
        IllegalArgumentException.class, "Cannot add redundant partition",
        () -> PartitionSpec.builderFor(SCHEMA).month("ts").day("ts").build());
    AssertHelpers.assertThrows("Should not allow month(ts) and hour(ts)",
        IllegalArgumentException.class, "Cannot add redundant partition",
        () -> PartitionSpec.builderFor(SCHEMA).month("ts").hour("ts").build());

    AssertHelpers.assertThrows("Should not allow day(ts) and day(ts)",
        IllegalArgumentException.class, "Cannot use partition name more than once",
        () -> PartitionSpec.builderFor(SCHEMA).day("ts").day("ts").build());
    AssertHelpers.assertThrows("Should not allow day(ts) and hour(ts)",
        IllegalArgumentException.class, "Cannot add redundant partition",
        () -> PartitionSpec.builderFor(SCHEMA).day("ts").hour("ts").build());

    AssertHelpers.assertThrows("Should not allow hour(ts) and hour(ts)",
        IllegalArgumentException.class, "Cannot use partition name more than once",
        () -> PartitionSpec.builderFor(SCHEMA).hour("ts").hour("ts").build());
  }

  @Test
  public void testMultipleDatePartitions() {
    AssertHelpers.assertThrows("Should not allow year(d) and year(d)",
        IllegalArgumentException.class, "Cannot use partition name more than once",
        () -> PartitionSpec.builderFor(SCHEMA).year("d").year("d").build());
    AssertHelpers.assertThrows("Should not allow year(d) and month(d)",
        IllegalArgumentException.class, "Cannot add redundant partition",
        () -> PartitionSpec.builderFor(SCHEMA).year("d").month("d").build());
    AssertHelpers.assertThrows("Should not allow year(d) and day(d)",
        IllegalArgumentException.class, "Cannot add redundant partition",
        () -> PartitionSpec.builderFor(SCHEMA).year("d").day("d").build());

    AssertHelpers.assertThrows("Should not allow month(d) and month(d)",
        IllegalArgumentException.class, "Cannot use partition name more than once",
        () -> PartitionSpec.builderFor(SCHEMA).month("d").month("d").build());
    AssertHelpers.assertThrows("Should not allow month(d) and day(d)",
        IllegalArgumentException.class, "Cannot add redundant partition",
        () -> PartitionSpec.builderFor(SCHEMA).month("d").day("d").build());

    AssertHelpers.assertThrows("Should not allow day(d) and day(d)",
        IllegalArgumentException.class, "Cannot use partition name more than once",
        () -> PartitionSpec.builderFor(SCHEMA).day("d").day("d").build());
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
    AssertHelpers.assertThrows("Should not allow identity(id) and identity(id)",
        IllegalArgumentException.class, "Cannot use partition name more than once",
        () -> PartitionSpec.builderFor(SCHEMA).identity("id").identity("id").build());
    AssertHelpers.assertThrows("Should not allow identity(id) and identity(id, name)",
        IllegalArgumentException.class, "Cannot add redundant partition",
        () -> PartitionSpec.builderFor(SCHEMA).identity("id").identity("id", "test-id").build());
    AssertHelpers.assertThrows("Should not allow identity(id) and identity(id, name)",
        IllegalArgumentException.class, "Cannot use partition name more than once",
        () -> PartitionSpec.builderFor(SCHEMA)
            .identity("id", "test-id").identity("d", "test-id").build());
  }

  @Test
  public void testSettingPartitionTransformsWithCustomTargetNames() {
    Assert.assertEquals(PartitionSpec.builderFor(SCHEMA).year("ts", "custom_year")
        .build().fields().get(0).name(), "custom_year");
    Assert.assertEquals(PartitionSpec.builderFor(SCHEMA).month("ts", "custom_month")
        .build().fields().get(0).name(), "custom_month");
    Assert.assertEquals(PartitionSpec.builderFor(SCHEMA).day("ts", "custom_day")
        .build().fields().get(0).name(), "custom_day");
    Assert.assertEquals(PartitionSpec.builderFor(SCHEMA).hour("ts", "custom_hour")
        .build().fields().get(0).name(), "custom_hour");
    Assert.assertEquals(PartitionSpec.builderFor(SCHEMA)
        .bucket("ts", 4, "custom_bucket")
        .build().fields().get(0).name(), "custom_bucket");
    Assert.assertEquals(PartitionSpec.builderFor(SCHEMA)
        .truncate("s", 1, "custom_truncate")
        .build().fields().get(0).name(), "custom_truncate");
  }

  @Test
  public void testSettingPartitionTransformsWithCustomTargetNamesThatAlreadyExist() {

    AssertHelpers.assertThrows("Should not allow target column name that exists in schema",
        IllegalArgumentException.class,
        "Cannot create partition from name that exists in schema: another_ts",
        () -> PartitionSpec.builderFor(SCHEMA).year("ts", "another_ts"));

    AssertHelpers.assertThrows("Should not allow target column name that exists in schema",
        IllegalArgumentException.class,
        "Cannot create partition from name that exists in schema: another_ts",
        () -> PartitionSpec.builderFor(SCHEMA).month("ts", "another_ts"));

    AssertHelpers.assertThrows("Should not allow target column name that exists in schema",
        IllegalArgumentException.class,
        "Cannot create partition from name that exists in schema: another_ts",
        () -> PartitionSpec.builderFor(SCHEMA).day("ts", "another_ts"));

    AssertHelpers.assertThrows("Should not allow target column name that exists in schema",
        IllegalArgumentException.class,
        "Cannot create partition from name that exists in schema: another_ts",
        () -> PartitionSpec.builderFor(SCHEMA).hour("ts", "another_ts"));

    AssertHelpers.assertThrows("Should not allow target column name that exists in schema",
        IllegalArgumentException.class,
        "Cannot create partition from name that exists in schema: another_ts",
        () -> PartitionSpec.builderFor(SCHEMA).truncate("ts", 2, "another_ts"));

    AssertHelpers.assertThrows("Should not allow target column name that exists in schema",
        IllegalArgumentException.class,
        "Cannot create partition from name that exists in schema: another_ts",
        () -> PartitionSpec.builderFor(SCHEMA).bucket("ts", 4, "another_ts"));

    AssertHelpers.assertThrows("Should not allow target column name sourced from a different column",
        IllegalArgumentException.class,
        "Cannot create identity partition sourced from different field in schema: another_ts",
        () -> PartitionSpec.builderFor(SCHEMA).identity("ts", "another_ts"));
  }

  @Test
  public void testMissingSourceColumn() {
    AssertHelpers.assertThrows("Should detect missing source column",
        IllegalArgumentException.class, "Cannot find source column",
        () -> PartitionSpec.builderFor(SCHEMA).year("missing").build());
    AssertHelpers.assertThrows("Should detect missing source column",
        IllegalArgumentException.class, "Cannot find source column",
        () -> PartitionSpec.builderFor(SCHEMA).month("missing").build());
    AssertHelpers.assertThrows("Should detect missing source column",
        IllegalArgumentException.class, "Cannot find source column",
        () -> PartitionSpec.builderFor(SCHEMA).day("missing").build());
    AssertHelpers.assertThrows("Should detect missing source column",
        IllegalArgumentException.class, "Cannot find source column",
        () -> PartitionSpec.builderFor(SCHEMA).hour("missing").build());
    AssertHelpers.assertThrows("Should detect missing source column",
        IllegalArgumentException.class, "Cannot find source column",
        () -> PartitionSpec.builderFor(SCHEMA).bucket("missing", 4).build());
    AssertHelpers.assertThrows("Should detect missing source column",
        IllegalArgumentException.class, "Cannot find source column",
        () -> PartitionSpec.builderFor(SCHEMA).truncate("missing", 5).build());
    AssertHelpers.assertThrows("Should detect missing source column",
        IllegalArgumentException.class, "Cannot find source column",
        () -> PartitionSpec.builderFor(SCHEMA).identity("missing").build());
  }

  @Test
  public void testAutoSettingPartitionFieldIds() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA)
        .year("ts", "custom_year")
        .bucket("ts", 4, "custom_bucket")
        .add(1, "id_partition2", "bucket[4]")
        .truncate("s", 1, "custom_truncate")
        .build();

    Assert.assertEquals(1000, spec.fields().get(0).fieldId());
    Assert.assertEquals(1001, spec.fields().get(1).fieldId());
    Assert.assertEquals(1002, spec.fields().get(2).fieldId());
    Assert.assertEquals(1003, spec.fields().get(3).fieldId());
    Assert.assertEquals(1003, spec.lastAssignedFieldId());
  }

  @Test
  public void testAddPartitionFieldsWithFieldIds() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA)
        .add(1, 1005, "id_partition1", "bucket[4]")
        .add(1, 1006, "id_partition2", "bucket[5]")
        .add(1, 1002, "id_partition3", "bucket[6]")
        .build();

    Assert.assertEquals(1005, spec.fields().get(0).fieldId());
    Assert.assertEquals(1006, spec.fields().get(1).fieldId());
    Assert.assertEquals(1002, spec.fields().get(2).fieldId());
    Assert.assertEquals(1006, spec.lastAssignedFieldId());
  }

  @Test
  public void testAddPartitionFieldsWithAndWithoutFieldIds() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA)
        .add(1, "id_partition2", "bucket[5]")
        .add(1, 1005, "id_partition1", "bucket[4]")
        .truncate("s", 1, "custom_truncate")
        .build();

    Assert.assertEquals(1000, spec.fields().get(0).fieldId());
    Assert.assertEquals(1005, spec.fields().get(1).fieldId());
    Assert.assertEquals(1006, spec.fields().get(2).fieldId());
    Assert.assertEquals(1006, spec.lastAssignedFieldId());
  }
}
