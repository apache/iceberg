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

import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.expressions.Expressions.bucket;
import static org.apache.iceberg.expressions.Expressions.day;
import static org.apache.iceberg.expressions.Expressions.hour;
import static org.apache.iceberg.expressions.Expressions.month;
import static org.apache.iceberg.expressions.Expressions.ref;
import static org.apache.iceberg.expressions.Expressions.truncate;
import static org.apache.iceberg.expressions.Expressions.year;

public class TestUpdatePartitionSpecV2 {
  private static final Schema SCHEMA = new Schema(
      Types.NestedField.required(1, "id", Types.LongType.get()),
      Types.NestedField.required(2, "ts", Types.TimestampType.withZone()),
      Types.NestedField.required(3, "category", Types.StringType.get()),
      Types.NestedField.optional(4, "data", Types.StringType.get())
  );

  private static final int FORMAT_VERSION = 2;
  private static final PartitionSpec UNPARTITIONED = PartitionSpec.builderFor(SCHEMA).build();
  private static final PartitionSpec PARTITIONED = PartitionSpec.builderFor(SCHEMA)
      .identity("category")
      .day("ts")
      .bucket("id", 16, "shard")
      .build();

  @Test
  public void testAddIdentityByName() {
    PartitionSpec updated = new BaseUpdatePartitionSpec(FORMAT_VERSION, UNPARTITIONED)
        .addField("category")
        .apply();

    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA)
        .identity("category")
        .build();

    Assert.assertEquals("Should match expected spec", expected, updated);
  }

  @Test
  public void testAddIdentityByTerm() {
    PartitionSpec updated = new BaseUpdatePartitionSpec(FORMAT_VERSION, UNPARTITIONED)
        .addField(ref("category"))
        .apply();

    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA)
        .identity("category")
        .build();

    Assert.assertEquals("Should match expected spec", expected, updated);
  }

  @Test
  public void testAddYear() {
    PartitionSpec updated = new BaseUpdatePartitionSpec(FORMAT_VERSION, UNPARTITIONED)
        .addField(year("ts"))
        .apply();

    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA)
        .year("ts")
        .build();

    Assert.assertEquals("Should match expected spec", expected, updated);
  }

  @Test
  public void testAddMonth() {
    PartitionSpec updated = new BaseUpdatePartitionSpec(FORMAT_VERSION, UNPARTITIONED)
        .addField(month("ts"))
        .apply();

    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA)
        .month("ts")
        .build();

    Assert.assertEquals("Should match expected spec", expected, updated);
  }

  @Test
  public void testAddDay() {
    PartitionSpec updated = new BaseUpdatePartitionSpec(FORMAT_VERSION, UNPARTITIONED)
        .addField(day("ts"))
        .apply();

    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA)
        .day("ts")
        .build();

    Assert.assertEquals("Should match expected spec", expected, updated);
  }

  @Test
  public void testAddHour() {
    PartitionSpec updated = new BaseUpdatePartitionSpec(FORMAT_VERSION, UNPARTITIONED)
        .addField(hour("ts"))
        .apply();

    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA)
        .hour("ts")
        .build();

    Assert.assertEquals("Should match expected spec", expected, updated);
  }

  @Test
  public void testAddBucket() {
    PartitionSpec updated = new BaseUpdatePartitionSpec(FORMAT_VERSION, UNPARTITIONED)
        .addField(bucket("id", 16))
        .apply();

    // added fields have different default names to avoid conflicts
    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA)
        .bucket("id", 16, "id_bucket_16")
        .build();

    Assert.assertEquals("Should match expected spec", expected, updated);
  }

  @Test
  public void testAddTruncate() {
    PartitionSpec updated = new BaseUpdatePartitionSpec(FORMAT_VERSION, UNPARTITIONED)
        .addField(truncate("data", 4))
        .apply();

    // added fields have different default names to avoid conflicts
    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA)
        .truncate("data", 4, "data_trunc_4")
        .build();

    Assert.assertEquals("Should match expected spec", expected, updated);
  }

  @Test
  public void testAddNamedPartition() {
    PartitionSpec updated = new BaseUpdatePartitionSpec(FORMAT_VERSION, UNPARTITIONED)
        .addField("shard", bucket("id", 16))
        .apply();

    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA)
        .bucket("id", 16, "shard")
        .build();

    Assert.assertEquals("Should match expected spec", expected, updated);
  }

  @Test
  public void testAddToExisting() {
    PartitionSpec updated = new BaseUpdatePartitionSpec(FORMAT_VERSION, PARTITIONED)
        .addField(truncate("data", 4))
        .apply();

    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA)
        .identity("category")
        .day("ts")
        .bucket("id", 16, "shard")
        .truncate("data", 4, "data_trunc_4")
        .build();

    Assert.assertEquals("Should match expected spec", expected, updated);
  }

  @Test
  public void testMultipleAdds() {
    PartitionSpec updated = new BaseUpdatePartitionSpec(FORMAT_VERSION, UNPARTITIONED)
        .addField("category")
        .addField(day("ts"))
        .addField("shard", bucket("id", 16))
        .addField("prefix", truncate("data", 4))
        .apply();

    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA)
        .identity("category")
        .day("ts")
        .bucket("id", 16, "shard")
        .truncate("data", 4, "prefix")
        .build();

    Assert.assertEquals("Should match expected spec", expected, updated);
  }

  @Test
  public void testAddHourToDay() {
    // multiple partitions for the same source with different time granularity is not allowed by the builder, but is
    // allowed when updating a spec so that existing columns in metadata continue to work.
    PartitionSpec byDay = new BaseUpdatePartitionSpec(FORMAT_VERSION, UNPARTITIONED)
        .addField(day("ts"))
        .apply();

    PartitionSpec byHour = new BaseUpdatePartitionSpec(FORMAT_VERSION, byDay)
        .addField(hour("ts"))
        .apply();

    Assert.assertEquals("Should have a day and an hour time field",
        ImmutableList.of(
            new PartitionField(2, 1000, "ts_day", Transforms.day(Types.TimestampType.withZone())),
            new PartitionField(2, 1001, "ts_hour", Transforms.hour(Types.TimestampType.withZone()))),
        byHour.fields());
  }

  @Test
  public void testAddMultipleBuckets() {
    PartitionSpec bucket16 = new BaseUpdatePartitionSpec(FORMAT_VERSION, UNPARTITIONED)
        .addField(bucket("id", 16))
        .apply();

    PartitionSpec bucket8 = new BaseUpdatePartitionSpec(FORMAT_VERSION, bucket16)
        .addField(bucket("id", 8))
        .apply();

    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA)
        .bucket("id", 16, "id_bucket_16")
        .bucket("id", 8, "id_bucket_8")
        .build();

    Assert.assertEquals("Should have a day and an hour time field", expected, bucket8);
  }

  @Test
  public void testRemoveIdentityByName() {
    PartitionSpec updated = new BaseUpdatePartitionSpec(FORMAT_VERSION, PARTITIONED)
        .removeField("category")
        .apply();

    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA)
        .add(id("ts"), 1001, "ts_day", Transforms.day(Types.TimestampType.withZone()))
        .add(id("id"), 1002, "shard", Transforms.bucket(Types.LongType.get(), 16))
        .build();

    Assert.assertEquals("Should match expected spec", expected, updated);
  }

  @Test
  public void testRemoveBucketByName() {
    PartitionSpec updated = new BaseUpdatePartitionSpec(FORMAT_VERSION, PARTITIONED)
        .removeField("shard")
        .apply();

    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA)
        .add(id("category"), 1000, "category", Transforms.identity(Types.StringType.get()))
        .add(id("ts"), 1001, "ts_day", Transforms.day(Types.TimestampType.withZone()))
        .build();

    Assert.assertEquals("Should match expected spec", expected, updated);
  }

  @Test
  public void testRemoveIdentityByEquivalent() {
    PartitionSpec updated = new BaseUpdatePartitionSpec(FORMAT_VERSION, PARTITIONED)
        .removeField(ref("category"))
        .apply();

    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA)
        .add(id("ts"), 1001, "ts_day", Transforms.day(Types.TimestampType.withZone()))
        .add(id("id"), 1002, "shard", Transforms.bucket(Types.LongType.get(), 16))
        .build();

    Assert.assertEquals("Should match expected spec", expected, updated);
  }

  @Test
  public void testRemoveDayByEquivalent() {
    PartitionSpec updated = new BaseUpdatePartitionSpec(FORMAT_VERSION, PARTITIONED)
        .removeField(day("ts"))
        .apply();

    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA)
        .add(id("category"), 1000, "category", Transforms.identity(Types.StringType.get()))
        .add(id("id"), 1002, "shard", Transforms.bucket(Types.LongType.get(), 16))
        .build();

    Assert.assertEquals("Should match expected spec", expected, updated);
  }

  @Test
  public void testRemoveBucketByEquivalent() {
    PartitionSpec updated = new BaseUpdatePartitionSpec(FORMAT_VERSION, PARTITIONED)
        .removeField(bucket("id", 16))
        .apply();

    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA)
        .identity("category")
        .day("ts")
        .build();

    Assert.assertEquals("Should match expected spec", expected, updated);
  }

  @Test
  public void testRename() {
    PartitionSpec updated = new BaseUpdatePartitionSpec(FORMAT_VERSION, PARTITIONED)
        .renameField("shard", "id_bucket") // rename back to default
        .apply();

    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA)
        .identity("category")
        .day("ts")
        .bucket("id", 16)
        .build();

    Assert.assertEquals("Should match expected spec", expected, updated);
  }

  @Test
  public void testMultipleChanges() {
    PartitionSpec updated = new BaseUpdatePartitionSpec(FORMAT_VERSION, PARTITIONED)
        .renameField("shard", "id_bucket") // rename back to default
        .removeField(day("ts"))
        .addField("prefix", truncate("data", 4))
        .apply();

    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA)
        .add(id("category"), 1000, "category", Transforms.identity(Types.StringType.get()))
        .add(id("id"), 1002, "id_bucket", Transforms.bucket(Types.LongType.get(), 16))
        .add(id("data"), 1003, "prefix", Transforms.truncate(Types.StringType.get(), 4))
        .build();

    Assert.assertEquals("Should match expected spec", expected, updated);
  }

  @Test
  public void testRemoveUnknownFieldByName() {
    AssertHelpers.assertThrows("Should fail trying to remove unknown field",
        IllegalArgumentException.class, "Cannot find partition field to remove",
        () -> new BaseUpdatePartitionSpec(FORMAT_VERSION, PARTITIONED).removeField("moon")
    );
  }

  @Test
  public void testRemoveUnknownFieldAfterAdd() {
    AssertHelpers.assertThrows("Should fail trying to remove unknown field",
        IllegalArgumentException.class, "Cannot find partition field to remove",
        () -> new BaseUpdatePartitionSpec(FORMAT_VERSION, PARTITIONED)
            .addField("data_trunc", truncate("data", 4))
            .removeField("data_trunc")
    );
  }

  @Test
  public void testRemoveUnknownFieldByEquivalent() {
    AssertHelpers.assertThrows("Should fail trying to remove unknown field",
        IllegalArgumentException.class, "Cannot find partition field to remove",
        () -> new BaseUpdatePartitionSpec(FORMAT_VERSION, PARTITIONED).removeField(hour("ts")) // day(ts) exists
    );
  }

  @Test
  public void testRenameUnknownField() {
    AssertHelpers.assertThrows("Should fail trying to remove unknown field",
        IllegalArgumentException.class, "Cannot find partition field to rename",
        () -> new BaseUpdatePartitionSpec(FORMAT_VERSION, PARTITIONED).renameField("shake", "seal")
    );
  }

  @Test
  public void testRenameAfterAdd() {
    AssertHelpers.assertThrows("Should fail trying to remove unknown field",
        IllegalArgumentException.class, "Cannot find partition field to rename",
        () -> new BaseUpdatePartitionSpec(FORMAT_VERSION, PARTITIONED)
            .addField("data_trunc", truncate("data", 4))
            .renameField("data_trunc", "prefix")
    );
  }

  @Test
  public void testDeleteAndRename() {
    AssertHelpers.assertThrows("Should fail trying to remove unknown field",
        IllegalArgumentException.class, "Cannot rename and delete partition field",
        () -> new BaseUpdatePartitionSpec(FORMAT_VERSION, PARTITIONED)
            .renameField("shard", "id_bucket")
            .removeField(bucket("id", 16)));
  }

  @Test
  public void testRenameAndDelete() {
    AssertHelpers.assertThrows("Should fail trying to remove unknown field",
        IllegalArgumentException.class, "Cannot delete and rename partition field",
        () -> new BaseUpdatePartitionSpec(FORMAT_VERSION, PARTITIONED)
            .removeField(bucket("id", 16))
            .renameField("shard", "id_bucket"));
  }

  private static int id(String name) {
    return SCHEMA.findField(name).fieldId();
  }
}
