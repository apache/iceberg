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

import static org.apache.iceberg.expressions.Expressions.bucket;
import static org.apache.iceberg.expressions.Expressions.day;
import static org.apache.iceberg.expressions.Expressions.hour;
import static org.apache.iceberg.expressions.Expressions.month;
import static org.apache.iceberg.expressions.Expressions.ref;
import static org.apache.iceberg.expressions.Expressions.truncate;
import static org.apache.iceberg.expressions.Expressions.year;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestUpdatePartitionSpec extends TestBase {
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.required(2, "ts", Types.TimestampType.withZone()),
          Types.NestedField.required(3, "category", Types.StringType.get()),
          Types.NestedField.optional(4, "data", Types.StringType.get()));

  private static final PartitionSpec UNPARTITIONED = PartitionSpec.builderFor(SCHEMA).build();
  private static final PartitionSpec PARTITIONED =
      PartitionSpec.builderFor(SCHEMA)
          .identity("category")
          .day("ts")
          .bucket("id", 16, "shard")
          .build();

  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(1, 2, 3);
  }

  @TestTemplate
  public void testAddIdentityByName() {
    PartitionSpec updated =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED).addField("category").apply();

    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA).identity("category").build();

    assertThat(updated).isEqualTo(expected);
  }

  @TestTemplate
  public void testAddIdentityByTerm() {
    PartitionSpec updated =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED).addField(ref("category")).apply();

    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA).identity("category").build();

    assertThat(updated).isEqualTo(expected);
  }

  @TestTemplate
  public void testAddYear() {
    PartitionSpec updated =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED).addField(year("ts")).apply();

    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA).year("ts").build();

    assertThat(updated).isEqualTo(expected);
  }

  @TestTemplate
  public void testAddMonth() {
    PartitionSpec updated =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED).addField(month("ts")).apply();

    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA).month("ts").build();

    assertThat(updated).isEqualTo(expected);
  }

  @TestTemplate
  public void testAddDay() {
    PartitionSpec updated =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED).addField(day("ts")).apply();

    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA).day("ts").build();

    assertThat(updated).isEqualTo(expected);
  }

  @TestTemplate
  public void testAddHour() {
    PartitionSpec updated =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED).addField(hour("ts")).apply();

    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA).hour("ts").build();

    assertThat(updated).isEqualTo(expected);
  }

  @TestTemplate
  public void testAddBucket() {
    PartitionSpec updated =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED)
            .addField(bucket("id", 16))
            .apply();

    // added fields have different default names to avoid conflicts
    PartitionSpec expected =
        PartitionSpec.builderFor(SCHEMA).bucket("id", 16, "id_bucket_16").build();

    assertThat(updated).isEqualTo(expected);
  }

  @TestTemplate
  public void testAddTruncate() {
    PartitionSpec updated =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED)
            .addField(truncate("data", 4))
            .apply();

    // added fields have different default names to avoid conflicts
    PartitionSpec expected =
        PartitionSpec.builderFor(SCHEMA).truncate("data", 4, "data_trunc_4").build();

    assertThat(updated).isEqualTo(expected);
  }

  @TestTemplate
  public void testAddNamedPartition() {
    PartitionSpec updated =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED)
            .addField("shard", bucket("id", 16))
            .apply();

    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA).bucket("id", 16, "shard").build();

    assertThat(updated).isEqualTo(expected);
  }

  @TestTemplate
  public void testAddToExisting() {
    PartitionSpec updated =
        new BaseUpdatePartitionSpec(formatVersion, PARTITIONED)
            .addField(truncate("data", 4))
            .apply();

    PartitionSpec expected =
        PartitionSpec.builderFor(SCHEMA)
            .identity("category")
            .day("ts")
            .bucket("id", 16, "shard")
            .truncate("data", 4, "data_trunc_4")
            .build();

    assertThat(updated).isEqualTo(expected);
  }

  @TestTemplate
  public void testMultipleAdds() {
    PartitionSpec updated =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED)
            .addField("category")
            .addField(day("ts"))
            .addField("shard", bucket("id", 16))
            .addField("prefix", truncate("data", 4))
            .apply();

    PartitionSpec expected =
        PartitionSpec.builderFor(SCHEMA)
            .identity("category")
            .day("ts")
            .bucket("id", 16, "shard")
            .truncate("data", 4, "prefix")
            .build();

    assertThat(updated).isEqualTo(expected);
  }

  @TestTemplate
  public void testAddHourToDay() {
    // multiple partitions for the same source with different time granularity is not allowed by the
    // builder, but is
    // allowed when updating a spec so that existing columns in metadata continue to work.
    PartitionSpec byDay =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED).addField(day("ts")).apply();

    PartitionSpec byHour =
        new BaseUpdatePartitionSpec(formatVersion, byDay).addField(hour("ts")).apply();

    assertThat(byHour.fields())
        .containsExactly(
            new PartitionField(2, 1000, "ts_day", Transforms.day()),
            new PartitionField(2, 1001, "ts_hour", Transforms.hour()));
  }

  @TestTemplate
  public void testAddMultipleBuckets() {
    PartitionSpec bucket16 =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED)
            .addField(bucket("id", 16))
            .apply();

    PartitionSpec bucket8 =
        new BaseUpdatePartitionSpec(formatVersion, bucket16).addField(bucket("id", 8)).apply();

    PartitionSpec expected =
        PartitionSpec.builderFor(SCHEMA)
            .bucket("id", 16, "id_bucket_16")
            .bucket("id", 8, "id_bucket_8")
            .build();

    assertThat(bucket8).isEqualTo(expected);
  }

  @TestTemplate
  public void testRemoveIdentityByName() {
    PartitionSpec updated =
        new BaseUpdatePartitionSpec(formatVersion, PARTITIONED).removeField("category").apply();

    PartitionSpec v1Expected =
        PartitionSpec.builderFor(SCHEMA)
            .alwaysNull("category", "category")
            .day("ts")
            .bucket("id", 16, "shard")
            .build();

    V1Assert.assertEquals("Should match expected spec", v1Expected, updated);

    PartitionSpec v2Expected =
        PartitionSpec.builderFor(SCHEMA)
            .add(id("ts"), 1001, "ts_day", Transforms.day())
            .add(id("id"), 1002, "shard", Transforms.bucket(16))
            .build();

    V2Assert.assertEquals("Should match expected spec", v2Expected, updated);
  }

  @TestTemplate
  public void testRemoveBucketByName() {
    PartitionSpec updated =
        new BaseUpdatePartitionSpec(formatVersion, PARTITIONED).removeField("shard").apply();

    PartitionSpec v1Expected =
        PartitionSpec.builderFor(SCHEMA)
            .identity("category")
            .day("ts")
            .alwaysNull("id", "shard")
            .build();

    V1Assert.assertEquals("Should match expected spec", v1Expected, updated);

    PartitionSpec v2Expected =
        PartitionSpec.builderFor(SCHEMA)
            .add(id("category"), 1000, "category", Transforms.identity())
            .add(id("ts"), 1001, "ts_day", Transforms.day())
            .build();

    V2Assert.assertEquals("Should match expected spec", v2Expected, updated);
  }

  @TestTemplate
  public void testRemoveIdentityByEquivalent() {
    PartitionSpec updated =
        new BaseUpdatePartitionSpec(formatVersion, PARTITIONED)
            .removeField(ref("category"))
            .apply();

    PartitionSpec v1Expected =
        PartitionSpec.builderFor(SCHEMA)
            .alwaysNull("category", "category")
            .day("ts")
            .bucket("id", 16, "shard")
            .build();

    V1Assert.assertEquals("Should match expected spec", v1Expected, updated);

    PartitionSpec v2Expected =
        PartitionSpec.builderFor(SCHEMA)
            .add(id("ts"), 1001, "ts_day", Transforms.day())
            .add(id("id"), 1002, "shard", Transforms.bucket(16))
            .build();

    V2Assert.assertEquals("Should match expected spec", v2Expected, updated);
  }

  @TestTemplate
  public void testRemoveDayByEquivalent() {
    PartitionSpec updated =
        new BaseUpdatePartitionSpec(formatVersion, PARTITIONED).removeField(day("ts")).apply();

    PartitionSpec v1Expected =
        PartitionSpec.builderFor(SCHEMA)
            .identity("category")
            .alwaysNull("ts", "ts_day")
            .bucket("id", 16, "shard")
            .build();

    V1Assert.assertEquals("Should match expected spec", v1Expected, updated);

    PartitionSpec v2Expected =
        PartitionSpec.builderFor(SCHEMA)
            .add(id("category"), 1000, "category", Transforms.identity())
            .add(id("id"), 1002, "shard", Transforms.bucket(16))
            .build();

    V2Assert.assertEquals("Should match expected spec", v2Expected, updated);
  }

  @TestTemplate
  public void testRemoveBucketByEquivalent() {
    PartitionSpec updated =
        new BaseUpdatePartitionSpec(formatVersion, PARTITIONED)
            .removeField(bucket("id", 16))
            .apply();

    PartitionSpec v1Expected =
        PartitionSpec.builderFor(SCHEMA)
            .identity("category")
            .day("ts")
            .alwaysNull("id", "shard")
            .build();

    V1Assert.assertEquals("Should match expected spec", v1Expected, updated);

    PartitionSpec v2Expected =
        PartitionSpec.builderFor(SCHEMA).identity("category").day("ts").build();

    V2Assert.assertEquals("Should match expected spec", v2Expected, updated);
  }

  @TestTemplate
  public void testRename() {
    PartitionSpec updated =
        new BaseUpdatePartitionSpec(formatVersion, PARTITIONED)
            .renameField("shard", "id_bucket") // rename back to default
            .apply();

    PartitionSpec expected =
        PartitionSpec.builderFor(SCHEMA).identity("category").day("ts").bucket("id", 16).build();

    assertThat(updated).isEqualTo(expected);
  }

  @TestTemplate
  public void testMultipleChanges() {
    PartitionSpec updated =
        new BaseUpdatePartitionSpec(formatVersion, PARTITIONED)
            .renameField("shard", "id_bucket") // rename back to default
            .removeField(day("ts"))
            .addField("prefix", truncate("data", 4))
            .apply();

    PartitionSpec v1Expected =
        PartitionSpec.builderFor(SCHEMA)
            .identity("category")
            .alwaysNull("ts", "ts_day")
            .bucket("id", 16)
            .truncate("data", 4, "prefix")
            .build();

    V1Assert.assertEquals("Should match expected spec", v1Expected, updated);

    PartitionSpec v2Expected =
        PartitionSpec.builderFor(SCHEMA)
            .add(id("category"), 1000, "category", Transforms.identity())
            .add(id("id"), 1002, "id_bucket", Transforms.bucket(16))
            .add(id("data"), 1003, "prefix", Transforms.truncate(4))
            .build();

    V2Assert.assertEquals("Should match expected spec", v2Expected, updated);
  }

  @TestTemplate
  public void testAddDeletedName() {
    PartitionSpec updated =
        new BaseUpdatePartitionSpec(formatVersion, PARTITIONED)
            .removeField(bucket("id", 16))
            .apply();

    PartitionSpec v1Expected =
        PartitionSpec.builderFor(SCHEMA)
            .identity("category")
            .day("ts")
            .alwaysNull("id", "shard")
            .build();

    V1Assert.assertEquals("Should match expected spec", v1Expected, updated);

    PartitionSpec v2Expected =
        PartitionSpec.builderFor(SCHEMA).identity("category").day("ts").build();

    V2Assert.assertEquals("Should match expected spec", v2Expected, updated);
  }

  @TestTemplate
  public void testRemoveNewlyAddedFieldByName() {
    assertThatThrownBy(
            () ->
                new BaseUpdatePartitionSpec(formatVersion, PARTITIONED)
                    .addField("prefix", truncate("data", 4))
                    .removeField("prefix"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot delete newly added field");
  }

  @TestTemplate
  public void testRemoveNewlyAddedFieldByTransform() {
    assertThatThrownBy(
            () ->
                new BaseUpdatePartitionSpec(formatVersion, PARTITIONED)
                    .addField("prefix", truncate("data", 4))
                    .removeField(truncate("data", 4)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot delete newly added field");
  }

  @TestTemplate
  public void testAddAlreadyAddedFieldByTransform() {
    assertThatThrownBy(
            () ->
                new BaseUpdatePartitionSpec(formatVersion, PARTITIONED)
                    .addField("prefix", truncate("data", 4))
                    .addField(truncate("data", 4)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot add duplicate partition field");
  }

  @TestTemplate
  public void testAddAlreadyAddedFieldByName() {
    assertThatThrownBy(
            () ->
                new BaseUpdatePartitionSpec(formatVersion, PARTITIONED)
                    .addField("prefix", truncate("data", 4))
                    .addField("prefix", truncate("data", 6)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot add duplicate partition field");
  }

  @TestTemplate
  public void testAddRedundantTimePartition() {
    assertThatThrownBy(
            () ->
                new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED)
                    .addField(day("ts"))
                    .addField(hour("ts"))) // conflicts with hour
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot add redundant partition field");

    assertThatThrownBy(
            () ->
                new BaseUpdatePartitionSpec(formatVersion, PARTITIONED)
                    .addField(hour("ts")) // does not conflict with day because day already exists
                    .addField(month("ts"))) // conflicts with hour
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot add redundant partition");
  }

  @TestTemplate
  public void testNoEffectAddDeletedSameFieldWithSameName() {
    PartitionSpec updated =
        new BaseUpdatePartitionSpec(formatVersion, PARTITIONED)
            .removeField("shard")
            .addField("shard", bucket("id", 16))
            .apply();
    assertThat(updated).isEqualTo(PARTITIONED);
    updated =
        new BaseUpdatePartitionSpec(formatVersion, PARTITIONED)
            .removeField("shard")
            .addField(bucket("id", 16))
            .apply();
    assertThat(updated).isEqualTo(PARTITIONED);
  }

  @TestTemplate
  public void testGenerateNewSpecAddDeletedSameFieldWithDifferentName() {
    PartitionSpec updated =
        new BaseUpdatePartitionSpec(formatVersion, PARTITIONED)
            .removeField("shard")
            .addField("new_shard", bucket("id", 16))
            .apply();
    assertThat(updated.fields()).hasSize(3);
    assertThat(updated.fields().get(0).name()).isEqualTo("category");
    assertThat(updated.fields().get(1).name()).isEqualTo("ts_day");
    assertThat(updated.fields().get(2).name()).isEqualTo("new_shard");
    assertThat(updated.fields().get(0).transform()).asString().isEqualTo("identity");
    assertThat(updated.fields().get(1).transform()).asString().isEqualTo("day");
    assertThat(updated.fields().get(2).transform()).asString().isEqualTo("bucket[16]");
  }

  @TestTemplate
  public void testAddDuplicateByName() {
    assertThatThrownBy(
            () -> new BaseUpdatePartitionSpec(formatVersion, PARTITIONED).addField("category"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot add duplicate partition field");
  }

  @TestTemplate
  public void testAddDuplicateByRef() {
    assertThatThrownBy(
            () -> new BaseUpdatePartitionSpec(formatVersion, PARTITIONED).addField(ref("category")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot add duplicate partition field");
  }

  @TestTemplate
  public void testAddDuplicateTransform() {
    assertThatThrownBy(
            () ->
                new BaseUpdatePartitionSpec(formatVersion, PARTITIONED).addField(bucket("id", 16)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot add duplicate partition field");
  }

  @TestTemplate
  public void testAddNamedDuplicate() {
    assertThatThrownBy(
            () ->
                new BaseUpdatePartitionSpec(formatVersion, PARTITIONED)
                    .addField("b16", bucket("id", 16)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot add duplicate partition field");
  }

  @TestTemplate
  public void testRemoveUnknownFieldByName() {
    assertThatThrownBy(
            () -> new BaseUpdatePartitionSpec(formatVersion, PARTITIONED).removeField("moon"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot find partition field to remove");
  }

  @TestTemplate
  public void testRemoveUnknownFieldByEquivalent() {
    assertThatThrownBy(
            () ->
                new BaseUpdatePartitionSpec(formatVersion, PARTITIONED)
                    .removeField(hour("ts")) // day(ts) exists
            )
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot find partition field to remove");
  }

  @TestTemplate
  public void testRenameUnknownField() {
    assertThatThrownBy(
            () ->
                new BaseUpdatePartitionSpec(formatVersion, PARTITIONED)
                    .renameField("shake", "seal"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find partition field to rename: shake");
  }

  @TestTemplate
  public void testRenameAfterAdd() {
    assertThatThrownBy(
            () ->
                new BaseUpdatePartitionSpec(formatVersion, PARTITIONED)
                    .addField("data_trunc", truncate("data", 4))
                    .renameField("data_trunc", "prefix"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot rename newly added partition field: data_trunc");
  }

  @TestTemplate
  public void testRenameAndDelete() {
    assertThatThrownBy(
            () ->
                new BaseUpdatePartitionSpec(formatVersion, PARTITIONED)
                    .renameField("shard", "id_bucket")
                    .removeField(bucket("id", 16)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot rename and delete partition field: shard");
  }

  @TestTemplate
  public void testDeleteAndRename() {
    assertThatThrownBy(
            () ->
                new BaseUpdatePartitionSpec(formatVersion, PARTITIONED)
                    .removeField(bucket("id", 16))
                    .renameField("shard", "id_bucket"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot delete and rename partition field: shard");
  }

  @TestTemplate
  public void testRemoveAndAddMultiTimes() {
    PartitionSpec addFirstTime =
        new BaseUpdatePartitionSpec(formatVersion, UNPARTITIONED)
            .addField("ts_date", day("ts"))
            .apply();
    PartitionSpec removeFirstTime =
        new BaseUpdatePartitionSpec(formatVersion, addFirstTime).removeField(day("ts")).apply();
    PartitionSpec addSecondTime =
        new BaseUpdatePartitionSpec(formatVersion, removeFirstTime)
            .addField("ts_date", day("ts"))
            .apply();
    PartitionSpec removeSecondTime =
        new BaseUpdatePartitionSpec(formatVersion, addSecondTime).removeField(day("ts")).apply();
    PartitionSpec addThirdTime =
        new BaseUpdatePartitionSpec(formatVersion, removeSecondTime).addField(month("ts")).apply();
    PartitionSpec updated =
        new BaseUpdatePartitionSpec(formatVersion, addThirdTime)
            .renameField("ts_month", "ts_date")
            .apply();

    if (formatVersion == 1) {
      assertThat(updated.fields()).hasSize(3);

      assertThat(updated.fields().get(0).name()).matches("^ts_date(?:_\\d+)+$");
      assertThat(updated.fields().get(1).name()).matches("^ts_date(?:_\\d+)+$");
      assertThat(updated.fields().get(2).name()).isEqualTo("ts_date");

      assertThat(updated.fields().get(0).transform()).asString().isEqualTo("void");
      assertThat(updated.fields().get(1).transform()).asString().isEqualTo("void");
      assertThat(updated.fields().get(2).transform()).asString().isEqualTo("month");
    }

    PartitionSpec v2Expected = PartitionSpec.builderFor(SCHEMA).month("ts", "ts_date").build();

    V2Assert.assertEquals("Should match expected spec", v2Expected, updated);
  }

  @TestTemplate
  public void testRemoveAndUpdateWithDifferentTransformation() {
    PartitionSpec expected = PartitionSpec.builderFor(SCHEMA).month("ts", "ts_transformed").build();
    PartitionSpec updated =
        new BaseUpdatePartitionSpec(formatVersion, expected)
            .removeField("ts_transformed")
            .addField("ts_transformed", day("ts"))
            .apply();

    if (formatVersion == 1) {
      assertThat(updated.fields()).hasSize(2);
      assertThat(updated.fields().get(0).name()).isEqualTo("ts_transformed_1000");
      assertThat(updated.fields().get(1).name()).isEqualTo("ts_transformed");

      assertThat(updated.fields().get(0).transform()).asString().isEqualTo("void");
      assertThat(updated.fields().get(1).transform()).asString().isEqualTo("day");
    } else {
      assertThat(updated.fields()).hasSize(1);
      assertThat(updated.fields().get(0).name()).isEqualTo("ts_transformed");
      assertThat(updated.fields().get(0).transform()).asString().isEqualTo("day");
    }
  }

  private static int id(String name) {
    return SCHEMA.findField(name).fieldId();
  }
}
