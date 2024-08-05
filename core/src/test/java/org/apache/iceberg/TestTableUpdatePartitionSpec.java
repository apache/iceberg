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
import static org.apache.iceberg.expressions.Expressions.truncate;
import static org.apache.iceberg.expressions.Expressions.year;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestTableUpdatePartitionSpec extends TestBase {

  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(1, 2, 3);
  }

  @BeforeEach
  public void verifyInitialSpec() {
    PartitionSpec initialSpec = PartitionSpec.builderFor(table.schema()).bucket("data", 16).build();
    assertThat(table.spec()).isEqualTo(initialSpec);
    assertThat(table.spec().lastAssignedFieldId()).isEqualTo(1000);
    assertThat(table.spec().specId()).isEqualTo(0);
  }

  @TestTemplate
  public void testCommitUpdatedSpec() {
    table.updateSpec().addField(bucket("id", 8)).commit();

    PartitionSpec evolvedSpec =
        PartitionSpec.builderFor(table.schema())
            .withSpecId(1)
            .bucket("data", 16)
            .bucket("id", 8, "id_bucket_8")
            .build();
    assertThat(table.spec())
        .as("Should append a partition field to the spec")
        .isEqualTo(evolvedSpec);
    assertThat(table.spec().lastAssignedFieldId()).isEqualTo(1001);

    table
        .updateSpec()
        .removeField("id_bucket_8")
        .removeField("data_bucket")
        .addField(truncate("data", 8))
        .commit();

    V1Assert.assertEquals(
        "Should soft delete id and data buckets",
        PartitionSpec.builderFor(table.schema())
            .withSpecId(2)
            .alwaysNull("data", "data_bucket")
            .alwaysNull("id", "id_bucket_8")
            .truncate("data", 8, "data_trunc_8")
            .build(),
        table.spec());

    V2Assert.assertEquals(
        "Should hard delete id and data buckets",
        PartitionSpec.builderFor(table.schema())
            .withSpecId(2)
            .add(2, 1002, "data_trunc_8", Transforms.truncate(8))
            .build(),
        table.spec());

    assertThat(table.spec().lastAssignedFieldId()).isEqualTo(1002);
  }

  @TestTemplate
  public void testCommitFromSpec() {
    table.updateSpec().addField(bucket("id", 8)).commit();

    PartitionSpec evolvedSpec =
        PartitionSpec.builderFor(table.schema())
            .withSpecId(1)
            .bucket("data", 16)
            .bucket("id", 8, "id_bucket_8")
            .build();
    assertThat(table.spec())
        .as("Should append a partition field to the spec")
        .isEqualTo(evolvedSpec);
    assertThat(table.spec().lastAssignedFieldId()).isEqualTo(1001);

    // Restart the spec
    table
        .updateSpec()
        .fromSpec(PartitionSpec.builderFor(table.schema()).build())
        .addField(bucket("id", 8))
        .commit();

    V1Assert.assertEquals(
        "Should soft delete data bucket",
        PartitionSpec.builderFor(table.schema())
            .withSpecId(2)
            .alwaysNull("data", "data_bucket")
            .bucket("id", 8, "id_bucket_8")
            .build(),
        table.spec());

    V2Assert.assertEquals(
        "Should hard delete data bucket",
        PartitionSpec.builderFor(table.schema())
            .withSpecId(2)
            .add(1, 1001, "id_bucket_8", Transforms.bucket(8))
            .build(),
        table.spec());
  }

  @TestTemplate
  public void testNoopCommit() {
    TableMetadata current = table.ops().current();
    int currentVersion = TestTables.metadataVersion("test");

    // no-op commit due to no-op
    table.updateSpec().commit();
    TableMetadata updated = table.ops().current();
    Integer updatedVersion = TestTables.metadataVersion("test");
    assertThat(updated).isEqualTo(current);
    currentVersion += 1;
    assertThat(updatedVersion).isEqualTo(currentVersion);

    // no-op commit due to no-op rename
    table.updateSpec().renameField("data_bucket", "data_bucket").commit();
    updated = table.ops().current();
    updatedVersion = TestTables.metadataVersion("test");
    assertThat(updated).isEqualTo(current);
    currentVersion += 1;
    assertThat(updatedVersion).isEqualTo(currentVersion);
  }

  @TestTemplate
  public void testRenameField() {
    table
        .updateSpec()
        .renameField("data_bucket", "data_partition")
        .addField(bucket("id", 8))
        .commit();

    PartitionSpec evolvedSpec =
        PartitionSpec.builderFor(table.schema())
            .withSpecId(1)
            .bucket("data", 16, "data_partition")
            .bucket("id", 8, "id_bucket_8")
            .build();

    assertThat(table.spec()).isEqualTo(evolvedSpec);
    assertThat(table.spec().lastAssignedFieldId()).isEqualTo(1001);

    table
        .updateSpec()
        .addField(truncate("id", 4))
        .renameField("data_partition", "data_bucket")
        .commit();

    evolvedSpec =
        PartitionSpec.builderFor(table.schema())
            .withSpecId(2)
            .bucket("data", 16, "data_bucket")
            .bucket("id", 8, "id_bucket_8")
            .truncate("id", 4, "id_trunc_4")
            .build();

    assertThat(table.spec()).isEqualTo(evolvedSpec);
    assertThat(table.spec().lastAssignedFieldId()).isEqualTo(1002);
  }

  @TestTemplate
  public void testRenameOnlyEvolution() {
    table.updateSpec().renameField("data_bucket", "data_partition").commit();

    PartitionSpec evolvedSpec =
        PartitionSpec.builderFor(table.schema())
            .withSpecId(1)
            .bucket("data", 16, "data_partition")
            .build();

    assertThat(table.spec()).isEqualTo(evolvedSpec);
    assertThat(table.spec().lastAssignedFieldId()).isEqualTo(1000);
  }

  @TestTemplate
  public void testRemoveAndAddField() {
    table.updateSpec().removeField("data_bucket").addField(bucket("id", 8)).commit();

    V1Assert.assertEquals(
        "Should soft delete data bucket",
        PartitionSpec.builderFor(table.schema())
            .withSpecId(1)
            .alwaysNull("data", "data_bucket")
            .bucket("id", 8, "id_bucket_8")
            .build(),
        table.spec());

    V2Assert.assertEquals(
        "Should hard delete data bucket",
        PartitionSpec.builderFor(table.schema())
            .withSpecId(1)
            .add(1, 1001, "id_bucket_8", Transforms.bucket(8))
            .build(),
        table.spec());

    assertThat(table.spec().lastAssignedFieldId()).isEqualTo(1001);
  }

  @TestTemplate
  public void testRemoveAndAddYearField() {
    table.updateSchema().addColumn("year_field", Types.DateType.get()).commit();
    table.updateSpec().addField(year("year_field")).commit();

    PartitionSpec evolvedSpec =
        PartitionSpec.builderFor(table.schema())
            .withSpecId(1)
            .bucket("data", 16)
            .year("year_field")
            .build();

    assertThat(table.spec()).isEqualTo(evolvedSpec);
    assertThat(table.spec().lastAssignedFieldId()).isEqualTo(1001);

    table.updateSpec().removeField("year_field_year").addField(year("year_field")).commit();

    V1Assert.assertEquals(
        "Should soft delete id and data buckets",
        PartitionSpec.builderFor(table.schema())
            .withSpecId(1)
            .bucket("data", 16)
            .year("year_field")
            .build(),
        table.spec());

    V2Assert.assertEquals(
        "Should remove and then add a year field",
        PartitionSpec.builderFor(table.schema())
            .withSpecId(1)
            .bucket("data", 16)
            .add(3, 1001, "year_field_year", Transforms.year())
            .build(),
        table.spec());

    assertThat(table.spec().lastAssignedFieldId()).isEqualTo(1001);
  }

  @TestTemplate
  public void testAddAndRemoveField() {
    table.updateSpec().addField(bucket("data", 6)).removeField("data_bucket").commit();

    V1Assert.assertEquals(
        "Should remove and then add a bucket field",
        PartitionSpec.builderFor(table.schema())
            .withSpecId(1)
            .alwaysNull("data", "data_bucket")
            .bucket("data", 6, "data_bucket_6")
            .build(),
        table.spec());
    V2Assert.assertEquals(
        "Should remove and then add a bucket field",
        PartitionSpec.builderFor(table.schema())
            .withSpecId(1)
            .add(2, 1001, "data_bucket_6", Transforms.bucket(6))
            .build(),
        table.spec());
    assertThat(table.spec().lastAssignedFieldId()).isEqualTo(1001);
  }

  @TestTemplate
  public void testReAddField() {
    table.updateSpec().removeField("data_bucket").commit();
    table.updateSpec().addField(bucket("data", 16)).commit();

    V1Assert.assertEquals(
        "Should remove and then add a bucket field",
        PartitionSpec.builderFor(table.schema())
            .withSpecId(2)
            .alwaysNull("data", "data_bucket")
            .bucket("data", 16, "data_bucket_16")
            .build(),
        table.spec());
    V1Assert.assertEquals("Should assign a new field id", 1001, table.spec().lastAssignedFieldId());

    V2Assert.assertEquals(
        "Should remove and then add a bucket field",
        PartitionSpec.builderFor(table.schema())
            .withSpecId(0)
            .add(2, 1000, "data_bucket", Transforms.bucket(16))
            .build(),
        table.spec());
    V2Assert.assertEquals(
        "Should not assign a new field id", 1000, table.spec().lastAssignedFieldId());
  }

  @TestTemplate
  public void testReAddFieldUsingUnpartitioned() {
    table.updateSpec().removeField("data_bucket").commit();
    table
        .updateSpec()
        .fromSpec(PartitionSpec.builderFor(table.schema()).build())
        .addField("id")
        .commit();

    V1Assert.assertEquals(
        "Should remove data field and add identity id field",
        PartitionSpec.builderFor(table.schema())
            .withSpecId(2)
            .alwaysNull("data", "data_bucket")
            .identity("id")
            .build(),
        table.spec());
    V1Assert.assertEquals("Should assign a new field id", 1001, table.spec().lastAssignedFieldId());

    V2Assert.assertEquals(
        "Should remove data field and add identity id field",
        PartitionSpec.builderFor(table.schema())
            .withSpecId(2)
            .add(1, 1001, "id", Transforms.identity())
            .build(),
        table.spec());
    V2Assert.assertEquals(
        "Should not assign a new field id", 1001, table.spec().lastAssignedFieldId());

    table
        .updateSpec()
        .fromSpec(PartitionSpec.builderFor(table.schema()).build())
        .commit();

    V1Assert.assertEquals(
        "Should remove data and id field",
        PartitionSpec.builderFor(table.schema())
            .withSpecId(3)
            .alwaysNull("data", "data_bucket")
            .alwaysNull("id", "id")
            .build(),
        table.spec());
    V1Assert.assertEquals(
        "Should not assign a new field id", 1001, table.spec().lastAssignedFieldId());

    V2Assert.assertEquals(
        "Should remove data and id field",
        PartitionSpec.builderFor(table.schema()).withSpecId(1).build(),
        table.spec());
    V2Assert.assertEquals(
        "Should not assign a new field id", 999, table.spec().lastAssignedFieldId());

    table
        .updateSpec()
        .fromSpec(PartitionSpec.builderFor(table.schema()).build())
        .addField("id")
        .commit();

    V1Assert.assertEquals(
        "Should re-add id field",
        PartitionSpec.builderFor(table.schema())
            .withSpecId(4)
            .alwaysNull("data", "data_bucket")
            .alwaysNull("id", "id_1001")
            .identity("id")
            .build(),
        table.spec());
    V1Assert.assertEquals("Should assign a new field id", 1002, table.spec().lastAssignedFieldId());

    V2Assert.assertEquals(
        "Should re-add id field",
        PartitionSpec.builderFor(table.schema())
            .withSpecId(2)
            .add(1, 1001, "id", Transforms.identity())
            .build(),
        table.spec());
    V2Assert.assertEquals(
        "Should not assign a new field id", 1001, table.spec().lastAssignedFieldId());
  }

  @TestTemplate
  public void testAddAfterLastFieldRemoved() {
    table.updateSpec().removeField("data_bucket").commit();

    V1Assert.assertEquals(
        "Should add a new id bucket",
        PartitionSpec.builderFor(table.schema())
            .withSpecId(1)
            .alwaysNull("data", "data_bucket")
            .build(),
        table.spec());
    V1Assert.assertEquals(
        "Should match the last assigned field id", 1000, table.spec().lastAssignedFieldId());
    V2Assert.assertEquals(
        "Should add a new id bucket",
        PartitionSpec.builderFor(table.schema()).withSpecId(1).build(),
        table.spec());
    V2Assert.assertEquals(
        "Should match the last assigned field id", 999, table.spec().lastAssignedFieldId());
    assertThat(table.ops().current().lastAssignedPartitionId()).isEqualTo(1000);

    table.updateSpec().addField(bucket("id", 8)).commit();

    V1Assert.assertEquals(
        "Should add a new id bucket",
        PartitionSpec.builderFor(table.schema())
            .withSpecId(2)
            .alwaysNull("data", "data_bucket")
            .bucket("id", 8, "id_bucket_8")
            .build(),
        table.spec());
    V2Assert.assertEquals(
        "Should add a new id bucket",
        PartitionSpec.builderFor(table.schema())
            .withSpecId(2)
            .add(1, 1001, "id_bucket_8", Transforms.bucket(8))
            .build(),
        table.spec());
    assertThat(table.spec().lastAssignedFieldId()).isEqualTo(1001);
    assertThat(table.ops().current().lastAssignedPartitionId()).isEqualTo(1001);
  }

  @TestTemplate
  public void testCommitUpdatedSpecWithoutSettingNewDefault() {
    PartitionSpec originalSpec = table.spec();
    table.updateSpec().addField("id").addNonDefaultSpec().commit();

    assertThat(table.spec())
        .as("Should not set the default spec for the table")
        .isSameAs(originalSpec);

    assertThat(table.specs().get(1))
        .as("The new spec created for the table")
        .isEqualTo(
            PartitionSpec.builderFor(table.schema())
                .withSpecId(1)
                .bucket("data", 16)
                .identity("id")
                .build());
  }
}
