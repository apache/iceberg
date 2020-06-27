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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestTablePartitionSpecUpdate extends TableTestBase {

  @Parameterized.Parameters
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] { 1 },
        new Object[] { 2 },
    };
  }

  public TestTablePartitionSpecUpdate(int formatVersion) {
    super(formatVersion);
  }

  @Before
  public void verifyInitialSpec() {
    PartitionSpec initialSpec = PartitionSpec.builderFor(table.schema()).bucket("data", 16).build();
    Assert.assertEquals("Should use the expected initial spec", initialSpec, table.spec());
    Assert.assertEquals(1000, table.spec().lastAssignedFieldId());
    Assert.assertEquals(0, table.spec().specId());
  }

  @Test
  public void testCommitUpdatedSpec() {
    table.updateSpec()
        .addBucketField("id", 8)
        .commit();

    PartitionSpec evolvedSpec = PartitionSpec.builderFor(table.schema())
        .withSpecId(1)
        .bucket("data", 16)
        .bucket("id", 8)
        .build();
    Assert.assertEquals("Should append a partition field to the spec", evolvedSpec, table.spec());
    Assert.assertEquals(1001, table.spec().lastAssignedFieldId());

    table.updateSpec()
        .removeField("id_bucket")
        .removeField("data_bucket")
        .addTruncateField("data", 8)
        .commit();

    V1Assert.assertEquals("Should soft delete id and data buckets", PartitionSpec.builderFor(table.schema())
        .withSpecId(2)
        .alwaysNull("data", "1000__[removed]")
        .alwaysNull("id", "1001__[removed]")
        .truncate("data", 8)
        .build(), table.spec());

    V2Assert.assertEquals("Should hard delete id and data buckets", PartitionSpec.builderFor(table.schema())
        .withSpecId(2)
        .add(2, 1002, "data_trunc", "truncate[8]")
        .build(), table.spec());

    Assert.assertEquals(1002, table.spec().lastAssignedFieldId());
  }

  @Test
  public void testRenameField() {
    table.updateSpec()
        .renameField("data_bucket", "data_partition")
        .addBucketField("id", 8)
        .commit();

    PartitionSpec evolvedSpec = PartitionSpec.builderFor(table.schema())
        .withSpecId(1)
        .bucket("data", 16, "data_partition")
        .bucket("id", 8, "id_bucket")
        .build();

    Assert.assertEquals("should match evolved spec", evolvedSpec, table.spec());
    Assert.assertEquals(1001, table.spec().lastAssignedFieldId());

    table.updateSpec()
        .addTruncateField("id", 4)
        .renameField("data_partition", "data_bucket")
        .commit();

    evolvedSpec = PartitionSpec.builderFor(table.schema())
        .withSpecId(2)
        .bucket("data", 16, "data_bucket")
        .bucket("id", 8, "id_bucket")
        .truncate("id", 4)
        .build();

    Assert.assertEquals("should match evolved spec", evolvedSpec, table.spec());
    Assert.assertEquals(1002, table.spec().lastAssignedFieldId());

    table.updateSpec()
        .renameField("id_bucket", "id_partition")
        .commit();

    evolvedSpec = PartitionSpec.builderFor(table.schema())
        .withSpecId(2)
        .bucket("data", 16, "data_bucket")
        .bucket("id", 8, "id_partition")
        .truncate("id", 4)
        .build();
    Assert.assertEquals("should match evolved spec", evolvedSpec, table.spec());
    Assert.assertEquals(1002, table.spec().lastAssignedFieldId());
  }

  @Test
  public void testRemoveField() {
    table.updateSpec()
        .removeField("data_bucket")
        .addBucketField("id", 8)
        .commit();

    V1Assert.assertEquals("Should soft delete data bucket", PartitionSpec.builderFor(table.schema())
        .withSpecId(1)
        .alwaysNull("data", "1000__[removed]")
        .bucket("id", 8)
        .build(), table.spec());

    V2Assert.assertEquals("Should hard delete data bucket", PartitionSpec.builderFor(table.schema())
        .withSpecId(1)
        .add(1, 1001, "id_bucket", "bucket[8]")
        .build(), table.spec());

    Assert.assertEquals(1001, table.spec().lastAssignedFieldId());
  }

  @Test
  public void testRemoveAndAddField() {
    table.updateSpec()
        .removeField("data_bucket")
        .commit();

    Assert.assertEquals("Should hard delete data bucket for both V1 and V2", PartitionSpec.builderFor(table.schema())
        .withSpecId(1)
        .build(), table.spec());

    table.updateSpec()
        .addBucketField("data", 8)
        .commit();

    V1Assert.assertEquals("Should add a new data bucket", PartitionSpec.builderFor(table.schema())
        .withSpecId(2)
        .alwaysNull("data", "1000__[removed]")
        .bucket("data", 8)
        .build(), table.spec());
    V2Assert.assertEquals("Should add a new data bucket", PartitionSpec.builderFor(table.schema())
        .withSpecId(2)
        .add(2, 1001, "data_bucket", "bucket[8]")
        .build(), table.spec());
    Assert.assertEquals(1001, table.spec().lastAssignedFieldId());

    table.updateSpec()
        .addBucketField("data", 6)
        .removeField("data_bucket")
        .commit();

    V1Assert.assertEquals("Should evolve to a new spec", PartitionSpec.builderFor(table.schema())
        .withSpecId(3)
        .alwaysNull("data", "1000__[removed]")
        .alwaysNull("data", "1001__[removed]")
        .bucket("data", 6)
        .build(), table.spec());
    V2Assert.assertEquals("Should evolve back to the initial spec", PartitionSpec.builderFor(table.schema())
        .withSpecId(3)
        .add(2, 1002, "data_bucket", "bucket[6]")
        .build(), table.spec());
    Assert.assertEquals("Should allocate a new field id", 1002, table.spec().lastAssignedFieldId());
  }

  @Test
  public void testRenameAndRemoveField() {
    table.updateSpec()
        .renameField("data_bucket", "data_partition")
        .removeField("data_bucket")
        .addBucketField("data", 8)
        .commit();

    V1Assert.assertEquals("Should remove the renamed field", PartitionSpec.builderFor(table.schema())
        .withSpecId(1)
        .alwaysNull("data", "1000__[removed]")
        .bucket("data", 8)
        .build(), table.spec());
    V2Assert.assertEquals("Should remove the renamed field", PartitionSpec.builderFor(table.schema())
        .withSpecId(1)
        .add(2, 1001, "data_bucket", "bucket[8]")
        .build(), table.spec());

    table.updateSpec()
        .addBucketField("data", 6)
        .removeField("data_bucket")
        .commit();

    V1Assert.assertEquals("Should remove and then add a bucket field", PartitionSpec.builderFor(table.schema())
        .withSpecId(2)
        .alwaysNull("data", "1000__[removed]")
        .alwaysNull("data", "1001__[removed]")
        .bucket("data", 6)
        .build(), table.spec());
    V2Assert.assertEquals("Should remove and then add a bucket field", PartitionSpec.builderFor(table.schema())
        .withSpecId(2)
        .add(2, 1002, "data_bucket", "bucket[6]")
        .build(), table.spec());
    Assert.assertEquals(1002, table.spec().lastAssignedFieldId());
  }

  @Test
  public void testFieldIdEvolution() {
    table.updateSpec()
        .addBucketField("id", 8)
        .commit();

    PartitionSpec evolvedSpec = PartitionSpec.builderFor(table.schema())
        .withSpecId(1)
        .bucket("data", 16)
        .bucket("id", 8)
        .build();
    Assert.assertEquals("Should append a partition field to the spec", evolvedSpec, table.spec());
    Assert.assertEquals(1001, table.spec().lastAssignedFieldId());

    table.updateSpec()
        .removeField("data_bucket")
        .addBucketField("data", 8)
        .commit();

    V1Assert.assertEquals("Should add a new data bucket", PartitionSpec.builderFor(table.schema())
        .withSpecId(2)
        .alwaysNull("data", "1000__[removed]")
        .bucket("id", 8)
        .bucket("data", 8)
        .build(), table.spec());
    V2Assert.assertEquals("Should add a new data bucket", PartitionSpec.builderFor(table.schema())
        .withSpecId(2)
        .add(1, 1001, "id_bucket", "bucket[8]")
        .add(2, 1002, "data_bucket", "bucket[8]")
        .build(), table.spec());
    Assert.assertEquals(1002, table.spec().lastAssignedFieldId());

    table.updateSpec()
        .removeField("data_bucket")
        .addBucketField("data", 16, "data_partition")
        .commit();

    Assert.assertEquals(
        "Should add back a removed data bucket, truncate the trailing removed fields, and reuse the spec id",
        PartitionSpec.builderFor(table.schema())
            .withSpecId(1)
            .bucket("data", 16, "data_partition")
            .bucket("id", 8)
            .build(),
        table.spec());
    Assert.assertEquals("Should reuse the field id", 1001, table.spec().lastAssignedFieldId());

    table.updateSpec()
        .removeField("id_bucket")
        .renameField("data_partition", "data_bucket")
        .commit();

    Assert.assertEquals(
        "Should rename an existing data bucket, truncate the trailing removed fields, and reuse the spec id",
        PartitionSpec.builderFor(table.schema())
            .withSpecId(0)
            .add(2, 1000, "data_bucket", "bucket[16]")
            .build(),
        table.spec());
    Assert.assertEquals("Should not add a new field id", 1000, table.spec().lastAssignedFieldId());

    table.updateSpec()
        .addTruncateField("id", 6)
        .commit();

    V1Assert.assertEquals("Should add a new data bucket and fill the gaps", PartitionSpec.builderFor(table.schema())
        .withSpecId(3)
        .add(2, 1000, "data_bucket", "bucket[16]")
        .add(1, 1001, "1001__[removed]", "void")
        .add(2, 1002, "1002__[removed]", "void")
        .add(1, 1003, "id_trunc", "truncate[6]")
        .build(), table.spec());
    V2Assert.assertEquals("Should add a new data bucket", PartitionSpec.builderFor(table.schema())
        .withSpecId(3)
        .add(2, 1000, "data_bucket", "bucket[16]")
        .add(1, 1003, "id_trunc", "truncate[6]")
        .build(), table.spec());
    Assert.assertEquals(1003, table.spec().lastAssignedFieldId());
  }
}
