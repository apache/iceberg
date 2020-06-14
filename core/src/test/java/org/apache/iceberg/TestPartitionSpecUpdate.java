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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestPartitionSpecUpdate extends TableTestBase {

  private int[] expectedFieldIds;

  private String[] expectedSpecs;

  @Parameterized.Parameters
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] { 1, new int[]{ 1000, 1001, 1001, 1000, 1000 },
            new String[]{"  1000: data_bucket_removed: void(2)\n", "  1002: id_bucket_removed: void(1)\n"} },
        new Object[] { 2, new int[]{ 1001, 1000, 1001, 1002, 1002 }, new String[]{"", ""} },
    };
  }

  public TestPartitionSpecUpdate(int formatVersion, int[] expectedFieldIds, String[] expectedSpecs) {
    super(formatVersion);
    this.expectedFieldIds = expectedFieldIds;
    this.expectedSpecs = expectedSpecs;
  }

  @Test
  public void testCommitUpdatedSpec() {
    Assert.assertEquals("[\n" +
        "  1000: data_bucket: bucket[16](2)\n" +
        "]", table.spec().toString());
    Assert.assertEquals(1000, table.spec().lastAssignedFieldId());

    table.updateSpec().clear()
        .addBucketField("id", 8)
        .addBucketField("data", 16)
        .commit();

    Assert.assertEquals("[\n  " +
        expectedFieldIds[0] + ": id_bucket: bucket[8](1)\n  " +
        expectedFieldIds[1] + ": data_bucket: bucket[16](2)\n" +
        "]", table.spec().toString());
    Assert.assertEquals(expectedFieldIds[2], table.spec().lastAssignedFieldId());

    table.updateSpec().clear()
        .addTruncateField("data", 8)
        .commit();

    Assert.assertEquals("[\n  " +
        expectedFieldIds[3] + ": data_trunc: truncate[8](2)\n" +
        "]", table.spec().toString());
    Assert.assertEquals(expectedFieldIds[4], table.spec().lastAssignedFieldId());
  }

  @Test
  public void testUpdateException() {
    AssertHelpers.assertThrows(
        "Should throw IllegalArgumentException if there is an invalid partition field",
        IllegalArgumentException.class, "Cannot use partition name more than once: id_bucket",
        () -> table.updateSpec().clear()
            .addBucketField("id", 8)
            .addBucketField("id", 16)
            .commit());
  }

  @Test
  public void testAddDuplicateFieldException() {
    AssertHelpers.assertThrows(
        "Should throw IllegalArgumentException if adding a duplicate partition field",
        IllegalArgumentException.class, "Cannot use partition name more than once: data_bucket",
        () -> table.updateSpec()
            .addBucketField("data", 16)
            .commit());
  }

  @Test
  public void testAddSamePartitionField() {
    Assert.assertEquals("[\n" +
        "  1000: data_bucket: bucket[16](2)\n" +
        "]", table.spec().toString());

    if (formatVersion == 1) {
      table.updateSpec()
          .addBucketField("data", 16, "data_partition")
          .commit();
      Assert.assertEquals("[\n" +
          "  1000: data_bucket: bucket[16](2)\n" +
          "  1001: data_partition: bucket[16](2)\n" +
          "]", table.spec().toString());
    } else {
      AssertHelpers.assertThrows(
          "Should throw IllegalArgumentException if adding a duplicate partition field",
          IllegalArgumentException.class,
          "Field Id 1000 has already been used in the existing partition fields",
          () -> table.updateSpec()
              .addBucketField("data", 16, "data_partition")
              .commit());
    }
  }

  @Test
  public void testAddField() {
    Assert.assertEquals("[\n" +
        "  1000: data_bucket: bucket[16](2)\n" +
        "]", table.spec().toString());
    Assert.assertEquals(1000, table.spec().lastAssignedFieldId());

    table.updateSpec()
        .addBucketField("data", 8, "data_partition")
        .addBucketField("id", 8)
        .addBucketField("data", 6, "data_field")
        .commit();

    Assert.assertEquals("[\n" +
        "  1000: data_bucket: bucket[16](2)\n" +
        "  1001: data_partition: bucket[8](2)\n" +
        "  1002: id_bucket: bucket[8](1)\n" +
        "  1003: data_field: bucket[6](2)\n" +
        "]", table.spec().toString());
    Assert.assertEquals(1003, table.spec().lastAssignedFieldId());
  }

  @Test
  public void testRenameField() {
    Assert.assertEquals("[\n" +
        "  1000: data_bucket: bucket[16](2)\n" +
        "]", table.spec().toString());
    Assert.assertEquals(1000, table.spec().lastAssignedFieldId());
    Assert.assertEquals(0, table.spec().specId());

    table.updateSpec()
        .renameField("data_bucket", "data_partition")
        .addBucketField("id", 8)
        .commit();

    Assert.assertEquals("[\n" +
        "  1000: data_partition: bucket[16](2)\n" +
        "  1001: id_bucket: bucket[8](1)\n" +
        "]", table.spec().toString());
    Assert.assertEquals(1001, table.spec().lastAssignedFieldId());
    Assert.assertEquals(1, table.spec().specId());
  }

  @Test
  public void testRenameFieldExceptions() {
    Assert.assertEquals("[\n" +
        "  1000: data_bucket: bucket[16](2)\n" +
        "]", table.spec().toString());
    Assert.assertEquals(1000, table.spec().lastAssignedFieldId());
    Assert.assertEquals(0, table.spec().specId());

    AssertHelpers.assertThrows(
        "Should throw IllegalArgumentException if only renaming a partition field",
        IllegalArgumentException.class,
        "Cannot set default partition spec to the current default",
        () -> table.updateSpec()
            .renameField("data_bucket", "data_partition")
            .commit());

    AssertHelpers.assertThrows(
        "Should throw IllegalArgumentException if renaming a non-existing partition field",
        IllegalArgumentException.class,
        "Cannot find an existing partition field with the name: not_existing",
        () -> table.updateSpec()
            .renameField("not_existing", "data_partition")
            .commit());

    AssertHelpers.assertThrows(
        "Should throw IllegalArgumentException if renaming a partition field to null",
        IllegalArgumentException.class,
        "Cannot use empty or null partition name: null",
        () -> table.updateSpec()
            .renameField("data_bucket", null)
            .commit());

    AssertHelpers.assertThrows(
        "Should throw IllegalArgumentException if renaming a not committed new partition field",
        IllegalArgumentException.class,
        "Cannot find an existing partition field with the name: not_committed",
        () -> table.updateSpec()
            .addBucketField("data", 6, "not_committed")
            .renameField("not_committed", "data_partition")
            .commit());
  }

  @Test
  public void testRemoveField() {
    Assert.assertEquals("[\n" +
        "  1000: data_bucket: bucket[16](2)\n" +
        "]", table.spec().toString());
    Assert.assertEquals(1000, table.spec().lastAssignedFieldId());
    Assert.assertEquals(0, table.spec().specId());

    table.updateSpec()
        .removeField("data_bucket")
        .addBucketField("id", 8)
        .commit();

    Assert.assertEquals("[\n" +
        this.expectedSpecs[0] +
        "  1001: id_bucket: bucket[8](1)\n" +
        "]", table.spec().toString());
    Assert.assertEquals(1001, table.spec().lastAssignedFieldId());
    Assert.assertEquals(1, table.spec().specId());
  }

  @Test
  public void testRemoveFieldException() {
    Assert.assertEquals("[\n" +
        "  1000: data_bucket: bucket[16](2)\n" +
        "]", table.spec().toString());
    Assert.assertEquals(1000, table.spec().lastAssignedFieldId());
    Assert.assertEquals(0, table.spec().specId());

    AssertHelpers.assertThrows(
        "Should throw IllegalStateException if removing a non-existing partition field",
        IllegalArgumentException.class,
        "Cannot find an existing partition field with the name: not_existing",
        () -> table.updateSpec()
            .removeField("not_existing")
            .commit());

    AssertHelpers.assertThrows(
        "Should throw IllegalStateException if removing a not committed new partition field",
        IllegalArgumentException.class,
        "Cannot find an existing partition field with the name: not_committed",
        () -> table.updateSpec()
            .addBucketField("data", 6, "not_committed")
            .removeField("not_committed")
            .commit());
  }

}
