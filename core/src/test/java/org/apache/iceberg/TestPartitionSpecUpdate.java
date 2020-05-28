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

  @Parameterized.Parameters
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] { 1, new int[]{ 1000, 1001, 1001, 1000, 1000 } },
        new Object[] { 2, new int[]{ 1001, 1000, 1001, 1002, 1002 } },
    };
  }

  public TestPartitionSpecUpdate(int formatVersion, int[] expectedFieldIds) {
    super(formatVersion);
    this.expectedFieldIds = expectedFieldIds;
  }

  @Test
  public void testCommitUpdatedSpec() {
    Assert.assertEquals("[\n" +
        "  1000: data_bucket: bucket[16](2)\n" +
        "]", table.spec().toString());
    Assert.assertEquals(1000, table.spec().lastAssignedFieldId());

    table.updateSpec().clear()
        .bucket("id", 8)
        .bucket("data", 16)
        .commit();

    Assert.assertEquals("[\n  " +
        expectedFieldIds[0] + ": id_bucket: bucket[8](1)\n  " +
        expectedFieldIds[1] + ": data_bucket: bucket[16](2)\n" +
        "]", table.spec().toString());
    Assert.assertEquals(expectedFieldIds[2], table.spec().lastAssignedFieldId());

    table.updateSpec().clear()
        .truncate("data", 8)
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
            .bucket("id", 8)
            .bucket("id", 16)
            .commit());
  }

  @Test
  public void testAddDuplicateFieldException() {
    AssertHelpers.assertThrows(
        "Should throw IllegalArgumentException if adding a duplicate partition field",
        IllegalArgumentException.class, "Cannot use partition name more than once: data_bucket",
        () -> table.updateSpec()
            .bucket("data", 16)
            .commit());
  }

  @Test
  public void testAddTheSamePartitionField() {
    Assert.assertEquals("[\n" +
        "  1000: data_bucket: bucket[16](2)\n" +
        "]", table.spec().toString());

    if (formatVersion == 1) {
      table.updateSpec()
          .addField(2, "data_partition", "bucket[16]")
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
              .addField(2, "data_partition", "bucket[16]")
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
        .addField(2, "data_partition", "bucket[8]")
        .bucket("id", 8)
        .commit();

    Assert.assertEquals("[\n" +
        "  1000: data_bucket: bucket[16](2)\n" +
        "  1001: data_partition: bucket[8](2)\n" +
        "  1002: id_bucket: bucket[8](1)\n" +
        "]", table.spec().toString());
    Assert.assertEquals(1002, table.spec().lastAssignedFieldId());
  }
}
