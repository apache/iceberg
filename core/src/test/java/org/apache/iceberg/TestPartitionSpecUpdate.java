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

import org.apache.iceberg.exceptions.ValidationException;
import org.junit.Assert;
import org.junit.Test;

public class TestPartitionSpecUpdate extends TableTestBase {

  @Test
  public void testCommitUpdatedSpec() {
    Assert.assertEquals("[\n" +
        "  1000: data_bucket: bucket[16](2)\n" +
        "]", table.spec().toString());
    Assert.assertEquals(1000, table.spec().lastAssignedFieldId());

    PartitionSpec spec = PartitionSpec.builderFor(table.schema())
        .bucket("id", 8)
        .bucket("data", 16)
        .build();
    table.updatePartitionSpec().update(spec).commit();

    Assert.assertEquals("[\n" +
        "  1001: id_bucket: bucket[8](1)\n" +
        "  1000: data_bucket: bucket[16](2)\n" +
        "]", table.spec().toString());
    Assert.assertEquals(1001, table.spec().lastAssignedFieldId());

    spec = PartitionSpec.builderFor(table.schema())
        .truncate("data", 8)
        .build();
    table.updatePartitionSpec().update(spec).commit();

    Assert.assertEquals("[\n" +
        "  1002: data_trunc: truncate[8](2)\n" +
        "]", table.spec().toString());
    Assert.assertEquals(1002, table.spec().lastAssignedFieldId());
  }

  @Test
  public void testCommitException() {
    AssertHelpers.assertThrows("Should throw NullPointerException if no spec to commit",
        NullPointerException.class, "new spec is not set",
        () -> table.updatePartitionSpec().commit());
  }

  @Test
  public void testUpdateCompatibility() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA)
        .bucket("id", 8)
        .bucket("data", 16)
        .build();

    AssertHelpers.assertThrows(
        "Should throw ValidationException if the new spec is not compatible with the table schema",
        ValidationException.class, "Cannot find source column for partition field",
        () -> table.updatePartitionSpec().update(spec));
  }
}
