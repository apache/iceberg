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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestPartitionSpecUpdate {

  private static final Schema SCHEMA = new Schema(
      required(1, "id", Types.IntegerType.get()),
      required(2, "data", Types.StringType.get()),
      optional(3, "ts", Types.TimestampType.withZone())
  );

  List<PartitionSpec> initialSpecs = Collections.singletonList(
      PartitionSpec.builderFor(SCHEMA).bucket("data", 16).build());
  PartitionSpecUpdate specUpdate;

  @Test
  public void testAddAndRemoveField() {
    specUpdate = new PartitionSpecUpdate(initialSpecs);
    Assert.assertEquals(
        PartitionSpec.builderFor(SCHEMA)
            .add(2, 1001, "data_bucket", "bucket[8]")
            .build(),
        specUpdate
            .removeField("data_bucket")
            .addBucketField("data", 8)
            .apply());

    specUpdate = new PartitionSpecUpdate(initialSpecs);
    Assert.assertEquals(
        PartitionSpec.builderFor(SCHEMA)
            .add(2, 1001, "data_bucket", "bucket[8]")
            .build(),
        specUpdate
            .addBucketField("data", 8)
            .removeField("data_bucket")
            .apply());
  }

  @Test
  public void testAddRemovedSamePartitionField() {
    specUpdate = new PartitionSpecUpdate(initialSpecs);
    Assert.assertEquals(
        "remove and add operations should cancel each other, equivalent to noop",
        PartitionSpec.builderFor(SCHEMA)
            .add(2, 1000, "data_bucket", "bucket[16]")
            .add(1, 1001, "id_bucket", "bucket[8]")
            .build(),
        specUpdate
            .removeField("data_bucket")
            .addBucketField("id", 8)
            .addBucketField("data", 16)
            .apply());

    specUpdate = new PartitionSpecUpdate(initialSpecs);
    Assert.assertEquals(
        "remove and add operations should cancel each other, equivalent to noop",
        PartitionSpec.builderFor(SCHEMA)
            .add(2, 1000, "data_bucket", "bucket[16]")
            .add(1, 1001, "id_bucket", "bucket[8]")
            .build(),
        specUpdate
            .addBucketField("id", 8)
            .addBucketField("data", 16)
            .removeField("data_bucket")
            .apply());

    specUpdate = new PartitionSpecUpdate(initialSpecs);
    Assert.assertEquals(
        "remove and add operations are equivalent to rename data_bucket to data_partition",
        PartitionSpec.builderFor(SCHEMA)
            .add(2, 1000, "data_partition", "bucket[16]")
            .build(),
        specUpdate
            .addBucketField("data", 16, "data_partition")
            .removeField("data_bucket")
            .apply());
  }

  @Test
  public void testUpdateException() {
    specUpdate = new PartitionSpecUpdate(initialSpecs);
    AssertHelpers.assertThrows(
        "Should throw IllegalArgumentException if there is an invalid partition field",
        IllegalArgumentException.class, "Cannot find an existing partition field with the name: id_bucket",
        () -> specUpdate
            .removeField("id_bucket")
            .apply());

    specUpdate = new PartitionSpecUpdate(initialSpecs);
    AssertHelpers.assertThrows(
        "Should throw IllegalArgumentException if there is a redundant partition field",
        IllegalArgumentException.class, "Cannot add redundant partition",
        () -> specUpdate
            .removeField("data_bucket")
            .addBucketField("id", 8)
            .addBucketField("id", 16)
            .apply());
  }

  @Test
  public void testAddDuplicateFieldException() {
    specUpdate = new PartitionSpecUpdate(initialSpecs);
    AssertHelpers.assertThrows(
        "Should throw IllegalArgumentException if adding a duplicate partition field",
        IllegalArgumentException.class, "Cannot use partition name more than once: data_bucket",
        () -> specUpdate
            .addTruncateField("data", 16, "data_bucket")
            .apply());

    specUpdate = new PartitionSpecUpdate(initialSpecs);
    AssertHelpers.assertThrows(
        "Should throw IllegalArgumentException if adding a duplicate partition field",
        IllegalArgumentException.class, "Cannot add redundant partition",
        () -> specUpdate
            .addBucketField("data", 16)
            .apply());

    specUpdate = new PartitionSpecUpdate(initialSpecs);
    AssertHelpers.assertThrows(
        "Should throw IllegalArgumentException if adding a redundant partition field",
        IllegalArgumentException.class,
        "Cannot add redundant partition:",
        () -> specUpdate
            .addBucketField("data", 16, "data_partition")
            .apply());

    specUpdate = new PartitionSpecUpdate(initialSpecs);
    AssertHelpers.assertThrows(
        "Should throw IllegalArgumentException if adding a redundant partition field",
        IllegalArgumentException.class, "Cannot add redundant partition",
        () -> specUpdate
            .addBucketField("data", 8, "data_partition")
            .apply());

    specUpdate = new PartitionSpecUpdate(initialSpecs);
    AssertHelpers.assertThrows(
        "Should throw IllegalArgumentException if adding a redundant partition field",
        IllegalArgumentException.class, "Cannot add redundant partition",
        () -> specUpdate
            .addBucketField("id", 8, "id_partition1")
            .addBucketField("id", 16, "id_partition2")
            .apply());
  }

  @Test
  public void testAddField() {
    specUpdate = new PartitionSpecUpdate(initialSpecs);
    PartitionSpec newSpec = specUpdate
        .addBucketField("id", 8)
        .addTruncateField("data", 8)
        .apply();

    PartitionSpec evolvedSpec = PartitionSpec.builderFor(SCHEMA)
        .bucket("data", 16)
        .bucket("id", 8)
        .truncate("data", 8)
        .build();

    Assert.assertEquals("should match evolved spec", evolvedSpec, newSpec);
    Assert.assertEquals(1002, newSpec.lastAssignedFieldId());

    specUpdate = new PartitionSpecUpdate(
        Arrays.asList(
            PartitionSpec.builderFor(SCHEMA).bucket("data", 16).bucket("id", 8).build(),
            PartitionSpec.builderFor(SCHEMA).bucket("data", 16).build()));

    Assert.assertEquals("Should not reuse field id for a new field",
        PartitionSpec.builderFor(SCHEMA)
            .add(2, 1000, "data_bucket", "bucket[16]")
            .add(2, 1002, "data", "identity")
            .build(),
        specUpdate
            .addIdentityField("data")
            .apply());
  }

  @Test
  public void testRenameField() {
    specUpdate = new PartitionSpecUpdate(initialSpecs);
    PartitionSpec newSpec = specUpdate
        .addBucketField("id", 8)
        .renameField("data_bucket", "data_partition")
        .apply();

    PartitionSpec evolvedSpec = PartitionSpec.builderFor(SCHEMA)
        .bucket("data", 16, "data_partition")
        .bucket("id", 8, "id_bucket")
        .build();

    Assert.assertEquals("should match evolved spec", evolvedSpec, newSpec);
    Assert.assertEquals(1001, newSpec.lastAssignedFieldId());
  }

  @Test
  public void testRenameFieldExceptions() {
    specUpdate = new PartitionSpecUpdate(initialSpecs);
    AssertHelpers.assertThrows(
        "Should throw IllegalArgumentException if renaming a non-existing partition field",
        IllegalArgumentException.class,
        "Cannot find an existing partition field with the name: not_existing",
        () -> specUpdate
            .renameField("not_existing", "data_partition")
            .apply());

    specUpdate = new PartitionSpecUpdate(initialSpecs);
    AssertHelpers.assertThrows(
        "Should throw IllegalArgumentException if renaming a partition field to null",
        IllegalArgumentException.class,
        "Cannot use an empty or null partition name: null",
        () -> specUpdate
            .renameField("data_bucket", null)
            .apply());

    specUpdate = new PartitionSpecUpdate(initialSpecs);
    AssertHelpers.assertThrows(
        "Should throw IllegalArgumentException if renaming a not committed new partition field",
        IllegalArgumentException.class,
        "Cannot find an existing partition field with the name: not_committed",
        () -> specUpdate
            .addBucketField("data", 6, "not_committed")
            .renameField("not_committed", "data_partition")
            .apply());

    specUpdate = new PartitionSpecUpdate(initialSpecs);
    AssertHelpers.assertThrows(
        "Should throw IllegalArgumentException if renaming a removed field",
        IllegalArgumentException.class,
        "Cannot find an existing partition field with the name: data_bucket",
        () -> specUpdate
            .removeField("data_bucket")
            .renameField("data_bucket", "data_partition")
            .apply());

    specUpdate = new PartitionSpecUpdate(
        Arrays.asList(
            PartitionSpec.builderFor(SCHEMA).bucket("data", 16).build(),
            PartitionSpec.builderFor(SCHEMA).bucket("data", 16).bucket("id", 8).build()));

    AssertHelpers.assertThrows(
        "Should throw IllegalArgumentException if renaming a field to the same name as another field",
        IllegalArgumentException.class,
        "Cannot use partition name more than once",
        () -> specUpdate
            .renameField("data_bucket", "id_bucket")
            .apply());
  }

  @Test
  public void testRemoveFieldException() {
    specUpdate = new PartitionSpecUpdate(initialSpecs);
    AssertHelpers.assertThrows(
        "Should throw IllegalStateException if removing a non-existing partition field",
        IllegalArgumentException.class,
        "Cannot find an existing partition field with the name: not_existing",
        () -> specUpdate
            .removeField("not_existing")
            .apply());

    specUpdate = new PartitionSpecUpdate(initialSpecs);
    AssertHelpers.assertThrows(
        "Should throw IllegalStateException if removing a not committed new partition field",
        IllegalArgumentException.class,
        "Cannot find an existing partition field with the name: not_committed",
        () -> specUpdate
            .addBucketField("id", 6, "not_committed")
            .removeField("not_committed")
            .apply());

    specUpdate = new PartitionSpecUpdate(initialSpecs);
    AssertHelpers.assertThrows(
        "Should throw IllegalStateException if removing a partition field more than once",
        IllegalArgumentException.class,
        "Cannot find an existing partition field with the name: data_bucket",
        () -> specUpdate
            .removeField("data_bucket")
            .removeField("data_bucket")
            .apply());
  }
}
