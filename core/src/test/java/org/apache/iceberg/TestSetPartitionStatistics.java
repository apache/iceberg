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
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestSetPartitionStatistics extends TableTestBase {
  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] {1, 2};
  }

  public TestSetPartitionStatistics(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testEmptyUpdateStatistics() {
    assertTableMetadataVersion(0);
    TableMetadata base = readMetadata();

    table.updatePartitionStatistics().commit();

    Assert.assertSame(
        "Base metadata should not change when commit is created", base, table.ops().current());
    assertTableMetadataVersion(1);
  }

  @Test
  public void testEmptyTransactionalUpdateStatistics() {
    assertTableMetadataVersion(0);
    TableMetadata base = readMetadata();

    Transaction transaction = table.newTransaction();
    transaction.updatePartitionStatistics().commit();
    transaction.commitTransaction();

    Assert.assertSame(
        "Base metadata should not change when commit is created", base, table.ops().current());
    assertTableMetadataVersion(0);
  }

  @Test
  public void testUpdateStatistics() {
    // Create a snapshot
    table.newFastAppend().commit();
    assertTableMetadataVersion(1);

    TableMetadata base = readMetadata();
    long snapshotId = base.currentSnapshot().snapshotId();
    PartitionStatisticsFile partitionStatisticsFile =
        ImmutableGenericPartitionStatisticsFile.builder()
            .snapshotId(snapshotId)
            .path("/some/partition/statistics/file.parquet")
            .fileSizeInBytes(42L)
            .build();

    table.updatePartitionStatistics().setPartitionStatistics(partitionStatisticsFile).commit();

    TableMetadata metadata = readMetadata();
    assertTableMetadataVersion(2);
    Assert.assertEquals(
        "Table snapshot should be the same after setting partition statistics file",
        snapshotId,
        metadata.currentSnapshot().snapshotId());
    Assert.assertEquals(
        "Table metadata should have partition statistics files",
        ImmutableList.of(partitionStatisticsFile),
        metadata.partitionStatisticsFiles());
  }

  @Test
  public void testRemoveStatistics() {
    // Create a snapshot
    table.newFastAppend().commit();
    assertTableMetadataVersion(1);

    TableMetadata base = readMetadata();
    long snapshotId = base.currentSnapshot().snapshotId();
    PartitionStatisticsFile partitionStatisticsFile =
        ImmutableGenericPartitionStatisticsFile.builder()
            .snapshotId(snapshotId)
            .path("/some/partition/statistics/file.parquet")
            .fileSizeInBytes(42L)
            .build();

    table.updatePartitionStatistics().setPartitionStatistics(partitionStatisticsFile).commit();

    TableMetadata metadata = readMetadata();
    assertTableMetadataVersion(2);
    Assert.assertEquals(
        "Table metadata should have partition statistics files",
        ImmutableList.of(partitionStatisticsFile),
        metadata.partitionStatisticsFiles());

    table.updatePartitionStatistics().removePartitionStatistics(snapshotId).commit();

    metadata = readMetadata();
    assertTableMetadataVersion(3);
    Assert.assertEquals(
        "Table metadata should have no partition statistics files",
        ImmutableList.of(),
        metadata.partitionStatisticsFiles());
  }

  private void assertTableMetadataVersion(int expected) {
    Assert.assertEquals(
        String.format("Table should be on version %s", expected), expected, (int) version());
  }
}
