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

import static org.apache.iceberg.ManifestEntry.Status.ADDED;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.partition.stats.PartitionStatsUtil;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestPartitionStats extends TableTestBase {
  public TestPartitionStats(int formatVersion) {
    super(formatVersion);
  }

  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] {1, 2};
  }

  @Test
  public void testNoPartitionStatsForUnPartitionedTable() throws Exception {
    TestTables.TestTable table =
        TestTables.create(
            temp.newFolder(),
            "unpartitioned",
            SCHEMA,
            PartitionSpec.unpartitioned(),
            formatVersion);
    table
        .newAppend()
        .appendFile(newUnpartitionedDataFile("data-1.parquet", PartitionSpec.unpartitioned()))
        .commit();

    Snapshot snapshot = table.currentSnapshot();
    Assertions.assertThat(snapshot.partitionStatsFileLocation()).isNull();
    Assertions.assertThat(table.partitionStatisticsFiles()).isEmpty();
  }

  @Test
  public void testNoPartitionStatsForUnPartitionedDataFiles() {
    // partitioned table but un-partitioned data file.
    table
        .newAppend()
        .appendFile(newUnpartitionedDataFile("data-1.parquet", PartitionSpec.unpartitioned()))
        .commit();

    Snapshot snapshot = table.currentSnapshot();
    Assertions.assertThat(snapshot.partitionStatsFileLocation()).isNull();
    Assertions.assertThat(table.partitionStatisticsFiles()).isEmpty();
  }

  @Test
  public void testNoPartitionStatsForNoDatafiles() {
    table.newAppend().commit();
    Snapshot snapshot = table.currentSnapshot();
    Assertions.assertThat(snapshot.partitionStatsFileLocation()).isNull();
    Assertions.assertThat(table.partitionStatisticsFiles()).isEmpty();
  }

  @Test
  public void testNoSideEffectsFromUncommittedAppend() {
    // Two uncommitted files
    table
        .newAppend()
        .appendFile(newDataFile("data_bucket=0"))
        .appendFile(newDataFile("data_bucket=0"));

    // One committed file
    table.newAppend().appendFile(newDataFile("data_bucket=0")).commit();

    validatePartitionStaticsFile(table.currentSnapshot());
    validatePartition(1, 1, 10);
  }

  @Test
  public void testPartitionStatsAfterAppendFilesInSinglePartition() {
    table
        .newAppend()
        .appendFile(newDataFile("data_bucket=0"))
        .appendFile(newDataFile("data_bucket=0"))
        .commit();

    validatePartitionStaticsFile(table.currentSnapshot());
    validatePartition(2, 2, 20);
  }

  @Test
  public void testPartitionStatsFileOneTxn() {
    Transaction transaction = table.newTransaction();
    transaction
        .newAppend()
        .appendFile(newDataFile("data_bucket=0"))
        .appendFile(newDataFile("data_bucket=0"))
        .commit();
    transaction.commitTransaction();

    validatePartitionStaticsFile(table.currentSnapshot());
  }

  @Test
  public void testPartitionStatsFileOnTransactionRetry() {
    int retryCount = 1;
    table
        .updateProperties()
        .set(TableProperties.COMMIT_NUM_RETRIES, String.valueOf(retryCount))
        .commit();

    Transaction transaction = table.newTransaction();
    transaction
        .newAppend()
        .appendFile(newDataFile("data_bucket=0"))
        .appendFile(newDataFile("data_bucket=0"))
        .commit();

    table.ops().failCommits(retryCount);
    transaction.commitTransaction();

    validatePartitionStaticsFile(table.currentSnapshot());
  }

  @Test
  public void testNoPartitionStatsFileOnTransactionFailure() {
    int retryCount = 1;
    table.newAppend().appendFile(newDataFile("data_bucket=0")).commit();

    Snapshot snapshot1 = table.currentSnapshot();
    validatePartitionStaticsFile(snapshot1);
    PartitionStatisticsFile partitionStatisticsFile = table.partitionStatisticsFiles().get(0);

    table
        .updateProperties()
        .set(TableProperties.COMMIT_NUM_RETRIES, String.valueOf(retryCount))
        .commit();

    Transaction transaction = table.newTransaction();
    transaction
        .newAppend()
        .appendFile(newDataFile("data_bucket=0"))
        .appendFile(newDataFile("data_bucket=0"))
        .commit();

    table.ops().failCommits(retryCount + 1);
    try {
      transaction.commitTransaction();
      fail("Expected CommitFailedException");
    } catch (Exception exception) {
      Assertions.assertThat(exception instanceof CommitFailedException).isTrue();
    }

    Assertions.assertThat(table.currentSnapshot()).isEqualTo(snapshot1);
    Assertions.assertThat(table.partitionStatisticsFiles())
        .containsExactly(partitionStatisticsFile);
  }

  @Test
  public void testPartitionStatsAfterFastAppendFilesInSinglePartition() {
    table
        .newFastAppend()
        .appendFile(newDataFile("data_bucket=0"))
        .appendFile(newDataFile("data_bucket=0"))
        .commit();

    validatePartitionStaticsFile(table.currentSnapshot());
  }

  @Test
  public void testPartitionStatsAfterAppendManifestInSinglePartition() throws Exception {
    table
        .newAppend()
        .appendManifest(
            writeManifest(
                "new-manifest0.avro",
                manifestEntry(ADDED, null, newDataFile("data_bucket=0")),
                manifestEntry(ADDED, null, newDataFile("data_bucket=0"))))
        .appendManifest(
            writeManifest(
                "new-manifest1.avro",
                manifestEntry(ADDED, null, newDataFile("data_bucket=0")),
                manifestEntry(ADDED, null, newDataFile("data_bucket=0"))))
        .commit();

    validatePartitionStaticsFile(table.currentSnapshot());
    validatePartition(4, 4, 40);
  }

  @Test
  public void testPartitionStatsAfterAppendManifestInSinglePartitionWhenSnapshotInheritanceEnabled()
      throws Exception {
    table.updateProperties().set(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, "true").commit();

    table
        .newAppend()
        .appendManifest(
            writeManifest(
                "new-manifest0.avro",
                manifestEntry(ADDED, null, newDataFile("data_bucket=0")),
                manifestEntry(ADDED, null, newDataFile("data_bucket=0"))))
        .appendManifest(
            writeManifest(
                "new-manifest1.avro",
                manifestEntry(ADDED, null, newDataFile("data_bucket=0")),
                manifestEntry(ADDED, null, newDataFile("data_bucket=0"))))
        .commit();

    validatePartitionStaticsFile(table.currentSnapshot());
    validatePartition(4, 4, 40);

    table
        .updateProperties()
        .set(
            TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED,
            String.valueOf(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED_DEFAULT))
        .commit();
  }

  @Test
  public void
      testPartitionStatsAfterFastAppendManifestInSinglePartitionWhenSnapshotInheritanceIsOn()
          throws Exception {
    table.updateProperties().set(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, "true").commit();

    table
        .newFastAppend()
        .appendManifest(
            writeManifest(
                "new-manifest0.avro",
                manifestEntry(ADDED, null, newDataFile("data_bucket=0")),
                manifestEntry(ADDED, null, newDataFile("data_bucket=0"))))
        .appendManifest(
            writeManifest(
                "new-manifest1.avro",
                manifestEntry(ADDED, null, newDataFile("data_bucket=0")),
                manifestEntry(ADDED, null, newDataFile("data_bucket=0"))))
        .commit();

    validatePartitionStaticsFile(table.currentSnapshot());
    validatePartition(4, 4, 40);

    table
        .updateProperties()
        .set(
            TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED,
            String.valueOf(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED_DEFAULT))
        .commit();
  }

  @Test
  public void testPartitionStatsAfterAppendManifestAcrossPartitions() throws Exception {
    table
        .newAppend()
        .appendManifest(
            writeManifest(
                "new-manifest0.avro",
                manifestEntry(ADDED, null, newDataFile("data_bucket=0")),
                manifestEntry(ADDED, null, newDataFile("data_bucket=1"))))
        .appendManifest(
            writeManifest(
                "new-manifest1.avro",
                manifestEntry(ADDED, null, newDataFile("data_bucket=0")),
                manifestEntry(ADDED, null, newDataFile("data_bucket=1"))))
        .commit();

    validatePartitionStaticsFile(table.currentSnapshot());
    PartitionData partitionData1 = new PartitionData(Partitioning.partitionType(table));
    partitionData1.set(0, 0);
    Partition partition1 =
        buildPartition(partitionData1, 2, 2, 20, table.currentSnapshot(), table.spec().specId());

    PartitionData partitionData2 = new PartitionData(Partitioning.partitionType(table));
    partitionData2.set(0, 1);
    Partition partition2 =
        buildPartition(partitionData2, 2, 2, 20, table.currentSnapshot(), table.spec().specId());
    validatePartitions(table, Lists.newArrayList(partition1, partition2));
  }

  @Test
  public void testPartitionStatsAfterAppendDeleteDataFilesAndManifestFilesInOneTxn()
      throws Exception {
    table
        .newAppend()
        .appendFile(newDataFile("data_bucket=0"))
        .appendFile(newDataFile("data_bucket=1"))
        .commit();

    Transaction transaction = table.newTransaction();
    transaction
        .newAppend()
        .appendFile(newDataFile("data_bucket=0"))
        .appendFile(newDataFile("data_bucket=1"))
        .appendManifest(writeManifest(newDataFile("data_bucket=0"), newDataFile("data_bucket=1")))
        .commit();
    transaction
        .newDelete()
        .deleteFile(newDataFile("data_bucket=0"))
        .deleteFile(newDataFile("data_bucket=1").path())
        .commit();
    transaction.commitTransaction();

    // Delete is unaffected due to the logic in ManifestFilterManager.manifestHasDeletedFiles()
    // returning false. TODO: Need to analyze.
    validatePartitionStaticsFile(table.currentSnapshot());
    PartitionData partitionData1 = new PartitionData(Partitioning.partitionType(table));
    partitionData1.set(0, 0);
    Partition partition1 =
        buildPartition(
            partitionData1,
            3,
            3,
            30,
            table.snapshot(table.currentSnapshot().parentId()),
            table.spec().specId());

    PartitionData partitionData2 = new PartitionData(Partitioning.partitionType(table));
    partitionData2.set(0, 1);
    Partition partition2 =
        buildPartition(
            partitionData2,
            3,
            3,
            30,
            table.snapshot(table.currentSnapshot().parentId()),
            table.spec().specId());
    validatePartitions(table, Lists.newArrayList(partition1, partition2));
  }

  @Test
  public void testPartitionStatsAfterFastAppendDeleteDataFilesAndManifestFilesInOneTxn()
      throws Exception {
    table
        .newFastAppend()
        .appendFile(newDataFile("data_bucket=0"))
        .appendFile(newDataFile("data_bucket=1"))
        .commit();

    Transaction transaction = table.newTransaction();
    transaction
        .newFastAppend()
        .appendFile(newDataFile("data_bucket=0"))
        .appendFile(newDataFile("data_bucket=1"))
        .appendManifest(writeManifest(newDataFile("data_bucket=0"), newDataFile("data_bucket=1")))
        .commit();
    transaction
        .newDelete()
        .deleteFile(newDataFile("data_bucket=0"))
        .deleteFile(newDataFile("data_bucket=1").path())
        .commit();
    transaction.commitTransaction();

    validatePartitionStaticsFile(table.currentSnapshot());
    // Delete is unaffected due to the logic in ManifestFilterManager.manifestHasDeletedFiles()
    // returning false. TODO: Need to analyze.
    validatePartitionStaticsFile(table.currentSnapshot());
    PartitionData partitionData1 = new PartitionData(Partitioning.partitionType(table));
    partitionData1.set(0, 0);
    Partition partition1 =
        buildPartition(
            partitionData1,
            3,
            3,
            30,
            table.snapshot(table.currentSnapshot().parentId()),
            table.spec().specId());

    PartitionData partitionData2 = new PartitionData(Partitioning.partitionType(table));
    partitionData2.set(0, 1);
    Partition partition2 =
        buildPartition(
            partitionData2,
            3,
            3,
            30,
            table.snapshot(table.currentSnapshot().parentId()),
            table.spec().specId());
    validatePartitions(table, Lists.newArrayList(partition1, partition2));
  }

  @Test
  public void testPartitionStatsAfterAppendFilesUpdatedPartitionSpec() {
    table
        .newAppend()
        .appendFile(newDataFile("data_bucket=0"))
        .appendFile(newDataFile("data_bucket=0"))
        .commit();

    validatePartitionStaticsFile(table.currentSnapshot());
    int specId = table.spec().specId();
    PartitionData partitionData1 = new PartitionData(Partitioning.partitionType(table));
    partitionData1.set(0, 0);
    Partition partition1 =
        buildPartition(partitionData1, 2, 2, 20, table.currentSnapshot(), specId);
    validatePartitions(table, Collections.singletonList(partition1));

    table.updateSpec().addField("id").commit();

    table
        .newAppend()
        .appendFile(newDataFile("data_bucket=1/id=2"))
        .appendFile(newDataFile("data_bucket=1/id=2"))
        .commit();

    validatePartitionStaticsFile(table.currentSnapshot());

    PartitionData partitionData1Coerced = new PartitionData(Partitioning.partitionType(table));
    partitionData1Coerced.set(0, 0);
    Partition partition1Coerced =
        buildPartition(
            partitionData1Coerced,
            2,
            2,
            20,
            table.snapshot(table.currentSnapshot().parentId()),
            specId);

    PartitionData partitionData2 = new PartitionData(Partitioning.partitionType(table));
    partitionData2.set(0, 1);
    partitionData2.set(1, 2);
    Partition partition2 =
        buildPartition(partitionData2, 2, 2, 20, table.currentSnapshot(), table.spec().specId());
    validatePartitions(table, Lists.newArrayList(partition1Coerced, partition2));
  }

  @Test
  public void testNoPartitionStatsAfterUpdateSpecRemovesField() {
    table.updateSpec().removeField("data_bucket").commit();

    table.newAppend().appendFile(newUnpartitionedDataFile("data-1.parquet", table.spec())).commit();

    Assertions.assertThat(table.currentSnapshot().partitionStatsFileLocation()).isNull();
    Assertions.assertThat(table.partitionStatisticsFiles()).isEmpty();
  }

  @Test
  public void testPartitionStatsAfterDeleteFiles() {
    table
        .newAppend()
        .appendFile(newDataFile("data_bucket=0"))
        .appendFile(newDataFile("data_bucket=0"))
        .appendFile(newDataFile("data_bucket=1"))
        .appendFile(newDataFile("data_bucket=1"))
        .commit();
    validatePartitionStaticsFile(table.currentSnapshot());

    table
        .newDelete()
        .deleteFile(newDataFile("data_bucket=0"))
        .deleteFile(newDataFile("data_bucket=1").path())
        .commit();
    validatePartitionStaticsFile(table.currentSnapshot());
  }

  @Test
  public void testPartitionStatsAfterUpdateSpecAndDeleteAllFiles() {
    table
        .newAppend()
        .appendFile(newDataFile("data_bucket=0"))
        .appendFile(newDataFile("data_bucket=0"))
        .commit();

    table.updateSpec().addField("id").commit();

    table
        .newAppend()
        .appendFile(newDataFile("data_bucket=1/id=2"))
        .appendFile(newDataFile("data_bucket=1/id=2"))
        .commit();
    validatePartitionStaticsFile(table.currentSnapshot());

    table.newDelete().deleteFromRowFilter(Expressions.alwaysTrue()).commit();
    validatePartitionStaticsFile(table.currentSnapshot());
  }

  @Test
  public void testPartitionStatsAfterRewriteDataFiles() {
    DataFile file1 = newDataFile("data_bucket=0");
    DataFile file2 = newDataFile("data_bucket=0");
    DataFile file3 = newDataFile("data_bucket=0");

    table.newAppend().appendFile(file1).appendFile(file2).commit();
    table.newRewrite().rewriteFiles(Sets.newHashSet(file1, file2), Sets.newHashSet(file3)).commit();

    validatePartitionStaticsFile(table.currentSnapshot());
  }

  @Test
  public void testPartitionStatsAfterRewriteManifestFiles() {
    table
        .newAppend()
        .appendFile(newDataFile("data_bucket=0"))
        .appendFile(newDataFile("data_bucket=0"))
        .appendFile(newDataFile("data_bucket=0"))
        .appendFile(newDataFile("data_bucket=0"))
        .commit();
    validatePartitionStaticsFile(table.currentSnapshot());

    table.rewriteManifests().clusterBy(ContentFile::format).commit();

    validatePartitionStaticsFile(table.currentSnapshot());
  }

  private DataFile newUnpartitionedDataFile(String newFilePath, PartitionSpec spec) {
    return DataFiles.builder(spec)
        .withPath(newFilePath)
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .build();
  }

  private void validatePartition(long dataRecordCount, int dataFileCount, long dataFileSize) {
    PartitionData partitionData = new PartitionData(Partitioning.partitionType(table));
    partitionData.set(0, 0);
    validatePartition(table, partitionData, dataRecordCount, dataFileCount, dataFileSize);
  }

  private void validatePartition(
      Table table,
      PartitionData partitionData,
      long dataRecordCount,
      int dataFileCount,
      long dataFileSize) {
    Partition partition =
        buildPartition(
            partitionData,
            dataRecordCount,
            dataFileCount,
            dataFileSize,
            table.currentSnapshot(),
            table.spec().specId());
    validatePartitions(table, Collections.singletonList(partition));
  }

  private void validatePartitions(Table table, List<Partition> partitions) {
    Assertions.assertThat(readPartitionStats(table, table.currentSnapshot()))
        .containsExactlyInAnyOrderElementsOf(partitions);
  }

  private static Partition buildPartition(
      PartitionData partitionData,
      long dataRecordCount,
      int dataFileCount,
      long dataFileSize,
      Snapshot snapshot,
      int specId) {
    return Partition.builder()
        .withPartitionData(partitionData)
        .withSpecId(specId)
        .withDataRecordCount(dataRecordCount)
        .withDataFileCount(dataFileCount)
        .withDataFileSizeInBytes(dataFileSize)
        .withPosDeleteRecordCount(0L)
        .withPosDeleteFileCount(0)
        .withEqDeleteRecordCount(0L)
        .withEqDeleteFileCount(0)
        .withLastUpdatedAt(snapshot.timestampMillis() * 1000L)
        .withLastUpdatedSnapshotId(snapshot.snapshotId())
        .build();
  }

  private List<Partition> readPartitionStats(Table table, Snapshot snapshot) {
    Schema schema = Partition.icebergSchema(Partitioning.partitionType(table));
    try (CloseableIterable<Partition> recordIterator =
        PartitionStatsUtil.readPartitionStatsFile(
            schema, Files.localInput(snapshot.partitionStatsFileLocation()))) {
      return Lists.newArrayList(recordIterator);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void validatePartitionStaticsFile(Snapshot snapshot) {
    validatePartitionStaticsFile(snapshot, table);
  }

  private void validatePartitionStaticsFile(Snapshot snapshot, Table table) {
    PartitionStatisticsFile partitionStatisticsFile =
        ImmutableGenericPartitionStatisticsFile.builder()
            .snapshotId(snapshot.snapshotId())
            .path(snapshot.partitionStatsFileLocation())
            .maxDataSequenceNumber(snapshot.sequenceNumber())
            .build();
    Assertions.assertThat(table.partitionStatisticsFiles()).contains(partitionStatisticsFile);
  }
}
