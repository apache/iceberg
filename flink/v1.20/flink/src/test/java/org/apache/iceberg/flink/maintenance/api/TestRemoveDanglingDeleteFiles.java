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
package org.apache.iceberg.flink.maintenance.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.StreamSupport;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.maintenance.operator.MetricsReporterFactoryForTests;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestRemoveDanglingDeleteFiles extends MaintenanceTaskTestBase {
  private Table table;
  private PartitionSpec spec;

  @BeforeEach
  void before() {
    MetricsReporterFactoryForTests.reset();
    this.table = createPartitionedTable();
    this.spec = table.spec();
    tableLoader().open();
  }

  @Test
  void testPartitionedDanglingDeletes() throws Exception {
    DataFile fileB =
        DataFiles.builder(spec)
            .withPath("/path/to/data-b.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data=b")
            .withRecordCount(1)
            .build();
    DataFile fileC =
        DataFiles.builder(spec)
            .withPath("/path/to/data-c.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data=c")
            .withRecordCount(1)
            .build();

    // Commit 1 (seq=1): Add data files in partitions b, c
    table.newAppend().appendFile(fileB).appendFile(fileC).commit();

    // Commit 2 (seq=2): Add delete files for partitions a (no data) and b (has data)
    DeleteFile deleteA =
        FileMetadata.deleteFileBuilder(spec)
            .ofEqualityDeletes()
            .withPath("/path/to/delete-a.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data=a")
            .withRecordCount(1)
            .build();
    DeleteFile deleteA2 =
        FileMetadata.deleteFileBuilder(spec)
            .ofPositionDeletes()
            .withPath("/path/to/delete-a2.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data=a")
            .withRecordCount(1)
            .build();
    DeleteFile deleteB =
        FileMetadata.deleteFileBuilder(spec)
            .ofEqualityDeletes()
            .withPath("/path/to/delete-b.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data=b")
            .withRecordCount(1)
            .build();

    table.newRowDelta().addDeletes(deleteA).addDeletes(deleteA2).addDeletes(deleteB).commit();

    // Commit 3 (seq=3): Add more data to partitions a and b
    DataFile fileA =
        DataFiles.builder(spec)
            .withPath("/path/to/data-a.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data=a")
            .withRecordCount(1)
            .build();
    DataFile fileB2 =
        DataFiles.builder(spec)
            .withPath("/path/to/data-b2.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data=b")
            .withRecordCount(1)
            .build();

    table.newAppend().appendFile(fileA).appendFile(fileB2).commit();

    int deleteFilesBefore = deleteFileCount();

    runRemoveDanglingDeletes();

    table.refresh();
    int deleteFilesAfter = deleteFileCount();

    // Partition a: deleteA (eq, seq=2) and deleteA2 (pos, seq=2) are dangling
    // because the only data file in partition a (fileA) has seq=3,
    // meaning min_data_seq=3 > delete_seq=2 for both eq and pos deletes
    // Partition b: deleteB (eq, seq=2) is NOT dangling
    // because fileB has seq=1 which is < delete_seq=2
    assertThat(deleteFilesBefore - deleteFilesAfter)
        .as("Should have removed 2 dangling delete files from partition a")
        .isEqualTo(2);
  }

  @Test
  void testEqualityDeletesWithEqualSeqNo() throws Exception {
    DataFile fileA =
        DataFiles.builder(spec)
            .withPath("/path/to/data-a.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data=a")
            .withRecordCount(1)
            .build();

    // Commit 1 (seq=1): Add data in partition a
    table.newAppend().appendFile(fileA).commit();

    // Commit 2 (seq=2): Add data + eq deletes in same commit for partitions a and b
    DataFile fileA2 =
        DataFiles.builder(spec)
            .withPath("/path/to/data-a2.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data=a")
            .withRecordCount(1)
            .build();
    DataFile fileB =
        DataFiles.builder(spec)
            .withPath("/path/to/data-b.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data=b")
            .withRecordCount(1)
            .build();
    DeleteFile eqDeleteA =
        FileMetadata.deleteFileBuilder(spec)
            .ofEqualityDeletes()
            .withPath("/path/to/eq-delete-a.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data=a")
            .withRecordCount(1)
            .build();
    DeleteFile eqDeleteB =
        FileMetadata.deleteFileBuilder(spec)
            .ofEqualityDeletes()
            .withPath("/path/to/eq-delete-b.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data=b")
            .withRecordCount(1)
            .build();

    table
        .newRowDelta()
        .addRows(fileA2)
        .addRows(fileB)
        .addDeletes(eqDeleteA)
        .addDeletes(eqDeleteB)
        .commit();

    int deleteFilesBefore = deleteFileCount();

    runRemoveDanglingDeletes();

    table.refresh();
    int deleteFilesAfter = deleteFileCount();

    // Partition a: eqDeleteA (seq=2) is NOT dangling because fileA (seq=1) exists
    //   and min_data_seq=1 < delete_seq=2
    // Partition b: eqDeleteB (seq=2) IS dangling because min_data_seq=2 == delete_seq=2
    //   (equality deletes with seq <= min_data_seq are dangling)
    assertThat(deleteFilesBefore - deleteFilesAfter)
        .as("Should have removed 1 dangling eq delete from partition b")
        .isEqualTo(1);
  }

  @Test
  void testUnpartitionedTable() throws Exception {
    // Drop the partitioned table and create an unpartitioned one
    dropTable();
    this.table = createTable();

    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/data.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();
    DeleteFile deleteFile =
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofEqualityDeletes()
            .withPath("/path/to/delete.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();

    table.newRowDelta().addRows(dataFile).addDeletes(deleteFile).commit();

    int deleteFilesBefore = deleteFileCount();

    runRemoveDanglingDeletes();

    table.refresh();
    int deleteFilesAfter = deleteFileCount();

    assertThat(deleteFilesAfter)
        .as("Unpartitioned table should be a no-op")
        .isEqualTo(deleteFilesBefore);
  }

  @Test
  void testNoDanglingDeletes() throws Exception {
    DataFile fileA =
        DataFiles.builder(spec)
            .withPath("/path/to/data-a.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data=a")
            .withRecordCount(1)
            .build();

    // Commit 1 (seq=1): Add data file
    table.newAppend().appendFile(fileA).commit();

    // Commit 2 (seq=2): Add delete file in same partition
    DeleteFile deleteA =
        FileMetadata.deleteFileBuilder(spec)
            .ofEqualityDeletes()
            .withPath("/path/to/delete-a.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data=a")
            .withRecordCount(1)
            .build();

    table.newRowDelta().addDeletes(deleteA).commit();

    int deleteFilesBefore = deleteFileCount();

    runRemoveDanglingDeletes();

    table.refresh();
    int deleteFilesAfter = deleteFileCount();

    // deleteA (seq=2) is not dangling because fileA (seq=1) has min_data_seq=1 < 2
    assertThat(deleteFilesAfter).as("No deletes should be removed").isEqualTo(deleteFilesBefore);
  }

  @Test
  void testNoSnapshot() throws Exception {
    // Drop and recreate the table so it has no snapshots
    dropTable();
    this.table = createPartitionedTable();

    // Should succeed with no changes
    runRemoveDanglingDeletes();
  }

  @Test
  void testFailure() throws Exception {
    DataFile fileA =
        DataFiles.builder(spec)
            .withPath("/path/to/data-a.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data=a")
            .withRecordCount(1)
            .build();

    table.newAppend().appendFile(fileA).commit();

    RemoveDanglingDeleteFiles.builder()
        .append(
            infra.triggerStream(),
            DUMMY_TABLE_NAME,
            DUMMY_TASK_NAME,
            0,
            tableLoader(),
            UID_SUFFIX,
            StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP,
            1)
        .sinkTo(infra.sink());

    runAndWaitForFailure(infra.env(), infra.source(), infra.sink());
  }

  private void runRemoveDanglingDeletes() throws Exception {
    RemoveDanglingDeleteFiles.builder()
        .parallelism(1)
        .uidSuffix(UID_SUFFIX)
        .append(
            infra.triggerStream(),
            DUMMY_TABLE_NAME,
            DUMMY_TASK_NAME,
            0,
            tableLoader(),
            "OTHER",
            StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP,
            1)
        .sinkTo(infra.sink());

    runAndWaitForSuccess(infra.env(), infra.source(), infra.sink());
  }

  private int deleteFileCount() {
    table.refresh();
    return (int)
        StreamSupport.stream(
                table.currentSnapshot().deleteManifests(table.io()).spliterator(), false)
            .mapToLong(manifest -> manifest.existingFilesCount() + manifest.addedFilesCount())
            .sum();
  }
}
