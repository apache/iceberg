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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.util.Map;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSnapshotSummary extends TestBase {

  @TestTemplate
  public void testFileSizeSummary() {
    assertThat(listManifestFiles()).hasSize(0);

    // fast append
    table.newFastAppend().appendFile(FILE_A).commit();
    Map<String, String> summary = table.currentSnapshot().summary();
    assertThat(summary)
        .containsEntry(SnapshotSummary.ADDED_FILE_SIZE_PROP, "10")
        .containsEntry(SnapshotSummary.TOTAL_FILE_SIZE_PROP, "10")
        .doesNotContainKey(SnapshotSummary.REMOVED_FILE_SIZE_PROP);

    // merge append
    table.newAppend().appendFile(FILE_B).commit();
    summary = table.currentSnapshot().summary();
    assertThat(summary)
        .containsEntry(SnapshotSummary.ADDED_FILE_SIZE_PROP, "10")
        .containsEntry(SnapshotSummary.TOTAL_FILE_SIZE_PROP, "20")
        .doesNotContainKey(SnapshotSummary.REMOVED_FILE_SIZE_PROP);

    table
        .newOverwrite()
        .deleteFile(FILE_A)
        .deleteFile(FILE_B)
        .addFile(FILE_C)
        .addFile(FILE_D)
        .commit();
    summary = table.currentSnapshot().summary();
    assertThat(summary)
        .containsEntry(SnapshotSummary.ADDED_FILE_SIZE_PROP, "20")
        .containsEntry(SnapshotSummary.REMOVED_FILE_SIZE_PROP, "20")
        .containsEntry(SnapshotSummary.TOTAL_FILE_SIZE_PROP, "20");

    table.newDelete().deleteFile(FILE_C).deleteFile(FILE_D).commit();
    summary = table.currentSnapshot().summary();
    assertThat(summary)
        .containsEntry(SnapshotSummary.REMOVED_FILE_SIZE_PROP, "20")
        .containsEntry(SnapshotSummary.TOTAL_FILE_SIZE_PROP, "0")
        .doesNotContainKey(SnapshotSummary.ADDED_FILE_SIZE_PROP);
  }

  @TestTemplate
  public void testFileSizeSummaryWithDeletes() {
    assumeThat(formatVersion).isEqualTo(2);

    table.newRowDelta().addDeletes(FILE_A_DELETES).addDeletes(FILE_A2_DELETES).commit();

    table.refresh();
    Map<String, String> summary = table.currentSnapshot().summary();
    assertThat(summary)
        .containsEntry(SnapshotSummary.ADD_EQ_DELETE_FILES_PROP, "1")
        .containsEntry(SnapshotSummary.ADD_POS_DELETE_FILES_PROP, "1");
  }

  @TestTemplate
  public void testIcebergVersionInSummary() {
    table.newFastAppend().appendFile(FILE_A).commit();
    Map<String, String> summary = table.currentSnapshot().summary();
    assertThat(summary).containsKey("iceberg-version");
  }

  @TestTemplate
  public void fastAppendWithDuplicates() {
    assertThat(listManifestFiles()).isEmpty();

    table
        .newFastAppend()
        .appendFile(FILE_A)
        .appendFile(DataFiles.builder(SPEC).copy(FILE_A).build())
        .appendFile(FILE_A)
        .commit();

    assertThat(table.currentSnapshot().summary())
        .hasSize(12)
        .containsEntry(SnapshotSummary.ADDED_FILES_PROP, "1")
        .containsEntry(SnapshotSummary.ADDED_FILE_SIZE_PROP, "10")
        .containsEntry(SnapshotSummary.ADDED_RECORDS_PROP, "1")
        .containsEntry(SnapshotSummary.CHANGED_PARTITION_COUNT_PROP, "1")
        .containsEntry(SnapshotSummary.TOTAL_DATA_FILES_PROP, "1")
        .containsEntry(SnapshotSummary.TOTAL_DELETE_FILES_PROP, "0")
        .containsEntry(SnapshotSummary.TOTAL_EQ_DELETES_PROP, "0")
        .containsEntry(SnapshotSummary.TOTAL_POS_DELETES_PROP, "0")
        .containsEntry(SnapshotSummary.TOTAL_FILE_SIZE_PROP, "10")
        .containsEntry(SnapshotSummary.TOTAL_RECORDS_PROP, "1")
        .containsEntry(SnapshotSummary.TARGET_BRANCH_PROP, "main");
  }

  @TestTemplate
  public void mergeAppendWithDuplicates() {
    assertThat(listManifestFiles()).isEmpty();

    table
        .newAppend()
        .appendFile(FILE_A)
        .appendFile(DataFiles.builder(SPEC).copy(FILE_A).build())
        .appendFile(FILE_A)
        .commit();

    assertThat(table.currentSnapshot().summary())
        .hasSize(12)
        .containsEntry(SnapshotSummary.ADDED_FILES_PROP, "1")
        .containsEntry(SnapshotSummary.ADDED_FILE_SIZE_PROP, "10")
        .containsEntry(SnapshotSummary.ADDED_RECORDS_PROP, "1")
        .containsEntry(SnapshotSummary.CHANGED_PARTITION_COUNT_PROP, "1")
        .containsEntry(SnapshotSummary.TOTAL_DATA_FILES_PROP, "1")
        .containsEntry(SnapshotSummary.TOTAL_DELETE_FILES_PROP, "0")
        .containsEntry(SnapshotSummary.TOTAL_EQ_DELETES_PROP, "0")
        .containsEntry(SnapshotSummary.TOTAL_POS_DELETES_PROP, "0")
        .containsEntry(SnapshotSummary.TOTAL_FILE_SIZE_PROP, "10")
        .containsEntry(SnapshotSummary.TOTAL_RECORDS_PROP, "1")
        .containsEntry(SnapshotSummary.TARGET_BRANCH_PROP, "main");
  }

  @TestTemplate
  public void overwriteWithDuplicates() {
    assertThat(listManifestFiles()).isEmpty();
    table.newFastAppend().appendFile(FILE_A).commit();

    table
        .newOverwrite()
        .deleteFile(FILE_A)
        .deleteFile(DataFiles.builder(SPEC).copy(FILE_A).build())
        .deleteFile(FILE_A)
        .addFile(FILE_C)
        .addFile(DataFiles.builder(SPEC).copy(FILE_C).build())
        .addFile(FILE_C)
        .commit();

    assertThat(table.currentSnapshot().summary())
        .hasSize(15)
        .containsEntry(SnapshotSummary.ADDED_FILES_PROP, "1")
        .containsEntry(SnapshotSummary.ADDED_FILE_SIZE_PROP, "10")
        .containsEntry(SnapshotSummary.ADDED_RECORDS_PROP, "1")
        .containsEntry(SnapshotSummary.CHANGED_PARTITION_COUNT_PROP, "2")
        .containsEntry(SnapshotSummary.DELETED_FILES_PROP, "1")
        .containsEntry(SnapshotSummary.DELETED_RECORDS_PROP, "1")
        .containsEntry(SnapshotSummary.REMOVED_FILE_SIZE_PROP, "10")
        .containsEntry(SnapshotSummary.TOTAL_DATA_FILES_PROP, "1")
        .containsEntry(SnapshotSummary.TOTAL_DELETE_FILES_PROP, "0")
        .containsEntry(SnapshotSummary.TOTAL_EQ_DELETES_PROP, "0")
        .containsEntry(SnapshotSummary.TOTAL_POS_DELETES_PROP, "0")
        .containsEntry(SnapshotSummary.TOTAL_FILE_SIZE_PROP, "10")
        .containsEntry(SnapshotSummary.TOTAL_RECORDS_PROP, "1")
        .containsEntry(SnapshotSummary.TARGET_BRANCH_PROP, "main");
  }

  @TestTemplate
  public void deleteWithDuplicates() {
    assertThat(listManifestFiles()).isEmpty();
    table.newFastAppend().appendFile(FILE_C).appendFile(FILE_D).commit();

    table
        .newDelete()
        .deleteFile(FILE_C)
        .deleteFile(DataFiles.builder(SPEC).copy(FILE_C).build())
        .deleteFile(FILE_C)
        .deleteFile(FILE_D)
        .deleteFile(DataFiles.builder(SPEC).copy(FILE_D).build())
        .deleteFile(FILE_D)
        .commit();

    assertThat(table.currentSnapshot().summary())
        .hasSize(12)
        .containsEntry(SnapshotSummary.CHANGED_PARTITION_COUNT_PROP, "2")
        .containsEntry(SnapshotSummary.DELETED_FILES_PROP, "2")
        .containsEntry(SnapshotSummary.DELETED_RECORDS_PROP, "2")
        .containsEntry(SnapshotSummary.REMOVED_FILE_SIZE_PROP, "20")
        .containsEntry(SnapshotSummary.TOTAL_DATA_FILES_PROP, "0")
        .containsEntry(SnapshotSummary.TOTAL_DELETE_FILES_PROP, "0")
        .containsEntry(SnapshotSummary.TOTAL_EQ_DELETES_PROP, "0")
        .containsEntry(SnapshotSummary.TOTAL_POS_DELETES_PROP, "0")
        .containsEntry(SnapshotSummary.TOTAL_FILE_SIZE_PROP, "0")
        .containsEntry(SnapshotSummary.TOTAL_RECORDS_PROP, "0")
        .containsEntry(SnapshotSummary.TARGET_BRANCH_PROP, "main");
  }

  @TestTemplate
  public void replacePartitionsWithDuplicates() {
    assertThat(listManifestFiles()).isEmpty();

    table
        .newReplacePartitions()
        .addFile(FILE_A)
        .addFile(DataFiles.builder(SPEC).copy(FILE_A).build())
        .addFile(FILE_A)
        .commit();

    assertThat(table.currentSnapshot().summary())
        .hasSize(13)
        .containsEntry(SnapshotSummary.ADDED_FILES_PROP, "1")
        .containsEntry(SnapshotSummary.ADDED_FILE_SIZE_PROP, "10")
        .containsEntry(SnapshotSummary.ADDED_RECORDS_PROP, "1")
        .containsEntry(SnapshotSummary.CHANGED_PARTITION_COUNT_PROP, "1")
        .containsEntry(SnapshotSummary.REPLACE_PARTITIONS_PROP, "true")
        .containsEntry(SnapshotSummary.TOTAL_DATA_FILES_PROP, "1")
        .containsEntry(SnapshotSummary.TOTAL_DELETE_FILES_PROP, "0")
        .containsEntry(SnapshotSummary.TOTAL_EQ_DELETES_PROP, "0")
        .containsEntry(SnapshotSummary.TOTAL_POS_DELETES_PROP, "0")
        .containsEntry(SnapshotSummary.TOTAL_FILE_SIZE_PROP, "10")
        .containsEntry(SnapshotSummary.TOTAL_RECORDS_PROP, "1")
        .containsEntry(SnapshotSummary.TARGET_BRANCH_PROP, "main");
  }

  @TestTemplate
  public void rowDeltaWithDuplicates() {
    assertThat(listManifestFiles()).isEmpty();

    table
        .newRowDelta()
        .addRows(FILE_A)
        .addRows(DataFiles.builder(SPEC).copy(FILE_A).build())
        .addRows(FILE_A)
        .commit();

    assertThat(table.currentSnapshot().summary())
        .hasSize(12)
        .containsEntry(SnapshotSummary.ADDED_FILES_PROP, "1")
        .containsEntry(SnapshotSummary.ADDED_FILE_SIZE_PROP, "10")
        .containsEntry(SnapshotSummary.ADDED_RECORDS_PROP, "1")
        .containsEntry(SnapshotSummary.CHANGED_PARTITION_COUNT_PROP, "1")
        .containsEntry(SnapshotSummary.TOTAL_DATA_FILES_PROP, "1")
        .containsEntry(SnapshotSummary.TOTAL_DELETE_FILES_PROP, "0")
        .containsEntry(SnapshotSummary.TOTAL_EQ_DELETES_PROP, "0")
        .containsEntry(SnapshotSummary.TOTAL_POS_DELETES_PROP, "0")
        .containsEntry(SnapshotSummary.TOTAL_FILE_SIZE_PROP, "10")
        .containsEntry(SnapshotSummary.TOTAL_RECORDS_PROP, "1")
        .containsEntry(SnapshotSummary.TARGET_BRANCH_PROP, "main");
  }

  @TestTemplate
  public void rowDeltaWithDeletesAndDuplicates() {
    assumeThat(formatVersion).isEqualTo(2);
    assertThat(listManifestFiles()).isEmpty();

    table
        .newRowDelta()
        .addRows(FILE_A)
        .addRows(DataFiles.builder(SPEC).copy(FILE_A).build())
        .addRows(FILE_A)
        .addDeletes(FILE_A_DELETES)
        .addDeletes(FileMetadata.deleteFileBuilder(SPEC).copy(FILE_A_DELETES).build())
        .addDeletes(FILE_A_DELETES)
        .commit();

    assertThat(table.currentSnapshot().summary())
        .hasSize(15)
        .containsEntry(SnapshotSummary.ADDED_FILES_PROP, "1")
        .containsEntry(SnapshotSummary.ADDED_DELETE_FILES_PROP, "1")
        .containsEntry(SnapshotSummary.ADDED_FILE_SIZE_PROP, "20") // size of data + delete file
        .containsEntry(SnapshotSummary.ADD_POS_DELETE_FILES_PROP, "1")
        .containsEntry(SnapshotSummary.ADDED_POS_DELETES_PROP, "1")
        .containsEntry(SnapshotSummary.ADDED_RECORDS_PROP, "1")
        .containsEntry(SnapshotSummary.CHANGED_PARTITION_COUNT_PROP, "1")
        .containsEntry(SnapshotSummary.TOTAL_DATA_FILES_PROP, "1")
        .containsEntry(SnapshotSummary.TOTAL_DELETE_FILES_PROP, "1")
        .containsEntry(SnapshotSummary.TOTAL_EQ_DELETES_PROP, "0")
        .containsEntry(SnapshotSummary.TOTAL_POS_DELETES_PROP, "1")
        .containsEntry(SnapshotSummary.TOTAL_FILE_SIZE_PROP, "20")
        .containsEntry(SnapshotSummary.TOTAL_RECORDS_PROP, "1")
        .containsEntry(SnapshotSummary.TARGET_BRANCH_PROP, "main");
  }

  @TestTemplate
  public void rewriteWithDuplicateFiles() {
    assertThat(listManifestFiles()).isEmpty();

    table.newAppend().appendFile(FILE_A2).appendFile(FILE_A2).appendFile(FILE_A2).commit();

    table
        .newRewrite()
        .deleteFile(FILE_A2)
        .deleteFile(DataFiles.builder(SPEC).copy(FILE_A2).build())
        .deleteFile(FILE_A2)
        .addFile(FILE_A)
        .addFile(DataFiles.builder(SPEC).copy(FILE_A).build())
        .addFile(FILE_A)
        .commit();

    assertThat(table.currentSnapshot().summary())
        .hasSize(15)
        .containsEntry(SnapshotSummary.ADDED_FILES_PROP, "1")
        .containsEntry(SnapshotSummary.ADDED_FILE_SIZE_PROP, "10")
        .containsEntry(SnapshotSummary.ADDED_RECORDS_PROP, "1")
        .containsEntry(SnapshotSummary.CHANGED_PARTITION_COUNT_PROP, "1")
        .containsEntry(SnapshotSummary.DELETED_FILES_PROP, "1")
        .containsEntry(SnapshotSummary.DELETED_RECORDS_PROP, "1")
        .containsEntry(SnapshotSummary.REMOVED_FILE_SIZE_PROP, "10")
        .containsEntry(SnapshotSummary.TOTAL_DATA_FILES_PROP, "1")
        .containsEntry(SnapshotSummary.TOTAL_DELETE_FILES_PROP, "0")
        .containsEntry(SnapshotSummary.TOTAL_EQ_DELETES_PROP, "0")
        .containsEntry(SnapshotSummary.TOTAL_POS_DELETES_PROP, "0")
        .containsEntry(SnapshotSummary.TOTAL_FILE_SIZE_PROP, "10")
        .containsEntry(SnapshotSummary.TOTAL_RECORDS_PROP, "1")
        .containsEntry(SnapshotSummary.TARGET_BRANCH_PROP, "main");
  }

  @TestTemplate
  public void rewriteWithDeletesAndDuplicates() {
    assumeThat(formatVersion).isEqualTo(2);
    assertThat(listManifestFiles()).isEmpty();

    table.newRowDelta().addRows(FILE_A2).addDeletes(FILE_A_DELETES).commit();

    table
        .newRewrite()
        .deleteFile(FILE_A_DELETES)
        .deleteFile(FileMetadata.deleteFileBuilder(SPEC).copy(FILE_A_DELETES).build())
        .deleteFile(FILE_A_DELETES)
        .addFile(FILE_B_DELETES)
        .addFile(FileMetadata.deleteFileBuilder(SPEC).copy(FILE_B_DELETES).build())
        .addFile(FILE_B_DELETES)
        .commit();

    assertThat(table.currentSnapshot().summary())
        .hasSize(17)
        .containsEntry(SnapshotSummary.ADDED_DELETE_FILES_PROP, "1")
        .containsEntry(SnapshotSummary.ADDED_FILE_SIZE_PROP, "10")
        .containsEntry(SnapshotSummary.ADD_POS_DELETE_FILES_PROP, "1")
        .containsEntry(SnapshotSummary.ADDED_POS_DELETES_PROP, "1")
        .containsEntry(SnapshotSummary.CHANGED_PARTITION_COUNT_PROP, "2")
        .containsEntry(SnapshotSummary.REMOVED_DELETE_FILES_PROP, "1")
        .containsEntry(SnapshotSummary.REMOVED_FILE_SIZE_PROP, "10")
        .containsEntry(SnapshotSummary.REMOVED_POS_DELETE_FILES_PROP, "1")
        .containsEntry(SnapshotSummary.REMOVED_POS_DELETES_PROP, "1")
        .containsEntry(SnapshotSummary.TOTAL_DATA_FILES_PROP, "1")
        .containsEntry(SnapshotSummary.TOTAL_DELETE_FILES_PROP, "1")
        .containsEntry(SnapshotSummary.TOTAL_EQ_DELETES_PROP, "0")
        .containsEntry(SnapshotSummary.TOTAL_POS_DELETES_PROP, "1")
        .containsEntry(SnapshotSummary.TOTAL_FILE_SIZE_PROP, "20")
        .containsEntry(SnapshotSummary.TOTAL_RECORDS_PROP, "1")
        .containsEntry(SnapshotSummary.TARGET_BRANCH_PROP, "main");
  }

  @TestTemplate
  public void testFileSizeSummaryWithDVs() {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(3);

    DeleteFile dv1 = newDV(FILE_A);
    table.newRowDelta().addDeletes(dv1).commit();

    DeleteFile dv2 = newDV(FILE_B);
    table.newRowDelta().addDeletes(dv2).commit();

    Map<String, String> summary1 = table.currentSnapshot().summary();
    long addedPosDeletes1 = dv2.recordCount();
    long addedFileSize1 = dv2.contentSizeInBytes();
    long totalPosDeletes1 = dv1.recordCount() + dv2.recordCount();
    long totalFileSize1 = dv1.contentSizeInBytes() + dv2.contentSizeInBytes();
    assertThat(summary1)
        .hasSize(13)
        .doesNotContainKey(SnapshotSummary.ADD_POS_DELETE_FILES_PROP)
        .doesNotContainKey(SnapshotSummary.REMOVED_POS_DELETE_FILES_PROP)
        .containsEntry(SnapshotSummary.ADDED_DELETE_FILES_PROP, "1")
        .doesNotContainKey(SnapshotSummary.REMOVED_DELETE_FILES_PROP)
        .containsEntry(SnapshotSummary.ADDED_DVS_PROP, "1")
        .doesNotContainKey(SnapshotSummary.REMOVED_DVS_PROP)
        .containsEntry(SnapshotSummary.ADDED_POS_DELETES_PROP, String.valueOf(addedPosDeletes1))
        .doesNotContainKey(SnapshotSummary.REMOVED_POS_DELETES_PROP)
        .containsEntry(SnapshotSummary.ADDED_FILE_SIZE_PROP, String.valueOf(addedFileSize1))
        .doesNotContainKey(SnapshotSummary.REMOVED_FILE_SIZE_PROP)
        .containsEntry(SnapshotSummary.TOTAL_DELETE_FILES_PROP, "2")
        .containsEntry(SnapshotSummary.TOTAL_POS_DELETES_PROP, String.valueOf(totalPosDeletes1))
        .containsEntry(SnapshotSummary.TOTAL_FILE_SIZE_PROP, String.valueOf(totalFileSize1))
        .containsEntry(SnapshotSummary.TOTAL_DATA_FILES_PROP, "0")
        .containsEntry(SnapshotSummary.TOTAL_EQ_DELETES_PROP, "0")
        .containsEntry(SnapshotSummary.TOTAL_RECORDS_PROP, "0")
        .containsEntry(SnapshotSummary.CHANGED_PARTITION_COUNT_PROP, "1")
        .containsEntry(SnapshotSummary.TARGET_BRANCH_PROP, "main");

    DeleteFile dv3 = newDV(FILE_A);
    table
        .newRowDelta()
        .removeDeletes(dv1)
        .removeDeletes(dv2)
        .addDeletes(dv3)
        .validateFromSnapshot(table.currentSnapshot().snapshotId())
        .commit();

    Map<String, String> summary2 = table.currentSnapshot().summary();
    long addedPosDeletes2 = dv3.recordCount();
    long removedPosDeletes2 = dv1.recordCount() + dv2.recordCount();
    long addedFileSize2 = dv3.contentSizeInBytes();
    long removedFileSize2 = dv1.contentSizeInBytes() + dv2.contentSizeInBytes();
    long totalPosDeletes2 = dv3.recordCount();
    long totalFileSize2 = dv3.contentSizeInBytes();
    assertThat(summary2)
        .hasSize(17)
        .doesNotContainKey(SnapshotSummary.ADD_POS_DELETE_FILES_PROP)
        .doesNotContainKey(SnapshotSummary.REMOVED_POS_DELETE_FILES_PROP)
        .containsEntry(SnapshotSummary.ADDED_DELETE_FILES_PROP, "1")
        .containsEntry(SnapshotSummary.REMOVED_DELETE_FILES_PROP, "2")
        .containsEntry(SnapshotSummary.ADDED_DVS_PROP, "1")
        .containsEntry(SnapshotSummary.REMOVED_DVS_PROP, "2")
        .containsEntry(SnapshotSummary.ADDED_POS_DELETES_PROP, String.valueOf(addedPosDeletes2))
        .containsEntry(SnapshotSummary.REMOVED_POS_DELETES_PROP, String.valueOf(removedPosDeletes2))
        .containsEntry(SnapshotSummary.ADDED_FILE_SIZE_PROP, String.valueOf(addedFileSize2))
        .containsEntry(SnapshotSummary.REMOVED_FILE_SIZE_PROP, String.valueOf(removedFileSize2))
        .containsEntry(SnapshotSummary.TOTAL_DELETE_FILES_PROP, "1")
        .containsEntry(SnapshotSummary.TOTAL_POS_DELETES_PROP, String.valueOf(totalPosDeletes2))
        .containsEntry(SnapshotSummary.TOTAL_FILE_SIZE_PROP, String.valueOf(totalFileSize2))
        .containsEntry(SnapshotSummary.TOTAL_DATA_FILES_PROP, "0")
        .containsEntry(SnapshotSummary.TOTAL_EQ_DELETES_PROP, "0")
        .containsEntry(SnapshotSummary.TOTAL_RECORDS_PROP, "0")
        .containsEntry(SnapshotSummary.CHANGED_PARTITION_COUNT_PROP, "2")
        .containsEntry(SnapshotSummary.TARGET_BRANCH_PROP, "main");
  }

  @TestTemplate
  public void rewriteManifestsWithDuplicateFiles() {
    assertThat(listManifestFiles()).isEmpty();

    table.newAppend().appendFile(FILE_A).commit();
    table.newAppend().appendFile(FILE_B).commit();
    table.newAppend().appendFile(FILE_C).commit();

    table.rewriteManifests().clusterBy(file -> "file").rewriteIf(ignored -> true).commit();

    assertThat(table.currentSnapshot().summary())
        .hasSize(13)
        .containsEntry(SnapshotSummary.CHANGED_PARTITION_COUNT_PROP, "0")
        .containsEntry(SnapshotSummary.TOTAL_DATA_FILES_PROP, "3")
        .containsEntry(SnapshotSummary.TOTAL_DELETE_FILES_PROP, "0")
        .containsEntry(SnapshotSummary.TOTAL_EQ_DELETES_PROP, "0")
        .containsEntry(SnapshotSummary.TOTAL_POS_DELETES_PROP, "0")
        .containsEntry(SnapshotSummary.TOTAL_FILE_SIZE_PROP, "30")
        .containsEntry(SnapshotSummary.TOTAL_RECORDS_PROP, "3")
        .containsEntry(SnapshotSummary.PROCESSED_MANIFEST_ENTRY_COUNT, "3")
        .containsEntry(SnapshotSummary.CREATED_MANIFESTS_COUNT, "1")
        .containsEntry(SnapshotSummary.KEPT_MANIFESTS_COUNT, "0")
        .containsEntry(SnapshotSummary.REPLACED_MANIFESTS_COUNT, "3")
        .containsEntry(SnapshotSummary.TARGET_BRANCH_PROP, "main");
  }
}
