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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import org.apache.iceberg.SupportsCommitValidation.CommitValidator;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.util.PropertyUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSnapshotProducer extends TestBase {

  @Test
  public void testManifestFileGroupSize() {
    assertManifestWriterCount(
        4 /* worker pool size */,
        100 /* file count */,
        1 /* manifest writer count */,
        "Must use 1 writer if file count is small");

    assertManifestWriterCount(
        4 /* worker pool size */,
        SnapshotProducer.MIN_FILE_GROUP_SIZE /* file count */,
        1 /* manifest writer count */,
        "Must use 1 writer if file count matches min group size");

    assertManifestWriterCount(
        4 /* worker pool size */,
        SnapshotProducer.MIN_FILE_GROUP_SIZE + 1 /* file count */,
        1 /* manifest writer count */,
        "Must use 1 writer if file count is slightly above min group size");

    assertManifestWriterCount(
        4 /* worker pool size */,
        (int) (1.25 * SnapshotProducer.MIN_FILE_GROUP_SIZE) /* file count */,
        1 /* manifest writer count */,
        "Must use 1 writer when file count is < 1.5 * min group size");

    assertManifestWriterCount(
        4 /* worker pool size */,
        (int) (1.5 * SnapshotProducer.MIN_FILE_GROUP_SIZE) /* file count */,
        2 /* manifest writer count */,
        "Must use 2 writers when file count is >= 1.5 * min group size");

    assertManifestWriterCount(
        3 /* worker pool size */,
        100 * SnapshotProducer.MIN_FILE_GROUP_SIZE /* file count */,
        3 /* manifest writer count */,
        "Must limit parallelism to worker pool size when file count is large");

    assertManifestWriterCount(
        32 /* worker pool size */,
        5 * SnapshotProducer.MIN_FILE_GROUP_SIZE /* file count */,
        5 /* manifest writer count */,
        "Must limit parallelism to avoid tiny manifests");
  }

  private void assertManifestWriterCount(
      int workerPoolSize, int fileCount, int expectedManifestWriterCount, String errMsg) {
    int writerCount = SnapshotProducer.manifestWriterCount(workerPoolSize, fileCount);
    assertThat(writerCount).as(errMsg).isEqualTo(expectedManifestWriterCount);
  }

  @TestTemplate
  public void testCommitValidationPreventingCommit() throws IOException {
    // Commit the first file
    table.newAppend().appendFile(FILE_A).commit();

    // Create a file with no records for testing
    DataFile fileNoRecords =
        DataFiles.builder(SPEC)
            .withPath("/path/to/data-no-records.parquet")
            .withFileSizeInBytes(100)
            .withRecordCount(0) // File with no records
            .build();

    // Create a CommitValidator that will reject commits based on snapshot summary
    CommitValidator validator =
        (baseMetadata, newMetadata) -> {
          long addedRecords =
              PropertyUtil.propertyAsInt(
                  newMetadata.currentSnapshot().summary(), SnapshotSummary.ADDED_RECORDS_PROP, 0);
          long addedFiles =
              PropertyUtil.propertyAsInt(
                  newMetadata.currentSnapshot().summary(), SnapshotSummary.ADDED_FILES_PROP, 0);
          // Reject if no records are added (empty file)
          if (addedFiles >= 1 && addedRecords == 0) {
            throw new CommitFailedException("Cannot add files with no records");
          }
        };

    // Test that the validator rejects commits with no records
    AppendFiles append1 = table.newAppend().appendFile(fileNoRecords);
    assertThatThrownBy(() -> ((SupportsCommitValidation) append1).commit(validator))
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Cannot add files with no records");

    // Verify the file was not committed
    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(1);
    assertThat(table.currentSnapshot().summary().get(SnapshotSummary.TOTAL_DATA_FILES_PROP))
        .isEqualTo("1");

    // Verify files were not committed
    assertThat(table.currentSnapshot().summary().get(SnapshotSummary.TOTAL_DATA_FILES_PROP))
        .isEqualTo("1");

    // Test that a valid commit passes the validator (FILE_B has only 1 record)
    AppendFiles append4 = table.newFastAppend().appendFile(FILE_B);
    ((SupportsCommitValidation) append4).commit(validator);

    // Verify the file was committed successfully
    assertThat(table.currentSnapshot().summary().get(SnapshotSummary.TOTAL_DATA_FILES_PROP))
        .isEqualTo("2");
    assertThat(table.currentSnapshot().summary().get(SnapshotSummary.ADDED_FILES_PROP))
        .isEqualTo("1");
    assertThat(table.currentSnapshot().summary().get(SnapshotSummary.ADDED_RECORDS_PROP))
        .isEqualTo("1");
  }

  @TestTemplate
  public void testCommitValidationWithCustomSummaryProperties() throws IOException {
    // Create a validator that checks custom summary properties
    CommitValidator customPropertyValidator =
        (baseMetadata, newMetadata) -> {
          String operationType = newMetadata.currentSnapshot().summary().get("operation-type");
          if ("restricted".equals(operationType)) {
            throw new CommitFailedException("Restricted operation type not allowed");
          }
        };

    // Add a file with a custom summary property that will be rejected
    AppendFiles append1 =
        table.newFastAppend().appendFile(FILE_A).set("operation-type", "restricted");

    assertThatThrownBy(() -> ((SupportsCommitValidation) append1).commit(customPropertyValidator))
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Restricted operation type not allowed");

    // Verify no snapshot was created
    assertThat(table.currentSnapshot()).isNull();

    // Add the file with an allowed operation type
    AppendFiles append2 = table.newFastAppend().appendFile(FILE_A).set("operation-type", "allowed");

    ((SupportsCommitValidation) append2).commit(customPropertyValidator);

    // Verify the snapshot was created with the custom property
    assertThat(table.currentSnapshot()).isNotNull();
    assertThat(table.currentSnapshot().summary().get("operation-type")).isEqualTo("allowed");
  }
}
