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

import static org.apache.iceberg.SnapshotSummary.PUBLISHED_WAP_ID_PROP;
import static org.apache.iceberg.SnapshotSummary.TARGET_BRANCH_PROP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
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
  public void testCommitValidationPreventsCommit() throws IOException {
    table.newAppend().commit();
    String validationMessage = "Validation force failed";

    // Create a CommitValidator that will reject commits
    SnapshotAncestryValidator validator =
        new SnapshotAncestryValidator() {
          @Override
          public Boolean apply(Iterable<Snapshot> baseSnapshots) {
            return false;
          }

          @Nonnull
          @Override
          public String errorMessage() {
            return validationMessage;
          }
        };

    // Test that the validator rejects commit
    AppendFiles append1 = table.newAppend().validateWith(validator).appendFile(FILE_A);
    assertThatThrownBy(append1::commit)
        .isInstanceOf(ValidationException.class)
        .hasMessage("Snapshot ancestry validation failed: " + validationMessage);

    // Verify the file was not committed
    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(0);
  }

  @TestTemplate
  public void testCommitValidationWithCustomSummaryProperties() throws IOException {
    String wapId = "wap-12345-staging-audit";

    // Create a validator that checks custom summary properties
    SnapshotAncestryValidator customPropertyValidator =
        baseSnapshots -> {
          List<String> publishedWapIds =
              Streams.stream(baseSnapshots)
                  .filter(snapshot -> snapshot.summary().containsKey(PUBLISHED_WAP_ID_PROP))
                  .map(snapshot -> snapshot.summary().get(PUBLISHED_WAP_ID_PROP))
                  .collect(Collectors.toList());

          return !publishedWapIds.contains(wapId);
        };

    // Add a file with and set a published WAP id
    table
        .newFastAppend()
        .validateWith(customPropertyValidator)
        .appendFile(FILE_A)
        .set(PUBLISHED_WAP_ID_PROP, wapId)
        .commit();

    // Verify the current state of the table
    assertThat(table.currentSnapshot().summary().get(PUBLISHED_WAP_ID_PROP)).isEqualTo(wapId);

    // Attempt to add the same published WAP id
    AppendFiles append2 =
        table
            .newFastAppend()
            .validateWith(customPropertyValidator)
            .appendFile(FILE_A)
            .set(PUBLISHED_WAP_ID_PROP, wapId);

    assertThatThrownBy(append2::commit)
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Snapshot ancestry validation failed");

    // Verify the table wasn't updated
    assertThat(table.snapshots()).hasSize(1);
  }

  @TestTemplate
  public void testPopulateTargetBranchSnapshotProperty() {
    // Commit to main branch
    table.newAppend().appendFile(FILE_A).commit();
    Snapshot mainSnapshot = table.currentSnapshot();
    assertThat(mainSnapshot.summary()).containsEntry(TARGET_BRANCH_PROP, SnapshotRef.MAIN_BRANCH);

    // Create and commit to branch1
    String branch1 = "branch1";
    table.manageSnapshots().createBranch(branch1, mainSnapshot.snapshotId()).commit();
    table.newAppend().appendFile(FILE_B).toBranch(branch1).commit();
    Snapshot branch1Snapshot = table.snapshot(branch1);
    assertThat(branch1Snapshot.summary()).containsEntry(TARGET_BRANCH_PROP, branch1);

    // Create and commit to branch2
    String branch2 = "branch2";
    table.manageSnapshots().createBranch(branch2, mainSnapshot.snapshotId()).commit();
    table.newAppend().appendFile(FILE_C).toBranch(branch2).commit();
    Snapshot branch2Snapshot = table.snapshot(branch2);
    assertThat(branch2Snapshot.summary()).containsEntry(TARGET_BRANCH_PROP, branch2);

    // Verify all snapshots have correct target branch
    assertThat(table.snapshot(mainSnapshot.snapshotId()).summary())
        .containsEntry(TARGET_BRANCH_PROP, SnapshotRef.MAIN_BRANCH);
    assertThat(table.snapshot(branch1Snapshot.snapshotId()).summary())
        .containsEntry(TARGET_BRANCH_PROP, branch1);
    assertThat(table.snapshot(branch2Snapshot.snapshotId()).summary())
        .containsEntry(TARGET_BRANCH_PROP, branch2);
  }
}
