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
import static org.apache.iceberg.avro.AvroTestHelpers.readAvroCodec;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
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
          public boolean validate(Iterable<Snapshot> baseSnapshots) {
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
  public void manifestNotCleanedUpWhenSnapshotNotLoadableAfterCommit() {
    // Uses a custom TableOps that returns stale metadata (without the new snapshot) on the
    // first refresh() after commit, simulating eventual consistency. Verifies that commit succeeds
    // and that the committed data is visible once the table is refreshed again
    String tableName = "stale-table-on-first-refresh";
    TestTables.TestTableOperations ops = opsWithStaleRefreshAfterCommit(tableName, tableDir);
    TestTables.TestTable tableWithStaleRefresh =
        TestTables.create(
            tableDir, tableName, SCHEMA, SPEC, SortOrder.unsorted(), formatVersion, ops);

    // the first refresh() after the commit will return stale metadata (without this snapshot), so
    // SnapshotProducer will skip cleanup to avoid accidentally deleting files that are part of the
    // committed snapshot but commit still succeeds
    tableWithStaleRefresh.newAppend().appendFile(FILE_A).commit();

    // Refresh again to get the real metadata; the snapshot must be visible now
    tableWithStaleRefresh.ops().refresh();
    Snapshot snapshot = tableWithStaleRefresh.currentSnapshot();
    assertThat(snapshot)
        .as("Committed snapshot must be visible after refresh (eventual consistency resolved)")
        .isNotNull();

    File metadata = Paths.get(tableDir.getPath(), "metadata").toFile();
    assertThat(snapshot.allManifests(tableWithStaleRefresh.io()))
        .isNotEmpty()
        .allSatisfy(
            manifest -> assertThat(metadata.listFiles()).contains(new File(manifest.path())));
  }

  /**
   * Creates a TableOperations that returns stale metadata (without the newly committed snapshot) on
   * the first refresh() after a commit. This simulates eventual consistency where the committed
   * snapshot is not yet visible. Used to verify that when the snapshot cannot be loaded after
   * commit, cleanup is skipped to avoid accidentally deleting files that are part of the committed
   * snapshot.
   */
  private static TestTables.TestTableOperations opsWithStaleRefreshAfterCommit(
      String name, File location) {
    return new TestTables.TestTableOperations(name, location) {
      private TableMetadata metadataToReturnOnNextRefresh;

      @Override
      public void commit(TableMetadata base, TableMetadata updatedMetadata) {
        super.commit(base, updatedMetadata);
        if (base != null) {
          // return stale metadata on the first refresh() call
          this.metadataToReturnOnNextRefresh = base;
        }
      }

      @Override
      public TableMetadata refresh() {
        if (metadataToReturnOnNextRefresh != null) {
          this.current = metadataToReturnOnNextRefresh;
          this.metadataToReturnOnNextRefresh = null;
          return current;
        }

        return super.refresh();
      }
    };
  }

  @TestTemplate
  public void testDefaultManifestCompression() throws IOException {
    table.newFastAppend().appendFile(FILE_A).commit();

    ManifestFile manifest = table.currentSnapshot().dataManifests(table.io()).get(0);
    assertThat(readAvroCodec(new File(manifest.path()))).isEqualTo("deflate");
  }

  @TestTemplate
  public void testManifestCompressionFromTableProperty() throws IOException {
    table.updateProperties().set(TableProperties.MANIFEST_COMPRESSION, "snappy").commit();

    table.newFastAppend().appendFile(FILE_A).commit();

    ManifestFile manifest = table.currentSnapshot().dataManifests(table.io()).get(0);
    assertThat(readAvroCodec(new File(manifest.path()))).isEqualTo("snappy");
  }
}
