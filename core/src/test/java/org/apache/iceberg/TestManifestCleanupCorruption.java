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

import java.io.File;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestManifestCleanupCorruption extends TestBase {

  /**
   * Test that reproduces the exact bug scenario: - First commit attempt succeeds but throws
   * exception (simulating network error) - Retry detects no-op (snapshot already committed)
   */
  @TestTemplate
  public void testManifestListCleanupWithCommitSucceedButClientError() throws Exception {
    AtomicInteger commitCount = new AtomicInteger(0);
    AtomicInteger deleteCount = new AtomicInteger(0);
    AtomicInteger appendCommitCount = new AtomicInteger(0);

    // Create custom table operations that simulate:
    // Table creation: succeeds normally
    // 1st append commit: commit succeeds but throws exception
    // 2nd append commit: commit succeeds normally (retry detects no-op)
    TestTables.TestTableOperations customOps =
        new TestTables.TestTableOperations(
            "test_table", tableDir, new TrackingFileIO(table.io(), deleteCount)) {
          @Override
          public void commit(TableMetadata base, TableMetadata metadata) {
            int count = commitCount.incrementAndGet();
            super.commit(base, metadata);

            // Skip table creation commit (first commit)
            if (count == 1) {
              return;
            }

            // For append commits: first append commit succeeds but throws exception
            int appendCount = appendCommitCount.incrementAndGet();
            if (appendCount == 1) {
              throw new CommitFailedException("Simulated network error after successful commit");
            }
          }
        };

    // Create table with custom operations
    TestTables.TestTable customTable =
        TestTables.create(tableDir, "test_table", SCHEMA, SPEC, SortOrder.unsorted(), 2, customOps);

    // Track manifest lists before operation
    List<String> manifestListsBefore = listManifestListFiles(tableDir);

    // First append - this will trigger the commit-succeed-but-throw scenario
    customTable.newAppend().appendFile(FILE_A).commit();

    // Get the committed snapshot
    Snapshot committedSnapshot = customTable.currentSnapshot();
    assertThat(committedSnapshot).isNotNull();

    String committedManifestList = committedSnapshot.manifestListLocation();

    // Verify the committed manifest list exists
    assertThat(new File(committedManifestList)).as("Committed manifest list should exist").exists();

    // Verify we can read manifests (table is not corrupted)
    List<ManifestFile> manifests = committedSnapshot.allManifests(customTable.io());
    assertThat(manifests).as("Should be able to read manifests").isNotEmpty();

    // List all manifest lists after operation
    List<String> manifestListsAfter = listManifestListFiles(tableDir);

    // The committed manifest list should still exist
    assertThat(manifestListsAfter)
        .as("Committed manifest list should not have been deleted")
        .contains(committedManifestList);

    // Verify exactly 2 commit attempts were made
    assertThat(commitCount.get())
        .as("Should have attempted commit twice (first failed with exception, second succeeded)")
        .isEqualTo(2);
  }

  /** Helper to list all manifest list files in the metadata directory */
  private List<String> listManifestListFiles(File dir) {
    File metadataDir = new File(dir, "metadata");
    if (!metadataDir.exists()) {
      return List.of();
    }

    File[] files =
        metadataDir.listFiles((d, name) -> name.startsWith("snap-") && name.endsWith(".avro"));
    if (files == null) {
      return List.of();
    }

    return java.util.Arrays.stream(files).map(File::getAbsolutePath).collect(Collectors.toList());
  }

  /** FileIO wrapper that tracks delete operations */
  static class TrackingFileIO implements FileIO {
    private final FileIO wrapped;
    private final AtomicInteger deleteCount;

    TrackingFileIO(FileIO wrapped, AtomicInteger deleteCount) {
      this.wrapped = wrapped;
      this.deleteCount = deleteCount;
    }

    @Override
    public org.apache.iceberg.io.InputFile newInputFile(String path) {
      return wrapped.newInputFile(path);
    }

    @Override
    public org.apache.iceberg.io.OutputFile newOutputFile(String path) {
      return wrapped.newOutputFile(path);
    }

    @Override
    public void deleteFile(String path) {
      deleteCount.incrementAndGet();
      wrapped.deleteFile(path);
    }
  }
}
