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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestSnapshotChanges {
  @TempDir private File tableDir;

  // Schema passed to create tables
  public static final Schema SCHEMA =
      new Schema(
          required(3, "id", Types.IntegerType.get()), required(4, "data", Types.StringType.get()));

  // Partition spec used to create tables
  protected static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA).build();

  public TestTables.TestTable table = null;

  @BeforeEach
  public void before() throws Exception {
    new File(tableDir, "metadata");
    this.table = TestTables.create(tableDir, "test", SCHEMA, SPEC, 2);
  }

  @AfterEach
  public void cleanupTables() {
    TestTables.clearTables();
  }

  @Test
  public void testAddedDataFiles() {
    DataFile addedFile =
        DataFiles.builder(SPEC)
            .withPath("/path/to/test-data.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();

    table.newFastAppend().appendFile(addedFile).commit();
    Snapshot snapshotWithAddedFile = table.currentSnapshot();

    // Test using SnapshotChanges object directly
    SnapshotChanges changes =
        SnapshotChanges.builder(snapshotWithAddedFile, table.io(), table.specs()).build();
    Iterable<DataFile> filesFromChanges = changes.addedDataFiles();
    assertThat(filesFromChanges).hasSize(1);

    // Verify the file path matches
    DataFile resultFile = filesFromChanges.iterator().next();
    assertThat(resultFile.path().toString()).isEqualTo(addedFile.path().toString());
  }

  @Test
  public void testRemovedDataFiles() {
    DataFile fileToRemove =
        DataFiles.builder(SPEC)
            .withPath("/path/to/file-to-remove.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();

    DataFile fileToKeep =
        DataFiles.builder(SPEC)
            .withPath("/path/to/file-to-keep.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();

    // Add both files
    table.newAppend().appendFile(fileToRemove).appendFile(fileToKeep).commit();

    // Remove one file
    table.newDelete().deleteFile(fileToRemove).commit();

    Snapshot snapshotAfterDelete = table.currentSnapshot();

    // Test using SnapshotChanges object directly (for caching multiple calls)
    SnapshotChanges changes =
        SnapshotChanges.builder(snapshotAfterDelete, table.io(), table.specs()).build();
    Iterable<DataFile> filesFromChangesFirstCall = changes.removedDataFiles();
    Iterable<DataFile> filesFromChangesSecondCall = changes.removedDataFiles();
    assertThat(filesFromChangesFirstCall).isSameAs(filesFromChangesSecondCall);

    // Verify the file path matches
    DataFile resultFile = filesFromChangesFirstCall.iterator().next();
    assertThat(resultFile.path().toString()).isEqualTo(fileToRemove.path().toString());
  }

  @Test
  public void testSnapshotChangesCaching() {
    DataFile firstFile =
        DataFiles.builder(SPEC)
            .withPath("/path/to/file1.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();

    DataFile secondFile =
        DataFiles.builder(SPEC)
            .withPath("/path/to/file2.parquet")
            .withFileSizeInBytes(20)
            .withRecordCount(2)
            .build();

    table.newAppend().appendFile(firstFile).appendFile(secondFile).commit();
    table.newDelete().deleteFile(firstFile).commit();

    Snapshot snapshotAfterDelete = table.currentSnapshot();

    SnapshotChanges changes =
        SnapshotChanges.builder(snapshotAfterDelete, table.io(), table.specs()).build();

    // First call should cache the data file changes
    Iterable<DataFile> firstCallResult = changes.removedDataFiles();
    assertThat(firstCallResult).hasSize(1);

    // Second call should return the cached results
    Iterable<DataFile> secondCallResult = changes.removedDataFiles();
    assertThat(secondCallResult).hasSize(1);

    // Both calls should return the same reference (cached)
    assertThat(firstCallResult).isSameAs(secondCallResult);
  }
}
