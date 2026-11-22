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
package org.apache.iceberg.actions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.TestBase;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.exceptions.ValidationException;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
class TestRewriteDataFilesCommitManager extends TestBase {
  @TestTemplate
  void expiredStartingSnapshotHasClearError() {
    table.newAppend().appendFile(FILE_A).commit();
    long startingSnapshotId = table.currentSnapshot().snapshotId();
    RewriteDataFilesCommitManager manager =
        new RewriteDataFilesCommitManager(table, startingSnapshotId);
    table.newAppend().appendFile(FILE_B).commit();
    table.expireSnapshots().expireSnapshotId(startingSnapshotId).commit();

    assertThatThrownBy(() -> manager.commitFileGroups(Set.of(rewriteGroup(FILE_C))))
        .isInstanceOf(ValidationException.class)
        .hasMessage(
            "Cannot rewrite files: starting snapshot %s is no longer available",
            startingSnapshotId);
  }

  @TestTemplate
  void partialProgressToleratesExpiredStartingSnapshotAndCleansOutput() throws IOException {
    table.newAppend().appendFile(FILE_A).commit();
    long startingSnapshotId = table.currentSnapshot().snapshotId();
    RewriteDataFilesCommitManager manager =
        new RewriteDataFilesCommitManager(table, startingSnapshotId);
    Path output = Files.createFile(temp.resolve("rewritten.parquet"));
    DataFile addedFile =
        DataFiles.builder(SPEC)
            .withPath(output.toString())
            .withFileSizeInBytes(0)
            .withRecordCount(1)
            .withPartitionPath("data_bucket=0")
            .build();
    RewriteFileGroup group = rewriteGroup(addedFile);
    table.newAppend().appendFile(FILE_B).commit();
    table.expireSnapshots().expireSnapshotId(startingSnapshotId).commit();

    RewriteDataFilesCommitManager.CommitService service = manager.service(1);
    service.start();
    service.offer(group);
    service.close();

    assertThat(service.results()).isEmpty();
    assertThat(output).doesNotExist();
  }

  @TestTemplate
  void staleTableValidatesHistoryAndCleansOutput() throws IOException {
    assumeTrue(formatVersion >= 2, "Delete conflict validation requires format v2 or later");
    table.newAppend().appendFile(FILE_A).commit();
    long startingSnapshotId = table.currentSnapshot().snapshotId();
    RewriteDataFilesCommitManager manager =
        new RewriteDataFilesCommitManager(table, startingSnapshotId);
    TestTables.TestTable concurrentTable = TestTables.load(tableDir, "test");
    concurrentTable.newAppend().appendFile(FILE_B).commit();
    long intermediateSnapshotId = concurrentTable.currentSnapshot().snapshotId();
    concurrentTable.newAppend().appendFile(FILE_C).commit();
    concurrentTable
        .expireSnapshots()
        .expireSnapshotId(startingSnapshotId)
        .expireSnapshotId(intermediateSnapshotId)
        .commit();

    assertThat(concurrentTable.snapshot(startingSnapshotId)).isNull();
    assertThat(table.snapshot(startingSnapshotId)).isNotNull();

    Path output = Files.createFile(temp.resolve("stale-rewritten.parquet"));
    DataFile addedFile =
        DataFiles.builder(SPEC)
            .withPath(output.toString())
            .withFileSizeInBytes(0)
            .withRecordCount(1)
            .withPartitionPath("data_bucket=0")
            .build();

    assertThatThrownBy(() -> manager.commitOrClean(Set.of(rewriteGroup(addedFile))))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith(
            "Cannot determine history between starting snapshot %s", startingSnapshotId);
    assertThat(output).doesNotExist();
    assertThat(table.snapshot(startingSnapshotId)).isNull();
  }

  private RewriteFileGroup rewriteGroup(DataFile addedFile) {
    RewriteFileGroup group = mock(RewriteFileGroup.class);
    when(group.rewrittenFiles()).thenReturn(Set.of(FILE_A));
    when(group.addedFiles()).thenReturn(Set.of(addedFile));
    when(group.danglingDVs()).thenReturn(Set.of());
    return group;
  }
}
