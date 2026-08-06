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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TestBase;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.DataFileSet;
import org.apache.iceberg.util.DeleteFileSet;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestRewriteDataFilesCommitManager extends TestBase {

  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(2);
  }

  @TestTemplate
  public void testCommitFailsWhenStartingSnapshotExpired() {
    // 1. Create a snapshot with data files
    table.newAppend().appendFile(FILE_A).commit();
    Snapshot startingSnapshot = table.currentSnapshot();
    long startingSnapshotId = startingSnapshot.snapshotId();

    // 2. Make another commit to advance the table
    table.newAppend().appendFile(FILE_B).commit();

    // 3. Expire the starting snapshot
    table.expireSnapshots().expireSnapshotId(startingSnapshotId).commit();

    // Confirm the starting snapshot is now null
    assertThat(table.snapshot(startingSnapshotId)).isNull();

    // 4. Create a commit manager pointing at the expired starting snapshot
    RewriteDataFilesCommitManager commitManager =
        new RewriteDataFilesCommitManager(table, startingSnapshotId, true);

    // 5. Create a mock RewriteFileGroup
    RewriteFileGroup mockGroup = mock(RewriteFileGroup.class);
    when(mockGroup.rewrittenFiles()).thenReturn(DataFileSet.of(ImmutableSet.of(FILE_A)));
    when(mockGroup.addedFiles()).thenReturn(DataFileSet.of(ImmutableSet.of(FILE_B)));
    when(mockGroup.danglingDVs()).thenReturn(DeleteFileSet.create());

    Set<RewriteFileGroup> fileGroups = Sets.newHashSet(mockGroup);

    // 6. Attempt commitFileGroups - should throw IllegalStateException about expired snapshot
    assertThatThrownBy(() -> commitManager.commitFileGroups(fileGroups))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("starting snapshot")
        .hasMessageContaining("expired");
  }

  @TestTemplate
  public void testCommitServicePropagatesExpiredSnapshotError() {
    // 1. Create a snapshot with data files
    table.newAppend().appendFile(FILE_A).commit();
    Snapshot startingSnapshot = table.currentSnapshot();
    long startingSnapshotId = startingSnapshot.snapshotId();

    // 2. Make another commit to advance the table
    table.newAppend().appendFile(FILE_B).commit();

    // 3. Expire the starting snapshot
    table.expireSnapshots().expireSnapshotId(startingSnapshotId).commit();

    // 4. Create a commit manager with partial progress service
    RewriteDataFilesCommitManager commitManager =
        new RewriteDataFilesCommitManager(table, startingSnapshotId, true);
    RewriteDataFilesCommitManager.CommitService commitService = commitManager.service(1);
    commitService.start();

    // 5. Create a mock RewriteFileGroup
    RewriteFileGroup mockGroup = mock(RewriteFileGroup.class);
    when(mockGroup.rewrittenFiles()).thenReturn(DataFileSet.of(ImmutableSet.of(FILE_A)));
    when(mockGroup.addedFiles()).thenReturn(DataFileSet.of(ImmutableSet.of(FILE_B)));
    when(mockGroup.danglingDVs()).thenReturn(DeleteFileSet.create());

    // 6. Offer the group - with rewritesPerCommit=1, the commit is attempted immediately
    //    and the IllegalStateException should propagate rather than being silently swallowed
    assertThatThrownBy(() -> commitService.offer(mockGroup))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("starting snapshot")
        .hasMessageContaining("expired");
  }
}
