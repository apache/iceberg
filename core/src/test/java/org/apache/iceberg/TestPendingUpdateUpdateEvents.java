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

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestPendingUpdateUpdateEvents extends TestBase {
  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(1, 2, 3);
  }

  @TestTemplate
  public void testFastAppendDoesNotPerformExtraMetadataReads() {
    table.newFastAppend().appendFile(FILE_A).commit();
    long startSnapshotId = table.currentSnapshot().snapshotId();

    AppendFiles appendFiles = table.newFastAppend().appendFile(FILE_A);
    int commitOnlyFetchCount = recordMetadataFetchesForCommitOnly(appendFiles);

    assertThat(table.currentSnapshot().snapshotId()).isNotEqualTo(startSnapshotId);
    // fetch metadata once
    assertThat(commitOnlyFetchCount).isEqualTo(1);
  }

  @TestTemplate
  public void testCherryPickOperationDoesNotPerformExtraMetadataReads() {
    table.newFastAppend().appendFile(FILE_A).commit();
    long startSnapshotId = table.currentSnapshot().snapshotId();

    table.newAppend().appendFile(FILE_B).stageOnly().commit();
    Snapshot stagedSnapshot = readMetadata().snapshots().get(1);

    ManageSnapshots manageSnapshots =
        table.manageSnapshots().cherrypick(stagedSnapshot.snapshotId());
    int commitOnlyFetchCount = recordMetadataFetchesForCommitOnly(manageSnapshots);

    assertThat(table.currentSnapshot().snapshotId()).isNotEqualTo(startSnapshotId);
    // fetch metadata twice, once in BaseTransaction#applyUpdates and once normally
    assertThat(commitOnlyFetchCount).isEqualTo(2);
  }

  private <T> int recordMetadataFetchesForCommitOnly(PendingUpdate<T> pendingUpdate) {
    pendingUpdate.apply();

    // determine number of metadata fetches during apply
    int beforeApplyFetchCount = table.ops().getMetadataFetchCount();
    pendingUpdate.apply();
    int afterApplyFetchCount = table.ops().getMetadataFetchCount();
    int applyOnlyFetchCount = afterApplyFetchCount - beforeApplyFetchCount;

    // determine number of metadata fetches during commit
    pendingUpdate.commit();
    int afterCommitFetchCount = table.ops().getMetadataFetchCount();
    return afterCommitFetchCount - afterApplyFetchCount - applyOnlyFetchCount;
  }
}
