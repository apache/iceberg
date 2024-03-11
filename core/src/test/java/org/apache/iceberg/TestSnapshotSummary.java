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
import java.util.Map;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSnapshotSummary extends TestBase {

  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(1, 2);
  }

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
        .addFile(FILE_D)
        .commit();
    summary = table.currentSnapshot().summary();
    assertThat(summary)
        .containsEntry(SnapshotSummary.ADDED_FILE_SIZE_PROP, "30")
        .containsEntry(SnapshotSummary.REMOVED_FILE_SIZE_PROP, "20")
        .containsEntry(SnapshotSummary.TOTAL_FILE_SIZE_PROP, "30");

    table.newDelete().deleteFile(FILE_C).deleteFile(FILE_D).commit();
    summary = table.currentSnapshot().summary();
    assertThat(summary)
        .containsEntry(SnapshotSummary.REMOVED_FILE_SIZE_PROP, "20")
        .containsEntry(SnapshotSummary.TOTAL_FILE_SIZE_PROP, "10")
        .doesNotContainKey(SnapshotSummary.ADDED_FILE_SIZE_PROP);
  }

  @TestTemplate
  public void testFileSizeSummaryWithDeletes() {
    if (formatVersion == 1) {
      return;
    }

    table.newRowDelta().addDeletes(FILE_A_DELETES).addDeletes(FILE_A2_DELETES).commit();

    table.refresh();
    Map<String, String> summary = table.currentSnapshot().summary();
    assertThat(summary)
        .containsEntry(SnapshotSummary.ADD_EQ_DELETE_FILES_PROP, "1")
        .containsEntry(SnapshotSummary.ADD_POS_DELETE_FILES_PROP, "1");
  }
}
