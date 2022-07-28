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

import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestSnapshotSummary extends TableTestBase {
  public TestSnapshotSummary(int formatVersion) {
    super(formatVersion);
  }

  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] {1, 2};
  }

  @Test
  public void testFileSizeSummary() {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    // fast append
    table.newFastAppend().appendFile(FILE_A).commit();
    Map<String, String> summary = table.currentSnapshot().summary();
    Assert.assertEquals("10", summary.get(SnapshotSummary.ADDED_FILE_SIZE_PROP));
    Assert.assertNull(summary.get(SnapshotSummary.REMOVED_FILE_SIZE_PROP));
    Assert.assertEquals("10", summary.get(SnapshotSummary.TOTAL_FILE_SIZE_PROP));

    // merge append
    table.newAppend().appendFile(FILE_B).commit();
    summary = table.currentSnapshot().summary();
    Assert.assertEquals("10", summary.get(SnapshotSummary.ADDED_FILE_SIZE_PROP));
    Assert.assertNull(summary.get(SnapshotSummary.REMOVED_FILE_SIZE_PROP));
    Assert.assertEquals("20", summary.get(SnapshotSummary.TOTAL_FILE_SIZE_PROP));

    table
        .newOverwrite()
        .deleteFile(FILE_A)
        .deleteFile(FILE_B)
        .addFile(FILE_C)
        .addFile(FILE_D)
        .addFile(FILE_D)
        .commit();
    summary = table.currentSnapshot().summary();
    Assert.assertEquals("30", summary.get(SnapshotSummary.ADDED_FILE_SIZE_PROP));
    Assert.assertEquals("20", summary.get(SnapshotSummary.REMOVED_FILE_SIZE_PROP));
    Assert.assertEquals("30", summary.get(SnapshotSummary.TOTAL_FILE_SIZE_PROP));

    table.newDelete().deleteFile(FILE_C).deleteFile(FILE_D).commit();
    summary = table.currentSnapshot().summary();
    Assert.assertNull(summary.get(SnapshotSummary.ADDED_FILE_SIZE_PROP));
    Assert.assertEquals("20", summary.get(SnapshotSummary.REMOVED_FILE_SIZE_PROP));
    Assert.assertEquals("10", summary.get(SnapshotSummary.TOTAL_FILE_SIZE_PROP));
  }

  @Test
  public void testFileSizeSummaryWithDeletes() {
    if (formatVersion == 1) {
      return;
    }

    table.newRowDelta().addDeletes(FILE_A_DELETES).addDeletes(FILE_A2_DELETES).commit();

    table.refresh();
    Map<String, String> summary = table.currentSnapshot().summary();
    Assert.assertEquals("1", summary.get(SnapshotSummary.ADD_EQ_DELETE_FILES_PROP));
    Assert.assertEquals("1", summary.get(SnapshotSummary.ADD_POS_DELETE_FILES_PROP));
  }
}
