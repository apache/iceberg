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

import org.apache.iceberg.util.SnapshotUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestSnapshotUtil extends TableTestBase {
  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[]{1, 2};
  }

  public TestSnapshotUtil(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testOldestSnapshotForEmptyTable() {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());
    Snapshot oldestSnapshot = SnapshotUtil.oldestSnapshot(table);
    Assert.assertNull("The oldest snapshot should be null", oldestSnapshot);
  }

  @Test
  public void testOldestSnapshotWithoutExpiredAction() {
    table.newFastAppend()
        .appendFile(FILE_A)
        .commit();
    Snapshot firstSnapshot = table.currentSnapshot();

    Snapshot oldestSnapshot = SnapshotUtil.oldestSnapshot(table);
    Assert.assertEquals("Table oldest snapshot should be first snapshot",
        firstSnapshot.snapshotId(), oldestSnapshot.snapshotId());

    table.newFastAppend()
        .appendFile(FILE_B)
        .commit();

    Assert.assertEquals("Table oldest snapshot should be first snapshot",
        firstSnapshot.snapshotId(), oldestSnapshot.snapshotId());
  }

  @Test
  public void testSingleCommitOldestSnapshotWithExpiredAction() {
    table.newFastAppend()
        .appendFile(FILE_A)
        .commit();
    Snapshot firstSnapshot = table.currentSnapshot();

    table.expireSnapshots().expireOlderThan(System.currentTimeMillis()).retainLast(1).commit();
    table.refresh();
    Snapshot oldestSnapshot = SnapshotUtil.oldestSnapshot(table);

    Assert.assertEquals("Table oldest snapshot should be first snapshot",
        firstSnapshot.snapshotId(), oldestSnapshot.snapshotId());
  }

  @Test
  public void testMultipleCommitOldestSnapshotWithExpiredAction() {
    table.newFastAppend()
        .appendFile(FILE_A)
        .commit();
    Snapshot firstSnapshot = table.currentSnapshot();

    table.newFastAppend()
        .appendFile(FILE_B)
        .commit();
    Snapshot secondSnapshot = table.currentSnapshot();

    table.newFastAppend()
        .appendFile(FILE_C)
        .commit();
    Snapshot thirdSnapshot = table.currentSnapshot();

    table.expireSnapshots().expireOlderThan(System.currentTimeMillis()).retainLast(2).commit();
    table.refresh();
    Snapshot oldestSnapshot = SnapshotUtil.oldestSnapshot(table);

    Assert.assertEquals("Table oldest snapshot should be second snapshot",
        secondSnapshot.snapshotId(), oldestSnapshot.snapshotId());
  }

}
