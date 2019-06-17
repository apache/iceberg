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

import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Test;

public class TestSnapshotSelection extends TableTestBase {

  @Test
  public void testSnapshotSelectionById() {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    table.newFastAppend()
        .appendFile(FILE_A)
        .commit();
    Snapshot firstSnapshot = table.currentSnapshot();

    table.newFastAppend()
        .appendFile(FILE_B)
        .commit();
    Snapshot secondSnapshot = table.currentSnapshot();

    Assert.assertEquals("Table should have two snapshots", 2, Iterables.size(table.snapshots()));
    validateSnapshot(null, table.snapshot(firstSnapshot.snapshotId()), FILE_A);
    validateSnapshot(firstSnapshot, table.snapshot(secondSnapshot.snapshotId()), FILE_B);
  }
}
