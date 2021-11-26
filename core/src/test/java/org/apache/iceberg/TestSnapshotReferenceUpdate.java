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

import java.io.File;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestSnapshotReferenceUpdate extends TableTestBase {

  private static final int FORMAT_V2 = 2;

  public TestSnapshotReferenceUpdate() {
    super(FORMAT_V2);
  }

  @Before
  public void setupTable() throws Exception {
    this.tableDir = temp.newFolder();
    Assert.assertTrue(tableDir.delete()); // created during table creation

    this.metadataDir = new File(tableDir, "metadata");
    this.table = create(SCHEMA, PartitionSpec.unpartitioned());
    SnapshotReference newBranchRef = SnapshotReference.builderFor(
        123,
        SnapshotReferenceType.BRANCH)
        .maxSnapshotAgeMs(1L)
        .minSnapshotsToKeep(1)
        .build();
    SnapshotReference newTagRef = SnapshotReference.builderFor(
        123,
        SnapshotReferenceType.TAG)
        .maxRefAgeMs(1L)
        .build();
    TableMetadata base = table.ops().current();
    Map<String, SnapshotReference> refs = Maps.newHashMap();
    refs.put("testTag", newTagRef);
    refs.put("testBranch", newBranchRef);
    List<Snapshot> branchSnapshot =
        Lists.newArrayList(new BaseSnapshot(table.ops().io(), 123, null, "file:/tmp/manifest1" +
            ".avro"));
    TableMetadata newTableMetadata = new TableMetadata(metadataDir.getAbsolutePath(), base.formatVersion(), base.uuid(),
        base.location(),
        base.lastSequenceNumber(), base.lastUpdatedMillis(), base.lastColumnId(), base.currentSchemaId(),
        base.schemas(), base.defaultSpecId(), base.specs(), base.lastAssignedPartitionId(),
        base.defaultSortOrderId(), base.sortOrders(), base.properties(), 123, branchSnapshot,
        base.snapshotLog(), base.previousFiles(), refs);
    table.ops().commit(base, newTableMetadata);
  }

  @Test
  public void testRemoveReference() {
    SnapshotReferenceUpdate updateSnapshotReference = new SnapshotReferenceUpdate(table.ops());
    updateSnapshotReference.removeReference("testBranch");
    updateSnapshotReference.commit();
    table.refresh();
    Assert.assertNull(table.ops().current().refs().get("testBranch"));
  }

  @Test
  public void testSetBranchRetention() {
    SnapshotReferenceUpdate updateSnapshotReference = new SnapshotReferenceUpdate(table.ops());
    updateSnapshotReference.setBranchRetention("testBranch", null, 2);
    updateSnapshotReference.commit();
    table.refresh();
    Assert.assertEquals(table.ops().current().refs().get("testBranch").maxSnapshotAgeMs().longValue(), 1);
    Assert.assertEquals(table.ops().current().refs().get("testBranch").minSnapshotsToKeep().longValue(), 2);
  }

  @Test
  public void testSetMaxRefAgeMs() {
    SnapshotReferenceUpdate updateSnapshotReference = new SnapshotReferenceUpdate(table.ops());
    updateSnapshotReference.setMaxRefAgeMs("testBranch", 2L);
    updateSnapshotReference.commit();
    table.refresh();
    Assert.assertEquals(table.ops().current().refs().get("testBranch").maxRefAgeMs().longValue(), 2);
  }

  @Test
  public void testUpName() {
    SnapshotReferenceUpdate updateSnapshotReference = new SnapshotReferenceUpdate(table.ops());
    updateSnapshotReference.updateName("testBranch", "newTestBranch");
    updateSnapshotReference.commit();
    table.refresh();
    Assert.assertFalse(table.ops().current().refs().containsKey("testBranch"));
    Assert.assertTrue(table.ops().current().refs().containsKey("newTestBranch"));
  }

  @Test
  public void testUpdateReference() {
    SnapshotReference newTagRef = SnapshotReference.builderFor(
        123,
        SnapshotReferenceType.BRANCH)
        .maxSnapshotAgeMs(3L)
        .build();
    SnapshotReferenceUpdate updateSnapshotReference = new SnapshotReferenceUpdate(table.ops());
    updateSnapshotReference.updateReference("testBranch", "newTestBranch", newTagRef);
    updateSnapshotReference.commit();
    table.refresh();
    Assert.assertFalse(table.ops().current().refs().containsKey("testBranch"));
    Assert.assertTrue(table.ops().current().refs().containsKey("newTestBranch"));
  }
}
