/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg;

import java.io.File;
import java.util.List;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.iceberg.Files.localInput;

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
    SnapshotReference newBranchRef = SnapshotReference.builderFor(123, "testBranch",
        SnapshotReferenceType.BRANCH)
        .withMaxSnapshotAgeMs(1)
        .withMinSnapshotsToKeep(1)
        .build();
    SnapshotReference newTagRef = SnapshotReference.builderFor(123, "testTag",
        SnapshotReferenceType.TAG)
        .withMaxSnapshotAgeMs(1)
        .build();
    TableMetadata base = table.ops().current();
    List<SnapshotReference> refs = Lists.newArrayList();
    refs.add(newTagRef);
    refs.add(newBranchRef);
    List<Snapshot> branchSnapshot =
        Lists.newArrayList(new BaseSnapshot(table.ops().io(), 123, null, "file:/tmp/manifest1" +
            ".avro"));
    TableMetadata newTableMetadata = new TableMetadata(localInput(metadataDir), base.formatVersion(), base.uuid(),
        base.location(),
        base.lastSequenceNumber(), base.lastUpdatedMillis(), base.lastColumnId(), base.currentSchemaId(),
        base.schemas(), base.defaultSpecId(), base.specs(), base.lastAssignedPartitionId(),
        base.defaultSortOrderId(), base.sortOrders(), base.properties(), 123, branchSnapshot,
        base.snapshotLog(), base.previousFiles(), refs, base.currentBranch());
    table.ops().commit(base, newTableMetadata);
  }

  @Test
  public void testRemoveSnapshotReference() {
    SnapshotReferenceUpdate updateSnapshotReference = new SnapshotReferenceUpdate(table.ops());
    updateSnapshotReference.removeSnapshotReference("testBranch");
    updateSnapshotReference.commit();
    table.refresh();
    Assert.assertNull(table.ops().current().ref("testBranch"));
  }

  @Test
  public void testUpdateMaxSnapshotAgeMs() {
    SnapshotReferenceUpdate updateSnapshotReference = new SnapshotReferenceUpdate(table.ops());
    updateSnapshotReference.updateMaxSnapshotAgeMs(2, "testBranch");
    updateSnapshotReference.commit();
    table.refresh();
    Assert.assertEquals(table.ops().current().ref("testBranch").maxSnapshotAgeMs(), 2);
  }

  @Test
  public void testUpdateMinSnapshotsToKeep() {
    SnapshotReferenceUpdate updateSnapshotReference = new SnapshotReferenceUpdate(table.ops());
    updateSnapshotReference.updateMinSnapshotsToKeep(2, "testBranch");
    updateSnapshotReference.commit();
    table.refresh();
    Assert.assertTrue(table.ops().current().ref("testBranch").minSnapshotsToKeep() == 2);

    AssertHelpers.assertThrows(
        "Check updateMinSnapshotsToKeep",
        ValidationException.class, "TAG type snapshot reference does not support setting minSnapshotsToKeep",
        () -> updateSnapshotReference.updateMinSnapshotsToKeep(2, "testTag"));
  }

  @Test
  public void testUpName() {
    SnapshotReferenceUpdate updateSnapshotReference = new SnapshotReferenceUpdate(table.ops());
    updateSnapshotReference.updateName("testBranch", "newTestBranch");
    updateSnapshotReference.commit();
    table.refresh();
    Assert.assertNull(table.ops().current().ref("testBranch"));
    Assert.assertNotNull(table.ops().current().ref("newTestBranch"));
  }

  @Test
  public void testUpdateReference() {
    SnapshotReference newTagRef = SnapshotReference.builderFor(123, "newTestTag",
        SnapshotReferenceType.TAG)
        .withMaxSnapshotAgeMs(1)
        .build();
    SnapshotReference oldTagRef = table.ops().current().ref("testTag");
    SnapshotReferenceUpdate updateSnapshotReference = new SnapshotReferenceUpdate(table.ops());
    updateSnapshotReference.updateReference(oldTagRef, newTagRef);
    updateSnapshotReference.commit();
    table.refresh();
    Assert.assertNull(table.ops().current().ref("testTag"));
    Assert.assertNotNull(table.ops().current().ref("newTestTag"));
  }
}
