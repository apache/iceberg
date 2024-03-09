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

import static org.apache.iceberg.TableProperties.MANIFEST_MERGE_ENABLED;
import static org.apache.iceberg.TableProperties.MANIFEST_MIN_MERGE_COUNT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ComparisonChain;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestBaseIncrementalChangelogScan
    extends ScanTestBase<
        IncrementalChangelogScan, ChangelogScanTask, ScanTaskGroup<ChangelogScanTask>> {

  @Override
  protected IncrementalChangelogScan newScan() {
    return table.newIncrementalChangelogScan();
  }

  @TestTemplate
  public void testDataFilters() {
    table.newFastAppend().appendFile(FILE_A).commit();

    Snapshot snap1 = table.currentSnapshot();
    ManifestFile snap1DataManifest = Iterables.getOnlyElement(snap1.dataManifests(table.io()));

    table.newFastAppend().appendFile(FILE_B).commit();

    Snapshot snap2 = table.currentSnapshot();

    assertThat(snap2.dataManifests(table.io())).as("Must be 2 data manifests").hasSize(2);

    withUnavailableLocations(
        ImmutableList.of(snap1DataManifest.path()),
        () -> {
          // bucket(k, 16) is 1 which is supposed to match only FILE_B
          IncrementalChangelogScan scan = newScan().filter(Expressions.equal("data", "k"));

          List<ChangelogScanTask> tasks = plan(scan);

          assertThat(tasks).as("Must have 1 task").hasSize(1);

          AddedRowsScanTask t1 = (AddedRowsScanTask) Iterables.getOnlyElement(tasks);
          assertThat(t1.changeOrdinal()).as("Ordinal must match").isEqualTo(1);
          assertThat(t1.commitSnapshotId()).as("Snapshot must match").isEqualTo(snap2.snapshotId());
          assertThat(t1.file().path()).as("Data file must match").isEqualTo(FILE_B.path());
          assertThat(t1.deletes()).as("Must be no deletes").isEmpty();
        });
  }

  @TestTemplate
  public void testOverwrites() {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Snapshot snap1 = table.currentSnapshot();

    table.newOverwrite().addFile(FILE_A2).deleteFile(FILE_A).commit();

    Snapshot snap2 = table.currentSnapshot();

    IncrementalChangelogScan scan =
        newScan().fromSnapshotExclusive(snap1.snapshotId()).toSnapshot(snap2.snapshotId());

    List<ChangelogScanTask> tasks = plan(scan);

    assertThat(tasks).as("Must have 2 tasks").hasSize(2);

    AddedRowsScanTask t1 = (AddedRowsScanTask) tasks.get(0);
    assertThat(t1.changeOrdinal()).as("Ordinal must match").isEqualTo(0);
    assertThat(t1.commitSnapshotId()).as("Snapshot must match").isEqualTo(snap2.snapshotId());
    assertThat(t1.file().path()).as("Data file must match").isEqualTo(FILE_A2.path());
    assertThat(t1.deletes()).as("Must be no deletes").isEmpty();

    DeletedDataFileScanTask t2 = (DeletedDataFileScanTask) tasks.get(1);
    assertThat(t2.changeOrdinal()).as("Ordinal must match").isEqualTo(0);
    assertThat(t2.commitSnapshotId()).as("Snapshot must match").isEqualTo(snap2.snapshotId());
    assertThat(t2.file().path()).as("Data file must match").isEqualTo(FILE_A.path());
    assertThat(t2.existingDeletes()).as("Must be no deletes").isEmpty();
  }

  @TestTemplate
  public void testFileDeletes() {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Snapshot snap1 = table.currentSnapshot();

    table.newDelete().deleteFile(FILE_A).commit();

    Snapshot snap2 = table.currentSnapshot();

    IncrementalChangelogScan scan =
        newScan().fromSnapshotExclusive(snap1.snapshotId()).toSnapshot(snap2.snapshotId());

    List<ChangelogScanTask> tasks = plan(scan);

    assertThat(tasks).as("Must have 1 task").hasSize(1);

    DeletedDataFileScanTask t1 = (DeletedDataFileScanTask) Iterables.getOnlyElement(tasks);
    assertThat(t1.changeOrdinal()).as("Ordinal must match").isEqualTo(0);
    assertThat(t1.commitSnapshotId()).as("Snapshot must match").isEqualTo(snap2.snapshotId());
    assertThat(t1.file().path()).as("Data file must match").isEqualTo(FILE_A.path());
    assertThat(t1.existingDeletes()).as("Must be no deletes").isEmpty();
  }

  @TestTemplate
  public void testExistingEntriesInNewDataManifestsAreIgnored() {
    table
        .updateProperties()
        .set(MANIFEST_MIN_MERGE_COUNT, "1")
        .set(MANIFEST_MERGE_ENABLED, "true")
        .commit();

    table.newAppend().appendFile(FILE_A).commit();

    table.newAppend().appendFile(FILE_B).commit();

    table.newAppend().appendFile(FILE_C).commit();

    Snapshot snap3 = table.currentSnapshot();

    ManifestFile manifest = Iterables.getOnlyElement(snap3.dataManifests(table.io()));
    assertThat(manifest.hasExistingFiles()).as("Manifest must have existing files").isTrue();

    IncrementalChangelogScan scan =
        newScan().fromSnapshotInclusive(snap3.snapshotId()).toSnapshot(snap3.snapshotId());

    List<ChangelogScanTask> tasks = plan(scan);

    assertThat(tasks).as("Must have 1 task").hasSize(1);

    AddedRowsScanTask t1 = (AddedRowsScanTask) Iterables.getOnlyElement(tasks);
    assertThat(t1.changeOrdinal()).as("Ordinal must match").isEqualTo(0);
    assertThat(t1.commitSnapshotId()).as("Snapshot must match").isEqualTo(snap3.snapshotId());
    assertThat(t1.file().path()).as("Data file must match").isEqualTo(FILE_C.path());
    assertThat(t1.deletes()).as("Must be no deletes").isEmpty();
  }

  @TestTemplate
  public void testManifestRewritesAreIgnored() throws IOException {
    table.newAppend().appendFile(FILE_A).commit();

    Snapshot snap1 = table.currentSnapshot();

    table.newAppend().appendFile(FILE_B).commit();

    Snapshot snap2 = table.currentSnapshot();

    ManifestFile newManifest =
        writeManifest(
            "manifest-file.avro",
            manifestEntry(ManifestEntry.Status.EXISTING, snap1.snapshotId(), FILE_A),
            manifestEntry(ManifestEntry.Status.EXISTING, snap2.snapshotId(), FILE_B));

    RewriteManifests rewriteManifests = table.rewriteManifests();

    for (ManifestFile manifest : snap2.dataManifests(table.io())) {
      rewriteManifests.deleteManifest(manifest);
    }

    rewriteManifests.addManifest(newManifest);

    rewriteManifests.commit();

    table.newAppend().appendFile(FILE_C).commit();

    Snapshot snap4 = table.currentSnapshot();

    List<ChangelogScanTask> tasks = plan(newScan());

    assertThat(tasks).as("Must have 3 tasks").hasSize(3);

    AddedRowsScanTask t1 = (AddedRowsScanTask) tasks.get(0);
    assertThat(t1.changeOrdinal()).as("Ordinal must match").isEqualTo(0);
    assertThat(t1.commitSnapshotId()).as("Snapshot must match").isEqualTo(snap1.snapshotId());
    assertThat(t1.file().path()).as("Data file must match").isEqualTo(FILE_A.path());
    assertThat(t1.deletes()).as("Must be no deletes").isEmpty();

    AddedRowsScanTask t2 = (AddedRowsScanTask) tasks.get(1);
    assertThat(t2.changeOrdinal()).as("Ordinal must match").isEqualTo(1);
    assertThat(t2.commitSnapshotId()).as("Snapshot must match").isEqualTo(snap2.snapshotId());
    assertThat(t2.file().path()).as("Data file must match").isEqualTo(FILE_B.path());
    assertThat(t2.deletes()).as("Must be no deletes").isEmpty();

    AddedRowsScanTask t3 = (AddedRowsScanTask) tasks.get(2);
    assertThat(t3.changeOrdinal()).as("Ordinal must match").isEqualTo(2);
    assertThat(t3.commitSnapshotId()).as("Snapshot must match").isEqualTo(snap4.snapshotId());
    assertThat(t3.file().path()).as("Data file must match").isEqualTo(FILE_C.path());
    assertThat(t3.deletes()).as("Must be no deletes").isEmpty();
  }

  @TestTemplate
  public void testDataFileRewrites() {
    table.newAppend().appendFile(FILE_A).commit();

    Snapshot snap1 = table.currentSnapshot();

    table.newAppend().appendFile(FILE_B).commit();

    Snapshot snap2 = table.currentSnapshot();

    table.newRewrite().rewriteFiles(ImmutableSet.of(FILE_A), ImmutableSet.of(FILE_A2)).commit();

    List<ChangelogScanTask> tasks = plan(newScan());

    assertThat(tasks).as("Must have 2 tasks").hasSize(2);

    AddedRowsScanTask t1 = (AddedRowsScanTask) tasks.get(0);
    assertThat(t1.changeOrdinal()).as("Ordinal must match").isEqualTo(0);
    assertThat(t1.commitSnapshotId()).as("Snapshot must match").isEqualTo(snap1.snapshotId());
    assertThat(t1.file().path()).as("Data file must match").isEqualTo(FILE_A.path());
    assertThat(t1.deletes()).as("Must be no deletes").isEmpty();

    AddedRowsScanTask t2 = (AddedRowsScanTask) tasks.get(1);
    assertThat(t2.changeOrdinal()).as("Ordinal must match").isEqualTo(1);
    assertThat(t2.commitSnapshotId()).as("Snapshot must match").isEqualTo(snap2.snapshotId());
    assertThat(t2.file().path()).as("Data file must match").isEqualTo(FILE_B.path());
    assertThat(t2.deletes()).as("Must be no deletes").isEmpty();
  }

  @TestTemplate
  public void testDeleteFilesAreNotSupported() {
    assumeThat(formatVersion).isEqualTo(2);

    table.newFastAppend().appendFile(FILE_A2).appendFile(FILE_B).commit();

    table.newRowDelta().addDeletes(FILE_A2_DELETES).commit();

    Assertions.assertThatThrownBy(() -> plan(newScan()))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Delete files are currently not supported in changelog scans");
  }

  // plans tasks and reorders them to have deterministic order
  private List<ChangelogScanTask> plan(IncrementalChangelogScan scan) {
    try (CloseableIterable<ChangelogScanTask> tasks = scan.planFiles()) {
      List<ChangelogScanTask> tasksAsList = Lists.newArrayList(tasks);
      tasksAsList.sort(taskComparator());
      return tasksAsList;

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Comparator<? super ChangelogScanTask> taskComparator() {
    return (t1, t2) ->
        ComparisonChain.start()
            .compare(t1.changeOrdinal(), t2.changeOrdinal())
            .compare(t1.getClass().getName(), t2.getClass().getName())
            .compare(path(t1), path(t2))
            .result();
  }

  private String path(ChangelogScanTask task) {
    return ((ContentScanTask<?>) task).file().path().toString();
  }
}
