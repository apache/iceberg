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
import static org.apache.iceberg.TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestRewriteManifests extends TestBase {
  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(1, 2);
  }

  @TestTemplate
  public void testRewriteManifestsAppendedDirectly() throws IOException {
    Table table = load();

    table.updateProperties().set(SNAPSHOT_ID_INHERITANCE_ENABLED, "true").commit();

    ManifestFile newManifest =
        writeManifest(
            "manifest-file-1.avro", manifestEntry(ManifestEntry.Status.ADDED, null, FILE_A));

    table.newFastAppend().appendManifest(newManifest).commit();
    long appendId = table.currentSnapshot().snapshotId();

    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(1);

    table.rewriteManifests().clusterBy(file -> "").commit();

    List<ManifestFile> manifests = table.currentSnapshot().allManifests(table.io());
    assertThat(manifests).hasSize(1);

    validateManifestEntries(
        manifests.get(0), ids(appendId), files(FILE_A), statuses(ManifestEntry.Status.EXISTING));
  }

  @TestTemplate
  public void testRewriteManifestsWithScanExecutor() throws IOException {
    Table table = load();

    table.updateProperties().set(SNAPSHOT_ID_INHERITANCE_ENABLED, "true").commit();

    ManifestFile newManifest =
        writeManifest(
            "manifest-file-1.avro", manifestEntry(ManifestEntry.Status.ADDED, null, FILE_A));

    table.newFastAppend().appendManifest(newManifest).commit();

    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(1);
    AtomicInteger scanThreadsIndex = new AtomicInteger(0);
    table
        .rewriteManifests()
        .clusterBy(file -> "")
        .scanManifestsWith(
            Executors.newFixedThreadPool(
                1,
                runnable -> {
                  Thread thread = new Thread(runnable);
                  thread.setName("scan-" + scanThreadsIndex.getAndIncrement());
                  thread.setDaemon(
                      true); // daemon threads will be terminated abruptly when the JVM exits
                  return thread;
                }))
        .commit();

    List<ManifestFile> manifests = table.currentSnapshot().allManifests(table.io());
    assertThat(manifests).hasSize(1);
    assertThat(scanThreadsIndex.get())
        .as("Thread should be created in provided pool")
        .isGreaterThan(0);
  }

  @TestTemplate
  public void testRewriteManifestsGeneratedAndAppendedDirectly() throws IOException {
    Table table = load();

    table.updateProperties().set(SNAPSHOT_ID_INHERITANCE_ENABLED, "true").commit();

    ManifestFile newManifest =
        writeManifest(
            "manifest-file-1.avro", manifestEntry(ManifestEntry.Status.ADDED, null, FILE_A));

    table.newFastAppend().appendManifest(newManifest).commit();
    long manifestAppendId = table.currentSnapshot().snapshotId();

    table.newFastAppend().appendFile(FILE_B).commit();
    long fileAppendId = table.currentSnapshot().snapshotId();

    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(2);

    table.rewriteManifests().clusterBy(file -> "").commit();

    List<ManifestFile> manifests = table.currentSnapshot().allManifests(table.io());
    assertThat(manifests).hasSize(1);

    // get the correct file order
    List<DataFile> files;
    List<Long> ids;
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifests.get(0), table.io())) {
      if (reader.iterator().next().path().equals(FILE_A.path())) {
        files = Arrays.asList(FILE_A, FILE_B);
        ids = Arrays.asList(manifestAppendId, fileAppendId);
      } else {
        files = Arrays.asList(FILE_B, FILE_A);
        ids = Arrays.asList(fileAppendId, manifestAppendId);
      }
    }

    validateManifestEntries(
        manifests.get(0),
        ids.iterator(),
        files.iterator(),
        statuses(ManifestEntry.Status.EXISTING, ManifestEntry.Status.EXISTING));
  }

  @TestTemplate
  public void testReplaceManifestsSeparate() {
    Table table = load();
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    long appendId = table.currentSnapshot().snapshotId();

    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(1);

    // cluster by path will split the manifest into two

    table.rewriteManifests().clusterBy(file -> file.path()).commit();

    List<ManifestFile> manifests = table.currentSnapshot().allManifests(table.io());
    assertThat(manifests).hasSize(2);
    manifests.sort(Comparator.comparing(ManifestFile::path));

    validateManifestEntries(
        manifests.get(0), ids(appendId), files(FILE_A), statuses(ManifestEntry.Status.EXISTING));
    validateManifestEntries(
        manifests.get(1), ids(appendId), files(FILE_B), statuses(ManifestEntry.Status.EXISTING));
  }

  @TestTemplate
  public void testReplaceManifestsConsolidate() throws IOException {
    Table table = load();

    table.newFastAppend().appendFile(FILE_A).commit();
    long appendIdA = table.currentSnapshot().snapshotId();
    table.newFastAppend().appendFile(FILE_B).commit();
    long appendIdB = table.currentSnapshot().snapshotId();

    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(2);

    // cluster by constant will combine manifests into one

    table.rewriteManifests().clusterBy(file -> "file").commit();

    List<ManifestFile> manifests = table.currentSnapshot().allManifests(table.io());
    assertThat(manifests).hasSize(1);

    // get the file order correct
    List<DataFile> files;
    List<Long> ids;
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifests.get(0), table.io())) {
      if (reader.iterator().next().path().equals(FILE_A.path())) {
        files = Arrays.asList(FILE_A, FILE_B);
        ids = Arrays.asList(appendIdA, appendIdB);
      } else {
        files = Arrays.asList(FILE_B, FILE_A);
        ids = Arrays.asList(appendIdB, appendIdA);
      }
    }

    validateManifestEntries(
        manifests.get(0),
        ids.iterator(),
        files.iterator(),
        statuses(ManifestEntry.Status.EXISTING, ManifestEntry.Status.EXISTING));
  }

  @TestTemplate
  public void testReplaceManifestsWithFilter() throws IOException {
    Table table = load();

    table.newFastAppend().appendFile(FILE_A).commit();
    long appendIdA = table.currentSnapshot().snapshotId();

    table.newFastAppend().appendFile(FILE_B).commit();
    long appendIdB = table.currentSnapshot().snapshotId();

    table.newFastAppend().appendFile(FILE_C).commit();
    long appendIdC = table.currentSnapshot().snapshotId();

    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(3);

    // keep the file A manifest, combine the other two

    table
        .rewriteManifests()
        .clusterBy(file -> "file")
        .rewriteIf(
            manifest -> {
              try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, table.io())) {
                return !reader.iterator().next().path().equals(FILE_A.path());
              } catch (IOException x) {
                throw new RuntimeIOException(x);
              }
            })
        .commit();

    List<ManifestFile> manifests = table.currentSnapshot().allManifests(table.io());
    assertThat(manifests).hasSize(2);

    // get the file order correct
    List<DataFile> files;
    List<Long> ids;
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifests.get(0), table.io())) {
      if (reader.iterator().next().path().equals(FILE_B.path())) {
        files = Arrays.asList(FILE_B, FILE_C);
        ids = Arrays.asList(appendIdB, appendIdC);
      } else {
        files = Arrays.asList(FILE_C, FILE_B);
        ids = Arrays.asList(appendIdC, appendIdB);
      }
    }

    validateManifestEntries(
        manifests.get(0),
        ids.iterator(),
        files.iterator(),
        statuses(ManifestEntry.Status.EXISTING, ManifestEntry.Status.EXISTING));
    validateManifestEntries(
        manifests.get(1), ids(appendIdA), files(FILE_A), statuses(ManifestEntry.Status.ADDED));
  }

  @TestTemplate
  public void testReplaceManifestsMaxSize() {
    Table table = load();
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    long appendId = table.currentSnapshot().snapshotId();

    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(1);

    // cluster by constant will combine manifests into one but small target size will create one per
    // entry
    BaseRewriteManifests rewriteManifests = spy((BaseRewriteManifests) table.rewriteManifests());
    when(rewriteManifests.getManifestTargetSizeBytes()).thenReturn(1L);
    rewriteManifests.clusterBy(file -> "file").commit();

    List<ManifestFile> manifests = table.currentSnapshot().allManifests(table.io());
    assertThat(manifests).hasSize(2);
    manifests.sort(Comparator.comparing(ManifestFile::path));

    validateManifestEntries(
        manifests.get(0), ids(appendId), files(FILE_A), statuses(ManifestEntry.Status.EXISTING));
    validateManifestEntries(
        manifests.get(1), ids(appendId), files(FILE_B), statuses(ManifestEntry.Status.EXISTING));
  }

  @TestTemplate
  public void testConcurrentRewriteManifest() throws IOException {
    Table table = load();
    table.newFastAppend().appendFile(FILE_A).commit();
    long appendIdA = table.currentSnapshot().snapshotId();
    table.newFastAppend().appendFile(FILE_B).commit();
    long appendIdB = table.currentSnapshot().snapshotId();

    // start a rewrite manifests that involves both manifests
    RewriteManifests rewrite = table.rewriteManifests();
    rewrite.clusterBy(file -> "file").apply();

    // commit a rewrite manifests that only involves one manifest
    table
        .rewriteManifests()
        .clusterBy(file -> "file")
        .rewriteIf(
            manifest -> {
              try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, table.io())) {
                return !reader.iterator().next().path().equals(FILE_A.path());
              } catch (IOException x) {
                throw new RuntimeIOException(x);
              }
            })
        .commit();

    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(2);

    // commit the rewrite manifests in progress - this should perform a full rewrite as the manifest
    // with file B is no longer part of the snapshot
    rewrite.commit();

    List<ManifestFile> manifests = table.currentSnapshot().allManifests(table.io());
    assertThat(manifests).hasSize(1);

    // get the file order correct
    List<DataFile> files;
    List<Long> ids;
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifests.get(0), table.io())) {
      if (reader.iterator().next().path().equals(FILE_A.path())) {
        files = Arrays.asList(FILE_A, FILE_B);
        ids = Arrays.asList(appendIdA, appendIdB);
      } else {
        files = Arrays.asList(FILE_B, FILE_A);
        ids = Arrays.asList(appendIdB, appendIdA);
      }
    }

    validateManifestEntries(
        manifests.get(0),
        ids.iterator(),
        files.iterator(),
        statuses(ManifestEntry.Status.EXISTING, ManifestEntry.Status.EXISTING));
  }

  @TestTemplate
  public void testAppendDuringRewriteManifest() {
    Table table = load();
    table.newFastAppend().appendFile(FILE_A).commit();
    long appendIdA = table.currentSnapshot().snapshotId();

    // start the rewrite manifests
    RewriteManifests rewrite = table.rewriteManifests();
    rewrite.clusterBy(file -> "file").apply();

    // append a file
    table.newFastAppend().appendFile(FILE_B).commit();
    long appendIdB = table.currentSnapshot().snapshotId();

    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(2);

    // commit the rewrite manifests in progress
    rewrite.commit();

    // the rewrite should only affect the first manifest, so we will end up with 2 manifests even
    // though we
    // have a single cluster key, rewritten one should be the first in the list

    List<ManifestFile> manifests = table.currentSnapshot().allManifests(table.io());
    assertThat(manifests).hasSize(2);

    validateManifestEntries(
        manifests.get(0), ids(appendIdA), files(FILE_A), statuses(ManifestEntry.Status.EXISTING));
    validateManifestEntries(
        manifests.get(1), ids(appendIdB), files(FILE_B), statuses(ManifestEntry.Status.ADDED));
  }

  @TestTemplate
  public void testRewriteManifestDuringAppend() {
    Table table = load();
    table.newFastAppend().appendFile(FILE_A).commit();
    long appendIdA = table.currentSnapshot().snapshotId();

    // start an append
    AppendFiles append = table.newFastAppend();
    append.appendFile(FILE_B).apply();

    // rewrite the manifests - only affects the first
    table.rewriteManifests().clusterBy(file -> "file").commit();

    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(1);

    // commit the append in progress
    append.commit();
    long appendIdB = table.currentSnapshot().snapshotId();

    List<ManifestFile> manifests = table.currentSnapshot().allManifests(table.io());
    assertThat(manifests).hasSize(2);

    // last append should be the first in the list

    validateManifestEntries(
        manifests.get(0), ids(appendIdB), files(FILE_B), statuses(ManifestEntry.Status.ADDED));
    validateManifestEntries(
        manifests.get(1), ids(appendIdA), files(FILE_A), statuses(ManifestEntry.Status.EXISTING));
  }

  @TestTemplate
  public void testBasicManifestReplacement() throws IOException {
    assertThat(table.currentSnapshot()).isNull();

    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Snapshot firstSnapshot = table.currentSnapshot();
    List<ManifestFile> firstSnapshotManifests = firstSnapshot.allManifests(table.io());
    assertThat(firstSnapshotManifests).hasSize(1);
    ManifestFile firstSnapshotManifest = firstSnapshotManifests.get(0);

    table.newFastAppend().appendFile(FILE_C).appendFile(FILE_D).commit();
    Snapshot secondSnapshot = table.currentSnapshot();

    ManifestFile firstNewManifest =
        writeManifest(
            "manifest-file-1.avro",
            manifestEntry(ManifestEntry.Status.EXISTING, firstSnapshot.snapshotId(), FILE_A));
    ManifestFile secondNewManifest =
        writeManifest(
            "manifest-file-2.avro",
            manifestEntry(ManifestEntry.Status.EXISTING, firstSnapshot.snapshotId(), FILE_B));

    RewriteManifests rewriteManifests = table.rewriteManifests();
    rewriteManifests.deleteManifest(firstSnapshotManifest);
    rewriteManifests.addManifest(firstNewManifest);
    rewriteManifests.addManifest(secondNewManifest);
    rewriteManifests.commit();

    Snapshot snapshot = table.currentSnapshot();
    List<ManifestFile> manifests = snapshot.allManifests(table.io());
    assertThat(manifests).hasSize(3);

    if (formatVersion == 1) {
      assertThat(manifests.get(0).path()).isNotEqualTo(firstNewManifest.path());
      assertThat(manifests.get(1).path()).isNotEqualTo(secondNewManifest.path());
    } else {
      assertThat(manifests.get(0).path()).isEqualTo(firstNewManifest.path());
      assertThat(manifests.get(1).path()).isEqualTo(secondNewManifest.path());
    }

    validateSummary(snapshot, 1, 1, 2, 0);

    validateManifestEntries(
        manifests.get(0),
        ids(firstSnapshot.snapshotId()),
        files(FILE_A),
        statuses(ManifestEntry.Status.EXISTING));

    validateManifestEntries(
        manifests.get(1),
        ids(firstSnapshot.snapshotId()),
        files(FILE_B),
        statuses(ManifestEntry.Status.EXISTING));

    validateManifestEntries(
        manifests.get(2),
        ids(secondSnapshot.snapshotId(), secondSnapshot.snapshotId()),
        files(FILE_C, FILE_D),
        statuses(ManifestEntry.Status.ADDED, ManifestEntry.Status.ADDED));
  }

  @TestTemplate
  public void testBasicManifestReplacementWithSnapshotIdInheritance() throws IOException {
    assertThat(table.currentSnapshot()).isNull();

    table.updateProperties().set(SNAPSHOT_ID_INHERITANCE_ENABLED, "true").commit();

    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Snapshot firstSnapshot = table.currentSnapshot();
    List<ManifestFile> firstSnapshotManifests = firstSnapshot.allManifests(table.io());
    assertThat(firstSnapshotManifests).hasSize(1);
    ManifestFile firstSnapshotManifest = firstSnapshotManifests.get(0);

    table.newFastAppend().appendFile(FILE_C).appendFile(FILE_D).commit();
    Snapshot secondSnapshot = table.currentSnapshot();

    ManifestFile firstNewManifest =
        writeManifest(
            "manifest-file-1.avro",
            manifestEntry(ManifestEntry.Status.EXISTING, firstSnapshot.snapshotId(), FILE_A));
    ManifestFile secondNewManifest =
        writeManifest(
            "manifest-file-2.avro",
            manifestEntry(ManifestEntry.Status.EXISTING, firstSnapshot.snapshotId(), FILE_B));

    RewriteManifests rewriteManifests = table.rewriteManifests();
    rewriteManifests.deleteManifest(firstSnapshotManifest);
    rewriteManifests.addManifest(firstNewManifest);
    rewriteManifests.addManifest(secondNewManifest);
    rewriteManifests.commit();

    Snapshot snapshot = table.currentSnapshot();
    List<ManifestFile> manifests = snapshot.allManifests(table.io());
    assertThat(manifests).hasSize(3);

    assertThat(manifests.get(0).path()).isEqualTo(firstNewManifest.path());
    assertThat(manifests.get(1).path()).isEqualTo(secondNewManifest.path());

    validateSummary(snapshot, 1, 1, 2, 0);

    validateManifestEntries(
        manifests.get(0),
        ids(firstSnapshot.snapshotId()),
        files(FILE_A),
        statuses(ManifestEntry.Status.EXISTING));

    validateManifestEntries(
        manifests.get(1),
        ids(firstSnapshot.snapshotId()),
        files(FILE_B),
        statuses(ManifestEntry.Status.EXISTING));

    validateManifestEntries(
        manifests.get(2),
        ids(secondSnapshot.snapshotId(), secondSnapshot.snapshotId()),
        files(FILE_C, FILE_D),
        statuses(ManifestEntry.Status.ADDED, ManifestEntry.Status.ADDED));

    // validate that any subsequent operation does not fail
    table.newDelete().deleteFromRowFilter(Expressions.alwaysTrue()).commit();
  }

  @TestTemplate
  public void testWithMultiplePartitionSpec() throws IOException {
    assertThat(table.currentSnapshot()).isNull();

    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    TableMetadata base = readMetadata();
    assertThat(base.currentSnapshot().allManifests(table.io())).hasSize(1);
    ManifestFile initialManifest = base.currentSnapshot().allManifests(table.io()).get(0);

    int initialPartitionSpecId = initialManifest.partitionSpecId();

    // build the new spec using the table's schema, which uses fresh IDs
    PartitionSpec newSpec =
        PartitionSpec.builderFor(base.schema()).bucket("data", 16).bucket("id", 4).build();

    // commit the new partition spec to the table manually
    table.ops().commit(base, base.updatePartitionSpec(newSpec));

    DataFile newFileY =
        DataFiles.builder(table.spec())
            .withPath("/path/to/data-y.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=2/id_bucket=3")
            .withRecordCount(1)
            .build();

    table.newAppend().appendFile(newFileY).commit();

    DataFile newFileZ =
        DataFiles.builder(table.spec())
            .withPath("/path/to/data-z.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=2/id_bucket=4")
            .withRecordCount(1)
            .build();

    table.newAppend().appendFile(newFileZ).commit();

    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(3);

    RewriteManifests rewriteManifests = table.rewriteManifests();
    // try to cluster in 1 manifest file, but because of 2 partition specs
    // we should still have 2 manifest files.
    rewriteManifests.clusterBy(dataFile -> "file").commit();
    List<ManifestFile> manifestFiles = table.currentSnapshot().allManifests(table.io());

    assertThat(manifestFiles).as("Rewrite manifest should produce 2 manifest files").hasSize(2);

    assertThat(manifestFiles.get(1).partitionSpecId())
        .as("2 manifest files should have different partitionSpecId")
        .isNotEqualTo(manifestFiles.get(0).partitionSpecId());

    matchNumberOfManifestFileWithSpecId(manifestFiles, initialPartitionSpecId, 1);

    matchNumberOfManifestFileWithSpecId(manifestFiles, table.ops().current().spec().specId(), 1);

    assertThat(manifestFiles.get(0).existingFilesCount())
        .as("first manifest file should have 2 data files")
        .isEqualTo(2);

    assertThat(manifestFiles.get(1).existingFilesCount())
        .as("second manifest file should have 2 data files")
        .isEqualTo(2);
  }

  @TestTemplate
  public void testManifestSizeWithMultiplePartitionSpec() throws IOException {
    assertThat(table.currentSnapshot()).isNull();

    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    TableMetadata base = readMetadata();
    assertThat(base.currentSnapshot().allManifests(table.io())).hasSize(1);
    ManifestFile initialManifest = base.currentSnapshot().allManifests(table.io()).get(0);
    int initialPartitionSpecId = initialManifest.partitionSpecId();

    // build the new spec using the table's schema, which uses fresh IDs
    PartitionSpec newSpec =
        PartitionSpec.builderFor(base.schema()).bucket("data", 16).bucket("id", 4).build();

    // commit the new partition spec to the table manually
    table.ops().commit(base, base.updatePartitionSpec(newSpec));

    DataFile newFileY =
        DataFiles.builder(table.spec())
            .withPath("/path/to/data-y.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=2/id_bucket=3")
            .withRecordCount(1)
            .build();

    table.newAppend().appendFile(newFileY).commit();

    DataFile newFileZ =
        DataFiles.builder(table.spec())
            .withPath("/path/to/data-z.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=2/id_bucket=4")
            .withRecordCount(1)
            .build();

    table.newAppend().appendFile(newFileZ).commit();

    assertThat(table.currentSnapshot().allManifests(table.io()))
        .as("Rewrite manifests should produce 3 manifest files")
        .hasSize(3);

    // cluster by constant will combine manifests into one but small target size will create one per
    // entry
    BaseRewriteManifests rewriteManifests = spy((BaseRewriteManifests) table.rewriteManifests());
    when(rewriteManifests.getManifestTargetSizeBytes()).thenReturn(1L);

    // rewriteManifests should produce 4 manifestFiles, because of targetByteSize=1
    rewriteManifests.clusterBy(dataFile -> "file").commit();
    List<ManifestFile> manifestFiles = table.currentSnapshot().allManifests(table.io());

    assertThat(manifestFiles).hasSize(4);

    matchNumberOfManifestFileWithSpecId(manifestFiles, initialPartitionSpecId, 2);

    matchNumberOfManifestFileWithSpecId(manifestFiles, table.ops().current().spec().specId(), 2);

    assertThat(manifestFiles.get(0).existingFilesCount()).isEqualTo(1);

    assertThat(manifestFiles.get(1).existingFilesCount()).isEqualTo(1);
    assertThat(manifestFiles.get(2).existingFilesCount()).isEqualTo(1);
    assertThat(manifestFiles.get(3).existingFilesCount()).isEqualTo(1);
  }

  @TestTemplate
  public void testManifestReplacementConcurrentAppend() throws IOException {
    assertThat(table.currentSnapshot()).isNull();

    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Snapshot firstSnapshot = table.currentSnapshot();
    List<ManifestFile> firstSnapshotManifests = firstSnapshot.allManifests(table.io());
    assertThat(firstSnapshotManifests).hasSize(1);
    ManifestFile firstSnapshotManifest = firstSnapshotManifests.get(0);

    ManifestFile firstNewManifest =
        writeManifest(
            "manifest-file-1.avro",
            manifestEntry(ManifestEntry.Status.EXISTING, firstSnapshot.snapshotId(), FILE_A));
    ManifestFile secondNewManifest =
        writeManifest(
            "manifest-file-2.avro",
            manifestEntry(ManifestEntry.Status.EXISTING, firstSnapshot.snapshotId(), FILE_B));

    RewriteManifests rewriteManifests = table.rewriteManifests();
    rewriteManifests.deleteManifest(firstSnapshotManifest);
    rewriteManifests.addManifest(firstNewManifest);
    rewriteManifests.addManifest(secondNewManifest);

    table.newFastAppend().appendFile(FILE_C).appendFile(FILE_D).commit();
    Snapshot secondSnapshot = table.currentSnapshot();

    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(2);

    rewriteManifests.commit();

    Snapshot snapshot = table.currentSnapshot();
    List<ManifestFile> manifests = snapshot.allManifests(table.io());
    assertThat(manifests).hasSize(3);

    validateSummary(snapshot, 1, 1, 2, 0);

    validateManifestEntries(
        manifests.get(0),
        ids(firstSnapshot.snapshotId()),
        files(FILE_A),
        statuses(ManifestEntry.Status.EXISTING));

    validateManifestEntries(
        manifests.get(1),
        ids(firstSnapshot.snapshotId()),
        files(FILE_B),
        statuses(ManifestEntry.Status.EXISTING));

    validateManifestEntries(
        manifests.get(2),
        ids(secondSnapshot.snapshotId(), secondSnapshot.snapshotId()),
        files(FILE_C, FILE_D),
        statuses(ManifestEntry.Status.ADDED, ManifestEntry.Status.ADDED));
  }

  @TestTemplate
  public void testManifestReplacementConcurrentDelete() throws IOException {
    assertThat(table.currentSnapshot()).isNull();

    table.updateProperties().set(MANIFEST_MERGE_ENABLED, "false").commit();

    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Snapshot firstSnapshot = table.currentSnapshot();
    List<ManifestFile> firstSnapshotManifests = firstSnapshot.allManifests(table.io());
    assertThat(firstSnapshotManifests).hasSize(1);
    ManifestFile firstSnapshotManifest = firstSnapshotManifests.get(0);

    table.newFastAppend().appendFile(FILE_C).appendFile(FILE_D).commit();
    long secondSnapshotId = table.currentSnapshot().snapshotId();

    ManifestFile firstNewManifest =
        writeManifest(
            "manifest-file-1.avro",
            manifestEntry(ManifestEntry.Status.EXISTING, firstSnapshot.snapshotId(), FILE_A));
    ManifestFile secondNewManifest =
        writeManifest(
            "manifest-file-2.avro",
            manifestEntry(ManifestEntry.Status.EXISTING, firstSnapshot.snapshotId(), FILE_B));

    RewriteManifests rewriteManifests = table.rewriteManifests();
    rewriteManifests.deleteManifest(firstSnapshotManifest);
    rewriteManifests.addManifest(firstNewManifest);
    rewriteManifests.addManifest(secondNewManifest);

    table.newDelete().deleteFile(FILE_C).commit();
    long thirdSnapshotId = table.currentSnapshot().snapshotId();

    rewriteManifests.commit();

    Snapshot snapshot = table.currentSnapshot();
    List<ManifestFile> manifests = snapshot.allManifests(table.io());
    assertThat(manifests).hasSize(3);

    validateSummary(snapshot, 1, 1, 2, 0);

    validateManifestEntries(
        manifests.get(0),
        ids(firstSnapshot.snapshotId()),
        files(FILE_A),
        statuses(ManifestEntry.Status.EXISTING));

    validateManifestEntries(
        manifests.get(1),
        ids(firstSnapshot.snapshotId()),
        files(FILE_B),
        statuses(ManifestEntry.Status.EXISTING));

    validateManifestEntries(
        manifests.get(2),
        ids(thirdSnapshotId, secondSnapshotId),
        files(FILE_C, FILE_D),
        statuses(ManifestEntry.Status.DELETED, ManifestEntry.Status.EXISTING));
  }

  @TestTemplate
  public void testManifestReplacementConcurrentConflictingDelete() throws IOException {
    assertThat(table.currentSnapshot()).isNull();

    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Snapshot firstSnapshot = table.currentSnapshot();
    List<ManifestFile> firstSnapshotManifests = firstSnapshot.allManifests(table.io());
    assertThat(firstSnapshotManifests).hasSize(1);
    ManifestFile firstSnapshotManifest = firstSnapshotManifests.get(0);

    ManifestFile firstNewManifest =
        writeManifest(
            "manifest-file-1.avro",
            manifestEntry(ManifestEntry.Status.EXISTING, firstSnapshot.snapshotId(), FILE_A));
    ManifestFile secondNewManifest =
        writeManifest(
            "manifest-file-2.avro",
            manifestEntry(ManifestEntry.Status.EXISTING, firstSnapshot.snapshotId(), FILE_B));

    RewriteManifests rewriteManifests = table.rewriteManifests();
    rewriteManifests.deleteManifest(firstSnapshotManifest);
    rewriteManifests.addManifest(firstNewManifest);
    rewriteManifests.addManifest(secondNewManifest);

    table.newDelete().deleteFile(FILE_A).commit();

    assertThatThrownBy(rewriteManifests::commit)
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Manifest is missing");
  }

  @TestTemplate
  public void testManifestReplacementCombinedWithRewrite() throws IOException {
    assertThat(table.currentSnapshot()).isNull();

    table.newFastAppend().appendFile(FILE_A).commit();

    Snapshot firstSnapshot = table.currentSnapshot();
    List<ManifestFile> firstSnapshotManifests = firstSnapshot.allManifests(table.io());
    assertThat(firstSnapshotManifests).hasSize(1);
    ManifestFile firstSnapshotManifest = firstSnapshotManifests.get(0);

    table.newFastAppend().appendFile(FILE_B).commit();

    Snapshot secondSnapshot = table.currentSnapshot();

    table.newFastAppend().appendFile(FILE_C).commit();

    table.newFastAppend().appendFile(FILE_D).commit();

    assertThat(table.snapshots()).hasSize(4);

    ManifestFile newManifest =
        writeManifest(
            "manifest-file-1.avro",
            manifestEntry(ManifestEntry.Status.EXISTING, firstSnapshot.snapshotId(), FILE_A));

    table
        .rewriteManifests()
        .deleteManifest(firstSnapshotManifest)
        .addManifest(newManifest)
        .clusterBy(dataFile -> "const-value")
        .rewriteIf(
            manifest -> {
              try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, table.io())) {
                return !reader.iterator().next().path().equals(FILE_B.path());
              } catch (IOException x) {
                throw new RuntimeIOException(x);
              }
            })
        .commit();

    Snapshot snapshot = table.currentSnapshot();
    List<ManifestFile> manifests = snapshot.allManifests(table.io());
    assertThat(manifests).hasSize(3);

    validateSummary(snapshot, 3, 1, 2, 2);

    validateManifestEntries(
        manifests.get(1),
        ids(firstSnapshot.snapshotId()),
        files(FILE_A),
        statuses(ManifestEntry.Status.EXISTING));

    validateManifestEntries(
        manifests.get(2),
        ids(secondSnapshot.snapshotId()),
        files(FILE_B),
        statuses(ManifestEntry.Status.ADDED));
  }

  @TestTemplate
  public void testManifestReplacementCombinedWithRewriteConcurrentDelete() throws IOException {
    assertThat(table.currentSnapshot()).isNull();

    table.updateProperties().set(MANIFEST_MERGE_ENABLED, "false").commit();

    table.newFastAppend().appendFile(FILE_A).commit();

    Snapshot firstSnapshot = table.currentSnapshot();
    List<ManifestFile> firstSnapshotManifests = firstSnapshot.allManifests(table.io());
    assertThat(firstSnapshotManifests).hasSize(1);
    ManifestFile firstSnapshotManifest = firstSnapshotManifests.get(0);

    table.newFastAppend().appendFile(FILE_B).commit();

    Snapshot secondSnapshot = table.currentSnapshot();

    table.newFastAppend().appendFile(FILE_C).commit();

    assertThat(table.snapshots()).hasSize(3);

    ManifestEntry<DataFile> entry =
        manifestEntry(ManifestEntry.Status.EXISTING, firstSnapshot.snapshotId(), FILE_A);
    // update the entry's sequence number or else it will be rejected by the writer
    entry.setDataSequenceNumber(firstSnapshot.sequenceNumber());
    ManifestFile newManifest = writeManifest("manifest-file-1.avro", entry);

    RewriteManifests rewriteManifests =
        table
            .rewriteManifests()
            .deleteManifest(firstSnapshotManifest)
            .addManifest(newManifest)
            .clusterBy(dataFile -> "const-value");

    rewriteManifests.apply();

    table.newDelete().deleteFile(FILE_C).commit();

    rewriteManifests.commit();

    Snapshot snapshot = table.currentSnapshot();
    List<ManifestFile> manifests = snapshot.allManifests(table.io());
    assertThat(manifests).hasSize(2);

    validateSummary(snapshot, 3, 0, 2, 1);

    validateManifestEntries(
        manifests.get(0),
        ids(secondSnapshot.snapshotId()),
        files(FILE_B),
        statuses(ManifestEntry.Status.EXISTING));

    validateManifestEntries(
        manifests.get(1),
        ids(firstSnapshot.snapshotId()),
        files(FILE_A),
        statuses(ManifestEntry.Status.EXISTING));
  }

  @TestTemplate
  public void testInvalidUsage() throws IOException {
    assertThat(table.currentSnapshot()).isNull();

    table.newFastAppend().appendFile(FILE_A).commit();

    Snapshot snapshot = table.currentSnapshot();
    List<ManifestFile> manifests = snapshot.allManifests(table.io());
    assertThat(manifests).hasSize(1);
    ManifestFile manifest = manifests.get(0);

    ManifestEntry<DataFile> appendEntry =
        manifestEntry(ManifestEntry.Status.ADDED, snapshot.snapshotId(), FILE_A);
    // update the entry's sequence number or else it will be rejected by the writer
    appendEntry.setDataSequenceNumber(snapshot.sequenceNumber());

    ManifestFile invalidAddedFileManifest = writeManifest("manifest-file-2.avro", appendEntry);

    assertThatThrownBy(
            () ->
                table
                    .rewriteManifests()
                    .deleteManifest(manifest)
                    .addManifest(invalidAddedFileManifest)
                    .commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot add manifest with added files");

    ManifestEntry<DataFile> deleteEntry =
        manifestEntry(ManifestEntry.Status.DELETED, snapshot.snapshotId(), FILE_A);
    // update the entry's sequence number or else it will be rejected by the writer
    deleteEntry.setDataSequenceNumber(snapshot.sequenceNumber());

    ManifestFile invalidDeletedFileManifest = writeManifest("manifest-file-3.avro", deleteEntry);

    assertThatThrownBy(
            () ->
                table
                    .rewriteManifests()
                    .deleteManifest(manifest)
                    .addManifest(invalidDeletedFileManifest)
                    .commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot add manifest with deleted files");

    assertThatThrownBy(() -> table.rewriteManifests().deleteManifest(manifest).commit())
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith(
            "Replaced and created manifests must have the same number of active files");
  }

  @TestTemplate
  public void testManifestReplacementFailure() throws IOException {
    assertThat(table.currentSnapshot()).isNull();

    table.newFastAppend().appendFile(FILE_A).commit();

    Snapshot firstSnapshot = table.currentSnapshot();
    List<ManifestFile> firstSnapshotManifests = firstSnapshot.allManifests(table.io());
    assertThat(firstSnapshotManifests).hasSize(1);
    ManifestFile firstSnapshotManifest = firstSnapshotManifests.get(0);

    table.newFastAppend().appendFile(FILE_B).commit();

    Snapshot secondSnapshot = table.currentSnapshot();
    List<ManifestFile> secondSnapshotManifests = secondSnapshot.allManifests(table.io());
    assertThat(secondSnapshotManifests).hasSize(2);
    ManifestFile secondSnapshotManifest = secondSnapshotManifests.get(0);

    ManifestFile newManifest =
        writeManifest(
            "manifest-file.avro",
            manifestEntry(ManifestEntry.Status.EXISTING, firstSnapshot.snapshotId(), FILE_A),
            manifestEntry(ManifestEntry.Status.EXISTING, secondSnapshot.snapshotId(), FILE_B));

    table.updateProperties().set(TableProperties.COMMIT_NUM_RETRIES, "1").commit();

    table.ops().failCommits(5);

    RewriteManifests rewriteManifests = table.rewriteManifests();
    rewriteManifests.deleteManifest(firstSnapshotManifest);
    rewriteManifests.deleteManifest(secondSnapshotManifest);
    rewriteManifests.addManifest(newManifest);

    assertThatThrownBy(rewriteManifests::commit)
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Injected failure");

    assertThat(new File(newManifest.path())).exists();
  }

  @TestTemplate
  public void testManifestReplacementFailureWithSnapshotIdInheritance() throws IOException {
    assertThat(table.currentSnapshot()).isNull();

    table.updateProperties().set(SNAPSHOT_ID_INHERITANCE_ENABLED, "true").commit();

    table.newFastAppend().appendFile(FILE_A).commit();

    Snapshot firstSnapshot = table.currentSnapshot();
    List<ManifestFile> firstSnapshotManifests = firstSnapshot.allManifests(table.io());
    assertThat(firstSnapshotManifests).hasSize(1);
    ManifestFile firstSnapshotManifest = firstSnapshotManifests.get(0);

    table.newFastAppend().appendFile(FILE_B).commit();

    Snapshot secondSnapshot = table.currentSnapshot();
    List<ManifestFile> secondSnapshotManifests = secondSnapshot.allManifests(table.io());
    assertThat(secondSnapshotManifests).hasSize(2);
    ManifestFile secondSnapshotManifest = secondSnapshotManifests.get(0);

    ManifestFile newManifest =
        writeManifest(
            "manifest-file.avro",
            manifestEntry(ManifestEntry.Status.EXISTING, firstSnapshot.snapshotId(), FILE_A),
            manifestEntry(ManifestEntry.Status.EXISTING, secondSnapshot.snapshotId(), FILE_B));

    table.updateProperties().set(TableProperties.COMMIT_NUM_RETRIES, "1").commit();

    table.ops().failCommits(5);

    RewriteManifests rewriteManifests = table.rewriteManifests();
    rewriteManifests.deleteManifest(firstSnapshotManifest);
    rewriteManifests.deleteManifest(secondSnapshotManifest);
    rewriteManifests.addManifest(newManifest);

    assertThatThrownBy(rewriteManifests::commit)
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Injected failure");

    assertThat(new File(newManifest.path())).exists();
  }

  @TestTemplate
  public void testRewriteManifestsOnBranchUnsupported() {

    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(1);

    assertThatThrownBy(() -> table.rewriteManifests().toBranch("someBranch").commit())
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage(
            "Cannot commit to branch someBranch: org.apache.iceberg.BaseRewriteManifests does not support branch commits");
  }

  @TestTemplate
  public void testRewriteDataManifestsPreservesDeletes() {
    assumeThat(formatVersion).isGreaterThan(1);

    Table table = load();

    // commit data files
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    // save the append snapshot info
    Snapshot appendSnapshot = table.currentSnapshot();
    long appendSnapshotId = appendSnapshot.snapshotId();
    long appendSnapshotSeq = appendSnapshot.sequenceNumber();

    // commit delete files
    table.newRowDelta().addDeletes(FILE_A_DELETES).addDeletes(FILE_A2_DELETES).commit();

    // save the delete snapshot info
    Snapshot deleteSnapshot = table.currentSnapshot();
    long deleteSnapshotId = deleteSnapshot.snapshotId();
    long deleteSnapshotSeq = deleteSnapshot.sequenceNumber();

    // there must be 1 data and 1 delete manifest before the rewrite
    assertManifestCounts(table, 1, 1);

    // rewrite manifests and cluster entries by file path
    table.rewriteManifests().clusterBy(file -> file.path().toString()).commit();

    Snapshot rewriteSnapshot = table.currentSnapshot();

    validateSummary(rewriteSnapshot, 1, 1, 2, 2);

    // the rewrite must replace the original data manifest with 2 new data manifests
    List<ManifestFile> dataManifests = sortedDataManifests(table.io(), rewriteSnapshot);
    assertThat(dataManifests).hasSize(2);
    validateManifest(
        dataManifests.get(0),
        dataSeqs(appendSnapshotSeq, appendSnapshotSeq),
        fileSeqs(appendSnapshotSeq, appendSnapshotSeq),
        ids(appendSnapshotId),
        files(FILE_A),
        statuses(ManifestEntry.Status.EXISTING));
    validateManifest(
        dataManifests.get(1),
        dataSeqs(appendSnapshotSeq, appendSnapshotSeq),
        fileSeqs(appendSnapshotSeq, appendSnapshotSeq),
        ids(appendSnapshotId),
        files(FILE_B),
        statuses(ManifestEntry.Status.EXISTING));

    // the rewrite must preserve the original delete manifest (rewriting is not supported yet)
    List<ManifestFile> deleteManifests = rewriteSnapshot.deleteManifests(table.io());
    ManifestFile deleteManifest = Iterables.getOnlyElement(deleteManifests);
    validateDeleteManifest(
        deleteManifest,
        dataSeqs(deleteSnapshotSeq, deleteSnapshotSeq),
        fileSeqs(deleteSnapshotSeq, deleteSnapshotSeq),
        ids(deleteSnapshotId, deleteSnapshotId),
        files(FILE_A_DELETES, FILE_A2_DELETES),
        statuses(ManifestEntry.Status.ADDED, ManifestEntry.Status.ADDED));
  }

  @TestTemplate
  public void testReplaceDeleteManifestsOnly() throws IOException {
    assumeThat(formatVersion).isGreaterThan(1);

    Table table = load();

    // commit data files
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    // save the append snapshot info
    Snapshot appendSnapshot = table.currentSnapshot();
    long appendSnapshotId = appendSnapshot.snapshotId();
    long appendSnapshotSeq = appendSnapshot.sequenceNumber();

    // commit delete files
    table.newRowDelta().addDeletes(FILE_A_DELETES).addDeletes(FILE_A2_DELETES).commit();

    // save the delete snapshot info
    Snapshot deleteSnapshot = table.currentSnapshot();
    long deleteSnapshotId = deleteSnapshot.snapshotId();
    long deleteSnapshotSeq = deleteSnapshot.sequenceNumber();

    // there must be 1 data and 1 delete manifest before the rewrite
    assertManifestCounts(table, 1, 1);

    // split the original delete manifest into 2 new delete manifests
    ManifestFile originalDeleteManifest =
        Iterables.getOnlyElement(deleteSnapshot.deleteManifests(table.io()));
    ManifestFile newDeleteManifest1 =
        writeManifest(
            "delete-manifest-file-1.avro",
            manifestEntry(
                ManifestEntry.Status.EXISTING,
                deleteSnapshotId,
                deleteSnapshotSeq,
                deleteSnapshotSeq,
                FILE_A_DELETES));
    ManifestFile newDeleteManifest2 =
        writeManifest(
            "delete-manifest-file-2.avro",
            manifestEntry(
                ManifestEntry.Status.EXISTING,
                deleteSnapshotId,
                deleteSnapshotSeq,
                deleteSnapshotSeq,
                FILE_A2_DELETES));

    // replace the original delete manifest with the new delete manifests
    table
        .rewriteManifests()
        .deleteManifest(originalDeleteManifest)
        .addManifest(newDeleteManifest1)
        .addManifest(newDeleteManifest2)
        .commit();

    Snapshot rewriteSnapshot = table.currentSnapshot();

    // the rewrite must preserve the original data manifest
    ManifestFile dataManifest = Iterables.getOnlyElement(rewriteSnapshot.dataManifests(table.io()));
    validateManifest(
        dataManifest,
        dataSeqs(appendSnapshotSeq, appendSnapshotSeq),
        fileSeqs(appendSnapshotSeq, appendSnapshotSeq),
        ids(appendSnapshotId, appendSnapshotId),
        files(FILE_A, FILE_B),
        statuses(ManifestEntry.Status.ADDED, ManifestEntry.Status.ADDED));

    // the rewrite must replace the original delete manifest with 2 new delete manifests
    List<ManifestFile> deleteManifests = rewriteSnapshot.deleteManifests(table.io());
    assertThat(deleteManifests).hasSize(2);
    validateDeleteManifest(
        deleteManifests.get(0),
        dataSeqs(deleteSnapshotSeq),
        fileSeqs(deleteSnapshotSeq),
        ids(deleteSnapshotId),
        files(FILE_A_DELETES),
        statuses(ManifestEntry.Status.EXISTING));
    validateDeleteManifest(
        deleteManifests.get(1),
        dataSeqs(deleteSnapshotSeq),
        fileSeqs(deleteSnapshotSeq),
        ids(deleteSnapshotId),
        files(FILE_A2_DELETES),
        statuses(ManifestEntry.Status.EXISTING));
  }

  @TestTemplate
  public void testReplaceDataAndDeleteManifests() throws IOException {
    assumeThat(formatVersion).isGreaterThan(1);

    Table table = load();

    // commit data files
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    // save the append snapshot info
    Snapshot appendSnapshot = table.currentSnapshot();
    long appendSnapshotId = appendSnapshot.snapshotId();
    long appendSnapshotSeq = appendSnapshot.sequenceNumber();

    // commit delete files
    table.newRowDelta().addDeletes(FILE_A_DELETES).addDeletes(FILE_A2_DELETES).commit();

    // save the delete snapshot info
    Snapshot deleteSnapshot = table.currentSnapshot();
    long deleteSnapshotId = deleteSnapshot.snapshotId();
    long deleteSnapshotSeq = deleteSnapshot.sequenceNumber();

    // there must be 1 data and 1 delete manifest before the rewrite
    assertManifestCounts(table, 1, 1);

    // split the original data manifest into 2 new data manifests
    ManifestFile originalDataManifest =
        Iterables.getOnlyElement(deleteSnapshot.dataManifests(table.io()));
    ManifestFile newDataManifest1 =
        writeManifest(
            "manifest-file-1.avro",
            manifestEntry(
                ManifestEntry.Status.EXISTING,
                appendSnapshotId,
                appendSnapshotSeq,
                appendSnapshotSeq,
                FILE_A));
    ManifestFile newDataManifest2 =
        writeManifest(
            "manifest-file-2.avro",
            manifestEntry(
                ManifestEntry.Status.EXISTING,
                appendSnapshotId,
                appendSnapshotSeq,
                appendSnapshotSeq,
                FILE_B));

    // split the original delete manifest into 2 new delete manifests
    ManifestFile originalDeleteManifest =
        Iterables.getOnlyElement(deleteSnapshot.deleteManifests(table.io()));
    ManifestFile newDeleteManifest1 =
        writeManifest(
            "delete-manifest-file-1.avro",
            manifestEntry(
                ManifestEntry.Status.EXISTING,
                deleteSnapshotId,
                deleteSnapshotSeq,
                deleteSnapshotSeq,
                FILE_A_DELETES));
    ManifestFile newDeleteManifest2 =
        writeManifest(
            "delete-manifest-file-2.avro",
            manifestEntry(
                ManifestEntry.Status.EXISTING,
                deleteSnapshotId,
                deleteSnapshotSeq,
                deleteSnapshotSeq,
                FILE_A2_DELETES));

    // replace the original data and delete manifests with new ones
    table
        .rewriteManifests()
        .deleteManifest(originalDataManifest)
        .addManifest(newDataManifest1)
        .addManifest(newDataManifest2)
        .deleteManifest(originalDeleteManifest)
        .addManifest(newDeleteManifest1)
        .addManifest(newDeleteManifest2)
        .commit();

    Snapshot rewriteSnapshot = table.currentSnapshot();

    // the rewrite must replace the original data manifest with 2 new data manifests
    List<ManifestFile> dataManifests = sortedDataManifests(table.io(), rewriteSnapshot);
    assertThat(dataManifests).hasSize(2);
    validateManifest(
        dataManifests.get(0),
        dataSeqs(appendSnapshotSeq),
        fileSeqs(appendSnapshotSeq),
        ids(appendSnapshotId),
        files(FILE_A),
        statuses(ManifestEntry.Status.EXISTING));
    validateManifest(
        dataManifests.get(1),
        dataSeqs(appendSnapshotSeq),
        fileSeqs(appendSnapshotSeq),
        ids(appendSnapshotId),
        files(FILE_B),
        statuses(ManifestEntry.Status.EXISTING));

    // the rewrite must replace the original delete manifest with 2 new delete manifests
    List<ManifestFile> deleteManifests = rewriteSnapshot.deleteManifests(table.io());
    assertThat(deleteManifests).hasSize(2);
    validateDeleteManifest(
        deleteManifests.get(0),
        dataSeqs(deleteSnapshotSeq),
        fileSeqs(deleteSnapshotSeq),
        ids(deleteSnapshotId),
        files(FILE_A_DELETES),
        statuses(ManifestEntry.Status.EXISTING));
    validateDeleteManifest(
        deleteManifests.get(1),
        dataSeqs(deleteSnapshotSeq),
        fileSeqs(deleteSnapshotSeq),
        ids(deleteSnapshotId),
        files(FILE_A2_DELETES),
        statuses(ManifestEntry.Status.EXISTING));
  }

  @TestTemplate
  public void testDeleteManifestReplacementConcurrentAppend() throws IOException {
    assumeThat(formatVersion).isGreaterThan(1);

    // commit data files
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    // save the initial append snapshot info
    Snapshot appendSnapshot = table.currentSnapshot();
    long appendSnapshotId = appendSnapshot.snapshotId();
    long appendSnapshotSeq = appendSnapshot.sequenceNumber();

    // commit delete files
    table.newRowDelta().addDeletes(FILE_A_DELETES).addDeletes(FILE_A2_DELETES).commit();

    // save the delete snapshot info
    Snapshot deleteSnapshot = table.currentSnapshot();
    long deleteSnapshotId = deleteSnapshot.snapshotId();
    long deleteSnapshotSeq = deleteSnapshot.sequenceNumber();

    // split the original delete manifest into 2 new delete manifests
    ManifestFile originalDeleteManifest =
        Iterables.getOnlyElement(deleteSnapshot.deleteManifests(table.io()));
    ManifestFile newDeleteManifest1 =
        writeManifest(
            "delete-manifest-file-1.avro",
            manifestEntry(
                ManifestEntry.Status.EXISTING,
                deleteSnapshotId,
                deleteSnapshotSeq,
                deleteSnapshotSeq,
                FILE_A_DELETES));
    ManifestFile newDeleteManifest2 =
        writeManifest(
            "delete-manifest-file-2.avro",
            manifestEntry(
                ManifestEntry.Status.EXISTING,
                deleteSnapshotId,
                deleteSnapshotSeq,
                deleteSnapshotSeq,
                FILE_A2_DELETES));

    // start the rewrite
    RewriteManifests rewriteManifests = table.rewriteManifests();
    rewriteManifests.deleteManifest(originalDeleteManifest);
    rewriteManifests.addManifest(newDeleteManifest1);
    rewriteManifests.addManifest(newDeleteManifest2);

    // commit another append concurrently
    table.newFastAppend().appendFile(FILE_C).appendFile(FILE_D).commit();

    // save the concurrent snapshot info
    Snapshot concurrentSnapshot = table.currentSnapshot();
    long concurrentSnapshotSeq = concurrentSnapshot.sequenceNumber();
    long concurrentSnapshotId = concurrentSnapshot.snapshotId();

    // there must be 2 data manifests and 1 delete manifest before the rewrite is committed
    assertManifestCounts(table, 2, 1);

    // commit the rewrite successfully as operations are not in conflict
    rewriteManifests.commit();

    Snapshot rewriteSnapshot = table.currentSnapshot();

    validateSummary(rewriteSnapshot, 1, 2, 2, 0);

    // the rewrite must preserve the original and added concurrently data manifests
    List<ManifestFile> dataManifests = rewriteSnapshot.dataManifests(table.io());
    assertThat(dataManifests).hasSize(2);
    validateManifest(
        dataManifests.get(0),
        dataSeqs(concurrentSnapshotSeq, concurrentSnapshotSeq),
        fileSeqs(concurrentSnapshotSeq, concurrentSnapshotSeq),
        ids(concurrentSnapshotId, concurrentSnapshotId),
        files(FILE_C, FILE_D),
        statuses(ManifestEntry.Status.ADDED, ManifestEntry.Status.ADDED));
    validateManifest(
        dataManifests.get(1),
        dataSeqs(appendSnapshotSeq, appendSnapshotSeq),
        fileSeqs(appendSnapshotSeq, appendSnapshotSeq),
        ids(appendSnapshotId, appendSnapshotId),
        files(FILE_A, FILE_B),
        statuses(ManifestEntry.Status.ADDED, ManifestEntry.Status.ADDED));

    // the rewrite must replace the original delete manifest with 2 new delete manifests
    List<ManifestFile> deleteManifests = rewriteSnapshot.deleteManifests(table.io());
    assertThat(deleteManifests).hasSize(2);
    validateDeleteManifest(
        deleteManifests.get(0),
        dataSeqs(deleteSnapshotSeq),
        fileSeqs(deleteSnapshotSeq),
        ids(deleteSnapshotId),
        files(FILE_A_DELETES),
        statuses(ManifestEntry.Status.EXISTING));
    validateDeleteManifest(
        deleteManifests.get(1),
        dataSeqs(deleteSnapshotSeq),
        fileSeqs(deleteSnapshotSeq),
        ids(deleteSnapshotId),
        files(FILE_A2_DELETES),
        statuses(ManifestEntry.Status.EXISTING));
  }

  @TestTemplate
  public void testDeleteManifestReplacementConcurrentDeleteFileRemoval() throws IOException {
    assumeThat(formatVersion).isGreaterThan(1);

    // commit data files
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    // save the initial append snapshot info
    Snapshot appendSnapshot = table.currentSnapshot();
    long appendSnapshotId = appendSnapshot.snapshotId();
    long appendSnapshotSeq = appendSnapshot.sequenceNumber();

    // commit the first set of delete files
    table.newRowDelta().addDeletes(FILE_A_DELETES).addDeletes(FILE_A2_DELETES).commit();

    // save the first delete snapshot info
    Snapshot deleteSnapshot1 = table.currentSnapshot();
    long deleteSnapshotId1 = deleteSnapshot1.snapshotId();
    long deleteSnapshotSeq1 = deleteSnapshot1.sequenceNumber();

    // commit the second set of delete files
    table.newRowDelta().addDeletes(FILE_B_DELETES).addDeletes(FILE_C2_DELETES).commit();

    // save the second delete snapshot info
    Snapshot deleteSnapshot2 = table.currentSnapshot();
    long deleteSnapshotId2 = deleteSnapshot2.snapshotId();
    long deleteSnapshotSeq2 = deleteSnapshot2.sequenceNumber();

    // split the original delete manifest into 2 new delete manifests
    ManifestFile originalDeleteManifest = deleteSnapshot1.deleteManifests(table.io()).get(0);
    ManifestFile newDeleteManifest1 =
        writeManifest(
            "delete-manifest-file-1.avro",
            manifestEntry(
                ManifestEntry.Status.EXISTING,
                deleteSnapshotId1,
                deleteSnapshotSeq1,
                deleteSnapshotSeq1,
                FILE_A_DELETES));
    ManifestFile newDeleteManifest2 =
        writeManifest(
            "delete-manifest-file-2.avro",
            manifestEntry(
                ManifestEntry.Status.EXISTING,
                deleteSnapshotId1,
                deleteSnapshotSeq1,
                deleteSnapshotSeq1,
                FILE_A2_DELETES));

    // start the rewrite
    RewriteManifests rewriteManifests = table.rewriteManifests();
    rewriteManifests.deleteManifest(originalDeleteManifest);
    rewriteManifests.addManifest(newDeleteManifest1);
    rewriteManifests.addManifest(newDeleteManifest2);

    // commit the third set of delete files concurrently
    table.newRewrite().deleteFile(FILE_B_DELETES).commit();

    Snapshot concurrentSnapshot = table.currentSnapshot();
    long concurrentSnapshotId = concurrentSnapshot.snapshotId();

    // there must be 1 data manifest and 2 delete manifests before the rewrite is committed
    assertManifestCounts(table, 1, 2);

    // commit the rewrite successfully as operations are not in conflict
    rewriteManifests.commit();

    Snapshot rewriteSnapshot = table.currentSnapshot();

    validateSummary(rewriteSnapshot, 1, 2, 2, 0);

    // the rewrite must preserve the original data manifest
    ManifestFile dataManifest = Iterables.getOnlyElement(rewriteSnapshot.dataManifests(table.io()));
    validateManifest(
        dataManifest,
        dataSeqs(appendSnapshotSeq, appendSnapshotSeq),
        fileSeqs(appendSnapshotSeq, appendSnapshotSeq),
        ids(appendSnapshotId, appendSnapshotId),
        files(FILE_A, FILE_B),
        statuses(ManifestEntry.Status.ADDED, ManifestEntry.Status.ADDED));

    // the rewrite must replace the first delete manifest with 2 new delete manifests
    // the rewrite must also keep the second delete manifest modified concurrently
    List<ManifestFile> deleteManifests = rewriteSnapshot.deleteManifests(table.io());
    assertThat(deleteManifests).hasSize(3);
    validateDeleteManifest(
        deleteManifests.get(0),
        dataSeqs(deleteSnapshotSeq1),
        fileSeqs(deleteSnapshotSeq1),
        ids(deleteSnapshotId1),
        files(FILE_A_DELETES),
        statuses(ManifestEntry.Status.EXISTING));
    validateDeleteManifest(
        deleteManifests.get(1),
        dataSeqs(deleteSnapshotSeq1),
        fileSeqs(deleteSnapshotSeq1),
        ids(deleteSnapshotId1),
        files(FILE_A2_DELETES),
        statuses(ManifestEntry.Status.EXISTING));
    validateDeleteManifest(
        deleteManifests.get(2),
        dataSeqs(deleteSnapshotSeq2, deleteSnapshotSeq2),
        fileSeqs(deleteSnapshotSeq2, deleteSnapshotSeq2),
        ids(concurrentSnapshotId, deleteSnapshotId2),
        files(FILE_B_DELETES, FILE_C2_DELETES),
        statuses(ManifestEntry.Status.DELETED, ManifestEntry.Status.EXISTING));
  }

  @TestTemplate
  public void testDeleteManifestReplacementConflictingDeleteFileRemoval() throws IOException {
    assumeThat(formatVersion).isGreaterThan(1);

    // commit data files
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).appendFile(FILE_C).commit();

    // commit delete files
    table.newRowDelta().addDeletes(FILE_A_DELETES).addDeletes(FILE_A2_DELETES).commit();

    // save the delete snapshot info
    Snapshot deleteSnapshot = table.currentSnapshot();
    long deleteSnapshotId = deleteSnapshot.snapshotId();
    long deleteSnapshotSeq = deleteSnapshot.sequenceNumber();

    // split the original delete manifest into 2 new delete manifests
    ManifestFile originalDeleteManifest = deleteSnapshot.deleteManifests(table.io()).get(0);
    ManifestFile newDeleteManifest1 =
        writeManifest(
            "delete-manifest-file-1.avro",
            manifestEntry(
                ManifestEntry.Status.EXISTING,
                deleteSnapshotId,
                deleteSnapshotSeq,
                deleteSnapshotSeq,
                FILE_A_DELETES));
    ManifestFile newDeleteManifest2 =
        writeManifest(
            "delete-manifest-file-2.avro",
            manifestEntry(
                ManifestEntry.Status.EXISTING,
                deleteSnapshotId,
                deleteSnapshotSeq,
                deleteSnapshotSeq,
                FILE_A2_DELETES));

    // start the rewrite
    RewriteManifests rewriteManifests = table.rewriteManifests();
    rewriteManifests.deleteManifest(originalDeleteManifest);
    rewriteManifests.addManifest(newDeleteManifest1);
    rewriteManifests.addManifest(newDeleteManifest2);

    // modify the original delete manifest concurrently
    table.newRewrite().deleteFile(FILE_A_DELETES).commit();

    // the rewrite must fail as the original delete manifest was replaced concurrently
    assertThatThrownBy(rewriteManifests::commit)
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Manifest is missing");
  }

  @TestTemplate
  public void testDeleteManifestReplacementFailure() throws IOException {
    assumeThat(formatVersion).isGreaterThan(1);

    // commit a data file
    table.newFastAppend().appendFile(FILE_A).commit();

    // commit the first delete file
    table.newRowDelta().addDeletes(FILE_A_DELETES).commit();

    // save the first delete snapshot info
    Snapshot deleteSnapshot1 = table.currentSnapshot();
    long deleteSnapshotId1 = deleteSnapshot1.snapshotId();
    long deleteSnapshotSeq1 = deleteSnapshot1.sequenceNumber();

    // commit the second delete file
    table.newRowDelta().addDeletes(FILE_A2_DELETES).commit();

    // save the second delete snapshot info
    Snapshot deleteSnapshot2 = table.currentSnapshot();
    long deleteSnapshotId2 = deleteSnapshot2.snapshotId();
    long deleteSnapshotSeq2 = deleteSnapshot2.sequenceNumber();

    // there must be 1 data manifest and 2 delete manifests before the rewrite
    assertManifestCounts(table, 1, 2);

    // combine the original delete manifests into 1 new delete manifest
    ManifestFile newDeleteManifest =
        writeManifest(
            "delete-manifest-file.avro",
            manifestEntry(
                ManifestEntry.Status.EXISTING,
                deleteSnapshotId1,
                deleteSnapshotSeq1,
                deleteSnapshotSeq1,
                FILE_A_DELETES),
            manifestEntry(
                ManifestEntry.Status.EXISTING,
                deleteSnapshotId2,
                deleteSnapshotSeq2,
                deleteSnapshotSeq2,
                FILE_A2_DELETES));

    // configure the table operations to fail
    table.updateProperties().set(TableProperties.COMMIT_NUM_RETRIES, "1").commit();
    table.ops().failCommits(5);

    // start the rewrite
    RewriteManifests rewriteManifests = table.rewriteManifests();
    List<ManifestFile> originalDeleteManifests = deleteSnapshot2.deleteManifests(table.io());
    for (ManifestFile originalDeleteManifest : originalDeleteManifests) {
      rewriteManifests.deleteManifest(originalDeleteManifest);
    }
    rewriteManifests.addManifest(newDeleteManifest);

    // the rewrite must fail
    assertThatThrownBy(rewriteManifests::commit)
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Injected failure");

    // the new manifest must not be deleted as the commit hasn't succeeded
    assertThat(new File(newDeleteManifest.path())).exists();
  }

  private void assertManifestCounts(
      Table table, int expectedDataManifestCount, int expectedDeleteManifestCount) {
    Snapshot snapshot = table.currentSnapshot();
    assertThat(snapshot.dataManifests(table.io())).hasSize(expectedDataManifestCount);
    assertThat(snapshot.deleteManifests(table.io())).hasSize(expectedDeleteManifestCount);
  }

  private List<ManifestFile> sortedDataManifests(FileIO io, Snapshot snapshot) {
    List<ManifestFile> manifests = Lists.newArrayList(snapshot.dataManifests(io));
    manifests.sort(Comparator.comparing(ManifestFile::path));
    return manifests;
  }

  private void validateSummary(
      Snapshot snapshot, int replaced, int kept, int created, int entryCount) {
    Map<String, String> summary = snapshot.summary();
    assertThat(summary)
        .containsEntry("manifests-replaced", String.valueOf(replaced))
        .containsEntry("manifests-kept", String.valueOf(kept))
        .containsEntry("manifests-created", String.valueOf(created))
        .containsEntry("entries-processed", String.valueOf(entryCount));
  }

  private void matchNumberOfManifestFileWithSpecId(
      List<ManifestFile> manifestFiles,
      int toBeMatchedPartitionSpecId,
      int numberOfManifestWithPartitionSpecID) {
    long matchedManifestsCounter =
        manifestFiles.stream()
            .filter(m -> m.partitionSpecId() == toBeMatchedPartitionSpecId)
            .count();

    assertThat(matchedManifestsCounter)
        .as(
            "manifest list should have "
                + numberOfManifestWithPartitionSpecID
                + " manifests matching this partitionSpecId "
                + toBeMatchedPartitionSpecId)
        .isEqualTo(numberOfManifestWithPartitionSpecID);
  }
}
