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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.required;

public class TableTestBase {
  // Schema passed to create tables
  public static final Schema SCHEMA = new Schema(
      required(3, "id", Types.IntegerType.get()),
      required(4, "data", Types.StringType.get())
  );

  // Partition spec used to create tables
  static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA)
      .bucket("data", 16)
      .build();

  static final DataFile FILE_A = DataFiles.builder(SPEC)
      .withPath("/path/to/data-a.parquet")
      .withFileSizeInBytes(0)
      .withPartitionPath("data_bucket=0") // easy way to set partition data for now
      .withRecordCount(1)
      .build();
  static final DataFile FILE_B = DataFiles.builder(SPEC)
      .withPath("/path/to/data-b.parquet")
      .withFileSizeInBytes(0)
      .withPartitionPath("data_bucket=1") // easy way to set partition data for now
      .withRecordCount(1)
      .build();
  static final DataFile FILE_C = DataFiles.builder(SPEC)
      .withPath("/path/to/data-c.parquet")
      .withFileSizeInBytes(0)
      .withPartitionPath("data_bucket=2") // easy way to set partition data for now
      .withRecordCount(1)
      .build();
  static final DataFile FILE_D = DataFiles.builder(SPEC)
      .withPath("/path/to/data-d.parquet")
      .withFileSizeInBytes(0)
      .withPartitionPath("data_bucket=3") // easy way to set partition data for now
      .withRecordCount(1)
      .build();

  static final FileIO FILE_IO = new TestTables.LocalFileIO();

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  File tableDir = null;
  File metadataDir = null;
  public TestTables.TestTable table = null;

  protected final int formatVersion;

  public TableTestBase(int formatVersion) {
    this.formatVersion = formatVersion;
  }

  @Before
  public void setupTable() throws Exception {
    this.tableDir = temp.newFolder();
    tableDir.delete(); // created by table create

    this.metadataDir = new File(tableDir, "metadata");
    this.table = create(SCHEMA, SPEC);
  }

  @After
  public void cleanupTables() {
    TestTables.clearTables();
  }

  List<File> listManifestFiles() {
    return listManifestFiles(tableDir);
  }

  List<File> listManifestFiles(File tableDirToList) {
    return Lists.newArrayList(new File(tableDirToList, "metadata").listFiles((dir, name) ->
        !name.startsWith("snap") && Files.getFileExtension(name).equalsIgnoreCase("avro")));
  }

  TestTables.TestTable create(Schema schema, PartitionSpec spec) {
    return TestTables.create(tableDir, "test", schema, spec, formatVersion);
  }

  TestTables.TestTable load() {
    return TestTables.load(tableDir, "test");
  }

  Integer version() {
    return TestTables.metadataVersion("test");
  }

  public TableMetadata readMetadata() {
    return TestTables.readMetadata("test");
  }

  ManifestFile writeManifest(DataFile... files) throws IOException {
    return writeManifest(null, files);
  }

  ManifestFile writeManifest(Long snapshotId, DataFile... files) throws IOException {
    File manifestFile = temp.newFile("input.m0.avro");
    Assert.assertTrue(manifestFile.delete());
    OutputFile outputFile = table.ops().io().newOutputFile(manifestFile.getCanonicalPath());

    ManifestWriter writer = ManifestFiles.write(formatVersion, table.spec(), outputFile, snapshotId);
    try {
      for (DataFile file : files) {
        writer.add(file);
      }
    } finally {
      writer.close();
    }

    return writer.toManifestFile();
  }

  ManifestFile writeManifest(String fileName, ManifestEntry... entries) throws IOException {
    return writeManifest(null, fileName, entries);
  }

  ManifestFile writeManifest(Long snapshotId, ManifestEntry... entries) throws IOException {
    return writeManifest(snapshotId, "input.m0.avro", entries);
  }

  ManifestFile writeManifest(Long snapshotId, String fileName, ManifestEntry... entries) throws IOException {
    File manifestFile = temp.newFile(fileName);
    Assert.assertTrue(manifestFile.delete());
    OutputFile outputFile = table.ops().io().newOutputFile(manifestFile.getCanonicalPath());

    ManifestWriter writer = ManifestFiles.write(formatVersion, table.spec(), outputFile, snapshotId);
    try {
      for (ManifestEntry entry : entries) {
        writer.addEntry(entry);
      }
    } finally {
      writer.close();
    }

    return writer.toManifestFile();
  }

  ManifestFile writeManifestWithName(String name, DataFile... files) throws IOException {
    File manifestFile = temp.newFile(name + ".avro");
    Assert.assertTrue(manifestFile.delete());
    OutputFile outputFile = table.ops().io().newOutputFile(manifestFile.getCanonicalPath());

    ManifestWriter writer = ManifestFiles.write(formatVersion, table.spec(), outputFile, null);
    try {
      for (DataFile file : files) {
        writer.add(file);
      }
    } finally {
      writer.close();
    }

    return writer.toManifestFile();
  }

  ManifestEntry manifestEntry(ManifestEntry.Status status, Long snapshotId, DataFile file) {
    GenericManifestEntry entry = new GenericManifestEntry(table.spec().partitionType());
    switch (status) {
      case ADDED:
        return entry.wrapAppend(snapshotId, file);
      case EXISTING:
        return entry.wrapExisting(snapshotId, 0L, file);
      case DELETED:
        return entry.wrapDelete(snapshotId, file);
      default:
        throw new IllegalArgumentException("Unexpected entry status: " + status);
    }
  }

  void validateSnapshot(Snapshot old, Snapshot snap, DataFile... newFiles) {
    List<ManifestFile> oldManifests = old != null ? old.manifests() : ImmutableList.of();

    // copy the manifests to a modifiable list and remove the existing manifests
    List<ManifestFile> newManifests = Lists.newArrayList(snap.manifests());
    for (ManifestFile oldManifest : oldManifests) {
      Assert.assertTrue("New snapshot should contain old manifests",
          newManifests.remove(oldManifest));
    }

    Assert.assertEquals("Should create 1 new manifest and reuse old manifests",
        1, newManifests.size());
    ManifestFile manifest = newManifests.get(0);

    long id = snap.snapshotId();
    Iterator<String> newPaths = paths(newFiles).iterator();

    for (ManifestEntry entry : ManifestFiles.read(manifest, FILE_IO).entries()) {
      DataFile file = entry.file();
      Assert.assertEquals("Path should match expected", newPaths.next(), file.path().toString());
      Assert.assertEquals("File's snapshot ID should match", id, (long) entry.snapshotId());
    }

    Assert.assertFalse("Should find all files in the manifest", newPaths.hasNext());
  }

  void validateTableFiles(Table tbl, DataFile... expectedFiles) {
    Set<CharSequence> expectedFilePaths = Sets.newHashSet();
    for (DataFile file : expectedFiles) {
      expectedFilePaths.add(file.path());
    }
    Set<CharSequence> actualFilePaths = Sets.newHashSet();
    for (FileScanTask task : tbl.newScan().planFiles()) {
      actualFilePaths.add(task.file().path());
    }
    Assert.assertEquals("Files should match", expectedFilePaths, actualFilePaths);
  }

  List<String> paths(DataFile... dataFiles) {
    List<String> paths = Lists.newArrayListWithExpectedSize(dataFiles.length);
    for (DataFile file : dataFiles) {
      paths.add(file.path().toString());
    }
    return paths;
  }

  static void validateManifest(ManifestFile manifest,
                               Iterator<Long> ids,
                               Iterator<DataFile> expectedFiles) {
    for (ManifestEntry entry : ManifestFiles.read(manifest, FILE_IO).entries()) {
      DataFile file = entry.file();
      DataFile expected = expectedFiles.next();
      Assert.assertEquals("Path should match expected",
          expected.path().toString(), file.path().toString());
      Assert.assertEquals("Snapshot ID should match expected ID",
          ids.next(), entry.snapshotId());
    }

    Assert.assertFalse("Should find all files in the manifest", expectedFiles.hasNext());
  }

  static void validateManifestEntries(ManifestFile manifest,
                                      Iterator<Long> ids,
                                      Iterator<DataFile> expectedFiles,
                                      Iterator<ManifestEntry.Status> expectedStatuses) {
    for (ManifestEntry entry : ManifestFiles.read(manifest, FILE_IO).entries()) {
      DataFile file = entry.file();
      DataFile expected = expectedFiles.next();
      final ManifestEntry.Status expectedStatus = expectedStatuses.next();
      Assert.assertEquals("Path should match expected",
          expected.path().toString(), file.path().toString());
      Assert.assertEquals("Snapshot ID should match expected ID",
          ids.next(), entry.snapshotId());
      Assert.assertEquals("Entry status should match expected ID",
          expectedStatus, entry.status());
    }

    Assert.assertFalse("Should find all files in the manifest", expectedFiles.hasNext());
  }

  static Iterator<ManifestEntry.Status> statuses(ManifestEntry.Status... statuses) {
    return Iterators.forArray(statuses);
  }

  static Iterator<Long> ids(Long... ids) {
    return Iterators.forArray(ids);
  }

  static Iterator<DataFile> files(DataFile... files) {
    return Iterators.forArray(files);
  }

  static Iterator<DataFile> files(ManifestFile manifest) {
    return ManifestFiles.read(manifest, FILE_IO).iterator();
  }
}
