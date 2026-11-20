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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.encryption.EncryptionUtil;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Iterators;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public class AdaptiveMetadataTestBase {
  // Schema passed to create tables
  public static final Schema SCHEMA =
      new Schema(
          required(3, "id", Types.IntegerType.get()), required(4, "data", Types.StringType.get()));

  protected static final int BUCKETS_NUMBER = 16;

  // Partition spec used to create tables
  public static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).bucket("data", BUCKETS_NUMBER).build();

  public static final DataFile FILE_A =
      DataFiles.builder(SPEC)
          .withPath("file:/warehouse/table/data/data_bucket=0/data-a.parquet")
          .withFileSizeInBytes(10)
          .withPartition(TestHelpers.Row.of(0))
          .withRecordCount(1)
          .build();

  static final DeleteFile FILE_A_DV =
      FileMetadata.deleteFileBuilder(SPEC)
          .ofPositionDeletes()
          .withPath("file:/warehouse/table/data/data_bucket=0/data-a-deletes.puffin")
          .withFileSizeInBytes(10)
          .withPartition(TestHelpers.Row.of(0))
          .withRecordCount(1)
          .withReferencedDataFile(FILE_A.location())
          .withContentOffset(4)
          .withContentSizeInBytes(6)
          .build();

  static final DeleteFile FILE_A_EQ_DELETES =
      FileMetadata.deleteFileBuilder(SPEC)
          .ofEqualityDeletes(1)
          .withPath("file:/warehouse/table/data/data_bucket=0/data-a-eq-deletes.parquet")
          .withFileSizeInBytes(10)
          .withPartition(TestHelpers.Row.of(0))
          .withRecordCount(1)
          .build();

  public static final DataFile FILE_B =
      DataFiles.builder(SPEC)
          .withPath("file:/warehouse/table/data/data_bucket=1/data-b.parquet")
          .withFileSizeInBytes(10)
          .withPartition(TestHelpers.Row.of(1))
          .withRecordCount(1)
          .withSplitOffsets(ImmutableList.of(1L))
          .build();

  private static final Iterator<FileContent> ALL_DATA_FILES = Iterators.cycle(FileContent.DATA);

  public InMemoryFileIO io = new InMemoryFileIO();
  public InMemoryTables.Table defaultTable = null;

  @BeforeEach
  public void setupTable() throws Exception {
    this.defaultTable = create("test", SCHEMA, SPEC, 4);
  }

  @AfterEach
  public void cleanupTables() {
    InMemoryTables.clearTables();
    io.clear();
  }

  protected InMemoryTables.Table create(
      String name, Schema schema, PartitionSpec spec, int formatVersion) {
    return InMemoryTables.create(name, io, schema, spec, formatVersion);
  }

  protected InMemoryTables.Table load(String name) {
    return InMemoryTables.load(name, io);
  }

  protected InMemoryTables.Table upgrade(String name, int newFormatVersion) {
    return InMemoryTables.upgrade(name, io, newFormatVersion);
  }

  public TableMetadata readMetadata() {
    return InMemoryTables.readMetadata("test");
  }

  protected Snapshot commit(Table table, SnapshotUpdate<?> snapshotUpdate, String branch) {
    Snapshot snapshot;
    if (branch.equals(SnapshotRef.MAIN_BRANCH)) {
      snapshotUpdate.commit();
      snapshot = table.currentSnapshot();
    } else {
      ((SnapshotProducer<?>) snapshotUpdate.toBranch(branch)).commit();
      snapshot = table.snapshot(branch);
    }

    return snapshot;
  }

  protected Snapshot apply(SnapshotUpdate<?> snapshotUpdate, String branch) {
    if (branch.equals(SnapshotRef.MAIN_BRANCH)) {
      return snapshotUpdate.apply();
    } else {
      return ((SnapshotProducer<?>) snapshotUpdate.toBranch(branch)).apply();
    }
  }

  protected static List<TrackedFile> readRoot(Table table, Snapshot snapshot) throws IOException {
    ManifestFile root = RootManifestFile.from(snapshot, table.location(), table.io());
    try (V4ManifestReader reader =
        V4ManifestReader.builder(root, table.io(), table.schema(), table.specs())
            .metricsConfig(MetricsConfig.forTable(table))
            .tableLocation(table.location())
            .build()) {
      return Lists.newArrayList(reader);
    }
  }

  protected void validateNoLeafManifests(List<TrackedFile> files) {
    assertThat(files).extracting(TrackedFile::contentType).containsOnly(FileContent.DATA);
  }

  protected Iterable<String> listLeafManifests(Table table) {
    SupportsPrefixOperations fileIO = (SupportsPrefixOperations) table.io();
    return Iterables.transform(
        fileIO.listPrefix(table.location() + "/metadata/leaf-"), FileInfo::location);
  }

  protected Iterable<String> listRootManifests(Table table) {
    SupportsPrefixOperations fileIO = (SupportsPrefixOperations) table.io();
    return Iterables.transform(
        fileIO.listPrefix(table.location() + "/metadata/root-"), FileInfo::location);
  }

  protected ManifestFile writeLeafManifest(Table table, OutputFile output, DataFile... files)
      throws IOException {
    Types.StructType contentStatsType =
        StatsUtil.statsWriteSchema(table.schema(), MetricsConfig.forTable(table));
    TrackedFileAdapters.DataTrackedFile wrapper = TrackedFileAdapters.forDataFile(table.schema());

    V4ManifestWriter writer =
        V4ManifestWriter.createLeaf(
            EncryptionUtil.plainAsEncryptedOutput(output),
            table.specs(),
            contentStatsType,
            Map.of(),
            table.schema().schemaId(),
            null);
    try (writer) {
      for (DataFile file : files) {
        writer.append(wrapper.wrap(file));
      }
    }

    return TrackedFileAdapters.asManifestFile(writer.toTrackedManifest());
  }

  protected static Iterator<FileContent> contents(FileContent... contents) {
    return Iterators.forArray(contents);
  }

  protected static Iterator<Long> dataSeqs(Long... seqs) {
    return Iterators.forArray(seqs);
  }

  protected static Iterator<Long> fileSeqs(Long... seqs) {
    return Iterators.forArray(seqs);
  }

  protected static Iterator<Long> ids(Long... ids) {
    return Iterators.forArray(ids);
  }

  protected static Iterator<String> locations(String... locations) {
    return Iterators.forArray(locations);
  }

  protected static Iterator<String> locations(DataFile... files) {
    return locations(Arrays.asList(files));
  }

  protected static Iterator<String> locations(List<DataFile> files) {
    return Iterables.transform(files, DataFile::location).iterator();
  }

  protected static Iterator<EntryStatus> statuses(EntryStatus... statuses) {
    return statuses(Arrays.asList(statuses));
  }

  protected static Iterator<EntryStatus> statuses(List<EntryStatus> statuses) {
    return statuses.iterator();
  }

  protected void validateFiles(
      List<TrackedFile> files, Iterator<Long> ids, Iterator<String> locations) {
    validateFiles(files, ALL_DATA_FILES, null, null, ids, locations, null);
  }

  protected void validateFiles(
      List<TrackedFile> files,
      Iterator<Long> dataSeqs,
      Iterator<Long> fileSeqs,
      Iterator<Long> ids,
      Iterator<String> locations) {
    validateFiles(files, ALL_DATA_FILES, dataSeqs, fileSeqs, ids, locations, null);
  }

  protected void validateFiles(
      List<TrackedFile> files,
      Iterator<Long> dataSeqs,
      Iterator<Long> fileSeqs,
      Iterator<Long> ids,
      Iterator<String> locations,
      Iterator<EntryStatus> statuses) {
    validateFiles(files, ALL_DATA_FILES, dataSeqs, fileSeqs, ids, locations, statuses);
  }

  protected void validateFiles(
      List<TrackedFile> files,
      Iterator<FileContent> contents,
      Iterator<Long> dataSeqs,
      Iterator<Long> fileSeqs,
      Iterator<Long> ids,
      Iterator<String> locations,
      Iterator<EntryStatus> statuses) {
    for (TrackedFile file : files) {
      assertThat(file.contentType()).as("Content type").isEqualTo(contents.next());
      assertThat(file.location()).as("Location").isEqualTo(locations.next());
      assertThat(file.tracking().snapshotId()).as("Snapshot ID").isEqualTo(ids.next());
      if (dataSeqs != null) {
        assertThat(file.tracking().dataSequenceNumber()).as("Data seq").isEqualTo(dataSeqs.next());
      }

      if (fileSeqs != null) {
        assertThat(file.tracking().fileSequenceNumber()).as("File seq").isEqualTo(fileSeqs.next());
      }

      if (statuses != null) {
        assertThat(file.tracking().status()).as("Status").isEqualTo(statuses.next());
      }
    }

    assertThat(locations).as("Should match expected number of files").isExhausted();
  }

  protected void validateBranchFiles(Table table, String ref, DataFile... expectedFiles) {
    validateBranchFiles(table, ref, Arrays.asList(expectedFiles));
  }

  protected void validateBranchFiles(Table table, String ref, Iterable<DataFile> expectedFiles) {
    Set<String> expectedLocations =
        Sets.newHashSet(Iterables.transform(expectedFiles, DataFile::location));
    Set<String> actualLocations =
        Sets.newHashSet(
            Iterables.transform(
                table.newScan().useRef(ref).planFiles(), task -> task.file().location()));
    assertThat(actualLocations).as("Files should match").isEqualTo(expectedLocations);
  }
}
