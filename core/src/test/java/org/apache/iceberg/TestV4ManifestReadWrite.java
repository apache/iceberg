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
import java.util.List;
import java.util.Map;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptingFileIO;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestV4ManifestReadWrite {
  private final FileIO io =
      EncryptingFileIO.combine(new InMemoryFileIO(), PlaintextEncryptionManager.instance());

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()), required(2, "data", Types.StringType.get()));

  private static final PartitionSpec SPEC = PartitionSpec.unpartitioned();

  private static final Map<Integer, PartitionSpec> SPECS_BY_ID =
      ImmutableMap.of(SPEC.specId(), SPEC);

  private static final long SNAPSHOT_ID = 987134631982734L;
  private static final long FIRST_ROW_ID = 100L;

  private static final String FILE_PATH = "s3://bucket/table/data/file.parquet";
  private static final FileFormat FILE_FORMAT = FileFormat.PARQUET;

  private static final Metrics METRICS =
      new Metrics(
          100L,
          ImmutableMap.of(1, 800L, 2, 2400L),
          ImmutableMap.of(1, 100L, 2, 100L),
          ImmutableMap.of(1, 0L, 2, 5L),
          null,
          ImmutableMap.of(1, Conversions.toByteBuffer(Types.LongType.get(), 1L)),
          ImmutableMap.of(1, Conversions.toByteBuffer(Types.LongType.get(), 100L)));

  private static final List<Long> OFFSETS = ImmutableList.of(4L);
  private static final Integer SORT_ORDER_ID = 0;

  private static final DataFile DATA_FILE =
      new GenericDataFile(
          0,
          FILE_PATH,
          FILE_FORMAT,
          null,
          150972L,
          METRICS,
          null,
          OFFSETS,
          SORT_ORDER_ID,
          FIRST_ROW_ID);

  @Test
  public void testWriteAndReadV4DataManifest() throws IOException {
    ManifestFile manifest = writeV4Manifest(DATA_FILE);

    // read back via V4ManifestReader
    V4ManifestReader reader = new V4ManifestReader(io.newInputFile(manifest), SPECS_BY_ID);

    List<TrackedFile> entries = Lists.newArrayList();
    try (CloseableIterable<TrackedFile> liveEntries = reader.liveEntries()) {
      for (TrackedFile tf : liveEntries) {
        entries.add(tf.copy());
      }
    }

    assertThat(entries).hasSize(1);

    TrackedFile entry = entries.get(0);
    assertThat(entry.contentType()).isEqualTo(FileContent.DATA);
    assertThat(entry.location()).isEqualTo(FILE_PATH);
    assertThat(entry.fileFormat()).isEqualTo(FILE_FORMAT);
    assertThat(entry.recordCount()).isEqualTo(100L);
    assertThat(entry.fileSizeInBytes()).isEqualTo(150972L);
    assertThat(entry.sortOrderId()).isEqualTo(SORT_ORDER_ID);
    assertThat(entry.splitOffsets()).isEqualTo(OFFSETS);

    Tracking tracking = entry.tracking();
    assertThat(tracking).isNotNull();
    assertThat(tracking.status()).isEqualTo(EntryStatus.ADDED);
    assertThat(tracking.snapshotId()).isEqualTo(SNAPSHOT_ID);
  }

  @Test
  public void testV4ManifestLiveEntries() throws IOException {
    // write a manifest with an ADDED entry, then read as existing + add another
    ManifestFile firstManifest = writeV4Manifest(DATA_FILE);

    // read the first manifest and rewrite with EXISTING + ADDED + DELETED entries
    DataFile secondFile =
        new GenericDataFile(
            0,
            "s3://bucket/table/data/file2.parquet",
            FILE_FORMAT,
            null,
            200000L,
            METRICS,
            null,
            OFFSETS,
            SORT_ORDER_ID,
            FIRST_ROW_ID + 100);

    DataFile deletedFile =
        new GenericDataFile(
            0,
            "s3://bucket/table/data/deleted.parquet",
            FILE_FORMAT,
            null,
            50000L,
            METRICS,
            null,
            OFFSETS,
            SORT_ORDER_ID,
            FIRST_ROW_ID + 200);

    String filename = FileFormat.PARQUET.addExtension("manifest-mixed-" + System.nanoTime());
    EncryptedOutputFile outputFile =
        PlaintextEncryptionManager.instance().encrypt(io.newOutputFile(filename));
    ManifestWriter<DataFile> writer =
        ManifestFiles.newWriter(4, SPEC, outputFile, SNAPSHOT_ID, FIRST_ROW_ID);
    try {
      writer.existing(DATA_FILE, SNAPSHOT_ID, 1L, 1L);
      writer.add(secondFile);
      writer.delete(deletedFile, 1L, 1L);
    } finally {
      writer.close();
    }

    ManifestFile manifest = writer.toManifestFile();

    // read liveEntries -- should only return EXISTING + ADDED, not DELETED
    V4ManifestReader reader = new V4ManifestReader(io.newInputFile(manifest), SPECS_BY_ID);
    List<TrackedFile> liveFiles = Lists.newArrayList();
    try (CloseableIterable<TrackedFile> live = reader.liveEntries()) {
      for (TrackedFile tf : live) {
        liveFiles.add(tf.copy());
      }
    }

    assertThat(liveFiles).hasSize(2);
    assertThat(liveFiles).allSatisfy(tf -> assertThat(tf.tracking().isLive()).isTrue());

    // read all entries -- should include all 3
    reader = new V4ManifestReader(io.newInputFile(manifest), SPECS_BY_ID);
    List<TrackedFile> allFiles = Lists.newArrayList();
    try (CloseableIterable<TrackedFile> all = reader.entries()) {
      for (TrackedFile tf : all) {
        allFiles.add(tf.copy());
      }
    }

    assertThat(allFiles).hasSize(3);
  }

  @Test
  public void testV4ManifestIterator() throws IOException {
    ManifestFile manifest = writeV4Manifest(DATA_FILE);
    V4ManifestReader reader = new V4ManifestReader(io.newInputFile(manifest), SPECS_BY_ID);

    List<TrackedFile> files = Lists.newArrayList(reader);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).location()).isEqualTo(FILE_PATH);
  }

  @Test
  public void testV4ManifestDataFileAdapter() throws IOException {
    ManifestFile manifest = writeV4Manifest(DATA_FILE);
    V4ManifestReader reader = new V4ManifestReader(io.newInputFile(manifest), SPECS_BY_ID);

    List<TrackedFile> entries = Lists.newArrayList();
    try (CloseableIterable<TrackedFile> liveEntries = reader.liveEntries()) {
      for (TrackedFile tf : liveEntries) {
        entries.add(tf.copy());
      }
    }

    assertThat(entries).hasSize(1);

    // convert to DataFile via adapter
    DataFile adapted = TrackedFileAdapters.asDataFile(entries.get(0), SPEC);
    assertThat(adapted.location()).isEqualTo(FILE_PATH);
    assertThat(adapted.format()).isEqualTo(FILE_FORMAT);
    assertThat(adapted.recordCount()).isEqualTo(100L);
    assertThat(adapted.fileSizeInBytes()).isEqualTo(150972L);
    assertThat(adapted.content()).isEqualTo(FileContent.DATA);
    assertThat(adapted.splitOffsets()).isEqualTo(OFFSETS);
  }

  @Test
  public void testRootManifestRoundTrip() throws IOException {
    ManifestFile leafManifest = writeV4Manifest(DATA_FILE);

    // write a root manifest containing this leaf manifest
    String rootFilename = FileFormat.PARQUET.addExtension("root-" + System.nanoTime());
    OutputFile rootOutput = io.newOutputFile(rootFilename);

    long commitSequenceNumber = 1L;
    Schema schema = V4Metadata.entrySchema(Types.StructType.of());
    try (FileAppender<StructLike> writer =
        InternalData.write(FileFormat.PARQUET, rootOutput)
            .schema(schema)
            .named("tracked_file")
            .meta("format-version", "4")
            .meta("content", "root")
            .overwrite()
            .build()) {
      writer.add(
          V4Metadata.manifestFileToTrackedFile(leafManifest, SNAPSHOT_ID, commitSequenceNumber));
    }

    // read back the root manifest and convert to ManifestFile
    V4ManifestReader reader = new V4ManifestReader(io.newInputFile(rootFilename), SPECS_BY_ID);
    List<ManifestFile> manifests = Lists.newArrayList();
    try (CloseableIterable<TrackedFile> entries = reader.liveEntries()) {
      for (TrackedFile tf : entries) {
        if (tf.contentType() == FileContent.DATA_MANIFEST
            || tf.contentType() == FileContent.DELETE_MANIFEST) {
          manifests.add(V4Metadata.trackedFileToManifestFile(tf.copy()));
        }
      }
    }

    assertThat(manifests).hasSize(1);

    ManifestFile result = manifests.get(0);
    assertThat(result.path()).isEqualTo(leafManifest.path());
    assertThat(result.length()).isEqualTo(leafManifest.length());
    assertThat(result.partitionSpecId()).isEqualTo(leafManifest.partitionSpecId());
    assertThat(result.content()).isEqualTo(ManifestContent.DATA);
    assertThat(result.snapshotId()).isEqualTo(SNAPSHOT_ID);
    assertThat(result.addedFilesCount()).isEqualTo(leafManifest.addedFilesCount());
    assertThat(result.existingFilesCount()).isEqualTo(leafManifest.existingFilesCount());
    assertThat(result.deletedFilesCount()).isEqualTo(leafManifest.deletedFilesCount());
    assertThat(result.addedRowsCount()).isEqualTo(leafManifest.addedRowsCount());
    assertThat(result.existingRowsCount()).isEqualTo(leafManifest.existingRowsCount());
    assertThat(result.deletedRowsCount()).isEqualTo(leafManifest.deletedRowsCount());
  }

  private ManifestFile writeV4Manifest(DataFile... files) throws IOException {
    String filename = FileFormat.PARQUET.addExtension("manifest-" + System.nanoTime());
    EncryptedOutputFile outputFile =
        PlaintextEncryptionManager.instance().encrypt(io.newOutputFile(filename));
    ManifestWriter<DataFile> writer =
        ManifestFiles.newWriter(4, SPEC, outputFile, SNAPSHOT_ID, FIRST_ROW_ID);
    try {
      for (DataFile file : files) {
        writer.add(file);
      }
    } finally {
      writer.close();
    }

    return writer.toManifestFile();
  }
}
