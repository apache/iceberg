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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptingFileIO;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

public class TestManifestWriterVersions {
  private final FileIO io = EncryptingFileIO.combine(new InMemoryFileIO(), encryptionManager());

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          required(2, "timestamp", Types.TimestampType.withZone()),
          required(3, "category", Types.StringType.get()),
          required(4, "data", Types.StringType.get()),
          required(5, "double", Types.DoubleType.get()));

  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA)
          .identity("category")
          .hour("timestamp")
          .bucket("id", 16)
          .build();

  private static final long SEQUENCE_NUMBER = 34L;
  private static final long SNAPSHOT_ID = 987134631982734L;
  private static final String PATH =
      "s3://bucket/table/category=cheesy/timestamp_hour=10/id_bucket=3/file.avro";
  private static final FileFormat FORMAT = FileFormat.AVRO;
  private static final PartitionData PARTITION =
      DataFiles.data(SPEC, "category=cheesy/timestamp_hour=10/id_bucket=3");
  private static final Metrics METRICS =
      new Metrics(
          1587L,
          ImmutableMap.of(1, 15L, 2, 122L, 3, 4021L, 4, 9411L, 5, 15L), // sizes
          ImmutableMap.of(1, 100L, 2, 100L, 3, 100L, 4, 100L, 5, 100L), // value counts
          ImmutableMap.of(1, 0L, 2, 0L, 3, 0L, 4, 0L, 5, 0L), // null value counts
          ImmutableMap.of(5, 10L), // nan value counts
          ImmutableMap.of(1, Conversions.toByteBuffer(Types.IntegerType.get(), 1)), // lower bounds
          ImmutableMap.of(1, Conversions.toByteBuffer(Types.IntegerType.get(), 1))); // upper bounds
  private static final List<Long> OFFSETS = ImmutableList.of(4L);
  private static final Integer SORT_ORDER_ID = 2;
  private static final long FIRST_ROW_ID = 100L;

  private static final DataFile DATA_FILE =
      new GenericDataFile(
          0, PATH, FORMAT, PARTITION, 150972L, METRICS, null, OFFSETS, SORT_ORDER_ID, FIRST_ROW_ID);

  private static final List<Integer> EQUALITY_IDS = ImmutableList.of(1);
  private static final int[] EQUALITY_ID_ARR = new int[] {1};

  private static final DeleteFile DELETE_FILE =
      new GenericDeleteFile(
          0,
          FileContent.EQUALITY_DELETES,
          PATH,
          FORMAT,
          PARTITION,
          22905L,
          METRICS,
          EQUALITY_ID_ARR,
          SORT_ORDER_ID,
          null,
          null,
          null,
          null,
          null);

  @TempDir private Path temp;

  @Test
  public void testV1Write() throws IOException {
    ManifestFile manifest = writeManifest(1);
    checkManifest(manifest, ManifestWriter.UNASSIGNED_SEQ);
    checkEntry(
        readManifest(manifest),
        ManifestWriter.UNASSIGNED_SEQ,
        ManifestWriter.UNASSIGNED_SEQ,
        FileContent.DATA);
  }

  @Test
  public void testV1WriteDelete() {
    assertThatThrownBy(() -> writeDeleteManifest(1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot write delete files in a v1 table");
  }

  @Test
  public void testV1WriteWithInheritance() throws IOException {
    ManifestFile manifest = writeAndReadManifestList(writeManifest(1), 1);
    checkManifest(manifest, 0L);

    // v1 should be read using sequence number 0 because it was missing from the manifest list file
    checkEntry(readManifest(manifest), 0L, 0L, FileContent.DATA);
  }

  @Test
  public void testV2Write() throws IOException {
    ManifestFile manifest = writeManifest(2);
    checkManifest(manifest, ManifestWriter.UNASSIGNED_SEQ);
    assertThat(manifest.content()).isEqualTo(ManifestContent.DATA);
    checkEntry(
        readManifest(manifest),
        ManifestWriter.UNASSIGNED_SEQ,
        ManifestWriter.UNASSIGNED_SEQ,
        FileContent.DATA);
  }

  @Test
  public void testV2WriteWithInheritance() throws IOException {
    ManifestFile manifest = writeAndReadManifestList(writeManifest(2), 2);
    checkManifest(manifest, SEQUENCE_NUMBER);
    assertThat(manifest.content()).isEqualTo(ManifestContent.DATA);

    // v2 should use the correct sequence number by inheriting it
    checkEntry(readManifest(manifest), SEQUENCE_NUMBER, SEQUENCE_NUMBER, FileContent.DATA);
  }

  @ParameterizedTest
  @FieldSource("org.apache.iceberg.TestHelpers#ALL_VERSIONS")
  public void testV2PlusWriteDelete(int formatVersion) throws IOException {
    assumeThat(formatVersion).isNotEqualTo(1);

    ManifestFile manifest = writeDeleteManifest(formatVersion);
    checkManifest(manifest, ManifestWriter.UNASSIGNED_SEQ);
    assertThat(manifest.content()).isEqualTo(ManifestContent.DELETES);
    checkEntry(
        readDeleteManifest(manifest),
        ManifestWriter.UNASSIGNED_SEQ,
        ManifestWriter.UNASSIGNED_SEQ,
        FileContent.EQUALITY_DELETES);
  }

  @ParameterizedTest
  @FieldSource("org.apache.iceberg.TestHelpers#ALL_VERSIONS")
  public void testV2WriteDeleteWithInheritance(int formatVersion) throws IOException {
    assumeThat(formatVersion).isNotEqualTo(1);

    ManifestFile manifest =
        writeAndReadManifestList(writeDeleteManifest(formatVersion), formatVersion);
    checkManifest(manifest, SEQUENCE_NUMBER);
    assertThat(manifest.content()).isEqualTo(ManifestContent.DELETES);

    // v2 should use the correct sequence number by inheriting it
    checkEntry(
        readDeleteManifest(manifest),
        SEQUENCE_NUMBER,
        SEQUENCE_NUMBER,
        FileContent.EQUALITY_DELETES);
  }

  @Test
  public void testV2ManifestListRewriteWithInheritance() throws IOException {
    // write with v1
    ManifestFile manifest = writeAndReadManifestList(writeManifest(1), 1);
    checkManifest(manifest, 0L);

    // rewrite existing metadata with v2 manifest list
    ManifestFile manifest2 = writeAndReadManifestList(manifest, 2);
    // the ManifestFile did not change and should still have its original sequence number, 0
    checkManifest(manifest2, 0L);

    // should not inherit the v2 sequence number because it was a rewrite
    checkEntry(readManifest(manifest2), 0L, 0L, FileContent.DATA);
  }

  @Test
  public void testV2ManifestRewriteWithInheritance() throws IOException {
    // write with v1
    ManifestFile manifest = writeAndReadManifestList(writeManifest(1), 1);
    checkManifest(manifest, 0L);

    // rewrite the manifest file using a v2 manifest
    ManifestFile rewritten = rewriteManifest(manifest, 2);
    checkRewrittenManifest(rewritten, ManifestWriter.UNASSIGNED_SEQ, 0L);

    // add the v2 manifest to a v2 manifest list, with a sequence number
    ManifestFile manifest2 = writeAndReadManifestList(rewritten, 2);
    // the ManifestFile is new so it has a sequence number, but the min sequence number 0 is from
    // the entry
    checkRewrittenManifest(manifest2, SEQUENCE_NUMBER, 0L);

    // should not inherit the v2 sequence number because it was written into the v2 manifest
    checkRewrittenEntry(readManifest(manifest2), 0L, FileContent.DATA);
  }

  @Test
  public void testV3Write() throws IOException {
    ManifestFile manifest = writeManifest(3);
    checkEntry(
        readManifest(manifest),
        ManifestWriter.UNASSIGNED_SEQ,
        ManifestWriter.UNASSIGNED_SEQ,
        FileContent.DATA,
        FIRST_ROW_ID);
  }

  @Test
  public void testV3WriteWithInheritance() throws IOException {
    DataFile withoutFirstRowId =
        DataFiles.builder(SPEC).copy(DATA_FILE).withFirstRowId(null).build();

    ManifestFile manifest = writeAndReadManifestList(writeManifest(3, withoutFirstRowId), 3);
    checkManifest(manifest, SEQUENCE_NUMBER);
    assertThat(manifest.content()).isEqualTo(ManifestContent.DATA);

    // v2+ should use the correct sequence number by inheriting it
    // v3 should use the correct first-row-id by inheriting it
    checkEntry(
        readManifest(manifest), SEQUENCE_NUMBER, SEQUENCE_NUMBER, FileContent.DATA, FIRST_ROW_ID);
  }

  @Test
  public void testV3WriteFirstRowIdAssignment() throws IOException {
    long rowsPerFile = METRICS.recordCount();
    DataFile withoutFirstRowId =
        DataFiles.builder(SPEC).copy(DATA_FILE).withFirstRowId(null).build();

    ManifestFile manifest =
        writeAndReadManifestList(writeManifest(3, withoutFirstRowId, withoutFirstRowId), 3);
    assertThat(manifest.content()).isEqualTo(ManifestContent.DATA);

    // v2+ should use the correct sequence number by inheriting it
    // v3 should use the correct first-row-id by inheriting it
    List<ManifestEntry<DataFile>> entries = readManifestAsList(manifest);
    long expectedFirstRowId = FIRST_ROW_ID;
    for (ManifestEntry<DataFile> entry : entries) {
      checkEntry(entry, SEQUENCE_NUMBER, SEQUENCE_NUMBER, FileContent.DATA, expectedFirstRowId);
      expectedFirstRowId += rowsPerFile;
    }
  }

  @Test
  public void testV3ManifestListRewriteWithInheritance() throws IOException {
    // write with v1
    ManifestFile manifest = writeAndReadManifestList(writeManifest(1), 1);
    checkManifest(manifest, 0L);

    // rewrite existing metadata with a manifest list
    ManifestFile manifest3 = writeAndReadManifestList(manifest, 3);
    // the ManifestFile did not change and should still have its original sequence number, 0
    checkManifest(manifest3, 0L);

    // should not inherit the sequence number because it was a rewrite
    checkEntry(readManifest(manifest3), 0L, 0L, FileContent.DATA, FIRST_ROW_ID);
  }

  @Test
  public void testV3ManifestRewriteWithInheritance() throws IOException {
    // write with v1
    ManifestFile manifest = writeAndReadManifestList(writeManifest(1), 1);
    checkManifest(manifest, 0L);

    // rewrite the manifest file using a v3 manifest
    ManifestFile rewritten = rewriteManifest(manifest, 3);
    checkRewrittenManifest(rewritten, ManifestWriter.UNASSIGNED_SEQ, 0L);

    // add the v3 manifest to a v3 manifest list, with a sequence number
    ManifestFile manifest3 = writeAndReadManifestList(rewritten, 3);
    // the ManifestFile is new so it has a sequence number, but the min sequence number 0 is from
    // the entry
    checkRewrittenManifest(manifest3, SEQUENCE_NUMBER, 0L);

    // should not inherit the v3 sequence number because it was written into the v3 manifest
    checkRewrittenEntry(readManifest(manifest3), 0L, FileContent.DATA, FIRST_ROW_ID);
  }

  void checkEntry(
      ManifestEntry<?> entry,
      Long expectedDataSequenceNumber,
      Long expectedFileSequenceNumber,
      FileContent content) {
    checkEntry(entry, expectedDataSequenceNumber, expectedFileSequenceNumber, content, null);
  }

  void checkEntry(
      ManifestEntry<?> entry,
      Long expectedDataSequenceNumber,
      Long expectedFileSequenceNumber,
      FileContent content,
      Long expectedRowId) {
    assertThat(entry.status()).isEqualTo(ManifestEntry.Status.ADDED);
    assertThat(entry.snapshotId()).isEqualTo(SNAPSHOT_ID);
    assertThat(entry.dataSequenceNumber()).isEqualTo(expectedDataSequenceNumber);
    assertThat(entry.fileSequenceNumber()).isEqualTo(expectedFileSequenceNumber);
    checkDataFile(entry.file(), content, expectedRowId);
  }

  void checkRewrittenEntry(
      ManifestEntry<DataFile> entry, Long expectedSequenceNumber, FileContent content) {
    checkRewrittenEntry(entry, expectedSequenceNumber, content, null);
  }

  void checkRewrittenEntry(
      ManifestEntry<DataFile> entry,
      Long expectedSequenceNumber,
      FileContent content,
      Long expectedRowId) {
    assertThat(entry.status()).isEqualTo(ManifestEntry.Status.EXISTING);
    assertThat(entry.snapshotId()).isEqualTo(SNAPSHOT_ID);
    assertThat(entry.dataSequenceNumber()).isEqualTo(expectedSequenceNumber);
    checkDataFile(entry.file(), content, expectedRowId);
  }

  void checkDataFile(ContentFile<?> dataFile, FileContent content, Long expectedRowId) {
    // DataFile is the superclass of DeleteFile, so this method can check both
    assertThat(dataFile.content()).isEqualTo(content);
    assertThat(dataFile.location()).isEqualTo(PATH);
    assertThat(dataFile.format()).isEqualTo(FORMAT);
    assertThat(dataFile.partition()).isEqualTo(PARTITION);
    assertThat(dataFile.recordCount()).isEqualTo(METRICS.recordCount());
    assertThat(dataFile.columnSizes()).isEqualTo(METRICS.columnSizes());
    assertThat(dataFile.valueCounts()).isEqualTo(METRICS.valueCounts());
    assertThat(dataFile.nullValueCounts()).isEqualTo(METRICS.nullValueCounts());
    assertThat(dataFile.nanValueCounts()).isEqualTo(METRICS.nanValueCounts());
    assertThat(dataFile.lowerBounds()).isEqualTo(METRICS.lowerBounds());
    assertThat(dataFile.upperBounds()).isEqualTo(METRICS.upperBounds());
    assertThat(dataFile.sortOrderId()).isEqualTo(SORT_ORDER_ID);
    switch (dataFile.content()) {
      case DATA:
        assertThat(dataFile.firstRowId()).isEqualTo(expectedRowId);
        assertThat(dataFile.equalityFieldIds()).isNull();
        break;
      case EQUALITY_DELETES:
        assertThat(dataFile.firstRowId()).isNull();
        assertThat(dataFile.equalityFieldIds()).isEqualTo(EQUALITY_IDS);
        break;
      case POSITION_DELETES:
        assertThat(dataFile.firstRowId()).isNull();
        assertThat(dataFile.equalityFieldIds()).isNull();
        break;
    }
  }

  void checkManifest(ManifestFile manifest, long expectedSequenceNumber) {
    assertThat(manifest.snapshotId()).isEqualTo(SNAPSHOT_ID);
    assertThat(manifest.sequenceNumber()).isEqualTo(expectedSequenceNumber);
    assertThat(manifest.minSequenceNumber()).isEqualTo(expectedSequenceNumber);
    assertThat(manifest.addedFilesCount()).isEqualTo(1);
    assertThat(manifest.existingFilesCount()).isEqualTo(0);
    assertThat(manifest.deletedFilesCount()).isEqualTo(0);
    assertThat(manifest.addedRowsCount()).isEqualTo(METRICS.recordCount());
    assertThat(manifest.existingRowsCount()).isEqualTo(0);
    assertThat(manifest.deletedRowsCount()).isEqualTo(0);
  }

  void checkRewrittenManifest(
      ManifestFile manifest, long expectedSequenceNumber, long expectedMinSequenceNumber) {
    assertThat(manifest.snapshotId()).isEqualTo(SNAPSHOT_ID);
    assertThat(manifest.sequenceNumber()).isEqualTo(expectedSequenceNumber);
    assertThat(manifest.minSequenceNumber()).isEqualTo(expectedMinSequenceNumber);
    assertThat(manifest.addedFilesCount()).isEqualTo(0);
    assertThat(manifest.existingFilesCount()).isEqualTo(1);
    assertThat(manifest.deletedFilesCount()).isEqualTo(0);
    assertThat(manifest.addedRowsCount()).isEqualTo(0);
    assertThat(manifest.existingRowsCount()).isEqualTo(METRICS.recordCount());
    assertThat(manifest.deletedRowsCount()).isEqualTo(0);
  }

  protected EncryptionManager encryptionManager() {
    return PlaintextEncryptionManager.instance();
  }

  private InputFile writeManifestList(ManifestFile manifest, int formatVersion) throws IOException {
    OutputFile manifestList = io.newOutputFile("manifest-list");
    try (FileAppender<ManifestFile> writer =
        ManifestLists.write(
            formatVersion,
            manifestList,
            PlaintextEncryptionManager.instance(),
            SNAPSHOT_ID,
            SNAPSHOT_ID - 1,
            formatVersion > 1 ? SEQUENCE_NUMBER : 0,
            FIRST_ROW_ID)) {
      writer.add(manifest);
    }

    return manifestList.toInputFile();
  }

  private ManifestFile writeAndReadManifestList(ManifestFile manifest, int formatVersion)
      throws IOException {
    List<ManifestFile> manifests = ManifestLists.read(writeManifestList(manifest, formatVersion));
    assertThat(manifests).hasSize(1);
    return manifests.get(0);
  }

  private ManifestFile rewriteManifest(ManifestFile manifest, int formatVersion)
      throws IOException {
    String filename = FileFormat.AVRO.addExtension("rewrite-manifest");
    EncryptedOutputFile manifestFile = encryptionManager().encrypt(io.newOutputFile(filename));
    ManifestWriter<DataFile> writer =
        ManifestFiles.write(formatVersion, SPEC, manifestFile, SNAPSHOT_ID);
    try {
      writer.existing(readManifest(manifest));
    } finally {
      writer.close();
    }
    return writer.toManifestFile();
  }

  private ManifestFile writeManifest(int formatVersion) throws IOException {
    return writeManifest(formatVersion, DATA_FILE);
  }

  private ManifestFile writeManifest(int formatVersion, DataFile... files) throws IOException {
    String filename = FileFormat.AVRO.addExtension("manifest");
    EncryptedOutputFile manifestFile = encryptionManager().encrypt(io.newOutputFile(filename));
    ManifestWriter<DataFile> writer =
        ManifestFiles.newWriter(formatVersion, SPEC, manifestFile, SNAPSHOT_ID, FIRST_ROW_ID);
    try {
      for (DataFile file : files) {
        writer.add(file);
      }
    } finally {
      writer.close();
    }
    return writer.toManifestFile();
  }

  private List<ManifestEntry<DataFile>> readManifestAsList(ManifestFile manifest)
      throws IOException {
    try (CloseableIterable<ManifestEntry<DataFile>> reader =
        ManifestFiles.read(manifest, io).entries()) {
      return Lists.newArrayList(Iterables.transform(reader, ManifestEntry::copy));
    }
  }

  private ManifestEntry<DataFile> readManifest(ManifestFile manifest) throws IOException {
    List<ManifestEntry<DataFile>> files = readManifestAsList(manifest);
    assertThat(files).hasSize(1);
    return files.get(0);
  }

  private ManifestFile writeDeleteManifest(int formatVersion) throws IOException {
    String filename = FileFormat.AVRO.addExtension("manifest");
    EncryptedOutputFile manifestFile = encryptionManager().encrypt(io.newOutputFile(filename));
    ManifestWriter<DeleteFile> writer =
        ManifestFiles.writeDeleteManifest(formatVersion, SPEC, manifestFile, SNAPSHOT_ID);
    try {
      writer.add(DELETE_FILE);
    } finally {
      writer.close();
    }
    return writer.toManifestFile();
  }

  private ManifestEntry<DeleteFile> readDeleteManifest(ManifestFile manifest) throws IOException {
    try (CloseableIterable<ManifestEntry<DeleteFile>> reader =
        ManifestFiles.readDeleteManifest(manifest, io, null).entries()) {
      List<ManifestEntry<DeleteFile>> entries = Lists.newArrayList(reader);
      assertThat(entries).hasSize(1);
      return entries.get(0);
    }
  }
}
