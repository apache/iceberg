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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import org.apache.avro.InvalidAvroMagicException;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptingFileIO;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.EncryptionTestHelpers;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestManifestEncryption {
  private static final FileIO FILE_IO = new TestTables.LocalFileIO();

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

  private static final ByteBuffer CONTENT_KEY_METADATA = ByteBuffer.allocate(100);

  private static final DataFile DATA_FILE =
      new GenericDataFile(
          0,
          PATH,
          FORMAT,
          PARTITION,
          150972L,
          METRICS,
          CONTENT_KEY_METADATA,
          OFFSETS,
          SORT_ORDER_ID);

  private static final List<Integer> EQUALITY_IDS = ImmutableList.of(1);
  private static final int[] EQUALITY_ID_ARR = new int[] {1};

  private static final DeleteFile DELETE_FILE =
      new GenericDeleteFile(
          SPEC.specId(),
          FileContent.EQUALITY_DELETES,
          PATH,
          FORMAT,
          PARTITION,
          22905L,
          METRICS,
          EQUALITY_ID_ARR,
          SORT_ORDER_ID,
          null,
          CONTENT_KEY_METADATA);

  private static final EncryptionManager ENCRYPTION_MANAGER =
      EncryptionTestHelpers.createEncryptionManager();

  @TempDir private Path temp;

  @Test
  public void testV1Write() throws IOException {
    ManifestFile manifest = writeManifest(1);
    checkEntry(
        readManifest(manifest),
        ManifestWriter.UNASSIGNED_SEQ,
        ManifestWriter.UNASSIGNED_SEQ,
        FileContent.DATA);
  }

  @Test
  public void testV2Write() throws IOException {
    ManifestFile manifest = writeManifest(2);
    checkEntry(
        readManifest(manifest),
        ManifestWriter.UNASSIGNED_SEQ,
        ManifestWriter.UNASSIGNED_SEQ,
        FileContent.DATA);
  }

  @Test
  public void testV2WriteDelete() throws IOException {
    ManifestFile manifest = writeDeleteManifest(2);
    checkEntry(
        readDeleteManifest(manifest),
        ManifestWriter.UNASSIGNED_SEQ,
        ManifestWriter.UNASSIGNED_SEQ,
        FileContent.EQUALITY_DELETES);
  }

  void checkEntry(
      ManifestEntry<?> entry,
      Long expectedDataSequenceNumber,
      Long expectedFileSequenceNumber,
      FileContent content) {
    assertThat(entry.status()).isEqualTo(ManifestEntry.Status.ADDED);
    assertThat(entry.snapshotId()).isEqualTo(SNAPSHOT_ID);
    assertThat(entry.dataSequenceNumber()).isEqualTo(expectedDataSequenceNumber);
    assertThat(entry.fileSequenceNumber()).isEqualTo(expectedFileSequenceNumber);
    checkDataFile(entry.file(), content);
  }

  void checkDataFile(ContentFile<?> dataFile, FileContent content) {
    // DataFile is the superclass of DeleteFile, so this method can check both
    assertThat(dataFile.content()).isEqualTo(content);
    assertThat(dataFile.path()).isEqualTo(PATH);
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
    if (dataFile.content() == FileContent.EQUALITY_DELETES) {
      assertThat(dataFile.equalityFieldIds()).isEqualTo(EQUALITY_IDS);
    } else {
      assertThat(dataFile.equalityFieldIds()).isNull();
    }
  }

  private ManifestFile writeManifest(int formatVersion) throws IOException {
    return writeManifest(DATA_FILE, formatVersion);
  }

  private ManifestFile writeManifest(DataFile file, int formatVersion) throws IOException {
    OutputFile manifestFile =
        Files.localOutput(
            FileFormat.AVRO.addExtension(
                File.createTempFile("manifest", null, temp.toFile()).toString()));
    EncryptedOutputFile encryptedManifest = ENCRYPTION_MANAGER.encrypt(manifestFile);
    ManifestWriter<DataFile> writer =
        ManifestFiles.write(formatVersion, SPEC, encryptedManifest, SNAPSHOT_ID);
    try {
      writer.add(file);
    } finally {
      writer.close();
    }
    return writer.toManifestFile();
  }

  private ManifestEntry<DataFile> readManifest(ManifestFile manifest) throws IOException {
    // First try to read without decryption
    assertThatThrownBy(
            () ->
                ManifestFiles.read(
                    manifest,
                    EncryptingFileIO.combine(FILE_IO, PlaintextEncryptionManager.instance()),
                    null))
        .isInstanceOf(RuntimeIOException.class)
        .hasMessageContaining("Failed to open file")
        .hasCauseInstanceOf(InvalidAvroMagicException.class);

    try (CloseableIterable<ManifestEntry<DataFile>> reader =
        ManifestFiles.read(manifest, EncryptingFileIO.combine(FILE_IO, ENCRYPTION_MANAGER), null)
            .entries()) {
      List<ManifestEntry<DataFile>> files = Lists.newArrayList(reader);
      assertThat(files).hasSize(1);
      return files.get(0);
    }
  }

  private ManifestFile writeDeleteManifest(int formatVersion) throws IOException {
    OutputFile manifestFile =
        Files.localOutput(
            FileFormat.AVRO.addExtension(
                File.createTempFile("manifest", null, temp.toFile()).toString()));
    EncryptedOutputFile encryptedManifest = ENCRYPTION_MANAGER.encrypt(manifestFile);
    ManifestWriter<DeleteFile> writer =
        ManifestFiles.writeDeleteManifest(formatVersion, SPEC, encryptedManifest, SNAPSHOT_ID);
    try {
      writer.add(DELETE_FILE);
    } finally {
      writer.close();
    }
    return writer.toManifestFile();
  }

  private ManifestEntry<DeleteFile> readDeleteManifest(ManifestFile manifest) throws IOException {
    try (CloseableIterable<ManifestEntry<DeleteFile>> reader =
        ManifestFiles.readDeleteManifest(
                manifest, EncryptingFileIO.combine(FILE_IO, ENCRYPTION_MANAGER), null)
            .entries()) {
      List<ManifestEntry<DeleteFile>> entries = Lists.newArrayList(reader);
      assertThat(entries).hasSize(1);
      return entries.get(0);
    }
  }
}
