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

import java.nio.ByteBuffer;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestV4MetadataConversions {
  private static final Schema SCHEMA =
      new Schema(
          required(3, "id", Types.IntegerType.get()), required(4, "data", Types.StringType.get()));

  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).bucket("data", 16).build();

  @Test
  public void testManifestFileToTrackedFileRoundTrip() {
    long snapshotId = 12345L;
    long sequenceNumber = 10L;

    ManifestFile manifest =
        new GenericManifestFile(
            "/path/to/manifest.parquet",
            5878L,
            0,
            ManifestContent.DATA,
            sequenceNumber,
            5L,
            snapshotId,
            null,
            null,
            3,
            30L,
            5,
            50L,
            1,
            10L,
            null);

    TrackedFileStruct tf =
        V4Metadata.manifestFileToTrackedFile(manifest, snapshotId, sequenceNumber, null);

    assertThat(tf.contentType()).isEqualTo(FileContent.DATA_MANIFEST);
    assertThat(tf.location()).isEqualTo("/path/to/manifest.parquet");
    assertThat(tf.fileSizeInBytes()).isEqualTo(5878L);
    assertThat(tf.specId()).isEqualTo(0);
    assertThat(tf.recordCount()).isEqualTo(9L); // 3 + 5 + 1

    Tracking tracking = tf.tracking();
    assertThat(tracking).isNotNull();
    assertThat(tracking.status()).isEqualTo(EntryStatus.ADDED);
    assertThat(tracking.snapshotId()).isEqualTo(snapshotId);
    assertThat(tracking.dataSequenceNumber()).isEqualTo(sequenceNumber);

    ManifestInfo info = tf.manifestInfo();
    assertThat(info).isNotNull();
    assertThat(info.addedFilesCount()).isEqualTo(3);
    assertThat(info.addedRowsCount()).isEqualTo(30L);
    assertThat(info.existingFilesCount()).isEqualTo(5);
    assertThat(info.existingRowsCount()).isEqualTo(50L);
    assertThat(info.deletedFilesCount()).isEqualTo(1);
    assertThat(info.deletedRowsCount()).isEqualTo(10L);
    assertThat(info.minSequenceNumber()).isEqualTo(5L);

    // round-trip back to ManifestFile
    ManifestFile roundTripped = V4Metadata.trackedFileToManifestFile(tf, null);

    assertThat(roundTripped.path()).isEqualTo(manifest.path());
    assertThat(roundTripped.length()).isEqualTo(manifest.length());
    assertThat(roundTripped.partitionSpecId()).isEqualTo(manifest.partitionSpecId());
    assertThat(roundTripped.content()).isEqualTo(manifest.content());
    assertThat(roundTripped.sequenceNumber()).isEqualTo(manifest.sequenceNumber());
    assertThat(roundTripped.minSequenceNumber()).isEqualTo(manifest.minSequenceNumber());
    assertThat(roundTripped.snapshotId()).isEqualTo(manifest.snapshotId());
    assertThat(roundTripped.addedFilesCount()).isEqualTo(manifest.addedFilesCount());
    assertThat(roundTripped.addedRowsCount()).isEqualTo(manifest.addedRowsCount());
    assertThat(roundTripped.existingFilesCount()).isEqualTo(manifest.existingFilesCount());
    assertThat(roundTripped.existingRowsCount()).isEqualTo(manifest.existingRowsCount());
    assertThat(roundTripped.deletedFilesCount()).isEqualTo(manifest.deletedFilesCount());
    assertThat(roundTripped.deletedRowsCount()).isEqualTo(manifest.deletedRowsCount());
  }

  @Test
  public void testManifestFileToTrackedFileDeleteManifest() {
    long snapshotId = 99L;
    long sequenceNumber = 7L;

    ManifestFile deleteManifest =
        new GenericManifestFile(
            "/path/to/delete-manifest.parquet",
            1024L,
            1,
            ManifestContent.DELETES,
            sequenceNumber,
            3L,
            snapshotId,
            null,
            null,
            2,
            20L,
            0,
            0L,
            0,
            0L,
            null);

    TrackedFileStruct tf =
        V4Metadata.manifestFileToTrackedFile(deleteManifest, snapshotId, sequenceNumber, null);

    assertThat(tf.contentType()).isEqualTo(FileContent.DELETE_MANIFEST);
    assertThat(tf.location()).isEqualTo("/path/to/delete-manifest.parquet");
    assertThat(tf.specId()).isEqualTo(1);
  }

  @Test
  public void testManifestFileToTrackedFileUnassignedSequenceNumber() {
    long snapshotId = 42L;
    long commitSeqNum = 15L;

    ManifestFile manifest =
        new GenericManifestFile(
            "/path/to/manifest.parquet",
            2048L,
            0,
            ManifestContent.DATA,
            ManifestWriter.UNASSIGNED_SEQ,
            ManifestWriter.UNASSIGNED_SEQ,
            snapshotId,
            null,
            null,
            1,
            10L,
            0,
            0L,
            0,
            0L,
            null);

    TrackedFileStruct tf =
        V4Metadata.manifestFileToTrackedFile(manifest, snapshotId, commitSeqNum, null);

    Tracking tracking = tf.tracking();
    assertThat(tracking.dataSequenceNumber()).isEqualTo(commitSeqNum);

    ManifestInfo info = tf.manifestInfo();
    assertThat(info.minSequenceNumber()).isEqualTo(commitSeqNum);
  }

  @Test
  public void testManifestFileToTrackedFileWithKeyMetadata() {
    long snapshotId = 1L;
    long sequenceNumber = 1L;
    ByteBuffer keyMetadata = ByteBuffer.wrap(new byte[] {1, 2, 3, 4});

    ManifestFile manifest =
        new GenericManifestFile(
            "/path/to/manifest.parquet",
            1024L,
            0,
            ManifestContent.DATA,
            sequenceNumber,
            sequenceNumber,
            snapshotId,
            null,
            keyMetadata,
            1,
            10L,
            0,
            0L,
            0,
            0L,
            null);

    TrackedFileStruct tf =
        V4Metadata.manifestFileToTrackedFile(manifest, snapshotId, sequenceNumber, null);

    assertThat(tf.keyMetadata()).isNotNull();
  }

  @Test
  public void testEntryToTrackedFileDataFile() {
    DataFile dataFile =
        DataFiles.builder(SPEC)
            .withPath("/path/to/data.parquet")
            .withFileSizeInBytes(350)
            .withPartitionPath("data_bucket=0")
            .withRecordCount(10)
            .withSplitOffsets(ImmutableList.of(4L))
            .build();

    Schema entrySchema = V4Metadata.entrySchema(SPEC.partitionType());
    GenericManifestEntry<DataFile> entry =
        new GenericManifestEntry<>(AvroSchemaUtil.convert(entrySchema, "manifest_entry"));
    entry.wrapAppend(100L, 5L, dataFile);

    TrackedFileStruct tf = V4Metadata.entryToTrackedFile(entry, 100L, null);

    assertThat(tf.contentType()).isEqualTo(FileContent.DATA);
    assertThat(tf.location()).isEqualTo("/path/to/data.parquet");
    assertThat(tf.fileSizeInBytes()).isEqualTo(350L);
    assertThat(tf.recordCount()).isEqualTo(10L);
    assertThat(tf.specId()).isEqualTo(SPEC.specId());
    assertThat(tf.splitOffsets()).isEqualTo(ImmutableList.of(4L));

    Tracking tracking = tf.tracking();
    assertThat(tracking).isNotNull();
    assertThat(tracking.status()).isEqualTo(EntryStatus.ADDED);
    assertThat(tracking.snapshotId()).isEqualTo(100L);
    assertThat(tracking.dataSequenceNumber()).isEqualTo(5L);
  }

  @Test
  public void testEntryToTrackedFileAddedWithNullSequenceNumber() {
    DataFile dataFile =
        DataFiles.builder(SPEC)
            .withPath("/path/to/data.parquet")
            .withFileSizeInBytes(100)
            .withPartitionPath("data_bucket=0")
            .withRecordCount(5)
            .build();

    Schema entrySchema = V4Metadata.entrySchema(SPEC.partitionType());
    GenericManifestEntry<DataFile> entry =
        new GenericManifestEntry<>(AvroSchemaUtil.convert(entrySchema, "manifest_entry"));
    entry.wrapAppend(200L, dataFile);

    TrackedFileStruct tf = V4Metadata.entryToTrackedFile(entry, 200L, null);

    Tracking tracking = tf.tracking();
    assertThat(tracking.status()).isEqualTo(EntryStatus.ADDED);
    assertThat(tracking.snapshotId()).isEqualTo(200L);
    assertThat(tracking.dataSequenceNumber()).isNull();
  }

  @Test
  public void testEntryToTrackedFileEqualityDeleteFile() {
    DeleteFile deleteFile =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofEqualityDeletes(3)
            .withPath("/path/to/eq-deletes.parquet")
            .withFileSizeInBytes(200)
            .withPartitionPath("data_bucket=0")
            .withRecordCount(3)
            .build();

    Schema entrySchema = V4Metadata.entrySchema(SPEC.partitionType());
    GenericManifestEntry<DeleteFile> entry =
        new GenericManifestEntry<>(AvroSchemaUtil.convert(entrySchema, "manifest_entry"));
    entry.wrapAppend(300L, 8L, deleteFile);

    TrackedFileStruct tf = V4Metadata.entryToTrackedFile(entry, 300L, null);

    assertThat(tf.contentType()).isEqualTo(FileContent.EQUALITY_DELETES);
    assertThat(tf.location()).isEqualTo("/path/to/eq-deletes.parquet");
    assertThat(tf.equalityIds()).isEqualTo(ImmutableList.of(3));
  }

  @Test
  public void testEntryToTrackedFileExistingEntry() {
    DataFile dataFile =
        DataFiles.builder(SPEC)
            .withPath("/path/to/data.parquet")
            .withFileSizeInBytes(100)
            .withPartitionPath("data_bucket=0")
            .withRecordCount(5)
            .build();

    Schema entrySchema = V4Metadata.entrySchema(SPEC.partitionType());
    GenericManifestEntry<DataFile> entry =
        new GenericManifestEntry<>(AvroSchemaUtil.convert(entrySchema, "manifest_entry"));
    entry.wrapExisting(400L, 12L, 12L, dataFile);

    TrackedFileStruct tf = V4Metadata.entryToTrackedFile(entry, 400L, null);

    Tracking tracking = tf.tracking();
    assertThat(tracking.status()).isEqualTo(EntryStatus.EXISTING);
    assertThat(tracking.snapshotId()).isEqualTo(400L);
    assertThat(tracking.dataSequenceNumber()).isEqualTo(12L);
    assertThat(tracking.fileSequenceNumber()).isEqualTo(12L);
  }

  @Test
  public void testEntrySchemaHasTrackedFileFields() {
    Schema schema = V4Metadata.entrySchema(Types.StructType.of());

    assertThat(schema.findField(TrackedFile.TRACKING.fieldId())).isNotNull();
    assertThat(schema.findField(TrackedFile.CONTENT_TYPE.fieldId())).isNotNull();
    assertThat(schema.findField(TrackedFile.LOCATION.fieldId())).isNotNull();
    assertThat(schema.findField(TrackedFile.FILE_FORMAT.fieldId())).isNotNull();
    assertThat(schema.findField(TrackedFile.RECORD_COUNT.fieldId())).isNotNull();
    assertThat(schema.findField(TrackedFile.FILE_SIZE_IN_BYTES.fieldId())).isNotNull();
    assertThat(schema.findField(TrackedFile.SPEC_ID.fieldId())).isNotNull();
    assertThat(schema.findField(TrackedFile.SORT_ORDER_ID.fieldId())).isNotNull();
    assertThat(schema.findField(TrackedFile.DELETION_VECTOR.fieldId())).isNotNull();
    assertThat(schema.findField(TrackedFile.MANIFEST_INFO.fieldId())).isNotNull();
    assertThat(schema.findField(TrackedFile.KEY_METADATA.fieldId())).isNotNull();
    assertThat(schema.findField(TrackedFile.SPLIT_OFFSETS.fieldId())).isNotNull();
    assertThat(schema.findField(TrackedFile.EQUALITY_IDS.fieldId())).isNotNull();
  }
}
