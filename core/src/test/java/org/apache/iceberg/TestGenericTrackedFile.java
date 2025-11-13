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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestGenericTrackedFile {

  @TempDir private Path temp;

  private static Schema getTrackedFileSchema() {
    return new Schema(GenericTrackedFile.BASE_TYPE.fields());
  }

  @Test
  public void testAvroRoundTripDataFile() throws IOException {
    // Create a tracked file representing a data file
    GenericTrackedFile original = new GenericTrackedFile();
    original.setContentType(FileContent.DATA);
    original.setLocation("s3://bucket/table/data/file1.parquet");
    original.setFileFormat(FileFormat.PARQUET);
    original.setPartitionSpecId(0);
    original.setRecordCount(1000L);
    original.setFileSizeInBytes(50000L);
    original.setSortOrderId(1);
    original.setSplitOffsets(ImmutableList.of(0L, 10000L, 20000L));

    // Set tracking info
    original.setStatus(TrackingInfo.Status.ADDED);
    original.setSnapshotId(12345L);
    original.setSequenceNumber(100L);
    original.setFileSequenceNumber(100L);
    original.setFirstRowId(0L);

    // Set key metadata
    original.setKeyMetadata(ByteBuffer.wrap(new byte[] {1, 2, 3, 4}));

    // Write to file
    OutputFile outputFile = Files.localOutput(temp.resolve("tracked-file.parquet").toFile());
    List<GenericTrackedFile> written;
    try (FileAppender<GenericTrackedFile> appender =
        InternalData.write(FileFormat.PARQUET, outputFile)
            .schema(getTrackedFileSchema())
            .named("tracked_file")
            .build()) {
      appender.add(original);
      written = ImmutableList.of(original);
    }

    // Read back
    InputFile inputFile = outputFile.toInputFile();
    List<GenericTrackedFile> read;
    try (CloseableIterable<GenericTrackedFile> files =
        InternalData.read(FileFormat.PARQUET, inputFile)
            .setRootType(GenericTrackedFile.class)
            .project(getTrackedFileSchema())
            .build()) {
      read = Lists.newArrayList(files);
    }

    // Verify
    assertThat(read).hasSize(1);
    GenericTrackedFile roundTripped = read.get(0);

    assertThat(roundTripped.contentType()).isEqualTo(FileContent.DATA);
    assertThat(roundTripped.location()).isEqualTo("s3://bucket/table/data/file1.parquet");
    assertThat(roundTripped.fileFormat()).isEqualTo(FileFormat.PARQUET);
    assertThat(roundTripped.partitionSpecId()).isEqualTo(0);
    assertThat(roundTripped.recordCount()).isEqualTo(1000L);
    assertThat(roundTripped.fileSizeInBytes()).isEqualTo(50000L);
    assertThat(roundTripped.sortOrderId()).isEqualTo(1);
    assertThat(roundTripped.splitOffsets()).containsExactly(0L, 10000L, 20000L);

    // Verify tracking info
    TrackingInfo trackingInfo = roundTripped.trackingInfo();
    assertThat(trackingInfo).isNotNull();
    assertThat(trackingInfo.status()).isEqualTo(TrackingInfo.Status.ADDED);
    assertThat(trackingInfo.snapshotId()).isEqualTo(12345L);
    assertThat(trackingInfo.sequenceNumber()).isEqualTo(100L);
    assertThat(trackingInfo.fileSequenceNumber()).isEqualTo(100L);
    assertThat(trackingInfo.firstRowId()).isEqualTo(0L);

    // Verify key metadata
    assertThat(roundTripped.keyMetadata()).isEqualTo(ByteBuffer.wrap(new byte[] {1, 2, 3, 4}));
  }

  @Test
  public void testAvroRoundTripPositionDeletesWithDeletionVector() throws IOException {
    // Create a tracked file representing position deletes with external deletion vector
    GenericTrackedFile original = new GenericTrackedFile();
    original.setContentType(FileContent.POSITION_DELETES);
    original.setLocation("s3://bucket/table/deletes/dv1.puffin");
    original.setFileFormat(FileFormat.PUFFIN);
    original.setPartitionSpecId(0);
    original.setRecordCount(50L); // 50 deleted positions
    original.setFileSizeInBytes(1000L);
    original.setReferencedFile("s3://bucket/table/data/file1.parquet");

    // Set deletion vector (external)
    original.setDeletionVectorOffset(100L);
    original.setDeletionVectorSizeInBytes(500L);

    // Set tracking info
    original.setStatus(TrackingInfo.Status.ADDED);
    original.setSnapshotId(12346L);
    original.setSequenceNumber(101L);
    original.setFileSequenceNumber(101L);

    // Write to file
    OutputFile outputFile = Files.localOutput(temp.resolve("dv-tracked-file.avro").toFile());
    try (FileAppender<GenericTrackedFile> appender =
        InternalData.write(FileFormat.AVRO, outputFile)
            .schema(getTrackedFileSchema())
            .named("tracked_file")
            .build()) {
      appender.add(original);
    }

    // Read back
    InputFile inputFile = outputFile.toInputFile();
    List<GenericTrackedFile> read;
    try (CloseableIterable<GenericTrackedFile> files =
        InternalData.read(FileFormat.AVRO, inputFile)
            .setRootType(GenericTrackedFile.class)
            .project(getTrackedFileSchema())
            .build()) {
      read = Lists.newArrayList(files);
    }

    // Verify
    assertThat(read).hasSize(1);
    GenericTrackedFile roundTripped = read.get(0);

    assertThat(roundTripped.contentType()).isEqualTo(FileContent.POSITION_DELETES);
    assertThat(roundTripped.location()).isEqualTo("s3://bucket/table/deletes/dv1.puffin");
    assertThat(roundTripped.fileFormat()).isEqualTo(FileFormat.PUFFIN);
    assertThat(roundTripped.recordCount()).isEqualTo(50L);
    assertThat(roundTripped.referencedFile()).isEqualTo("s3://bucket/table/data/file1.parquet");

    // Verify deletion vector
    DeletionVector dv = roundTripped.deletionVector();
    assertThat(dv).isNotNull();
    assertThat(dv.offset()).isEqualTo(100L);
    assertThat(dv.sizeInBytes()).isEqualTo(500L);
    assertThat(dv.inlineContent()).isNull();
  }

  @Test
  public void testAvroRoundTripManifestDVWithInlineContent() throws IOException {
    // Create a tracked file representing a manifest deletion vector with inline content
    GenericTrackedFile original = new GenericTrackedFile();
    original.setContentType(FileContent.MANIFEST_DV);
    original.setFileFormat(FileFormat.PUFFIN); // Even inline DVs need a format
    original.setPartitionSpecId(0);
    original.setRecordCount(3L); // 3 deleted manifest entries
    original.setReferencedFile("s3://bucket/table/metadata/manifest1.avro");

    // Set inline deletion vector
    byte[] inlineBitmap = new byte[] {(byte) 0b00000111}; // positions 0, 1, 2 deleted
    original.setDeletionVectorInlineContent(ByteBuffer.wrap(inlineBitmap));

    // Set tracking info
    original.setStatus(TrackingInfo.Status.ADDED);
    original.setSnapshotId(12347L);

    // Write to file
    OutputFile outputFile =
        Files.localOutput(temp.resolve("manifest-dv-tracked-file.avro").toFile());
    try (FileAppender<GenericTrackedFile> appender =
        InternalData.write(FileFormat.AVRO, outputFile)
            .schema(getTrackedFileSchema())
            .named("tracked_file")
            .build()) {
      appender.add(original);
    }

    // Read back
    InputFile inputFile = outputFile.toInputFile();
    List<GenericTrackedFile> read;
    try (CloseableIterable<GenericTrackedFile> files =
        InternalData.read(FileFormat.AVRO, inputFile)
            .setRootType(GenericTrackedFile.class)
            .project(getTrackedFileSchema())
            .build()) {
      read = Lists.newArrayList(files);
    }

    // Verify
    assertThat(read).hasSize(1);
    GenericTrackedFile roundTripped = read.get(0);

    assertThat(roundTripped.contentType()).isEqualTo(FileContent.MANIFEST_DV);
    assertThat(roundTripped.location()).isNull(); // Can be null for inline DVs
    assertThat(roundTripped.recordCount()).isEqualTo(3L);
    assertThat(roundTripped.referencedFile())
        .isEqualTo("s3://bucket/table/metadata/manifest1.avro");

    // Verify inline deletion vector
    DeletionVector dv = roundTripped.deletionVector();
    assertThat(dv).isNotNull();
    assertThat(dv.offset()).isNull();
    assertThat(dv.sizeInBytes()).isNull();
    assertThat(dv.inlineContent()).isEqualTo(ByteBuffer.wrap(inlineBitmap));
  }

  @Test
  public void testAvroRoundTripDataManifestWithStats() throws IOException {
    // Create a tracked file representing a data manifest entry
    GenericTrackedFile original = new GenericTrackedFile();
    original.setContentType(FileContent.DATA_MANIFEST);
    original.setLocation("s3://bucket/table/metadata/manifest-data-1.avro");
    original.setFileFormat(FileFormat.AVRO);
    original.setPartitionSpecId(0);
    original.setRecordCount(100L); // 100 entries in the manifest
    original.setFileSizeInBytes(25000L);

    // Set manifest stats
    original.setAddedFilesCount(10);
    original.setExistingFilesCount(85);
    original.setDeletedFilesCount(5);
    original.setAddedRowsCount(10000L);
    original.setExistingRowsCount(850000L);
    original.setDeletedRowsCount(5000L);
    original.setMinSequenceNumber(50L);

    // Set tracking info
    original.setStatus(TrackingInfo.Status.EXISTING);
    original.setSnapshotId(12348L);
    original.setSequenceNumber(102L);
    original.setFileSequenceNumber(100L);
    original.setFirstRowId(100000L); // Starting row ID for new data files

    // Write to file
    OutputFile outputFile =
        Files.localOutput(temp.resolve("data-manifest-tracked-file.avro").toFile());
    try (FileAppender<GenericTrackedFile> appender =
        InternalData.write(FileFormat.AVRO, outputFile)
            .schema(getTrackedFileSchema())
            .named("tracked_file")
            .build()) {
      appender.add(original);
    }

    // Read back
    InputFile inputFile = outputFile.toInputFile();
    List<GenericTrackedFile> read;
    try (CloseableIterable<GenericTrackedFile> files =
        InternalData.read(FileFormat.AVRO, inputFile)
            .setRootType(GenericTrackedFile.class)
            .project(getTrackedFileSchema())
            .build()) {
      read = Lists.newArrayList(files);
    }

    // Verify
    assertThat(read).hasSize(1);
    GenericTrackedFile roundTripped = read.get(0);

    assertThat(roundTripped.contentType()).isEqualTo(FileContent.DATA_MANIFEST);
    assertThat(roundTripped.location())
        .isEqualTo("s3://bucket/table/metadata/manifest-data-1.avro");
    assertThat(roundTripped.fileFormat()).isEqualTo(FileFormat.AVRO);
    assertThat(roundTripped.recordCount()).isEqualTo(100L);
    assertThat(roundTripped.fileSizeInBytes()).isEqualTo(25000L);

    // Verify manifest stats
    ManifestStats stats = roundTripped.manifestStats();
    assertThat(stats).isNotNull();
    assertThat(stats.addedFilesCount()).isEqualTo(10);
    assertThat(stats.existingFilesCount()).isEqualTo(85);
    assertThat(stats.deletedFilesCount()).isEqualTo(5);
    assertThat(stats.addedRowsCount()).isEqualTo(10000L);
    assertThat(stats.existingRowsCount()).isEqualTo(850000L);
    assertThat(stats.deletedRowsCount()).isEqualTo(5000L);
    assertThat(stats.minSequenceNumber()).isEqualTo(50L);

    // Verify tracking info
    TrackingInfo trackingInfo = roundTripped.trackingInfo();
    assertThat(trackingInfo).isNotNull();
    assertThat(trackingInfo.status()).isEqualTo(TrackingInfo.Status.EXISTING);
    assertThat(trackingInfo.firstRowId()).isEqualTo(100000L);
  }

  @Test
  public void testAvroRoundTripEqualityDeletes() throws IOException {
    // Create a tracked file representing equality deletes
    GenericTrackedFile original = new GenericTrackedFile();
    original.setContentType(FileContent.EQUALITY_DELETES);
    original.setLocation("s3://bucket/table/deletes/eq-delete-1.parquet");
    original.setFileFormat(FileFormat.PARQUET);
    original.setPartitionSpecId(0);
    original.setRecordCount(25L);
    original.setFileSizeInBytes(5000L);
    original.setEqualityIds(ImmutableList.of(1, 2, 5)); // Field IDs for equality comparison
    original.setSortOrderId(1);

    // Set tracking info
    original.setStatus(TrackingInfo.Status.ADDED);
    original.setSnapshotId(12349L);
    original.setSequenceNumber(103L);
    original.setFileSequenceNumber(103L);

    // Write to file
    OutputFile outputFile =
        Files.localOutput(temp.resolve("equality-delete-tracked-file.avro").toFile());
    try (FileAppender<GenericTrackedFile> appender =
        InternalData.write(FileFormat.AVRO, outputFile)
            .schema(getTrackedFileSchema())
            .named("tracked_file")
            .build()) {
      appender.add(original);
    }

    // Read back
    InputFile inputFile = outputFile.toInputFile();
    List<GenericTrackedFile> read;
    try (CloseableIterable<GenericTrackedFile> files =
        InternalData.read(FileFormat.AVRO, inputFile)
            .setRootType(GenericTrackedFile.class)
            .project(getTrackedFileSchema())
            .build()) {
      read = Lists.newArrayList(files);
    }

    // Verify
    assertThat(read).hasSize(1);
    GenericTrackedFile roundTripped = read.get(0);

    assertThat(roundTripped.contentType()).isEqualTo(FileContent.EQUALITY_DELETES);
    assertThat(roundTripped.location()).isEqualTo("s3://bucket/table/deletes/eq-delete-1.parquet");
    assertThat(roundTripped.fileFormat()).isEqualTo(FileFormat.PARQUET);
    assertThat(roundTripped.recordCount()).isEqualTo(25L);
    assertThat(roundTripped.equalityIds()).containsExactly(1, 2, 5);
    assertThat(roundTripped.sortOrderId()).isEqualTo(1);
  }

  @Test
  public void testCopy() {
    GenericTrackedFile original = new GenericTrackedFile();
    original.setContentType(FileContent.DATA);
    original.setLocation("s3://bucket/table/data/file1.parquet");
    original.setFileFormat(FileFormat.PARQUET);
    original.setPartitionSpecId(0);
    original.setRecordCount(1000L);
    original.setFileSizeInBytes(50000L);

    // Set manifest stats (should be copied)
    original.setAddedFilesCount(10);
    original.setMinSequenceNumber(50L);

    GenericTrackedFile copy = original.copy();

    // Verify copy is equal but separate instance
    assertThat(copy).isNotSameAs(original);
    assertThat(copy.contentType()).isEqualTo(original.contentType());
    assertThat(copy.location()).isEqualTo(original.location());
    assertThat(copy.recordCount()).isEqualTo(original.recordCount());

    // Verify stats were copied
    assertThat(copy.manifestStats()).isNotNull();
    assertThat(copy.manifestStats().addedFilesCount()).isEqualTo(10);
  }

  @Test
  public void testCopyWithoutStats() {
    GenericTrackedFile original = new GenericTrackedFile();
    original.setContentType(FileContent.DATA);
    original.setLocation("s3://bucket/table/data/file1.parquet");
    original.setFileFormat(FileFormat.PARQUET);
    original.setPartitionSpecId(0);
    original.setRecordCount(1000L);
    original.setFileSizeInBytes(50000L);

    // Set manifest stats (should NOT be copied)
    original.setAddedFilesCount(10);
    original.setMinSequenceNumber(50L);

    GenericTrackedFile copy = original.copyWithoutStats();

    // Verify copy is equal but stats are dropped
    assertThat(copy).isNotSameAs(original);
    assertThat(copy.contentType()).isEqualTo(original.contentType());
    assertThat(copy.location()).isEqualTo(original.location());
    assertThat(copy.recordCount()).isEqualTo(original.recordCount());

    // Verify stats were NOT copied
    assertThat(copy.manifestStats()).isNull();
  }
}
