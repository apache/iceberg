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

public class TestTrackedFileStruct {

  @TempDir private Path temp;

  private static Schema getWriteSchema() {
    return new Schema(TrackedFileStruct.WRITE_TYPE.fields());
  }

  private static Schema getReadSchema() {
    return new Schema(TrackedFileStruct.BASE_TYPE.fields());
  }

  @Test
  public void testAvroRoundTripDataFile() throws IOException {
    // Create a tracked file representing a data file
    TrackedFileStruct original = new TrackedFileStruct();
    original.setContentType(FileContent.DATA);
    original.setLocation("s3://bucket/table/data/file1.parquet");
    original.setFileFormat(FileFormat.PARQUET);
    original.setPartitionSpecId(0);
    original.setRecordCount(1000L);
    original.setFileSizeInBytes(50000L);
    original.setSortOrderId(1);
    original.setSplitOffsets(ImmutableList.of(0L, 10000L, 20000L));

    // Set tracking info
    TrackedFileStruct.TrackingInfoStruct trackingStruct = original.ensureTrackingInfo();
    trackingStruct.setStatus(TrackingInfo.Status.ADDED);
    trackingStruct.setSnapshotId(12345L);
    trackingStruct.setSequenceNumber(100L);
    trackingStruct.setFileSequenceNumber(100L);
    trackingStruct.setFirstRowId(0L);

    // Set key metadata
    original.setKeyMetadata(ByteBuffer.wrap(new byte[] {1, 2, 3, 4}));

    // Write to file
    OutputFile outputFile = Files.localOutput(temp.resolve("tracked-file.parquet").toFile());
    List<TrackedFileStruct> written;
    try (FileAppender<TrackedFileStruct> appender =
        InternalData.write(FileFormat.PARQUET, outputFile)
            .schema(getWriteSchema())
            .named("tracked_file")
            .build()) {
      appender.add(original);
      written = ImmutableList.of(original);
    }

    // Read back
    InputFile inputFile = outputFile.toInputFile();
    List<TrackedFileStruct> read;
    try (CloseableIterable<TrackedFileStruct> files =
        InternalData.read(FileFormat.PARQUET, inputFile)
            .setRootType(TrackedFileStruct.class)
            .project(getReadSchema())
            .build()) {
      read = Lists.newArrayList(files);
    }

    // Verify
    assertThat(read).hasSize(1);
    TrackedFileStruct roundTripped = read.get(0);

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
    assertThat(trackingInfo.dataSequenceNumber()).isEqualTo(100L);
    assertThat(trackingInfo.fileSequenceNumber()).isEqualTo(100L);
    assertThat(trackingInfo.firstRowId()).isEqualTo(0L);

    // Verify key metadata
    assertThat(roundTripped.keyMetadata()).isEqualTo(ByteBuffer.wrap(new byte[] {1, 2, 3, 4}));
  }

  @Test
  public void testAvroRoundTripPositionDeletesWithDeletionVector() throws IOException {
    // Create a tracked file representing position deletes with external deletion vector
    TrackedFileStruct original = new TrackedFileStruct();
    original.setContentType(FileContent.POSITION_DELETES);
    original.setLocation("s3://bucket/table/deletes/dv1.puffin");
    original.setFileFormat(FileFormat.PUFFIN);
    original.setPartitionSpecId(0);
    original.setRecordCount(50L); // 50 deleted positions
    original.setFileSizeInBytes(1000L);
    original.setReferencedFile("s3://bucket/table/data/file1.parquet");

    // Set content info (for external deletion vector)
    TrackedFileStruct.ContentInfoStruct contentInfo = new TrackedFileStruct.ContentInfoStruct();
    contentInfo.setOffset(100L);
    contentInfo.setSizeInBytes(500L);
    original.setContentInfo(contentInfo);

    // Set tracking info
    TrackedFileStruct.TrackingInfoStruct tracking = original.ensureTrackingInfo();
    tracking.setStatus(TrackingInfo.Status.ADDED);
    tracking.setSnapshotId(12346L);
    tracking.setSequenceNumber(101L);
    tracking.setFileSequenceNumber(101L);

    // Write to file
    OutputFile outputFile = Files.localOutput(temp.resolve("dv-tracked-file.avro").toFile());
    try (FileAppender<TrackedFileStruct> appender =
        InternalData.write(FileFormat.AVRO, outputFile)
            .schema(getWriteSchema())
            .named("tracked_file")
            .build()) {
      appender.add(original);
    }

    // Read back
    InputFile inputFile = outputFile.toInputFile();
    List<TrackedFileStruct> read;
    try (CloseableIterable<TrackedFileStruct> files =
        InternalData.read(FileFormat.AVRO, inputFile)
            .setRootType(TrackedFileStruct.class)
            .project(getReadSchema())
            .build()) {
      read = Lists.newArrayList(files);
    }

    // Verify
    assertThat(read).hasSize(1);
    TrackedFileStruct roundTripped = read.get(0);

    assertThat(roundTripped.contentType()).isEqualTo(FileContent.POSITION_DELETES);
    assertThat(roundTripped.location()).isEqualTo("s3://bucket/table/deletes/dv1.puffin");
    assertThat(roundTripped.fileFormat()).isEqualTo(FileFormat.PUFFIN);
    assertThat(roundTripped.recordCount()).isEqualTo(50L);
    assertThat(roundTripped.referencedFile()).isEqualTo("s3://bucket/table/data/file1.parquet");

    // Verify deletion vector
    ContentInfo dv = roundTripped.contentInfo();
    assertThat(dv).isNotNull();
    assertThat(dv.offset()).isEqualTo(100L);
    assertThat(dv.sizeInBytes()).isEqualTo(500L);
  }

  @Test
  public void testAvroRoundTripDataManifestWithManifestDV() throws IOException {
    // Create a tracked file representing a data manifest entry with an inline manifest DV
    TrackedFileStruct original = new TrackedFileStruct();
    original.setContentType(FileContent.DATA_MANIFEST);
    original.setLocation("s3://bucket/table/metadata/manifest-data-1.avro");
    original.setFileFormat(FileFormat.AVRO);
    original.setPartitionSpecId(0);
    original.setRecordCount(100L); // 100 entries in the manifest
    original.setFileSizeInBytes(25000L);

    // Set manifest DV - marks positions 0, 1, 2 as deleted in the referenced manifest
    byte[] dvContent = new byte[] {(byte) 0b00000111};
    original.setManifestDV(ByteBuffer.wrap(dvContent));

    // Set tracking info
    original.ensureTrackingInfo().setStatus(TrackingInfo.Status.ADDED);
    original.ensureTrackingInfo().setSnapshotId(12347L);

    // Write to file
    OutputFile outputFile = Files.localOutput(temp.resolve("manifest-with-dv.avro").toFile());
    try (FileAppender<TrackedFileStruct> appender =
        InternalData.write(FileFormat.AVRO, outputFile)
            .schema(getWriteSchema())
            .named("tracked_file")
            .build()) {
      appender.add(original);
    }

    // Read back
    InputFile inputFile = outputFile.toInputFile();
    List<TrackedFileStruct> read;
    try (CloseableIterable<TrackedFileStruct> files =
        InternalData.read(FileFormat.AVRO, inputFile)
            .setRootType(TrackedFileStruct.class)
            .project(getReadSchema())
            .build()) {
      read = Lists.newArrayList(files);
    }

    // Verify
    assertThat(read).hasSize(1);
    TrackedFileStruct roundTripped = read.get(0);

    assertThat(roundTripped.contentType()).isEqualTo(FileContent.DATA_MANIFEST);
    assertThat(roundTripped.location())
        .isEqualTo("s3://bucket/table/metadata/manifest-data-1.avro");
    assertThat(roundTripped.recordCount()).isEqualTo(100L);

    // Verify manifest DV
    ByteBuffer manifestDV = roundTripped.manifestDV();
    assertThat(manifestDV).isNotNull();
    assertThat(manifestDV).isEqualTo(ByteBuffer.wrap(dvContent));
  }

  @Test
  public void testAvroRoundTripDataManifestWithStats() throws IOException {
    // Create a tracked file representing a data manifest entry
    TrackedFileStruct original = new TrackedFileStruct();
    original.setContentType(FileContent.DATA_MANIFEST);
    original.setLocation("s3://bucket/table/metadata/manifest-data-1.avro");
    original.setFileFormat(FileFormat.AVRO);
    original.setPartitionSpecId(0);
    original.setRecordCount(100L); // 100 entries in the manifest
    original.setFileSizeInBytes(25000L);

    // Set manifest stats
    TrackedFileStruct.ManifestStatsStruct manifestStats =
        new TrackedFileStruct.ManifestStatsStruct();
    manifestStats.setAddedFilesCount(10);
    manifestStats.setExistingFilesCount(85);
    manifestStats.setDeletedFilesCount(5);
    manifestStats.setAddedRowsCount(10000L);
    manifestStats.setExistingRowsCount(850000L);
    manifestStats.setDeletedRowsCount(5000L);
    manifestStats.setMinSequenceNumber(50L);
    original.setManifestStats(manifestStats);

    // Set tracking info
    TrackedFileStruct.TrackingInfoStruct trackingStruct = original.ensureTrackingInfo();
    trackingStruct.setStatus(TrackingInfo.Status.EXISTING);
    trackingStruct.setSnapshotId(12348L);
    trackingStruct.setSequenceNumber(102L);
    trackingStruct.setFileSequenceNumber(100L);
    trackingStruct.setFirstRowId(100000L); // Starting row ID for new data files

    // Write to file
    OutputFile outputFile =
        Files.localOutput(temp.resolve("data-manifest-tracked-file.avro").toFile());
    try (FileAppender<TrackedFileStruct> appender =
        InternalData.write(FileFormat.AVRO, outputFile)
            .schema(getWriteSchema())
            .named("tracked_file")
            .build()) {
      appender.add(original);
    }

    // Read back
    InputFile inputFile = outputFile.toInputFile();
    List<TrackedFileStruct> read;
    try (CloseableIterable<TrackedFileStruct> files =
        InternalData.read(FileFormat.AVRO, inputFile)
            .setRootType(TrackedFileStruct.class)
            .project(getReadSchema())
            .build()) {
      read = Lists.newArrayList(files);
    }

    // Verify
    assertThat(read).hasSize(1);
    TrackedFileStruct roundTripped = read.get(0);

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
    TrackedFileStruct original = new TrackedFileStruct();
    original.setContentType(FileContent.EQUALITY_DELETES);
    original.setLocation("s3://bucket/table/deletes/eq-delete-1.parquet");
    original.setFileFormat(FileFormat.PARQUET);
    original.setPartitionSpecId(0);
    original.setRecordCount(25L);
    original.setFileSizeInBytes(5000L);
    original.setEqualityIds(ImmutableList.of(1, 2, 5)); // Field IDs for equality comparison
    original.setSortOrderId(1);

    // Set tracking info
    TrackedFileStruct.TrackingInfoStruct tracking = original.ensureTrackingInfo();
    tracking.setStatus(TrackingInfo.Status.ADDED);
    tracking.setSnapshotId(12349L);
    tracking.setSequenceNumber(103L);
    tracking.setFileSequenceNumber(103L);

    // Write to file
    OutputFile outputFile =
        Files.localOutput(temp.resolve("equality-delete-tracked-file.avro").toFile());
    try (FileAppender<TrackedFileStruct> appender =
        InternalData.write(FileFormat.AVRO, outputFile)
            .schema(getWriteSchema())
            .named("tracked_file")
            .build()) {
      appender.add(original);
    }

    // Read back
    InputFile inputFile = outputFile.toInputFile();
    List<TrackedFileStruct> read;
    try (CloseableIterable<TrackedFileStruct> files =
        InternalData.read(FileFormat.AVRO, inputFile)
            .setRootType(TrackedFileStruct.class)
            .project(getReadSchema())
            .build()) {
      read = Lists.newArrayList(files);
    }

    // Verify
    assertThat(read).hasSize(1);
    TrackedFileStruct roundTripped = read.get(0);

    assertThat(roundTripped.contentType()).isEqualTo(FileContent.EQUALITY_DELETES);
    assertThat(roundTripped.location()).isEqualTo("s3://bucket/table/deletes/eq-delete-1.parquet");
    assertThat(roundTripped.fileFormat()).isEqualTo(FileFormat.PARQUET);
    assertThat(roundTripped.recordCount()).isEqualTo(25L);
    assertThat(roundTripped.equalityIds()).containsExactly(1, 2, 5);
    assertThat(roundTripped.sortOrderId()).isEqualTo(1);
  }

  @Test
  public void testCopy() {
    TrackedFileStruct original = new TrackedFileStruct();
    original.setContentType(FileContent.DATA);
    original.setLocation("s3://bucket/table/data/file1.parquet");
    original.setFileFormat(FileFormat.PARQUET);
    original.setPartitionSpecId(0);
    original.setRecordCount(1000L);
    original.setFileSizeInBytes(50000L);

    // Set manifest stats (should be copied)
    TrackedFileStruct.ManifestStatsStruct stats = new TrackedFileStruct.ManifestStatsStruct();
    stats.setAddedFilesCount(10);
    stats.setMinSequenceNumber(50L);
    original.setManifestStats(stats);

    TrackedFile copy = original.copy();

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
    TrackedFileStruct original = new TrackedFileStruct();
    original.setContentType(FileContent.DATA);
    original.setLocation("s3://bucket/table/data/file1.parquet");
    original.setFileFormat(FileFormat.PARQUET);
    original.setPartitionSpecId(0);
    original.setRecordCount(1000L);
    original.setFileSizeInBytes(50000L);

    // Set manifest stats (should NOT be copied)
    TrackedFileStruct.ManifestStatsStruct stats = new TrackedFileStruct.ManifestStatsStruct();
    stats.setAddedFilesCount(10);
    stats.setMinSequenceNumber(50L);
    original.setManifestStats(stats);

    TrackedFile copy = original.copyWithoutStats();

    // Verify copy is equal but stats are dropped
    assertThat(copy).isNotSameAs(original);
    assertThat(copy.contentType()).isEqualTo(original.contentType());
    assertThat(copy.location()).isEqualTo(original.location());
    assertThat(copy.recordCount()).isEqualTo(original.recordCount());

    // Verify stats were NOT copied
    assertThat(copy.manifestStats()).isNull();
  }
}
