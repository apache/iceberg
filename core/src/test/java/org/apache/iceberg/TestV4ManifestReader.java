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
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestV4ManifestReader {

  private final FileIO io = new InMemoryFileIO();
  private static final long SNAPSHOT_ID = 12345L;
  private static final long SEQUENCE_NUMBER = 100L;

  @TempDir private Path temp;

  @Test
  public void testReadFlatManifest() throws IOException {
    TrackedFileStruct file1 = createDataFile("file1.parquet", 1000L, 50000L);
    TrackedFileStruct.TrackingInfoStruct tracking1 = file1.ensureTrackingInfo();
    tracking1.setStatus(TrackingInfo.Status.ADDED);
    tracking1.setSnapshotId(SNAPSHOT_ID);
    tracking1.setSequenceNumber(SEQUENCE_NUMBER);
    tracking1.setFileSequenceNumber(SEQUENCE_NUMBER);

    TrackedFileStruct file2 = createDataFile("file2.parquet", 2000L, 100000L);
    TrackedFileStruct.TrackingInfoStruct tracking2 = file2.ensureTrackingInfo();
    tracking2.setStatus(TrackingInfo.Status.EXISTING);
    tracking2.setSnapshotId(SNAPSHOT_ID - 1);
    tracking2.setSequenceNumber(SEQUENCE_NUMBER - 1);
    tracking2.setFileSequenceNumber(SEQUENCE_NUMBER - 1);

    String manifestPath = writeManifest(file1, file2);

    V4ManifestReader reader =
        V4ManifestReaders.readRoot(manifestPath, io, SNAPSHOT_ID, SEQUENCE_NUMBER);

    List<TrackedFile> files = Lists.newArrayList(reader);

    assertThat(files).hasSize(2);

    TrackedFile read1 = files.get(0);
    assertThat(read1.location()).isNotNull();
    assertThat(read1.location()).endsWith("file1.parquet");
    assertThat(read1.recordCount()).isEqualTo(1000L);
    assertThat(read1.fileSizeInBytes()).isEqualTo(50000L);
    assertThat(read1.pos()).isNotNull();
    assertThat(read1.pos()).isEqualTo(0L);

    TrackedFile read2 = files.get(1);
    assertThat(read2.location()).endsWith("file2.parquet");
    assertThat(read2.recordCount()).isEqualTo(2000L);
    assertThat(read2.pos()).isEqualTo(1L);
  }

  @Test
  public void testInheritSnapshotId() throws IOException {
    TrackedFileStruct file1 = createDataFile("file1.parquet", 1000L, 50000L);
    file1.ensureTrackingInfo().setStatus(TrackingInfo.Status.ADDED);

    String manifestPath = writeManifest(file1);

    V4ManifestReader reader =
        V4ManifestReaders.readRoot(manifestPath, io, SNAPSHOT_ID, SEQUENCE_NUMBER);

    List<TrackedFile> files = Lists.newArrayList(reader);

    assertThat(files).hasSize(1);
    TrackedFile read = files.get(0);

    TrackingInfo tracking = read.trackingInfo();
    assertThat(tracking).isNotNull();
    assertThat(tracking.snapshotId()).isEqualTo(SNAPSHOT_ID);
    assertThat(tracking.dataSequenceNumber()).isEqualTo(SEQUENCE_NUMBER);
    assertThat(tracking.fileSequenceNumber()).isEqualTo(SEQUENCE_NUMBER);
  }

  @Test
  public void testInheritSequenceNumberForAddedOnly() throws IOException {
    TrackedFileStruct added = createDataFile("added.parquet", 1000L, 50000L);
    TrackedFileStruct.TrackingInfoStruct addedTracking = added.ensureTrackingInfo();
    addedTracking.setStatus(TrackingInfo.Status.ADDED);
    addedTracking.setSnapshotId(SNAPSHOT_ID);

    TrackedFileStruct existing = createDataFile("existing.parquet", 2000L, 100000L);
    TrackedFileStruct.TrackingInfoStruct existingTracking = existing.ensureTrackingInfo();
    existingTracking.setStatus(TrackingInfo.Status.EXISTING);
    existingTracking.setSnapshotId(SNAPSHOT_ID - 1);
    existingTracking.setSequenceNumber(SEQUENCE_NUMBER - 10);
    existingTracking.setFileSequenceNumber(SEQUENCE_NUMBER - 10);

    String manifestPath = writeManifest(added, existing);

    V4ManifestReader reader =
        V4ManifestReaders.readRoot(manifestPath, io, SNAPSHOT_ID, SEQUENCE_NUMBER);

    List<TrackedFile> files = Lists.newArrayList(reader);

    assertThat(files).hasSize(2);

    TrackedFile readAdded = files.get(0);
    assertThat(readAdded.trackingInfo().status()).isEqualTo(TrackingInfo.Status.ADDED);
    assertThat(readAdded.trackingInfo().dataSequenceNumber()).isEqualTo(SEQUENCE_NUMBER);

    TrackedFile readExisting = files.get(1);
    assertThat(readExisting.trackingInfo().status()).isEqualTo(TrackingInfo.Status.EXISTING);
    assertThat(readExisting.trackingInfo().dataSequenceNumber()).isEqualTo(SEQUENCE_NUMBER - 10);
  }

  @Test
  public void testRowIdAssignmentInLeafManifest() throws IOException {
    // Create data files for the leaf manifest
    TrackedFileStruct file1 = createDataFile("file1.parquet", 1000L, 50000L);
    TrackedFileStruct.TrackingInfoStruct tracking1 = file1.ensureTrackingInfo();
    tracking1.setStatus(TrackingInfo.Status.ADDED);
    tracking1.setSnapshotId(SNAPSHOT_ID);

    TrackedFileStruct file2 = createDataFile("file2.parquet", 2000L, 100000L);
    TrackedFileStruct.TrackingInfoStruct tracking2 = file2.ensureTrackingInfo();
    tracking2.setStatus(TrackingInfo.Status.ADDED);
    tracking2.setSnapshotId(SNAPSHOT_ID);

    String manifestPath = writeManifest(file1, file2);

    // Create a DATA_MANIFEST entry that points to the manifest, with first_row_id set
    long startingRowId = 1000L;
    TrackedFileStruct manifestEntry = new TrackedFileStruct();
    manifestEntry.setContentType(FileContent.DATA_MANIFEST);
    manifestEntry.setLocation(manifestPath);
    manifestEntry.setFileFormat(FileFormat.AVRO);
    TrackedFileStruct.TrackingInfoStruct manifestTracking = manifestEntry.ensureTrackingInfo();
    manifestTracking.setStatus(TrackingInfo.Status.ADDED);
    manifestTracking.setSnapshotId(SNAPSHOT_ID);
    manifestTracking.setSequenceNumber(SEQUENCE_NUMBER);
    manifestTracking.setFirstRowId(startingRowId);

    Map<Integer, PartitionSpec> specsById = ImmutableMap.of();

    V4ManifestReader reader = V4ManifestReaders.readLeaf(manifestEntry, io, specsById);

    List<TrackedFile> files = Lists.newArrayList(reader);

    assertThat(files).hasSize(2);

    TrackedFile read1 = files.get(0);
    assertThat(read1.trackingInfo().firstRowId()).isEqualTo(1000L);

    TrackedFile read2 = files.get(1);
    assertThat(read2.trackingInfo().firstRowId()).isEqualTo(2000L);
  }

  @Test
  public void testLiveEntriesFilterDeleted() throws IOException {
    TrackedFileStruct added = createDataFile("added.parquet", 1000L, 50000L);
    TrackedFileStruct.TrackingInfoStruct addedTracking = added.ensureTrackingInfo();
    addedTracking.setStatus(TrackingInfo.Status.ADDED);
    addedTracking.setSnapshotId(SNAPSHOT_ID);

    TrackedFileStruct deleted = createDataFile("deleted.parquet", 2000L, 100000L);
    TrackedFileStruct.TrackingInfoStruct deletedTracking = deleted.ensureTrackingInfo();
    deletedTracking.setStatus(TrackingInfo.Status.DELETED);
    deletedTracking.setSnapshotId(SNAPSHOT_ID);

    TrackedFileStruct existing = createDataFile("existing.parquet", 3000L, 150000L);
    TrackedFileStruct.TrackingInfoStruct existingTracking = existing.ensureTrackingInfo();
    existingTracking.setStatus(TrackingInfo.Status.EXISTING);
    existingTracking.setSnapshotId(SNAPSHOT_ID - 1);

    String manifestPath = writeManifest(added, deleted, existing);

    V4ManifestReader reader =
        V4ManifestReaders.readRoot(manifestPath, io, SNAPSHOT_ID, SEQUENCE_NUMBER);

    List<TrackedFile> liveFiles = Lists.newArrayList(reader.liveFiles());

    assertThat(liveFiles).hasSize(2);

    List<String> locations = Lists.newArrayList();
    List<TrackingInfo.Status> statuses = Lists.newArrayList();
    for (TrackedFile file : liveFiles) {
      locations.add(file.location());
      statuses.add(file.trackingInfo().status());
    }

    assertThat(locations)
        .anyMatch(loc -> loc.endsWith("added.parquet"))
        .anyMatch(loc -> loc.endsWith("existing.parquet"));
    assertThat(statuses)
        .containsExactlyInAnyOrder(TrackingInfo.Status.ADDED, TrackingInfo.Status.EXISTING);
    assertThat(locations).noneMatch(loc -> loc.endsWith("deleted.parquet"));
  }

  @Test
  public void testColumnProjection() throws IOException {
    TrackedFileStruct file1 = createDataFile("file1.parquet", 1000L, 50000L);
    TrackedFileStruct.TrackingInfoStruct tracking = file1.ensureTrackingInfo();
    tracking.setStatus(TrackingInfo.Status.ADDED);
    tracking.setSnapshotId(SNAPSHOT_ID);
    file1.setSortOrderId(5);

    String manifestPath = writeManifest(file1);

    // Create a projection schema with only location and record_count fields
    Schema projection = new Schema(TrackedFile.LOCATION, TrackedFile.RECORD_COUNT);

    V4ManifestReader reader =
        V4ManifestReaders.readRoot(manifestPath, io, SNAPSHOT_ID, SEQUENCE_NUMBER)
            .project(projection);

    List<TrackedFile> files = Lists.newArrayList(reader);

    assertThat(files).hasSize(1);
    TrackedFile read = files.get(0);
    assertThat(read.location()).endsWith("file1.parquet");
    assertThat(read.recordCount()).isEqualTo(1000L);
  }

  @Test
  public void testPositionTracking() throws IOException {
    TrackedFileStruct file1 = createDataFile("file1.parquet", 1000L, 50000L);
    TrackedFileStruct file2 = createDataFile("file2.parquet", 2000L, 100000L);
    TrackedFileStruct file3 = createDataFile("file3.parquet", 3000L, 150000L);

    String manifestPath = writeManifest(file1, file2, file3);

    V4ManifestReader reader =
        V4ManifestReaders.readRoot(manifestPath, io, SNAPSHOT_ID, SEQUENCE_NUMBER);

    List<TrackedFile> files = Lists.newArrayList(reader.allFiles());

    assertThat(files).hasSize(3);

    List<Long> positions = Lists.newArrayList();
    for (TrackedFile file : files) {
      assertThat(file.pos()).isNotNull();
      positions.add(file.pos());
    }

    assertThat(positions).containsExactlyInAnyOrder(0L, 1L, 2L);
  }

  private TrackedFileStruct createDataFile(String filename, long recordCount, long fileSize) {
    TrackedFileStruct file = new TrackedFileStruct();
    file.setContentType(FileContent.DATA);
    file.setLocation("s3://bucket/table/data/" + filename);
    file.setFileFormat(FileFormat.PARQUET);
    file.setPartitionSpecId(0);
    file.setRecordCount(recordCount);
    file.setFileSizeInBytes(fileSize);
    return file;
  }

  private String writeManifest(TrackedFileStruct... files) throws IOException {
    OutputFile outputFile = io.newOutputFile("manifest-" + System.nanoTime() + ".avro");

    try (FileAppender<TrackedFileStruct> appender =
        InternalData.write(FileFormat.AVRO, outputFile)
            .schema(new Schema(TrackedFileStruct.BASE_TYPE.fields()))
            .named("tracked_file")
            .build()) {
      for (TrackedFileStruct file : files) {
        appender.add(file);
      }
    }

    return outputFile.location();
  }
}
