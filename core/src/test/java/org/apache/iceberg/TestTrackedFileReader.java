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
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestTrackedFileReader {

  private final FileIO io = new InMemoryFileIO();
  private static final long SNAPSHOT_ID = 12345L;
  private static final long SEQUENCE_NUMBER = 100L;

  @TempDir private Path temp;

  @Test
  public void testReadFlatManifest() throws IOException {
    GenericTrackedFile file1 = createDataFile("file1.parquet", 1000L, 50000L);
    file1.setStatus(TrackingInfo.Status.ADDED);
    file1.setSnapshotId(SNAPSHOT_ID);
    file1.setSequenceNumber(SEQUENCE_NUMBER);
    file1.setFileSequenceNumber(SEQUENCE_NUMBER);

    GenericTrackedFile file2 = createDataFile("file2.parquet", 2000L, 100000L);
    file2.setStatus(TrackingInfo.Status.EXISTING);
    file2.setSnapshotId(SNAPSHOT_ID - 1);
    file2.setSequenceNumber(SEQUENCE_NUMBER - 1);
    file2.setFileSequenceNumber(SEQUENCE_NUMBER - 1);

    String manifestPath = writeManifest(file1, file2);

    TrackedFileReader reader =
        TrackedFileReaders.readRoot(manifestPath, io, SNAPSHOT_ID, SEQUENCE_NUMBER, null);

    List<TrackedFile<?>> files = Lists.newArrayList(reader);

    assertThat(files).hasSize(2);

    TrackedFile<?> read1 = files.get(0);
    assertThat(read1.location()).isNotNull();
    assertThat(read1.location()).endsWith("file1.parquet");
    assertThat(read1.recordCount()).isEqualTo(1000L);
    assertThat(read1.fileSizeInBytes()).isEqualTo(50000L);
    assertThat(read1.pos()).isNotNull();
    assertThat(read1.pos()).isEqualTo(0L);

    TrackedFile<?> read2 = files.get(1);
    assertThat(read2.location()).endsWith("file2.parquet");
    assertThat(read2.recordCount()).isEqualTo(2000L);
    assertThat(read2.pos()).isEqualTo(1L);
  }

  @Test
  public void testInheritSnapshotId() throws IOException {
    GenericTrackedFile file1 = createDataFile("file1.parquet", 1000L, 50000L);
    file1.setStatus(TrackingInfo.Status.ADDED);

    String manifestPath = writeManifest(file1);

    TrackedFileReader reader =
        TrackedFileReaders.readRoot(manifestPath, io, SNAPSHOT_ID, SEQUENCE_NUMBER, null);

    List<TrackedFile<?>> files = Lists.newArrayList(reader);

    assertThat(files).hasSize(1);
    TrackedFile<?> read = files.get(0);

    TrackingInfo tracking = read.trackingInfo();
    assertThat(tracking).isNotNull();
    assertThat(tracking.snapshotId()).isEqualTo(SNAPSHOT_ID);
    assertThat(tracking.sequenceNumber()).isEqualTo(SEQUENCE_NUMBER);
    assertThat(tracking.fileSequenceNumber()).isEqualTo(SEQUENCE_NUMBER);
  }

  @Test
  public void testInheritSequenceNumberForAddedOnly() throws IOException {
    GenericTrackedFile added = createDataFile("added.parquet", 1000L, 50000L);
    added.setStatus(TrackingInfo.Status.ADDED);
    added.setSnapshotId(SNAPSHOT_ID);

    GenericTrackedFile existing = createDataFile("existing.parquet", 2000L, 100000L);
    existing.setStatus(TrackingInfo.Status.EXISTING);
    existing.setSnapshotId(SNAPSHOT_ID - 1);
    existing.setSequenceNumber(SEQUENCE_NUMBER - 10);
    existing.setFileSequenceNumber(SEQUENCE_NUMBER - 10);

    String manifestPath = writeManifest(added, existing);

    TrackedFileReader reader =
        TrackedFileReaders.readRoot(manifestPath, io, SNAPSHOT_ID, SEQUENCE_NUMBER, null);

    List<TrackedFile<?>> files = Lists.newArrayList(reader);

    assertThat(files).hasSize(2);

    TrackedFile<?> readAdded = files.get(0);
    assertThat(readAdded.trackingInfo().status()).isEqualTo(TrackingInfo.Status.ADDED);
    assertThat(readAdded.trackingInfo().sequenceNumber()).isEqualTo(SEQUENCE_NUMBER);

    TrackedFile<?> readExisting = files.get(1);
    assertThat(readExisting.trackingInfo().status()).isEqualTo(TrackingInfo.Status.EXISTING);
    assertThat(readExisting.trackingInfo().sequenceNumber()).isEqualTo(SEQUENCE_NUMBER - 10);
  }

  @Test
  public void testRowIdAssignment() throws IOException {
    GenericTrackedFile file1 = createDataFile("file1.parquet", 1000L, 50000L);
    file1.setStatus(TrackingInfo.Status.ADDED);
    file1.setSnapshotId(SNAPSHOT_ID);

    GenericTrackedFile file2 = createDataFile("file2.parquet", 2000L, 100000L);
    file2.setStatus(TrackingInfo.Status.ADDED);
    file2.setSnapshotId(SNAPSHOT_ID);

    String manifestPath = writeManifest(file1, file2);

    long startingRowId = 1000L;
    TrackedFileReader reader =
        TrackedFileReaders.readRoot(manifestPath, io, SNAPSHOT_ID, SEQUENCE_NUMBER, startingRowId);

    List<TrackedFile<?>> files = Lists.newArrayList(reader);

    assertThat(files).hasSize(2);

    TrackedFile<?> read1 = files.get(0);
    assertThat(read1.trackingInfo().firstRowId()).isEqualTo(1000L);

    TrackedFile<?> read2 = files.get(1);
    assertThat(read2.trackingInfo().firstRowId()).isEqualTo(2000L);
  }

  @Test
  public void testLiveEntriesFilterDeleted() throws IOException {
    GenericTrackedFile added = createDataFile("added.parquet", 1000L, 50000L);
    added.setStatus(TrackingInfo.Status.ADDED);
    added.setSnapshotId(SNAPSHOT_ID);

    GenericTrackedFile deleted = createDataFile("deleted.parquet", 2000L, 100000L);
    deleted.setStatus(TrackingInfo.Status.DELETED);
    deleted.setSnapshotId(SNAPSHOT_ID);

    GenericTrackedFile existing = createDataFile("existing.parquet", 3000L, 150000L);
    existing.setStatus(TrackingInfo.Status.EXISTING);
    existing.setSnapshotId(SNAPSHOT_ID - 1);

    String manifestPath = writeManifest(added, deleted, existing);

    TrackedFileReader reader =
        TrackedFileReaders.readRoot(manifestPath, io, SNAPSHOT_ID, SEQUENCE_NUMBER, null);

    List<TrackedFile<?>> liveFiles = Lists.newArrayList(reader.liveEntries());

    assertThat(liveFiles).hasSize(2);

    List<String> locations = Lists.newArrayList();
    List<TrackingInfo.Status> statuses = Lists.newArrayList();
    for (TrackedFile<?> file : liveFiles) {
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
    GenericTrackedFile file1 = createDataFile("file1.parquet", 1000L, 50000L);
    file1.setStatus(TrackingInfo.Status.ADDED);
    file1.setSnapshotId(SNAPSHOT_ID);
    file1.setSortOrderId(5);

    String manifestPath = writeManifest(file1);

    TrackedFileReader reader =
        TrackedFileReaders.readRoot(manifestPath, io, SNAPSHOT_ID, SEQUENCE_NUMBER, null)
            .select(ImmutableList.of("location", "record_count"));

    List<TrackedFile<?>> files = Lists.newArrayList(reader);

    assertThat(files).hasSize(1);
    TrackedFile<?> read = files.get(0);
    assertThat(read.location()).endsWith("file1.parquet");
    assertThat(read.recordCount()).isEqualTo(1000L);
  }

  @Test
  public void testPositionTracking() throws IOException {
    GenericTrackedFile file1 = createDataFile("file1.parquet", 1000L, 50000L);
    GenericTrackedFile file2 = createDataFile("file2.parquet", 2000L, 100000L);
    GenericTrackedFile file3 = createDataFile("file3.parquet", 3000L, 150000L);

    String manifestPath = writeManifest(file1, file2, file3);

    TrackedFileReader reader =
        TrackedFileReaders.readRoot(manifestPath, io, SNAPSHOT_ID, SEQUENCE_NUMBER, null);

    List<TrackedFile<?>> files = Lists.newArrayList(reader.entries());

    assertThat(files).hasSize(3);

    List<Long> positions = Lists.newArrayList();
    for (TrackedFile<?> file : files) {
      assertThat(file.pos()).isNotNull();
      positions.add(file.pos());
    }

    assertThat(positions).containsExactlyInAnyOrder(0L, 1L, 2L);
  }

  private GenericTrackedFile createDataFile(String filename, long recordCount, long fileSize) {
    GenericTrackedFile file = new GenericTrackedFile();
    file.setContentType(FileContent.DATA);
    file.setLocation("s3://bucket/table/data/" + filename);
    file.setFileFormat(FileFormat.PARQUET);
    file.setPartitionSpecId(0);
    file.setRecordCount(recordCount);
    file.setFileSizeInBytes(fileSize);
    return file;
  }

  private String writeManifest(GenericTrackedFile... files) throws IOException {
    OutputFile outputFile = io.newOutputFile("manifest-" + System.nanoTime() + ".parquet");

    try (FileAppender<GenericTrackedFile> appender =
        InternalData.write(FileFormat.PARQUET, outputFile)
            .schema(new Schema(GenericTrackedFile.BASE_TYPE.fields()))
            .named("tracked_file")
            .build()) {
      for (GenericTrackedFile file : files) {
        appender.add(file);
      }
    }

    return outputFile.location();
  }
}
