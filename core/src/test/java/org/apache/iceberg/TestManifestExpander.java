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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.roaringbitmap.RoaringBitmap;

public class TestManifestExpander {

  private final FileIO io = new InMemoryFileIO();
  private final Map<Integer, PartitionSpec> specsById;

  private static final long SNAPSHOT_ID = 12345L;
  private static final long SEQUENCE_NUMBER = 100L;

  @TempDir private Path temp;

  public TestManifestExpander() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "data", Types.StringType.get()));
    PartitionSpec spec = PartitionSpec.unpartitioned();
    this.specsById = ImmutableMap.of(spec.specId(), spec);
  }

  @Test
  public void testRootWithOnlyDirectFiles() throws IOException {
    GenericTrackedFile file1 = createDataFile("file1.parquet", 1000L);
    file1.setStatus(TrackingInfo.Status.ADDED);

    GenericTrackedFile file2 = createDataFile("file2.parquet", 2000L);
    file2.setStatus(TrackingInfo.Status.EXISTING);

    String rootPath = writeRootManifest(file1, file2);

    V4ManifestReader rootReader =
        V4ManifestReaders.readRoot(rootPath, io, SNAPSHOT_ID, SEQUENCE_NUMBER, null);
    ManifestExpander reader = new ManifestExpander(rootReader, io, specsById);

    List<TrackedFile<?>> allFiles = Lists.newArrayList(reader.allTrackedFiles());

    assertThat(allFiles).hasSize(2);
    assertThat(allFiles.get(0).contentType()).isEqualTo(FileContent.DATA);
    assertThat(allFiles.get(1).contentType()).isEqualTo(FileContent.DATA);
  }

  @Test
  public void testRootWithOnlyManifests() throws IOException {
    GenericTrackedFile dataFile1 = createDataFile("data1.parquet", 1000L);
    GenericTrackedFile dataFile2 = createDataFile("data2.parquet", 2000L);
    String dataManifestPath = writeLeafManifest(dataFile1, dataFile2);

    GenericTrackedFile manifestEntry = createManifestEntry(dataManifestPath, 2, 2000L);

    String rootPath = writeRootManifest(manifestEntry);

    V4ManifestReader rootReader =
        V4ManifestReaders.readRoot(rootPath, io, SNAPSHOT_ID, SEQUENCE_NUMBER, null);
    ManifestExpander reader = new ManifestExpander(rootReader, io, specsById);

    List<TrackedFile<?>> allFiles = Lists.newArrayList(reader.allTrackedFiles());

    assertThat(allFiles).hasSize(2);
    assertThat(allFiles.get(0).location()).endsWith("data1.parquet");
    assertThat(allFiles.get(1).location()).endsWith("data2.parquet");
  }

  @Test
  public void testRootWithMixedDirectAndManifests() throws IOException {
    GenericTrackedFile directFile = createDataFile("direct.parquet", 500L);
    directFile.setStatus(TrackingInfo.Status.ADDED);

    GenericTrackedFile leafFile1 = createDataFile("leaf1.parquet", 1000L);
    GenericTrackedFile leafFile2 = createDataFile("leaf2.parquet", 2000L);
    String leafManifestPath = writeLeafManifest(leafFile1, leafFile2);

    GenericTrackedFile manifestEntry = createManifestEntry(leafManifestPath, 2, 3000L);

    String rootPath = writeRootManifest(directFile, manifestEntry);

    V4ManifestReader rootReader =
        V4ManifestReaders.readRoot(rootPath, io, SNAPSHOT_ID, SEQUENCE_NUMBER, null);
    ManifestExpander reader = new ManifestExpander(rootReader, io, specsById);

    List<TrackedFile<?>> allFiles = Lists.newArrayList(reader.allTrackedFiles());

    assertThat(allFiles).hasSize(3);

    List<String> locations = Lists.newArrayList();
    for (TrackedFile<?> file : allFiles) {
      locations.add(file.location());
    }

    assertThat(locations)
        .anyMatch(loc -> loc.endsWith("direct.parquet"))
        .anyMatch(loc -> loc.endsWith("leaf1.parquet"))
        .anyMatch(loc -> loc.endsWith("leaf2.parquet"));
  }

  @Test
  public void testMultipleDataManifests() throws IOException {
    GenericTrackedFile file1 = createDataFile("file1.parquet", 1000L);
    GenericTrackedFile file2 = createDataFile("file2.parquet", 2000L);
    String manifest1Path = writeLeafManifest(file1, file2);

    GenericTrackedFile file3 = createDataFile("file3.parquet", 3000L);
    String manifest2Path = writeLeafManifest(file3);

    GenericTrackedFile manifestEntry1 = createManifestEntry(manifest1Path, 2, 3000L);
    GenericTrackedFile manifestEntry2 = createManifestEntry(manifest2Path, 1, 3000L);

    String rootPath = writeRootManifest(manifestEntry1, manifestEntry2);

    V4ManifestReader rootReader =
        V4ManifestReaders.readRoot(rootPath, io, SNAPSHOT_ID, SEQUENCE_NUMBER, null);
    ManifestExpander reader = new ManifestExpander(rootReader, io, specsById);

    List<TrackedFile<?>> allFiles = Lists.newArrayList(reader.allTrackedFiles());

    assertThat(allFiles).hasSize(3);
    assertThat(allFiles).allMatch(tf -> tf.contentType() == FileContent.DATA);
  }

  @Test
  public void testDeleteManifests() throws IOException {
    GenericTrackedFile deleteFile1 = createDeleteFile("delete1.parquet", 100L);
    GenericTrackedFile deleteFile2 = createDeleteFile("delete2.parquet", 200L);
    String deleteManifestPath = writeLeafManifest(deleteFile1, deleteFile2);

    GenericTrackedFile manifestEntry = createDeleteManifestEntry(deleteManifestPath, 2, 300L);

    String rootPath = writeRootManifest(manifestEntry);

    V4ManifestReader rootReader =
        V4ManifestReaders.readRoot(rootPath, io, SNAPSHOT_ID, SEQUENCE_NUMBER, null);
    ManifestExpander reader = new ManifestExpander(rootReader, io, specsById);

    List<TrackedFile<?>> allFiles = Lists.newArrayList(reader.allTrackedFiles());

    assertThat(allFiles).hasSize(2);
    assertThat(allFiles).allMatch(tf -> tf.contentType() == FileContent.POSITION_DELETES);
  }

  @Test
  public void testMixedDataAndDeleteManifests() throws IOException {
    GenericTrackedFile dataFile = createDataFile("data.parquet", 1000L);
    String dataManifestPath = writeLeafManifest(dataFile);

    GenericTrackedFile deleteFile = createDeleteFile("delete.parquet", 100L);
    String deleteManifestPath = writeLeafManifest(deleteFile);

    GenericTrackedFile dataManifestEntry = createManifestEntry(dataManifestPath, 1, 1000L);
    GenericTrackedFile deleteManifestEntry = createDeleteManifestEntry(deleteManifestPath, 1, 100L);

    String rootPath = writeRootManifest(dataManifestEntry, deleteManifestEntry);

    V4ManifestReader rootReader =
        V4ManifestReaders.readRoot(rootPath, io, SNAPSHOT_ID, SEQUENCE_NUMBER, null);
    ManifestExpander reader = new ManifestExpander(rootReader, io, specsById);

    List<TrackedFile<?>> allFiles = Lists.newArrayList(reader.allTrackedFiles());

    assertThat(allFiles).hasSize(2);

    long dataFiles = allFiles.stream().filter(tf -> tf.contentType() == FileContent.DATA).count();
    long deleteFiles =
        allFiles.stream().filter(tf -> tf.contentType() == FileContent.POSITION_DELETES).count();

    assertThat(dataFiles).isEqualTo(1);
    assertThat(deleteFiles).isEqualTo(1);
  }

  @Test
  public void testPlanDataFilesOnlyReturnsData() throws IOException {
    GenericTrackedFile dataFile = createDataFile("data.parquet", 1000L);
    GenericTrackedFile deleteFile = createDeleteFile("delete.parquet", 100L);

    String rootPath = writeRootManifest(dataFile, deleteFile);

    V4ManifestReader rootReader =
        V4ManifestReaders.readRoot(rootPath, io, SNAPSHOT_ID, SEQUENCE_NUMBER, null);
    ManifestExpander expander = new ManifestExpander(rootReader, io, specsById);

    List<ManifestExpander.DataFileScanInfo> scanInfos =
        Lists.newArrayList(expander.planDataFiles());

    assertThat(scanInfos).hasSize(1);
    assertThat(scanInfos.get(0).dataFile().contentType()).isEqualTo(FileContent.DATA);
    assertThat(scanInfos.get(0).dataFile().location()).endsWith("data.parquet");
  }

  @Test
  public void testIgnoreDeleted() throws IOException {
    GenericTrackedFile added = createDataFile("added.parquet", 1000L);
    added.setStatus(TrackingInfo.Status.ADDED);

    GenericTrackedFile deleted = createDataFile("deleted.parquet", 2000L);
    deleted.setStatus(TrackingInfo.Status.DELETED);

    GenericTrackedFile existing = createDataFile("existing.parquet", 3000L);
    existing.setStatus(TrackingInfo.Status.EXISTING);

    String rootPath = writeRootManifest(added, deleted, existing);

    V4ManifestReader rootReader =
        V4ManifestReaders.readRoot(rootPath, io, SNAPSHOT_ID, SEQUENCE_NUMBER, null);
    ManifestExpander expander = new ManifestExpander(rootReader, io, specsById).ignoreDeleted();

    List<ManifestExpander.DataFileScanInfo> scanInfos =
        Lists.newArrayList(expander.planDataFiles());

    assertThat(scanInfos).hasSize(2);
    assertThat(scanInfos).noneMatch(info -> info.dataFile().location().endsWith("deleted.parquet"));
  }

  @Test
  public void testIgnoreExisting() throws IOException {
    GenericTrackedFile added = createDataFile("added.parquet", 1000L);
    added.setStatus(TrackingInfo.Status.ADDED);

    GenericTrackedFile existing = createDataFile("existing.parquet", 2000L);
    existing.setStatus(TrackingInfo.Status.EXISTING);

    String rootPath = writeRootManifest(added, existing);

    V4ManifestReader rootReader =
        V4ManifestReaders.readRoot(rootPath, io, SNAPSHOT_ID, SEQUENCE_NUMBER, null);
    ManifestExpander expander = new ManifestExpander(rootReader, io, specsById).ignoreExisting();

    List<ManifestExpander.DataFileScanInfo> scanInfos =
        Lists.newArrayList(expander.planDataFiles());

    assertThat(scanInfos).hasSize(1);
    assertThat(scanInfos.get(0).dataFile().location()).endsWith("added.parquet");
  }

  @Test
  public void testPlanDataFilesFromManifests() throws IOException {
    GenericTrackedFile dataFile1 = createDataFile("data1.parquet", 1000L);
    GenericTrackedFile dataFile2 = createDataFile("data2.parquet", 2000L);
    String dataManifestPath = writeLeafManifest(dataFile1, dataFile2);

    GenericTrackedFile manifestEntry = createManifestEntry(dataManifestPath, 2, 3000L);

    String rootPath = writeRootManifest(manifestEntry);

    V4ManifestReader rootReader =
        V4ManifestReaders.readRoot(rootPath, io, SNAPSHOT_ID, SEQUENCE_NUMBER, null);
    ManifestExpander expander = new ManifestExpander(rootReader, io, specsById);

    List<ManifestExpander.DataFileScanInfo> scanInfos =
        Lists.newArrayList(expander.planDataFiles());

    assertThat(scanInfos).hasSize(2);
    assertThat(scanInfos).allMatch(info -> info.dataFile().contentType() == FileContent.DATA);
  }

  @Test
  public void testDeleteMatching() throws IOException {
    String dataFilePath = "s3://bucket/table/data/file1.parquet";
    GenericTrackedFile dataFile = createDataFile("file1.parquet", 1000L);
    dataFile.setLocation(dataFilePath);

    GenericTrackedFile deleteFile = createDeleteFile("delete1.parquet", 50L);
    deleteFile.setReferencedFile(dataFilePath);

    String rootPath = writeRootManifest(dataFile, deleteFile);

    V4ManifestReader rootReader =
        V4ManifestReaders.readRoot(rootPath, io, SNAPSHOT_ID, SEQUENCE_NUMBER, null);
    ManifestExpander expander = new ManifestExpander(rootReader, io, specsById);

    List<ManifestExpander.DataFileScanInfo> scanInfos =
        Lists.newArrayList(expander.planDataFiles());

    assertThat(scanInfos).hasSize(1);
    ManifestExpander.DataFileScanInfo scanInfo = scanInfos.get(0);

    assertThat(scanInfo.dataFile().location()).isEqualTo(dataFilePath);
    assertThat(scanInfo.deleteFiles()).hasSize(1);
    assertThat(scanInfo.deleteFiles().get(0).location()).endsWith("delete1.parquet");
  }

  @Test
  public void testMultipleDeletesMatchToSameFile() throws IOException {
    String dataFilePath = "s3://bucket/table/data/file1.parquet";
    GenericTrackedFile dataFile = createDataFile("file1.parquet", 1000L);
    dataFile.setLocation(dataFilePath);

    GenericTrackedFile deleteFile1 = createDeleteFile("delete1.parquet", 50L);
    deleteFile1.setReferencedFile(dataFilePath);

    GenericTrackedFile deleteFile2 = createDeleteFile("delete2.parquet", 30L);
    deleteFile2.setReferencedFile(dataFilePath);

    String rootPath = writeRootManifest(dataFile, deleteFile1, deleteFile2);

    V4ManifestReader rootReader =
        V4ManifestReaders.readRoot(rootPath, io, SNAPSHOT_ID, SEQUENCE_NUMBER, null);
    ManifestExpander expander = new ManifestExpander(rootReader, io, specsById);

    List<ManifestExpander.DataFileScanInfo> scanInfos =
        Lists.newArrayList(expander.planDataFiles());

    assertThat(scanInfos).hasSize(1);
    assertThat(scanInfos.get(0).deleteFiles()).hasSize(2);
  }

  @Test
  public void testDeleteWithoutReferencedFileNotMatched() throws IOException {
    GenericTrackedFile dataFile = createDataFile("data.parquet", 1000L);

    GenericTrackedFile deleteFile = createDeleteFile("delete.parquet", 50L);

    String rootPath = writeRootManifest(dataFile, deleteFile);

    V4ManifestReader rootReader =
        V4ManifestReaders.readRoot(rootPath, io, SNAPSHOT_ID, SEQUENCE_NUMBER, null);
    ManifestExpander expander = new ManifestExpander(rootReader, io, specsById);

    List<ManifestExpander.DataFileScanInfo> scanInfos =
        Lists.newArrayList(expander.planDataFiles());

    assertThat(scanInfos).hasSize(1);
    assertThat(scanInfos.get(0).deleteFiles()).isEmpty();
  }

  @Test
  public void testDataFileWithNoDeletes() throws IOException {
    GenericTrackedFile dataFile = createDataFile("data.parquet", 1000L);

    String rootPath = writeRootManifest(dataFile);

    V4ManifestReader rootReader =
        V4ManifestReaders.readRoot(rootPath, io, SNAPSHOT_ID, SEQUENCE_NUMBER, null);
    ManifestExpander expander = new ManifestExpander(rootReader, io, specsById);

    List<ManifestExpander.DataFileScanInfo> scanInfos =
        Lists.newArrayList(expander.planDataFiles());

    assertThat(scanInfos).hasSize(1);
    assertThat(scanInfos.get(0).deleteFiles()).isEmpty();
  }

  @Test
  public void testSequenceNumberFiltering() throws IOException {
    String dataFilePath = "s3://bucket/table/data/file1.parquet";
    GenericTrackedFile dataFile = createDataFile("file1.parquet", 1000L);
    dataFile.setLocation(dataFilePath);
    dataFile.setSequenceNumber(100L);

    GenericTrackedFile delete1 = createDeleteFile("delete1.parquet", 50L);
    delete1.setReferencedFile(dataFilePath);
    delete1.setSequenceNumber(95L);

    GenericTrackedFile delete2 = createDeleteFile("delete2.parquet", 30L);
    delete2.setReferencedFile(dataFilePath);
    delete2.setSequenceNumber(105L);

    String rootPath = writeRootManifest(dataFile, delete1, delete2);

    V4ManifestReader rootReader =
        V4ManifestReaders.readRoot(rootPath, io, SNAPSHOT_ID, SEQUENCE_NUMBER, null);
    ManifestExpander expander = new ManifestExpander(rootReader, io, specsById);

    List<ManifestExpander.DataFileScanInfo> scanInfos =
        Lists.newArrayList(expander.planDataFiles());

    assertThat(scanInfos).hasSize(1);
    assertThat(scanInfos.get(0).deleteFiles()).hasSize(1);
    assertThat(scanInfos.get(0).deleteFiles().get(0).location()).endsWith("delete1.parquet");
  }

  private GenericTrackedFile createDataFile(String filename, long recordCount) {
    GenericTrackedFile file = new GenericTrackedFile();
    file.setContentType(FileContent.DATA);
    file.setLocation("s3://bucket/table/data/" + filename);
    file.setFileFormat(FileFormat.PARQUET);
    file.setPartitionSpecId(0);
    file.setRecordCount(recordCount);
    file.setFileSizeInBytes(recordCount * 100);
    file.setStatus(TrackingInfo.Status.ADDED);
    file.setSnapshotId(SNAPSHOT_ID);
    return file;
  }

  private GenericTrackedFile createDeleteFile(String filename, long recordCount) {
    GenericTrackedFile file = new GenericTrackedFile();
    file.setContentType(FileContent.POSITION_DELETES);
    file.setLocation("s3://bucket/table/deletes/" + filename);
    file.setFileFormat(FileFormat.PARQUET);
    file.setPartitionSpecId(0);
    file.setRecordCount(recordCount);
    file.setFileSizeInBytes(recordCount * 50);
    file.setStatus(TrackingInfo.Status.ADDED);
    file.setSnapshotId(SNAPSHOT_ID);
    return file;
  }

  private GenericTrackedFile createManifestEntry(
      String manifestLocation, int fileCount, long totalRows) {
    GenericTrackedFile entry = new GenericTrackedFile();
    entry.setContentType(FileContent.DATA_MANIFEST);
    entry.setLocation(manifestLocation);
    entry.setFileFormat(FileFormat.PARQUET);
    entry.setPartitionSpecId(0);
    entry.setRecordCount(fileCount);
    entry.setFileSizeInBytes(10000L);

    entry.setAddedFilesCount(fileCount);
    entry.setExistingFilesCount(0);
    entry.setDeletedFilesCount(0);
    entry.setAddedRowsCount(totalRows);
    entry.setExistingRowsCount(0L);
    entry.setDeletedRowsCount(0L);
    entry.setMinSequenceNumber(SEQUENCE_NUMBER);

    entry.setStatus(TrackingInfo.Status.ADDED);
    entry.setSnapshotId(SNAPSHOT_ID);
    entry.setSequenceNumber(SEQUENCE_NUMBER);
    entry.setFileSequenceNumber(SEQUENCE_NUMBER);

    return entry;
  }

  private GenericTrackedFile createDeleteManifestEntry(
      String manifestLocation, int fileCount, long totalRows) {
    GenericTrackedFile entry = new GenericTrackedFile();
    entry.setContentType(FileContent.DELETE_MANIFEST);
    entry.setLocation(manifestLocation);
    entry.setFileFormat(FileFormat.PARQUET);
    entry.setPartitionSpecId(0);
    entry.setRecordCount(fileCount);
    entry.setFileSizeInBytes(5000L);

    entry.setAddedFilesCount(fileCount);
    entry.setExistingFilesCount(0);
    entry.setDeletedFilesCount(0);
    entry.setAddedRowsCount(totalRows);
    entry.setExistingRowsCount(0L);
    entry.setDeletedRowsCount(0L);
    entry.setMinSequenceNumber(SEQUENCE_NUMBER);

    entry.setStatus(TrackingInfo.Status.ADDED);
    entry.setSnapshotId(SNAPSHOT_ID);
    entry.setSequenceNumber(SEQUENCE_NUMBER);
    entry.setFileSequenceNumber(SEQUENCE_NUMBER);

    return entry;
  }

  private String writeRootManifest(GenericTrackedFile... entries) throws IOException {
    return writeManifest("root-manifest", entries);
  }

  private String writeLeafManifest(GenericTrackedFile... entries) throws IOException {
    return writeManifest("leaf-manifest", entries);
  }

  private String writeManifest(String prefix, GenericTrackedFile... entries) throws IOException {
    OutputFile outputFile = io.newOutputFile(prefix + "-" + System.nanoTime() + ".parquet");

    try (FileAppender<GenericTrackedFile> appender =
        InternalData.write(FileFormat.PARQUET, outputFile)
            .schema(new Schema(GenericTrackedFile.BASE_TYPE.fields()))
            .named("tracked_file")
            .build()) {
      for (GenericTrackedFile entry : entries) {
        appender.add(entry);
      }
    }

    return outputFile.location();
  }

  @Test
  public void testManifestDVFiltersPositions() throws IOException {
    GenericTrackedFile file1 = createDataFile("file1.parquet", 1000L);
    GenericTrackedFile file2 = createDataFile("file2.parquet", 2000L);
    GenericTrackedFile file3 = createDataFile("file3.parquet", 3000L);
    String leafManifestPath = writeLeafManifest(file1, file2, file3);

    GenericTrackedFile manifestEntry = createManifestEntry(leafManifestPath, 3, 6000L);

    GenericTrackedFile manifestDV = createManifestDV(leafManifestPath, new long[] {1});

    String rootPath = writeRootManifest(manifestEntry, manifestDV);

    V4ManifestReader rootReader =
        V4ManifestReaders.readRoot(rootPath, io, SNAPSHOT_ID, SEQUENCE_NUMBER, null);
    ManifestExpander expander = new ManifestExpander(rootReader, io, specsById);

    List<ManifestExpander.DataFileScanInfo> scanInfos =
        Lists.newArrayList(expander.planDataFiles());

    assertThat(scanInfos).hasSize(2);
    assertThat(scanInfos)
        .anyMatch(info -> info.dataFile().location().endsWith("file1.parquet"))
        .anyMatch(info -> info.dataFile().location().endsWith("file3.parquet"));
    assertThat(scanInfos).noneMatch(info -> info.dataFile().location().endsWith("file2.parquet"));
  }

  @Test
  public void testManifestWithoutDV() throws IOException {
    GenericTrackedFile file1 = createDataFile("file1.parquet", 1000L);
    String leafManifestPath = writeLeafManifest(file1);

    GenericTrackedFile manifestEntry = createManifestEntry(leafManifestPath, 1, 1000L);

    String rootPath = writeRootManifest(manifestEntry);

    V4ManifestReader rootReader =
        V4ManifestReaders.readRoot(rootPath, io, SNAPSHOT_ID, SEQUENCE_NUMBER, null);
    ManifestExpander expander = new ManifestExpander(rootReader, io, specsById);

    List<ManifestExpander.DataFileScanInfo> scanInfos =
        Lists.newArrayList(expander.planDataFiles());

    assertThat(scanInfos).hasSize(1);
    assertThat(scanInfos.get(0).dataFile().location()).endsWith("file1.parquet");
  }

  @Test
  public void testMultipleManifestDVsForSameManifest() throws IOException {
    String leafManifestPath = writeLeafManifest(createDataFile("file1.parquet", 1000L));

    GenericTrackedFile manifestEntry = createManifestEntry(leafManifestPath, 1, 1000L);
    GenericTrackedFile manifestDV1 = createManifestDV(leafManifestPath, new long[] {0});
    GenericTrackedFile manifestDV2 = createManifestDV(leafManifestPath, new long[] {1});

    String rootPath = writeRootManifest(manifestEntry, manifestDV1, manifestDV2);

    V4ManifestReader rootReader =
        V4ManifestReaders.readRoot(rootPath, io, SNAPSHOT_ID, SEQUENCE_NUMBER, null);
    ManifestExpander expander = new ManifestExpander(rootReader, io, specsById);

    assertThatThrownBy(() -> Lists.newArrayList(expander.allTrackedFiles()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Multiple MANIFEST_DVs found for manifest");
  }

  @Test
  public void testManifestDVDeletesMultiplePositions() throws IOException {
    GenericTrackedFile file1 = createDataFile("file1.parquet", 1000L);
    GenericTrackedFile file2 = createDataFile("file2.parquet", 2000L);
    GenericTrackedFile file3 = createDataFile("file3.parquet", 3000L);
    GenericTrackedFile file4 = createDataFile("file4.parquet", 4000L);
    String leafManifestPath = writeLeafManifest(file1, file2, file3, file4);

    GenericTrackedFile manifestEntry = createManifestEntry(leafManifestPath, 4, 10000L);

    GenericTrackedFile manifestDV = createManifestDV(leafManifestPath, new long[] {0, 2});

    String rootPath = writeRootManifest(manifestEntry, manifestDV);

    V4ManifestReader rootReader =
        V4ManifestReaders.readRoot(rootPath, io, SNAPSHOT_ID, SEQUENCE_NUMBER, null);
    ManifestExpander expander = new ManifestExpander(rootReader, io, specsById);

    List<ManifestExpander.DataFileScanInfo> scanInfos =
        Lists.newArrayList(expander.planDataFiles());

    assertThat(scanInfos).hasSize(2);
    assertThat(scanInfos)
        .anyMatch(info -> info.dataFile().location().endsWith("file2.parquet"))
        .anyMatch(info -> info.dataFile().location().endsWith("file4.parquet"));
  }

  private GenericTrackedFile createManifestDV(String targetManifest, long[] deletedPositions)
      throws IOException {
    RoaringBitmap bitmap = new RoaringBitmap();
    for (long pos : deletedPositions) {
      // positions are 0-based and should not exceed Integer.MAX_VALUE
      bitmap.add((int) pos);
    }

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    bitmap.serialize(new DataOutputStream(baos));
    byte[] serialized = baos.toByteArray();

    GenericTrackedFile dv = new GenericTrackedFile();
    dv.setContentType(FileContent.MANIFEST_DV);
    dv.setFileFormat(FileFormat.PUFFIN);
    dv.setPartitionSpecId(0);
    dv.setRecordCount(deletedPositions.length);
    dv.setReferencedFile(targetManifest);
    dv.setDeletionVectorInlineContent(ByteBuffer.wrap(serialized));
    dv.setStatus(TrackingInfo.Status.ADDED);
    dv.setSnapshotId(SNAPSHOT_ID);

    return dv;
  }

  @Test
  public void testParallelManifestReading() throws IOException {
    GenericTrackedFile file1 = createDataFile("file1.parquet", 1000L);
    String manifest1Path = writeLeafManifest(file1);

    GenericTrackedFile file2 = createDataFile("file2.parquet", 2000L);
    GenericTrackedFile file3 = createDataFile("file3.parquet", 3000L);
    String manifest2Path = writeLeafManifest(file2, file3);

    GenericTrackedFile manifestEntry1 = createManifestEntry(manifest1Path, 1, 1000L);
    GenericTrackedFile manifestEntry2 = createManifestEntry(manifest2Path, 2, 5000L);

    String rootPath = writeRootManifest(manifestEntry1, manifestEntry2);

    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      V4ManifestReader rootReader =
          V4ManifestReaders.readRoot(rootPath, io, SNAPSHOT_ID, SEQUENCE_NUMBER, null);
      ManifestExpander expander =
          new ManifestExpander(rootReader, io, specsById).planWith(executor);

      List<ManifestExpander.DataFileScanInfo> scanInfos =
          Lists.newArrayList(expander.planDataFiles());

      assertThat(scanInfos).hasSize(3);
      List<String> locations = Lists.newArrayList();
      for (ManifestExpander.DataFileScanInfo info : scanInfos) {
        locations.add(info.dataFile().location());
      }

      assertThat(locations)
          .anyMatch(loc -> loc.endsWith("file1.parquet"))
          .anyMatch(loc -> loc.endsWith("file2.parquet"))
          .anyMatch(loc -> loc.endsWith("file3.parquet"));
    } finally {
      executor.shutdown();
    }
  }
}
