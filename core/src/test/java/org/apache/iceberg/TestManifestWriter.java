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

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.ManifestEntry.Status;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Assumptions;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestManifestWriter extends TableTestBase {
  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] {1, 2};
  }

  public TestManifestWriter(int formatVersion) {
    super(formatVersion);
  }

  private static final int FILE_SIZE_CHECK_ROWS_DIVISOR = 250;
  private static final long SMALL_FILE_SIZE = 10L;

  @Test
  public void testManifestStats() throws IOException {
    ManifestFile manifest =
        writeManifest(
            "manifest.avro",
            manifestEntry(Status.ADDED, null, newFile(10)),
            manifestEntry(Status.ADDED, null, newFile(20)),
            manifestEntry(Status.ADDED, null, newFile(5)),
            manifestEntry(Status.ADDED, null, newFile(5)),
            manifestEntry(Status.EXISTING, null, newFile(15)),
            manifestEntry(Status.EXISTING, null, newFile(10)),
            manifestEntry(Status.EXISTING, null, newFile(1)),
            manifestEntry(Status.DELETED, null, newFile(5)),
            manifestEntry(Status.DELETED, null, newFile(2)));

    Assert.assertTrue("Added files should be present", manifest.hasAddedFiles());
    Assert.assertEquals("Added files count should match", 4, (int) manifest.addedFilesCount());
    Assert.assertEquals("Added rows count should match", 40L, (long) manifest.addedRowsCount());

    Assert.assertTrue("Existing files should be present", manifest.hasExistingFiles());
    Assert.assertEquals(
        "Existing files count should match", 3, (int) manifest.existingFilesCount());
    Assert.assertEquals(
        "Existing rows count should match", 26L, (long) manifest.existingRowsCount());

    Assert.assertTrue("Deleted files should be present", manifest.hasDeletedFiles());
    Assert.assertEquals("Deleted files count should match", 2, (int) manifest.deletedFilesCount());
    Assert.assertEquals("Deleted rows count should match", 7L, (long) manifest.deletedRowsCount());
  }

  @Test
  public void testManifestPartitionStats() throws IOException {
    ManifestFile manifest =
        writeManifest(
            "manifest.avro",
            manifestEntry(Status.ADDED, null, newFile(10, TestHelpers.Row.of(1))),
            manifestEntry(Status.EXISTING, null, newFile(15, TestHelpers.Row.of(2))),
            manifestEntry(Status.DELETED, null, newFile(2, TestHelpers.Row.of(3))));

    List<ManifestFile.PartitionFieldSummary> partitions = manifest.partitions();
    Assert.assertEquals("Partition field summaries count should match", 1, partitions.size());
    ManifestFile.PartitionFieldSummary partitionFieldSummary = partitions.get(0);
    Assert.assertFalse("contains_null should be false", partitionFieldSummary.containsNull());
    Assert.assertFalse("contains_nan should be false", partitionFieldSummary.containsNaN());
    Assert.assertEquals(
        "Lower bound should match",
        Integer.valueOf(1),
        Conversions.fromByteBuffer(Types.IntegerType.get(), partitionFieldSummary.lowerBound()));
    Assert.assertEquals(
        "Upper bound should match",
        Integer.valueOf(3),
        Conversions.fromByteBuffer(Types.IntegerType.get(), partitionFieldSummary.upperBound()));
  }

  @Test
  public void testWriteManifestWithSequenceNumber() throws IOException {
    Assume.assumeTrue("sequence number is only valid for format version > 1", formatVersion > 1);
    File manifestFile = temp.newFile("manifest.avro");
    Assert.assertTrue(manifestFile.delete());
    OutputFile outputFile = table.ops().io().newOutputFile(manifestFile.getCanonicalPath());
    ManifestWriter<DataFile> writer =
        ManifestFiles.write(formatVersion, table.spec(), outputFile, 1L);
    writer.add(newFile(10, TestHelpers.Row.of(1)), 1000L);
    writer.close();
    ManifestFile manifest = writer.toManifestFile();
    Assert.assertEquals("Manifest should have no sequence number", -1L, manifest.sequenceNumber());
    ManifestReader<DataFile> manifestReader = ManifestFiles.read(manifest, table.io());
    for (ManifestEntry<DataFile> entry : manifestReader.entries()) {
      Assert.assertEquals(
          "Custom data sequence number should be used for all manifest entries",
          1000L,
          (long) entry.dataSequenceNumber());
      Assert.assertEquals(
          "File sequence number must be unassigned",
          ManifestWriter.UNASSIGNED_SEQ,
          entry.fileSequenceNumber().longValue());
    }
  }

  @Test
  public void testCommitManifestWithExplicitDataSequenceNumber() throws IOException {
    Assume.assumeTrue("Sequence numbers are valid for format version > 1", formatVersion > 1);

    DataFile file1 = newFile(50);
    DataFile file2 = newFile(50);

    long dataSequenceNumber = 25L;

    ManifestFile manifest =
        writeManifest(
            "manifest.avro",
            manifestEntry(Status.ADDED, null, dataSequenceNumber, null, file1),
            manifestEntry(Status.ADDED, null, dataSequenceNumber, null, file2));

    Assert.assertEquals(
        "Manifest should have no sequence number before commit",
        ManifestWriter.UNASSIGNED_SEQ,
        manifest.sequenceNumber());

    table.newFastAppend().appendManifest(manifest).commit();

    long commitSnapshotId = table.currentSnapshot().snapshotId();

    ManifestFile committedManifest = table.currentSnapshot().dataManifests(table.io()).get(0);

    Assert.assertEquals(
        "Committed manifest sequence number must be correct",
        1L,
        committedManifest.sequenceNumber());

    Assert.assertEquals(
        "Committed manifest min sequence number must be correct",
        dataSequenceNumber,
        committedManifest.minSequenceNumber());

    validateManifest(
        committedManifest,
        dataSeqs(dataSequenceNumber, dataSequenceNumber),
        fileSeqs(1L, 1L),
        ids(commitSnapshotId, commitSnapshotId),
        files(file1, file2),
        statuses(Status.ADDED, Status.ADDED));
  }

  @Test
  public void testCommitManifestWithExistingEntriesWithoutFileSequenceNumber() throws IOException {
    Assume.assumeTrue("Sequence numbers are valid for format version > 1", formatVersion > 1);

    DataFile file1 = newFile(50);
    DataFile file2 = newFile(50);

    table.newFastAppend().appendFile(file1).appendFile(file2).commit();

    Snapshot appendSnapshot = table.currentSnapshot();
    long appendSequenceNumber = appendSnapshot.sequenceNumber();
    long appendSnapshotId = appendSnapshot.snapshotId();

    ManifestFile originalManifest = appendSnapshot.dataManifests(table.io()).get(0);

    ManifestFile newManifest =
        writeManifest(
            "manifest.avro",
            manifestEntry(Status.EXISTING, appendSnapshotId, appendSequenceNumber, null, file1),
            manifestEntry(Status.EXISTING, appendSnapshotId, appendSequenceNumber, null, file2));

    Assert.assertEquals(
        "Manifest should have no sequence number before commit",
        ManifestWriter.UNASSIGNED_SEQ,
        newManifest.sequenceNumber());

    table.rewriteManifests().deleteManifest(originalManifest).addManifest(newManifest).commit();

    Snapshot rewriteSnapshot = table.currentSnapshot();

    ManifestFile committedManifest = table.currentSnapshot().dataManifests(table.io()).get(0);

    Assert.assertEquals(
        "Committed manifest sequence number must be correct",
        rewriteSnapshot.sequenceNumber(),
        committedManifest.sequenceNumber());

    Assert.assertEquals(
        "Committed manifest min sequence number must be correct",
        appendSequenceNumber,
        committedManifest.minSequenceNumber());

    validateManifest(
        committedManifest,
        dataSeqs(appendSequenceNumber, appendSequenceNumber),
        fileSeqs(null, null),
        ids(appendSnapshotId, appendSnapshotId),
        files(file1, file2),
        statuses(Status.EXISTING, Status.EXISTING));
  }

  @Test
  public void testRollingManifestWriterNoRecords() throws IOException {
    RollingManifestWriter<DataFile> writer = newRollingWriteManifest(SMALL_FILE_SIZE);

    writer.close();
    Assertions.assertThat(writer.toManifestFiles()).isEmpty();

    writer.close();
    Assertions.assertThat(writer.toManifestFiles()).isEmpty();
  }

  @Test
  public void testRollingDeleteManifestWriterNoRecords() throws IOException {
    Assumptions.assumeThat(formatVersion).isGreaterThan(1);
    RollingManifestWriter<DeleteFile> writer = newRollingWriteDeleteManifest(SMALL_FILE_SIZE);

    writer.close();
    Assertions.assertThat(writer.toManifestFiles()).isEmpty();

    writer.close();
    Assertions.assertThat(writer.toManifestFiles()).isEmpty();
  }

  @Test
  public void testRollingManifestWriterSplitFiles() throws IOException {
    RollingManifestWriter<DataFile> writer = newRollingWriteManifest(SMALL_FILE_SIZE);

    int[] addedFileCounts = new int[3];
    int[] existingFileCounts = new int[3];
    int[] deletedFileCounts = new int[3];
    long[] addedRowCounts = new long[3];
    long[] existingRowCounts = new long[3];
    long[] deletedRowCounts = new long[3];

    for (int i = 0; i < FILE_SIZE_CHECK_ROWS_DIVISOR * 3; i++) {
      int type = i % 3;
      int fileIndex = i / FILE_SIZE_CHECK_ROWS_DIVISOR;
      if (type == 0) {
        writer.add(newFile(i));
        addedFileCounts[fileIndex] += 1;
        addedRowCounts[fileIndex] += i;
      } else if (type == 1) {
        writer.existing(newFile(i), 1, 1, null);
        existingFileCounts[fileIndex] += 1;
        existingRowCounts[fileIndex] += i;
      } else {
        writer.delete(newFile(i), 1, null);
        deletedFileCounts[fileIndex] += 1;
        deletedRowCounts[fileIndex] += i;
      }
    }

    writer.close();
    List<ManifestFile> manifestFiles = writer.toManifestFiles();
    Assertions.assertThat(manifestFiles.size()).isEqualTo(3);

    checkManifests(
        manifestFiles,
        addedFileCounts,
        existingFileCounts,
        deletedFileCounts,
        addedRowCounts,
        existingRowCounts,
        deletedRowCounts);

    writer.close();
    manifestFiles = writer.toManifestFiles();
    Assertions.assertThat(manifestFiles.size()).isEqualTo(3);

    checkManifests(
        manifestFiles,
        addedFileCounts,
        existingFileCounts,
        deletedFileCounts,
        addedRowCounts,
        existingRowCounts,
        deletedRowCounts);
  }

  @Test
  public void testRollingDeleteManifestWriterSplitFiles() throws IOException {
    Assumptions.assumeThat(formatVersion).isGreaterThan(1);
    RollingManifestWriter<DeleteFile> writer = newRollingWriteDeleteManifest(SMALL_FILE_SIZE);

    int[] addedFileCounts = new int[3];
    int[] existingFileCounts = new int[3];
    int[] deletedFileCounts = new int[3];
    long[] addedRowCounts = new long[3];
    long[] existingRowCounts = new long[3];
    long[] deletedRowCounts = new long[3];
    for (int i = 0; i < 3 * FILE_SIZE_CHECK_ROWS_DIVISOR; i++) {
      int type = i % 3;
      int fileIndex = i / FILE_SIZE_CHECK_ROWS_DIVISOR;
      if (type == 0) {
        writer.add(newPosDeleteFile(i));
        addedFileCounts[fileIndex] += 1;
        addedRowCounts[fileIndex] += i;
      } else if (type == 1) {
        writer.existing(newPosDeleteFile(i), 1, 1, null);
        existingFileCounts[fileIndex] += 1;
        existingRowCounts[fileIndex] += i;
      } else {
        writer.delete(newPosDeleteFile(i), 1, null);
        deletedFileCounts[fileIndex] += 1;
        deletedRowCounts[fileIndex] += i;
      }
    }

    writer.close();
    List<ManifestFile> manifestFiles = writer.toManifestFiles();
    Assertions.assertThat(manifestFiles.size()).isEqualTo(3);

    checkManifests(
        manifestFiles,
        addedFileCounts,
        existingFileCounts,
        deletedFileCounts,
        addedRowCounts,
        existingRowCounts,
        deletedRowCounts);

    writer.close();
    manifestFiles = writer.toManifestFiles();
    Assertions.assertThat(manifestFiles.size()).isEqualTo(3);

    checkManifests(
        manifestFiles,
        addedFileCounts,
        existingFileCounts,
        deletedFileCounts,
        addedRowCounts,
        existingRowCounts,
        deletedRowCounts);
  }

  private void checkManifests(
      List<ManifestFile> manifests,
      int[] addedFileCounts,
      int[] existingFileCounts,
      int[] deletedFileCounts,
      long[] addedRowCounts,
      long[] existingRowCounts,
      long[] deletedRowCounts) {
    for (int i = 0; i < manifests.size(); i++) {
      ManifestFile manifest = manifests.get(i);

      Assertions.assertThat(manifest.hasAddedFiles()).isTrue();
      Assertions.assertThat(manifest.addedFilesCount()).isEqualTo(addedFileCounts[i]);
      Assertions.assertThat(manifest.addedRowsCount()).isEqualTo(addedRowCounts[i]);

      Assertions.assertThat(manifest.hasExistingFiles()).isTrue();
      Assertions.assertThat(manifest.existingFilesCount()).isEqualTo(existingFileCounts[i]);
      Assertions.assertThat(manifest.existingRowsCount()).isEqualTo(existingRowCounts[i]);

      Assertions.assertThat(manifest.hasDeletedFiles()).isTrue();
      Assertions.assertThat(manifest.deletedFilesCount()).isEqualTo(deletedFileCounts[i]);
      Assertions.assertThat(manifest.deletedRowsCount()).isEqualTo(deletedRowCounts[i]);
    }
  }

  @Test
  public void testWriteManifestWithCompression() throws IOException {
    validateManifestCompressionCodec(
        compressionCodec -> writeManifest(SNAPSHOT_ID, compressionCodec, FILE_A),
        manifest -> ManifestFiles.read(manifest, FILE_IO));
  }

  @Test
  public void testWriteDeleteManifestWithCompression() throws IOException {
    Assume.assumeTrue("delete files are only written for format version > 1", formatVersion > 1);
    validateManifestCompressionCodec(
        compressionCodec ->
            writeDeleteManifest(formatVersion, SNAPSHOT_ID, compressionCodec, FILE_A_DELETES),
        manifest -> ManifestFiles.readDeleteManifest(manifest, FILE_IO, null));
  }

  private DataFile newFile(long recordCount) {
    return newFile(recordCount, null);
  }

  private DataFile newFile(long recordCount, StructLike partition) {
    String fileName = UUID.randomUUID().toString();
    DataFiles.Builder builder =
        DataFiles.builder(SPEC)
            .withPath("data_bucket=0/" + fileName + ".parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(recordCount);
    if (partition != null) {
      builder.withPartition(partition);
    }
    return builder.build();
  }

  private DeleteFile newPosDeleteFile(long recordCount) {
    return FileMetadata.deleteFileBuilder(SPEC)
        .ofPositionDeletes()
        .withPath("/path/to/delete-" + UUID.randomUUID() + ".parquet")
        .withFileSizeInBytes(10)
        .withRecordCount(recordCount)
        .build();
  }

  private RollingManifestWriter<DataFile> newRollingWriteManifest(long targetFileSize) {
    return new RollingManifestWriter<>(
        () -> {
          OutputFile newManifestFile = newManifestFile();
          return ManifestFiles.write(formatVersion, SPEC, newManifestFile, null);
        },
        targetFileSize);
  }

  private RollingManifestWriter<DeleteFile> newRollingWriteDeleteManifest(long targetFileSize) {
    return new RollingManifestWriter<>(
        () -> {
          OutputFile newManifestFile = newManifestFile();
          return ManifestFiles.writeDeleteManifest(formatVersion, SPEC, newManifestFile, null);
        },
        targetFileSize);
  }

  private OutputFile newManifestFile() {
    try {
      return Files.localOutput(FileFormat.AVRO.addExtension(temp.newFile().toString()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  <F extends ContentFile<F>> void validateManifestCompressionCodec(
      CheckedFunction<String, ManifestFile> createManifestFunc,
      CheckedFunction<ManifestFile, ManifestReader<F>> manifestReaderFunc)
      throws IOException {
    for (Map.Entry<String, String> entry : CODEC_METADATA_MAPPING.entrySet()) {
      String codec = entry.getKey();
      String expectedCodecValue = entry.getValue();

      ManifestFile manifest = createManifestFunc.apply(codec);

      try (ManifestReader<F> reader = manifestReaderFunc.apply(manifest)) {
        Map<String, String> metadata = reader.metadata();
        Assert.assertEquals(
            "Manifest file codec value must match",
            expectedCodecValue,
            metadata.get(AVRO_CODEC_KEY));
      }
    }
  }
}
