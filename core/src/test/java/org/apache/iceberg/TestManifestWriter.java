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
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.ManifestEntry.Status;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestManifestWriter extends TestBase {
  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(1, 2);
  }

  private static final int FILE_SIZE_CHECK_ROWS_DIVISOR = 250;
  private static final long SMALL_FILE_SIZE = 10L;

  @TestTemplate
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

    assertThat(manifest.hasAddedFiles()).isTrue();
    assertThat(manifest.addedFilesCount()).isEqualTo(4);
    assertThat(manifest.addedRowsCount()).isEqualTo(40);
    assertThat(manifest.hasExistingFiles()).isTrue();
    assertThat(manifest.existingFilesCount()).isEqualTo(3);
    assertThat(manifest.existingRowsCount()).isEqualTo(26);
    assertThat(manifest.hasDeletedFiles()).isTrue();
    assertThat(manifest.deletedFilesCount()).isEqualTo(2);
    assertThat(manifest.deletedRowsCount()).isEqualTo(7);
  }

  @TestTemplate
  public void testManifestPartitionStats() throws IOException {
    ManifestFile manifest =
        writeManifest(
            "manifest.avro",
            manifestEntry(Status.ADDED, null, newFile(10, TestHelpers.Row.of(1))),
            manifestEntry(Status.EXISTING, null, newFile(15, TestHelpers.Row.of(2))),
            manifestEntry(Status.DELETED, null, newFile(2, TestHelpers.Row.of(3))));

    List<ManifestFile.PartitionFieldSummary> partitions = manifest.partitions();
    assertThat(partitions).hasSize(1);
    ManifestFile.PartitionFieldSummary partitionFieldSummary = partitions.get(0);
    assertThat(partitionFieldSummary.containsNull()).isFalse();
    assertThat(partitionFieldSummary.containsNaN()).isFalse();
    assertThat(
            (Integer)
                Conversions.fromByteBuffer(
                    Types.IntegerType.get(), partitionFieldSummary.lowerBound()))
        .isEqualTo(1);
    assertThat(
            (Integer)
                Conversions.fromByteBuffer(
                    Types.IntegerType.get(), partitionFieldSummary.upperBound()))
        .isEqualTo(3);
  }

  @TestTemplate
  public void testWriteManifestWithSequenceNumber() throws IOException {
    assumeThat(formatVersion).isGreaterThan(1);
    File manifestFile = File.createTempFile("manifest", ".avro", temp.toFile());
    assertThat(manifestFile.delete()).isTrue();
    OutputFile outputFile = table.ops().io().newOutputFile(manifestFile.getCanonicalPath());
    ManifestWriter<DataFile> writer =
        ManifestFiles.write(formatVersion, table.spec(), outputFile, 1L);
    writer.add(newFile(10, TestHelpers.Row.of(1)), 1000L);
    writer.close();
    ManifestFile manifest = writer.toManifestFile();
    assertThat(manifest.sequenceNumber()).isEqualTo(-1);
    ManifestReader<DataFile> manifestReader = ManifestFiles.read(manifest, table.io());
    for (ManifestEntry<DataFile> entry : manifestReader.entries()) {
      assertThat(entry.dataSequenceNumber()).isEqualTo(1000);
      assertThat(entry.fileSequenceNumber()).isEqualTo(ManifestWriter.UNASSIGNED_SEQ);
    }
  }

  @TestTemplate
  public void testCommitManifestWithExplicitDataSequenceNumber() throws IOException {
    assumeThat(formatVersion).isGreaterThan(1);

    DataFile file1 = newFile(50);
    DataFile file2 = newFile(50);

    long dataSequenceNumber = 25L;

    ManifestFile manifest =
        writeManifest(
            "manifest.avro",
            manifestEntry(Status.ADDED, null, dataSequenceNumber, null, file1),
            manifestEntry(Status.ADDED, null, dataSequenceNumber, null, file2));

    assertThat(manifest.sequenceNumber()).isEqualTo(ManifestWriter.UNASSIGNED_SEQ);

    table.newFastAppend().appendManifest(manifest).commit();

    long commitSnapshotId = table.currentSnapshot().snapshotId();

    ManifestFile committedManifest = table.currentSnapshot().dataManifests(table.io()).get(0);

    assertThat(committedManifest.sequenceNumber()).isEqualTo(1);

    assertThat(committedManifest.minSequenceNumber()).isEqualTo(dataSequenceNumber);

    validateManifest(
        committedManifest,
        dataSeqs(dataSequenceNumber, dataSequenceNumber),
        fileSeqs(1L, 1L),
        ids(commitSnapshotId, commitSnapshotId),
        files(file1, file2),
        statuses(Status.ADDED, Status.ADDED));
  }

  @TestTemplate
  public void testCommitManifestWithExistingEntriesWithoutFileSequenceNumber() throws IOException {
    assumeThat(formatVersion).isGreaterThan(1);

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

    assertThat(newManifest.sequenceNumber()).isEqualTo(ManifestWriter.UNASSIGNED_SEQ);

    table.rewriteManifests().deleteManifest(originalManifest).addManifest(newManifest).commit();

    Snapshot rewriteSnapshot = table.currentSnapshot();

    ManifestFile committedManifest = table.currentSnapshot().dataManifests(table.io()).get(0);

    assertThat(committedManifest.sequenceNumber()).isEqualTo(rewriteSnapshot.sequenceNumber());

    assertThat(committedManifest.minSequenceNumber()).isEqualTo(appendSequenceNumber);

    validateManifest(
        committedManifest,
        dataSeqs(appendSequenceNumber, appendSequenceNumber),
        fileSeqs(null, null),
        ids(appendSnapshotId, appendSnapshotId),
        files(file1, file2),
        statuses(Status.EXISTING, Status.EXISTING));
  }

  @TestTemplate
  public void testRollingManifestWriterNoRecords() throws IOException {
    RollingManifestWriter<DataFile> writer = newRollingWriteManifest(SMALL_FILE_SIZE);

    writer.close();
    assertThat(writer.toManifestFiles()).isEmpty();

    writer.close();
    assertThat(writer.toManifestFiles()).isEmpty();
  }

  @TestTemplate
  public void testRollingDeleteManifestWriterNoRecords() throws IOException {
    assumeThat(formatVersion).isGreaterThan(1);
    RollingManifestWriter<DeleteFile> writer = newRollingWriteDeleteManifest(SMALL_FILE_SIZE);

    writer.close();
    assertThat(writer.toManifestFiles()).isEmpty();

    writer.close();
    assertThat(writer.toManifestFiles()).isEmpty();
  }

  @TestTemplate
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
    assertThat(manifestFiles).hasSize(3);

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
    assertThat(manifestFiles).hasSize(3);

    checkManifests(
        manifestFiles,
        addedFileCounts,
        existingFileCounts,
        deletedFileCounts,
        addedRowCounts,
        existingRowCounts,
        deletedRowCounts);
  }

  @TestTemplate
  public void testRollingDeleteManifestWriterSplitFiles() throws IOException {
    assumeThat(formatVersion).isGreaterThan(1);
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
    assertThat(manifestFiles).hasSize(3);

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
    assertThat(manifestFiles).hasSize(3);

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

      assertThat(manifest.hasAddedFiles()).isTrue();
      assertThat(manifest.addedFilesCount()).isEqualTo(addedFileCounts[i]);
      assertThat(manifest.addedRowsCount()).isEqualTo(addedRowCounts[i]);

      assertThat(manifest.hasExistingFiles()).isTrue();
      assertThat(manifest.existingFilesCount()).isEqualTo(existingFileCounts[i]);
      assertThat(manifest.existingRowsCount()).isEqualTo(existingRowCounts[i]);

      assertThat(manifest.hasDeletedFiles()).isTrue();
      assertThat(manifest.deletedFilesCount()).isEqualTo(deletedFileCounts[i]);
      assertThat(manifest.deletedRowsCount()).isEqualTo(deletedRowCounts[i]);
    }
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
      return Files.localOutput(
          FileFormat.AVRO.addExtension(
              File.createTempFile("manifest", null, temp.toFile()).toString()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
