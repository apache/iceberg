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

import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSnapshot extends TestBase {
  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(1, 2);
  }

  @TestTemplate
  public void testAppendFilesFromTable() {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    // collect data files from deserialization
    Iterable<DataFile> filesToAdd = table.currentSnapshot().addedDataFiles(table.io());

    table.newDelete().deleteFile(FILE_A).deleteFile(FILE_B).commit();

    Snapshot oldSnapshot = table.currentSnapshot();

    AppendFiles fastAppend = table.newFastAppend();
    for (DataFile file : filesToAdd) {
      fastAppend.appendFile(file);
    }

    Snapshot newSnapshot = fastAppend.apply();
    validateSnapshot(oldSnapshot, newSnapshot, FILE_A, FILE_B);
  }

  @TestTemplate
  public void testAppendFoundFiles() {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Iterable<DataFile> filesToAdd =
        FindFiles.in(table)
            .inPartition(table.spec(), StaticDataTask.Row.of(0))
            .inPartition(table.spec(), StaticDataTask.Row.of(1))
            .collect();

    table.newDelete().deleteFile(FILE_A).deleteFile(FILE_B).commit();

    Snapshot oldSnapshot = table.currentSnapshot();

    AppendFiles fastAppend = table.newFastAppend();
    for (DataFile file : filesToAdd) {
      fastAppend.appendFile(file);
    }

    Snapshot newSnapshot = fastAppend.apply();
    validateSnapshot(oldSnapshot, newSnapshot, FILE_A, FILE_B);
  }

  @TestTemplate
  public void testCachedDataFiles() {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    table.updateSpec().addField(Expressions.truncate("data", 2)).commit();

    DataFile secondSnapshotDataFile = newDataFile("data_bucket=8/data_trunc_2=aa");

    table.newFastAppend().appendFile(secondSnapshotDataFile).commit();

    DataFile thirdSnapshotDataFile = newDataFile("data_bucket=8/data_trunc_2=bb");

    table.newOverwrite().deleteFile(FILE_A).addFile(thirdSnapshotDataFile).commit();

    Snapshot thirdSnapshot = table.currentSnapshot();

    Iterable<DataFile> removedDataFiles = thirdSnapshot.removedDataFiles(FILE_IO);
    assertThat(removedDataFiles).as("Must have 1 removed data file").hasSize(1);

    DataFile removedDataFile = Iterables.getOnlyElement(removedDataFiles);
    assertThat(removedDataFile.path()).isEqualTo(FILE_A.path());
    assertThat(removedDataFile.specId()).isEqualTo(FILE_A.specId());
    assertThat(removedDataFile.partition()).isEqualTo(FILE_A.partition());

    Iterable<DataFile> addedDataFiles = thirdSnapshot.addedDataFiles(FILE_IO);
    assertThat(addedDataFiles).as("Must have 1 added data file").hasSize(1);

    DataFile addedDataFile = Iterables.getOnlyElement(addedDataFiles);
    assertThat(addedDataFile.path()).isEqualTo(thirdSnapshotDataFile.path());
    assertThat(addedDataFile.specId()).isEqualTo(thirdSnapshotDataFile.specId());
    assertThat(addedDataFile.partition()).isEqualTo(thirdSnapshotDataFile.partition());
  }

  @TestTemplate
  public void testCachedDeleteFiles() {
    assumeThat(formatVersion).as("Delete files only supported in V2").isGreaterThanOrEqualTo(2);

    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    table.updateSpec().addField(Expressions.truncate("data", 2)).commit();

    int specId = table.spec().specId();

    DataFile secondSnapshotDataFile = newDataFile("data_bucket=8/data_trunc_2=aa");
    DeleteFile secondSnapshotDeleteFile = newDeleteFile(specId, "data_bucket=8/data_trunc_2=aa");

    table
        .newRowDelta()
        .addRows(secondSnapshotDataFile)
        .addDeletes(secondSnapshotDeleteFile)
        .commit();

    DeleteFile thirdSnapshotDeleteFile = newDeleteFile(specId, "data_bucket=8/data_trunc_2=aa");

    ImmutableSet<DeleteFile> replacedDeleteFiles = ImmutableSet.of(secondSnapshotDeleteFile);
    ImmutableSet<DeleteFile> newDeleteFiles = ImmutableSet.of(thirdSnapshotDeleteFile);

    table
        .newRewrite()
        .rewriteFiles(ImmutableSet.of(), replacedDeleteFiles, ImmutableSet.of(), newDeleteFiles)
        .commit();

    Snapshot thirdSnapshot = table.currentSnapshot();

    Iterable<DeleteFile> removedDeleteFiles = thirdSnapshot.removedDeleteFiles(FILE_IO);
    assertThat(removedDeleteFiles).as("Must have 1 removed delete file").hasSize(1);

    DeleteFile removedDeleteFile = Iterables.getOnlyElement(removedDeleteFiles);
    assertThat(removedDeleteFile.path()).isEqualTo(secondSnapshotDeleteFile.path());
    assertThat(removedDeleteFile.specId()).isEqualTo(secondSnapshotDeleteFile.specId());
    assertThat(removedDeleteFile.partition()).isEqualTo(secondSnapshotDeleteFile.partition());

    Iterable<DeleteFile> addedDeleteFiles = thirdSnapshot.addedDeleteFiles(FILE_IO);
    assertThat(addedDeleteFiles).as("Must have 1 added delete file").hasSize(1);

    DeleteFile addedDeleteFile = Iterables.getOnlyElement(addedDeleteFiles);
    assertThat(addedDeleteFile.path()).isEqualTo(thirdSnapshotDeleteFile.path());
    assertThat(addedDeleteFile.specId()).isEqualTo(thirdSnapshotDeleteFile.specId());
    assertThat(addedDeleteFile.partition()).isEqualTo(thirdSnapshotDeleteFile.partition());
  }

  @TestTemplate
  public void testSequenceNumbersInAddedDataFiles() {
    long expectedSequenceNumber = 0L;
    if (formatVersion >= 2) {
      expectedSequenceNumber = 1L;
    }

    runAddedDataFileSequenceNumberTest(expectedSequenceNumber);

    if (formatVersion >= 2) {
      ++expectedSequenceNumber;
    }

    runAddedDataFileSequenceNumberTest(expectedSequenceNumber);
  }

  private void runAddedDataFileSequenceNumberTest(long expectedSequenceNumber) {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Snapshot snapshot = table.currentSnapshot();
    Iterable<DataFile> addedDataFiles = snapshot.addedDataFiles(table.io());

    assertThat(snapshot.sequenceNumber())
        .as("Sequence number mismatch in Snapshot")
        .isEqualTo(expectedSequenceNumber);

    for (DataFile df : addedDataFiles) {
      assertThat(df.dataSequenceNumber().longValue())
          .as("Data sequence number mismatch")
          .isEqualTo(expectedSequenceNumber);
      assertThat(df.fileSequenceNumber().longValue())
          .as("File sequence number mismatch")
          .isEqualTo(expectedSequenceNumber);
    }
  }

  @TestTemplate
  public void testSequenceNumbersInRemovedDataFiles() {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    long expectedSnapshotSequenceNumber = 0L;
    if (formatVersion >= 2) {
      expectedSnapshotSequenceNumber = 2L;
    }

    long expectedFileSequenceNumber = 0L;
    if (formatVersion >= 2) {
      expectedFileSequenceNumber = 1L;
    }

    runRemovedDataFileSequenceNumberTest(
        FILE_A, expectedSnapshotSequenceNumber, expectedFileSequenceNumber);

    if (formatVersion >= 2) {
      ++expectedSnapshotSequenceNumber;
    }

    runRemovedDataFileSequenceNumberTest(
        FILE_B, expectedSnapshotSequenceNumber, expectedFileSequenceNumber);
  }

  private void runRemovedDataFileSequenceNumberTest(
      DataFile fileToRemove, long expectedSnapshotSequenceNumber, long expectedFileSequenceNumber) {
    table.newDelete().deleteFile(fileToRemove).commit();

    Snapshot snapshot = table.currentSnapshot();
    Iterable<DataFile> removedDataFiles = snapshot.removedDataFiles(table.io());
    assertThat(removedDataFiles).as("Must have 1 removed data file").hasSize(1);

    DataFile removedDataFile = Iterables.getOnlyElement(removedDataFiles);

    assertThat(snapshot.sequenceNumber())
        .as("Sequence number mismatch in Snapshot")
        .isEqualTo(expectedSnapshotSequenceNumber);
    assertThat(removedDataFile.dataSequenceNumber().longValue())
        .as("Data sequence number mismatch")
        .isEqualTo(expectedFileSequenceNumber);
    assertThat(removedDataFile.fileSequenceNumber().longValue())
        .as("File sequence number mismatch")
        .isEqualTo(expectedFileSequenceNumber);
  }

  @TestTemplate
  public void testSequenceNumbersInAddedDeleteFiles() {
    assumeThat(formatVersion).as("Delete files only supported in V2").isGreaterThanOrEqualTo(2);

    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    int specId = table.spec().specId();

    runAddedDeleteFileSequenceNumberTest(newDeleteFile(specId, "data_bucket=8"), 2);

    runAddedDeleteFileSequenceNumberTest(newDeleteFile(specId, "data_bucket=28"), 3);
  }

  private void runAddedDeleteFileSequenceNumberTest(
      DeleteFile deleteFileToAdd, long expectedSequenceNumber) {
    table.newRowDelta().addDeletes(deleteFileToAdd).commit();

    Snapshot snapshot = table.currentSnapshot();
    Iterable<DeleteFile> addedDeleteFiles = snapshot.addedDeleteFiles(table.io());
    assertThat(addedDeleteFiles).as("Must have 1 added delete file").hasSize(1);

    DeleteFile addedDeleteFile = Iterables.getOnlyElement(addedDeleteFiles);

    assertThat(snapshot.sequenceNumber())
        .as("Sequence number mismatch in Snapshot")
        .isEqualTo(expectedSequenceNumber);
    assertThat(addedDeleteFile.dataSequenceNumber().longValue())
        .as("Data sequence number mismatch")
        .isEqualTo(expectedSequenceNumber);
    assertThat(addedDeleteFile.fileSequenceNumber().longValue())
        .as("File sequence number mismatch")
        .isEqualTo(expectedSequenceNumber);
  }
}
