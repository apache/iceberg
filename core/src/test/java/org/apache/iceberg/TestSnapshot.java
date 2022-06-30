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

import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestSnapshot extends TableTestBase {
  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] { 1, 2 };
  }

  public TestSnapshot(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testAppendFilesFromTable() {
    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

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

  @Test
  public void testAppendFoundFiles() {
    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Iterable<DataFile> filesToAdd = FindFiles.in(table)
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

  @Test
  public void testCachedDataFiles() {
    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    table.updateSpec()
        .addField(Expressions.truncate("data", 2))
        .commit();

    DataFile secondSnapshotDataFile = newDataFile("data_bucket=8/data_trunc_2=aa");

    table.newFastAppend()
        .appendFile(secondSnapshotDataFile)
        .commit();

    DataFile thirdSnapshotDataFile = newDataFile("data_bucket=8/data_trunc_2=bb");

    table.newOverwrite()
        .deleteFile(FILE_A)
        .addFile(thirdSnapshotDataFile)
        .commit();

    Snapshot thirdSnapshot = table.currentSnapshot();

    Iterable<DataFile> removedDataFiles = thirdSnapshot.removedDataFiles(FILE_IO);
    Assert.assertEquals("Must have 1 removed data file", 1, Iterables.size(removedDataFiles));

    DataFile removedDataFile = Iterables.getOnlyElement(removedDataFiles);
    Assert.assertEquals("Path must match", FILE_A.path(), removedDataFile.path());
    Assert.assertEquals("Spec ID must match", FILE_A.specId(), removedDataFile.specId());
    Assert.assertEquals("Partition must match", FILE_A.partition(), removedDataFile.partition());

    Iterable<DataFile> addedDataFiles = thirdSnapshot.addedDataFiles(FILE_IO);
    Assert.assertEquals("Must have 1 added data file", 1, Iterables.size(addedDataFiles));

    DataFile addedDataFile = Iterables.getOnlyElement(addedDataFiles);
    Assert.assertEquals("Path must match", thirdSnapshotDataFile.path(), addedDataFile.path());
    Assert.assertEquals("Spec ID must match", thirdSnapshotDataFile.specId(), addedDataFile.specId());
    Assert.assertEquals("Partition must match", thirdSnapshotDataFile.partition(), addedDataFile.partition());
  }

  @Test
  public void testCachedDeleteFiles() {
    Assume.assumeTrue("Delete files only supported in V2", formatVersion >= 2);

    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    table.updateSpec()
        .addField(Expressions.truncate("data", 2))
        .commit();

    int specId = table.spec().specId();

    DataFile secondSnapshotDataFile = newDataFile("data_bucket=8/data_trunc_2=aa");
    DeleteFile secondSnapshotDeleteFile = newDeleteFile(specId, "data_bucket=8/data_trunc_2=aa");

    table.newRowDelta()
        .addRows(secondSnapshotDataFile)
        .addDeletes(secondSnapshotDeleteFile)
        .commit();

    DeleteFile thirdSnapshotDeleteFile = newDeleteFile(specId, "data_bucket=8/data_trunc_2=aa");

    ImmutableSet<DeleteFile> replacedDeleteFiles = ImmutableSet.of(secondSnapshotDeleteFile);
    ImmutableSet<DeleteFile> newDeleteFiles = ImmutableSet.of(thirdSnapshotDeleteFile);

    table.newRewrite()
        .rewriteFiles(ImmutableSet.of(), replacedDeleteFiles, ImmutableSet.of(), newDeleteFiles)
        .commit();

    Snapshot thirdSnapshot = table.currentSnapshot();

    Iterable<DeleteFile> removedDeleteFiles = thirdSnapshot.removedDeleteFiles(FILE_IO);
    Assert.assertEquals("Must have 1 removed delete file", 1, Iterables.size(removedDeleteFiles));

    DeleteFile removedDeleteFile = Iterables.getOnlyElement(removedDeleteFiles);
    Assert.assertEquals("Path must match", secondSnapshotDeleteFile.path(), removedDeleteFile.path());
    Assert.assertEquals("Spec ID must match", secondSnapshotDeleteFile.specId(), removedDeleteFile.specId());
    Assert.assertEquals("Partition must match", secondSnapshotDeleteFile.partition(), removedDeleteFile.partition());

    Iterable<DeleteFile> addedDeleteFiles = thirdSnapshot.addedDeleteFiles(FILE_IO);
    Assert.assertEquals("Must have 1 added delete file", 1, Iterables.size(addedDeleteFiles));

    DeleteFile addedDeleteFile = Iterables.getOnlyElement(addedDeleteFiles);
    Assert.assertEquals("Path must match", thirdSnapshotDeleteFile.path(), addedDeleteFile.path());
    Assert.assertEquals("Spec ID must match", thirdSnapshotDeleteFile.specId(), addedDeleteFile.specId());
    Assert.assertEquals("Partition must match", thirdSnapshotDeleteFile.partition(), addedDeleteFile.partition());
  }
}
