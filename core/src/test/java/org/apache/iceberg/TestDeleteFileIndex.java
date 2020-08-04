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
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.StructLikeWrapper;
import org.junit.Assert;
import org.junit.Test;

public class TestDeleteFileIndex extends TableTestBase {
  public TestDeleteFileIndex() {
    super(2 /* table format version */);
  }

  static final DeleteFile FILE_A_POS_1 = FileMetadata.deleteFileBuilder(SPEC)
      .ofPositionDeletes()
      .withPath("/path/to/data-a-pos-deletes.parquet")
      .withFileSizeInBytes(10)
      .withPartition(FILE_A.partition())
      .withRecordCount(1)
      .build();
  static final DeleteFile FILE_A_POS_2 = FILE_A_POS_1.copy();

  static final DeleteFile FILE_A_EQ_1 = FileMetadata.deleteFileBuilder(SPEC)
      .ofEqualityDeletes()
      .withPath("/path/to/data-a-eq-deletes.parquet")
      .withFileSizeInBytes(10)
      .withPartition(FILE_A.partition())
      .withRecordCount(1)
      .build();
  static final DeleteFile FILE_A_EQ_2 = FILE_A_EQ_1.copy();
  static final DeleteFile[] DELETE_FILES = new DeleteFile[] { FILE_A_POS_1, FILE_A_EQ_1, FILE_A_POS_2, FILE_A_EQ_2 };

  static final DataFile UNPARTITIONED_FILE = DataFiles.builder(PartitionSpec.unpartitioned())
      .withPath("/path/to/data-a.parquet")
      .withFileSizeInBytes(10)
      .withRecordCount(1)
      .build();

  static final DeleteFile UNPARTITIONED_POS_DELETES = FileMetadata.deleteFileBuilder(SPEC)
      .ofPositionDeletes()
      .withPath("/path/to/data-a-pos-deletes.parquet")
      .withFileSizeInBytes(10)
      .withRecordCount(1)
      .build();

  static final DeleteFile UNPARTITIONED_EQ_DELETES = FileMetadata.deleteFileBuilder(SPEC)
      .ofEqualityDeletes()
      .withPath("/path/to/data-a-eq-deletes.parquet")
      .withFileSizeInBytes(10)
      .withRecordCount(1)
      .build();

  @Test
  public void testUnpartitionedDeletes() {
    StructLike emptyPartition = new PartitionData(PartitionSpec.unpartitioned().partitionType());

    DeleteFileIndex index = new DeleteFileIndex(ImmutableMap.of(
        Pair.of(1, StructLikeWrapper.wrap(emptyPartition)),
        Pair.of(new long[] { 3, 5, 5, 6 }, DELETE_FILES)));

    Assert.assertArrayEquals("All deletes should apply to seq 0",
        DELETE_FILES, index.forDataFile(1, 0, UNPARTITIONED_FILE));
    Assert.assertArrayEquals("All deletes should apply to seq 3",
        DELETE_FILES, index.forDataFile(1, 3, UNPARTITIONED_FILE));
    Assert.assertArrayEquals("Last 3 deletes should apply to seq 4",
        Arrays.copyOfRange(DELETE_FILES, 1, 4), index.forDataFile(1, 4, UNPARTITIONED_FILE));
    Assert.assertArrayEquals("Last 3 deletes should apply to seq 5",
        Arrays.copyOfRange(DELETE_FILES, 1, 4), index.forDataFile(1, 5, UNPARTITIONED_FILE));
    Assert.assertArrayEquals("Last delete should apply to seq 6",
        Arrays.copyOfRange(DELETE_FILES, 3, 4), index.forDataFile(1, 6, UNPARTITIONED_FILE));
    Assert.assertArrayEquals("No deletes should apply to seq 7",
        new DataFile[0], index.forDataFile(1, 7, UNPARTITIONED_FILE));
    Assert.assertArrayEquals("No deletes should apply to seq 10",
        new DataFile[0], index.forDataFile(1, 10, UNPARTITIONED_FILE));

    Assert.assertEquals("No deletes should apply to partitioned files, partition not in index",
        0, index.forDataFile(1, 0, FILE_B).length);
  }

  @Test
  public void testPartitionedDeleteIndex() {
    DeleteFileIndex index = new DeleteFileIndex(ImmutableMap.of(
        Pair.of(1, StructLikeWrapper.wrap(FILE_A.partition())),
        Pair.of(new long[] { 3, 5, 5, 6 }, DELETE_FILES),
        Pair.of(1, StructLikeWrapper.wrap(FILE_C.partition())),
        Pair.of(new long[0], new DeleteFile[0])));

    Assert.assertArrayEquals("All deletes should apply to seq 0",
        DELETE_FILES, index.forDataFile(1, 0, FILE_A));
    Assert.assertArrayEquals("All deletes should apply to seq 3",
        DELETE_FILES, index.forDataFile(1, 3, FILE_A));
    Assert.assertArrayEquals("Last 3 deletes should apply to seq 4",
        Arrays.copyOfRange(DELETE_FILES, 1, 4), index.forDataFile(1, 4, FILE_A));
    Assert.assertArrayEquals("Last 3 deletes should apply to seq 5",
        Arrays.copyOfRange(DELETE_FILES, 1, 4), index.forDataFile(1, 5, FILE_A));
    Assert.assertArrayEquals("Last delete should apply to seq 6",
        Arrays.copyOfRange(DELETE_FILES, 3, 4), index.forDataFile(1, 6, FILE_A));
    Assert.assertArrayEquals("No deletes should apply to seq 7",
        new DataFile[0], index.forDataFile(1, 7, FILE_A));
    Assert.assertArrayEquals("No deletes should apply to seq 10",
        new DataFile[0], index.forDataFile(1, 10, FILE_A));

    Assert.assertEquals("No deletes should apply to FILE_B, partition not in index",
        0, index.forDataFile(1, 0, FILE_B).length);

    Assert.assertEquals("No deletes should apply to FILE_C, no indexed delete files",
        0, index.forDataFile(1, 0, FILE_C).length);

    Assert.assertEquals("No deletes should apply to FILE_A with a different specId",
        0, index.forDataFile(2, 0, FILE_A).length);
  }

  @Test
  public void testUnpartitionedTableScan() throws IOException {
    File location = temp.newFolder();
    Assert.assertTrue(location.delete());

    Table unpartitioned = TestTables.create(location, "unpartitioned", SCHEMA, PartitionSpec.unpartitioned(), 2);

    unpartitioned.newAppend()
        .appendFile(FILE_A)
        .commit();

    // add a delete file
    unpartitioned.newRowDelta()
        .addDeletes(UNPARTITIONED_POS_DELETES)
        .commit();

    List<FileScanTask> tasks = Lists.newArrayList(unpartitioned.newScan().planFiles().iterator());
    Assert.assertEquals("Should have one task", 1, tasks.size());

    FileScanTask task = tasks.get(0);
    Assert.assertEquals("Should have the correct data file path",
        UNPARTITIONED_FILE.path(), task.file().path());
    Assert.assertEquals("Should have one associated delete file",
        1, task.deletes().size());
    Assert.assertEquals("Should have expected delete file",
        UNPARTITIONED_POS_DELETES.path(), task.deletes().get(0).path());

    // add a second delete file
    unpartitioned.newRowDelta()
        .addDeletes(UNPARTITIONED_EQ_DELETES)
        .commit();

    tasks = Lists.newArrayList(unpartitioned.newScan().planFiles().iterator());
    task = tasks.get(0);
    Assert.assertEquals("Should have the correct data file path",
        UNPARTITIONED_FILE.path(), task.file().path());
    Assert.assertEquals("Should have two associated delete files",
        2, task.deletes().size());
    Assert.assertEquals("Should have expected delete files",
        Lists.newArrayList(UNPARTITIONED_POS_DELETES.path(), UNPARTITIONED_EQ_DELETES.path()),
        Lists.transform(task.deletes(), ContentFile::path));
  }

  @Test
  public void testUnpartitionedTableSequenceNumbers() throws IOException {
    File location = temp.newFolder();
    Assert.assertTrue(location.delete());

    Table unpartitioned = TestTables.create(location, "unpartitioned", SCHEMA, PartitionSpec.unpartitioned(), 2);

    // add data, pos deletes, and eq deletes in the same sequence number
    // the position deletes will be applied to the data file, but the equality deletes will not
    unpartitioned.newRowDelta()
        .addRows(FILE_A)
        .addDeletes(UNPARTITIONED_POS_DELETES)
        .addDeletes(UNPARTITIONED_EQ_DELETES)
        .commit();

    Assert.assertEquals("Table should contain 2 delete files",
        2, (long) unpartitioned.currentSnapshot().deleteManifests().get(0).addedFilesCount());

    List<FileScanTask> tasks = Lists.newArrayList(unpartitioned.newScan().planFiles().iterator());
    Assert.assertEquals("Should have one task", 1, tasks.size());

    FileScanTask task = tasks.get(0);
    Assert.assertEquals("Should have the correct data file path",
        UNPARTITIONED_FILE.path(), task.file().path());
    Assert.assertEquals("Should have one associated delete file",
        1, task.deletes().size());
    Assert.assertEquals("Should have only pos delete file",
        UNPARTITIONED_POS_DELETES.path(), task.deletes().get(0).path());
  }
}
