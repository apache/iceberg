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
package org.apache.iceberg.io;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public abstract class TestPositionDeltaWriters<T> extends WriterTestBase<T> {

  @Parameterized.Parameters(name = "FileFormat={0}")
  public static Object[] parameters() {
    return new Object[][] {
      new Object[] {FileFormat.AVRO},
      new Object[] {FileFormat.ORC},
      new Object[] {FileFormat.PARQUET}
    };
  }

  private static final int TABLE_FORMAT_VERSION = 2;
  private static final long TARGET_FILE_SIZE = 128L * 1024 * 1024;

  private final FileFormat fileFormat;
  private OutputFileFactory fileFactory = null;

  public TestPositionDeltaWriters(FileFormat fileFormat) {
    super(TABLE_FORMAT_VERSION);
    this.fileFormat = fileFormat;
  }

  protected abstract StructLikeSet toSet(Iterable<T> records);

  protected FileFormat format() {
    return fileFormat;
  }

  @Override
  @Before
  public void setupTable() throws Exception {
    this.tableDir = temp.newFolder();
    Assert.assertTrue(tableDir.delete()); // created during table creation

    this.metadataDir = new File(tableDir, "metadata");
    this.table = create(SCHEMA, PartitionSpec.unpartitioned());
    this.fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(fileFormat).build();
  }

  @Test
  public void testPositionDeltaWithOneDataWriter() throws IOException {
    FileWriterFactory<T> writerFactory = newWriterFactory(table.schema());

    ClusteredDataWriter<T> dataWriter =
        new ClusteredDataWriter<>(writerFactory, fileFactory, table.io(), TARGET_FILE_SIZE);
    ClusteredPositionDeleteWriter<T> deleteWriter =
        new ClusteredPositionDeleteWriter<>(
            writerFactory, fileFactory, table.io(), TARGET_FILE_SIZE);
    PositionDeltaWriter<T> deltaWriter = new BasePositionDeltaWriter<>(dataWriter, deleteWriter);

    deltaWriter.insert(toRow(1, "insert"), table.spec(), null);
    deltaWriter.update(toRow(2, "update"), table.spec(), null);
    deltaWriter.close();

    WriteResult result = deltaWriter.result();
    DataFile[] dataFiles = result.dataFiles();
    DeleteFile[] deleteFiles = result.deleteFiles();
    CharSequence[] referencedDataFiles = result.referencedDataFiles();

    Assert.assertEquals("Must be 1 data files", 1, dataFiles.length);
    Assert.assertEquals("Must be no delete files", 0, deleteFiles.length);
    Assert.assertEquals("Must not reference data files", 0, referencedDataFiles.length);
  }

  @Test
  public void testPositionDeltaInsertOnly() throws IOException {
    FileWriterFactory<T> writerFactory = newWriterFactory(table.schema());

    ClusteredDataWriter<T> insertWriter =
        new ClusteredDataWriter<>(writerFactory, fileFactory, table.io(), TARGET_FILE_SIZE);
    ClusteredDataWriter<T> updateWriter =
        new ClusteredDataWriter<>(writerFactory, fileFactory, table.io(), TARGET_FILE_SIZE);
    ClusteredPositionDeleteWriter<T> deleteWriter =
        new ClusteredPositionDeleteWriter<>(
            writerFactory, fileFactory, table.io(), TARGET_FILE_SIZE);
    PositionDeltaWriter<T> deltaWriter =
        new BasePositionDeltaWriter<>(insertWriter, updateWriter, deleteWriter);

    deltaWriter.insert(toRow(1, "aaa"), table.spec(), null);
    deltaWriter.close();

    WriteResult result = deltaWriter.result();
    DataFile[] dataFiles = result.dataFiles();
    DeleteFile[] deleteFiles = result.deleteFiles();
    CharSequence[] referencedDataFiles = result.referencedDataFiles();

    Assert.assertEquals("Must be 1 data files", 1, dataFiles.length);
    Assert.assertEquals("Must be no delete files", 0, deleteFiles.length);
    Assert.assertEquals("Must not reference data files", 0, referencedDataFiles.length);

    RowDelta rowDelta = table.newRowDelta();
    for (DataFile dataFile : dataFiles) {
      rowDelta.addRows(dataFile);
    }
    rowDelta.commit();

    List<T> expectedRows = ImmutableList.of(toRow(1, "aaa"));
    Assert.assertEquals("Records should match", toSet(expectedRows), actualRowSet("*"));
  }

  @Test
  public void testPositionDeltaDeleteOnly() throws IOException {
    FileWriterFactory<T> writerFactory = newWriterFactory(table.schema());

    // add an unpartitioned data file
    ImmutableList<T> rows1 = ImmutableList.of(toRow(1, "aaa"), toRow(2, "aaa"), toRow(11, "aaa"));
    DataFile dataFile1 = writeData(writerFactory, fileFactory, rows1, table.spec(), null);
    table.newFastAppend().appendFile(dataFile1).commit();

    // partition by data
    table.updateSpec().addField(Expressions.ref("data")).commit();

    // add a data file partitioned by data
    ImmutableList<T> rows2 = ImmutableList.of(toRow(3, "bbb"), toRow(4, "bbb"));
    DataFile dataFile2 =
        writeData(
            writerFactory, fileFactory, rows2, table.spec(), partitionKey(table.spec(), "bbb"));
    table.newFastAppend().appendFile(dataFile2).commit();

    PartitionSpec unpartitionedSpec = table.specs().get(0);
    PartitionSpec partitionedSpec = table.specs().get(1);

    ClusteredDataWriter<T> insertWriter =
        new ClusteredDataWriter<>(writerFactory, fileFactory, table.io(), TARGET_FILE_SIZE);
    ClusteredDataWriter<T> updateWriter =
        new ClusteredDataWriter<>(writerFactory, fileFactory, table.io(), TARGET_FILE_SIZE);
    ClusteredPositionDeleteWriter<T> deleteWriter =
        new ClusteredPositionDeleteWriter<>(
            writerFactory, fileFactory, table.io(), TARGET_FILE_SIZE);
    PositionDeltaWriter<T> deltaWriter =
        new BasePositionDeltaWriter<>(insertWriter, updateWriter, deleteWriter);

    deltaWriter.delete(dataFile1.path(), 2L, unpartitionedSpec, null);
    deltaWriter.delete(dataFile2.path(), 1L, partitionedSpec, partitionKey(partitionedSpec, "bbb"));

    deltaWriter.close();

    WriteResult result = deltaWriter.result();
    DataFile[] dataFiles = result.dataFiles();
    DeleteFile[] deleteFiles = result.deleteFiles();
    CharSequence[] referencedDataFiles = result.referencedDataFiles();

    Assert.assertEquals("Must be 0 data files", 0, dataFiles.length);
    Assert.assertEquals("Must be 2 delete files", 2, deleteFiles.length);
    Assert.assertEquals("Must reference 2 data files", 2, referencedDataFiles.length);

    RowDelta rowDelta = table.newRowDelta();
    for (DeleteFile deleteFile : deleteFiles) {
      rowDelta.addDeletes(deleteFile);
    }
    rowDelta.commit();

    List<T> expectedRows = ImmutableList.of(toRow(1, "aaa"), toRow(2, "aaa"), toRow(3, "bbb"));
    Assert.assertEquals("Records should match", toSet(expectedRows), actualRowSet("*"));
  }

  @Test
  public void testPositionDeltaMultipleSpecs() throws IOException {
    FileWriterFactory<T> writerFactory = newWriterFactory(table.schema());

    // add an unpartitioned data file
    ImmutableList<T> rows1 = ImmutableList.of(toRow(1, "aaa"), toRow(2, "aaa"), toRow(11, "aaa"));
    DataFile dataFile1 = writeData(writerFactory, fileFactory, rows1, table.spec(), null);
    table.newFastAppend().appendFile(dataFile1).commit();

    // partition by data
    table.updateSpec().addField(Expressions.ref("data")).commit();

    // add a data file partitioned by data
    ImmutableList<T> rows2 = ImmutableList.of(toRow(3, "bbb"), toRow(4, "bbb"));
    DataFile dataFile2 =
        writeData(
            writerFactory, fileFactory, rows2, table.spec(), partitionKey(table.spec(), "bbb"));
    table.newFastAppend().appendFile(dataFile2).commit();

    PartitionSpec unpartitionedSpec = table.specs().get(0);
    PartitionSpec partitionedSpec = table.specs().get(1);

    ClusteredDataWriter<T> insertWriter =
        new ClusteredDataWriter<>(writerFactory, fileFactory, table.io(), TARGET_FILE_SIZE);
    ClusteredDataWriter<T> updateWriter =
        new ClusteredDataWriter<>(writerFactory, fileFactory, table.io(), TARGET_FILE_SIZE);
    ClusteredPositionDeleteWriter<T> deleteWriter =
        new ClusteredPositionDeleteWriter<>(
            writerFactory, fileFactory, table.io(), TARGET_FILE_SIZE);
    PositionDeltaWriter<T> deltaWriter =
        new BasePositionDeltaWriter<>(insertWriter, updateWriter, deleteWriter);

    deltaWriter.delete(dataFile1.path(), 2L, unpartitionedSpec, null);
    deltaWriter.delete(dataFile2.path(), 1L, partitionedSpec, partitionKey(partitionedSpec, "bbb"));
    deltaWriter.insert(toRow(10, "ccc"), partitionedSpec, partitionKey(partitionedSpec, "ccc"));

    deltaWriter.close();

    WriteResult result = deltaWriter.result();
    DataFile[] dataFiles = result.dataFiles();
    DeleteFile[] deleteFiles = result.deleteFiles();
    CharSequence[] referencedDataFiles = result.referencedDataFiles();

    Assert.assertEquals("Must be 1 data files", 1, dataFiles.length);
    Assert.assertEquals("Must be 2 delete files", 2, deleteFiles.length);
    Assert.assertEquals("Must reference 2 data files", 2, referencedDataFiles.length);

    RowDelta rowDelta = table.newRowDelta();
    for (DataFile dataFile : dataFiles) {
      rowDelta.addRows(dataFile);
    }
    for (DeleteFile deleteFile : deleteFiles) {
      rowDelta.addDeletes(deleteFile);
    }
    rowDelta.commit();

    List<T> expectedRows =
        ImmutableList.of(toRow(1, "aaa"), toRow(2, "aaa"), toRow(3, "bbb"), toRow(10, "ccc"));
    Assert.assertEquals("Records should match", toSet(expectedRows), actualRowSet("*"));
  }
}
