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
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public abstract class TestPartitioningWriters<T> extends WriterTestBase<T> {

  @Parameterized.Parameters(name = "FileFormat={0}")
  public static Object[] parameters() {
    return new Object[][] {
      new Object[] {FileFormat.AVRO},
      new Object[] {FileFormat.PARQUET},
      new Object[] {FileFormat.ORC},
    };
  }

  private static final int TABLE_FORMAT_VERSION = 2;
  private static final long TARGET_FILE_SIZE = 128L * 1024 * 1024;

  private final FileFormat fileFormat;
  private OutputFileFactory fileFactory = null;

  public TestPartitioningWriters(FileFormat fileFormat) {
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
  public void testClusteredDataWriterNoRecords() throws IOException {
    FileWriterFactory<T> writerFactory = newWriterFactory(table.schema());
    ClusteredDataWriter<T> writer =
        new ClusteredDataWriter<>(writerFactory, fileFactory, table.io(), TARGET_FILE_SIZE);

    writer.close();
    Assert.assertEquals("Must be no data files", 0, writer.result().dataFiles().size());

    writer.close();
    Assert.assertEquals("Must be no data files", 0, writer.result().dataFiles().size());
  }

  @Test
  public void testClusteredDataWriterMultiplePartitions() throws IOException {
    table.updateSpec().addField(Expressions.ref("data")).commit();

    FileWriterFactory<T> writerFactory = newWriterFactory(table.schema());
    ClusteredDataWriter<T> writer =
        new ClusteredDataWriter<>(writerFactory, fileFactory, table.io(), TARGET_FILE_SIZE);

    PartitionSpec spec = table.spec();

    writer.write(toRow(1, "aaa"), spec, partitionKey(spec, "aaa"));
    writer.write(toRow(2, "aaa"), spec, partitionKey(spec, "aaa"));
    writer.write(toRow(3, "bbb"), spec, partitionKey(spec, "bbb"));
    writer.write(toRow(4, "bbb"), spec, partitionKey(spec, "bbb"));
    writer.write(toRow(5, "ccc"), spec, partitionKey(spec, "ccc"));

    writer.close();

    DataWriteResult result = writer.result();
    Assert.assertEquals("Must be 3 data files", 3, result.dataFiles().size());

    RowDelta rowDelta = table.newRowDelta();
    result.dataFiles().forEach(rowDelta::addRows);
    rowDelta.commit();

    List<T> expectedRows =
        ImmutableList.of(
            toRow(1, "aaa"), toRow(2, "aaa"), toRow(3, "bbb"), toRow(4, "bbb"), toRow(5, "ccc"));
    Assert.assertEquals("Records should match", toSet(expectedRows), actualRowSet("*"));
  }

  @Test
  public void testClusteredDataWriterOutOfOrderPartitions() throws IOException {
    table.updateSpec().addField(Expressions.ref("data")).commit();

    FileWriterFactory<T> writerFactory = newWriterFactory(table.schema());
    ClusteredDataWriter<T> writer =
        new ClusteredDataWriter<>(writerFactory, fileFactory, table.io(), TARGET_FILE_SIZE);

    PartitionSpec spec = table.spec();

    writer.write(toRow(1, "aaa"), spec, partitionKey(spec, "aaa"));
    writer.write(toRow(2, "aaa"), spec, partitionKey(spec, "aaa"));
    writer.write(toRow(3, "bbb"), spec, partitionKey(spec, "bbb"));
    writer.write(toRow(4, "bbb"), spec, partitionKey(spec, "bbb"));
    writer.write(toRow(5, "ccc"), spec, partitionKey(spec, "ccc"));

    AssertHelpers.assertThrows(
        "Should fail to write out of order partitions",
        IllegalStateException.class,
        "Encountered records that belong to already closed files",
        () -> writer.write(toRow(6, "aaa"), spec, partitionKey(spec, "aaa")));

    writer.close();
  }

  @Test
  public void testClusteredEqualityDeleteWriterNoRecords() throws IOException {
    List<Integer> equalityFieldIds = ImmutableList.of(table.schema().findField("id").fieldId());
    Schema equalityDeleteRowSchema = table.schema().select("id");
    FileWriterFactory<T> writerFactory =
        newWriterFactory(table.schema(), equalityFieldIds, equalityDeleteRowSchema);
    ClusteredEqualityDeleteWriter<T> writer =
        new ClusteredEqualityDeleteWriter<>(
            writerFactory, fileFactory, table.io(), TARGET_FILE_SIZE);

    writer.close();
    Assert.assertEquals(0, writer.result().deleteFiles().size());
    Assert.assertEquals(0, writer.result().referencedDataFiles().size());
    Assert.assertFalse(writer.result().referencesDataFiles());

    writer.close();
    Assert.assertEquals(0, writer.result().deleteFiles().size());
    Assert.assertEquals(0, writer.result().referencedDataFiles().size());
    Assert.assertFalse(writer.result().referencesDataFiles());
  }

  @Test
  public void testClusteredEqualityDeleteWriterMultipleSpecs() throws IOException {
    List<Integer> equalityFieldIds = ImmutableList.of(table.schema().findField("id").fieldId());
    Schema equalityDeleteRowSchema = table.schema().select("id");
    FileWriterFactory<T> writerFactory =
        newWriterFactory(table.schema(), equalityFieldIds, equalityDeleteRowSchema);

    // add an unpartitioned data file
    ImmutableList<T> rows1 = ImmutableList.of(toRow(1, "aaa"), toRow(2, "aaa"), toRow(11, "aaa"));
    DataFile dataFile1 = writeData(writerFactory, fileFactory, rows1, table.spec(), null);
    table.newFastAppend().appendFile(dataFile1).commit();

    // partition by bucket
    table.updateSpec().addField(Expressions.bucket("data", 16)).commit();

    // add a data file partitioned by bucket
    ImmutableList<T> rows2 = ImmutableList.of(toRow(3, "bbb"), toRow(4, "bbb"), toRow(12, "bbb"));
    DataFile dataFile2 =
        writeData(
            writerFactory, fileFactory, rows2, table.spec(), partitionKey(table.spec(), "bbb"));
    table.newFastAppend().appendFile(dataFile2).commit();

    // partition by data
    table
        .updateSpec()
        .removeField(Expressions.bucket("data", 16))
        .addField(Expressions.ref("data"))
        .commit();

    // add a data file partitioned by data
    ImmutableList<T> rows3 = ImmutableList.of(toRow(5, "ccc"), toRow(13, "ccc"));
    DataFile dataFile3 =
        writeData(
            writerFactory, fileFactory, rows3, table.spec(), partitionKey(table.spec(), "ccc"));
    table.newFastAppend().appendFile(dataFile3).commit();

    ClusteredEqualityDeleteWriter<T> writer =
        new ClusteredEqualityDeleteWriter<>(
            writerFactory, fileFactory, table.io(), TARGET_FILE_SIZE);

    PartitionSpec unpartitionedSpec = table.specs().get(0);
    PartitionSpec bucketSpec = table.specs().get(1);
    PartitionSpec identitySpec = table.specs().get(2);

    writer.write(toRow(1, "aaa"), unpartitionedSpec, null);
    writer.write(toRow(2, "aaa"), unpartitionedSpec, null);
    writer.write(toRow(3, "bbb"), bucketSpec, partitionKey(bucketSpec, "bbb"));
    writer.write(toRow(4, "bbb"), bucketSpec, partitionKey(bucketSpec, "bbb"));
    writer.write(toRow(5, "ccc"), identitySpec, partitionKey(identitySpec, "ccc"));

    writer.close();

    DeleteWriteResult result = writer.result();
    Assert.assertEquals("Must be 3 delete files", 3, result.deleteFiles().size());
    Assert.assertEquals(
        "Must not reference data files", 0, writer.result().referencedDataFiles().size());
    Assert.assertFalse("Must not reference data files", writer.result().referencesDataFiles());

    RowDelta rowDelta = table.newRowDelta();
    result.deleteFiles().forEach(rowDelta::addDeletes);
    rowDelta.commit();

    List<T> expectedRows = ImmutableList.of(toRow(11, "aaa"), toRow(12, "bbb"), toRow(13, "ccc"));
    Assert.assertEquals("Records should match", toSet(expectedRows), actualRowSet("*"));
  }

  @Test
  public void testClusteredEqualityDeleteWriterOutOfOrderSpecsAndPartitions() throws IOException {
    List<Integer> equalityFieldIds = ImmutableList.of(table.schema().findField("id").fieldId());
    Schema equalityDeleteRowSchema = table.schema().select("id");
    FileWriterFactory<T> writerFactory =
        newWriterFactory(table.schema(), equalityFieldIds, equalityDeleteRowSchema);

    table.updateSpec().addField(Expressions.bucket("data", 16)).commit();

    table
        .updateSpec()
        .removeField(Expressions.bucket("data", 16))
        .addField(Expressions.ref("data"))
        .commit();

    ClusteredEqualityDeleteWriter<T> writer =
        new ClusteredEqualityDeleteWriter<>(
            writerFactory, fileFactory, table.io(), TARGET_FILE_SIZE);

    PartitionSpec unpartitionedSpec = table.specs().get(0);
    PartitionSpec bucketSpec = table.specs().get(1);
    PartitionSpec identitySpec = table.specs().get(2);

    writer.write(toRow(1, "aaa"), unpartitionedSpec, null);
    writer.write(toRow(2, "aaa"), unpartitionedSpec, null);
    writer.write(toRow(3, "bbb"), bucketSpec, partitionKey(bucketSpec, "bbb"));
    writer.write(toRow(4, "bbb"), bucketSpec, partitionKey(bucketSpec, "bbb"));
    writer.write(toRow(5, "ccc"), identitySpec, partitionKey(identitySpec, "ccc"));
    writer.write(toRow(6, "ddd"), identitySpec, partitionKey(identitySpec, "ddd"));

    AssertHelpers.assertThrows(
        "Should fail to write out of order partitions",
        IllegalStateException.class,
        "Encountered records that belong to already closed files",
        () -> writer.write(toRow(7, "ccc"), identitySpec, partitionKey(identitySpec, "ccc")));

    AssertHelpers.assertThrows(
        "Should fail to write out of order specs",
        IllegalStateException.class,
        "Encountered records that belong to already closed files",
        () -> writer.write(toRow(7, "aaa"), unpartitionedSpec, null));

    writer.close();
  }

  @Test
  public void testClusteredPositionDeleteWriterNoRecords() throws IOException {
    FileWriterFactory<T> writerFactory = newWriterFactory(table.schema());
    ClusteredPositionDeleteWriter<T> writer =
        new ClusteredPositionDeleteWriter<>(
            writerFactory, fileFactory, table.io(), TARGET_FILE_SIZE);

    writer.close();
    Assert.assertEquals(0, writer.result().deleteFiles().size());
    Assert.assertEquals(0, writer.result().referencedDataFiles().size());
    Assert.assertFalse(writer.result().referencesDataFiles());

    writer.close();
    Assert.assertEquals(0, writer.result().deleteFiles().size());
    Assert.assertEquals(0, writer.result().referencedDataFiles().size());
    Assert.assertFalse(writer.result().referencesDataFiles());
  }

  @Test
  public void testClusteredPositionDeleteWriterMultipleSpecs() throws IOException {
    FileWriterFactory<T> writerFactory = newWriterFactory(table.schema());

    // add an unpartitioned data file
    ImmutableList<T> rows1 = ImmutableList.of(toRow(1, "aaa"), toRow(2, "aaa"), toRow(11, "aaa"));
    DataFile dataFile1 = writeData(writerFactory, fileFactory, rows1, table.spec(), null);
    table.newFastAppend().appendFile(dataFile1).commit();

    // partition by bucket
    table.updateSpec().addField(Expressions.bucket("data", 16)).commit();

    // add a data file partitioned by bucket
    ImmutableList<T> rows2 = ImmutableList.of(toRow(3, "bbb"), toRow(4, "bbb"), toRow(12, "bbb"));
    DataFile dataFile2 =
        writeData(
            writerFactory, fileFactory, rows2, table.spec(), partitionKey(table.spec(), "bbb"));
    table.newFastAppend().appendFile(dataFile2).commit();

    // partition by data
    table
        .updateSpec()
        .removeField(Expressions.bucket("data", 16))
        .addField(Expressions.ref("data"))
        .commit();

    // add a data file partitioned by data
    ImmutableList<T> rows3 = ImmutableList.of(toRow(5, "ccc"), toRow(13, "ccc"));
    DataFile dataFile3 =
        writeData(
            writerFactory, fileFactory, rows3, table.spec(), partitionKey(table.spec(), "ccc"));
    table.newFastAppend().appendFile(dataFile3).commit();

    ClusteredPositionDeleteWriter<T> writer =
        new ClusteredPositionDeleteWriter<>(
            writerFactory, fileFactory, table.io(), TARGET_FILE_SIZE);

    PartitionSpec unpartitionedSpec = table.specs().get(0);
    PartitionSpec bucketSpec = table.specs().get(1);
    PartitionSpec identitySpec = table.specs().get(2);

    writer.write(positionDelete(dataFile1.path(), 0L, null), unpartitionedSpec, null);
    writer.write(positionDelete(dataFile1.path(), 1L, null), unpartitionedSpec, null);
    writer.write(
        positionDelete(dataFile2.path(), 0L, null), bucketSpec, partitionKey(bucketSpec, "bbb"));
    writer.write(
        positionDelete(dataFile2.path(), 1L, null), bucketSpec, partitionKey(bucketSpec, "bbb"));
    writer.write(
        positionDelete(dataFile3.path(), 0L, null),
        identitySpec,
        partitionKey(identitySpec, "ccc"));

    writer.close();

    DeleteWriteResult result = writer.result();
    Assert.assertEquals("Must be 3 delete files", 3, result.deleteFiles().size());
    Assert.assertEquals(
        "Must reference 3 data files", 3, writer.result().referencedDataFiles().size());
    Assert.assertTrue("Must reference data files", writer.result().referencesDataFiles());

    RowDelta rowDelta = table.newRowDelta();
    result.deleteFiles().forEach(rowDelta::addDeletes);
    rowDelta.commit();

    List<T> expectedRows = ImmutableList.of(toRow(11, "aaa"), toRow(12, "bbb"), toRow(13, "ccc"));
    Assert.assertEquals("Records should match", toSet(expectedRows), actualRowSet("*"));
  }

  @Test
  public void testClusteredPositionDeleteWriterOutOfOrderSpecsAndPartitions() throws IOException {
    FileWriterFactory<T> writerFactory = newWriterFactory(table.schema());

    table.updateSpec().addField(Expressions.bucket("data", 16)).commit();

    table
        .updateSpec()
        .removeField(Expressions.bucket("data", 16))
        .addField(Expressions.ref("data"))
        .commit();

    ClusteredPositionDeleteWriter<T> writer =
        new ClusteredPositionDeleteWriter<>(
            writerFactory, fileFactory, table.io(), TARGET_FILE_SIZE);

    PartitionSpec unpartitionedSpec = table.specs().get(0);
    PartitionSpec bucketSpec = table.specs().get(1);
    PartitionSpec identitySpec = table.specs().get(2);

    writer.write(positionDelete("file-1.parquet", 0L, null), unpartitionedSpec, null);
    writer.write(positionDelete("file-1.parquet", 1L, null), unpartitionedSpec, null);
    writer.write(
        positionDelete("file-2.parquet", 0L, null), bucketSpec, partitionKey(bucketSpec, "bbb"));
    writer.write(
        positionDelete("file-2.parquet", 1L, null), bucketSpec, partitionKey(bucketSpec, "bbb"));
    writer.write(
        positionDelete("file-3.parquet", 0L, null),
        identitySpec,
        partitionKey(identitySpec, "ccc"));
    writer.write(
        positionDelete("file-4.parquet", 0L, null),
        identitySpec,
        partitionKey(identitySpec, "ddd"));

    AssertHelpers.assertThrows(
        "Should fail to write out of order partitions",
        IllegalStateException.class,
        "Encountered records that belong to already closed files",
        () -> {
          PositionDelete<T> positionDelete = positionDelete("file-5.parquet", 1L, null);
          writer.write(positionDelete, identitySpec, partitionKey(identitySpec, "ccc"));
        });

    AssertHelpers.assertThrows(
        "Should fail to write out of order specs",
        IllegalStateException.class,
        "Encountered records that belong to already closed files",
        () -> {
          PositionDelete<T> positionDelete = positionDelete("file-1.parquet", 3L, null);
          writer.write(positionDelete, unpartitionedSpec, null);
        });

    writer.close();
  }

  @Test
  public void testFanoutDataWriterNoRecords() throws IOException {
    FileWriterFactory<T> writerFactory = newWriterFactory(table.schema());
    FanoutDataWriter<T> writer =
        new FanoutDataWriter<>(writerFactory, fileFactory, table.io(), TARGET_FILE_SIZE);

    writer.close();
    Assert.assertEquals("Must be no data files", 0, writer.result().dataFiles().size());

    writer.close();
    Assert.assertEquals("Must be no data files", 0, writer.result().dataFiles().size());
  }

  @Test
  public void testFanoutDataWriterMultiplePartitions() throws IOException {
    table.updateSpec().addField(Expressions.ref("data")).commit();

    FileWriterFactory<T> writerFactory = newWriterFactory(table.schema());
    FanoutDataWriter<T> writer =
        new FanoutDataWriter<>(writerFactory, fileFactory, table.io(), TARGET_FILE_SIZE);

    PartitionSpec spec = table.spec();

    writer.write(toRow(1, "aaa"), spec, partitionKey(spec, "aaa"));
    writer.write(toRow(3, "bbb"), spec, partitionKey(spec, "bbb"));
    writer.write(toRow(2, "aaa"), spec, partitionKey(spec, "aaa"));
    writer.write(toRow(4, "bbb"), spec, partitionKey(spec, "bbb"));
    writer.write(toRow(5, "ccc"), spec, partitionKey(spec, "ccc"));

    writer.close();

    DataWriteResult result = writer.result();
    Assert.assertEquals("Must be 3 data files", 3, result.dataFiles().size());

    RowDelta rowDelta = table.newRowDelta();
    result.dataFiles().forEach(rowDelta::addRows);
    rowDelta.commit();

    List<T> expectedRows =
        ImmutableList.of(
            toRow(1, "aaa"), toRow(2, "aaa"), toRow(3, "bbb"), toRow(4, "bbb"), toRow(5, "ccc"));
    Assert.assertEquals("Records should match", toSet(expectedRows), actualRowSet("*"));
  }
}
