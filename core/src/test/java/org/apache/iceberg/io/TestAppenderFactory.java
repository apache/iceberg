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
import java.util.Locale;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableTestBase;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public abstract class TestAppenderFactory<T> extends TableTestBase {
  private static final int FORMAT_V2 = 2;

  private final FileFormat format;
  private final boolean partitioned;

  private PartitionKey partition = null;

  @Parameterized.Parameters(name = "FileFormat={0}, Partitioned={1}")
  public static Object[] parameters() {
    return new Object[][] {
        new Object[] {"avro", false},
        new Object[] {"avro", true},
        new Object[] {"parquet", false},
        new Object[] {"parquet", true}
    };
  }


  public TestAppenderFactory(String fileFormat, boolean partitioned) {
    super(FORMAT_V2);
    this.format = FileFormat.valueOf(fileFormat.toUpperCase(Locale.ENGLISH));
    this.partitioned = partitioned;
  }

  @Before
  public void setupTable() throws Exception {
    this.tableDir = temp.newFolder();
    Assert.assertTrue(tableDir.delete()); // created by table create

    this.metadataDir = new File(tableDir, "metadata");

    if (partitioned) {
      this.table = create(SCHEMA, SPEC);
    } else {
      this.table = create(SCHEMA, PartitionSpec.unpartitioned());
    }
    this.partition = createPartitionKey();

    table.updateProperties()
        .defaultFormat(format)
        .commit();
  }

  protected abstract FileAppenderFactory<T> createAppenderFactory(List<Integer> equalityFieldIds,
                                                                  Schema eqDeleteSchema,
                                                                  Schema posDeleteRowSchema);

  protected abstract T createRow(Integer id, String data);

  protected abstract StructLikeSet expectedRowSet(Iterable<T> records) throws IOException;

  protected abstract StructLikeSet actualRowSet(String... columns) throws IOException;

  private OutputFileFactory createFileFactory() {
    return new OutputFileFactory(table.spec(), format, table.locationProvider(), table.io(),
        table.encryption(), 1, 1);
  }

  private PartitionKey createPartitionKey() {
    if (table.spec().isUnpartitioned()) {
      return null;
    }

    Record record = GenericRecord.create(table.spec().schema()).copy(ImmutableMap.of("data", "aaa"));

    PartitionKey partitionKey = new PartitionKey(table.spec(), table.schema());
    partitionKey.partition(record);

    return partitionKey;
  }

  private List<T> testRowSet() {
    return Lists.newArrayList(
        createRow(1, "aaa"),
        createRow(2, "bbb"),
        createRow(3, "ccc"),
        createRow(4, "ddd"),
        createRow(5, "eee")
    );
  }

  private DataFile prepareDataFile(List<T> rowSet, FileAppenderFactory<T> appenderFactory,
                                   OutputFileFactory outputFileFactory) throws IOException {
    DataWriter<T> writer = appenderFactory.newDataWriter(outputFileFactory.newOutputFile(), format, partition);
    try (DataWriter<T> closeableWriter = writer) {
      for (T row : rowSet) {
        closeableWriter.add(row);
      }
    }

    return writer.toDataFile();
  }

  @Test
  public void testDataWriter() throws IOException {
    FileAppenderFactory<T> appenderFactory = createAppenderFactory(null, null, null);
    OutputFileFactory outputFileFactory = createFileFactory();

    List<T> rowSet = testRowSet();
    DataFile dataFile = prepareDataFile(rowSet, appenderFactory, outputFileFactory);

    table.newRowDelta()
        .addRows(dataFile)
        .commit();

    Assert.assertEquals("Should have the expected records.", expectedRowSet(rowSet), actualRowSet("*"));
  }

  @Test
  public void testEqDeleteWriter() throws IOException {
    List<Integer> equalityFieldIds = Lists.newArrayList(table.schema().findField("id").fieldId());
    FileAppenderFactory<T> appenderFactory = createAppenderFactory(equalityFieldIds,
        table.schema().select("id"), null);
    OutputFileFactory outputFileFactory = createFileFactory();

    List<T> rowSet = testRowSet();
    DataFile dataFile = prepareDataFile(rowSet, appenderFactory, outputFileFactory);

    table.newRowDelta()
        .addRows(dataFile)
        .commit();

    List<T> deletes = Lists.newArrayList(
        createRow(1, "aaa"),
        createRow(3, "bbb"),
        createRow(5, "ccc")
    );
    EqualityDeleteWriter<T> eqDeleteWriter =
        appenderFactory.newEqDeleteWriter(outputFileFactory.newOutputFile(), format, partition);
    try (EqualityDeleteWriter<T> closeableWriter = eqDeleteWriter) {
      closeableWriter.deleteAll(deletes);
    }

    table.newRowDelta()
        .addDeletes(eqDeleteWriter.toDeleteFile())
        .commit();

    List<T> expected = Lists.newArrayList(
        createRow(2, "bbb"),
        createRow(4, "ddd")
    );
    Assert.assertEquals("Should have the expected records", expectedRowSet(expected), actualRowSet("*"));
  }

  @Test
  public void testPosDeleteWriter() throws IOException {
    // Initialize FileAppenderFactory without pos-delete row schema.
    FileAppenderFactory<T> appenderFactory = createAppenderFactory(null, null, null);
    OutputFileFactory outputFileFactory = createFileFactory();

    List<T> rowSet = testRowSet();
    DataFile dataFile = prepareDataFile(rowSet, appenderFactory, outputFileFactory);

    List<Pair<CharSequence, Long>> deletes = Lists.newArrayList(
        Pair.of(dataFile.path(), 0L),
        Pair.of(dataFile.path(), 2L),
        Pair.of(dataFile.path(), 4L)
    );

    PositionDeleteWriter<T> eqDeleteWriter =
        appenderFactory.newPosDeleteWriter(outputFileFactory.newOutputFile(), format, partition);
    try (PositionDeleteWriter<T> closeableWriter = eqDeleteWriter) {
      for (Pair<CharSequence, Long> delete : deletes) {
        closeableWriter.delete(delete.first(), delete.second());
      }
    }

    table.newRowDelta()
        .addRows(dataFile)
        .addDeletes(eqDeleteWriter.toDeleteFile())
        .validateDataFilesExist(eqDeleteWriter.referencedDataFiles())
        .validateDeletedFiles()
        .commit();

    List<T> expected = Lists.newArrayList(
        createRow(2, "bbb"),
        createRow(4, "ddd")
    );
    Assert.assertEquals("Should have the expected records", expectedRowSet(expected), actualRowSet("*"));
  }

  @Test
  public void testPosDeleteWriterWithRowSchema() throws IOException {
    FileAppenderFactory<T> appenderFactory = createAppenderFactory(null, null, table.schema());
    OutputFileFactory outputFileFactory = createFileFactory();

    List<T> rowSet = testRowSet();
    DataFile dataFile = prepareDataFile(rowSet, appenderFactory, outputFileFactory);

    List<PositionDelete<T>> deletes = Lists.newArrayList(
        new PositionDelete<T>().set(dataFile.path(), 0, rowSet.get(0)),
        new PositionDelete<T>().set(dataFile.path(), 2, rowSet.get(2)),
        new PositionDelete<T>().set(dataFile.path(), 4, rowSet.get(4))
    );

    PositionDeleteWriter<T> eqDeleteWriter =
        appenderFactory.newPosDeleteWriter(outputFileFactory.newOutputFile(), format, partition);
    try (PositionDeleteWriter<T> closeableWriter = eqDeleteWriter) {
      for (PositionDelete<T> delete : deletes) {
        closeableWriter.delete(delete.path(), delete.pos(), delete.row());
      }
    }

    table.newRowDelta()
        .addRows(dataFile)
        .addDeletes(eqDeleteWriter.toDeleteFile())
        .validateDataFilesExist(eqDeleteWriter.referencedDataFiles())
        .validateDeletedFiles()
        .commit();

    List<T> expected = Lists.newArrayList(
        createRow(2, "bbb"),
        createRow(4, "ddd")
    );
    Assert.assertEquals("Should have the expected records", expectedRowSet(expected), actualRowSet("*"));
  }
}
