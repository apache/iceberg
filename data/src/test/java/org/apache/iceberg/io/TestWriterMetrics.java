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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public abstract class TestWriterMetrics<T> {

  private static final int FORMAT_V2 = 2;

  protected static final Types.NestedField ID_FIELD = required(1, "id", Types.IntegerType.get());
  protected static final Types.NestedField DATA_FIELD = optional(2, "data", Types.StringType.get());

  protected static final Types.StructType NESTED_FIELDS =
      Types.StructType.of(
          required(4, "booleanField", Types.BooleanType.get()),
          optional(5, "longValue", Types.LongType.get()));

  protected static final Types.NestedField STRUCT_FIELD = optional(3, "structField", NESTED_FIELDS);

  // create a schema with all supported fields
  protected static final Schema SCHEMA = new Schema(ID_FIELD, DATA_FIELD, STRUCT_FIELD);

  protected static final SortOrder sortOrder =
      SortOrder.builderFor(SCHEMA).asc("id").asc("structField.longValue").build();

  protected static final Map<String, String> properties =
      ImmutableMap.of(TableProperties.DEFAULT_WRITE_METRICS_MODE, "none");

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  protected FileFormat fileFormat;
  protected TestTables.TestTable table = null;
  private OutputFileFactory fileFactory = null;

  @Parameterized.Parameters(name = "FileFormat = {0}")
  public static Object[][] parameters() {
    return new Object[][] {{FileFormat.ORC}, {FileFormat.PARQUET}};
  }

  public TestWriterMetrics(FileFormat fileFormat) {
    this.fileFormat = fileFormat;
  }

  protected abstract FileWriterFactory<T> newWriterFactory(Table sourceTable);

  protected abstract T toRow(Integer id, String data, boolean boolValue, Long longValue);

  protected abstract T toGenericRow(int value, int repeated);

  @Before
  public void setupTable() throws Exception {
    File tableDir = temp.newFolder();
    tableDir.delete(); // created by table create

    this.table =
        TestTables.create(
            tableDir, "test", SCHEMA, PartitionSpec.unpartitioned(), sortOrder, FORMAT_V2);
    table.updateProperties().set(TableProperties.DEFAULT_WRITE_METRICS_MODE, "none").commit();

    this.fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(fileFormat).build();
  }

  @After
  public void after() {
    TestTables.clearTables();
  }

  @Test
  public void verifySortedColMetric() throws Exception {
    T row = toRow(3, "3", true, 3L);
    DataWriter dataWriter =
        newWriterFactory(table)
            .newDataWriter(fileFactory.newOutputFile(), PartitionSpec.unpartitioned(), null);
    dataWriter.write(row);
    dataWriter.close();
    DataFile dataFile = dataWriter.toDataFile();

    // Only two sorted fields (id, structField.longValue) will have metrics
    Map<Integer, ByteBuffer> lowerBounds = dataFile.lowerBounds();
    Assert.assertEquals(
        3, (int) Conversions.fromByteBuffer(Types.IntegerType.get(), lowerBounds.get(1)));
    Assert.assertFalse(lowerBounds.containsKey(2));
    Assert.assertFalse(lowerBounds.containsKey(3));
    Assert.assertFalse(lowerBounds.containsKey(4));
    Assert.assertEquals(
        3L, (long) Conversions.fromByteBuffer(Types.LongType.get(), lowerBounds.get(5)));

    Map<Integer, ByteBuffer> upperBounds = dataFile.upperBounds();
    Assert.assertEquals(
        3, (int) Conversions.fromByteBuffer(Types.IntegerType.get(), upperBounds.get(1)));
    Assert.assertFalse(upperBounds.containsKey(2));
    Assert.assertFalse(upperBounds.containsKey(3));
    Assert.assertFalse(upperBounds.containsKey(4));
    Assert.assertEquals(
        3L, (long) Conversions.fromByteBuffer(Types.LongType.get(), upperBounds.get(5)));
  }

  @Test
  public void testPositionDeleteMetrics() throws IOException {
    FileWriterFactory<T> writerFactory = newWriterFactory(table);
    EncryptedOutputFile outputFile = fileFactory.newOutputFile();
    PositionDeleteWriter<T> deleteWriter =
        writerFactory.newPositionDeleteWriter(outputFile, table.spec(), null);

    try {
      T deletedRow = toRow(3, "3", true, 3L);
      PositionDelete<T> positionDelete = PositionDelete.create();
      positionDelete.set("File A", 1, deletedRow);
      deleteWriter.write(positionDelete);
    } finally {
      deleteWriter.close();
    }

    DeleteFile deleteFile = deleteWriter.toDeleteFile();

    int pathFieldId = MetadataColumns.DELETE_FILE_PATH.fieldId();
    int posFieldId = MetadataColumns.DELETE_FILE_POS.fieldId();

    // should have metrics for _file and _pos as well as two sorted fields (id,
    // structField.longValue)

    Map<Integer, ByteBuffer> lowerBounds = deleteFile.lowerBounds();

    Assert.assertEquals(
        CharBuffer.wrap("File A"),
        Conversions.fromByteBuffer(Types.StringType.get(), lowerBounds.get(pathFieldId)));
    Assert.assertEquals(
        1L, (long) Conversions.fromByteBuffer(Types.LongType.get(), lowerBounds.get(posFieldId)));

    Assert.assertEquals(
        3, (int) Conversions.fromByteBuffer(Types.IntegerType.get(), lowerBounds.get(1)));
    Assert.assertFalse(lowerBounds.containsKey(2));
    Assert.assertFalse(lowerBounds.containsKey(3));
    Assert.assertFalse(lowerBounds.containsKey(4));
    Assert.assertEquals(
        3L, (long) Conversions.fromByteBuffer(Types.LongType.get(), lowerBounds.get(5)));

    Map<Integer, ByteBuffer> upperBounds = deleteFile.upperBounds();

    Assert.assertEquals(
        CharBuffer.wrap("File A"),
        Conversions.fromByteBuffer(Types.StringType.get(), upperBounds.get(pathFieldId)));
    Assert.assertEquals(
        1L, (long) Conversions.fromByteBuffer(Types.LongType.get(), upperBounds.get(posFieldId)));

    Assert.assertEquals(
        3, (int) Conversions.fromByteBuffer(Types.IntegerType.get(), upperBounds.get(1)));
    Assert.assertFalse(upperBounds.containsKey(2));
    Assert.assertFalse(upperBounds.containsKey(3));
    Assert.assertFalse(upperBounds.containsKey(4));
    Assert.assertEquals(
        3L, (long) Conversions.fromByteBuffer(Types.LongType.get(), upperBounds.get(5)));
  }

  @Test
  public void testPositionDeleteMetricsCoveringMultipleDataFiles() throws IOException {
    FileWriterFactory<T> writerFactory = newWriterFactory(table);
    EncryptedOutputFile outputFile = fileFactory.newOutputFile();
    PositionDeleteWriter<T> deleteWriter =
        writerFactory.newPositionDeleteWriter(outputFile, table.spec(), null);

    try {
      PositionDelete<T> positionDelete = PositionDelete.create();

      positionDelete.set("File A", 1, toRow(3, "3", true, 3L));
      deleteWriter.write(positionDelete);

      positionDelete.set("File B", 1, toRow(3, "3", true, 3L));
      deleteWriter.write(positionDelete);

    } finally {
      deleteWriter.close();
    }

    DeleteFile deleteFile = deleteWriter.toDeleteFile();

    // should have NO bounds for path and position as the file covers multiple data paths
    Map<Integer, ByteBuffer> lowerBounds = deleteFile.lowerBounds();
    Assert.assertEquals(2, lowerBounds.size());
    Assert.assertEquals(
        3, (int) Conversions.fromByteBuffer(Types.IntegerType.get(), lowerBounds.get(1)));
    Assert.assertEquals(
        3L, (long) Conversions.fromByteBuffer(Types.LongType.get(), lowerBounds.get(5)));

    Map<Integer, ByteBuffer> upperBounds = deleteFile.upperBounds();
    Assert.assertEquals(2, upperBounds.size());
    Assert.assertEquals(
        3, (int) Conversions.fromByteBuffer(Types.IntegerType.get(), upperBounds.get(1)));
    Assert.assertEquals(
        3L, (long) Conversions.fromByteBuffer(Types.LongType.get(), upperBounds.get(5)));
  }


  @Test
  public void testMaxColumnsBounded() throws IOException {
    File tableDir = temp.newFolder();
    tableDir.delete(); // created by table create

    List<Types.NestedField> fields = Arrays.asList(ID_FIELD, DATA_FIELD, STRUCT_FIELD);

    Schema maxColSchema = new Schema(fields);

    Table maxColumnTable =
            TestTables.create(
                    tableDir,
                    "max_col_table",
                    maxColSchema,
                    PartitionSpec.unpartitioned(),
                    SortOrder.unsorted(),
                    FORMAT_V2);

    long maxInferredColumns = 3;

    maxColumnTable.updateProperties().set(TableProperties.METRICS_MAX_INFERRED_COLUMN_DEFAULTS, String.valueOf(maxInferredColumns)).commit();

    OutputFileFactory maxColFactory =
            OutputFileFactory.builderFor(maxColumnTable, 1, 1).format(fileFormat).build();

    T row = toRow(1,"data", false, Long.MAX_VALUE);
    DataWriter<T> dataWriter =
            newWriterFactory(maxColumnTable)
                    .newDataWriter(maxColFactory.newOutputFile(), PartitionSpec.unpartitioned(), null);
    dataWriter.write(row);
    dataWriter.close();
    DataFile dataFile = dataWriter.toDataFile();
    assertThat(dataFile.upperBounds().keySet().size()).isEqualTo(maxInferredColumns);

  }

  @Test
  public void testMaxColumns() throws IOException {
    File tableDir = temp.newFolder();
    tableDir.delete(); // created by table create

    int numColumns = TableProperties.METRICS_MAX_INFERRED_COLUMN_DEFAULTS_DEFAULT + 1;
    List<Types.NestedField> fields = Lists.newArrayListWithCapacity(numColumns);
    for (int i = 0; i < numColumns; i++) {
      fields.add(required(i, "col" + i, Types.IntegerType.get()));
    }
    Schema maxColSchema = new Schema(fields);

    Table maxColumnTable =
        TestTables.create(
            tableDir,
            "max_col_table",
            maxColSchema,
            PartitionSpec.unpartitioned(),
            SortOrder.unsorted(),
            FORMAT_V2);
    OutputFileFactory maxColFactory =
        OutputFileFactory.builderFor(maxColumnTable, 1, 1).format(fileFormat).build();

    T row = toGenericRow(1, numColumns);
    DataWriter<T> dataWriter =
        newWriterFactory(maxColumnTable)
            .newDataWriter(maxColFactory.newOutputFile(), PartitionSpec.unpartitioned(), null);
    dataWriter.write(row);
    dataWriter.close();
    DataFile dataFile = dataWriter.toDataFile();

    // start at 1 because IDs were reassigned in the table
    int id = 1;
    for (; id <= TableProperties.METRICS_MAX_INFERRED_COLUMN_DEFAULTS_DEFAULT; id += 1) {
      Assert.assertNotNull("Should have lower bound metrics", dataFile.lowerBounds().get(id));
      Assert.assertNotNull("Should have upper bound metrics", dataFile.upperBounds().get(id));
      Assert.assertNull(
          "Should not have nan value metrics (not floating point)",
          dataFile.nanValueCounts().get(id));
      Assert.assertNotNull("Should have null value metrics", dataFile.nullValueCounts().get(id));
      Assert.assertNotNull("Should have value metrics", dataFile.valueCounts().get(id));
    }

    // Remaining fields should not have metrics
    for (; id <= numColumns; id += 1) {
      Assert.assertNull("Should not have any lower bound metrics", dataFile.lowerBounds().get(id));
      Assert.assertNull("Should not have any upper bound metrics", dataFile.upperBounds().get(id));
      Assert.assertNull("Should not have any nan value metrics", dataFile.nanValueCounts().get(id));
      Assert.assertNull(
          "Should not have any null value metrics", dataFile.nullValueCounts().get(id));
      Assert.assertNull("Should not have any value metrics", dataFile.valueCounts().get(id));
    }
  }

  @Test
  public void testMaxColumnsWithDefaultOverride() throws IOException {
    File tableDir = temp.newFolder();
    tableDir.delete(); // created by table create

    int numColumns = TableProperties.METRICS_MAX_INFERRED_COLUMN_DEFAULTS_DEFAULT + 1;
    List<Types.NestedField> fields = Lists.newArrayListWithCapacity(numColumns);
    for (int i = 0; i < numColumns; i++) {
      fields.add(required(i, "col" + i, Types.IntegerType.get()));
    }
    Schema maxColSchema = new Schema(fields);

    Table maxColumnTable =
        TestTables.create(
            tableDir,
            "max_col_table",
            maxColSchema,
            PartitionSpec.unpartitioned(),
            SortOrder.unsorted(),
            FORMAT_V2);
    maxColumnTable
        .updateProperties()
        .set(
            TableProperties.DEFAULT_WRITE_METRICS_MODE,
            TableProperties.DEFAULT_WRITE_METRICS_MODE_DEFAULT)
        .commit();
    OutputFileFactory maxColFactory =
        OutputFileFactory.builderFor(maxColumnTable, 1, 1).format(fileFormat).build();

    T row = toGenericRow(1, numColumns);
    DataWriter<T> dataWriter =
        newWriterFactory(maxColumnTable)
            .newDataWriter(maxColFactory.newOutputFile(), PartitionSpec.unpartitioned(), null);
    dataWriter.write(row);
    dataWriter.close();
    DataFile dataFile = dataWriter.toDataFile();

    // Field should have metrics because the user set the default explicitly
    Map<Integer, ByteBuffer> upperBounds = dataFile.upperBounds();
    Map<Integer, ByteBuffer> lowerBounds = dataFile.upperBounds();
    for (int i = 0; i < numColumns; i++) {
      Assert.assertEquals(
          1, (int) Conversions.fromByteBuffer(Types.IntegerType.get(), upperBounds.get(1)));
      Assert.assertEquals(
          1, (int) Conversions.fromByteBuffer(Types.IntegerType.get(), lowerBounds.get(1)));
    }
  }
}
