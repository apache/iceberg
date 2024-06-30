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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
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

  @TempDir private File tempDir;

  protected FileFormat fileFormat;
  protected TestTables.TestTable table = null;
  private OutputFileFactory fileFactory = null;

  @Parameters(name = "fileFormat = {0}")
  public static Collection<FileFormat> parameters() {
    return Arrays.asList(FileFormat.ORC, FileFormat.PARQUET);
  }

  public TestWriterMetrics(FileFormat fileFormat) {
    this.fileFormat = fileFormat;
  }

  protected abstract FileWriterFactory<T> newWriterFactory(Table sourceTable);

  protected abstract T toRow(Integer id, String data, boolean boolValue, Long longValue);

  protected abstract T toGenericRow(int value, int repeated);

  @BeforeEach
  public void setupTable() throws Exception {
    File tableDir = new File(tempDir, "table");
    tableDir.delete(); // created by table create

    this.table =
        TestTables.create(
            tableDir, "test", SCHEMA, PartitionSpec.unpartitioned(), sortOrder, FORMAT_V2);
    table.updateProperties().set(TableProperties.DEFAULT_WRITE_METRICS_MODE, "none").commit();

    this.fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(fileFormat).build();
  }

  @AfterEach
  public void after() {
    TestTables.clearTables();
  }

  @TestTemplate
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
    assertThat((int) Conversions.fromByteBuffer(Types.IntegerType.get(), lowerBounds.get(1)))
        .isEqualTo(3);
    assertThat(lowerBounds).doesNotContainKey(2);
    assertThat(lowerBounds).doesNotContainKey(3);
    assertThat(lowerBounds).doesNotContainKey(4);
    assertThat((long) Conversions.fromByteBuffer(Types.LongType.get(), lowerBounds.get(5)))
        .isEqualTo(3L);

    Map<Integer, ByteBuffer> upperBounds = dataFile.upperBounds();
    assertThat((int) Conversions.fromByteBuffer(Types.IntegerType.get(), upperBounds.get(1)))
        .isEqualTo(3);
    assertThat(upperBounds).doesNotContainKey(2);
    assertThat(upperBounds).doesNotContainKey(3);
    assertThat(upperBounds).doesNotContainKey(4);
    assertThat((long) Conversions.fromByteBuffer(Types.LongType.get(), upperBounds.get(5)))
        .isEqualTo(3L);
  }

  @TestTemplate
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

    assertThat(Conversions.<T>fromByteBuffer(Types.StringType.get(), lowerBounds.get(pathFieldId)))
        .isEqualTo(CharBuffer.wrap("File A"));
    assertThat((long) Conversions.fromByteBuffer(Types.LongType.get(), lowerBounds.get(posFieldId)))
        .isEqualTo(1L);

    assertThat((int) Conversions.fromByteBuffer(Types.IntegerType.get(), lowerBounds.get(1)))
        .isEqualTo(3);
    assertThat(lowerBounds).doesNotContainKey(2);
    assertThat(lowerBounds).doesNotContainKey(3);
    assertThat(lowerBounds).doesNotContainKey(4);
    assertThat((long) Conversions.fromByteBuffer(Types.LongType.get(), lowerBounds.get(5)))
        .isEqualTo(3L);

    Map<Integer, ByteBuffer> upperBounds = deleteFile.upperBounds();

    assertThat(Conversions.<T>fromByteBuffer(Types.StringType.get(), upperBounds.get(pathFieldId)))
        .isEqualTo(CharBuffer.wrap("File A"));
    assertThat((long) Conversions.fromByteBuffer(Types.LongType.get(), upperBounds.get(posFieldId)))
        .isEqualTo(1L);

    assertThat((int) Conversions.fromByteBuffer(Types.IntegerType.get(), upperBounds.get(1)))
        .isEqualTo(3);
    assertThat(upperBounds).doesNotContainKey(2);
    assertThat(upperBounds).doesNotContainKey(3);
    assertThat(upperBounds).doesNotContainKey(4);
    assertThat((long) Conversions.fromByteBuffer(Types.LongType.get(), upperBounds.get(5)))
        .isEqualTo(3L);
  }

  @TestTemplate
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
    assertThat(lowerBounds).hasSize(2);
    assertThat((int) Conversions.fromByteBuffer(Types.IntegerType.get(), lowerBounds.get(1)))
        .isEqualTo(3);
    assertThat((long) Conversions.fromByteBuffer(Types.LongType.get(), lowerBounds.get(5)))
        .isEqualTo(3L);

    Map<Integer, ByteBuffer> upperBounds = deleteFile.upperBounds();
    assertThat(upperBounds).hasSize(2);
    assertThat((int) Conversions.fromByteBuffer(Types.IntegerType.get(), upperBounds.get(1)))
        .isEqualTo(3);
    assertThat((long) Conversions.fromByteBuffer(Types.LongType.get(), upperBounds.get(5)))
        .isEqualTo(3L);
  }

  @TestTemplate
  public void testMaxColumns() throws IOException {
    File tableDir = new File(tempDir, "table");
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
      assertThat(dataFile.lowerBounds().get(id)).as("Should have lower bound metrics").isNotNull();
      assertThat(dataFile.upperBounds().get(id)).as("Should have upper bound metrics").isNotNull();
      assertThat(dataFile.nanValueCounts().get(id))
          .as("Should not have nan value metrics (not floating point)")
          .isNull();
      assertThat(dataFile.nullValueCounts().get(id))
          .as("Should have null value metrics")
          .isNotNull();
      assertThat(dataFile.valueCounts().get(id)).as("Should have value metrics").isNotNull();
    }

    // Remaining fields should not have metrics
    for (; id <= numColumns; id += 1) {
      assertThat(dataFile.lowerBounds().get(id))
          .as("Should not have any lower bound metrics")
          .isNull();
      assertThat(dataFile.upperBounds().get(id))
          .as("Should not have any upper bound metrics")
          .isNull();
      assertThat(dataFile.nanValueCounts().get(id))
          .as("Should not have any nan value metrics")
          .isNull();
      assertThat(dataFile.nullValueCounts().get(id))
          .as("Should not have any null value metrics")
          .isNull();
      assertThat(dataFile.valueCounts().get(id)).as("Should not have any value metrics").isNull();
    }
  }

  @TestTemplate
  public void testMaxColumnsWithDefaultOverride() throws IOException {
    File tableDir = new File(tempDir, "table");
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
      assertThat((int) Conversions.fromByteBuffer(Types.IntegerType.get(), upperBounds.get(1)))
          .isEqualTo(1);
      assertThat((int) Conversions.fromByteBuffer(Types.IntegerType.get(), lowerBounds.get(1)))
          .isEqualTo(1);
    }
  }
}
