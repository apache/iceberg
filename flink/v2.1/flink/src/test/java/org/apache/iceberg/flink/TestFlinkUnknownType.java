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
package org.apache.iceberg.flink;

import static org.apache.iceberg.flink.TestFixtures.DATABASE;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.flink.sink.RowDataTaskWriterFactory;
import org.apache.iceberg.flink.sink.TaskWriterFactory;
import org.apache.iceberg.flink.source.DataIterator;
import org.apache.iceberg.flink.source.RowDataFileScanTaskReader;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
class TestFlinkUnknownType {
  private static final long TARGET_FILE_SIZE = 128 * 1024 * 1024;

  private static final Schema SCHEMA_NULL =
      new Schema(
          Lists.newArrayList(
              Types.NestedField.required(1, "id", Types.IntegerType.get()),
              Types.NestedField.optional(2, "data", Types.StringType.get()),
              Types.NestedField.optional(3, "unknown_col", Types.UnknownType.get()),
              Types.NestedField.optional(4, "data1", Types.StringType.get())));

  @RegisterExtension
  private static final HadoopCatalogExtension CATALOG_EXTENSION =
      new HadoopCatalogExtension(DATABASE, TestFixtures.TABLE);

  @TempDir private Path warehouseDir;

  @Parameter private FileFormat fileFormat;

  @Parameters(name = "fileFormat={0}")
  public static Iterable<Object[]> parameters() {
    return ImmutableList.of(
        new Object[] {FileFormat.PARQUET},
        new Object[] {FileFormat.AVRO},
        new Object[] {FileFormat.ORC});
  }

  @TestTemplate
  void testV3TableUnknownTypeRead() throws Exception {
    Table table = createTableWithNull(fileFormat);

    List<GenericRowData> expected = createExcepted();
    List<Record> expectedRecords = records(expected, table.schema());
    insert(table, expectedRecords, fileFormat);
    List<GenericRowData> records = Lists.newArrayList();
    CloseableIterable<CombinedScanTask> combinedScanTasks = table.newScan().planTasks();
    for (CombinedScanTask combinedScanTask : combinedScanTasks) {
      DataIterator<RowData> dataIterator =
          createDataIterator(combinedScanTask, table.schema(), table.schema());
      while (dataIterator.hasNext()) {
        GenericRowData rowData = (GenericRowData) dataIterator.next();
        records.add(
            GenericRowData.of(
                rowData.getInt(0),
                rowData.getString(1),
                rowData.getField(2),
                rowData.getString(3)));
      }
    }

    assertThat(records).containsExactlyInAnyOrderElementsOf(expected);
  }

  @TestTemplate
  void testV3TableUnknownTypeWrite() throws Exception {
    Table table = createTableWithNull(fileFormat);
    List<GenericRowData> exceptList = createExcepted();
    try (TaskWriter<RowData> taskWriter = createTaskWriter(table)) {
      for (GenericRowData rowData : exceptList) {
        taskWriter.write(rowData);
      }

      taskWriter.close();
      AppendFiles appendFiles = table.newAppend();
      for (DataFile dataFile : taskWriter.dataFiles()) {
        appendFiles.appendFile(dataFile);
      }

      appendFiles.commit();
      List<Record> records = SimpleDataUtil.tableRecords(table);
      List<Record> expectedRecords = records(exceptList, table.schema());
      assertThat(records).containsExactlyInAnyOrderElementsOf(expectedRecords);
    }
  }

  private static Table createTableWithNull(FileFormat fileFormat) {
    return CATALOG_EXTENSION
        .catalog()
        .createTable(
            TestFixtures.TABLE_IDENTIFIER,
            SCHEMA_NULL,
            PartitionSpec.unpartitioned(),
            null,
            ImmutableMap.of("format-version", "3", "write.format.default", fileFormat.name()));
  }

  private List<GenericRowData> createExcepted() {
    return Lists.newArrayList(
        GenericRowData.of(1, StringData.fromString("data"), null, StringData.fromString("data1")),
        GenericRowData.of(2, StringData.fromString("data"), null, StringData.fromString("data1")));
  }

  private DataIterator<RowData> createDataIterator(
      CombinedScanTask combinedTask, Schema tableSchema, Schema projectSchema) {
    return new DataIterator<>(
        new RowDataFileScanTaskReader(
            tableSchema, projectSchema, null, true, Collections.emptyList()),
        combinedTask,
        new HadoopFileIO(new Configuration()),
        PlaintextEncryptionManager.instance());
  }

  private TaskWriter<RowData> createTaskWriter(Table table) {
    RowType flinkWriteType = FlinkSchemaUtil.convert(table.schema());
    TaskWriterFactory<RowData> taskWriterFactory =
        new RowDataTaskWriterFactory(
            table, flinkWriteType, TARGET_FILE_SIZE, fileFormat, table.properties(), null, false);
    taskWriterFactory.initialize(1, 1);
    return taskWriterFactory.create();
  }

  private List<Record> records(List<GenericRowData> records, Schema schema) {
    GenericRecord record = GenericRecord.create(schema);
    ImmutableList.Builder<Record> builder = ImmutableList.builder();
    records.forEach(
        recordData -> {
          GenericRecord copy = record.copy();
          for (int i = 0; i < recordData.getArity(); i++) {
            Object field = recordData.getField(i);
            if (field instanceof StringData) {
              copy.set(i, recordData.getField(i).toString());
            } else {
              copy.set(i, recordData.getField(i));
            }
          }

          builder.add(copy);
        });

    return builder.build();
  }

  private void insert(Table table, List<Record> records, FileFormat format) throws IOException {
    new GenericAppenderHelper(table, format, warehouseDir).appendToTable(records);
    table.refresh();
  }
}
