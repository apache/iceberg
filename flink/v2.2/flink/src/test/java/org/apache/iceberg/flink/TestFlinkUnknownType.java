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

import java.nio.file.Path;
import java.util.List;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
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
import org.apache.iceberg.flink.sink.RowDataTaskWriterFactory;
import org.apache.iceberg.flink.sink.TaskWriterFactory;
import org.apache.iceberg.flink.source.DataIterator;
import org.apache.iceberg.flink.source.reader.ReaderUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
class TestFlinkUnknownType {
  private static final long TARGET_FILE_SIZE = 128 * 1024 * 1024;
  private static final Schema SCHEMA_WITH_UNKNOWN_COL =
      new Schema(
          Lists.newArrayList(
              Types.NestedField.required(1, "id", Types.IntegerType.get()),
              Types.NestedField.optional(2, "data", Types.StringType.get()),
              Types.NestedField.optional(3, "unknown_col", Types.UnknownType.get()),
              Types.NestedField.optional(4, "data1", Types.StringType.get())));
  private static final List<GenericRowData> EXCEPTED_ROW_DATA =
      Lists.newArrayList(
          GenericRowData.of(1, StringData.fromString("data"), null, StringData.fromString("data1")),
          GenericRowData.of(
              2, StringData.fromString("data"), null, StringData.fromString("data1")));
  private static final List<Record> EXPECTED_RECORDS = exceptedRecords();

  @RegisterExtension
  private static final HadoopCatalogExtension CATALOG_EXTENSION =
      new HadoopCatalogExtension(DATABASE, TestFixtures.TABLE);

  @TempDir private Path warehouseDir;

  @Parameter private FileFormat fileFormat;

  private Table table;

  @Parameters(name = "fileFormat={0}")
  public static Iterable<Object[]> parameters() {
    return ImmutableList.of(
        new Object[] {FileFormat.PARQUET},
        new Object[] {FileFormat.AVRO},
        new Object[] {FileFormat.ORC});
  }

  @BeforeEach
  public void before() {
    table =
        CATALOG_EXTENSION
            .catalog()
            .createTable(
                TestFixtures.TABLE_IDENTIFIER,
                SCHEMA_WITH_UNKNOWN_COL,
                PartitionSpec.unpartitioned(),
                null,
                ImmutableMap.of("format-version", "3", "write.format.default", fileFormat.name()));
  }

  @TestTemplate
  void testV3TableUnknownTypeRead() throws Exception {
    new GenericAppenderHelper(table, fileFormat, warehouseDir).appendToTable(EXPECTED_RECORDS);
    table.refresh();

    List<GenericRowData> genericRowData = Lists.newArrayList();
    CloseableIterable<CombinedScanTask> combinedScanTasks = table.newScan().planTasks();
    for (CombinedScanTask combinedScanTask : combinedScanTasks) {
      DataIterator<RowData> dataIterator =
          ReaderUtil.createDataIterator(combinedScanTask, table.schema(), table.schema());
      while (dataIterator.hasNext()) {
        GenericRowData rowData = (GenericRowData) dataIterator.next();
        genericRowData.add(
            GenericRowData.of(
                rowData.getInt(0),
                rowData.getString(1),
                rowData.getField(2),
                rowData.getString(3)));
      }
    }

    assertThat(genericRowData).containsExactlyInAnyOrderElementsOf(EXCEPTED_ROW_DATA);
  }

  @TestTemplate
  void testV3TableUnknownTypeWrite() throws Exception {
    try (TaskWriter<RowData> taskWriter = createTaskWriter()) {
      for (GenericRowData rowData : EXCEPTED_ROW_DATA) {
        taskWriter.write(rowData);
      }

      taskWriter.close();
      AppendFiles appendFiles = table.newAppend();
      for (DataFile dataFile : taskWriter.dataFiles()) {
        appendFiles.appendFile(dataFile);
      }

      appendFiles.commit();
      List<Record> records = SimpleDataUtil.tableRecords(table);
      assertThat(records).containsExactlyInAnyOrderElementsOf(exceptedRecords());
    }
  }

  private TaskWriter<RowData> createTaskWriter() {
    RowType flinkWriteType = FlinkSchemaUtil.convert(table.schema());
    TaskWriterFactory<RowData> taskWriterFactory =
        new RowDataTaskWriterFactory(
            table, flinkWriteType, TARGET_FILE_SIZE, fileFormat, table.properties(), null, false);
    taskWriterFactory.initialize(1, 1);
    return taskWriterFactory.create();
  }

  private static List<Record> exceptedRecords() {
    GenericRecord record = GenericRecord.create(SCHEMA_WITH_UNKNOWN_COL);
    ImmutableList.Builder<Record> builder = ImmutableList.builder();
    EXCEPTED_ROW_DATA.forEach(
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
}
