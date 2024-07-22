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
package org.apache.iceberg.flink.sink;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.data.RandomRowData;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
public class TestTaskWriters {
  private static final Configuration CONF = new Configuration();
  private static final long TARGET_FILE_SIZE = 128 * 1024 * 1024;

  @TempDir protected java.nio.file.Path temporaryFolder;

  @Parameters(name = "format = {0}, partitioned = {1}")
  public static Object[][] parameters() {
    return new Object[][] {
      {FileFormat.AVRO, true},
      {FileFormat.AVRO, false},
      {FileFormat.ORC, true},
      {FileFormat.ORC, false},
      {FileFormat.PARQUET, true},
      {FileFormat.PARQUET, false}
    };
  }

  @Parameter(index = 0)
  private FileFormat format;

  @Parameter(index = 1)
  private boolean partitioned;

  private Table table;

  @BeforeEach
  public void before() throws IOException {
    File folder = Files.createTempDirectory(temporaryFolder, "junit").toFile();
    // Construct the iceberg table with the specified file format.
    Map<String, String> props = ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name());
    table = SimpleDataUtil.createTable(folder.getAbsolutePath(), props, partitioned);
  }

  @TestTemplate
  public void testWriteZeroRecord() throws IOException {
    try (TaskWriter<RowData> taskWriter = createTaskWriter(TARGET_FILE_SIZE)) {
      taskWriter.close();

      DataFile[] dataFiles = taskWriter.dataFiles();
      assertThat(dataFiles).isNotNull().isEmpty();

      // Close again.
      taskWriter.close();
      dataFiles = taskWriter.dataFiles();
      assertThat(dataFiles).isNotNull().isEmpty();
    }
  }

  @TestTemplate
  public void testCloseTwice() throws IOException {
    try (TaskWriter<RowData> taskWriter = createTaskWriter(TARGET_FILE_SIZE)) {
      taskWriter.write(SimpleDataUtil.createRowData(1, "hello"));
      taskWriter.write(SimpleDataUtil.createRowData(2, "world"));
      taskWriter.close(); // The first close
      taskWriter.close(); // The second close

      int expectedFiles = partitioned ? 2 : 1;
      DataFile[] dataFiles = taskWriter.dataFiles();
      assertThat(dataFiles).hasSize(expectedFiles);

      FileSystem fs = FileSystem.get(CONF);
      for (DataFile dataFile : dataFiles) {
        assertThat(fs.exists(new Path(dataFile.path().toString()))).isTrue();
      }
    }
  }

  @TestTemplate
  public void testAbort() throws IOException {
    try (TaskWriter<RowData> taskWriter = createTaskWriter(TARGET_FILE_SIZE)) {
      taskWriter.write(SimpleDataUtil.createRowData(1, "hello"));
      taskWriter.write(SimpleDataUtil.createRowData(2, "world"));

      taskWriter.abort();
      DataFile[] dataFiles = taskWriter.dataFiles();

      int expectedFiles = partitioned ? 2 : 1;
      assertThat(dataFiles).hasSize(expectedFiles);

      FileSystem fs = FileSystem.get(CONF);
      for (DataFile dataFile : dataFiles) {
        assertThat(fs.exists(new Path(dataFile.path().toString()))).isFalse();
      }
    }
  }

  @TestTemplate
  public void testCompleteFiles() throws IOException {
    try (TaskWriter<RowData> taskWriter = createTaskWriter(TARGET_FILE_SIZE)) {
      taskWriter.write(SimpleDataUtil.createRowData(1, "a"));
      taskWriter.write(SimpleDataUtil.createRowData(2, "b"));
      taskWriter.write(SimpleDataUtil.createRowData(3, "c"));
      taskWriter.write(SimpleDataUtil.createRowData(4, "d"));

      DataFile[] dataFiles = taskWriter.dataFiles();
      int expectedFiles = partitioned ? 4 : 1;
      assertThat(dataFiles).hasSize(expectedFiles);

      dataFiles = taskWriter.dataFiles();
      assertThat(dataFiles).hasSize(expectedFiles);

      FileSystem fs = FileSystem.get(CONF);
      for (DataFile dataFile : dataFiles) {
        assertThat(fs.exists(new Path(dataFile.path().toString()))).isTrue();
      }

      AppendFiles appendFiles = table.newAppend();
      for (DataFile dataFile : dataFiles) {
        appendFiles.appendFile(dataFile);
      }
      appendFiles.commit();

      // Assert the data rows.
      SimpleDataUtil.assertTableRecords(
          table,
          Lists.newArrayList(
              SimpleDataUtil.createRecord(1, "a"),
              SimpleDataUtil.createRecord(2, "b"),
              SimpleDataUtil.createRecord(3, "c"),
              SimpleDataUtil.createRecord(4, "d")));
    }
  }

  @TestTemplate
  public void testRollingWithTargetFileSize() throws IOException {
    try (TaskWriter<RowData> taskWriter = createTaskWriter(4)) {
      List<RowData> rows = Lists.newArrayListWithCapacity(8000);
      List<Record> records = Lists.newArrayListWithCapacity(8000);
      for (int i = 0; i < 2000; i++) {
        for (String data : new String[] {"a", "b", "c", "d"}) {
          rows.add(SimpleDataUtil.createRowData(i, data));
          records.add(SimpleDataUtil.createRecord(i, data));
        }
      }

      for (RowData row : rows) {
        taskWriter.write(row);
      }

      DataFile[] dataFiles = taskWriter.dataFiles();
      assertThat(dataFiles).hasSize(8);

      AppendFiles appendFiles = table.newAppend();
      for (DataFile dataFile : dataFiles) {
        appendFiles.appendFile(dataFile);
      }
      appendFiles.commit();

      // Assert the data rows.
      SimpleDataUtil.assertTableRecords(table, records);
    }
  }

  @TestTemplate
  public void testRandomData() throws IOException {
    try (TaskWriter<RowData> taskWriter = createTaskWriter(TARGET_FILE_SIZE)) {
      Iterable<RowData> rows = RandomRowData.generate(SimpleDataUtil.SCHEMA, 100, 1996);
      for (RowData row : rows) {
        taskWriter.write(row);
      }

      taskWriter.close();
      DataFile[] dataFiles = taskWriter.dataFiles();
      AppendFiles appendFiles = table.newAppend();
      for (DataFile dataFile : dataFiles) {
        appendFiles.appendFile(dataFile);
      }
      appendFiles.commit();

      // Assert the data rows.
      SimpleDataUtil.assertTableRows(table, Lists.newArrayList(rows));
    }
  }

  private TaskWriter<RowData> createTaskWriter(long targetFileSize) {
    TaskWriterFactory<RowData> taskWriterFactory =
        new RowDataTaskWriterFactory(
            SerializableTable.copyOf(table),
            (RowType) SimpleDataUtil.FLINK_SCHEMA.toRowDataType().getLogicalType(),
            targetFileSize,
            format,
            table.properties(),
            null,
            false);
    taskWriterFactory.initialize(1, 1);
    return taskWriterFactory.create();
  }
}
