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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.data.RandomData;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.taskio.OutputFileFactory;
import org.apache.iceberg.taskio.TaskWriter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestTaskWriters {
  private static final Configuration CONF = new Configuration();
  private static final long TARGET_FILE_SIZE = 128 * 1024 * 1024;

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  // TODO add ORC unit test once the readers and writers are ready.
  @Parameterized.Parameters(name = "format = {0}, partitioned = {1}")
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] {"parquet", true},
        new Object[] {"parquet", false},
        new Object[] {"avro", true},
        new Object[] {"avro", false},
    };
  }

  private final FileFormat format;
  private final boolean partitioned;

  private String path;
  private Table table;

  public TestTaskWriters(String format, boolean partitioned) {
    this.format = FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH));
    this.partitioned = partitioned;
  }

  @Before
  public void before() throws IOException {
    File folder = tempFolder.newFolder();
    path = folder.getAbsolutePath();

    // Construct the iceberg table with the specified file format.
    Map<String, String> props = ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name());
    table = SimpleDataUtil.createTable(path, props, partitioned);
  }

  @Test
  public void testWriteZeroRecord() throws IOException {
    try (TaskWriter<Row> taskWriter = createTaskWriter(TARGET_FILE_SIZE)) {
      taskWriter.close();

      List<DataFile> dataFiles = taskWriter.pollCompleteFiles();
      Assert.assertNotNull(dataFiles);
      Assert.assertEquals(0, dataFiles.size());

      // Close again.
      taskWriter.close();
      dataFiles = taskWriter.pollCompleteFiles();
      Assert.assertNotNull(dataFiles);
      Assert.assertEquals(0, dataFiles.size());
    }
  }

  @Test
  public void testCloseTwice() throws IOException {
    try (TaskWriter<Row> taskWriter = createTaskWriter(TARGET_FILE_SIZE)) {
      taskWriter.write(Row.of(1, "hello"));
      taskWriter.write(Row.of(2, "world"));
      taskWriter.close(); // The first close
      taskWriter.close(); // The second close

      int expectedFiles = partitioned ? 2 : 1;
      List<DataFile> dataFiles = taskWriter.pollCompleteFiles();
      Assert.assertEquals(expectedFiles, dataFiles.size());

      FileSystem fs = FileSystem.get(CONF);
      for (DataFile dataFile : dataFiles) {
        Assert.assertTrue(fs.exists(new Path(dataFile.path().toString())));
      }
    }
  }

  @Test
  public void testAbort() throws IOException {
    try (TaskWriter<Row> taskWriter = createTaskWriter(TARGET_FILE_SIZE)) {
      taskWriter.write(Row.of(1, "hello"));
      taskWriter.write(Row.of(2, "world"));

      taskWriter.abort();
      List<DataFile> dataFiles = taskWriter.pollCompleteFiles();

      int expectedFiles = partitioned ? 2 : 1;
      Assert.assertEquals(expectedFiles, dataFiles.size());

      FileSystem fs = FileSystem.get(CONF);
      for (DataFile dataFile : dataFiles) {
        Assert.assertFalse(fs.exists(new Path(dataFile.path().toString())));
      }
    }
  }

  @Test
  public void testPollCompleteFiles() throws IOException {
    try (TaskWriter<Row> taskWriter = createTaskWriter(TARGET_FILE_SIZE)) {
      taskWriter.write(Row.of(1, "a"));
      taskWriter.write(Row.of(2, "b"));
      taskWriter.write(Row.of(3, "c"));
      taskWriter.write(Row.of(4, "d"));

      List<DataFile> dataFiles = taskWriter.pollCompleteFiles();
      Assert.assertEquals(0, dataFiles.size());

      taskWriter.close();
      dataFiles = taskWriter.pollCompleteFiles();
      int expectedFiles = partitioned ? 4 : 1;
      Assert.assertEquals(expectedFiles, dataFiles.size());

      FileSystem fs = FileSystem.get(CONF);
      for (DataFile dataFile : dataFiles) {
        Assert.assertTrue(fs.exists(new Path(dataFile.path().toString())));
      }
      AppendFiles appendFiles = table.newAppend();
      dataFiles.forEach(appendFiles::appendFile);
      appendFiles.commit();

      // Assert the data rows.
      SimpleDataUtil.assertTableRecords(path, Lists.newArrayList(
          SimpleDataUtil.createRecord(1, "a"),
          SimpleDataUtil.createRecord(2, "b"),
          SimpleDataUtil.createRecord(3, "c"),
          SimpleDataUtil.createRecord(4, "d")
      ));

      dataFiles = taskWriter.pollCompleteFiles();
      Assert.assertEquals(0, dataFiles.size());
    }
  }

  @Test
  public void testCopiedCompleteFiles() throws IOException {
    try (TaskWriter<Row> taskWriter = createTaskWriter(4)) {
      List<Row> rows = Lists.newArrayListWithCapacity(1000);
      for (int i = 0; i < 1000; i++) {
        rows.add(Row.of(i, "a"));
      }

      for (Row row : rows) {
        taskWriter.write(row);
      }

      List<DataFile> dataFiles = taskWriter.pollCompleteFiles();
      Assert.assertEquals(1, dataFiles.size());
      AssertHelpers.assertThrows("Complete file list are immutable", UnsupportedOperationException.class,
          () -> dataFiles.add(null));

      List<DataFile> emptyList = taskWriter.pollCompleteFiles();
      Assert.assertEquals(0, emptyList.size());
      AssertHelpers.assertThrows("Empty complete file list are immutable", UnsupportedOperationException.class,
          () -> emptyList.add(null));

      // It should not open any new file when closed a writer without writing new record.
      taskWriter.close();
      Assert.assertEquals(0, taskWriter.pollCompleteFiles().size());
    }
  }

  @Test
  public void testRollingWithTargetFileSize() throws IOException {
    try (TaskWriter<Row> taskWriter = createTaskWriter(4)) {
      List<Row> rows = Lists.newArrayListWithCapacity(8000);
      List<Record> records = Lists.newArrayListWithCapacity(8000);
      for (int i = 0; i < 2000; i++) {
        for (String data : new String[] {"a", "b", "c", "d"}) {
          rows.add(Row.of(i, data));
          records.add(SimpleDataUtil.createRecord(i, data));
        }
      }

      for (Row row : rows) {
        taskWriter.write(row);
      }

      List<DataFile> dataFiles = taskWriter.pollCompleteFiles();
      Assert.assertEquals(8, dataFiles.size());

      AppendFiles appendFiles = table.newAppend();
      dataFiles.forEach(appendFiles::appendFile);
      appendFiles.commit();

      // Assert the data rows.
      SimpleDataUtil.assertTableRecords(path, records);
    }
  }

  @Test
  public void testRandomData() throws IOException {
    try (TaskWriter<Row> taskWriter = createTaskWriter(TARGET_FILE_SIZE)) {
      Iterable<Row> rows = RandomData.generate(SimpleDataUtil.SCHEMA, 100, 1996);
      for (Row row : rows) {
        taskWriter.write(row);
      }

      taskWriter.close();
      List<DataFile> dataFiles = taskWriter.pollCompleteFiles();
      AppendFiles appendFiles = table.newAppend();
      dataFiles.forEach(appendFiles::appendFile);
      appendFiles.commit();

      // Assert the data rows.
      SimpleDataUtil.assertTableRows(path, Lists.newArrayList(rows));
    }
  }

  private TaskWriter<Row> createTaskWriter(long targetFileSize) {
    TaskWriterFactory.FlinkFileAppenderFactory appenderFactory =
        new TaskWriterFactory.FlinkFileAppenderFactory(table.schema(), table.properties());

    OutputFileFactory outputFileFactory = new OutputFileFactory(table.spec(), format, table.locationProvider(),
        table.io(), table.encryption(), 1, 1);

    return TaskWriterFactory.createTaskWriter(table.schema(), table.spec(), format,
        appenderFactory, outputFileFactory, table.io(), targetFileSize);
  }
}
