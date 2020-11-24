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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.data.RandomRowData;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriterResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.flink.SimpleDataUtil.ROW_TYPE;
import static org.apache.iceberg.flink.SimpleDataUtil.SCHEMA;
import static org.apache.iceberg.flink.SimpleDataUtil.createEqDelete;
import static org.apache.iceberg.flink.SimpleDataUtil.createRowData;

@RunWith(Parameterized.class)
public class TestTaskWriters {
  private static final Configuration CONF = new Configuration();
  private static final long TARGET_FILE_SIZE = 128 * 1024 * 1024;

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Parameterized.Parameters(name = "format = {0}, partitioned = {1}")
  public static Object[][] parameters() {
    return new Object[][] {
        {"avro", true},
        {"avro", false},
        {"orc", true},
        {"orc", false},
        {"parquet", true},
        {"parquet", false}
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
    Map<String, String> props = ImmutableMap.of(
        TableProperties.DEFAULT_FILE_FORMAT, format.name(),
        TableProperties.FORMAT_VERSION, "2");
    table = SimpleDataUtil.createTable(path, props, partitioned);
  }

  @Test
  public void testWriteZeroRecord() throws IOException {
    try (TaskWriter<RowData> taskWriter = createTaskWriter(TARGET_FILE_SIZE)) {
      taskWriter.close();

      DataFile[] dataFiles = taskWriter.complete().dataFiles();
      Assert.assertNotNull(dataFiles);
      Assert.assertEquals(0, dataFiles.length);

      // Close again.
      taskWriter.close();
      dataFiles = taskWriter.complete().dataFiles();
      Assert.assertNotNull(dataFiles);
      Assert.assertEquals(0, dataFiles.length);
    }
  }

  @Test
  public void testCloseTwice() throws IOException {
    try (TaskWriter<RowData> taskWriter = createTaskWriter(TARGET_FILE_SIZE)) {
      taskWriter.write(createRowData(1, "hello"));
      taskWriter.write(createRowData(2, "world"));
      taskWriter.close(); // The first close
      taskWriter.close(); // The second close

      int expectedFiles = partitioned ? 2 : 1;
      DataFile[] dataFiles = taskWriter.complete().dataFiles();
      Assert.assertEquals(expectedFiles, dataFiles.length);

      FileSystem fs = FileSystem.get(CONF);
      for (DataFile dataFile : dataFiles) {
        Assert.assertTrue(fs.exists(new Path(dataFile.path().toString())));
      }
    }
  }

  @Test
  public void testAbort() throws IOException {
    try (TaskWriter<RowData> taskWriter = createTaskWriter(TARGET_FILE_SIZE)) {
      taskWriter.write(createRowData(1, "hello"));
      taskWriter.write(createRowData(2, "world"));

      taskWriter.abort();
      DataFile[] dataFiles = taskWriter.complete().dataFiles();

      int expectedFiles = partitioned ? 2 : 1;
      Assert.assertEquals(expectedFiles, dataFiles.length);

      FileSystem fs = FileSystem.get(CONF);
      for (DataFile dataFile : dataFiles) {
        Assert.assertFalse(fs.exists(new Path(dataFile.path().toString())));
      }
    }
  }

  @Test
  public void testCompleteFiles() throws IOException {
    try (TaskWriter<RowData> taskWriter = createTaskWriter(TARGET_FILE_SIZE)) {
      taskWriter.write(createRowData(1, "a"));
      taskWriter.write(createRowData(2, "b"));
      taskWriter.write(createRowData(3, "c"));
      taskWriter.write(createRowData(4, "d"));

      DataFile[] dataFiles = taskWriter.complete().dataFiles();
      int expectedFiles = partitioned ? 4 : 1;
      Assert.assertEquals(expectedFiles, dataFiles.length);

      dataFiles = taskWriter.complete().dataFiles();
      Assert.assertEquals(expectedFiles, dataFiles.length);

      FileSystem fs = FileSystem.get(CONF);
      for (DataFile dataFile : dataFiles) {
        Assert.assertTrue(fs.exists(new Path(dataFile.path().toString())));
      }

      AppendFiles appendFiles = table.newAppend();
      for (DataFile dataFile : dataFiles) {
        appendFiles.appendFile(dataFile);
      }
      appendFiles.commit();

      // Assert the data rows.
      SimpleDataUtil.assertTableRecords(path, Lists.newArrayList(
          SimpleDataUtil.createRecord(1, "a"),
          SimpleDataUtil.createRecord(2, "b"),
          SimpleDataUtil.createRecord(3, "c"),
          SimpleDataUtil.createRecord(4, "d")
      ));
    }
  }

  @Test
  public void testRollingWithTargetFileSize() throws IOException {
    // TODO ORC don't support target file size before closed.
    if (format == FileFormat.ORC) {
      return;
    }
    try (TaskWriter<RowData> taskWriter = createTaskWriter(4)) {
      List<RowData> rows = Lists.newArrayListWithCapacity(8000);
      List<Record> records = Lists.newArrayListWithCapacity(8000);
      for (int i = 0; i < 2000; i++) {
        for (String data : new String[] {"a", "b", "c", "d"}) {
          rows.add(createRowData(i, data));
          records.add(SimpleDataUtil.createRecord(i, data));
        }
      }

      for (RowData row : rows) {
        taskWriter.write(row);
      }

      DataFile[] dataFiles = taskWriter.complete().dataFiles();
      Assert.assertEquals(8, dataFiles.length);

      AppendFiles appendFiles = table.newAppend();
      for (DataFile dataFile : dataFiles) {
        appendFiles.appendFile(dataFile);
      }
      appendFiles.commit();

      // Assert the data rows.
      SimpleDataUtil.assertTableRecords(path, records);
    }
  }

  @Test
  public void testRandomData() throws IOException {
    try (TaskWriter<RowData> taskWriter = createTaskWriter(TARGET_FILE_SIZE)) {
      Iterable<RowData> rows = RandomRowData.generate(SCHEMA, 100, 1996);
      for (RowData row : rows) {
        taskWriter.write(row);
      }

      taskWriter.close();
      DataFile[] dataFiles = taskWriter.complete().dataFiles();
      AppendFiles appendFiles = table.newAppend();
      for (DataFile dataFile : dataFiles) {
        appendFiles.appendFile(dataFile);
      }
      appendFiles.commit();

      // Assert the data rows.
      SimpleDataUtil.assertTableRows(path, Lists.newArrayList(rows));
    }
  }

  @Test
  public void testWriteEqualityDelete() throws IOException {
    if (format == FileFormat.ORC) {
      return;
    }

    try (TaskWriter<RowData> taskWriter = createEqDeleteTaskWriter(TARGET_FILE_SIZE)) {
      taskWriter.write(createRowData(1, "aaa"));
      taskWriter.write(createRowData(2, "bbb"));

      taskWriter.write(createEqDelete(1, "aaa"));
      taskWriter.write(createEqDelete(2, "bbb"));

      taskWriter.write(createRowData(3, "ccc"));

      WriterResult result = taskWriter.complete();
      Assert.assertEquals(result.dataFiles().length, partitioned ? 3 : 1);
      Assert.assertEquals(result.deleteFiles().length, partitioned ? 4 : 2);
      commitTransaction(result);

      assertTableRecords(Sets.newHashSet(createRowData(3, "ccc")));
    }

    try (TaskWriter<RowData> taskWriter = createEqDeleteTaskWriter(TARGET_FILE_SIZE)) {
      taskWriter.write(createEqDelete(3, "ccc"));

      WriterResult result = taskWriter.complete();
      Assert.assertEquals(result.dataFiles().length, 0);
      Assert.assertEquals(result.deleteFiles().length, 1);
      commitTransaction(result);

      assertTableRecords(ImmutableSet.of());
    }
  }

  @Test
  public void testEqualityDeleteSameRow() throws IOException {
    if (format == FileFormat.ORC) {
      return;
    }

    try (TaskWriter<RowData> taskWriter = createEqDeleteTaskWriter(TARGET_FILE_SIZE)) {
      taskWriter.write(createRowData(1, "aaa"));
      taskWriter.write(createEqDelete(1, "aaa"));
      taskWriter.write(createRowData(1, "aaa"));
      taskWriter.write(createEqDelete(1, "aaa"));
      taskWriter.write(createRowData(1, "aaa"));

      WriterResult result = taskWriter.complete();
      Assert.assertEquals(result.dataFiles().length, 1);
      Assert.assertEquals(result.deleteFiles().length, 2);
      commitTransaction(result);

      assertTableRecords(ImmutableSet.of(createRowData(1, "aaa")));
    }

    try (TaskWriter<RowData> taskWriter = createEqDeleteTaskWriter(TARGET_FILE_SIZE)) {
      taskWriter.write(createRowData(1, "aaa"));

      WriterResult result = taskWriter.complete();
      Assert.assertEquals(result.dataFiles().length, 1);
      Assert.assertEquals(result.deleteFiles().length, 0);
      commitTransaction(result);

      assertTableRecords(ImmutableSet.of(createRowData(1, "aaa"), createRowData(1, "aaa")));
    }
  }

  private void commitTransaction(WriterResult result) {
    RowDelta rowDelta = table.newRowDelta();

    for (DataFile dataFile : result.dataFiles()) {
      rowDelta.addRows(dataFile);
    }

    for (DeleteFile deleteFile : result.deleteFiles()) {
      rowDelta.addDeletes(deleteFile);
    }

    rowDelta.commit();
  }

  private void assertTableRecords(Set<RowData> expectedRowDataSet) {
    StructLikeSet expectedSet = StructLikeSet.create(SCHEMA.asStruct());
    for (RowData rowData : expectedRowDataSet) {
      RowDataWrapper wrapper = new RowDataWrapper(ROW_TYPE, SCHEMA.asStruct());
      expectedSet.add(wrapper.wrap(rowData));
    }

    StructLikeSet actualSet = StructLikeSet.create(SCHEMA.asStruct());
    Iterables.addAll(actualSet, IcebergGenerics.read(table).build());
    Assert.assertEquals(expectedSet, actualSet);
  }

  private TaskWriter<RowData> createEqDeleteTaskWriter(long targetFileSize) {
    List<Integer> equalityFieldIds = Lists.newArrayList(table.schema().findField("id").fieldId());

    return createTaskWriter(targetFileSize, equalityFieldIds);
  }

  private TaskWriter<RowData> createTaskWriter(long targetFileSize) {
    return createTaskWriter(targetFileSize, null);
  }

  private TaskWriter<RowData> createTaskWriter(long targetFileSize, List<Integer> equalityFieldIds) {
    TaskWriterFactory<RowData> taskWriterFactory = new RowDataTaskWriterFactory(table.schema(),
        (RowType) SimpleDataUtil.FLINK_SCHEMA.toRowDataType().getLogicalType(), table.spec(),
        table.locationProvider(), table.io(), table.encryption(),
        targetFileSize, format, table.properties(), equalityFieldIds);
    taskWriterFactory.initialize(1, 1);
    return taskWriterFactory.create();
  }
}
