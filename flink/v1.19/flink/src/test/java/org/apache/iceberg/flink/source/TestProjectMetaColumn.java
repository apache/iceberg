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
package org.apache.iceberg.flink.source;

import java.io.IOException;
import java.util.List;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.flink.data.RowDataProjection;
import org.apache.iceberg.flink.sink.RowDataTaskWriterFactory;
import org.apache.iceberg.flink.sink.TaskWriterFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestProjectMetaColumn {

  @Rule public final TemporaryFolder folder = new TemporaryFolder();
  private final FileFormat format;

  @Parameterized.Parameters(name = "fileFormat={0}")
  public static Iterable<Object[]> parameters() {
    return Lists.newArrayList(
        new Object[] {FileFormat.PARQUET},
        new Object[] {FileFormat.ORC},
        new Object[] {FileFormat.AVRO});
  }

  public TestProjectMetaColumn(FileFormat format) {
    this.format = format;
  }

  private void testSkipToRemoveMetaColumn(int formatVersion) throws IOException {
    // Create the table with given format version.
    String location = folder.getRoot().getAbsolutePath();
    Table table =
        SimpleDataUtil.createTable(
            location,
            ImmutableMap.of(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion)),
            false);

    List<RowData> rows =
        Lists.newArrayList(
            SimpleDataUtil.createInsert(1, "AAA"),
            SimpleDataUtil.createInsert(2, "BBB"),
            SimpleDataUtil.createInsert(3, "CCC"));
    writeAndCommit(table, ImmutableList.of(), false, rows);

    FlinkInputFormat input =
        FlinkSource.forRowData().tableLoader(TableLoader.fromHadoopTable(location)).buildFormat();

    List<RowData> results = Lists.newArrayList();
    TestHelpers.readRowData(
        input,
        rowData -> {
          // If project to remove the meta columns, it will get a RowDataProjection.
          Assert.assertTrue(rowData instanceof GenericRowData);
          results.add(TestHelpers.copyRowData(rowData, SimpleDataUtil.ROW_TYPE));
        });

    // Assert the results.
    TestHelpers.assertRows(rows, results, SimpleDataUtil.ROW_TYPE);
  }

  @Test
  public void testV1SkipToRemoveMetaColumn() throws IOException {
    testSkipToRemoveMetaColumn(1);
  }

  @Test
  public void testV2SkipToRemoveMetaColumn() throws IOException {
    testSkipToRemoveMetaColumn(2);
  }

  @Test
  public void testV2RemoveMetaColumn() throws Exception {
    // Create the v2 table.
    String location = folder.getRoot().getAbsolutePath();
    Table table =
        SimpleDataUtil.createTable(
            location, ImmutableMap.of(TableProperties.FORMAT_VERSION, "2"), false);

    List<RowData> rows =
        Lists.newArrayList(
            SimpleDataUtil.createInsert(1, "AAA"),
            SimpleDataUtil.createDelete(1, "AAA"),
            SimpleDataUtil.createInsert(2, "AAA"),
            SimpleDataUtil.createInsert(2, "BBB"));
    int eqFieldId = table.schema().findField("data").fieldId();
    writeAndCommit(table, ImmutableList.of(eqFieldId), true, rows);

    FlinkInputFormat input =
        FlinkSource.forRowData().tableLoader(TableLoader.fromHadoopTable(location)).buildFormat();

    List<RowData> results = Lists.newArrayList();
    TestHelpers.readRowData(
        input,
        rowData -> {
          // If project to remove the meta columns, it will get a RowDataProjection.
          Assert.assertTrue(rowData instanceof RowDataProjection);
          results.add(TestHelpers.copyRowData(rowData, SimpleDataUtil.ROW_TYPE));
        });

    // Assert the results.
    TestHelpers.assertRows(
        ImmutableList.of(
            SimpleDataUtil.createInsert(2, "AAA"), SimpleDataUtil.createInsert(2, "BBB")),
        results,
        SimpleDataUtil.ROW_TYPE);
  }

  private void writeAndCommit(
      Table table, List<Integer> eqFieldIds, boolean upsert, List<RowData> rows)
      throws IOException {
    TaskWriter<RowData> writer = createTaskWriter(table, eqFieldIds, upsert);
    try (TaskWriter<RowData> io = writer) {
      for (RowData row : rows) {
        io.write(row);
      }
    }

    RowDelta delta = table.newRowDelta();
    WriteResult result = writer.complete();

    for (DataFile dataFile : result.dataFiles()) {
      delta.addRows(dataFile);
    }

    for (DeleteFile deleteFile : result.deleteFiles()) {
      delta.addDeletes(deleteFile);
    }

    delta.commit();
  }

  private TaskWriter<RowData> createTaskWriter(
      Table table, List<Integer> equalityFieldIds, boolean upsert) {
    TaskWriterFactory<RowData> taskWriterFactory =
        new RowDataTaskWriterFactory(
            SerializableTable.copyOf(table),
            SimpleDataUtil.ROW_TYPE,
            TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT,
            format,
            table.properties(),
            equalityFieldIds,
            upsert);

    taskWriterFactory.initialize(1, 1);
    return taskWriterFactory.create();
  }
}
