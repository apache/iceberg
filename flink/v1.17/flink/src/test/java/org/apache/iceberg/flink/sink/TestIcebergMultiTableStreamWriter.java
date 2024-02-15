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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkWriteConf;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.data.TableAwareWriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

@RunWith(Parameterized.class)
public class TestIcebergMultiTableStreamWriter {
  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  @Rule public TemporaryFolder tempFolder1 = new TemporaryFolder();

  @Rule public TemporaryFolder tempFolder2 = new TemporaryFolder();

  MockedStatic<TableLoader> mockStatic = Mockito.mockStatic(TableLoader.class);

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

  private Table table;
  private Table table1;
  private Table table2;

  private final FileFormat format;
  private final boolean partitioned;

  private CatalogLoader catalogLoader;

  private TableLoader tableLoader;

  public TestIcebergMultiTableStreamWriter(String format, boolean partitioned) {
    this.format = FileFormat.fromString(format);
    this.partitioned = partitioned;
  }

  @Before
  public void before() throws IOException {
    File folder = tempFolder.newFolder();
    File folder1 = tempFolder1.newFolder();
    File folder2 = tempFolder2.newFolder();
    Map<String, String> props = ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name());
    table = SimpleDataUtil.createTable(folder.getAbsolutePath(), props, partitioned);
    table1 = SimpleDataUtil.createTable(folder1.getAbsolutePath(), props, partitioned);
    table2 = SimpleDataUtil.createTable(folder2.getAbsolutePath(), props, partitioned);
    catalogLoader = Mockito.mock(CatalogLoader.class);
    tableLoader = Mockito.mock(TableLoader.class);
    mockStatic
        .when(() -> TableLoader.fromCatalog(Mockito.any(), Mockito.any()))
        .thenReturn(tableLoader);
  }

  @After
  public void after() {
    mockStatic.close();
  }

  @Test
  public void testWrite() throws Exception {
    long checkpointId = 1L;
    try (OneInputStreamOperatorTestHarness<RowData, TableAwareWriteResult> testHarness =
        createIcebergStreamWriter()) {
      Mockito.when(tableLoader.loadTable()).thenReturn(table1).thenReturn(table2);

      testHarness.processElement(SimpleDataUtil.createRowData(1, "1.1"), 1);
      testHarness.processElement(SimpleDataUtil.createRowData(2, "1.2"), 1);
      testHarness.processElement(SimpleDataUtil.createRowData(3, "2.1"), 1);

      testHarness.prepareSnapshotPreBarrier(checkpointId);
      List<TableAwareWriteResult> writeResult = testHarness.extractOutputValues();
      Assert.assertEquals(writeResult.size(), 2);
      int expectedFiles = partitioned ? 3 : 2;
      Assert.assertEquals(
          writeResult.stream()
              .mapToLong(result -> result.getWriteResult().dataFiles().length)
              .sum(),
          expectedFiles);
      Assert.assertEquals(
          writeResult.stream()
              .mapToLong(result -> result.getWriteResult().deleteFiles().length)
              .sum(),
          0);
      //            Assert.assertEquals(writeResult.dataFiles().length, 2);
      //            Assert.assertEquals(writeResult.deleteFiles().length, 0);
      checkpointId = checkpointId + 1;
      testHarness.processElement(SimpleDataUtil.createRowData(4, "1.2"), 1);
      testHarness.processElement(SimpleDataUtil.createRowData(5, "2.2"), 1);
      testHarness.prepareSnapshotPreBarrier(checkpointId);
      writeResult = testHarness.extractOutputValues();
      expectedFiles = partitioned ? 5 : 4;
      Assert.assertEquals(
          writeResult.stream()
              .mapToLong(result -> result.getWriteResult().dataFiles().length)
              .sum(),
          expectedFiles);
      Assert.assertEquals(
          writeResult.stream()
              .mapToLong(result -> result.getWriteResult().deleteFiles().length)
              .sum(),
          0);
      //            Assert.assertEquals(writeResult.deleteFiles().length, 0);
      //            Assert.assertEquals(writeResult.dataFiles().length, 4);

      AppendFiles appendFiles1 = table1.newAppend();
      AppendFiles appendFiles2 = table2.newAppend();
      writeResult.forEach(
          result -> {
            if (result.getSerializableTable().name().equals(table1.name())) {
              Arrays.stream(result.getWriteResult().dataFiles()).forEach(appendFiles1::appendFile);
            }
            if (result.getSerializableTable().name().equals(table2.name())) {
              Arrays.stream(result.getWriteResult().dataFiles()).forEach(appendFiles2::appendFile);
            }
          });
      appendFiles1.commit();
      appendFiles2.commit();

      SimpleDataUtil.assertTableRecords(table, Lists.newArrayList());

      SimpleDataUtil.assertTableRecords(
          table1,
          Lists.newArrayList(
              SimpleDataUtil.createRecord(1, "1.1"),
              SimpleDataUtil.createRecord(2, "1.2"),
              SimpleDataUtil.createRecord(4, "1.2")));

      SimpleDataUtil.assertTableRecords(
          table2,
          Lists.newArrayList(
              SimpleDataUtil.createRecord(3, "2.1"), SimpleDataUtil.createRecord(5, "2.2")));
    }
  }

  private OneInputStreamOperatorTestHarness<RowData, TableAwareWriteResult>
      createIcebergStreamWriter() throws Exception {
    return createIcebergStreamWriter(table, SimpleDataUtil.FLINK_SCHEMA);
  }

  private OneInputStreamOperatorTestHarness<RowData, TableAwareWriteResult>
      createIcebergStreamWriter(Table icebergTable, TableSchema flinkSchema) throws Exception {
    RowType flinkRowType = FlinkSink.toFlinkRowType(icebergTable.schema(), flinkSchema);
    FlinkWriteConf flinkWriteConfig =
        new FlinkWriteConf(
            icebergTable, Maps.newHashMap(), new org.apache.flink.configuration.Configuration());

    IcebergMultiTableStreamWriter<RowData> streamWriter =
        new IcebergMultiTableStreamWriter<>(
            table.name(),
            new TestPayloadSinkProvider(),
            catalogLoader,
            flinkWriteConfig,
            Collections.emptyList());
    OneInputStreamOperatorTestHarness<RowData, TableAwareWriteResult> harness =
        new OneInputStreamOperatorTestHarness<>(streamWriter, 1, 1, 0);

    harness.setup();
    harness.open();

    return harness;
  }
}
