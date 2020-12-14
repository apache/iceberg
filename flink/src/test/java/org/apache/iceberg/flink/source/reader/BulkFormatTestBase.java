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

package org.apache.iceberg.flink.source.reader;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.flink.source.FlinkSplitGenerator;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public abstract class BulkFormatTestBase {

  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  protected static final ScanContext scanContext = ScanContext.builder()
      .project(TestFixtures.SCHEMA)
      .build();
  protected static final RowType rowType = FlinkSchemaUtil
      .convert(scanContext.project());
  private static final DataStructureConverter<Object, Object> rowDataConverter = DataStructureConverters.getConverter(
      TypeConversions.fromLogicalToDataType(rowType));
  private static final org.apache.flink.configuration.Configuration flinkConfig =
      new org.apache.flink.configuration.Configuration();

  protected abstract BulkFormat<RowData, IcebergSourceSplit> getBulkFormat();

  private final FileFormat fileFormat;

  public BulkFormatTestBase(FileFormat fileFormat) {
    this.fileFormat = fileFormat;
  }

  private String warehouse;
  private HadoopCatalog catalog;
  protected Table table;
  private List<List<Record>> recordBatchList;
  private List<DataFile> dataFileList;
  private IcebergSourceSplit icebergSplit;

  @Before
  public void before() throws IOException {
    final File warehouseFile = TEMPORARY_FOLDER.newFolder();
    Assert.assertTrue(warehouseFile.delete());
    warehouse = "file:" + warehouseFile;
    org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
    catalog = new HadoopCatalog(hadoopConf, warehouse);
    table = catalog.createTable(TestFixtures.TABLE_IDENTIFIER, TestFixtures.SCHEMA);

    final GenericAppenderHelper dataAppender = new GenericAppenderHelper(
        table, fileFormat, TEMPORARY_FOLDER);
    recordBatchList = new ArrayList<>(3);
    dataFileList = new ArrayList<>(2);
    for (int i = 0; i < 3; ++i) {
      List<Record> records = RandomGenericData.generate(TestFixtures.SCHEMA, 2, i);
      recordBatchList.add(records);
      DataFile dataFile = dataAppender.writeFile(null, records);
      dataFileList.add(dataFile);
      dataAppender.appendToTable(dataFile);
    }

    final List<IcebergSourceSplit> splits = FlinkSplitGenerator.planIcebergSourceSplits(table, scanContext);
    Assert.assertEquals(1, splits.size());
    Assert.assertEquals(3, splits.get(0).task().files().size());
    icebergSplit = sortFilesAsAppendOrder(splits.get(0), dataFileList);
  }

  /**
   * Split planning doesn't guarantee the order is the same as appended.
   * So we re-arrange the list to make the assertion simpler
   */
  public static IcebergSourceSplit sortFilesAsAppendOrder(IcebergSourceSplit split, List<DataFile> dataFiles) {
    Collection<FileScanTask> files = split.task().files();
    Assert.assertEquals(files.size(), dataFiles.size());
    FileScanTask[] sortedFileArray = new FileScanTask[files.size()];
    for (FileScanTask fileScanTask : files) {
      for (int i = 0; i < dataFiles.size(); ++i) {
        if (fileScanTask.file().path().toString().equals(dataFiles.get(i).path().toString())) {
          sortedFileArray[i] = fileScanTask;
        }
      }
    }
    List<FileScanTask> sortedFileList = Lists.newArrayList(sortedFileArray);
    Assert.assertThat(sortedFileList, CoreMatchers.everyItem(CoreMatchers.notNullValue(FileScanTask.class)));
    CombinedScanTask rearrangedCombinedTask = new BaseCombinedScanTask(sortedFileList);
    return IcebergSourceSplit.fromCombinedScanTask(rearrangedCombinedTask);
  }

  @After
  public void after() throws IOException {
    catalog.dropTable(TestFixtures.TABLE_IDENTIFIER);
    catalog.close();
  }

  @Test
  public void testNoCheckpointedPosition() throws IOException {
    final IcebergSourceSplit split = icebergSplit;
    final BulkFormat.Reader<RowData> reader = getBulkFormat().createReader(flinkConfig, split);

    final BulkFormat.RecordIterator<RowData> iter0 = reader.readBatch();
    final List<Row> rows0 = toRows(iter0, 0L, 0L);
    TestHelpers.assertRecords(rows0, recordBatchList.get(0), TestFixtures.SCHEMA);

    final BulkFormat.RecordIterator<RowData> iter1 = reader.readBatch();
    final List<Row> rows1 = toRows(iter1, 1L, 0L);
    TestHelpers.assertRecords(rows1, recordBatchList.get(1), TestFixtures.SCHEMA);

    final BulkFormat.RecordIterator<RowData> iter2 = reader.readBatch();
    final List<Row> rows2 = toRows(iter2, 2L, 0L);
    TestHelpers.assertRecords(rows2, recordBatchList.get(2), TestFixtures.SCHEMA);
  }

  @Test
  public void testCheckpointedPositionBeforeFirstFile() throws IOException {
    final IcebergSourceSplit split = new IcebergSourceSplit(
        icebergSplit.task(),
        new CheckpointedPosition(0L, 0L));
    final BulkFormat.Reader<RowData> reader = getBulkFormat().restoreReader(flinkConfig, split);

    final BulkFormat.RecordIterator<RowData> iter0 = reader.readBatch();
    final List<Row> rows0 = toRows(iter0, 0L, 0L);
    TestHelpers.assertRecords(rows0, recordBatchList.get(0), TestFixtures.SCHEMA);

    final BulkFormat.RecordIterator<RowData> iter1 = reader.readBatch();
    final List<Row> rows1 = toRows(iter1, 1L, 0L);
    TestHelpers.assertRecords(rows1, recordBatchList.get(1), TestFixtures.SCHEMA);

    final BulkFormat.RecordIterator<RowData> iter2 = reader.readBatch();
    final List<Row> rows2 = toRows(iter2, 2L, 0L);
    TestHelpers.assertRecords(rows2, recordBatchList.get(2), TestFixtures.SCHEMA);
  }

  @Test
  public void testCheckpointedPositionMiddleFirstFile() throws IOException {
    final IcebergSourceSplit split = new IcebergSourceSplit(
        icebergSplit.task(),
        new CheckpointedPosition(0L, 1L));
    final BulkFormat.Reader<RowData> reader = getBulkFormat().restoreReader(flinkConfig, split);

    final BulkFormat.RecordIterator<RowData> iter0 = reader.readBatch();
    final List<Row> rows0 = toRows(iter0, 0L, 1L);
    TestHelpers.assertRecords(rows0, recordBatchList.get(0).subList(1, 2), TestFixtures.SCHEMA);

    final BulkFormat.RecordIterator<RowData> iter1 = reader.readBatch();
    final List<Row> rows1 = toRows(iter1, 1L, 0L);
    TestHelpers.assertRecords(rows1, recordBatchList.get(1), TestFixtures.SCHEMA);

    final BulkFormat.RecordIterator<RowData> iter2 = reader.readBatch();
    final List<Row> row2 = toRows(iter2, 2L, 0L);
    TestHelpers.assertRecords(row2, recordBatchList.get(2), TestFixtures.SCHEMA);
  }

  @Test
  public void testCheckpointedPositionAfterFirstFile() throws IOException {
    final IcebergSourceSplit split = new IcebergSourceSplit(
        icebergSplit.task(),
        new CheckpointedPosition(0L, 2L));
    final BulkFormat.Reader<RowData> reader = getBulkFormat().restoreReader(flinkConfig, split);

    final BulkFormat.RecordIterator<RowData> iter1 = reader.readBatch();
    final List<Row> rows1 = toRows(iter1, 1L, 0L);
    TestHelpers.assertRecords(rows1, recordBatchList.get(1), TestFixtures.SCHEMA);

    final BulkFormat.RecordIterator<RowData> iter2 = reader.readBatch();
    final List<Row> row2 = toRows(iter2, 2L, 0L);
    TestHelpers.assertRecords(row2, recordBatchList.get(2), TestFixtures.SCHEMA);
  }

  @Test
  public void testCheckpointedPositionBeforeSecondFile() throws IOException {
    final IcebergSourceSplit split = new IcebergSourceSplit(
        icebergSplit.task(),
        new CheckpointedPosition(1L, 0L));
    final BulkFormat.Reader<RowData> reader = getBulkFormat().restoreReader(flinkConfig, split);

    final BulkFormat.RecordIterator<RowData> iter1 = reader.readBatch();
    final List<Row> rows1 = toRows(iter1, 1L, 0L);
    TestHelpers.assertRecords(rows1, recordBatchList.get(1), TestFixtures.SCHEMA);

    final BulkFormat.RecordIterator<RowData> iter2 = reader.readBatch();
    final List<Row> row2 = toRows(iter2, 2L, 0L);
    TestHelpers.assertRecords(row2, recordBatchList.get(2), TestFixtures.SCHEMA);
  }

  @Test
  public void testCheckpointedPositionMidSecondFile() throws IOException {
    final IcebergSourceSplit split = new IcebergSourceSplit(
        icebergSplit.task(),
        new CheckpointedPosition(1L, 1L));
    final BulkFormat.Reader<RowData> reader = getBulkFormat().restoreReader(flinkConfig, split);

    final BulkFormat.RecordIterator<RowData> iter1 = reader.readBatch();
    final List<Row> rows1 = toRows(iter1, 1L, 1L);
    TestHelpers.assertRecords(rows1, recordBatchList.get(1).subList(1, 2), TestFixtures.SCHEMA);

    final BulkFormat.RecordIterator<RowData> iter2 = reader.readBatch();
    final List<Row> row2 = toRows(iter2, 2L, 0L);
    TestHelpers.assertRecords(row2, recordBatchList.get(2), TestFixtures.SCHEMA);
  }

  private List<Row> toRows(final BulkFormat.RecordIterator<RowData> iter,
                           final long exptectedFileOffset,
                           final long startRecordOffset) {
    if (iter == null) {
      return Collections.emptyList();
    }
    final List<Row> result = new ArrayList<>();
    RecordAndPosition<RowData> recordAndPosition;
    long recordOffset = startRecordOffset;
    while ((recordAndPosition = iter.next()) != null) {
      Assert.assertEquals(exptectedFileOffset, recordAndPosition.getOffset());
      Assert.assertEquals(recordOffset, recordAndPosition.getRecordSkipCount() - 1);
      recordOffset++;
      final Row row = (Row) rowDataConverter.toExternal(recordAndPosition.getRecord());
      result.add(row);
    }
    // it is important to release the batch
    iter.releaseBatch();
    return result;
  }
}
