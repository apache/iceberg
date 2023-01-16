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

import java.io.IOException;
import java.util.List;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public abstract class ReaderFunctionTestBase<T> {

  @Parameterized.Parameters(name = "fileFormat={0}")
  public static Object[][] parameters() {
    return new Object[][] {
      new Object[] {FileFormat.AVRO},
      new Object[] {FileFormat.ORC},
      new Object[] {FileFormat.PARQUET}
    };
  }

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  protected abstract ReaderFunction<T> readerFunction();

  protected abstract void assertRecords(List<Record> expected, List<T> actual, Schema schema);

  private final FileFormat fileFormat;
  private final GenericAppenderFactory appenderFactory;

  public ReaderFunctionTestBase(FileFormat fileFormat) {
    this.fileFormat = fileFormat;
    this.appenderFactory = new GenericAppenderFactory(TestFixtures.SCHEMA);
  }

  private void assertRecordsAndPosition(
      List<Record> expectedRecords,
      int expectedFileOffset,
      long startRecordOffset,
      RecordsWithSplitIds<RecordAndPosition<T>> batch) {
    batch.nextSplit();
    List<T> actualRecords = Lists.newArrayList();
    long recordOffset = startRecordOffset;
    RecordAndPosition<T> recordAndPosition;
    while ((recordAndPosition = batch.nextRecordFromSplit()) != null) {
      actualRecords.add(recordAndPosition.record());
      Assert.assertEquals(
          "expected file offset", expectedFileOffset, recordAndPosition.fileOffset());
      Assert.assertEquals(
          "expected record offset", recordOffset, recordAndPosition.recordOffset() - 1);
      recordOffset++;
    }

    Assert.assertEquals("expected record count", expectedRecords.size(), actualRecords.size());
    assertRecords(expectedRecords, actualRecords, TestFixtures.SCHEMA);
  }

  @Test
  public void testNoCheckpointedPosition() throws IOException {
    List<List<Record>> recordBatchList =
        ReaderUtil.createRecordBatchList(TestFixtures.SCHEMA, 3, 2);
    CombinedScanTask combinedScanTask =
        ReaderUtil.createCombinedScanTask(
            recordBatchList, TEMPORARY_FOLDER, fileFormat, appenderFactory);
    IcebergSourceSplit split = IcebergSourceSplit.fromCombinedScanTask(combinedScanTask);
    CloseableIterator<RecordsWithSplitIds<RecordAndPosition<T>>> reader =
        readerFunction().apply(split);

    RecordsWithSplitIds<RecordAndPosition<T>> batch0 = reader.next();
    assertRecordsAndPosition(recordBatchList.get(0), 0, 0L, batch0);
    batch0.recycle();

    RecordsWithSplitIds<RecordAndPosition<T>> batch1 = reader.next();
    assertRecordsAndPosition(recordBatchList.get(1), 1, 0L, batch1);
    batch1.recycle();

    RecordsWithSplitIds<RecordAndPosition<T>> batch2 = reader.next();
    assertRecordsAndPosition(recordBatchList.get(2), 2, 0L, batch2);
    batch2.recycle();
  }

  @Test
  public void testCheckpointedPositionBeforeFirstFile() throws IOException {
    List<List<Record>> recordBatchList =
        ReaderUtil.createRecordBatchList(TestFixtures.SCHEMA, 3, 2);
    CombinedScanTask combinedScanTask =
        ReaderUtil.createCombinedScanTask(
            recordBatchList, TEMPORARY_FOLDER, fileFormat, appenderFactory);
    IcebergSourceSplit split = IcebergSourceSplit.fromCombinedScanTask(combinedScanTask, 0, 0L);
    CloseableIterator<RecordsWithSplitIds<RecordAndPosition<T>>> reader =
        readerFunction().apply(split);

    RecordsWithSplitIds<RecordAndPosition<T>> batch0 = reader.next();
    assertRecordsAndPosition(recordBatchList.get(0), 0, 0L, batch0);
    batch0.recycle();

    RecordsWithSplitIds<RecordAndPosition<T>> batch1 = reader.next();
    assertRecordsAndPosition(recordBatchList.get(1), 1, 0L, batch1);
    batch1.recycle();

    RecordsWithSplitIds<RecordAndPosition<T>> batch2 = reader.next();
    assertRecordsAndPosition(recordBatchList.get(2), 2, 0L, batch2);
    batch2.recycle();
  }

  @Test
  public void testCheckpointedPositionMiddleFirstFile() throws IOException {
    List<List<Record>> recordBatchList =
        ReaderUtil.createRecordBatchList(TestFixtures.SCHEMA, 3, 2);
    CombinedScanTask combinedScanTask =
        ReaderUtil.createCombinedScanTask(
            recordBatchList, TEMPORARY_FOLDER, fileFormat, appenderFactory);
    IcebergSourceSplit split = IcebergSourceSplit.fromCombinedScanTask(combinedScanTask, 0, 1L);
    CloseableIterator<RecordsWithSplitIds<RecordAndPosition<T>>> reader =
        readerFunction().apply(split);

    RecordsWithSplitIds<RecordAndPosition<T>> batch0 = reader.next();
    assertRecordsAndPosition(recordBatchList.get(0).subList(1, 2), 0, 1L, batch0);
    batch0.recycle();

    RecordsWithSplitIds<RecordAndPosition<T>> batch1 = reader.next();
    assertRecordsAndPosition(recordBatchList.get(1), 1, 0L, batch1);
    batch1.recycle();

    RecordsWithSplitIds<RecordAndPosition<T>> batch2 = reader.next();
    assertRecordsAndPosition(recordBatchList.get(2), 2, 0L, batch2);
    batch2.recycle();
  }

  @Test
  public void testCheckpointedPositionAfterFirstFile() throws IOException {
    List<List<Record>> recordBatchList =
        ReaderUtil.createRecordBatchList(TestFixtures.SCHEMA, 3, 2);
    CombinedScanTask combinedScanTask =
        ReaderUtil.createCombinedScanTask(
            recordBatchList, TEMPORARY_FOLDER, fileFormat, appenderFactory);
    IcebergSourceSplit split = IcebergSourceSplit.fromCombinedScanTask(combinedScanTask, 0, 2L);
    CloseableIterator<RecordsWithSplitIds<RecordAndPosition<T>>> reader =
        readerFunction().apply(split);

    RecordsWithSplitIds<RecordAndPosition<T>> batch1 = reader.next();
    assertRecordsAndPosition(recordBatchList.get(1), 1, 0L, batch1);
    batch1.recycle();

    RecordsWithSplitIds<RecordAndPosition<T>> batch2 = reader.next();
    assertRecordsAndPosition(recordBatchList.get(2), 2, 0L, batch2);
    batch2.recycle();
  }

  @Test
  public void testCheckpointedPositionBeforeSecondFile() throws IOException {
    List<List<Record>> recordBatchList =
        ReaderUtil.createRecordBatchList(TestFixtures.SCHEMA, 3, 2);
    CombinedScanTask combinedScanTask =
        ReaderUtil.createCombinedScanTask(
            recordBatchList, TEMPORARY_FOLDER, fileFormat, appenderFactory);
    IcebergSourceSplit split = IcebergSourceSplit.fromCombinedScanTask(combinedScanTask, 1, 0L);
    CloseableIterator<RecordsWithSplitIds<RecordAndPosition<T>>> reader =
        readerFunction().apply(split);

    RecordsWithSplitIds<RecordAndPosition<T>> batch1 = reader.next();
    assertRecordsAndPosition(recordBatchList.get(1), 1, 0L, batch1);
    batch1.recycle();

    RecordsWithSplitIds<RecordAndPosition<T>> batch2 = reader.next();
    assertRecordsAndPosition(recordBatchList.get(2), 2, 0L, batch2);
    batch2.recycle();
  }

  @Test
  public void testCheckpointedPositionMidSecondFile() throws IOException {
    List<List<Record>> recordBatchList =
        ReaderUtil.createRecordBatchList(TestFixtures.SCHEMA, 3, 2);
    CombinedScanTask combinedScanTask =
        ReaderUtil.createCombinedScanTask(
            recordBatchList, TEMPORARY_FOLDER, fileFormat, appenderFactory);
    IcebergSourceSplit split = IcebergSourceSplit.fromCombinedScanTask(combinedScanTask, 1, 1L);
    CloseableIterator<RecordsWithSplitIds<RecordAndPosition<T>>> reader =
        readerFunction().apply(split);

    RecordsWithSplitIds<RecordAndPosition<T>> batch1 = reader.next();
    assertRecordsAndPosition(recordBatchList.get(1).subList(1, 2), 1, 1L, batch1);
    batch1.recycle();

    RecordsWithSplitIds<RecordAndPosition<T>> batch2 = reader.next();
    assertRecordsAndPosition(recordBatchList.get(2), 2, 0L, batch2);
    batch2.recycle();
  }
}
