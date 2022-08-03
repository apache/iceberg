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

import java.util.Arrays;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SourceReaderOptions;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.flink.source.DataIterator;
import org.apache.iceberg.io.CloseableIterator;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestArrayPoolDataIteratorBatcherRowData {

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();
  private static final FileFormat fileFormat = FileFormat.PARQUET;

  private final GenericAppenderFactory appenderFactory;
  private final DataIteratorBatcher<RowData> batcher;

  public TestArrayPoolDataIteratorBatcherRowData() {
    Configuration config = new Configuration();
    // set array pool size to 1
    config.set(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY, 1);
    // set batch array size to 2
    config.set(FlinkConfigOptions.SOURCE_READER_FETCH_BATCH_RECORD_COUNT, 2);
    this.batcher =
        new ArrayPoolDataIteratorBatcher<>(config, new RowDataRecordFactory(TestFixtures.ROW_TYPE));
    this.appenderFactory = new GenericAppenderFactory(TestFixtures.SCHEMA);
  }

  /** Read a CombinedScanTask that contains a single file with less than a full batch of records */
  @Test
  public void testSingleFileLessThanOneFullBatch() throws Exception {
    List<Record> records = RandomGenericData.generate(TestFixtures.SCHEMA, 1, 1);
    FileScanTask fileTask =
        ReaderUtil.createFileTask(records, TEMPORARY_FOLDER.newFile(), fileFormat, appenderFactory);
    CombinedScanTask combinedTask = new BaseCombinedScanTask(fileTask);
    DataIterator<RowData> dataIterator = ReaderUtil.createDataIterator(combinedTask);
    String splitId = "someSplitId";
    CloseableIterator<RecordsWithSplitIds<RecordAndPosition<RowData>>> recordBatchIterator =
        batcher.batch(splitId, dataIterator);

    ArrayBatchRecords<RowData> batch = (ArrayBatchRecords<RowData>) recordBatchIterator.next();
    Assert.assertTrue(batch.finishedSplits().isEmpty());
    Assert.assertEquals(splitId, batch.nextSplit());
    // reusable array size should be the configured value of 2
    Assert.assertEquals(2, batch.records().length);
    // assert actual number of records in the array
    Assert.assertEquals(1, batch.numberOfRecords());

    RecordAndPosition<RowData> recordAndPosition = batch.nextRecordFromSplit();

    ///////////////////////////////
    // assert first record

    Assert.assertEquals(0, recordAndPosition.fileOffset());
    // The position points to where the reader should resume after this record is processed.
    Assert.assertEquals(1, recordAndPosition.recordOffset());
    TestHelpers.assertRowData(TestFixtures.SCHEMA, records.get(0), recordAndPosition.record());

    Assert.assertNull(batch.nextRecordFromSplit());
    Assert.assertNull(batch.nextSplit());
    batch.recycle();

    // assert end of input
    Assert.assertFalse(recordBatchIterator.hasNext());
  }

  /**
   * Read a CombinedScanTask that contains a single file with multiple batches.
   *
   * <p>Insert 5 records in a single file that should result in 3 batches
   */
  @Test
  public void testSingleFileWithMultipleBatches() throws Exception {
    List<Record> records = RandomGenericData.generate(TestFixtures.SCHEMA, 5, 1);
    FileScanTask fileTask =
        ReaderUtil.createFileTask(records, TEMPORARY_FOLDER.newFile(), fileFormat, appenderFactory);
    CombinedScanTask combinedTask = new BaseCombinedScanTask(fileTask);
    DataIterator<RowData> dataIterator = ReaderUtil.createDataIterator(combinedTask);
    String splitId = "someSplitId";
    CloseableIterator<RecordsWithSplitIds<RecordAndPosition<RowData>>> recordBatchIterator =
        batcher.batch(splitId, dataIterator);

    ///////////////////////////////
    // assert first batch with full batch of 2 records

    ArrayBatchRecords<RowData> batch0 = (ArrayBatchRecords<RowData>) recordBatchIterator.next();
    Assert.assertTrue(batch0.finishedSplits().isEmpty());
    Assert.assertEquals(splitId, batch0.nextSplit());
    // reusable array size should be the configured value of 2
    Assert.assertEquals(2, batch0.records().length);
    // assert actual number of records in the array
    Assert.assertEquals(2, batch0.numberOfRecords());

    RecordAndPosition<RowData> recordAndPosition;

    // assert first record
    recordAndPosition = batch0.nextRecordFromSplit();
    Assert.assertEquals(0, recordAndPosition.fileOffset());
    // The position points to where the reader should resume after this record is processed.
    Assert.assertEquals(1, recordAndPosition.recordOffset());
    TestHelpers.assertRowData(TestFixtures.SCHEMA, records.get(0), recordAndPosition.record());

    // assert second record
    recordAndPosition = batch0.nextRecordFromSplit();
    Assert.assertEquals(0, recordAndPosition.fileOffset());
    // The position points to where the reader should resume after this record is processed.
    Assert.assertEquals(2, recordAndPosition.recordOffset());
    TestHelpers.assertRowData(TestFixtures.SCHEMA, records.get(1), recordAndPosition.record());

    Assert.assertNull(batch0.nextRecordFromSplit());
    Assert.assertNull(batch0.nextSplit());
    batch0.recycle();

    ///////////////////////////////
    // assert second batch with full batch of 2 records

    ArrayBatchRecords<RowData> batch1 = (ArrayBatchRecords<RowData>) recordBatchIterator.next();
    // assert array is reused
    Assert.assertSame(batch0.records(), batch1.records());
    Assert.assertTrue(batch1.finishedSplits().isEmpty());
    Assert.assertEquals(splitId, batch1.nextSplit());
    // reusable array size should be the configured value of 2
    Assert.assertEquals(2, batch1.records().length);
    // assert actual number of records in the array
    Assert.assertEquals(2, batch1.numberOfRecords());

    // assert third record
    recordAndPosition = batch1.nextRecordFromSplit();
    Assert.assertEquals(0, recordAndPosition.fileOffset());
    // The position points to where the reader should resume after this record is processed.
    Assert.assertEquals(3, recordAndPosition.recordOffset());
    TestHelpers.assertRowData(TestFixtures.SCHEMA, records.get(2), recordAndPosition.record());

    // assert fourth record
    recordAndPosition = batch1.nextRecordFromSplit();
    Assert.assertEquals(0, recordAndPosition.fileOffset());
    // The position points to where the reader should resume after this record is processed.
    Assert.assertEquals(4, recordAndPosition.recordOffset());
    TestHelpers.assertRowData(TestFixtures.SCHEMA, records.get(3), recordAndPosition.record());

    Assert.assertNull(batch1.nextRecordFromSplit());
    Assert.assertNull(batch1.nextSplit());
    batch1.recycle();

    ///////////////////////////////
    // assert third batch with partial batch of 1 record

    ArrayBatchRecords<RowData> batch2 = (ArrayBatchRecords<RowData>) recordBatchIterator.next();
    // assert array is reused
    Assert.assertSame(batch0.records(), batch2.records());
    Assert.assertTrue(batch2.finishedSplits().isEmpty());
    Assert.assertEquals(splitId, batch2.nextSplit());
    // reusable array size should be the configured value of 2
    Assert.assertEquals(2, batch2.records().length);
    // assert actual number of records in the array
    Assert.assertEquals(1, batch2.numberOfRecords());

    // assert fifth record
    recordAndPosition = batch2.nextRecordFromSplit();
    Assert.assertEquals(0, recordAndPosition.fileOffset());
    // The position points to where the reader should resume after this record is processed.
    Assert.assertEquals(5, recordAndPosition.recordOffset());
    TestHelpers.assertRowData(TestFixtures.SCHEMA, records.get(4), recordAndPosition.record());

    Assert.assertNull(batch2.nextRecordFromSplit());
    Assert.assertNull(batch2.nextSplit());
    batch2.recycle();

    // assert end of input
    Assert.assertFalse(recordBatchIterator.hasNext());
  }

  /**
   * Read a CombinedScanTask that contains with multiple files.
   *
   * <p>In this test, we also seek the iterator to starting position (1, 1).
   */
  @Test
  public void testMultipleFilesWithSeekPosition() throws Exception {
    List<Record> records0 = RandomGenericData.generate(TestFixtures.SCHEMA, 1, 1);
    FileScanTask fileTask0 =
        ReaderUtil.createFileTask(
            records0, TEMPORARY_FOLDER.newFile(), fileFormat, appenderFactory);
    List<Record> records1 = RandomGenericData.generate(TestFixtures.SCHEMA, 4, 2);
    FileScanTask fileTask1 =
        ReaderUtil.createFileTask(
            records1, TEMPORARY_FOLDER.newFile(), fileFormat, appenderFactory);
    List<Record> records2 = RandomGenericData.generate(TestFixtures.SCHEMA, 3, 3);
    FileScanTask fileTask2 =
        ReaderUtil.createFileTask(
            records2, TEMPORARY_FOLDER.newFile(), fileFormat, appenderFactory);
    CombinedScanTask combinedTask =
        new BaseCombinedScanTask(Arrays.asList(fileTask0, fileTask1, fileTask2));

    DataIterator<RowData> dataIterator = ReaderUtil.createDataIterator(combinedTask);
    // seek to file1 and after record 1
    dataIterator.seek(1, 1);

    String splitId = "someSplitId";
    CloseableIterator<RecordsWithSplitIds<RecordAndPosition<RowData>>> recordBatchIterator =
        batcher.batch(splitId, dataIterator);

    ///////////////////////////////
    // file0 is skipped by seek

    ///////////////////////////////
    // file1 has 4 records. because the seek position, first record is skipped.
    // we should read 3 remaining records in 2 batches:
    // batch10 with 2 records and batch11 with 1 records.

    // assert first batch from file1 with full batch of 2 records

    // variable naming convention: batch<fileOffset><batchId>
    ArrayBatchRecords<RowData> batch10 = (ArrayBatchRecords<RowData>) recordBatchIterator.next();
    Assert.assertTrue(batch10.finishedSplits().isEmpty());
    Assert.assertEquals(splitId, batch10.nextSplit());
    // reusable array size should be the configured value of 2
    Assert.assertEquals(2, batch10.records().length);
    // assert actual number of records in the array
    Assert.assertEquals(2, batch10.numberOfRecords());

    RecordAndPosition<RowData> recordAndPosition;

    recordAndPosition = batch10.nextRecordFromSplit();
    Assert.assertEquals(1, recordAndPosition.fileOffset());
    // seek should skip the first record in file1. starting from the second record
    Assert.assertEquals(2, recordAndPosition.recordOffset());
    TestHelpers.assertRowData(TestFixtures.SCHEMA, records1.get(1), recordAndPosition.record());

    recordAndPosition = batch10.nextRecordFromSplit();
    Assert.assertEquals(1, recordAndPosition.fileOffset());
    // The position points to where the reader should resume after this record is processed.
    Assert.assertEquals(3, recordAndPosition.recordOffset());
    TestHelpers.assertRowData(TestFixtures.SCHEMA, records1.get(2), recordAndPosition.record());

    Assert.assertNull(batch10.nextRecordFromSplit());
    Assert.assertNull(batch10.nextSplit());
    batch10.recycle();

    // assert second batch from file1 with partial batch of 1 record

    // variable naming convention: batch_<fileOffset>_<batchId>
    ArrayBatchRecords<RowData> batch11 = (ArrayBatchRecords<RowData>) recordBatchIterator.next();
    // assert array is reused
    Assert.assertSame(batch10.records(), batch11.records());
    Assert.assertTrue(batch11.finishedSplits().isEmpty());
    Assert.assertEquals(splitId, batch11.nextSplit());
    // reusable array size should be the configured value of 2
    Assert.assertEquals(2, batch11.records().length);
    // assert actual number of records in the array
    Assert.assertEquals(1, batch11.numberOfRecords());

    recordAndPosition = batch11.nextRecordFromSplit();
    Assert.assertEquals(1, recordAndPosition.fileOffset());
    // The position points to where the reader should resume after this record is processed.
    Assert.assertEquals(4, recordAndPosition.recordOffset());
    TestHelpers.assertRowData(TestFixtures.SCHEMA, records1.get(3), recordAndPosition.record());

    Assert.assertNull(batch11.nextRecordFromSplit());
    Assert.assertNull(batch11.nextSplit());
    batch11.recycle();

    ///////////////////////////////
    // file2 has 3 records.
    // we should read 3 records in 2 batches:
    // batch20 with 2 records and batch21 with 1 records

    // assert first batch from file2 with full batch of 2 records

    // variable naming convention: batch_<fileOffset>_<batchId>
    ArrayBatchRecords<RowData> batch20 = (ArrayBatchRecords<RowData>) recordBatchIterator.next();
    // assert array is reused
    Assert.assertSame(batch10.records(), batch20.records());
    Assert.assertTrue(batch20.finishedSplits().isEmpty());
    Assert.assertEquals(splitId, batch20.nextSplit());
    // reusable array size should be the configured value of 2
    Assert.assertEquals(2, batch20.records().length);
    // assert actual number of records in the array
    Assert.assertEquals(2, batch20.numberOfRecords());

    recordAndPosition = batch20.nextRecordFromSplit();
    Assert.assertEquals(2, recordAndPosition.fileOffset());
    // The position points to where the reader should resume after this record is processed.
    Assert.assertEquals(1, recordAndPosition.recordOffset());
    TestHelpers.assertRowData(TestFixtures.SCHEMA, records2.get(0), recordAndPosition.record());

    recordAndPosition = batch20.nextRecordFromSplit();
    Assert.assertEquals(2, recordAndPosition.fileOffset());
    // The position points to where the reader should resume after this record is processed.
    Assert.assertEquals(2, recordAndPosition.recordOffset());
    TestHelpers.assertRowData(TestFixtures.SCHEMA, records2.get(1), recordAndPosition.record());

    Assert.assertNull(batch20.nextRecordFromSplit());
    Assert.assertNull(batch20.nextSplit());
    batch20.recycle();

    ///////////////////////////////
    // assert second batch from file2 with partial batch of 1 record

    // variable naming convention: batch_<fileOffset>_<batchId>
    ArrayBatchRecords<RowData> batch21 = (ArrayBatchRecords<RowData>) recordBatchIterator.next();
    // assert array is reused
    Assert.assertSame(batch10.records(), batch21.records());
    Assert.assertTrue(batch21.finishedSplits().isEmpty());
    Assert.assertEquals(splitId, batch21.nextSplit());
    // reusable array size should be the configured value of 2
    Assert.assertEquals(2, batch21.records().length);
    // assert actual number of records in the array
    Assert.assertEquals(1, batch21.numberOfRecords());

    recordAndPosition = batch21.nextRecordFromSplit();
    Assert.assertEquals(2, recordAndPosition.fileOffset());
    // The position points to where the reader should resume after this record is processed.
    Assert.assertEquals(3, recordAndPosition.recordOffset());
    TestHelpers.assertRowData(TestFixtures.SCHEMA, records2.get(2), recordAndPosition.record());

    Assert.assertNull(batch21.nextRecordFromSplit());
    Assert.assertNull(batch21.nextSplit());
    batch21.recycle();

    // assert end of input
    Assert.assertFalse(recordBatchIterator.hasNext());
  }
}
