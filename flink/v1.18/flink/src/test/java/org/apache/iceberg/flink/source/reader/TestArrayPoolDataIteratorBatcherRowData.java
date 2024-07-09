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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.nio.file.Path;
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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestArrayPoolDataIteratorBatcherRowData {

  @TempDir protected Path temporaryFolder;
  private static final FileFormat FILE_FORMAT = FileFormat.PARQUET;
  private static final Configuration config = new Configuration();

  private final GenericAppenderFactory appenderFactory =
      new GenericAppenderFactory(TestFixtures.SCHEMA);;
  private final DataIteratorBatcher<RowData> batcher =
      new ArrayPoolDataIteratorBatcher<>(config, new RowDataRecordFactory(TestFixtures.ROW_TYPE));

  @BeforeAll
  public static void setConfig() {
    // set array pool size to 1
    config.set(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY, 1);
    // set batch array size to 2
    config.set(FlinkConfigOptions.SOURCE_READER_FETCH_BATCH_RECORD_COUNT, 2);
  }

  /** Read a CombinedScanTask that contains a single file with less than a full batch of records */
  @Test
  public void testSingleFileLessThanOneFullBatch() throws Exception {
    List<Record> records = RandomGenericData.generate(TestFixtures.SCHEMA, 1, 1);
    FileScanTask fileTask =
        ReaderUtil.createFileTask(
            records,
            File.createTempFile("junit", null, temporaryFolder.toFile()),
            FILE_FORMAT,
            appenderFactory);
    CombinedScanTask combinedTask = new BaseCombinedScanTask(fileTask);
    DataIterator<RowData> dataIterator = ReaderUtil.createDataIterator(combinedTask);
    String splitId = "someSplitId";
    CloseableIterator<RecordsWithSplitIds<RecordAndPosition<RowData>>> recordBatchIterator =
        batcher.batch(splitId, dataIterator);

    ArrayBatchRecords<RowData> batch = (ArrayBatchRecords<RowData>) recordBatchIterator.next();
    assertThat(batch.finishedSplits()).isEmpty();
    assertThat(batch.nextSplit()).isEqualTo(splitId);
    assertThat(batch.records()).hasSize(2);
    assertThat(batch.numberOfRecords()).isEqualTo(1);

    RecordAndPosition<RowData> recordAndPosition = batch.nextRecordFromSplit();

    ///////////////////////////////
    // assert first record

    assertThat(recordAndPosition.fileOffset()).isEqualTo(0);
    assertThat(recordAndPosition.recordOffset())
        .as("The position points to where the reader should resume after this record is processed.")
        .isEqualTo(1);
    TestHelpers.assertRowData(TestFixtures.SCHEMA, records.get(0), recordAndPosition.record());

    assertThat(batch.nextRecordFromSplit()).isNull();
    assertThat(batch.nextSplit()).isNull();
    batch.recycle();

    assertThat(recordBatchIterator).isExhausted();
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
        ReaderUtil.createFileTask(
            records,
            File.createTempFile("junit", null, temporaryFolder.toFile()),
            FILE_FORMAT,
            appenderFactory);
    CombinedScanTask combinedTask = new BaseCombinedScanTask(fileTask);
    DataIterator<RowData> dataIterator = ReaderUtil.createDataIterator(combinedTask);
    String splitId = "someSplitId";
    CloseableIterator<RecordsWithSplitIds<RecordAndPosition<RowData>>> recordBatchIterator =
        batcher.batch(splitId, dataIterator);

    ///////////////////////////////
    // assert first batch with full batch of 2 records

    ArrayBatchRecords<RowData> batch0 = (ArrayBatchRecords<RowData>) recordBatchIterator.next();
    assertThat(batch0.finishedSplits()).isEmpty();
    assertThat(batch0.nextSplit()).isEqualTo(splitId);
    assertThat(batch0.records()).hasSize(2);
    assertThat(batch0.numberOfRecords()).isEqualTo(2);

    RecordAndPosition<RowData> recordAndPosition;

    // assert first record
    recordAndPosition = batch0.nextRecordFromSplit();
    assertThat(recordAndPosition.fileOffset()).isEqualTo(0);
    assertThat(recordAndPosition.recordOffset())
        .as("The position points to where the reader should resume after this record is processed.")
        .isEqualTo(1);
    TestHelpers.assertRowData(TestFixtures.SCHEMA, records.get(0), recordAndPosition.record());

    // assert second record
    recordAndPosition = batch0.nextRecordFromSplit();
    assertThat(recordAndPosition.fileOffset()).isEqualTo(0);
    assertThat(recordAndPosition.recordOffset())
        .as("The position points to where the reader should resume after this record is processed.")
        .isEqualTo(2);
    TestHelpers.assertRowData(TestFixtures.SCHEMA, records.get(1), recordAndPosition.record());

    assertThat(batch0.nextRecordFromSplit()).isNull();
    assertThat(batch0.nextSplit()).isNull();
    batch0.recycle();

    ///////////////////////////////
    // assert second batch with full batch of 2 records

    ArrayBatchRecords<RowData> batch1 = (ArrayBatchRecords<RowData>) recordBatchIterator.next();
    assertThat(batch1.records()).containsExactlyInAnyOrder(batch0.records());
    assertThat(batch1.finishedSplits()).isEmpty();
    assertThat(batch1.nextSplit()).isEqualTo(splitId);
    assertThat(batch1.records()).hasSize(2);
    assertThat(batch1.numberOfRecords()).isEqualTo(2);

    // assert third record
    recordAndPosition = batch1.nextRecordFromSplit();
    assertThat(recordAndPosition.fileOffset()).isEqualTo(0);
    assertThat(recordAndPosition.recordOffset())
        .as("The position points to where the reader should resume after this record is processed.")
        .isEqualTo(3);
    TestHelpers.assertRowData(TestFixtures.SCHEMA, records.get(2), recordAndPosition.record());

    // assert fourth record
    recordAndPosition = batch1.nextRecordFromSplit();
    assertThat(recordAndPosition.fileOffset()).isEqualTo(0);
    assertThat(recordAndPosition.recordOffset())
        .as("The position points to where the reader should resume after this record is processed.")
        .isEqualTo(4);
    TestHelpers.assertRowData(TestFixtures.SCHEMA, records.get(3), recordAndPosition.record());

    assertThat(batch1.nextRecordFromSplit()).isNull();
    assertThat(batch1.nextSplit()).isNull();
    batch1.recycle();

    ///////////////////////////////
    // assert third batch with partial batch of 1 record

    ArrayBatchRecords<RowData> batch2 = (ArrayBatchRecords<RowData>) recordBatchIterator.next();
    assertThat(batch2.records()).containsExactlyInAnyOrder(batch0.records());
    assertThat(batch2.finishedSplits()).isEmpty();
    assertThat(batch2.nextSplit()).isEqualTo(splitId);
    assertThat(batch2.records()).hasSize(2);
    assertThat(batch2.numberOfRecords()).isEqualTo(1);

    // assert fifth record
    recordAndPosition = batch2.nextRecordFromSplit();
    assertThat(recordAndPosition.fileOffset()).isEqualTo(0);
    assertThat(recordAndPosition.recordOffset())
        .as("The position points to where the reader should resume after this record is processed.")
        .isEqualTo(5);
    TestHelpers.assertRowData(TestFixtures.SCHEMA, records.get(4), recordAndPosition.record());

    assertThat(batch2.nextRecordFromSplit()).isNull();
    assertThat(batch2.nextSplit()).isNull();
    batch2.recycle();

    assertThat(recordBatchIterator).isExhausted();
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
            records0,
            File.createTempFile("junit", null, temporaryFolder.toFile()),
            FILE_FORMAT,
            appenderFactory);
    List<Record> records1 = RandomGenericData.generate(TestFixtures.SCHEMA, 4, 2);
    FileScanTask fileTask1 =
        ReaderUtil.createFileTask(
            records1,
            File.createTempFile("junit", null, temporaryFolder.toFile()),
            FILE_FORMAT,
            appenderFactory);
    List<Record> records2 = RandomGenericData.generate(TestFixtures.SCHEMA, 3, 3);
    FileScanTask fileTask2 =
        ReaderUtil.createFileTask(
            records2,
            File.createTempFile("junit", null, temporaryFolder.toFile()),
            FILE_FORMAT,
            appenderFactory);
    CombinedScanTask combinedTask =
        new BaseCombinedScanTask(Arrays.asList(fileTask0, fileTask1, fileTask2));

    DataIterator<RowData> dataIterator = ReaderUtil.createDataIterator(combinedTask);
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
    assertThat(batch10.finishedSplits()).isEmpty();
    assertThat(batch10.nextSplit()).isEqualTo(splitId);
    assertThat(batch10.records()).hasSize(2);
    assertThat(batch10.numberOfRecords()).isEqualTo(2);

    RecordAndPosition<RowData> recordAndPosition;

    recordAndPosition = batch10.nextRecordFromSplit();
    assertThat(recordAndPosition.fileOffset()).isEqualTo(1);
    assertThat(recordAndPosition.recordOffset())
        .as("seek should skip the first record in file1. starting from the second record")
        .isEqualTo(2);
    TestHelpers.assertRowData(TestFixtures.SCHEMA, records1.get(1), recordAndPosition.record());

    recordAndPosition = batch10.nextRecordFromSplit();
    assertThat(recordAndPosition.fileOffset()).isEqualTo(1);
    assertThat(recordAndPosition.recordOffset())
        .as("The position points to where the reader should resume after this record is processed.")
        .isEqualTo(3);
    TestHelpers.assertRowData(TestFixtures.SCHEMA, records1.get(2), recordAndPosition.record());

    assertThat(batch10.nextRecordFromSplit()).isNull();
    assertThat(batch10.nextSplit()).isNull();
    batch10.recycle();

    // assert second batch from file1 with partial batch of 1 record

    // variable naming convention: batch_<fileOffset>_<batchId>
    ArrayBatchRecords<RowData> batch11 = (ArrayBatchRecords<RowData>) recordBatchIterator.next();
    assertThat(batch11.records()).containsExactlyInAnyOrder(batch10.records());
    assertThat(batch11.finishedSplits()).isEmpty();
    assertThat(batch11.nextSplit()).isEqualTo(splitId);
    assertThat(batch11.records()).hasSize(2);
    assertThat(batch11.numberOfRecords()).isEqualTo(1);

    recordAndPosition = batch11.nextRecordFromSplit();
    assertThat(recordAndPosition.fileOffset()).isEqualTo(1);
    assertThat(recordAndPosition.recordOffset())
        .as("The position points to where the reader should resume after this record is processed.")
        .isEqualTo(4);
    TestHelpers.assertRowData(TestFixtures.SCHEMA, records1.get(3), recordAndPosition.record());

    assertThat(batch11.nextRecordFromSplit()).isNull();
    assertThat(batch11.nextSplit()).isNull();
    batch11.recycle();

    ///////////////////////////////
    // file2 has 3 records.
    // we should read 3 records in 2 batches:
    // batch20 with 2 records and batch21 with 1 records

    // assert first batch from file2 with full batch of 2 records

    // variable naming convention: batch_<fileOffset>_<batchId>
    ArrayBatchRecords<RowData> batch20 = (ArrayBatchRecords<RowData>) recordBatchIterator.next();
    assertThat(batch20.records()).containsExactlyInAnyOrder(batch10.records());
    assertThat(batch20.finishedSplits()).isEmpty();
    assertThat(batch20.nextSplit()).isEqualTo(splitId);
    assertThat(batch20.records()).hasSize(2);
    assertThat(batch20.numberOfRecords()).isEqualTo(2);

    recordAndPosition = batch20.nextRecordFromSplit();
    assertThat(recordAndPosition.fileOffset()).isEqualTo(2);
    assertThat(recordAndPosition.recordOffset())
        .as("The position points to where the reader should resume after this record is processed.")
        .isEqualTo(1);
    TestHelpers.assertRowData(TestFixtures.SCHEMA, records2.get(0), recordAndPosition.record());

    recordAndPosition = batch20.nextRecordFromSplit();
    assertThat(recordAndPosition.fileOffset()).isEqualTo(2);
    assertThat(recordAndPosition.recordOffset())
        .as("The position points to where the reader should resume after this record is processed.")
        .isEqualTo(2);
    TestHelpers.assertRowData(TestFixtures.SCHEMA, records2.get(1), recordAndPosition.record());

    assertThat(batch20.nextRecordFromSplit()).isNull();
    assertThat(batch20.nextSplit()).isNull();
    batch20.recycle();

    ///////////////////////////////
    // assert second batch from file2 with partial batch of 1 record

    // variable naming convention: batch_<fileOffset>_<batchId>
    ArrayBatchRecords<RowData> batch21 = (ArrayBatchRecords<RowData>) recordBatchIterator.next();
    assertThat(batch21.records()).containsExactlyInAnyOrder(batch10.records());
    assertThat(batch21.finishedSplits()).isEmpty();
    assertThat(batch21.nextSplit()).isEqualTo(splitId);
    assertThat(batch21.records()).hasSize(2);
    assertThat(batch21.numberOfRecords()).isEqualTo(1);

    recordAndPosition = batch21.nextRecordFromSplit();
    assertThat(recordAndPosition.fileOffset()).isEqualTo(2);

    assertThat(recordAndPosition.recordOffset())
        .as("The position points to where the reader should resume after this record is processed.")
        .isEqualTo(3);
    TestHelpers.assertRowData(TestFixtures.SCHEMA, records2.get(2), recordAndPosition.record());

    assertThat(batch21.nextRecordFromSplit()).isNull();
    assertThat(batch21.nextSplit()).isNull();
    batch21.recycle();

    assertThat(recordBatchIterator).isExhausted();
  }
}
