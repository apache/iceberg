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
import java.util.Collections;
import java.util.List;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.split.SerializableComparator;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestIcebergSourceReader {
  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  private final GenericAppenderFactory appenderFactory;

  public TestIcebergSourceReader() {
    this.appenderFactory = new GenericAppenderFactory(TestFixtures.SCHEMA);
  }

  @Test
  public void testReaderMetrics() throws Exception {
    TestingReaderOutput<RowData> readerOutput = new TestingReaderOutput<>();
    TestingMetricGroup metricGroup = new TestingMetricGroup();
    TestingReaderContext readerContext = new TestingReaderContext(new Configuration(), metricGroup);
    IcebergSourceReader reader = createReader(metricGroup, readerContext, null);
    reader.start();

    testOneSplitFetcher(reader, readerOutput, metricGroup, 1);
    testOneSplitFetcher(reader, readerOutput, metricGroup, 2);
  }

  @Test
  public void testReaderOrder() throws Exception {
    // Create 2 splits
    List<List<Record>> recordBatchList1 =
        ReaderUtil.createRecordBatchList(0L, TestFixtures.SCHEMA, 1, 1);
    CombinedScanTask task1 =
        ReaderUtil.createCombinedScanTask(
            recordBatchList1, TEMPORARY_FOLDER, FileFormat.PARQUET, appenderFactory);

    List<List<Record>> recordBatchList2 =
        ReaderUtil.createRecordBatchList(1L, TestFixtures.SCHEMA, 1, 1);
    CombinedScanTask task2 =
        ReaderUtil.createCombinedScanTask(
            recordBatchList2, TEMPORARY_FOLDER, FileFormat.PARQUET, appenderFactory);

    // Sort the splits in one way
    List<RowData> rowDataList1 =
        read(
            Arrays.asList(
                IcebergSourceSplit.fromCombinedScanTask(task1),
                IcebergSourceSplit.fromCombinedScanTask(task2)),
            2);

    // Reverse the splits
    List<RowData> rowDataList2 =
        read(
            Arrays.asList(
                IcebergSourceSplit.fromCombinedScanTask(task2),
                IcebergSourceSplit.fromCombinedScanTask(task1)),
            2);

    // Check that the order of the elements is not changed
    Assert.assertEquals(rowDataList1.get(0), rowDataList2.get(0));
    Assert.assertEquals(rowDataList1.get(1), rowDataList2.get(1));
  }

  private List<RowData> read(List<IcebergSourceSplit> splits, long expected) throws Exception {
    TestingMetricGroup metricGroup = new TestingMetricGroup();
    TestingReaderContext readerContext = new TestingReaderContext(new Configuration(), metricGroup);
    // Using IdBasedComparator, so we can have a deterministic order of the splits
    IcebergSourceReader reader = createReader(metricGroup, readerContext, new IdBasedComparator());
    reader.start();

    reader.addSplits(splits);
    TestingReaderOutput<RowData> readerOutput = new TestingReaderOutput<>();
    while (readerOutput.getEmittedRecords().size() < expected) {
      reader.pollNext(readerOutput);
    }

    reader.pollNext(readerOutput);

    Assert.assertEquals(expected, readerOutput.getEmittedRecords().size());
    return readerOutput.getEmittedRecords();
  }

  private void testOneSplitFetcher(
      IcebergSourceReader reader,
      TestingReaderOutput<RowData> readerOutput,
      TestingMetricGroup metricGroup,
      int expectedCount)
      throws Exception {
    long seed = expectedCount;
    // Each split should contain only one file with one record
    List<List<Record>> recordBatchList =
        ReaderUtil.createRecordBatchList(seed, TestFixtures.SCHEMA, 1, 1);
    CombinedScanTask task =
        ReaderUtil.createCombinedScanTask(
            recordBatchList, TEMPORARY_FOLDER, FileFormat.PARQUET, appenderFactory);
    IcebergSourceSplit split = IcebergSourceSplit.fromCombinedScanTask(task);
    reader.addSplits(Collections.singletonList(split));

    while (readerOutput.getEmittedRecords().size() < expectedCount) {
      reader.pollNext(readerOutput);
    }

    Assert.assertEquals(expectedCount, readerOutput.getEmittedRecords().size());
    TestHelpers.assertRowData(
        TestFixtures.SCHEMA,
        recordBatchList.get(0).get(0),
        readerOutput.getEmittedRecords().get(expectedCount - 1));
    Assert.assertEquals(expectedCount, metricGroup.counters().get("assignedSplits").getCount());

    // One more poll will get null record batch.
    // That will finish the split and cause split fetcher to be closed due to idleness.
    // Then next split will create a new split reader.
    reader.pollNext(readerOutput);
  }

  private IcebergSourceReader createReader(
      MetricGroup metricGroup,
      SourceReaderContext readerContext,
      SerializableComparator<IcebergSourceSplit> splitComparator) {
    IcebergSourceReaderMetrics readerMetrics =
        new IcebergSourceReaderMetrics(metricGroup, "db.tbl");
    RowDataReaderFunction readerFunction =
        new RowDataReaderFunction(
            new Configuration(),
            TestFixtures.SCHEMA,
            TestFixtures.SCHEMA,
            null,
            true,
            new HadoopFileIO(new org.apache.hadoop.conf.Configuration()),
            new PlaintextEncryptionManager(),
            Collections.emptyList());
    return new IcebergSourceReader<>(
        SerializableRecordEmitter.defaultEmitter(),
        readerMetrics,
        readerFunction,
        splitComparator,
        readerContext);
  }

  private static class IdBasedComparator implements SerializableComparator<IcebergSourceSplit> {
    @Override
    public int compare(IcebergSourceSplit o1, IcebergSourceSplit o2) {
      return o1.splitId().compareTo(o2.splitId());
    }
  }
}
