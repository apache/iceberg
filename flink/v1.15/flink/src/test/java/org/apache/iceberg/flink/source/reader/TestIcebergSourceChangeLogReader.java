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
import java.util.Arrays;
import java.util.List;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.IncrementalChangelogScan;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.HadoopTableResource;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.source.split.IcebergSourceChangeLogSplit;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestIcebergSourceChangeLogReader {
  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();
  private static final Schema SCHEMA = TestFixtures.SCHEMA;

  @Rule
  public final HadoopTableResource tableResource =
      new HadoopTableResource(
          TEMPORARY_FOLDER, TestFixtures.DATABASE, TestFixtures.TABLE, SCHEMA, TestFixtures.SPEC);

  private Table table;
  private DataFile dataFile1;
  private DataFile dataFile2;

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  private final Row row1 = Row.ofKind(RowKind.INSERT, "aa", 1L, "2021-01-01");
  private final Row row2 = Row.ofKind(RowKind.INSERT, "bb", 2L, "2021-01-01");
  private final Row row3 = Row.ofKind(RowKind.INSERT, "cc", 3L, "2021-01-01");
  private final Row row4 = Row.ofKind(RowKind.INSERT, "dd", 4L, "2021-01-01");
  private final Row row5 = Row.ofKind(RowKind.INSERT, "ee", 5L, "2021-01-01");

  private final Row row6 = Row.ofKind(RowKind.INSERT, "ff", 6L, "2021-01-03");
  private final Row row7 = Row.ofKind(RowKind.INSERT, "gg", 7L, "2021-01-03");
  private final Row row8 = Row.ofKind(RowKind.INSERT, "qq", 8L, "2021-01-03");

  private final List<Row> rows1 = Lists.newArrayList(row1, row2, row3, row4, row5);
  private final List<Row> rows1Delete =
      Lists.newArrayList(
          Row.ofKind(RowKind.DELETE, "aa", 1L, "2021-01-01"),
          Row.ofKind(RowKind.DELETE, "bb", 2L, "2021-01-01"),
          Row.ofKind(RowKind.DELETE, "cc", 3L, "2021-01-01"),
          Row.ofKind(RowKind.DELETE, "dd", 4L, "2021-01-01"),
          Row.ofKind(RowKind.DELETE, "ee", 5L, "2021-01-01"));
  private final List<Row> rows3 = Lists.newArrayList(row6, row7, row8);

  @Before
  public void before() throws IOException {
    table = tableResource.table();

    // write data to files
    dataFile1 = writeFile("2021-01-01", row1, row2, row3, row4, row5);
    dataFile2 = writeFile("2021-01-03", row6, row7, row8);
  }

  @Test
  public void testInsert() throws Exception {
    table.newAppend().appendFile(dataFile1).commit();
    table.newDelete().deleteFile(dataFile1).commit();

    CloseableIterable<ScanTaskGroup<ChangelogScanTask>> taskGroups = newScan().planTasks();

    TestingMetricGroup metricGroup = new TestingMetricGroup();
    TestingReaderContext readerContext = new TestingReaderContext(new Configuration(), metricGroup);
    IcebergSourceReader reader = createReader(metricGroup, readerContext);
    reader.start();
    IcebergTestingReaderOutput readerOutput =
        new IcebergTestingReaderOutput(FlinkSchemaUtil.convert(table.schema()));

    for (ScanTaskGroup<ChangelogScanTask> taskGroup : taskGroups) {
      IcebergSourceChangeLogSplit split = IcebergSourceSplit.fromChangeLogScanTask(taskGroup);
      reader.addSplits(Arrays.asList(split));
    }
    while (readerOutput.getEmittedRecords().size() < 10) {
      reader.pollNext(readerOutput);
    }

    List<RowData> emittedRecords = readerOutput.getEmittedRecords();
    List<Row> expected = Lists.newArrayList();
    expected.addAll(rows1);
    expected.addAll(rows1Delete);

    List<Row> result =
        org.apache.iceberg.flink.TestHelpers.convertRowDataToRow(
            emittedRecords, TestFixtures.ROW_TYPE);
    org.apache.iceberg.flink.TestHelpers.assertRows(result, expected);

    reader.pollNext(readerOutput);
  }

  @Test
  public void testMixDeleteAndInsert() throws Exception {
    table.newAppend().appendFile(dataFile1).commit();
    table.newDelete().deleteFile(dataFile1).commit();
    table.newAppend().appendFile(dataFile2).commit();

    CloseableIterable<ScanTaskGroup<ChangelogScanTask>> taskGroups = newScan().planTasks();

    TestingMetricGroup metricGroup = new TestingMetricGroup();
    TestingReaderContext readerContext = new TestingReaderContext(new Configuration(), metricGroup);
    IcebergSourceReader reader = createReader(metricGroup, readerContext);
    reader.start();

    IcebergTestingReaderOutput readerOutput =
        new IcebergTestingReaderOutput(FlinkSchemaUtil.convert(table.schema()));
    for (ScanTaskGroup<ChangelogScanTask> taskGroup : taskGroups) {
      IcebergSourceChangeLogSplit split = IcebergSourceSplit.fromChangeLogScanTask(taskGroup);
      reader.addSplits(Arrays.asList(split));
    }

    while (readerOutput.getEmittedRecords().size() < 13) {
      reader.pollNext(readerOutput);
    }

    List<RowData> emittedRecords = readerOutput.getEmittedRecords();

    List<Row> expected = Lists.newArrayList();
    expected.addAll(rows1);
    expected.addAll(rows1Delete);
    expected.addAll(rows3);

    List<Row> result =
        org.apache.iceberg.flink.TestHelpers.convertRowDataToRow(
            emittedRecords, TestFixtures.ROW_TYPE);
    org.apache.iceberg.flink.TestHelpers.assertRows(result, expected);

    reader.pollNext(readerOutput);
  }

  private IncrementalChangelogScan newScan() {
    return table.newIncrementalChangelogScan();
  }

  private DataFile writeFile(String partition, Row... rows) throws IOException {
    GenericAppenderHelper appender =
        new GenericAppenderHelper(table, FileFormat.AVRO, TEMPORARY_FOLDER);

    GenericRecord gRecord = GenericRecord.create(table.schema());
    List<Record> records = Lists.newArrayList();
    for (Row row : rows) {
      records.add(
          gRecord.copy(
              "data", row.getField(0),
              "id", row.getField(1),
              "dt", row.getField(2)));
    }

    return appender.writeFile(org.apache.iceberg.TestHelpers.Row.of(partition, 0), records);
  }

  private IcebergSourceReader createReader(
      MetricGroup metricGroup, SourceReaderContext readerContext) {
    IcebergSourceReaderMetrics readerMetrics =
        new IcebergSourceReaderMetrics(metricGroup, "db.tbl");
    RowChangeLogReaderFunction readerFunction =
        new RowChangeLogReaderFunction(
            new Configuration(),
            TestFixtures.SCHEMA,
            TestFixtures.SCHEMA,
            null,
            true,
            new HadoopFileIO(new org.apache.hadoop.conf.Configuration()),
            new PlaintextEncryptionManager(),
            null);
    return new IcebergSourceReader<>(readerMetrics, readerFunction, null, readerContext);
  }
}
