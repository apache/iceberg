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

import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Set;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.FlinkWriteConf;
import org.apache.iceberg.flink.FlinkWriteOptions;
import org.apache.iceberg.flink.RowDataConverter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestIcebergPartitionTimeStreamWriter {
  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  private String tablePath;
  private File folder;
  private Table table;

  private final FileFormat format;
  private final int formatVersion;

  @Parameterized.Parameters(name = "format = {0}, FormatVersion={1}")
  public static Object[][] parameters() {
    return new Object[][] {new Object[] {"avro", 1}, new Object[] {"avro", 2}};
  }

  public TestIcebergPartitionTimeStreamWriter(String format, int formatVersion) {
    this.format = FileFormat.fromString(format);
    this.formatVersion = formatVersion;
  }

  @After
  public void after() throws IOException {
    TestTables.clearTables();
  }

  @Before
  public void before() throws IOException {
    folder = tempFolder.newFolder();
    tablePath = folder.getAbsolutePath();

    // Construct the iceberg table.
    table = TestTables.create(folder, "test", SCHEMA, SPEC, formatVersion);

    table
        .updateProperties()
        .set(TableProperties.DEFAULT_FILE_FORMAT, format.name())
        .set(FlinkWriteOptions.SINK_PARTITION_COMMIT_ENABLED.key(), "true")
        .set(FlinkWriteOptions.SINK_PARTITION_COMMIT_DELAY.key(), "1h")
        .set(FlinkWriteOptions.PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN.key(), "$d $h:00:00")
        .commit();
  }

  @Test
  public void testWritingTable() throws Exception {
    long checkpointId = 1;
    long timestamp = 1;
    try (OneInputStreamOperatorTestHarness<RowData, WriteResult> testHarness =
        createPartitionStreamWriter()) {
      // The first checkpoint
      Record record1 = createRecord(table, 1, "data" + 1, "2022-02-02", "10");
      RowData rowData1 = RowDataConverter.convert(table.schema(), record1);
      testHarness.processElement(rowData1, timestamp);

      Record record2 = createRecord(table, 2, "data" + 2, "2022-02-02", "11");
      RowData rowData2 = RowDataConverter.convert(table.schema(), record2);
      testHarness.processElement(rowData2, timestamp);

      testHarness.processWatermark(
          LocalDateTime.of(2022, 2, 2, 11, 5).toInstant(ZoneOffset.UTC).toEpochMilli());
      testHarness.prepareSnapshotPreBarrier(checkpointId++);
      WriteResult result = WriteResult.builder().addAll(testHarness.extractOutputValues()).build();
      Assert.assertEquals(0, result.deleteFiles().length);
      Assert.assertEquals(1, result.dataFiles().length);

      // The second checkpoint
      testHarness.processWatermark(
          LocalDateTime.of(2022, 2, 2, 12, 5).toInstant(ZoneOffset.UTC).toEpochMilli());
      testHarness.prepareSnapshotPreBarrier(checkpointId);
      result = WriteResult.builder().addAll(testHarness.extractOutputValues()).build();
      Assert.assertEquals(0, result.deleteFiles().length);
      Assert.assertEquals(2, result.dataFiles().length);

      // The third checkpoint
      Record record3 = createRecord(table, 3, "data" + 3, "2022-02-02", "10");
      RowData rowData3 = RowDataConverter.convert(table.schema(), record3);
      testHarness.processElement(rowData3, timestamp++);

      Record record4 = createRecord(table, 4, "data" + 4, "2022-02-02", "11");
      RowData rowData4 = RowDataConverter.convert(table.schema(), record4);
      testHarness.processElement(rowData4, timestamp);

      Record record5 = createRecord(table, 5, "data" + 5, "2022-02-02", "12");
      RowData rowData5 = RowDataConverter.convert(table.schema(), record5);
      testHarness.processElement(rowData5, timestamp);

      testHarness.processWatermark(
          LocalDateTime.of(2022, 2, 2, 13, 5).toInstant(ZoneOffset.UTC).toEpochMilli());
      testHarness.prepareSnapshotPreBarrier(checkpointId);
      result = WriteResult.builder().addAll(testHarness.extractOutputValues()).build();
      Assert.assertEquals(0, result.deleteFiles().length);
      Assert.assertEquals(5, result.dataFiles().length);
    }
  }

  @Test
  public void testSnapshotTwice() throws Exception {
    long checkpointId = 1;
    long timestamp = 1;

    try (OneInputStreamOperatorTestHarness<RowData, WriteResult> testHarness =
        createPartitionStreamWriter()) {

      Record record1 = createRecord(table, 1, "data" + 1, "2022-02-02", "10");
      RowData rowData1 = RowDataConverter.convert(table.schema(), record1);
      testHarness.processElement(rowData1, timestamp++);

      Record record2 = createRecord(table, 2, "data" + 2, "2022-02-02", "11");
      RowData rowData2 = RowDataConverter.convert(table.schema(), record2);
      testHarness.processElement(rowData2, timestamp);

      // At 11h watermark , 11h data uncommitted
      testHarness.processWatermark(
          LocalDateTime.of(2022, 2, 2, 11, 5).toInstant(ZoneOffset.UTC).toEpochMilli());

      testHarness.prepareSnapshotPreBarrier(checkpointId++);

      Record record3 = createRecord(table, 3, "data" + 3, "2022-02-02", "10");
      RowData rowData3 = RowDataConverter.convert(table.schema(), record3);
      testHarness.processElement(rowData3, timestamp++);

      Record record4 = createRecord(table, 4, "data" + 4, "2022-02-02", "12");
      RowData rowData4 = RowDataConverter.convert(table.schema(), record4);
      testHarness.processElement(rowData4, timestamp);

      testHarness.processWatermark(
          LocalDateTime.of(2022, 2, 2, 13, 5).toInstant(ZoneOffset.UTC).toEpochMilli());

      testHarness.prepareSnapshotPreBarrier(checkpointId++);

      WriteResult result = WriteResult.builder().addAll(testHarness.extractOutputValues()).build();

      long expectedDataFiles = 4;
      WriteResult.builder().addAll(testHarness.extractOutputValues()).build();
      Assert.assertEquals(0, result.deleteFiles().length);
      Assert.assertEquals(expectedDataFiles, result.dataFiles().length);

      // snapshot again immediately.
      for (int i = 0; i < 5; i++) {
        testHarness.prepareSnapshotPreBarrier(checkpointId++);

        result = WriteResult.builder().addAll(testHarness.extractOutputValues()).build();
        Assert.assertEquals(0, result.deleteFiles().length);
        Assert.assertEquals(expectedDataFiles, result.dataFiles().length);
      }
    }
  }

  @Test
  public void testTableWithoutSnapshot() throws Exception {
    try (OneInputStreamOperatorTestHarness<RowData, WriteResult> testHarness =
        createPartitionStreamWriter()) {
      Assert.assertEquals(0, testHarness.extractOutputValues().size());
    }
    // Even if we closed the iceberg stream writer, there's no orphan data file.
    Assert.assertEquals(0, scanDataFiles().size());

    try (OneInputStreamOperatorTestHarness<RowData, WriteResult> testHarness =
        createPartitionStreamWriter()) {
      Record record1 = createRecord(table, 1, "data" + 1, "2022-02-28", "10");
      RowData rowData1 = RowDataConverter.convert(table.schema(), record1);
      testHarness.processElement(rowData1, 1);

      testHarness.processWatermark(
          LocalDateTime.of(2022, 2, 28, 10, 30).toInstant(ZoneOffset.UTC).toEpochMilli());
      Assert.assertEquals(0, testHarness.extractOutputValues().size());

      // At 11h watermark, still not emit the data file yet, because there is no checkpoint.
      testHarness.processWatermark(
          LocalDateTime.of(2022, 2, 28, 11, 30).toInstant(ZoneOffset.UTC).toEpochMilli());
      Assert.assertEquals(0, testHarness.extractOutputValues().size());
    }
    // Once we closed the iceberg stream writer, there will left an orphan data file.
    Assert.assertEquals(1, scanDataFiles().size());
  }

  @Test
  public void testBoundedStreamCloseWithEmittingDataFiles() throws Exception {
    long timestamp = 1;

    try (OneInputStreamOperatorTestHarness<RowData, WriteResult> testHarness =
        createPartitionStreamWriter()) {
      Record record1 = createRecord(table, 1, "data" + 1, "2022-02-02", "10");
      RowData rowData1 = RowDataConverter.convert(table.schema(), record1);
      testHarness.processElement(rowData1, timestamp++);

      Record record2 = createRecord(table, 2, "data" + 2, "2022-02-02", "11");
      RowData rowData2 = RowDataConverter.convert(table.schema(), record2);
      testHarness.processElement(rowData2, timestamp);

      testHarness.processWatermark(
          LocalDateTime.of(2022, 2, 2, 11, 5).toInstant(ZoneOffset.UTC).toEpochMilli());
      Assertions.assertThat(testHarness.getOneInputOperator()).isInstanceOf(BoundedOneInput.class);
      // When calling endInput, ignore the watermark and commit all data.
      ((BoundedOneInput) testHarness.getOneInputOperator()).endInput();
      // At 11h watermark , 10h-11h data committed
      WriteResult result = WriteResult.builder().addAll(testHarness.extractOutputValues()).build();
      Assert.assertEquals(0, result.deleteFiles().length);
      Assert.assertEquals(2, result.dataFiles().length);

      // invoke endInput again.
      testHarness.processWatermark(
          LocalDateTime.of(2022, 2, 2, 12, 35).toInstant(ZoneOffset.UTC).toEpochMilli());
      ((BoundedOneInput) testHarness.getOneInputOperator()).endInput();

      result = WriteResult.builder().addAll(testHarness.extractOutputValues()).build();
      Assert.assertEquals(0, result.deleteFiles().length);
      // Datafiles should not be sent again
      Assert.assertEquals(2, result.dataFiles().length);
    }
  }

  @Test
  public void testBoundedStreamTriggeredEndInputBeforeTriggeringCheckpoint() throws Exception {
    long timestamp = 1;

    try (OneInputStreamOperatorTestHarness<RowData, WriteResult> testHarness =
        createPartitionStreamWriter()) {
      Record record1 = createRecord(table, 1, "data" + 1, "2022-02-02", "10");
      RowData rowData1 = RowDataConverter.convert(table.schema(), record1);
      testHarness.processElement(rowData1, timestamp++);

      Record record2 = createRecord(table, 2, "data" + 2, "2022-02-02", "11");
      RowData rowData2 = RowDataConverter.convert(table.schema(), record2);
      testHarness.processElement(rowData2, timestamp++);

      testHarness.processWatermark(
          LocalDateTime.of(2022, 2, 2, 11, 5).toInstant(ZoneOffset.UTC).toEpochMilli());
      testHarness.endInput();

      WriteResult result = WriteResult.builder().addAll(testHarness.extractOutputValues()).build();
      Assert.assertEquals(0, result.deleteFiles().length);
      Assert.assertEquals(2, result.dataFiles().length);

      Record record3 = createRecord(table, 3, "data" + 3, "2022-02-03", "11");
      RowData rowData3 = RowDataConverter.convert(table.schema(), record3);
      testHarness.processElement(rowData3, timestamp);

      testHarness.prepareSnapshotPreBarrier(1L);

      result = WriteResult.builder().addAll(testHarness.extractOutputValues()).build();
      Assert.assertEquals(0, result.deleteFiles().length);
      // It should be ensured that after endInput is triggered, when prepareSnapshotPreBarrier
      // is triggered, write should only send WriteResult once
      Assert.assertEquals(3, result.dataFiles().length);
    }
  }

  private Set<String> scanDataFiles() throws IOException {
    Path dataDir = new Path(tablePath, "data");
    FileSystem fs = FileSystem.get(new Configuration());
    if (!fs.exists(dataDir)) {
      return ImmutableSet.of();
    } else {
      Set<String> paths = Sets.newHashSet();
      RemoteIterator<LocatedFileStatus> iterators = fs.listFiles(dataDir, true);
      while (iterators.hasNext()) {
        LocatedFileStatus status = iterators.next();
        if (status.isFile()) {
          Path path = status.getPath();
          if (path.getName().endsWith("." + format.toString().toLowerCase())) {
            paths.add(path.toString());
          }
        }
      }
      return paths;
    }
  }

  // Schema passed to create tables
  public static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          required(2, "data", Types.StringType.get()),
          required(3, "d", Types.StringType.get()),
          required(4, "h", Types.StringType.get()));

  protected static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("d").identity("h").build();

  private static Record createRecord(
      Table table, Integer id, String data, String day, String hour) {
    Record record = GenericRecord.create(table.schema());
    record.setField("id", id);
    record.setField("data", data);
    record.setField("d", day);
    record.setField("h", hour);
    return record;
  }

  private OneInputStreamOperatorTestHarness<RowData, WriteResult> createPartitionStreamWriter()
      throws Exception {
    return createPartitionStreamWriter(table, FlinkSchemaUtil.toSchema(SCHEMA));
  }

  private OneInputStreamOperatorTestHarness<RowData, WriteResult> createPartitionStreamWriter(
      Table icebergTable, TableSchema flinkSchema) throws Exception {
    RowType flinkRowType = FlinkSink.toFlinkRowType(icebergTable.schema(), flinkSchema);
    FlinkWriteConf flinkWriteConfig =
        new FlinkWriteConf(
            icebergTable, Maps.newHashMap(), new org.apache.flink.configuration.Configuration());

    IcebergStreamWriter<RowData> streamWriter =
        FlinkSink.createStreamWriter(icebergTable, flinkWriteConfig, flinkRowType, null);
    OneInputStreamOperatorTestHarness<RowData, WriteResult> harness =
        new OneInputStreamOperatorTestHarness<>(streamWriter, 1, 1, 0);

    harness.setup();
    harness.open();

    return harness;
  }
}
