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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkWriteConf;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
public class TestIcebergStreamWriter {
  @TempDir protected java.nio.file.Path temporaryFolder;

  private Table table;

  @Parameter(index = 0)
  private FileFormat format;

  @Parameter(index = 1)
  private boolean partitioned;

  @Parameters(name = "format = {0}, partitioned = {1}")
  public static Object[][] parameters() {
    return new Object[][] {
      {FileFormat.AVRO, true},
      {FileFormat.AVRO, false},
      {FileFormat.ORC, true},
      {FileFormat.ORC, false},
      {FileFormat.PARQUET, true},
      {FileFormat.PARQUET, false}
    };
  }

  @BeforeEach
  public void before() throws IOException {
    File folder = Files.createTempDirectory(temporaryFolder, "junit").toFile();
    // Construct the iceberg table.
    Map<String, String> props = ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name());
    table = SimpleDataUtil.createTable(folder.getAbsolutePath(), props, partitioned);
  }

  @TestTemplate
  public void testWritingTable() throws Exception {
    long checkpointId = 1L;
    try (OneInputStreamOperatorTestHarness<RowData, WriteResult> testHarness =
        createIcebergStreamWriter()) {
      // The first checkpoint
      testHarness.processElement(SimpleDataUtil.createRowData(1, "hello"), 1);
      testHarness.processElement(SimpleDataUtil.createRowData(2, "world"), 1);
      testHarness.processElement(SimpleDataUtil.createRowData(3, "hello"), 1);

      testHarness.prepareSnapshotPreBarrier(checkpointId);
      int expectedDataFiles = partitioned ? 2 : 1;
      WriteResult result = WriteResult.builder().addAll(testHarness.extractOutputValues()).build();
      assertThat(result.deleteFiles()).isEmpty();
      assertThat(result.dataFiles()).hasSize(expectedDataFiles);

      checkpointId = checkpointId + 1;

      // The second checkpoint
      testHarness.processElement(SimpleDataUtil.createRowData(4, "foo"), 1);
      testHarness.processElement(SimpleDataUtil.createRowData(5, "bar"), 2);

      testHarness.prepareSnapshotPreBarrier(checkpointId);
      expectedDataFiles = partitioned ? 4 : 2;
      result = WriteResult.builder().addAll(testHarness.extractOutputValues()).build();
      assertThat(result.deleteFiles()).isEmpty();
      assertThat(result.dataFiles()).hasSize(expectedDataFiles);

      // Commit the iceberg transaction.
      AppendFiles appendFiles = table.newAppend();
      Arrays.stream(result.dataFiles()).forEach(appendFiles::appendFile);
      appendFiles.commit();

      // Assert the table records.
      SimpleDataUtil.assertTableRecords(
          table,
          Lists.newArrayList(
              SimpleDataUtil.createRecord(1, "hello"),
              SimpleDataUtil.createRecord(2, "world"),
              SimpleDataUtil.createRecord(3, "hello"),
              SimpleDataUtil.createRecord(4, "foo"),
              SimpleDataUtil.createRecord(5, "bar")));
    }
  }

  @TestTemplate
  public void testSnapshotTwice() throws Exception {
    long checkpointId = 1;
    long timestamp = 1;
    try (OneInputStreamOperatorTestHarness<RowData, WriteResult> testHarness =
        createIcebergStreamWriter()) {
      testHarness.processElement(SimpleDataUtil.createRowData(1, "hello"), timestamp++);
      testHarness.processElement(SimpleDataUtil.createRowData(2, "world"), timestamp);

      testHarness.prepareSnapshotPreBarrier(checkpointId++);
      int expectedDataFiles = partitioned ? 2 : 1;
      WriteResult result = WriteResult.builder().addAll(testHarness.extractOutputValues()).build();
      assertThat(result.deleteFiles()).isEmpty();
      assertThat(result.dataFiles()).hasSize(expectedDataFiles);

      // snapshot again immediately.
      for (int i = 0; i < 5; i++) {
        testHarness.prepareSnapshotPreBarrier(checkpointId++);

        result = WriteResult.builder().addAll(testHarness.extractOutputValues()).build();
        assertThat(result.deleteFiles()).isEmpty();
        assertThat(result.dataFiles()).hasSize(expectedDataFiles);
      }
    }
  }

  @TestTemplate
  public void testTableWithoutSnapshot() throws Exception {
    try (OneInputStreamOperatorTestHarness<RowData, WriteResult> testHarness =
        createIcebergStreamWriter()) {
      assertThat(testHarness.extractOutputValues()).isEmpty();
    }
    // Even if we closed the iceberg stream writer, there's no orphan data file.
    assertThat(scanDataFiles()).isEmpty();

    try (OneInputStreamOperatorTestHarness<RowData, WriteResult> testHarness =
        createIcebergStreamWriter()) {
      testHarness.processElement(SimpleDataUtil.createRowData(1, "hello"), 1);
      // Still not emit the data file yet, because there is no checkpoint.
      assertThat(testHarness.extractOutputValues()).isEmpty();
    }
    // Once we closed the iceberg stream writer, there will left an orphan data file.
    assertThat(scanDataFiles()).hasSize(1);
  }

  private Set<String> scanDataFiles() throws IOException {
    Path dataDir = new Path(table.location(), "data");
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
          if (path.getName().endsWith("." + format.toString().toLowerCase(Locale.ROOT))) {
            paths.add(path.toString());
          }
        }
      }
      return paths;
    }
  }

  @TestTemplate
  public void testBoundedStreamCloseWithEmittingDataFiles() throws Exception {
    try (OneInputStreamOperatorTestHarness<RowData, WriteResult> testHarness =
        createIcebergStreamWriter()) {
      testHarness.processElement(SimpleDataUtil.createRowData(1, "hello"), 1);
      testHarness.processElement(SimpleDataUtil.createRowData(2, "world"), 2);

      assertThat(testHarness.getOneInputOperator()).isInstanceOf(BoundedOneInput.class);
      ((BoundedOneInput) testHarness.getOneInputOperator()).endInput();

      int expectedDataFiles = partitioned ? 2 : 1;
      WriteResult result = WriteResult.builder().addAll(testHarness.extractOutputValues()).build();
      assertThat(result.deleteFiles()).isEmpty();
      assertThat(result.dataFiles()).hasSize(expectedDataFiles);

      ((BoundedOneInput) testHarness.getOneInputOperator()).endInput();

      result = WriteResult.builder().addAll(testHarness.extractOutputValues()).build();
      assertThat(result.deleteFiles()).isEmpty();
      // Datafiles should not be sent again
      assertThat(result.dataFiles()).hasSize(expectedDataFiles);
    }
  }

  @TestTemplate
  public void testBoundedStreamTriggeredEndInputBeforeTriggeringCheckpoint() throws Exception {
    try (OneInputStreamOperatorTestHarness<RowData, WriteResult> testHarness =
        createIcebergStreamWriter()) {
      testHarness.processElement(SimpleDataUtil.createRowData(1, "hello"), 1);
      testHarness.processElement(SimpleDataUtil.createRowData(2, "world"), 2);

      testHarness.endInput();

      int expectedDataFiles = partitioned ? 2 : 1;
      WriteResult result = WriteResult.builder().addAll(testHarness.extractOutputValues()).build();
      assertThat(result.deleteFiles()).isEmpty();
      assertThat(result.dataFiles()).hasSize(expectedDataFiles);

      testHarness.prepareSnapshotPreBarrier(1L);

      result = WriteResult.builder().addAll(testHarness.extractOutputValues()).build();
      assertThat(result.deleteFiles()).isEmpty();
      // It should be ensured that after endInput is triggered, when prepareSnapshotPreBarrier
      // is triggered, write should only send WriteResult once
      assertThat(result.dataFiles()).hasSize(expectedDataFiles);
    }
  }

  @TestTemplate
  public void testTableWithTargetFileSize() throws Exception {
    // Adjust the target-file-size in table properties.
    table
        .updateProperties()
        .set(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES, "4") // ~4 bytes; low enough to trigger
        .commit();

    List<RowData> rows = Lists.newArrayListWithCapacity(8000);
    List<Record> records = Lists.newArrayListWithCapacity(8000);
    for (int i = 0; i < 2000; i++) {
      for (String data : new String[] {"a", "b", "c", "d"}) {
        rows.add(SimpleDataUtil.createRowData(i, data));
        records.add(SimpleDataUtil.createRecord(i, data));
      }
    }

    try (OneInputStreamOperatorTestHarness<RowData, WriteResult> testHarness =
        createIcebergStreamWriter()) {
      for (RowData row : rows) {
        testHarness.processElement(row, 1);
      }

      // snapshot the operator.
      testHarness.prepareSnapshotPreBarrier(1);
      WriteResult result = WriteResult.builder().addAll(testHarness.extractOutputValues()).build();
      assertThat(result.deleteFiles()).isEmpty();
      assertThat(result.dataFiles()).hasSize(8);

      // Assert that the data file have the expected records.
      for (DataFile dataFile : result.dataFiles()) {
        assertThat(dataFile.recordCount()).isEqualTo(1000);
      }

      // Commit the iceberg transaction.
      AppendFiles appendFiles = table.newAppend();
      Arrays.stream(result.dataFiles()).forEach(appendFiles::appendFile);
      appendFiles.commit();
    }

    // Assert the table records.
    SimpleDataUtil.assertTableRecords(table, records);
  }

  @TestTemplate
  public void testPromotedFlinkDataType() throws Exception {
    Schema iSchema =
        new Schema(
            Types.NestedField.required(1, "tinyint", Types.IntegerType.get()),
            Types.NestedField.required(2, "smallint", Types.IntegerType.get()),
            Types.NestedField.optional(3, "int", Types.IntegerType.get()));
    TableSchema flinkSchema =
        TableSchema.builder()
            .field("tinyint", DataTypes.TINYINT().notNull())
            .field("smallint", DataTypes.SMALLINT().notNull())
            .field("int", DataTypes.INT().nullable())
            .build();

    PartitionSpec spec;
    if (partitioned) {
      spec =
          PartitionSpec.builderFor(iSchema)
              .identity("smallint")
              .identity("tinyint")
              .identity("int")
              .build();
    } else {
      spec = PartitionSpec.unpartitioned();
    }

    String location =
        Files.createTempDirectory(temporaryFolder, "junit").toFile().getAbsolutePath();
    Map<String, String> props = ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name());
    Table icebergTable = new HadoopTables().create(iSchema, spec, props, location);

    List<RowData> rows =
        Lists.newArrayList(
            GenericRowData.of((byte) 0x01, (short) -32768, 101),
            GenericRowData.of((byte) 0x02, (short) 0, 102),
            GenericRowData.of((byte) 0x03, (short) 32767, 103));

    Record record = GenericRecord.create(iSchema);
    List<Record> expected =
        Lists.newArrayList(
            record.copy(ImmutableMap.of("tinyint", 1, "smallint", -32768, "int", 101)),
            record.copy(ImmutableMap.of("tinyint", 2, "smallint", 0, "int", 102)),
            record.copy(ImmutableMap.of("tinyint", 3, "smallint", 32767, "int", 103)));

    try (OneInputStreamOperatorTestHarness<RowData, WriteResult> testHarness =
        createIcebergStreamWriter(icebergTable, flinkSchema)) {
      for (RowData row : rows) {
        testHarness.processElement(row, 1);
      }
      testHarness.prepareSnapshotPreBarrier(1);
      WriteResult result = WriteResult.builder().addAll(testHarness.extractOutputValues()).build();
      assertThat(result.deleteFiles()).isEmpty();
      assertThat(result.dataFiles()).hasSize(partitioned ? 3 : 1);

      // Commit the iceberg transaction.
      AppendFiles appendFiles = icebergTable.newAppend();
      Arrays.stream(result.dataFiles()).forEach(appendFiles::appendFile);
      appendFiles.commit();
    }

    SimpleDataUtil.assertTableRecords(location, expected);
  }

  private OneInputStreamOperatorTestHarness<RowData, WriteResult> createIcebergStreamWriter()
      throws Exception {
    return createIcebergStreamWriter(table, SimpleDataUtil.FLINK_SCHEMA);
  }

  private OneInputStreamOperatorTestHarness<RowData, WriteResult> createIcebergStreamWriter(
      Table icebergTable, TableSchema flinkSchema) throws Exception {
    RowType flinkRowType = FlinkSink.toFlinkRowType(icebergTable.schema(), flinkSchema);
    FlinkWriteConf flinkWriteConfig =
        new FlinkWriteConf(
            icebergTable, Maps.newHashMap(), new org.apache.flink.configuration.Configuration());

    IcebergStreamWriter<RowData> streamWriter =
        FlinkSink.createStreamWriter(() -> icebergTable, flinkWriteConfig, flinkRowType, null);
    OneInputStreamOperatorTestHarness<RowData, WriteResult> harness =
        new OneInputStreamOperatorTestHarness<>(streamWriter, 1, 1, 0);

    harness.setup();
    harness.open();

    return harness;
  }
}
