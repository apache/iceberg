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
package org.apache.iceberg.flink.sink.writer;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkWriteConf;
import org.apache.iceberg.flink.FlinkWriteOptions;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.sink.SinkTestUtil;
import org.apache.iceberg.flink.sink.committer.SinkV2Committable;
import org.apache.iceberg.flink.source.reader.TestingMetricGroup;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestSinkV2Writer {
  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();
  private Table table;

  private final FileFormat format;
  private final boolean partitioned;

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

  public TestSinkV2Writer(String format, boolean partitioned) {
    this.format = FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH));
    this.partitioned = partitioned;
  }

  @Before
  public void before() throws IOException {
    File folder = tempFolder.newFolder();
    // Construct the iceberg table.
    Map<String, String> props = ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name());
    table = SimpleDataUtil.createTable(folder.getAbsolutePath(), props, partitioned);
  }

  @Test
  public void testPreCommit() throws Exception {
    SinkV2Writer sinkV2Writer = createIcebergStreamWriter();
    sinkV2Writer.write(SimpleDataUtil.createRowData(1, "hello"), new ContextImpl());
    sinkV2Writer.write(SimpleDataUtil.createRowData(2, "hello"), new ContextImpl());
    sinkV2Writer.write(SimpleDataUtil.createRowData(3, "hello"), new ContextImpl());
    sinkV2Writer.write(SimpleDataUtil.createRowData(4, "hello"), new ContextImpl());
    sinkV2Writer.write(SimpleDataUtil.createRowData(5, "hello"), new ContextImpl());

    Collection<SinkV2Committable> sinkV2Committables = sinkV2Writer.prepareCommit();
    Assert.assertEquals(1, sinkV2Committables.size());
  }

  @Test
  public void testWritingTable() throws Exception {
    SinkV2Writer sinkV2Writer = createIcebergStreamWriter();
    // The first checkpoint
    sinkV2Writer.write(SimpleDataUtil.createRowData(1, "hello"), new ContextImpl());
    sinkV2Writer.write(SimpleDataUtil.createRowData(2, "world"), new ContextImpl());
    sinkV2Writer.write(SimpleDataUtil.createRowData(3, "hello"), new ContextImpl());

    Collection<SinkV2Committable> sinkV2Committables = sinkV2Writer.prepareCommit();
    Assert.assertEquals(1, sinkV2Committables.size());

    AppendFiles appendFiles = table.newAppend();

    WriteResult result = sinkV2Committables.iterator().next().writeResult();

    long expectedDataFiles = partitioned ? 2 : 1;
    Assert.assertEquals(0, result.deleteFiles().length);
    Assert.assertEquals(expectedDataFiles, result.dataFiles().length);

    Arrays.stream(result.dataFiles()).forEach(appendFiles::appendFile);

    // The second checkpoint
    sinkV2Writer.write(SimpleDataUtil.createRowData(4, "foo"), new ContextImpl());
    sinkV2Writer.write(SimpleDataUtil.createRowData(5, "bar"), new ContextImpl());

    sinkV2Committables = sinkV2Writer.prepareCommit();

    result = sinkV2Committables.iterator().next().writeResult();

    Assert.assertEquals(0, result.deleteFiles().length);
    Assert.assertEquals(expectedDataFiles, result.dataFiles().length);

    // Commit the iceberg transaction.
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

  @Test
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

    try (SinkV2Writer sinkV2Writer = createIcebergStreamWriter()) {
      for (RowData row : rows) {
        sinkV2Writer.write(row, new ContextImpl());
      }
      Collection<SinkV2Committable> sinkV2Committables = sinkV2Writer.prepareCommit();
      WriteResult result = sinkV2Committables.iterator().next().writeResult();

      Assert.assertEquals(0, result.deleteFiles().length);
      Assert.assertEquals(8, result.dataFiles().length);

      // Assert that the data file have the expected records.
      for (DataFile dataFile : result.dataFiles()) {
        Assert.assertEquals(1000, dataFile.recordCount());
      }

      // Commit the iceberg transaction.
      AppendFiles appendFiles = table.newAppend();
      Arrays.stream(result.dataFiles()).forEach(appendFiles::appendFile);
      appendFiles.commit();
    }
    // Assert the table records.
    SimpleDataUtil.assertTableRecords(table, records);
  }

  @Test
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

    String location = tempFolder.newFolder().getAbsolutePath();
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

    SinkV2Writer sinkV2Writer = createIcebergStreamWriter(icebergTable, flinkSchema);
    for (RowData row : rows) {
      sinkV2Writer.write(row, new ContextImpl());
    }

    Collection<SinkV2Committable> sinkV2Committables = sinkV2Writer.prepareCommit();

    WriteResult result = sinkV2Committables.iterator().next().writeResult();

    Assert.assertEquals(0, result.deleteFiles().length);
    Assert.assertEquals(partitioned ? 3 : 1, result.dataFiles().length);

    // Commit the iceberg transaction.
    AppendFiles appendFiles = icebergTable.newAppend();
    Arrays.stream(result.dataFiles()).forEach(appendFiles::appendFile);
    appendFiles.commit();

    SimpleDataUtil.assertTableRecords(location, expected);
  }

  // ------------------------------- Utility Methods --------------------------------

  private static class ContextImpl implements SinkWriter.Context {
    private final long watermark;
    private final Long timestamp;

    ContextImpl() {
      this(0, 0L);
    }

    private ContextImpl(long watermark, Long timestamp) {
      this.watermark = watermark;
      this.timestamp = timestamp;
    }

    @Override
    public long currentWatermark() {
      return watermark;
    }

    @Override
    public Long timestamp() {
      return timestamp;
    }
  }

  SinkV2Writer createIcebergStreamWriter() {
    return createIcebergStreamWriter(table, SimpleDataUtil.FLINK_SCHEMA);
  }

  SinkV2Writer createIcebergStreamWriter(Table icebergTable, TableSchema flinkSchema) {
    RowType flinkRowType = SinkTestUtil.toFlinkRowType(icebergTable.schema(), flinkSchema);

    return createStreamWriter(icebergTable, flinkRowType, null, false);
  }

  SinkV2Writer createStreamWriter(
      Table newTable,
      RowType newFlinkRowType,
      List<Integer> newEqualityFieldIds,
      boolean newUpsert) {
    Map<String, String> writeOptions = Maps.newHashMap();
    writeOptions.put(FlinkWriteOptions.WRITE_UPSERT_ENABLED.key(), Boolean.toString(newUpsert));
    FlinkWriteConf flinkWriteConf = new FlinkWriteConf(newTable, writeOptions, new Configuration());

    RowDataTaskWriterFactory taskWriterFactory =
        new RowDataTaskWriterFactory(
            SerializableTable.copyOf(newTable),
            newFlinkRowType,
            flinkWriteConf.targetDataFileSize(),
            flinkWriteConf.dataFileFormat(),
            table.properties(),
            newEqualityFieldIds,
            flinkWriteConf.upsertMode());

    IcebergStreamWriterMetrics metrics =
        new IcebergStreamWriterMetrics(new TestingMetricGroup(), "name");

    return new SinkV2Writer(() -> table, taskWriterFactory, metrics, 1, 1);
  }
}
