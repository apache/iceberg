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
import java.util.Map;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.common.DynFields;
import org.apache.iceberg.flink.FlinkWriteConf;
import org.apache.iceberg.flink.FlinkWriteOptions;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestCompressionSettings {
  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  private Table table;

  private final Map<String, String> initProperties;

  @Parameterized.Parameters(name = "tableProperties = {0}")
  public static Object[] parameters() {
    return new Object[] {
      ImmutableMap.of(),
      ImmutableMap.of(
          TableProperties.AVRO_COMPRESSION,
          "zstd",
          TableProperties.AVRO_COMPRESSION_LEVEL,
          "3",
          TableProperties.PARQUET_COMPRESSION,
          "zstd",
          TableProperties.PARQUET_COMPRESSION_LEVEL,
          "3",
          TableProperties.ORC_COMPRESSION,
          "zstd",
          TableProperties.ORC_COMPRESSION_STRATEGY,
          "compression")
    };
  }

  public TestCompressionSettings(Map<String, String> initProperties) {
    this.initProperties = initProperties;
  }

  @Before
  public void before() throws IOException {
    File folder = tempFolder.newFolder();
    table = SimpleDataUtil.createTable(folder.getAbsolutePath(), initProperties, false);
  }

  @Test
  public void testCompressionAvro() throws Exception {
    // No override provided
    Map<String, String> resultProperties =
        appenderProperties(
            table,
            SimpleDataUtil.FLINK_SCHEMA,
            ImmutableMap.of(FlinkWriteOptions.WRITE_FORMAT.key(), "AVRO"));

    if (initProperties.get(TableProperties.AVRO_COMPRESSION) == null) {
      Assert.assertEquals(
          TableProperties.AVRO_COMPRESSION_DEFAULT,
          resultProperties.get(TableProperties.AVRO_COMPRESSION));
      Assert.assertEquals(
          TableProperties.AVRO_COMPRESSION_LEVEL_DEFAULT,
          resultProperties.get(TableProperties.AVRO_COMPRESSION_LEVEL));
    } else {
      Assert.assertEquals(
          initProperties.get(TableProperties.AVRO_COMPRESSION),
          resultProperties.get(TableProperties.AVRO_COMPRESSION));
      Assert.assertEquals(
          initProperties.get(TableProperties.AVRO_COMPRESSION_LEVEL),
          resultProperties.get(TableProperties.AVRO_COMPRESSION_LEVEL));
    }

    // Override compression to snappy and some random level
    resultProperties =
        appenderProperties(
            table,
            SimpleDataUtil.FLINK_SCHEMA,
            ImmutableMap.of(
                FlinkWriteOptions.WRITE_FORMAT.key(),
                "AVRO",
                FlinkWriteOptions.COMPRESSION_CODEC.key(),
                "snappy",
                FlinkWriteOptions.COMPRESSION_LEVEL.key(),
                "6"));

    Assert.assertEquals("snappy", resultProperties.get(TableProperties.AVRO_COMPRESSION));
    Assert.assertEquals("6", resultProperties.get(TableProperties.AVRO_COMPRESSION_LEVEL));
  }

  @Test
  public void testCompressionParquet() throws Exception {
    // No override provided
    Map<String, String> resultProperties =
        appenderProperties(
            table,
            SimpleDataUtil.FLINK_SCHEMA,
            ImmutableMap.of(FlinkWriteOptions.WRITE_FORMAT.key(), "PARQUET"));

    if (initProperties.get(TableProperties.PARQUET_COMPRESSION) == null) {
      Assert.assertEquals(
          TableProperties.PARQUET_COMPRESSION_DEFAULT_SINCE_1_4_0,
          resultProperties.get(TableProperties.PARQUET_COMPRESSION));
      Assert.assertEquals(
          TableProperties.PARQUET_COMPRESSION_LEVEL_DEFAULT,
          resultProperties.get(TableProperties.PARQUET_COMPRESSION_DEFAULT_SINCE_1_4_0));
    } else {
      Assert.assertEquals(
          initProperties.get(TableProperties.PARQUET_COMPRESSION),
          resultProperties.get(TableProperties.PARQUET_COMPRESSION));
      Assert.assertEquals(
          initProperties.get(TableProperties.PARQUET_COMPRESSION_LEVEL),
          resultProperties.get(TableProperties.PARQUET_COMPRESSION_LEVEL));
    }

    // Override compression to snappy and some random level
    resultProperties =
        appenderProperties(
            table,
            SimpleDataUtil.FLINK_SCHEMA,
            ImmutableMap.of(
                FlinkWriteOptions.WRITE_FORMAT.key(),
                "PARQUET",
                FlinkWriteOptions.COMPRESSION_CODEC.key(),
                "snappy",
                FlinkWriteOptions.COMPRESSION_LEVEL.key(),
                "6"));

    Assert.assertEquals("snappy", resultProperties.get(TableProperties.PARQUET_COMPRESSION));
    Assert.assertEquals("6", resultProperties.get(TableProperties.PARQUET_COMPRESSION_LEVEL));
  }

  @Test
  public void testCompressionOrc() throws Exception {
    // No override provided
    Map<String, String> resultProperties =
        appenderProperties(
            table,
            SimpleDataUtil.FLINK_SCHEMA,
            ImmutableMap.of(FlinkWriteOptions.WRITE_FORMAT.key(), "ORC"));

    if (initProperties.get(TableProperties.ORC_COMPRESSION) == null) {
      Assert.assertEquals(
          TableProperties.ORC_COMPRESSION_DEFAULT,
          resultProperties.get(TableProperties.ORC_COMPRESSION));
      Assert.assertEquals(
          TableProperties.ORC_COMPRESSION_STRATEGY_DEFAULT,
          resultProperties.get(TableProperties.ORC_COMPRESSION_STRATEGY));
    } else {
      Assert.assertEquals(
          initProperties.get(TableProperties.ORC_COMPRESSION),
          resultProperties.get(TableProperties.ORC_COMPRESSION));
      Assert.assertEquals(
          initProperties.get(TableProperties.ORC_COMPRESSION_STRATEGY),
          resultProperties.get(TableProperties.ORC_COMPRESSION_STRATEGY));
    }

    // Override compression to snappy and a different strategy
    resultProperties =
        appenderProperties(
            table,
            SimpleDataUtil.FLINK_SCHEMA,
            ImmutableMap.of(
                FlinkWriteOptions.WRITE_FORMAT.key(),
                "ORC",
                FlinkWriteOptions.COMPRESSION_CODEC.key(),
                "snappy",
                FlinkWriteOptions.COMPRESSION_STRATEGY.key(),
                "speed"));

    Assert.assertEquals("snappy", resultProperties.get(TableProperties.ORC_COMPRESSION));
    Assert.assertEquals("speed", resultProperties.get(TableProperties.ORC_COMPRESSION_STRATEGY));
  }

  private static OneInputStreamOperatorTestHarness<RowData, WriteResult> createIcebergStreamWriter(
      Table icebergTable, TableSchema flinkSchema, Map<String, String> override) throws Exception {
    RowType flinkRowType = FlinkSink.toFlinkRowType(icebergTable.schema(), flinkSchema);
    FlinkWriteConf flinkWriteConfig =
        new FlinkWriteConf(
            icebergTable, override, new org.apache.flink.configuration.Configuration());

    IcebergStreamWriter<RowData> streamWriter =
        FlinkSink.createStreamWriter(() -> icebergTable, flinkWriteConfig, flinkRowType, null);
    OneInputStreamOperatorTestHarness<RowData, WriteResult> harness =
        new OneInputStreamOperatorTestHarness<>(streamWriter, 1, 1, 0);

    harness.setup();
    harness.open();

    return harness;
  }

  private static Map<String, String> appenderProperties(
      Table table, TableSchema schema, Map<String, String> override) throws Exception {
    try (OneInputStreamOperatorTestHarness<RowData, WriteResult> testHarness =
        createIcebergStreamWriter(table, schema, override)) {
      testHarness.processElement(SimpleDataUtil.createRowData(1, "hello"), 1);

      testHarness.prepareSnapshotPreBarrier(1L);
      DynFields.BoundField<IcebergStreamWriter> operatorField =
          DynFields.builder()
              .hiddenImpl(testHarness.getOperatorFactory().getClass(), "operator")
              .build(testHarness.getOperatorFactory());
      DynFields.BoundField<TaskWriter> writerField =
          DynFields.builder()
              .hiddenImpl(IcebergStreamWriter.class, "writer")
              .build(operatorField.get());
      DynFields.BoundField<FlinkAppenderFactory> appenderField =
          DynFields.builder()
              .hiddenImpl(BaseTaskWriter.class, "appenderFactory")
              .build(writerField.get());
      DynFields.BoundField<Map<String, String>> propsField =
          DynFields.builder()
              .hiddenImpl(FlinkAppenderFactory.class, "props")
              .build(appenderField.get());
      return propsField.get();
    }
  }
}
