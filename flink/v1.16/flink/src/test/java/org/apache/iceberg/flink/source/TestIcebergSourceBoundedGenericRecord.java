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
package org.apache.iceberg.flink.source;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.HadoopCatalogResource;
import org.apache.iceberg.flink.MiniClusterResource;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.flink.data.RowDataToRowMapper;
import org.apache.iceberg.flink.sink.AvroGenericRecordToRowDataMapper;
import org.apache.iceberg.flink.source.assigner.SimpleSplitAssignerFactory;
import org.apache.iceberg.flink.source.reader.AvroGenericRecordReaderFunction;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestIcebergSourceBoundedGenericRecord {
  @ClassRule
  public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
      MiniClusterResource.createWithClassloaderCheckDisabled();

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule
  public final HadoopCatalogResource catalogResource =
      new HadoopCatalogResource(TEMPORARY_FOLDER, TestFixtures.DATABASE, TestFixtures.TABLE);

  @Parameterized.Parameters(name = "format={0}, parallelism = {1}")
  public static Object[][] parameters() {
    return new Object[][] {
      {"avro", 2},
      {"parquet", 2},
      {"orc", 2}
    };
  }

  private final FileFormat fileFormat;
  private final int parallelism;

  public TestIcebergSourceBoundedGenericRecord(String format, int parallelism) {
    this.fileFormat = FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH));
    this.parallelism = parallelism;
  }

  @Test
  public void testUnpartitionedTable() throws Exception {
    Table table =
        catalogResource.catalog().createTable(TestFixtures.TABLE_IDENTIFIER, TestFixtures.SCHEMA);
    List<Record> expectedRecords = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0L);
    new GenericAppenderHelper(table, fileFormat, TEMPORARY_FOLDER).appendToTable(expectedRecords);
    TestHelpers.assertRecords(run(), expectedRecords, TestFixtures.SCHEMA);
  }

  @Test
  public void testPartitionedTable() throws Exception {
    String dateStr = "2020-03-20";
    Table table =
        catalogResource
            .catalog()
            .createTable(TestFixtures.TABLE_IDENTIFIER, TestFixtures.SCHEMA, TestFixtures.SPEC);
    List<Record> expectedRecords = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0L);
    for (int i = 0; i < expectedRecords.size(); ++i) {
      expectedRecords.get(i).setField("dt", dateStr);
    }

    new GenericAppenderHelper(table, fileFormat, TEMPORARY_FOLDER)
        .appendToTable(org.apache.iceberg.TestHelpers.Row.of(dateStr, 0), expectedRecords);
    TestHelpers.assertRecords(run(), expectedRecords, TestFixtures.SCHEMA);
  }

  @Test
  public void testProjection() throws Exception {
    Table table =
        catalogResource
            .catalog()
            .createTable(TestFixtures.TABLE_IDENTIFIER, TestFixtures.SCHEMA, TestFixtures.SPEC);
    List<Record> expectedRecords = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0L);
    new GenericAppenderHelper(table, fileFormat, TEMPORARY_FOLDER)
        .appendToTable(org.apache.iceberg.TestHelpers.Row.of("2020-03-20", 0), expectedRecords);
    // select the "data" field (fieldId == 1)
    Schema projectedSchema = TypeUtil.select(TestFixtures.SCHEMA, Sets.newHashSet(1));
    List<Row> expectedRows =
        Arrays.asList(Row.of(expectedRecords.get(0).get(0)), Row.of(expectedRecords.get(1).get(0)));
    TestHelpers.assertRows(
        run(projectedSchema, Collections.emptyList(), Collections.emptyMap()), expectedRows);
  }

  private List<Row> run() throws Exception {
    return run(null, Collections.emptyList(), Collections.emptyMap());
  }

  private List<Row> run(
      Schema projectedSchema, List<Expression> filters, Map<String, String> options)
      throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(parallelism);
    env.getConfig().enableObjectReuse();

    Configuration config = new Configuration();
    config.setInteger(FlinkConfigOptions.SOURCE_READER_FETCH_BATCH_RECORD_COUNT, 128);
    Table table;
    try (TableLoader tableLoader = catalogResource.tableLoader()) {
      tableLoader.open();
      table = tableLoader.loadTable();
    }

    AvroGenericRecordReaderFunction readerFunction =
        new AvroGenericRecordReaderFunction(
            TestFixtures.TABLE_IDENTIFIER.name(),
            new Configuration(),
            table.schema(),
            null,
            null,
            false,
            table.io(),
            table.encryption(),
            filters);

    IcebergSource.Builder<GenericRecord> sourceBuilder =
        IcebergSource.<GenericRecord>builder()
            .tableLoader(catalogResource.tableLoader())
            .readerFunction(readerFunction)
            .assignerFactory(new SimpleSplitAssignerFactory())
            .flinkConfig(config);
    if (projectedSchema != null) {
      sourceBuilder.project(projectedSchema);
    }

    sourceBuilder.filters(filters);
    sourceBuilder.setAll(options);

    Schema readSchema = projectedSchema != null ? projectedSchema : table.schema();
    RowType rowType = FlinkSchemaUtil.convert(readSchema);
    org.apache.avro.Schema avroSchema =
        AvroSchemaUtil.convert(readSchema, TestFixtures.TABLE_IDENTIFIER.name());

    DataStream<Row> stream =
        env.fromSource(
                sourceBuilder.build(),
                WatermarkStrategy.noWatermarks(),
                "testBasicRead",
                new GenericRecordAvroTypeInfo(avroSchema))
            // There are two reasons for converting GenericRecord back to Row.
            // 1. Avro GenericRecord/Schema is not serializable.
            // 2. leverage the TestHelpers.assertRecords for validation.
            .map(AvroGenericRecordToRowDataMapper.forAvroSchema(avroSchema))
            .map(new RowDataToRowMapper(rowType));

    try (CloseableIterator<Row> iter = stream.executeAndCollect()) {
      return Lists.newArrayList(iter);
    }
  }
}
