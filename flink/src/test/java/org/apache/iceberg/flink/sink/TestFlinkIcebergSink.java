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
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.FiniteTestSource;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableTestBase;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.flink.SimpleDataUtil.FLINK_SCHEMA;
import static org.apache.iceberg.flink.SimpleDataUtil.ROW_TYPE;

@RunWith(Parameterized.class)
public class TestFlinkIcebergSink extends TableTestBase {
  private static final TypeInformation<Row> ROW_TYPE_INFO = new RowTypeInfo(
      FLINK_SCHEMA.getFieldTypes());
  private static final DataFormatConverters.RowConverter CONVERTER = new DataFormatConverters.RowConverter(
      FLINK_SCHEMA.getFieldDataTypes());

  private static final int FORMAT_V2 = 2;
  private static final String TABLE_NAME = "flink_delta_table";

  @ClassRule
  public static MiniClusterWithClientResource miniClusterResource = new MiniClusterWithClientResource(
      new MiniClusterResourceConfiguration.Builder()
          .setNumberTaskManagers(1)
          .setNumberSlotsPerTaskManager(4)
          .build());

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private Table table;
  private StreamExecutionEnvironment env;
  private TableLoader tableLoader;

  private final FileFormat format;
  private final int parallelism;
  private final boolean partitioned;

  @Parameterized.Parameters(name = "format={0}, parallelism = {1}, partitioned = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
        {"avro", 1, true},
        {"avro", 1, false},
        {"avro", 2, true},
        {"avro", 2, false},
        {"orc", 1, true},
        {"orc", 1, false},
        {"orc", 2, true},
        {"orc", 2, false},
        {"parquet", 1, true},
        {"parquet", 1, false},
        {"parquet", 2, true},
        {"parquet", 2, false}
    };
  }

  public TestFlinkIcebergSink(String format, int parallelism, boolean partitioned) {
    super(FORMAT_V2);
    this.format = FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH));
    this.parallelism = parallelism;
    this.partitioned = partitioned;
  }

  @Before
  public void before() throws IOException {
    File tableDir = tempFolder.newFolder();
    Assert.assertTrue(tableDir.delete());

    if (partitioned) {
      PartitionSpec spec = PartitionSpec.builderFor(SimpleDataUtil.SCHEMA)
          .bucket("data", 16)
          .build();
      this.table = TestTables.create(tableDir, TABLE_NAME, SimpleDataUtil.SCHEMA, spec, formatVersion);
    } else {
      this.table = TestTables.create(tableDir, TABLE_NAME, SimpleDataUtil.SCHEMA,
          PartitionSpec.unpartitioned(), formatVersion);
    }

    // Update table's properties to the given file format.
    table.updateProperties().set(TableProperties.DEFAULT_FILE_FORMAT, format.name()).commit();

    env = StreamExecutionEnvironment.getExecutionEnvironment()
        .enableCheckpointing(100)
        .setParallelism(parallelism)
        .setMaxParallelism(parallelism);

    tableLoader = new TestTableLoader(tableDir);
  }

  private List<RowData> convertToRowData(List<Row> rows) {
    return rows.stream().map(CONVERTER::toInternal).collect(Collectors.toList());
  }

  @Test
  public void testWriteRowData() throws Exception {
    List<Row> rows = Lists.newArrayList(
        Row.of(1, "hello"),
        Row.of(2, "world"),
        Row.of(3, "foo")
    );
    DataStream<RowData> dataStream = env.addSource(new FiniteTestSource<>(rows), ROW_TYPE_INFO)
        .map(CONVERTER::toInternal, RowDataTypeInfo.of(ROW_TYPE));

    FlinkSink.forRowData(dataStream)
        .table(table)
        .tableLoader(tableLoader)
        .writeParallelism(parallelism)
        .build();

    // Execute the program.
    env.execute("Test Iceberg DataStream");

    // Assert the iceberg table's records. NOTICE: the FiniteTestSource will checkpoint the same rows twice, so it will
    // commit the same row list into iceberg twice.
    List<RowData> expectedRows = Lists.newArrayList(Iterables.concat(convertToRowData(rows), convertToRowData(rows)));
    SimpleDataUtil.assertTableRows(table, expectedRows);
  }

  private void testWriteRow(TableSchema tableSchema) throws Exception {
    List<Row> rows = Lists.newArrayList(
        Row.of(4, "bar"),
        Row.of(5, "apache")
    );
    DataStream<Row> dataStream = env.addSource(new FiniteTestSource<>(rows), ROW_TYPE_INFO);

    FlinkSink.forRow(dataStream, FLINK_SCHEMA)
        .table(table)
        .tableLoader(tableLoader)
        .tableSchema(tableSchema)
        .writeParallelism(parallelism)
        .build();

    // Execute the program.
    env.execute("Test Iceberg DataStream.");

    List<RowData> expectedRows = Lists.newArrayList(Iterables.concat(convertToRowData(rows), convertToRowData(rows)));
    SimpleDataUtil.assertTableRows(table, expectedRows);
  }

  @Test
  public void testWriteRow() throws Exception {
    testWriteRow(null);
  }

  @Test
  public void testWriteRowWithTableSchema() throws Exception {
    testWriteRow(FLINK_SCHEMA);
  }

  @Test
  public void testUpsertRow() throws Exception {
    Assume.assumeFalse("ORC format does not support equality delete.", FileFormat.ORC.equals(format));

    List<Row> rows = Lists.newArrayList(
        Row.ofKind(RowKind.INSERT, 1, "aaa"),
        Row.ofKind(RowKind.INSERT, 2, "bbb"),
        Row.ofKind(RowKind.UPDATE_BEFORE, 1, "aaa"),
        Row.ofKind(RowKind.UPDATE_AFTER, 1, "ccc"),
        Row.ofKind(RowKind.INSERT, 3, "ccc")
    );
    DataStream<Row> dataStream = env.addSource(new FiniteTestSource<>(rows), ROW_TYPE_INFO);

    FlinkSink.forRow(dataStream, FLINK_SCHEMA)
        .tableLoader(tableLoader)
        .writeParallelism(parallelism)
        .equalityFieldNames(Lists.newArrayList("id"))
        .build();

    env.execute("Execute the upsert program.");

    List<RowData> rowDataList = Lists.newArrayList(
        SimpleDataUtil.createRowData(1, "ccc"),
        SimpleDataUtil.createRowData(2, "bbb"),
        SimpleDataUtil.createRowData(3, "ccc")
    );
    List<RowData> expectedResult = Lists.newArrayList(Iterables.concat(rowDataList, rowDataList));
    SimpleDataUtil.assertTableRows(table, expectedResult);
  }

  private static class TestTableLoader implements TableLoader {
    private static final long serialVersionUID = 1L;

    private final File testTableDir;

    TestTableLoader(File testTableDir) {
      this.testTableDir = testTableDir;
    }

    @Override
    public void open() {

    }

    @Override
    public Table loadTable() {
      return TestTables.load(testTableDir, TABLE_NAME);
    }

    @Override
    public void close() {

    }
  }
}
