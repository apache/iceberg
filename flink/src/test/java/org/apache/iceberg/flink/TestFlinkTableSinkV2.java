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

package org.apache.iceberg.flink;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.source.BoundedTestSource;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestFlinkTableSinkV2 extends FlinkCatalogTestBase {
  private static final String TABLE_NAME_V2 = "test_table_v2";

  private final StreamExecutionEnvironment env;

  private StreamTableEnvironment tEnv;
  private Map<String, String> tableProps;

  @Parameterized.Parameters(name = "catalogName={0}, format={1}")
  public static Iterable<Object[]> parameters() {
    return ImmutableList.of(
        new Object[] {"testhive", "avro"},
        new Object[] {"testhive", "parquet"},
        new Object[] {"testhadoop", "avro"},
        new Object[] {"testhadoop", "parquet"}
    );
  }

  public TestFlinkTableSinkV2(String catalogName, String format) {
    super(catalogName, new String[0]);

    this.env = StreamExecutionEnvironment.getExecutionEnvironment()
        .enableCheckpointing(400);
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    this.tableProps = ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format, TableProperties.FORMAT_VERSION, "2");
  }

  @Override
  protected TableEnvironment getTableEnv() {
    if (tEnv == null) {
      synchronized (this) {
        if (tEnv == null) {
          tEnv = StreamTableEnvironment.create(env, EnvironmentSettings
              .newInstance()
              .useBlinkPlanner()
              .inStreamingMode()
              .build());
        }
      }
    }
    return tEnv;
  }

  @Before
  public void before() {
    super.before();
    sql("CREATE DATABASE %s", flinkDatabase);
    sql("USE CATALOG %s", catalogName);
    sql("USE %s", DATABASE);
  }

  @After
  public void clean() {
    sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, TABLE_NAME_V2);
    sql("DROP DATABASE IF EXISTS %s", flinkDatabase);
    super.clean();
  }

  private static Row row(String rowKind, int id, String data) {
    return SimpleDataUtil.createRow(rowKind, id, data);
  }

  private Record record(int id, String data) {
    return SimpleDataUtil.createRecord(id, data);
  }

  @Test
  public void testCase() throws Exception {
    sql("CREATE TABLE %s (id INT, data VARCHAR, PRIMARY KEY(id) NOT ENFORCED) WITH %s",
        TABLE_NAME_V2, toWithClause(tableProps));
    Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME_V2));

    TableSchema flinkSchema = TableSchema.builder()
        .field("id", DataTypes.INT().notNull())
        .field("data", DataTypes.STRING().notNull())
        .primaryKey("id")
        .build();
    RowType rowType = (RowType) flinkSchema.toRowDataType().getLogicalType();

    DataStream<RowData> inputStream = env.addSource(new BoundedTestSource<>(
            row("+I", 1, "aaa"),
            row("-D", 1, "aaa"),
            row("+I", 1, "bbb"),
            row("+I", 2, "bbb"),
            row("-U", 2, "bbb"),
            row("+U", 2, "ccc")
        ),
        new RowTypeInfo(flinkSchema.getFieldTypes()))
        .keyBy(row -> row.getField(0))
        .map(new DataFormatConverters.RowConverter(flinkSchema.getFieldDataTypes())::toInternal,
            RowDataTypeInfo.of(rowType));

    tEnv.createTemporaryView("source_change_logs", tEnv.fromDataStream(inputStream));

    sql("INSERT INTO %s SELECT * FROM source_change_logs", TABLE_NAME_V2);

    List<Record> expectedRecords = ImmutableList.of(
        record(1, "bbb"),
        record(2, "ccc")
    );

    Assert.assertEquals("Should have the expected records", expectedRowSet(table, expectedRecords),
        actualRowSet(table, "*"));
  }

  private static StructLikeSet expectedRowSet(Table table, List<Record> records) {
    return SimpleDataUtil.expectedRowSet(table, records.toArray(new Record[0]));
  }

  private static StructLikeSet actualRowSet(Table table, String... columns) throws IOException {
    return SimpleDataUtil.actualRowSet(table, columns);
  }
}
