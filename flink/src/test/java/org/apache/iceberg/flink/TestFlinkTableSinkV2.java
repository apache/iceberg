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
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
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
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.source.BoundedTestSource;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestFlinkTableSinkV2 extends FlinkCatalogTestBase {
  private static final String TABLE_NAME_V2_1 = "test_table_v2_1";

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
    super(catalogName, Namespace.empty());

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
    sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, TABLE_NAME_V2_1);
    sql("DROP DATABASE IF EXISTS %s", flinkDatabase);
    super.clean();
  }

  private static Row dateRow(String rowKind, LocalDate dt, String data) {
    return SimpleDataUtil.createDateRow(rowKind, dt, data);
  }

  private Record dateRecord(LocalDate dt, String data) {
    return SimpleDataUtil.createDateRecord(dt, data);
  }

  @Test
  public void testDatePrimaryKeyCase() throws Exception {
    sql("CREATE TABLE %s (dt DATE, data VARCHAR, PRIMARY KEY(dt) NOT ENFORCED) WITH %s",
            TABLE_NAME_V2_1, toWithClause(tableProps));
    Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME_V2_1));

    TableSchema flinkSchema = TableSchema.builder()
            .field("dt", DataTypes.DATE().notNull())
            .field("data", DataTypes.STRING().notNull())
            .primaryKey("dt")
            .build();
    RowType rowType = (RowType) flinkSchema.toRowDataType().getLogicalType();

    DataStream<RowData> inputStream = env.addSource(new BoundedTestSource<>(
                    dateRow("+I", LocalDate.parse("2021-09-11"), "aaa"),
                    dateRow("-D", LocalDate.parse("2021-09-11"), "aaa"),
                    dateRow("+I", LocalDate.parse("2021-09-11"), "bbb"),
                    dateRow("+I", LocalDate.parse("2021-09-12"), "bbb"),
                    dateRow("-U", LocalDate.parse("2021-09-12"), "bbb"),
                    dateRow("+U", LocalDate.parse("2021-09-12"), "ccc")
            ),
            new RowTypeInfo(flinkSchema.getFieldTypes()))
            .keyBy(row -> row.getField(0))
            .map(new DataFormatConverters.RowConverter(flinkSchema.getFieldDataTypes())::toInternal,
                    InternalTypeInfo.of(rowType));

    tEnv.createTemporaryView("source_change_logs_1", tEnv.fromDataStream(inputStream, "dt, data"));

    sql("INSERT INTO %s SELECT * FROM source_change_logs_1", TABLE_NAME_V2_1);

    List<Record> expectedRecords = ImmutableList.of(
            dateRecord(LocalDate.parse("2021-09-11"), "bbb"),
            dateRecord(LocalDate.parse("2021-09-12"), "ccc")
    );
    StructLikeSet expectedSet = StructLikeSet.create(table.schema().asStruct());
    Iterables.addAll(expectedSet, expectedRecords.stream()
            .map(record -> new InternalRecordWrapper(table.schema().asStruct()).wrap(record))
            .collect(Collectors.toList()));

    StructLikeSet actual = actualRowSet(table, "*");
    StructLikeSet actualSet = StructLikeSet.create(table.schema().asStruct());
    Iterables.addAll(actualSet, actual.stream()
            .map(record -> new InternalRecordWrapper(table.schema().asStruct()).wrap(record))
            .collect(Collectors.toList()));

    Assert.assertEquals("Should have the expected records", expectedSet, actualSet);
  }

  private static StructLikeSet actualRowSet(Table table, String... columns) throws IOException {
    return SimpleDataUtil.actualRowSet(table, columns);
  }
}
