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

import java.util.List;
import java.util.Map;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestFlinkTableSink extends FlinkCatalogTestBase {
  private static final String TABLE_NAME = "test_table";
  private TableEnvironment tEnv;
  private org.apache.iceberg.Table icebergTable;

  private final FileFormat format;
  private final boolean isStreamingJob;

  @Parameterized.Parameters(name = "{index}: format={0}, isStreaming={1}, catalogName={2}, baseNamespace={3}")
  public static Iterable<Object[]> parameters() {
    List<Object[]> parameters = Lists.newArrayList();
    for (FileFormat format : new FileFormat[] {FileFormat.ORC, FileFormat.AVRO, FileFormat.PARQUET}) {
      for (Boolean isStreaming : new Boolean[] {true, false}) {
        for (Object[] catalogParams : FlinkCatalogTestBase.parameters()) {
          String catalogName = (String) catalogParams[0];
          String[] baseNamespace = (String[]) catalogParams[1];
          parameters.add(new Object[] {format, isStreaming, catalogName, baseNamespace});
        }
      }
    }
    return parameters;
  }

  public TestFlinkTableSink(FileFormat format, Boolean isStreamingJob, String catalogName, String[] baseNamespace) {
    super(catalogName, baseNamespace);
    this.format = format;
    this.isStreamingJob = isStreamingJob;
  }

  @Override
  protected TableEnvironment getTableEnv() {
    if (tEnv == null) {
      synchronized (this) {
        EnvironmentSettings.Builder settingsBuilder = EnvironmentSettings
            .newInstance()
            .useBlinkPlanner();
        if (isStreamingJob) {
          settingsBuilder.inStreamingMode();
          StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
          env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
          env.enableCheckpointing(400);
          tEnv = StreamTableEnvironment.create(env, settingsBuilder.build());
        } else {
          settingsBuilder.inBatchMode();
          tEnv = TableEnvironment.create(settingsBuilder.build());
        }
      }
    }
    return tEnv;
  }

  @Before
  public void before() {
    sql("CREATE DATABASE %s", flinkDatabase);
    sql("USE CATALOG %s", catalogName);
    sql("USE %s", DATABASE);

    Map<String, String> properties = ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name());
    this.icebergTable = validationCatalog
        .createTable(TableIdentifier.of(icebergNamespace, TABLE_NAME),
            SimpleDataUtil.SCHEMA,
            PartitionSpec.unpartitioned(),
            properties);
  }

  @After
  public void clean() {
    sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, TABLE_NAME);
    sql("DROP DATABASE IF EXISTS %s", flinkDatabase);
  }

  @Test
  public void testStreamSQL() throws Exception {
    // Register the rows into a temporary table.
    Table sourceTable = getTableEnv().fromValues(SimpleDataUtil.FLINK_SCHEMA.toRowDataType(),
        Expressions.row(1, "hello"),
        Expressions.row(2, "world"),
        Expressions.row(3, (String) null),
        Expressions.row(null, "bar")
    );
    getTableEnv().createTemporaryView("sourceTable", sourceTable);

    // Redirect the records from source table to destination table.
    sql("INSERT INTO %s SELECT id,data from sourceTable", TABLE_NAME);

    // Assert the table records as expected.
    SimpleDataUtil.assertTableRecords(icebergTable, Lists.newArrayList(
        SimpleDataUtil.createRecord(1, "hello"),
        SimpleDataUtil.createRecord(2, "world"),
        SimpleDataUtil.createRecord(3, null),
        SimpleDataUtil.createRecord(null, "bar")
    ));
  }

  @Test
  public void testOverwriteTable() throws Exception {
    Assume.assumeFalse("Flink unbounded streaming does not support overwrite operation", isStreamingJob);

    sql("INSERT INTO %s SELECT 1, 'a'", TABLE_NAME);
    SimpleDataUtil.assertTableRecords(icebergTable, Lists.newArrayList(
        SimpleDataUtil.createRecord(1, "a")
    ));

    sql("INSERT OVERWRITE %s SELECT 2, 'b'", TABLE_NAME);
    SimpleDataUtil.assertTableRecords(icebergTable, Lists.newArrayList(
        SimpleDataUtil.createRecord(2, "b")
    ));
  }
}
