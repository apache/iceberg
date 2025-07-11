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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.source.BoundedTableFactory;
import org.apache.iceberg.flink.source.BoundedTestSource;
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestFlinkTableSinkCompaction extends CatalogTestBase {

  private static final TypeInformation<Row> ROW_TYPE_INFO =
      new RowTypeInfo(
          SimpleDataUtil.FLINK_SCHEMA.getColumnDataTypes().stream()
              .map(ExternalTypeInfo::of)
              .toArray(TypeInformation[]::new));

  private static final DataFormatConverters.RowConverter CONVERTER =
      new DataFormatConverters.RowConverter(
          SimpleDataUtil.FLINK_SCHEMA.getColumnDataTypes().toArray(DataType[]::new));

  private static final String TABLE_NAME = "test_table";
  private StreamTableEnvironment tEnv;
  private StreamExecutionEnvironment env;
  private Table icebergTable;
  private static final String TABLE_PROPERTIES =
      "'flink-maintenance.lock.type'='jdbc',"
          + "'flink-maintenance.lock.jdbc.uri'='jdbc:sqlite:file::memory:?ic',"
          + "'flink-maintenance.lock.jdbc.init-lock-table'='true',"
          + "'flink-maintenance.rewrite.rewrite-all'='true',"
          + "'flink-maintenance.rewrite.schedule.data-file-size'='1',"
          + "'flink-maintenance.lock-check-delay-seconds'='60'";

  @Parameter(index = 2)
  private boolean userSqlHint;

  @Parameters(name = "catalogName={0}, baseNamespace={1}, userSqlHint={2}")
  public static List<Object[]> parameters() {
    return Arrays.asList(
        new Object[] {"testhadoop_basenamespace", Namespace.of("l0", "l1"), true},
        new Object[] {"testhadoop_basenamespace", Namespace.of("l0", "l1"), false});
  }

  @Override
  protected StreamTableEnvironment getTableEnv() {
    if (tEnv == null) {
      synchronized (this) {
        EnvironmentSettings.Builder settingsBuilder = EnvironmentSettings.newInstance();
        settingsBuilder.inStreamingMode();
        env =
            StreamExecutionEnvironment.getExecutionEnvironment(
                MiniFlinkClusterExtension.DISABLE_CLASSLOADER_CHECK_CONFIG);
        env.enableCheckpointing(100);
        tEnv = StreamTableEnvironment.create(env, settingsBuilder.build());
      }
    }

    tEnv.getConfig()
        .getConfiguration()
        .set(FlinkConfigOptions.TABLE_EXEC_ICEBERG_USE_V2_SINK, true)
        .set(FlinkWriteOptions.COMPACTION_ENABLE, true);

    return tEnv;
  }

  @Override
  @BeforeEach
  public void before() {
    super.before();
    sql("CREATE DATABASE %s", flinkDatabase);
    sql("USE CATALOG %s", catalogName);
    sql("USE %s", DATABASE);
    if (userSqlHint) {
      sql("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    } else {
      sql("CREATE TABLE %s (id int, data varchar) with (%s)", TABLE_NAME, TABLE_PROPERTIES);
    }

    icebergTable = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME));
  }

  @Override
  @AfterEach
  public void clean() {
    sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, TABLE_NAME);
    dropDatabase(flinkDatabase, true);
    BoundedTableFactory.clearDataSets();
    super.clean();
  }

  @TestTemplate
  public void testSQLCompactionE2e() throws Exception {
    List<Row> rows = Lists.newArrayList(Row.of(1, "hello"), Row.of(2, "world"), Row.of(3, "foo"));
    DataStream<RowData> dataStream =
        env.addSource(new BoundedTestSource<>(rows.toArray(new Row[0])), ROW_TYPE_INFO)
            .map(CONVERTER::toInternal, FlinkCompatibilityUtil.toTypeInfo(SimpleDataUtil.ROW_TYPE));

    getTableEnv().createTemporaryView("sourceTable", dataStream);

    // Redirect the records from source table to destination table.
    if (userSqlHint) {
      sql(
          "INSERT INTO %s /*+ OPTIONS(%s) */ SELECT id,data from sourceTable",
          TABLE_NAME, TABLE_PROPERTIES);
    } else {
      sql("INSERT INTO %s SELECT id,data from sourceTable", TABLE_NAME);
    }

    // Assert the table records as expected.
    SimpleDataUtil.assertTableRecords(
        icebergTable,
        Lists.newArrayList(
            SimpleDataUtil.createRecord(1, "hello"),
            SimpleDataUtil.createRecord(2, "world"),
            SimpleDataUtil.createRecord(3, "foo")));

    // check the data file count after compact
    List<DataFile> afterCompactDataFiles =
        getDataFiles(icebergTable.currentSnapshot(), icebergTable);
    assertThat(afterCompactDataFiles).hasSize(1);

    // check the data file count before compact
    List<DataFile> preCompactDataFiles =
        getDataFiles(
            icebergTable.snapshot(icebergTable.currentSnapshot().parentId()), icebergTable);
    assertThat(preCompactDataFiles).hasSize(3);
  }

  private List<DataFile> getDataFiles(Snapshot snapshot, Table table) throws IOException {
    List<DataFile> dataFiles = Lists.newArrayList();
    for (ManifestFile dataManifest : snapshot.dataManifests(table.io())) {
      try (ManifestReader<DataFile> reader = ManifestFiles.read(dataManifest, table.io())) {
        reader.iterator().forEachRemaining(dataFiles::add);
      }
    }

    return dataFiles;
  }
}
