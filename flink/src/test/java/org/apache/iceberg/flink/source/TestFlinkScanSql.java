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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.types.Row;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Test Flink SELECT SQLs.
 */
public class TestFlinkScanSql extends TestFlinkScan {

  private TableEnvironment tEnv;

  public TestFlinkScanSql(String fileFormat) {
    super(fileFormat);
  }

  @Override
  public void before() throws IOException {
    super.before();
    tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build());
    tEnv.executeSql(String.format("create catalog iceberg_catalog with (" +
                                  "'type'='iceberg', 'catalog-type'='hadoop', 'warehouse'='%s')", warehouse));
    tEnv.executeSql("use catalog iceberg_catalog");
    tEnv.getConfig().getConfiguration().set(TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED, true);
  }

  @Override
  protected List<Row> executeWithOptions(
      Table table, List<String> projectFields, CatalogLoader loader, Long snapshotId, Long startSnapshotId,
      Long endSnapshotId, Long asOfTimestamp, List<Expression> filters, String sqlFilter) {
    String fields = projectFields == null ? "*" : String.join(",", projectFields);

    if (loader != null) {
      tEnv.registerCatalog(
          "new_catalog", new FlinkCatalog("new_catalog", "default", new String[0], loader, conf, true));
      tEnv.executeSql("use catalog new_catalog");
    }

    Map<String, String> optionMap = Maps.newHashMap();
    if (snapshotId != null) {
      optionMap.put(ScanOptions.SNAPSHOT_ID.key(), snapshotId.toString());
    }
    if (startSnapshotId != null) {
      optionMap.put(ScanOptions.START_SNAPSHOT_ID.key(), startSnapshotId.toString());
    }
    if (endSnapshotId != null) {
      optionMap.put(ScanOptions.END_SNAPSHOT_ID.key(), endSnapshotId.toString());
    }
    if (asOfTimestamp != null) {
      optionMap.put(ScanOptions.AS_OF_TIMESTAMP.key(), asOfTimestamp.toString());
    }

    StringBuilder builder = new StringBuilder();
    optionMap.forEach((key, value) ->
        builder.append("'").append(key).append("'").append("=").append("'").append(value).append("'").append(","));
    String options = builder.toString();
    if (options.endsWith(",")) {
      options = options.substring(0, options.length() - 1);
    }
    if (!options.isEmpty()) {
      options = String.format("/*+ OPTIONS(%s)*/", options);
    }

    String filter = "";
    if (sqlFilter != null) {
      filter = " where " + sqlFilter;
    }

    String sql = String.format("select %s from t %s %s", fields, options, filter);
    return Lists.newArrayList(tEnv.executeSql(sql).collect());
  }

  @Override
  protected void assertResiduals(Schema schema, List<Row> results, List<Record> writeRecords,
                                 List<Record> filteredRecords) {
    // should filter the data.
    assertRecords(results, filteredRecords, schema);
  }

  @Override
  protected void assertNestedProjection(Table table, List<Record> records) {
    List<Row> result = Lists.newArrayList(tEnv.executeSql("select nested.f2,data from t").collect());

    List<Row> expected = Lists.newArrayList();
    for (Record record : records) {
      expected.add(Row.of(((Record) record.get(1)).get(1), record.get(0)));
    }

    assertRows(result, expected);
  }
}
