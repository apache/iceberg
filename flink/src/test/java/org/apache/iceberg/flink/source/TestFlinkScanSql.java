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
import java.util.Arrays;
import java.util.List;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.types.Row;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Test;

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
    tEnv.executeSql(String.format(
        "create catalog iceberg_catalog with ('type'='iceberg', 'catalog-type'='hadoop', 'warehouse'='%s')",
        warehouse));
    tEnv.executeSql("use catalog iceberg_catalog");
    tEnv.getConfig().getConfiguration().set(TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED, true);
  }

  @Override
  protected List<Row> execute(Table table, List<String> projectFields) throws IOException {
    String sql = String.format("select %s from t", String.join(",", projectFields));
    return executeSQL(sql);
  }

  @Override
  protected List<Row> execute(Table table, ScanOptions options) throws IOException {
    StringBuilder builder = new StringBuilder();

    if (options.snapshotId() != null) {
      builder.append(optionToKv(ScanOptions.SNAPSHOT_ID.key(), options.snapshotId())).append(",");
    }
    if (options.startSnapshotId() != null) {
      builder.append(optionToKv(ScanOptions.START_SNAPSHOT_ID.key(), options.startSnapshotId())).append(",");
    }
    if (options.endSnapshotId() != null) {
      builder.append(optionToKv(ScanOptions.END_SNAPSHOT_ID.key(), options.endSnapshotId())).append(",");
    }
    if (options.asOfTimestamp() != null) {
      builder.append(optionToKv(ScanOptions.AS_OF_TIMESTAMP.key(), options.asOfTimestamp())).append(",");
    }

    String optionStr = builder.toString();

    if (optionStr.endsWith(",")) {
      optionStr = optionStr.substring(0, optionStr.length() - 1);
    }

    if (!optionStr.isEmpty()) {
      optionStr = String.format("/*+ OPTIONS(%s)*/", optionStr);
    }

    String sql = String.format("select * from t %s", optionStr);
    return executeSQL(sql);
  }

  @Override
  protected List<Row> execute(Table table, List<Expression> filters, String sqlFilter) throws IOException {
    String sql = String.format("select * from t where %s", sqlFilter);
    return executeSQL(sql);
  }

  @Test
  public void testResiduals() throws Exception {
    Table table = catalog.createTable(TableIdentifier.of("default", "t"), SCHEMA, SPEC);

    List<Record> writeRecords = RandomGenericData.generate(SCHEMA, 2, 0L);
    writeRecords.get(0).set(1, 123L);
    writeRecords.get(0).set(2, "2020-03-20");
    writeRecords.get(1).set(1, 456L);
    writeRecords.get(1).set(2, "2020-03-20");

    GenericAppenderHelper helper = new GenericAppenderHelper(table, fileFormat, TEMPORARY_FOLDER);

    List<Record> expectedRecords = Lists.newArrayList();
    expectedRecords.add(writeRecords.get(0));

    DataFile dataFile1 = helper.writeFile(TestHelpers.Row.of("2020-03-20", 0), writeRecords);
    DataFile dataFile2 = helper.writeFile(TestHelpers.Row.of("2020-03-21", 0),
        RandomGenericData.generate(SCHEMA, 2, 0L));
    helper.appendToTable(dataFile1, dataFile2);

    List<Expression> filters = Arrays.asList(Expressions.equal("dt", "2020-03-20"), Expressions.equal("id", 123));
    assertRecords(execute(table, filters, "dt='2020-03-20' and id=123"), expectedRecords, SCHEMA);
  }

  private List<Row> executeSQL(String sql) {
    return Lists.newArrayList(tEnv.executeSql(sql).collect());
  }

  private String optionToKv(String key, Object value) {
    return "'" + key + "'='" + value + "'";
  }
}
