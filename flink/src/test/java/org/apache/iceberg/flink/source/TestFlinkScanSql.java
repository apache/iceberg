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
import org.apache.flink.configuration.Configuration;
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
import org.apache.iceberg.flink.FlinkTableOptions;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
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
  protected List<Row> run(FlinkSource.Builder formatBuilder, Map<String, String> sqlOptions, String sqlFilter,
                          String... sqlSelectedFields) {
    String select = String.join(",", sqlSelectedFields);

    StringBuilder builder = new StringBuilder();
    sqlOptions.forEach((key, value) -> builder.append(optionToKv(key, value)).append(","));

    String optionStr = builder.toString();

    if (optionStr.endsWith(",")) {
      optionStr = optionStr.substring(0, optionStr.length() - 1);
    }

    if (!optionStr.isEmpty()) {
      optionStr = String.format("/*+ OPTIONS(%s)*/", optionStr);
    }

    String sql = String.format("select %s from t %s %s", select, optionStr, sqlFilter);
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

    Expression filter = Expressions.and(Expressions.equal("dt", "2020-03-20"), Expressions.equal("id", 123));
    assertRecords(runWithFilter(filter, "where dt='2020-03-20' and id=123"), expectedRecords, SCHEMA);
  }

  @Test
  public void testInferedParallelism() throws IOException {
    Table table = catalog.createTable(TableIdentifier.of("default", "t"), SCHEMA, SPEC);

    TableLoader tableLoader = TableLoader.fromHadoopTable(table.location());
    FlinkInputFormat flinkInputFormat = FlinkSource.forRowData().tableLoader(tableLoader).table(table).buildFormat();
    ScanContext scanContext = ScanContext.builder().build();

    // Empty table, infer parallelism should be at least 1
    int parallelism = FlinkSource.forRowData().inferParallelism(flinkInputFormat, scanContext);
    Assert.assertEquals("Should produce the expected parallelism.", 1, parallelism);

    GenericAppenderHelper helper = new GenericAppenderHelper(table, fileFormat, TEMPORARY_FOLDER);
    DataFile dataFile1 = helper.writeFile(TestHelpers.Row.of("2020-03-20", 0),
        RandomGenericData.generate(SCHEMA, 2, 0L));
    DataFile dataFile2 = helper.writeFile(TestHelpers.Row.of("2020-03-21", 0),
        RandomGenericData.generate(SCHEMA, 2, 0L));
    helper.appendToTable(dataFile1, dataFile2);

    // Make sure to generate 2 CombinedScanTasks
    long maxFileLen = Math.max(dataFile1.fileSizeInBytes(), dataFile2.fileSizeInBytes());
    executeSQL(String
        .format("ALTER TABLE t SET ('read.split.open-file-cost'='1', 'read.split.target-size'='%s')", maxFileLen));

    // 2 splits (max infer is the default value 100 , max > splits num), the parallelism is splits num : 2
    parallelism = FlinkSource.forRowData().inferParallelism(flinkInputFormat, scanContext);
    Assert.assertEquals("Should produce the expected parallelism.", 2, parallelism);

    // 2 splits and limit is 1 , max infer parallelism is default 100，
    // which is greater than splits num and limit, the parallelism is the limit value : 1
    parallelism = FlinkSource.forRowData().inferParallelism(flinkInputFormat, ScanContext.builder().limit(1).build());
    Assert.assertEquals("Should produce the expected parallelism.", 1, parallelism);

    // 2 splits and max infer parallelism is 1 (max < splits num), the parallelism is  1
    Configuration configuration = new Configuration();
    configuration.setInteger(FlinkTableOptions.TABLE_EXEC_ICEBERG_INFER_SOURCE_PARALLELISM_MAX, 1);
    parallelism = FlinkSource.forRowData()
        .flinkConf(configuration)
        .inferParallelism(flinkInputFormat, ScanContext.builder().build());
    Assert.assertEquals("Should produce the expected parallelism.", 1, parallelism);

    // 2 splits, max infer parallelism is 1, limit is 3, the parallelism is max infer parallelism : 1
    parallelism = FlinkSource.forRowData()
        .flinkConf(configuration)
        .inferParallelism(flinkInputFormat, ScanContext.builder().limit(3).build());
    Assert.assertEquals("Should produce the expected parallelism.", 1, parallelism);

    // 2 splits, infer parallelism is disabled, the parallelism is flink default parallelism 1
    configuration.setBoolean(FlinkTableOptions.TABLE_EXEC_ICEBERG_INFER_SOURCE_PARALLELISM, false);
    parallelism = FlinkSource.forRowData()
        .flinkConf(configuration)
        .inferParallelism(flinkInputFormat, ScanContext.builder().limit(3).build());
    Assert.assertEquals("Should produce the expected parallelism.", 1, parallelism);
  }

  private List<Row> executeSQL(String sql) {
    return Lists.newArrayList(tEnv.executeSql(sql).collect());
  }

  private String optionToKv(String key, Object value) {
    return "'" + key + "'='" + value + "'";
  }
}
