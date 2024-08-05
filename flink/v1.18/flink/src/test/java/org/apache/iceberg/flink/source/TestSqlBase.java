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

import static org.apache.iceberg.flink.TestFixtures.DATABASE;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.flink.MiniFlinkClusterExtension;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

/** Test other more advanced usage of SQL. They don't need to run for every file format. */
public abstract class TestSqlBase {
  @RegisterExtension
  public static MiniClusterExtension miniClusterExtension =
      MiniFlinkClusterExtension.createWithClassloaderCheckDisabled();

  @RegisterExtension
  public static final HadoopCatalogExtension CATALOG_EXTENSION =
      new HadoopCatalogExtension(DATABASE, TestFixtures.TABLE);

  @TempDir protected Path temporaryFolder;

  private volatile TableEnvironment tEnv;

  protected TableEnvironment getTableEnv() {
    if (tEnv == null) {
      synchronized (this) {
        if (tEnv == null) {
          this.tEnv =
              TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        }
      }
    }
    return tEnv;
  }

  @BeforeEach
  public abstract void before() throws IOException;

  @Test
  public void testResiduals() throws Exception {
    Table table =
        CATALOG_EXTENSION
            .catalog()
            .createTable(TestFixtures.TABLE_IDENTIFIER, TestFixtures.SCHEMA, TestFixtures.SPEC);

    List<Record> writeRecords = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0L);
    writeRecords.get(0).set(1, 123L);
    writeRecords.get(0).set(2, "2020-03-20");
    writeRecords.get(1).set(1, 456L);
    writeRecords.get(1).set(2, "2020-03-20");

    GenericAppenderHelper helper =
        new GenericAppenderHelper(table, FileFormat.PARQUET, temporaryFolder);

    List<Record> expectedRecords = Lists.newArrayList();
    expectedRecords.add(writeRecords.get(0));

    DataFile dataFile1 = helper.writeFile(TestHelpers.Row.of("2020-03-20", 0), writeRecords);
    DataFile dataFile2 =
        helper.writeFile(
            TestHelpers.Row.of("2020-03-21", 0),
            RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0L));
    helper.appendToTable(dataFile1, dataFile2);

    org.apache.iceberg.flink.TestHelpers.assertRecords(
        run(Maps.newHashMap(), "where dt='2020-03-20' and id=123", "*"),
        expectedRecords,
        TestFixtures.SCHEMA);
  }

  @Test
  public void testExposeLocality() throws Exception {
    Table table =
        CATALOG_EXTENSION
            .catalog()
            .createTable(TestFixtures.TABLE_IDENTIFIER, TestFixtures.SCHEMA, TestFixtures.SPEC);

    TableLoader tableLoader = TableLoader.fromHadoopTable(table.location());
    List<Record> expectedRecords = RandomGenericData.generate(TestFixtures.SCHEMA, 10, 0L);
    expectedRecords.forEach(expectedRecord -> expectedRecord.set(2, "2020-03-20"));

    GenericAppenderHelper helper =
        new GenericAppenderHelper(table, FileFormat.PARQUET, temporaryFolder);
    DataFile dataFile = helper.writeFile(TestHelpers.Row.of("2020-03-20", 0), expectedRecords);
    helper.appendToTable(dataFile);

    // test sql api
    Configuration tableConf = getTableEnv().getConfig().getConfiguration();
    tableConf.setBoolean(
        FlinkConfigOptions.TABLE_EXEC_ICEBERG_EXPOSE_SPLIT_LOCALITY_INFO.key(), false);

    List<Row> results = SqlHelpers.sql(getTableEnv(), "select * from t");
    org.apache.iceberg.flink.TestHelpers.assertRecords(
        results, expectedRecords, TestFixtures.SCHEMA);

    // test table api
    tableConf.setBoolean(
        FlinkConfigOptions.TABLE_EXEC_ICEBERG_EXPOSE_SPLIT_LOCALITY_INFO.key(), true);
    FlinkSource.Builder builder = FlinkSource.forRowData().tableLoader(tableLoader).table(table);

    // When running with CI or local, `localityEnabled` will be false even if this configuration is
    // enabled
    assertThat(SourceUtil.isLocalityEnabled(table, tableConf, true))
        .as("Expose split locality info should be false.")
        .isFalse();

    results = run(Maps.newHashMap(), "where dt='2020-03-20'", "*");
    org.apache.iceberg.flink.TestHelpers.assertRecords(
        results, expectedRecords, TestFixtures.SCHEMA);
  }

  protected List<Row> run(
      Map<String, String> options, String sqlFilter, String... sqlSelectedFields) {
    String select = String.join(",", sqlSelectedFields);
    String optionStr = SqlHelpers.sqlOptionsToString(options);
    return SqlHelpers.sql(getTableEnv(), "select %s from t %s %s", select, optionStr, sqlFilter);
  }
}
