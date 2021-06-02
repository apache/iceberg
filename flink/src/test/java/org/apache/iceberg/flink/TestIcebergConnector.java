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
import java.util.concurrent.ExecutionException;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestIcebergConnector extends FlinkTestBase {

  private static TableEnvironment tEnv;

  @Rule
  public final TemporaryFolder warehouse = new TemporaryFolder();


  @BeforeClass
  public static void beforeClass() {
    EnvironmentSettings settings = EnvironmentSettings
        .newInstance()
        .useBlinkPlanner()
        .inBatchMode()
        .build();

    tEnv = TableEnvironment.create(settings);
    tEnv.getConfig().getConfiguration()
        .set(CoreOptions.DEFAULT_PARALLELISM, 1)
        .set(FlinkTableOptions.TABLE_EXEC_ICEBERG_INFER_SOURCE_PARALLELISM, false);
  }

  @Test
  public void testHadoopCatalog() {
    Map<String, String> tableProps = Maps.newHashMap();
    tableProps.put("connector", "iceberg");
    tableProps.put("catalog-name", "test-hadoop");
    tableProps.put("catalog-type", "hadoop");
    tableProps.put("catalog-database", "local_db");
    tableProps.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseRoot());

    sql("CREATE TABLE hadoop_table (id BIGINT, data STRING) WITH %s", toWithClause(tableProps));
    sql("INSERT INTO hadoop_table VALUES (1, 'aaa'), (2, 'bbb'), (3, 'ccc')");
    Assert.assertEquals("Should have expected rows",
        Lists.newArrayList(Row.of(1L, "aaa"), Row.of(2L, "bbb"), Row.of(3L, "ccc")),
        sql("SELECT * FROM hadoop_table"));

    // Drop and create it again.
    sql("DROP TABLE hadoop_table");
    sql("CREATE TABLE hadoop_table (id BIGINT, data STRING) WITH %s", toWithClause(tableProps));
    Assert.assertEquals("Should have expected rows",
        Lists.newArrayList(Row.of(1L, "aaa"), Row.of(2L, "bbb"), Row.of(3L, "ccc")),
        sql("SELECT * FROM hadoop_table"));

    sql("DROP TABLE hadoop_table");
  }

  @Test
  public void testHiveCatalog() {
    Map<String, String> tableProps = Maps.newHashMap();
    tableProps.put("connector", "iceberg");
    tableProps.put("catalog-name", "test-hive");
    tableProps.put("catalog-type", "hive");
    tableProps.put("catalog-database", "default");
    tableProps.put(CatalogProperties.URI, FlinkCatalogTestBase.getURI(hiveConf));
    tableProps.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseRoot());

    sql("CREATE TABLE hive_table(id BIGINT, data STRING) WITH %s", FlinkCatalogTestBase.toWithClause(tableProps));
    sql("INSERT INTO hive_table VALUES (1, 'AAA'), (2, 'BBB'), (3, 'CCC')");
    Assert.assertEquals("Should have the expected rows",
        Lists.newArrayList(Row.of(1L, "AAA"), Row.of(2L, "BBB"), Row.of(3L, "CCC")),
        sql("SELECT * FROM hive_table"));

    // Drop and create it again.
    sql("DROP TABLE hive_table");
    sql("CREATE TABLE hive_table (id BIGINT, data STRING) WITH %s", toWithClause(tableProps));
    Assert.assertEquals("Should have expected rows",
        Lists.newArrayList(Row.of(1L, "AAA"), Row.of(2L, "BBB"), Row.of(3L, "CCC")),
        sql("SELECT * FROM hive_table"));

    sql("DROP TABLE hive_table");
  }

  private String toWithClause(Map<String, String> props) {
    return FlinkCatalogTestBase.toWithClause(props);
  }

  private String warehouseRoot() {
    return String.format("file://%s", warehouse.getRoot().getAbsolutePath());
  }

  protected List<Row> sql(String query, Object... args) {
    TableResult tableResult = tEnv.executeSql(String.format(query, args));
    try {
      tableResult.await();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }

    try (CloseableIterator<Row> iter = tableResult.collect()) {
      return Lists.newArrayList(iter);
    } catch (Exception e) {
      throw new RuntimeException("Failed to collect table result", e);
    }
  }
}
