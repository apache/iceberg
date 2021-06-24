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

import java.util.Map;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestIcebergConnector extends FlinkTestBase {

  private static final String TABLE_NAME = "test_table";

  @Rule
  public final TemporaryFolder warehouse = new TemporaryFolder();

  private final boolean isStreaming;
  private volatile TableEnvironment tEnv;

  @Parameterized.Parameters(name = "isStreaming={0}")
  public static Iterable<Object[]> parameters() {
    return Lists.newArrayList(
        new Object[] {true},
        new Object[] {false}
    );
  }

  public TestIcebergConnector(boolean isStreaming) {
    this.isStreaming = isStreaming;
  }

  @Override
  protected TableEnvironment getTableEnv() {
    if (tEnv == null) {
      synchronized (this) {
        if (tEnv == null) {
          EnvironmentSettings.Builder settingsBuilder = EnvironmentSettings
              .newInstance()
              .useBlinkPlanner();
          if (isStreaming) {
            settingsBuilder.inStreamingMode();
            StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment(MiniClusterResource.DISABLE_CLASSLOADER_CHECK_CONFIG);
            env.enableCheckpointing(400);
            env.setMaxParallelism(2);
            env.setParallelism(2);
            tEnv = StreamTableEnvironment.create(env, settingsBuilder.build());
          } else {
            settingsBuilder.inBatchMode();
            tEnv = TableEnvironment.create(settingsBuilder.build());
          }
          // Set only one parallelism.
          tEnv.getConfig().getConfiguration()
              .set(CoreOptions.DEFAULT_PARALLELISM, 1)
              .set(FlinkTableOptions.TABLE_EXEC_ICEBERG_INFER_SOURCE_PARALLELISM, false);
        }
      }
    }
    return tEnv;
  }

  @After
  public void after() {
    sql("DROP TABLE IF EXISTS %s", TABLE_NAME);
  }

  @Test
  public void testHadoop() {
    Map<String, String> tableProps = Maps.newHashMap();
    tableProps.put("connector", "iceberg");
    tableProps.put("catalog-name", "test-hadoop");
    tableProps.put("catalog-type", "hadoop");
    tableProps.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseRoot());

    sql("CREATE TABLE %s (id BIGINT, data STRING) WITH %s", TABLE_NAME, toWithClause(tableProps));
    sql("INSERT INTO %s VALUES (1, 'aaa'), (2, 'bbb'), (3, 'ccc')", TABLE_NAME);
    Assert.assertEquals("Should have expected rows",
        Lists.newArrayList(Row.of(1L, "aaa"), Row.of(2L, "bbb"), Row.of(3L, "ccc")),
        sql("SELECT * FROM %s", TABLE_NAME));

    // Drop and create it again.
    sql("DROP TABLE %s", TABLE_NAME);
    sql("CREATE TABLE %s (id BIGINT, data STRING) WITH %s", TABLE_NAME, toWithClause(tableProps));
    Assert.assertEquals("Should have expected rows",
        Lists.newArrayList(Row.of(1L, "aaa"), Row.of(2L, "bbb"), Row.of(3L, "ccc")),
        sql("SELECT * FROM %s", TABLE_NAME));
  }

  @Test
  public void testHadoopCreateDatabaseIfNotExist() {
    Map<String, String> tableProps = Maps.newHashMap();
    tableProps.put("connector", "iceberg");
    tableProps.put("catalog-name", "test-hadoop");
    tableProps.put("catalog-type", "hadoop");
    tableProps.put("catalog-database", "not_existing_db");
    tableProps.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseRoot());

    sql("CREATE TABLE %s (id BIGINT, data STRING) WITH %s", TABLE_NAME, toWithClause(tableProps));
    sql("INSERT INTO %s VALUES (1, 'aaa'), (2, 'bbb'), (3, 'ccc')", TABLE_NAME);
    Assert.assertEquals("Should have expected rows",
        Lists.newArrayList(Row.of(1L, "aaa"), Row.of(2L, "bbb"), Row.of(3L, "ccc")),
        sql("SELECT * FROM %s", TABLE_NAME));

    FlinkCatalogFactory factory = new FlinkCatalogFactory();
    Catalog flinkCatalog = factory.createCatalog("test-hadoop", tableProps, new Configuration());
    Assert.assertTrue("Should have created the database",
        flinkCatalog.databaseExists("not_existing_db"));
    Assert.assertTrue("Should have created the table",
        flinkCatalog.tableExists(new ObjectPath("not_existing_db", TABLE_NAME)));
  }

  @Test
  public void testHive() throws Exception {
    Map<String, String> tableProps = Maps.newHashMap();
    tableProps.put("connector", "iceberg");
    tableProps.put("catalog-name", "test-hive");
    tableProps.put("catalog-type", "hive");
    tableProps.put(CatalogProperties.URI, FlinkCatalogTestBase.getURI(hiveConf));
    tableProps.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseRoot());

    HiveMetaStoreClient metaStoreClient = new HiveMetaStoreClient(hiveConf);
    try {
      sql("CREATE TABLE %s(id BIGINT, data STRING) WITH %s", TABLE_NAME, toWithClause(tableProps));
      sql("INSERT INTO %s VALUES (1, 'AAA'), (2, 'BBB'), (3, 'CCC')", TABLE_NAME);
      Assert.assertEquals("Should have the expected rows",
          Lists.newArrayList(Row.of(1L, "AAA"), Row.of(2L, "BBB"), Row.of(3L, "CCC")),
          sql("SELECT * FROM %s", TABLE_NAME));

      // Drop and create it again.
      sql("DROP TABLE %s", TABLE_NAME);
      sql("CREATE TABLE %s (id BIGINT, data STRING) WITH %s", TABLE_NAME, toWithClause(tableProps));
      Assert.assertEquals("Should have expected rows",
          Lists.newArrayList(Row.of(1L, "AAA"), Row.of(2L, "BBB"), Row.of(3L, "CCC")),
          sql("SELECT * FROM %s", TABLE_NAME));

      sql("DROP TABLE %s", TABLE_NAME);
    } finally {
      metaStoreClient.dropTable("default", TABLE_NAME);
      metaStoreClient.close();
    }
  }

  @Test
  public void testHiveCreateDatabaseIfNotExist() throws Exception {
    Map<String, String> tableProps = Maps.newHashMap();
    tableProps.put("connector", "iceberg");
    tableProps.put("catalog-name", "test-hive");
    tableProps.put("catalog-type", "hive");
    tableProps.put("catalog-database", "not_existing_db");
    tableProps.put(CatalogProperties.URI, FlinkCatalogTestBase.getURI(hiveConf));
    tableProps.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseRoot());

    HiveMetaStoreClient metaStoreClient = new HiveMetaStoreClient(hiveConf);
    try {
      sql("CREATE TABLE %s (id BIGINT, data STRING) WITH %s", TABLE_NAME, toWithClause(tableProps));
      sql("INSERT INTO %s VALUES (1, 'aaa'), (2, 'bbb'), (3, 'ccc')", TABLE_NAME);
      Assert.assertEquals("Should have expected rows",
          Lists.newArrayList(Row.of(1L, "aaa"), Row.of(2L, "bbb"), Row.of(3L, "ccc")),
          sql("SELECT * FROM %s", TABLE_NAME));

      FlinkCatalogFactory factory = new FlinkCatalogFactory();
      Catalog catalog = factory.createCatalog("test-hive", tableProps, hiveConf);
      Assert.assertTrue("Should have created the database", catalog.databaseExists("not_existing_db"));
      Assert.assertTrue("Should have created the table",
          catalog.tableExists(new ObjectPath("not_existing_db", TABLE_NAME)));
    } finally {
      metaStoreClient.dropTable("not_existing_db", TABLE_NAME);
      metaStoreClient.dropDatabase("not_existing_db");
      metaStoreClient.close();
    }
  }

  private String toWithClause(Map<String, String> props) {
    return FlinkCatalogTestBase.toWithClause(props);
  }

  private String warehouseRoot() {
    return String.format("file://%s", warehouse.getRoot().getAbsolutePath());
  }
}
