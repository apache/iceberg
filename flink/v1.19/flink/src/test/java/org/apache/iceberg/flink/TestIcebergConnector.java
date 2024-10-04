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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
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
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestIcebergConnector extends TestBase {

  private static final String TABLE_NAME = "test_table";

  @Parameter(index = 0)
  private String catalogName;

  @Parameter(index = 1)
  private Map<String, String> properties;

  @Parameter(index = 2)
  private boolean isStreaming;

  private volatile TableEnvironment tEnv;

  @Parameters(name = "catalogName = {0}, properties = {1}, isStreaming = {2}")
  public static Iterable<Object[]> parameters() {
    return Lists.newArrayList(
        // Create iceberg table in the hadoop catalog and default database.
        new Object[] {
          "testhadoop",
          ImmutableMap.of(
              "connector", "iceberg",
              "catalog-type", "hadoop"),
          true
        },
        new Object[] {
          "testhadoop",
          ImmutableMap.of(
              "connector", "iceberg",
              "catalog-type", "hadoop",
              "catalog-table", "not_existing_table"),
          true
        },
        new Object[] {
          "testhadoop",
          ImmutableMap.of(
              "connector", "iceberg",
              "catalog-type", "hadoop"),
          false
        },
        // Create iceberg table in the hadoop catalog and not_existing_db.
        new Object[] {
          "testhadoop",
          ImmutableMap.of(
              "connector", "iceberg",
              "catalog-type", "hadoop",
              "catalog-database", "not_existing_db"),
          true
        },
        new Object[] {
          "testhadoop",
          ImmutableMap.of(
              "connector", "iceberg",
              "catalog-type", "hadoop",
              "catalog-database", "not_existing_db",
              "catalog-table", "not_existing_table"),
          true
        },
        new Object[] {
          "testhadoop",
          ImmutableMap.of(
              "connector", "iceberg",
              "catalog-type", "hadoop",
              "catalog-database", "not_existing_db"),
          false
        },
        // Create iceberg table in the hive catalog and default database.
        new Object[] {
          "testhive",
          ImmutableMap.of(
              "connector", "iceberg",
              "catalog-type", "hive"),
          true
        },
        new Object[] {
          "testhive",
          ImmutableMap.of(
              "connector", "iceberg",
              "catalog-type", "hive",
              "catalog-table", "not_existing_table"),
          true
        },
        new Object[] {
          "testhive",
          ImmutableMap.of(
              "connector", "iceberg",
              "catalog-type", "hive"),
          false
        },
        // Create iceberg table in the hive catalog and not_existing_db.
        new Object[] {
          "testhive",
          ImmutableMap.of(
              "connector", "iceberg",
              "catalog-type", "hive",
              "catalog-database", "not_existing_db"),
          true
        },
        new Object[] {
          "testhive",
          ImmutableMap.of(
              "connector", "iceberg",
              "catalog-type", "hive",
              "catalog-database", "not_existing_db",
              "catalog-table", "not_existing_table"),
          true
        },
        new Object[] {
          "testhive",
          ImmutableMap.of(
              "connector", "iceberg",
              "catalog-type", "hive",
              "catalog-database", "not_existing_db"),
          false
        });
  }

  @Override
  protected TableEnvironment getTableEnv() {
    if (tEnv == null) {
      synchronized (this) {
        if (tEnv == null) {
          EnvironmentSettings.Builder settingsBuilder = EnvironmentSettings.newInstance();
          if (isStreaming) {
            settingsBuilder.inStreamingMode();
            StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(
                    MiniFlinkClusterExtension.DISABLE_CLASSLOADER_CHECK_CONFIG);
            env.enableCheckpointing(400);
            env.setMaxParallelism(2);
            env.setParallelism(2);
            tEnv = StreamTableEnvironment.create(env, settingsBuilder.build());
          } else {
            settingsBuilder.inBatchMode();
            tEnv = TableEnvironment.create(settingsBuilder.build());
          }
          // Set only one parallelism.
          tEnv.getConfig()
              .getConfiguration()
              .set(CoreOptions.DEFAULT_PARALLELISM, 1)
              .set(FlinkConfigOptions.TABLE_EXEC_ICEBERG_INFER_SOURCE_PARALLELISM, false);
        }
      }
    }
    return tEnv;
  }

  @AfterEach
  public void after() throws TException {
    sql("DROP TABLE IF EXISTS %s", TABLE_NAME);

    // Clean the created orphan databases and tables from hive-metastore.
    if (isHiveCatalog()) {
      HiveMetaStoreClient metaStoreClient = new HiveMetaStoreClient(hiveConf);
      try {
        metaStoreClient.dropTable(databaseName(), tableName());
        if (!isDefaultDatabaseName()) {
          try {
            metaStoreClient.dropDatabase(databaseName());
          } catch (Exception ignored) {
            // Ignore
          }
        }
      } finally {
        metaStoreClient.close();
      }
    }
  }

  private void testCreateConnectorTable() {
    Map<String, String> tableProps = createTableProps();

    // Create table under the flink's current database.
    sql("CREATE TABLE %s (id BIGINT, data STRING) WITH %s", TABLE_NAME, toWithClause(tableProps));
    sql("INSERT INTO %s VALUES (1, 'AAA'), (2, 'BBB'), (3, 'CCC')", TABLE_NAME);
    assertThat(sql("SELECT * FROM %s", TABLE_NAME))
        .containsExactlyInAnyOrder(Row.of(1L, "AAA"), Row.of(2L, "BBB"), Row.of(3L, "CCC"));

    FlinkCatalogFactory factory = new FlinkCatalogFactory();
    Catalog flinkCatalog = factory.createCatalog(catalogName, tableProps, new Configuration());
    assertThat(flinkCatalog.databaseExists(databaseName())).isTrue();
    assertThat(flinkCatalog.tableExists(new ObjectPath(databaseName(), tableName()))).isTrue();

    // Drop and create it again.
    sql("DROP TABLE %s", TABLE_NAME);
    sql("CREATE TABLE %s (id BIGINT, data STRING) WITH %s", TABLE_NAME, toWithClause(tableProps));
    assertThat(sql("SELECT * FROM %s", TABLE_NAME))
        .containsExactlyInAnyOrder(Row.of(1L, "AAA"), Row.of(2L, "BBB"), Row.of(3L, "CCC"));
  }

  @TestTemplate
  public void testCreateTableUnderDefaultDatabase() {
    testCreateConnectorTable();
  }

  @TestTemplate
  public void testCatalogDatabaseConflictWithFlinkDatabase() {
    sql("CREATE DATABASE IF NOT EXISTS `%s`", databaseName());
    sql("USE `%s`", databaseName());
    testCreateConnectorTable();
    // Ensure that the table was created under the specific database.
    assertThatThrownBy(
            () -> sql("CREATE TABLE `default_catalog`.`%s`.`%s`", databaseName(), TABLE_NAME))
        .isInstanceOf(org.apache.flink.table.api.TableException.class)
        .hasMessageStartingWith("Could not execute CreateTable in path");
  }

  @TestTemplate
  public void testConnectorTableInIcebergCatalog() {
    // Create the catalog properties
    Map<String, String> catalogProps = Maps.newHashMap();
    catalogProps.put("type", "iceberg");
    if (isHiveCatalog()) {
      catalogProps.put("catalog-type", "hive");
      catalogProps.put(CatalogProperties.URI, CatalogTestBase.getURI(hiveConf));
    } else {
      catalogProps.put("catalog-type", "hadoop");
    }
    catalogProps.put(CatalogProperties.WAREHOUSE_LOCATION, createWarehouse());

    // Create the table properties
    Map<String, String> tableProps = createTableProps();

    // Create a connector table in an iceberg catalog.
    sql("CREATE CATALOG `test_catalog` WITH %s", toWithClause(catalogProps));
    try {
      assertThatThrownBy(
              () ->
                  sql(
                      "CREATE TABLE `test_catalog`.`%s`.`%s` (id BIGINT, data STRING) WITH %s",
                      FlinkCatalogFactory.DEFAULT_DATABASE_NAME,
                      TABLE_NAME,
                      toWithClause(tableProps)))
          .cause()
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage(
              "Cannot create the table with 'connector'='iceberg' table property in an iceberg catalog, "
                  + "Please create table with 'connector'='iceberg' property in a non-iceberg catalog or "
                  + "create table without 'connector'='iceberg' related properties in an iceberg table.");
    } finally {
      sql("DROP CATALOG IF EXISTS `test_catalog`");
    }
  }

  private Map<String, String> createTableProps() {
    Map<String, String> tableProps = Maps.newHashMap(properties);
    tableProps.put("catalog-name", catalogName);
    tableProps.put(CatalogProperties.WAREHOUSE_LOCATION, createWarehouse());
    if (isHiveCatalog()) {
      tableProps.put(CatalogProperties.URI, CatalogTestBase.getURI(hiveConf));
    }
    return tableProps;
  }

  private boolean isHiveCatalog() {
    return "testhive".equalsIgnoreCase(catalogName);
  }

  private boolean isDefaultDatabaseName() {
    return FlinkCatalogFactory.DEFAULT_DATABASE_NAME.equalsIgnoreCase(databaseName());
  }

  private String tableName() {
    return properties.getOrDefault("catalog-table", TABLE_NAME);
  }

  private String databaseName() {
    return properties.getOrDefault("catalog-database", "default_database");
  }

  private String createWarehouse() {
    try {
      return String.format(
          "file://%s",
          Files.createTempDirectory(temporaryDirectory, "junit").toFile().getAbsolutePath());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
