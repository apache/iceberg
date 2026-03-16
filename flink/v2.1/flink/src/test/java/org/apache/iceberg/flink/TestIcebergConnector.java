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
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecord;
import org.apache.iceberg.flink.sink.dynamic.DynamicTableRecordGenerator;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
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
              "connector",
              "iceberg",
              "catalog-type",
              "hadoop",
              "catalog-database",
              "test_database"),
          true
        },
        new Object[] {
          "testhadoop",
          ImmutableMap.of(
              "connector",
              "iceberg",
              "catalog-type",
              "hadoop",
              "catalog-database",
              "test_database",
              "catalog-table",
              "not_existing_table"),
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
        .isInstanceOf(org.apache.flink.table.api.ValidationException.class)
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

  @TestTemplate
  public void testCreateDynamicIcebergSink() throws DatabaseAlreadyExistException {
    Map<String, String> tableProps = createTableProps();
    Map<String, String> dynamicTableProps = Maps.newHashMap(tableProps);
    dynamicTableProps.put("use-dynamic-iceberg-sink", "true");
    dynamicTableProps.put(
        "dynamic-record-generator-impl", SimpleRowDataTableRecordGenerator.class.getName());
    dynamicTableProps.put("table.props.key1", "val1");

    FlinkCatalogFactory factory = new FlinkCatalogFactory();
    FlinkCatalog flinkCatalog =
        (FlinkCatalog) factory.createCatalog(catalogName, tableProps, new Configuration());
    flinkCatalog.createDatabase(
        databaseName(), new CatalogDatabaseImpl(Maps.newHashMap(), null), true);

    // Create table with dynamic sink enabled
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, database_name STRING, table_name STRING) WITH %s",
        TABLE_NAME + "_dynamic", toWithClause(dynamicTableProps));

    // Insert data with database and table information
    sql(
        "INSERT INTO %s VALUES (1, 'AAA', '%s', '%s'), (2, 'BBB', '%s', '%s'), (3, 'CCC', '%s', '%s')",
        TABLE_NAME + "_dynamic",
        databaseName(),
        tableName(),
        databaseName(),
        tableName(),
        databaseName(),
        tableName());

    // Verify the table and data exists
    ObjectPath objectPath = new ObjectPath(databaseName(), tableName());
    assertThat(flinkCatalog.tableExists(objectPath)).isTrue();
    Table table =
        flinkCatalog
            .getCatalogLoader()
            .loadCatalog()
            .loadTable(TableIdentifier.of(databaseName(), tableName()));
    assertThat(table.properties()).containsEntry("key1", "val1");

    tableProps.put("catalog-database", databaseName());
    sql("CREATE TABLE %s (id BIGINT, data STRING) WITH %s", tableName(), toWithClause(tableProps));
    assertThat(sql("SELECT * FROM %s", tableName()))
        .containsExactlyInAnyOrder(Row.of(1L, "AAA"), Row.of(2L, "BBB"), Row.of(3L, "CCC"));
  }

  @TestTemplate
  public void testMissingDynamicRecordGeneratorImpl() throws DatabaseAlreadyExistException {
    Map<String, String> tableProps = createTableProps();
    tableProps.put("use-dynamic-iceberg-sink", "true");

    FlinkCatalogFactory factory = new FlinkCatalogFactory();
    FlinkCatalog flinkCatalog =
        (FlinkCatalog) factory.createCatalog(catalogName, tableProps, new Configuration());
    flinkCatalog.createDatabase(
        databaseName(), new CatalogDatabaseImpl(Maps.newHashMap(), null), true);

    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, database_name STRING, table_name STRING) WITH %s",
        TABLE_NAME + "_dynamic", toWithClause(tableProps));

    assertThatThrownBy(
            () ->
                sql(
                    "INSERT INTO %s VALUES (1, 'AAA', '%s', '%s')",
                    TABLE_NAME + "_dynamic", databaseName(), tableName()))
        .cause()
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid dynamic record generator value: null. dynamic-record-generator-impl  must be specified when use-dynamic-iceberg-sink is true.");
  }

  public static class SimpleRowDataTableRecordGenerator extends DynamicTableRecordGenerator {

    private int databaseFieldIndex = -1;
    private int tableFieldIndex = -1;

    public SimpleRowDataTableRecordGenerator(RowType rowType) {
      super(rowType);
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
      String[] fieldNames = rowType().getFieldNames().toArray(new String[0]);

      for (int i = 0; i < fieldNames.length; i++) {
        if ("database_name".equals(fieldNames[i])) {
          databaseFieldIndex = i;
        } else if ("table_name".equals(fieldNames[i])) {
          tableFieldIndex = i;
        }
      }
    }

    @Override
    public void generate(RowData inputRecord, Collector<DynamicRecord> out) throws Exception {
      // Extract database and table names using the discovered field indexes
      String databaseName = inputRecord.getString(databaseFieldIndex).toString();
      String tableName = inputRecord.getString(tableFieldIndex).toString();

      // Create schema for the actual data fields (excluding metadata fields)
      Schema schema =
          new Schema(
              Types.NestedField.required(0, "id", Types.LongType.get()),
              Types.NestedField.required(1, "data", Types.StringType.get()));

      TableIdentifier tableIdentifier = TableIdentifier.of(databaseName, tableName);

      DynamicRecord dynamicRecord =
          new DynamicRecord(
              tableIdentifier,
              "main",
              schema,
              inputRecord,
              PartitionSpec.unpartitioned(),
              DistributionMode.NONE,
              1);
      out.collect(dynamicRecord);
    }
  }
}
