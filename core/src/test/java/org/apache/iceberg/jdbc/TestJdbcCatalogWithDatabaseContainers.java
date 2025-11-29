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
package org.apache.iceberg.jdbc;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.Db2Container;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.containers.PostgreSQLContainer;

public class TestJdbcCatalogWithDatabaseContainers {

  public static final String DEFAULT = "DEFAULT";
  public static final String ICEBERG_SCHEMA = "iceberg_schema";
  static Configuration conf = new Configuration();
  static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get(), "unique ID"),
          required(2, "data", Types.StringType.get()));
  @TempDir java.nio.file.Path tableDir;

  @Test
  public void testInitializeAndOperationsWithSqlite() {
    //      public void testInitialize() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, this.tableDir.toAbsolutePath().toString());
    //    properties.put(CatalogProperties.URI, "jdbc:sqlite:build/tmp/test-catalog.db");
    properties.put(CatalogProperties.URI, "jdbc:sqlite:file::memory:?icebergDB");
    properties.put(JdbcUtil.SCHEMA_VERSION_PROPERTY, JdbcUtil.SchemaVersion.V1.name());
    JdbcCatalog jdbcCatalog = new JdbcCatalog();
    jdbcCatalog.setConf(conf);
    jdbcCatalog.initialize("test_jdbc_catalog", properties);
    // second initialization should not fail even if tables are already created
    jdbcCatalog.initialize("test_jdbc_catalog", properties);
    jdbcCatalog.initialize("test_jdbc_catalog", properties);
    testCatalogOperations(jdbcCatalog);
  }

  @Test
  void mytest() {
    testInitializeAndOperationsWithOracle("iceberg_schema");
  }

  @ParameterizedTest(name = "schema {0}")
  @ValueSource(strings = {DEFAULT, ICEBERG_SCHEMA})
  public void testInitializeAndOperationsWithPostgres(String schemaName) {
    var jdbcCatalog =
        testInitializeWithDatabaseContainer(
            new PostgreSQLContainer(), schemaName.equals(DEFAULT) ? null : schemaName);
    testCatalogOperations(jdbcCatalog);
  }

  @ParameterizedTest(name = "schema {0}")
  @ValueSource(strings = {DEFAULT, ICEBERG_SCHEMA})
  public void testInitializeAndOperationsWithDb2(String schemaName) {
    var jdbcCatalog =
        testInitializeWithDatabaseContainer(
            new Db2Container(), schemaName.equals(DEFAULT) ? null : schemaName);
    testCatalogOperations(jdbcCatalog);
  }

  @ParameterizedTest(name = "schema {0}")
  @ValueSource(strings = {DEFAULT, ICEBERG_SCHEMA})
  public void testInitializeAndOperationsWithOracle(String schemaName) {
    var jdbcCatalog =
        testInitializeWithDatabaseContainer(
            new OracleContainer().withUsername(schemaName.equals(DEFAULT) ? "test" : schemaName),
            schemaName.equals(DEFAULT) ? null : schemaName);
    testCatalogOperations(jdbcCatalog);
  }

  private JdbcCatalog testInitializeWithDatabaseContainer(
      JdbcDatabaseContainer<?> dbContainer, String schemaName) {
    dbContainer.start();
    try {
      if (dbContainer instanceof PostgreSQLContainer) {
        Class.forName("org.postgresql.Driver");
      } else if (dbContainer instanceof OracleContainer) {
        Class.forName("oracle.jdbc.OracleDriver");
      } else if (dbContainer instanceof Db2Container) {
        Class.forName("com.ibm.db2.jcc.DB2Driver");
      }
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("JDBC driver not found", e);
    }
    createSchemaIfNecessary(dbContainer, schemaName);
    Map<String, String> properties = Maps.newHashMap();
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, this.tableDir.toAbsolutePath().toString());
    properties.put(JdbcUtil.SCHEMA_VERSION_PROPERTY, JdbcUtil.SchemaVersion.V1.name());
    if (schemaName != null) {
      properties.put(JdbcUtil.SCHEMA_NAME_PROPERTY, schemaName);
    }
    properties.put(CatalogProperties.URI, dbContainer.getJdbcUrl());
    properties.put(JdbcCatalog.PROPERTY_PREFIX + CatalogProperties.USER, dbContainer.getUsername());
    properties.put(JdbcCatalog.PROPERTY_PREFIX + "password", dbContainer.getPassword());
    JdbcCatalog jdbcCatalog = new JdbcCatalog();
    jdbcCatalog.setConf(conf);
    jdbcCatalog.initialize(
        "test_" + dbContainer.getClass().getSimpleName().toLowerCase() + "_jdbc_catalog",
        properties);

    assertTableCreation(dbContainer, schemaName);
    return jdbcCatalog;
  }

  private static void assertTableCreation(JdbcDatabaseContainer<?> dbContainer, String schemaName) {
    // assert that the tables were created in the correct schema
    try (Connection conn = dbContainer.createConnection("")) {
      DatabaseMetaData metaData = conn.getMetaData();
      String schema = null;
      if (dbContainer instanceof PostgreSQLContainer) {
        schema = schemaName == null ? "public" : schemaName;
      } else if (dbContainer instanceof OracleContainer) {
        schema =
            schemaName == null ? dbContainer.getUsername().toUpperCase() : schemaName.toUpperCase();
      } else if (dbContainer instanceof Db2Container) {
        schema =
            schemaName == null ? dbContainer.getUsername().toUpperCase() : schemaName.toUpperCase();
      }
      try (ResultSet tables = metaData.getTables(null, schema, null, new String[] {"TABLE"})) {
        Set<String> tableNames = Sets.newHashSet();
        while (tables.next()) {
          tableNames.add(tables.getString("TABLE_NAME").toLowerCase());
        }
        Set.of("iceberg_namespace_properties", "iceberg_tables")
            .forEach(
                expectedTable -> {
                  assertThat(tableNames)
                      .withFailMessage(
                          () ->
                              "expected table "
                                  + expectedTable
                                  + " not found in schema "
                                  + schemaName)
                      .contains(expectedTable);
                });
      }
    } catch (SQLException e) {
      throw new RuntimeException("Failed to verify catalog tables in database", e);
    }
  }

  private static void createSchemaIfNecessary(
      JdbcDatabaseContainer<?> dbContainer, String schemaName) {
    // create the schema if specified
    if (schemaName != null) {
      try (Connection conn = dbContainer.createConnection("")) {
        try (var stmt = conn.createStatement()) {
          if (dbContainer instanceof PostgreSQLContainer) {
            stmt.execute("CREATE SCHEMA IF NOT EXISTS " + schemaName);
          } else if (dbContainer instanceof OracleContainer) {
            // we are using the user as schema in Oracle
          } else if (dbContainer instanceof Db2Container) {
            stmt.execute("CREATE SCHEMA " + schemaName);
          }
        }
      } catch (SQLException e) {
        throw new RuntimeException("Failed to create schema " + schemaName, e);
      }
    }
  }

  /**
   * Helper method to test basic catalog operations. This method tests the main functionality of
   * JdbcCatalog including namespace, table, and view operations. It is called by the
   * database-specific initialization tests to ensure that the catalog works correctly across
   * different database backends.
   *
   * @param jdbcCatalog the initialized JdbcCatalog instance to test
   */
  private void testCatalogOperations(JdbcCatalog jdbcCatalog) {
    // Test catalog name (Catalog interface)
    assertThat(jdbcCatalog.name()).isNotNull();

    // Test namespace creation with metadata (SupportsNamespaces interface)
    Namespace testNamespace = Namespace.of("test_db", "test_ns");
    Map<String, String> namespaceMetadata = Maps.newHashMap();
    namespaceMetadata.put("owner", "test_user");
    namespaceMetadata.put("description", "Test namespace");
    jdbcCatalog.createNamespace(testNamespace, namespaceMetadata);
    assertThat(jdbcCatalog.namespaceExists(testNamespace)).isTrue();

    // Test loading namespace metadata (SupportsNamespaces interface)
    Map<String, String> loadedMetadata = jdbcCatalog.loadNamespaceMetadata(testNamespace);
    assertThat(loadedMetadata).containsEntry("owner", "test_user");
    assertThat(loadedMetadata).containsEntry("description", "Test namespace");

    // Test listing top-level namespaces (SupportsNamespaces interface)
    assertThat(jdbcCatalog.listNamespaces()).contains(Namespace.of("test_db"));

    // Test listing child namespaces (SupportsNamespaces interface)
    List<Namespace> childNamespaces = jdbcCatalog.listNamespaces(Namespace.of("test_db"));
    assertThat(childNamespaces).contains(testNamespace);

    // Test setting namespace properties (SupportsNamespaces interface)
    Map<String, String> newProperties = Maps.newHashMap();
    newProperties.put("location", "/test/location");
    newProperties.put("owner", "updated_user");
    boolean propsSet = jdbcCatalog.setProperties(testNamespace, newProperties);
    assertThat(propsSet).isTrue();

    // Verify properties were updated
    Map<String, String> updatedMetadata = jdbcCatalog.loadNamespaceMetadata(testNamespace);
    assertThat(updatedMetadata).containsEntry("location", "/test/location");
    assertThat(updatedMetadata).containsEntry("owner", "updated_user");
    assertThat(updatedMetadata).containsEntry("description", "Test namespace");

    // Test removing namespace properties (SupportsNamespaces interface)
    boolean propsRemoved =
        jdbcCatalog.removeProperties(testNamespace, Sets.newHashSet("description"));
    assertThat(propsRemoved).isTrue();

    // Verify property was removed
    Map<String, String> metadataAfterRemoval = jdbcCatalog.loadNamespaceMetadata(testNamespace);
    assertThat(metadataAfterRemoval).doesNotContainKey("description");
    assertThat(metadataAfterRemoval).containsEntry("owner", "updated_user");

    // Test table operations (Catalog interface)
    TableIdentifier tableId = TableIdentifier.of(testNamespace, "test_table");
    Table table = jdbcCatalog.createTable(tableId, SCHEMA, PartitionSpec.unpartitioned());
    assertThat(table).isNotNull();
    assertThat(jdbcCatalog.tableExists(tableId)).isTrue();
    assertThat(jdbcCatalog.listTables(testNamespace)).contains(tableId);

    // Test table loading (Catalog interface)
    Table loadedTable = jdbcCatalog.loadTable(tableId);
    assertThat(loadedTable.schema().asStruct()).isEqualTo(SCHEMA.asStruct());

    // Test table renaming (Catalog interface)
    TableIdentifier renamedTableId = TableIdentifier.of(testNamespace, "renamed_table");
    jdbcCatalog.renameTable(tableId, renamedTableId);
    assertThat(jdbcCatalog.tableExists(renamedTableId)).isTrue();
    assertThat(jdbcCatalog.tableExists(tableId)).isFalse();

    // Test view operations (ViewCatalog interface - only available in SchemaVersion.V1)
    TableIdentifier viewId = TableIdentifier.of(testNamespace, "test_view");
    jdbcCatalog
        .buildView(viewId)
        .withQuery("spark", "SELECT * FROM renamed_table")
        .withSchema(SCHEMA)
        .withDefaultNamespace(testNamespace)
        .create();

    assertThat(jdbcCatalog.viewExists(viewId)).isTrue();
    assertThat(jdbcCatalog.listViews(testNamespace)).contains(viewId);

    // Test view renaming (ViewCatalog interface)
    TableIdentifier renamedViewId = TableIdentifier.of(testNamespace, "renamed_view");
    jdbcCatalog.renameView(viewId, renamedViewId);
    assertThat(jdbcCatalog.viewExists(renamedViewId)).isTrue();
    assertThat(jdbcCatalog.viewExists(viewId)).isFalse();

    // Test dropping view (ViewCatalog interface)
    assertThat(jdbcCatalog.dropView(renamedViewId)).isTrue();
    assertThat(jdbcCatalog.viewExists(renamedViewId)).isFalse();

    // Test dropping table (Catalog interface)
    assertThat(jdbcCatalog.dropTable(renamedTableId)).isTrue();
    assertThat(jdbcCatalog.tableExists(renamedTableId)).isFalse();

    // Test dropping namespace (SupportsNamespaces interface)
    assertThat(jdbcCatalog.dropNamespace(testNamespace)).isTrue();
    assertThat(jdbcCatalog.namespaceExists(testNamespace)).isFalse();
  }
}
