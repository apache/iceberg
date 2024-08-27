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

import static org.apache.iceberg.NullOrder.NULLS_FIRST;
import static org.apache.iceberg.SortDirection.ASC;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.CatalogTests;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.View;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.sqlite.SQLiteDataSource;

public class TestJdbcCatalog extends CatalogTests<JdbcCatalog> {

  static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get(), "unique ID"),
          required(2, "data", Types.StringType.get()));
  static final PartitionSpec PARTITION_SPEC =
      PartitionSpec.builderFor(SCHEMA).bucket("data", 16).build();

  static Configuration conf = new Configuration();
  private static JdbcCatalog catalog;
  private static String warehouseLocation;

  @TempDir java.nio.file.Path tableDir;

  @Override
  protected JdbcCatalog catalog() {
    return catalog;
  }

  @Override
  protected boolean supportsNamespaceProperties() {
    return true;
  }

  @Override
  protected boolean supportsNestedNamespaces() {
    return true;
  }

  protected List<String> metadataVersionFiles(String location) {
    return Stream.of(new File(location).listFiles())
        .filter(file -> !file.isDirectory())
        .map(File::getName)
        .filter(fileName -> fileName.endsWith("metadata.json"))
        .collect(Collectors.toList());
  }

  protected List<String> manifestFiles(String location) {
    return Stream.of(new File(location).listFiles())
        .filter(file -> !file.isDirectory())
        .map(File::getName)
        .filter(fileName -> fileName.endsWith(".avro"))
        .collect(Collectors.toList());
  }

  @BeforeEach
  public void setupTable() throws Exception {
    catalog = initCatalog("test_jdbc_catalog", Maps.newHashMap());
  }

  private JdbcCatalog initCatalog(String catalogName, Map<String, String> props) {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(
        CatalogProperties.URI,
        "jdbc:sqlite:file::memory:?ic" + UUID.randomUUID().toString().replace("-", ""));

    properties.put(JdbcCatalog.PROPERTY_PREFIX + "username", "user");
    properties.put(JdbcCatalog.PROPERTY_PREFIX + "password", "password");
    warehouseLocation = this.tableDir.toAbsolutePath().toString();
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);
    properties.put("type", "jdbc");
    properties.putAll(props);

    return (JdbcCatalog) CatalogUtil.buildIcebergCatalog(catalogName, properties, conf);
  }

  @Test
  public void testInitialize() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, this.tableDir.toAbsolutePath().toString());
    properties.put(CatalogProperties.URI, "jdbc:sqlite:file::memory:?icebergDB");
    JdbcCatalog jdbcCatalog = new JdbcCatalog();
    jdbcCatalog.setConf(conf);
    jdbcCatalog.initialize("test_jdbc_catalog", properties);
    // second initialization should not fail even if tables are already created
    jdbcCatalog.initialize("test_jdbc_catalog", properties);
    jdbcCatalog.initialize("test_jdbc_catalog", properties);
  }

  @Test
  public void testDisableInitCatalogTablesOverridesDefault() throws Exception {
    // as this test uses different connections, we can't use memory database (as it's per
    // connection), but a file database instead
    java.nio.file.Path dbFile = Files.createTempFile("icebergInitCatalogTables", "db");
    String jdbcUrl = "jdbc:sqlite:" + dbFile.toAbsolutePath();

    Map<String, String> properties = Maps.newHashMap();
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, this.tableDir.toAbsolutePath().toString());
    properties.put(CatalogProperties.URI, jdbcUrl);
    properties.put(JdbcUtil.INIT_CATALOG_TABLES_PROPERTY, "false");

    JdbcCatalog jdbcCatalog = new JdbcCatalog();
    jdbcCatalog.initialize("test_jdbc_catalog", properties);

    assertThat(catalogTablesExist(jdbcUrl)).isFalse();

    assertThatThrownBy(() -> jdbcCatalog.listNamespaces())
        .isInstanceOf(UncheckedSQLException.class)
        .hasMessage(String.format("Failed to execute query: %s", JdbcUtil.LIST_ALL_NAMESPACES_SQL));
  }

  @Test
  public void testEnableInitCatalogTablesOverridesDefault() throws Exception {
    // as this test uses different connections, we can't use memory database (as it's per
    // connection), but a file database instead
    java.nio.file.Path dbFile = Files.createTempFile("icebergInitCatalogTables", "db");
    String jdbcUrl = "jdbc:sqlite:" + dbFile.toAbsolutePath();

    Map<String, String> properties = Maps.newHashMap();
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, this.tableDir.toAbsolutePath().toString());
    properties.put(CatalogProperties.URI, jdbcUrl);
    properties.put(JdbcUtil.INIT_CATALOG_TABLES_PROPERTY, "true");

    JdbcCatalog jdbcCatalog = new JdbcCatalog(null, null, false);
    jdbcCatalog.initialize("test_jdbc_catalog", properties);

    assertThat(catalogTablesExist(jdbcUrl)).isTrue();
  }

  @Test
  public void testRetryingErrorCodesProperty() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, this.tableDir.toAbsolutePath().toString());
    properties.put(CatalogProperties.URI, "jdbc:sqlite:file::memory:?icebergDB");
    properties.put(JdbcUtil.RETRYABLE_STATUS_CODES, "57000,57P03,57P04");
    JdbcCatalog jdbcCatalog = new JdbcCatalog();
    jdbcCatalog.setConf(conf);
    jdbcCatalog.initialize("test_catalog_with_retryable_status_codes", properties);
    JdbcClientPool jdbcClientPool = jdbcCatalog.connectionPool();
    List<SQLException> expectedRetryableExceptions =
        Lists.newArrayList(
            new SQLException("operator_intervention", "57000"),
            new SQLException("cannot_connect_now", "57P03"),
            new SQLException("database_dropped", "57P04"));
    JdbcClientPool.COMMON_RETRYABLE_CONNECTION_SQL_STATES.forEach(
        code -> expectedRetryableExceptions.add(new SQLException("some failure", code)));

    expectedRetryableExceptions.forEach(
        exception -> {
          assertThat(jdbcClientPool.isConnectionException(exception))
              .as(String.format("%s status should be retryable", exception.getSQLState()))
              .isTrue();
        });

    // Test the same retryable status codes but with spaces in the configuration
    properties.put(JdbcUtil.RETRYABLE_STATUS_CODES, "57000, 57P03, 57P04");
    jdbcCatalog.initialize("test_catalog_with_retryable_status_codes_with_spaces", properties);
    JdbcClientPool updatedClientPool = jdbcCatalog.connectionPool();
    expectedRetryableExceptions.forEach(
        exception -> {
          assertThat(updatedClientPool.isConnectionException(exception))
              .as(String.format("%s status should be retryable", exception.getSQLState()))
              .isTrue();
        });
  }

  @Test
  public void testSqlNonTransientExceptionNotRetryable() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, this.tableDir.toAbsolutePath().toString());
    properties.put(CatalogProperties.URI, "jdbc:sqlite:file::memory:?icebergDB");
    properties.put(JdbcUtil.RETRYABLE_STATUS_CODES, "57000,57P03,57P04");
    JdbcCatalog jdbcCatalog = new JdbcCatalog();
    jdbcCatalog.setConf(conf);
    jdbcCatalog.initialize("test_catalog_with_retryable_status_codes", properties);
    JdbcClientPool jdbcClientPool = jdbcCatalog.connectionPool();
    assertThat(
            jdbcClientPool.isConnectionException(
                new SQLNonTransientConnectionException("Failed to authenticate")))
        .as("SQL Non Transient exception is not retryable")
        .isFalse();
  }

  @Test
  public void testInitSchemaV0() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, this.tableDir.toAbsolutePath().toString());
    properties.put(CatalogProperties.URI, "jdbc:sqlite:file::memory:?icebergDBV0");
    properties.put(JdbcUtil.SCHEMA_VERSION_PROPERTY, JdbcUtil.SchemaVersion.V0.name());
    JdbcCatalog jdbcCatalog = new JdbcCatalog();
    jdbcCatalog.setConf(conf);
    jdbcCatalog.initialize("v0catalog", properties);

    TableIdentifier tableIdent = TableIdentifier.of(Namespace.of("ns1"), "tbl");
    Table table =
        jdbcCatalog
            .buildTable(tableIdent, SCHEMA)
            .withPartitionSpec(PARTITION_SPEC)
            .withProperty("key1", "value1")
            .withProperty("key2", "value2")
            .create();

    assertThat(table.schema().asStruct()).isEqualTo(SCHEMA.asStruct());
    assertThat(table.spec().fields()).hasSize(1);
    assertThat(table.properties()).containsEntry("key1", "value1").containsEntry("key2", "value2");

    assertThat(jdbcCatalog.listTables(Namespace.of("ns1"))).hasSize(1).contains(tableIdent);

    assertThatThrownBy(() -> jdbcCatalog.listViews(Namespace.of("namespace1")))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage(JdbcCatalog.VIEW_WARNING_LOG_MESSAGE);

    assertThatThrownBy(
            () -> jdbcCatalog.buildView(TableIdentifier.of("namespace1", "view")).create())
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage(JdbcCatalog.VIEW_WARNING_LOG_MESSAGE);
  }

  @Test
  public void testSchemaIsMigratedToAddViewSupport() throws Exception {
    // as this test uses different connections, we can't use memory database (as it's per
    // connection), but a file database instead
    java.nio.file.Path dbFile = Files.createTempFile("icebergSchemaUpdate", "db");
    String jdbcUrl = "jdbc:sqlite:" + dbFile.toAbsolutePath();

    initLegacySchema(jdbcUrl);

    Map<String, String> properties = Maps.newHashMap();
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, this.tableDir.toAbsolutePath().toString());
    properties.put(CatalogProperties.URI, jdbcUrl);
    properties.put(JdbcUtil.SCHEMA_VERSION_PROPERTY, JdbcUtil.SchemaVersion.V1.name());
    JdbcCatalog jdbcCatalog = new JdbcCatalog();
    jdbcCatalog.setConf(conf);
    jdbcCatalog.initialize("TEST", properties);

    TableIdentifier tableOne = TableIdentifier.of("namespace1", "table1");
    TableIdentifier tableTwo = TableIdentifier.of("namespace2", "table2");
    assertThat(jdbcCatalog.listTables(Namespace.of("namespace1")))
        .hasSize(1)
        .containsExactly(tableOne);

    assertThat(jdbcCatalog.listTables(Namespace.of("namespace2")))
        .hasSize(1)
        .containsExactly(tableTwo);

    assertThat(jdbcCatalog.listViews(Namespace.of("namespace1"))).isEmpty();

    TableIdentifier view = TableIdentifier.of("namespace1", "view");
    jdbcCatalog
        .buildView(view)
        .withQuery("spark", "select * from tbl")
        .withSchema(SCHEMA)
        .withDefaultNamespace(Namespace.of("namespace1"))
        .create();

    assertThat(jdbcCatalog.listViews(Namespace.of("namespace1"))).hasSize(1).containsExactly(view);

    TableIdentifier tableThree = TableIdentifier.of("namespace2", "table3");
    jdbcCatalog.createTable(tableThree, SCHEMA);
    assertThat(jdbcCatalog.tableExists(tableThree)).isTrue();

    // testing append datafile to check commit, it should not throw an exception
    jdbcCatalog.loadTable(tableOne).newAppend().appendFile(FILE_A).commit();
    jdbcCatalog.loadTable(tableTwo).newAppend().appendFile(FILE_B).commit();

    assertThat(jdbcCatalog.tableExists(tableOne)).isTrue();
    assertThat(jdbcCatalog.tableExists(tableTwo)).isTrue();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testExistingV0SchemaSupport(boolean initializeCatalogTables) throws Exception {
    // as this test uses different connection, we can't use memory database (as it's per
    // connection), but a
    // file database instead
    java.nio.file.Path dbFile = Files.createTempFile("icebergOldSchema", "db");
    String jdbcUrl = "jdbc:sqlite:" + dbFile.toAbsolutePath();

    initLegacySchema(jdbcUrl);

    Map<String, String> properties = Maps.newHashMap();
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, this.tableDir.toAbsolutePath().toString());
    properties.put(CatalogProperties.URI, jdbcUrl);
    JdbcCatalog jdbcCatalog = new JdbcCatalog(null, null, initializeCatalogTables);
    jdbcCatalog.setConf(conf);
    jdbcCatalog.initialize("TEST", properties);

    TableIdentifier tableOne = TableIdentifier.of("namespace1", "table1");
    TableIdentifier tableTwo = TableIdentifier.of("namespace2", "table2");

    assertThat(jdbcCatalog.listTables(Namespace.of("namespace1")))
        .hasSize(1)
        .containsExactly(tableOne);

    assertThat(jdbcCatalog.listTables(Namespace.of("namespace2")))
        .hasSize(1)
        .containsExactly(tableTwo);

    TableIdentifier newTable = TableIdentifier.of("namespace1", "table2");
    jdbcCatalog.buildTable(newTable, SCHEMA).create();

    assertThat(jdbcCatalog.listTables(Namespace.of("namespace1")))
        .hasSize(2)
        .containsExactly(tableOne, newTable);

    assertThatThrownBy(() -> jdbcCatalog.listViews(Namespace.of("namespace1")))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage(JdbcCatalog.VIEW_WARNING_LOG_MESSAGE);

    assertThatThrownBy(
            () -> jdbcCatalog.buildView(TableIdentifier.of("namespace1", "view")).create())
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage(JdbcCatalog.VIEW_WARNING_LOG_MESSAGE);
  }

  @Test
  public void testCreateTableBuilder() {
    TableIdentifier tableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    Table table =
        catalog
            .buildTable(tableIdent, SCHEMA)
            .withPartitionSpec(PARTITION_SPEC)
            .withProperties(null)
            .withProperty("key1", "value1")
            .withProperties(ImmutableMap.of("key2", "value2"))
            .create();

    assertThat(table.schema()).hasToString(SCHEMA.toString());
    assertThat(table.spec().fields()).hasSize(1);
    assertThat(table.properties()).containsEntry("key1", "value1").containsEntry("key2", "value2");
  }

  @Test
  public void testCreateTableTxnBuilder() {
    TableIdentifier tableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    Transaction txn =
        catalog
            .buildTable(tableIdent, SCHEMA)
            .withPartitionSpec(null)
            .withProperty("key1", "testval1")
            .createTransaction();
    txn.commitTransaction();
    Table table = catalog.loadTable(tableIdent);

    assertThat(table.schema()).hasToString(SCHEMA.toString());
    assertThat(table.spec().isUnpartitioned()).isTrue();
    assertThat(table.properties()).containsEntry("key1", "testval1");
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  public void testReplaceTxnBuilder(int formatVersion) {
    TableIdentifier tableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl");

    final DataFile fileA =
        DataFiles.builder(PARTITION_SPEC)
            .withPath("/path/to/data-a.parquet")
            .withFileSizeInBytes(0)
            .withPartitionPath("data_bucket=0") // easy way to set partition data for now
            .withRecordCount(2) // needs at least one record or else metrics will filter it out
            .build();

    Transaction createTxn =
        catalog
            .buildTable(tableIdent, SCHEMA)
            .withPartitionSpec(PARTITION_SPEC)
            .withProperty("key1", "value1")
            .withProperty(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion))
            .createOrReplaceTransaction();

    createTxn.newAppend().appendFile(fileA).commit();

    createTxn.commitTransaction();

    Table table = catalog.loadTable(tableIdent);
    assertThat(table.currentSnapshot()).isNotNull();

    Transaction replaceTxn =
        catalog.buildTable(tableIdent, SCHEMA).withProperty("key2", "value2").replaceTransaction();
    replaceTxn.commitTransaction();

    table = catalog.loadTable(tableIdent);
    assertThat(table.currentSnapshot()).isNull();
    if (formatVersion == 1) {
      PartitionSpec v1Expected =
          PartitionSpec.builderFor(table.schema())
              .alwaysNull("data", "data_bucket")
              .withSpecId(1)
              .build();
      assertThat(table.spec())
          .as("Table should have a spec with one void field")
          .isEqualTo(v1Expected);
    } else {
      assertThat(table.spec().isUnpartitioned()).as("Table spec must be unpartitioned").isTrue();
    }

    assertThat(table.properties()).containsEntry("key1", "value1").containsEntry("key2", "value2");
  }

  @Test
  public void testCreateTableDefaultSortOrder() {
    TableIdentifier tableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    Table table = catalog.createTable(tableIdent, SCHEMA, PARTITION_SPEC);

    SortOrder sortOrder = table.sortOrder();
    assertThat(sortOrder.orderId()).as("Order ID must match").isZero();
    assertThat(sortOrder.isUnsorted()).as("Order must unsorted").isTrue();
  }

  @Test
  public void testCreateTableCustomSortOrder() {
    TableIdentifier tableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    SortOrder order = SortOrder.builderFor(SCHEMA).asc("id", NULLS_FIRST).build();
    Table table =
        catalog
            .buildTable(tableIdent, SCHEMA)
            .withPartitionSpec(PARTITION_SPEC)
            .withSortOrder(order)
            .create();

    SortOrder sortOrder = table.sortOrder();
    assertThat(sortOrder.orderId()).as("Order ID must match").isEqualTo(1);
    assertThat(sortOrder.fields()).as("Order must have 1 field").hasSize(1);
    assertThat(sortOrder.fields().get(0).direction()).as("Direction must match ").isEqualTo(ASC);
    assertThat(sortOrder.fields().get(0).nullOrder())
        .as("Null order must match ")
        .isEqualTo(NULLS_FIRST);
    Transform<?, ?> transform = Transforms.identity();
    assertThat(sortOrder.fields().get(0).transform())
        .as("Transform must match")
        .isEqualTo(transform);
  }

  @Test
  public void testBasicCatalog() throws Exception {
    TableIdentifier testTable = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    catalog.createTable(testTable, SCHEMA, PartitionSpec.unpartitioned());
    String metaLocation = catalog.defaultWarehouseLocation(testTable);

    FileSystem fs = Util.getFs(new Path(metaLocation), conf);
    assertThat(fs.isDirectory(new Path(metaLocation))).isTrue();

    assertThatThrownBy(() -> catalog.createTable(testTable, SCHEMA, PartitionSpec.unpartitioned()))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessage("Table already exists: db.ns1.ns2.tbl");

    catalog.dropTable(testTable);
  }

  @Test
  public void testCreateAndDropTableWithoutNamespace() throws Exception {
    TableIdentifier testTable = TableIdentifier.of("tbl");
    Table table = catalog.createTable(testTable, SCHEMA, PartitionSpec.unpartitioned());

    assertThat(SCHEMA).hasToString(table.schema().toString());
    assertThat(table.name()).isEqualTo(catalog.name() + ".tbl");
    String metaLocation = catalog.defaultWarehouseLocation(testTable);

    FileSystem fs = Util.getFs(new Path(metaLocation), conf);
    assertThat(fs.isDirectory(new Path(metaLocation))).isTrue();

    catalog.dropTable(testTable, true);
  }

  @Test
  public void testDefaultWarehouseLocation() throws Exception {
    TableIdentifier testTable = TableIdentifier.of("tbl");
    TableIdentifier testTable2 = TableIdentifier.of(Namespace.of("ns"), "tbl");
    assertThat(warehouseLocation + "/" + testTable.name())
        .isEqualTo(catalog.defaultWarehouseLocation(testTable));
    assertThat(warehouseLocation + "/" + testTable2.namespace() + "/" + testTable2.name())
        .isEqualTo(catalog.defaultWarehouseLocation(testTable2));
  }

  @Test
  public void testConcurrentCommit() throws IOException {
    TableIdentifier tableIdentifier = TableIdentifier.of("db", "table");
    Table table = catalog.createTable(tableIdentifier, SCHEMA, PartitionSpec.unpartitioned());
    // append file and commit!
    String data = tableDir.resolve("data.parquet").toAbsolutePath().toString();
    Files.write(Paths.get(data), Lists.newArrayList(), StandardCharsets.UTF_8);
    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(data)
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();
    table.newAppend().appendFile(dataFile).commit();
    assertThat(table.history()).hasSize(1);
    catalog.dropTable(tableIdentifier);
    data = tableDir.resolve("data2.parquet").toAbsolutePath().toString();
    Files.write(Paths.get(data), Lists.newArrayList(), StandardCharsets.UTF_8);
    DataFile dataFile2 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(data)
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();

    assertThatThrownBy(() -> table.newAppend().appendFile(dataFile2).commit())
        .isInstanceOf(NoSuchTableException.class)
        .hasMessage(
            "Failed to load table db.table from catalog test_jdbc_catalog: dropped by another process");
  }

  @Test
  public void testCommitHistory() throws IOException {
    TableIdentifier testTable = TableIdentifier.of("db", "ns", "tbl");
    catalog.createTable(testTable, SCHEMA, PartitionSpec.unpartitioned());
    Table table = catalog.loadTable(testTable);

    String data = tableDir.resolve("data.parquet").toAbsolutePath().toString();
    Files.write(Paths.get(data), Lists.newArrayList(), StandardCharsets.UTF_8);
    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(data)
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();
    table.newAppend().appendFile(dataFile).commit();
    assertThat(table.history()).hasSize(1);

    data = tableDir.resolve("data2.parquet").toAbsolutePath().toString();
    Files.write(Paths.get(data), Lists.newArrayList(), StandardCharsets.UTF_8);
    dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(data)
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();
    table.newAppend().appendFile(dataFile).commit();
    assertThat(table.history()).hasSize(2);

    data = tableDir.resolve("data3.parquet").toAbsolutePath().toString();
    Files.write(Paths.get(data), Lists.newArrayList(), StandardCharsets.UTF_8);
    dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(data)
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();
    table.newAppend().appendFile(dataFile).commit();
    assertThat(table.history()).hasSize(3);
  }

  @Test
  public void testDropTable() {
    TableIdentifier testTable = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    TableIdentifier testTable2 = TableIdentifier.of("db", "ns1", "ns2", "tbl2");
    catalog.createTable(testTable, SCHEMA, PartitionSpec.unpartitioned());
    catalog.createTable(testTable2, SCHEMA, PartitionSpec.unpartitioned());
    catalog.dropTable(testTable);
    assertThat(catalog.listTables(testTable.namespace())).doesNotContain(testTable);
    catalog.dropTable(testTable2);

    assertThatThrownBy(() -> catalog.listTables(testTable2.namespace()))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessage("Namespace does not exist: db.ns1.ns2");

    assertThat(catalog.dropTable(TableIdentifier.of("db", "tbl-not-exists"))).isFalse();
  }

  @Test
  public void testDropTableWithoutMetadataFile() {
    TableIdentifier testTable = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    catalog.createTable(testTable, SCHEMA, PartitionSpec.unpartitioned());
    String metadataFileLocation = catalog.newTableOps(testTable).current().metadataFileLocation();
    TableOperations ops = catalog.newTableOps(testTable);
    ops.io().deleteFile(metadataFileLocation);
    assertThat(catalog.dropTable(testTable)).isTrue();
    assertThatThrownBy(() -> catalog.loadTable(testTable))
        .isInstanceOf(NoSuchTableException.class)
        .hasMessageContaining("Table does not exist:");
  }

  @Test
  public void testRenameTable() {
    TableIdentifier from = TableIdentifier.of("db", "tbl1");
    TableIdentifier to = TableIdentifier.of("db", "tbl2-newtable");
    catalog.createTable(from, SCHEMA, PartitionSpec.unpartitioned());
    catalog.renameTable(from, to);
    assertThat(catalog.listTables(to.namespace())).contains(to).doesNotContain(from);
    assertThat(catalog.loadTable(to).name()).endsWith(to.name());

    assertThatThrownBy(() -> catalog.renameTable(TableIdentifier.of("db", "tbl-not-exists"), to))
        .isInstanceOf(NoSuchTableException.class)
        .hasMessage("Table does not exist: db.tbl-not-exists");

    // rename table to existing table name!
    TableIdentifier from2 = TableIdentifier.of("db", "tbl2");
    catalog.createTable(from2, SCHEMA, PartitionSpec.unpartitioned());
    assertThatThrownBy(() -> catalog.renameTable(from2, to))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessage("Table already exists: db.tbl2-newtable");
  }

  @Test
  public void testListTables() {
    TableIdentifier tbl1 = TableIdentifier.of("db", "tbl1");
    TableIdentifier tbl2 = TableIdentifier.of("db", "tbl2");
    TableIdentifier tbl3 = TableIdentifier.of("db", "tbl2", "subtbl2");
    TableIdentifier tbl4 = TableIdentifier.of("db", "ns1", "tbl3");
    TableIdentifier tbl5 = TableIdentifier.of("db", "metadata", "metadata");

    Lists.newArrayList(tbl1, tbl2, tbl3, tbl4, tbl5)
        .forEach(t -> catalog.createTable(t, SCHEMA, PartitionSpec.unpartitioned()));

    List<TableIdentifier> tbls1 = catalog.listTables(Namespace.of("db"));
    Set<String> tblSet = Sets.newHashSet(tbls1.stream().map(TableIdentifier::name).iterator());
    assertThat(tblSet).hasSize(2).contains("tbl1", "tbl2");

    List<TableIdentifier> tbls2 = catalog.listTables(Namespace.of("db", "ns1"));
    assertThat(tbls2).hasSize(1);
    assertThat(tbls2.get(0).name()).isEqualTo("tbl3");

    assertThatThrownBy(() -> catalog.listTables(Namespace.of("db", "ns1", "ns2")))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessage("Namespace does not exist: db.ns1.ns2");
  }

  @Test
  public void testCallingLocationProviderWhenNoCurrentMetadata() {
    TableIdentifier tableIdent = TableIdentifier.of("ns1", "ns2", "table1");
    Transaction create = catalog.newCreateTableTransaction(tableIdent, SCHEMA);
    create.table().locationProvider(); // NPE triggered if not handled appropriately
    create.commitTransaction();

    assertThat(catalog.listTables(Namespace.of("ns1", "ns2"))).as("1 table expected").hasSize(1);
    catalog.dropTable(tableIdent, true);
  }

  @Test
  public void testExistingTableUpdate() {
    TableIdentifier tableIdent = TableIdentifier.of("ns1", "ns2", "table1");
    Transaction create = catalog.newCreateTableTransaction(tableIdent, SCHEMA);
    create.table().locationProvider(); // NPE triggered if not handled appropriately
    create.commitTransaction();
    Table icebergTable = catalog.loadTable(tableIdent);
    // add a column
    icebergTable.updateSchema().addColumn("Coll3", Types.LongType.get()).commit();
    icebergTable = catalog.loadTable(tableIdent);
    // Only 2 snapshotFile Should exist and no manifests should exist
    assertThat(metadataVersionFiles(icebergTable.location() + "/metadata/")).hasSize(2);
    assertThat(manifestFiles(icebergTable.location() + "/metadata/")).isEmpty();
    assertThat(icebergTable.schema().asStruct()).isNotEqualTo(SCHEMA.asStruct());
    assertThat(icebergTable.schema().asStruct().toString()).contains("Coll3");
  }

  @Test
  public void testTableName() {
    TableIdentifier tableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    catalog.buildTable(tableIdent, SCHEMA).withPartitionSpec(PARTITION_SPEC).create();
    Table table = catalog.loadTable(tableIdent);
    assertThat(table.name()).as("Name must match").isEqualTo(catalog.name() + ".db.ns1.ns2.tbl");

    TableIdentifier snapshotsTableIdent =
        TableIdentifier.of("db", "ns1", "ns2", "tbl", "snapshots");
    Table snapshotsTable = catalog.loadTable(snapshotsTableIdent);
    assertThat(snapshotsTable.name())
        .as("Name must match")
        .isEqualTo(catalog.name() + ".db.ns1.ns2.tbl.snapshots");
  }

  @Test
  public void testListNamespace() {
    TableIdentifier tbl1 = TableIdentifier.of("db", "ns1", "ns2", "metadata");
    TableIdentifier tbl2 = TableIdentifier.of("db", "ns2", "ns3", "tbl2");
    TableIdentifier tbl3 = TableIdentifier.of("db", "ns3", "tbl4");
    TableIdentifier tbl4 = TableIdentifier.of("db", "metadata");
    TableIdentifier tbl5 = TableIdentifier.of("db2", "metadata");
    TableIdentifier tbl6 = TableIdentifier.of("tbl6");
    TableIdentifier tbl7 = TableIdentifier.of("db2", "ns4", "tbl5");
    TableIdentifier tbl8 = TableIdentifier.of("d_", "ns5", "tbl6");
    TableIdentifier tbl9 = TableIdentifier.of("d%", "ns6", "tbl7");

    Lists.newArrayList(tbl1, tbl2, tbl3, tbl4, tbl5, tbl6, tbl7, tbl8, tbl9)
        .forEach(t -> catalog.createTable(t, SCHEMA, PartitionSpec.unpartitioned()));

    List<Namespace> nsp1 = catalog.listNamespaces(Namespace.of("db"));
    assertThat(nsp1).hasSize(3);
    Set<String> tblSet = Sets.newHashSet(nsp1.stream().map(Namespace::toString).iterator());
    assertThat(tblSet).hasSize(3).contains("db.ns1", "db.ns2", "db.ns3");

    List<Namespace> nsp2 = catalog.listNamespaces(Namespace.of("db", "ns1"));
    assertThat(nsp2).hasSize(1);
    assertThat(nsp2.get(0)).hasToString("db.ns1.ns2");

    List<Namespace> nsp3 = catalog.listNamespaces();
    Set<String> tblSet2 = Sets.newHashSet(nsp3.stream().map(Namespace::toString).iterator());
    assertThat(tblSet2).hasSize(5).contains("db", "db2", "d_", "d%", "");

    List<Namespace> nsp4 = catalog.listNamespaces();
    Set<String> tblSet3 = Sets.newHashSet(nsp4.stream().map(Namespace::toString).iterator());
    assertThat(tblSet3).hasSize(5).contains("db", "db2", "d_", "d%", "");

    List<Namespace> nsp5 = catalog.listNamespaces(Namespace.of("d_"));
    assertThat(nsp5).hasSize(1);
    assertThat(nsp5.get(0)).hasToString("d_.ns5");

    List<Namespace> nsp6 = catalog.listNamespaces(Namespace.of("d%"));
    assertThat(nsp6).hasSize(1);
    assertThat(nsp6.get(0)).hasToString("d%.ns6");

    assertThatThrownBy(() -> catalog.listNamespaces(Namespace.of("db", "db2", "ns2")))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessage("Namespace does not exist: db.db2.ns2");
  }

  @Test
  public void testLoadNamespaceMeta() {
    TableIdentifier tbl1 = TableIdentifier.of("db", "ns1", "ns2", "metadata");
    TableIdentifier tbl2 = TableIdentifier.of("db", "ns2", "ns3", "tbl2");
    TableIdentifier tbl3 = TableIdentifier.of("db", "ns3", "tbl4");
    TableIdentifier tbl4 = TableIdentifier.of("db", "metadata");

    Lists.newArrayList(tbl1, tbl2, tbl3, tbl4)
        .forEach(t -> catalog.createTable(t, SCHEMA, PartitionSpec.unpartitioned()));

    assertThat(catalog.loadNamespaceMetadata(Namespace.of("db"))).containsKey("location");

    assertThatThrownBy(() -> catalog.loadNamespaceMetadata(Namespace.of("db", "db2", "ns2")))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessage("Namespace does not exist: db.db2.ns2");
  }

  @Test
  public void testNamespaceExists() {
    TableIdentifier tbl1 = TableIdentifier.of("db", "ns1", "ns2", "metadata");
    TableIdentifier tbl2 = TableIdentifier.of("db", "ns2", "ns3", "tbl2");
    TableIdentifier tbl3 = TableIdentifier.of("db", "ns3", "tbl4");
    TableIdentifier tbl4 = TableIdentifier.of("db", "metadata");

    Lists.newArrayList(tbl1, tbl2, tbl3, tbl4)
        .forEach(t -> catalog.createTable(t, SCHEMA, PartitionSpec.unpartitioned()));
    assertThat(catalog.namespaceExists(Namespace.of("db", "ns1", "ns2")))
        .as("Should true to namespace exist")
        .isTrue();
    assertThat(catalog.namespaceExists(Namespace.of("db", "db2", "not_exist")))
        .as("Should false to namespace doesn't exist")
        .isFalse();
  }

  @Test
  public void testDropNamespace() {
    assertThat(catalog.dropNamespace(Namespace.of("db", "ns1_not_exitss")))
        .as("Should return false if drop does not modify state")
        .isFalse();

    TableIdentifier tbl0 = TableIdentifier.of("db", "ns1", "ns2", "tbl2");
    TableIdentifier tbl1 = TableIdentifier.of("db", "ns1", "ns2", "tbl1");
    TableIdentifier tbl2 = TableIdentifier.of("db", "ns1", "tbl2");
    TableIdentifier tbl3 = TableIdentifier.of("db", "ns3", "tbl4");
    TableIdentifier tbl4 = TableIdentifier.of("db", "tbl");

    Lists.newArrayList(tbl0, tbl1, tbl2, tbl3, tbl4)
        .forEach(t -> catalog.createTable(t, SCHEMA, PartitionSpec.unpartitioned()));

    assertThatThrownBy(() -> catalog.dropNamespace(tbl1.namespace()))
        .isInstanceOf(NamespaceNotEmptyException.class)
        .hasMessage("Namespace db.ns1.ns2 is not empty. 2 tables exist.");

    assertThatThrownBy(() -> catalog.dropNamespace(tbl2.namespace()))
        .isInstanceOf(NamespaceNotEmptyException.class)
        .hasMessage("Namespace db.ns1 is not empty. 1 tables exist.");

    assertThatThrownBy(() -> catalog.dropNamespace(tbl4.namespace()))
        .isInstanceOf(NamespaceNotEmptyException.class)
        .hasMessage("Namespace db is not empty. 1 tables exist.");
  }

  @Test
  public void testCreateNamespace() {
    Namespace testNamespace = Namespace.of("testDb", "ns1", "ns2");
    assertThat(catalog.namespaceExists(testNamespace)).isFalse();
    assertThat(catalog.namespaceExists(Namespace.of("testDb", "ns1"))).isFalse();
    catalog.createNamespace(testNamespace);
    assertThat(catalog.namespaceExists(testNamespace)).isTrue();
    assertThat(catalog.namespaceExists(Namespace.of("testDb"))).isTrue();
    assertThat(catalog.namespaceExists(Namespace.of("testDb", "ns1"))).isTrue();
    assertThat(catalog.namespaceExists(Namespace.of("testDb", "ns1", "ns2"))).isTrue();
    assertThat(catalog.namespaceExists(Namespace.of("ns1", "ns2"))).isFalse();
    assertThat(catalog.namespaceExists(Namespace.of("testDb", "ns%"))).isFalse();
    assertThat(catalog.namespaceExists(Namespace.of("testDb", "ns_"))).isFalse();
    assertThat(catalog.namespaceExists(Namespace.of("testDb", "ns1", "ns2", "ns3"))).isFalse();
  }

  @Test
  public void testCreateNamespaceWithBackslashCharacter() {
    Namespace testNamespace = Namespace.of("test\\Db", "ns\\1", "ns3");
    assertThat(catalog.namespaceExists(testNamespace)).isFalse();
    catalog.createNamespace(testNamespace);
    assertThat(catalog.namespaceExists(testNamespace)).isTrue();
    assertThat(catalog.namespaceExists(Namespace.of("test\\Db", "ns\\1"))).isTrue();
    // test that SQL special characters `%`,`.`,`_` are escaped and returns false
    assertThat(catalog.namespaceExists(Namespace.of("test\\%Db", "ns\\.1"))).isFalse();
    assertThat(catalog.namespaceExists(Namespace.of("test%Db", "ns\\.1"))).isFalse();
    assertThat(catalog.namespaceExists(Namespace.of("test%Db"))).isFalse();
    assertThat(catalog.namespaceExists(Namespace.of("test%"))).isFalse();
    assertThat(catalog.namespaceExists(Namespace.of("test\\%"))).isFalse();
    assertThat(catalog.namespaceExists(Namespace.of("test_Db", "ns\\.1"))).isFalse();
    // test that backslash with `%` is escaped and treated correctly
    testNamespace = Namespace.of("test\\%Db2", "ns1");
    assertThat(catalog.namespaceExists(testNamespace)).isFalse();
    catalog.createNamespace(testNamespace);
    assertThat(catalog.namespaceExists(testNamespace)).isTrue();
    assertThat(catalog.namespaceExists(Namespace.of("test\\%Db2"))).isTrue();
    assertThat(catalog.namespaceExists(Namespace.of("test%Db2"))).isFalse();
    assertThat(catalog.namespaceExists(Namespace.of("test\\_Db2"))).isFalse();
  }

  @Test
  public void testCreateNamespaceWithPercentCharacter() {
    Namespace testNamespace = Namespace.of("testDb%", "ns%1");
    assertThat(catalog.namespaceExists(testNamespace)).isFalse();
    catalog.createNamespace(testNamespace);
    assertThat(catalog.namespaceExists(testNamespace)).isTrue();
    assertThat(catalog.namespaceExists(Namespace.of("testDb%"))).isTrue();
    // test that searching with SQL special characters `\`,`%` are escaped and returns false
    assertThat(catalog.namespaceExists(Namespace.of("testDb\\%"))).isFalse();
    assertThat(catalog.namespaceExists(Namespace.of("testDb"))).isFalse();
    assertThat(catalog.namespaceExists(Namespace.of("tes%Db%"))).isFalse();
    assertThat(catalog.namespaceExists(Namespace.of("testDb%", "ns%"))).isFalse();
  }

  @Test
  public void testCreateNamespaceWithUnderscoreCharacter() {
    Namespace testNamespace = Namespace.of("test_Db", "ns_1", "ns_");
    catalog.createNamespace(testNamespace);
    assertThat(catalog.namespaceExists(testNamespace)).isTrue();
    assertThat(catalog.namespaceExists(Namespace.of("test_Db", "ns_1"))).isTrue();
    // test that searching with SQL special characters `_`,`%` are escaped and returns false
    assertThat(catalog.namespaceExists(Namespace.of("test_Db"))).isTrue();
    assertThat(catalog.namespaceExists(Namespace.of("test_D_"))).isFalse();
    assertThat(catalog.namespaceExists(Namespace.of("test_D%"))).isFalse();
    assertThat(catalog.namespaceExists(Namespace.of("test_Db", "ns_"))).isFalse();
    assertThat(catalog.namespaceExists(Namespace.of("test_Db", "ns_%"))).isFalse();
  }

  @Test
  public void testCreateTableInNonExistingNamespace() {
    try (JdbcCatalog jdbcCatalog = initCatalog("non_strict_jdbc_catalog", ImmutableMap.of())) {
      Namespace namespace = Namespace.of("test\\D_b%", "ns1", "ns2");
      TableIdentifier identifier = TableIdentifier.of(namespace, "someTable");
      assertThat(jdbcCatalog.namespaceExists(namespace)).isFalse();
      assertThat(jdbcCatalog.tableExists(identifier)).isFalse();

      // default=non-strict mode allows creating a table in a non-existing namespace
      jdbcCatalog.createTable(identifier, SCHEMA, PARTITION_SPEC);
      assertThat(jdbcCatalog.loadTable(identifier)).isNotNull();
    }
  }

  @Test
  public void testCreateTableInNonExistingNamespaceStrictMode() {
    try (JdbcCatalog jdbcCatalog =
        initCatalog(
            "strict_jdbc_catalog", ImmutableMap.of(JdbcUtil.STRICT_MODE_PROPERTY, "true"))) {
      Namespace namespace = Namespace.of("testDb", "ns1", "ns2");
      TableIdentifier identifier = TableIdentifier.of(namespace, "someTable");
      assertThat(jdbcCatalog.namespaceExists(namespace)).isFalse();
      assertThat(jdbcCatalog.tableExists(identifier)).isFalse();
      assertThatThrownBy(() -> jdbcCatalog.createTable(identifier, SCHEMA, PARTITION_SPEC))
          .isInstanceOf(NoSuchNamespaceException.class)
          .hasMessage(
              "Cannot create table testDb.ns1.ns2.someTable in catalog strict_jdbc_catalog. Namespace testDb.ns1.ns2 does not exist");

      assertThat(jdbcCatalog.tableExists(identifier)).isFalse();

      jdbcCatalog.createNamespace(namespace);
      assertThat(jdbcCatalog.tableExists(identifier)).isFalse();
      jdbcCatalog.createTable(identifier, SCHEMA, PARTITION_SPEC);
      assertThat(jdbcCatalog.loadTable(identifier)).isNotNull();
    }
  }

  @Test
  public void testCreateNamespaceWithMetadata() {
    Namespace testNamespace = Namespace.of("testDb", "ns1", "ns2");

    // Test with metadata
    Map<String, String> testMetadata =
        ImmutableMap.of("key_1", "value_1", "key_2", "value_2", "key_3", "value_3");
    catalog.createNamespace(testNamespace, testMetadata);
    assertThat(catalog.namespaceExists(testNamespace)).isTrue();
  }

  @Test
  public void testNamespaceLocation() {
    Namespace testNamespace = Namespace.of("testDb", "ns1", "ns2");

    // Test with location
    Map<String, String> testMetadata = ImmutableMap.of();
    catalog.createNamespace(testNamespace, testMetadata);

    assertThat(catalog.loadNamespaceMetadata(testNamespace)).containsKey("location");
  }

  @Test
  public void testNamespaceCustomLocation() {
    Namespace testNamespace = Namespace.of("testDb", "ns1", "ns2");
    String namespaceLocation = "file:///tmp/warehouse/ns/path";

    // Test with location
    Map<String, String> testMetadata = ImmutableMap.of("location", namespaceLocation);
    catalog.createNamespace(testNamespace, testMetadata);

    assertThat(catalog.loadNamespaceMetadata(testNamespace))
        .containsEntry("location", namespaceLocation);
  }

  @Test
  public void testSetProperties() {
    Namespace testNamespace = Namespace.of("testDb", "ns1", "ns2");
    Map<String, String> testMetadata =
        ImmutableMap.of("key_1", "value_1", "key_2", "value_2", "key_3", "value_3");
    catalog.createNamespace(testNamespace, testMetadata);

    // Add more properties to set to test insert and update
    Map<String, String> propertiesToSet =
        ImmutableMap.of(
            "key_5",
            "value_5",
            "key_3",
            "new_value_3",
            "key_1",
            "new_value_1",
            "key_4",
            "value_4",
            "key_2",
            "new_value_2");
    assertThat(catalog.namespaceExists(testNamespace)).isTrue();
    assertThat(catalog.setProperties(testNamespace, propertiesToSet)).isTrue();

    Map<String, String> allProperties = catalog.loadNamespaceMetadata(testNamespace);
    assertThat(allProperties).hasSize(6);

    Map<String, String> namespaceProperties = catalog.loadNamespaceMetadata(testNamespace);
    assertThat(propertiesToSet.keySet())
        .as("All new keys should be in the namespace properties")
        .isEqualTo(Sets.intersection(propertiesToSet.keySet(), namespaceProperties.keySet()));

    // values should match
    for (Map.Entry<String, String> keyValue : propertiesToSet.entrySet()) {
      assertThat(namespaceProperties)
          .as("Value for key " + keyValue.getKey() + " should match")
          .containsEntry(keyValue.getKey(), keyValue.getValue());
    }
  }

  @Test
  public void testRemoveProperties() {
    Namespace testNamespace = Namespace.of("testDb", "ns1", "ns2");
    Map<String, String> testMetadata =
        ImmutableMap.of(
            "key_1", "value_1", "key_2", "value_2", "key_3", "value_3", "key_4", "value_4");
    catalog.createNamespace(testNamespace, testMetadata);

    Set<String> propertiesToRemove = ImmutableSet.of("key_2", "key_4");
    catalog.removeProperties(testNamespace, propertiesToRemove);
    Map<String, String> remainderProperties = catalog.loadNamespaceMetadata(testNamespace);

    assertThat(remainderProperties)
        .hasSize(3)
        .containsKey("key_1")
        .containsKey("key_3")
        .containsKey("location");

    // Remove remaining properties to test if it deletes the namespace
    Set<String> allProperties = ImmutableSet.of("key_1", "key_3");
    catalog.removeProperties(testNamespace, allProperties);
    assertThat(catalog.namespaceExists(testNamespace)).isTrue();
  }

  @Test
  public void testConversions() {
    Namespace ns = Namespace.of("db", "db2", "ns2");
    String nsString = JdbcUtil.namespaceToString(ns);
    assertThat(JdbcUtil.stringToNamespace(nsString)).isEqualTo(ns);
  }

  @Test
  public void testCatalogWithCustomMetricsReporter() throws IOException {
    JdbcCatalog catalogWithCustomReporter =
        initCatalog(
            "test_jdbc_catalog_with_custom_reporter",
            ImmutableMap.of(
                CatalogProperties.METRICS_REPORTER_IMPL, CustomMetricsReporter.class.getName()));
    try {
      catalogWithCustomReporter.buildTable(TABLE, SCHEMA).create();
      Table table = catalogWithCustomReporter.loadTable(TABLE);
      table
          .newFastAppend()
          .appendFile(
              DataFiles.builder(PartitionSpec.unpartitioned())
                  .withPath(FileFormat.PARQUET.addExtension(UUID.randomUUID().toString()))
                  .withFileSizeInBytes(10)
                  .withRecordCount(2)
                  .build())
          .commit();
      try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
        assertThat(tasks.iterator()).hasNext();
      }
    } finally {
      catalogWithCustomReporter.dropTable(TABLE);
    }
    // counter of custom metrics reporter should have been increased
    // 1x for commit metrics / 1x for scan metrics
    assertThat(CustomMetricsReporter.COUNTER.get()).isEqualTo(2);
  }

  @Test
  public void testCommitExceptionWithoutMessage() {
    TableIdentifier tableIdent = TableIdentifier.of("db", "tbl");
    BaseTable table = (BaseTable) catalog.buildTable(tableIdent, SCHEMA).create();
    TableOperations ops = table.operations();
    TableMetadata metadataV1 = ops.current();

    table.updateSchema().addColumn("n", Types.IntegerType.get()).commit();
    ops.refresh();

    try (MockedStatic<JdbcUtil> mockedStatic = Mockito.mockStatic(JdbcUtil.class)) {
      mockedStatic
          .when(() -> JdbcUtil.loadTable(any(), any(), any(), any()))
          .thenThrow(new SQLException());
      assertThatThrownBy(() -> ops.commit(ops.current(), metadataV1))
          .isInstanceOf(UncheckedSQLException.class)
          .hasMessageStartingWith("Unknown failure");
    }
  }

  @Test
  public void testCommitExceptionWithMessage() {
    TableIdentifier tableIdent = TableIdentifier.of("db", "tbl");
    BaseTable table = (BaseTable) catalog.buildTable(tableIdent, SCHEMA).create();
    TableOperations ops = table.operations();
    TableMetadata metadataV1 = ops.current();

    table.updateSchema().addColumn("n", Types.IntegerType.get()).commit();
    ops.refresh();

    try (MockedStatic<JdbcUtil> mockedStatic = Mockito.mockStatic(JdbcUtil.class)) {
      mockedStatic
          .when(() -> JdbcUtil.loadTable(any(), any(), any(), any()))
          .thenThrow(new SQLException("constraint failed"));
      assertThatThrownBy(() -> ops.commit(ops.current(), metadataV1))
          .isInstanceOf(AlreadyExistsException.class)
          .hasMessageStartingWith("Table already exists: " + tableIdent);
    }
  }

  @Test
  public void testTablePropsDefinedAtCatalogLevel() throws IOException {
    TableIdentifier tableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    ImmutableMap<String, String> catalogProps =
        ImmutableMap.of(
            CatalogProperties.WAREHOUSE_LOCATION,
            this.tableDir.toAbsolutePath().toString(),
            CatalogProperties.URI,
            "jdbc:sqlite:file::memory:?icebergDBV0",
            JdbcUtil.SCHEMA_VERSION_PROPERTY,
            JdbcUtil.SchemaVersion.V0.name(),
            "table-default.key1",
            "catalog-default-key1",
            "table-default.key2",
            "catalog-default-key2",
            "table-default.key3",
            "catalog-default-key3",
            "table-override.key3",
            "catalog-override-key3",
            "table-override.key4",
            "catalog-override-key4");
    JdbcCatalog jdbcCatalog = new JdbcCatalog();
    jdbcCatalog.setConf(conf);
    jdbcCatalog.initialize("v0catalog", catalogProps);
    Table table =
        jdbcCatalog
            .buildTable(tableIdent, SCHEMA)
            .withProperties(null)
            .withProperty("key2", "table-key2")
            .withProperty("key3", "table-key3")
            .withProperty("key5", "table-key5")
            .create();

    assertThat(table.properties().get("key1"))
        .as("Table defaults set for the catalog must be added to the table properties.")
        .isEqualTo("catalog-default-key1");
    assertThat(table.properties().get("key2"))
        .as("Table property must override table default properties set at catalog level.")
        .isEqualTo("table-key2");
    assertThat(table.properties().get("key3"))
        .as(
            "Table property override set at catalog level must override table default"
                + " properties set at catalog level and table property specified.")
        .isEqualTo("catalog-override-key3");
    assertThat(table.properties().get("key4"))
        .as("Table override not in table props or defaults should be added to table properties")
        .isEqualTo("catalog-override-key4");
    assertThat(table.properties().get("key5"))
        .as(
            "Table properties without any catalog level default or override should be added to table"
                + " properties.")
        .isEqualTo("table-key5");
  }

  @Test
  public void testViewPropsDefinedAtCatalogLevel() throws IOException {
    TableIdentifier viewIdent = TableIdentifier.of("db", "ns1");
    ImmutableMap<String, String> catalogProps =
        ImmutableMap.of(
            CatalogProperties.WAREHOUSE_LOCATION,
            this.tableDir.toAbsolutePath().toString(),
            CatalogProperties.URI,
            "jdbc:sqlite:file::memory:?icebergDBV0",
            JdbcUtil.SCHEMA_VERSION_PROPERTY,
            JdbcUtil.SchemaVersion.V1.name(),
            "table-default.key1",
            "catalog-default-key1",
            "table-default.key2",
            "catalog-default-key2",
            "table-default.key3",
            "catalog-default-key3",
            "table-override.key3",
            "catalog-override-key3",
            "table-override.key4",
            "catalog-override-key4");
    JdbcCatalog jdbcCatalog = new JdbcCatalog();
    jdbcCatalog.setConf(conf);
    jdbcCatalog.initialize("v0catalog", catalogProps);
    View view =
        jdbcCatalog
            .buildView(viewIdent)
            .withQuery("spark", "SELECT * FROM t1")
            .withSchema(SCHEMA)
            .withDefaultNamespace(Namespace.of("db"))
            .withProperty("key2", "view-key2")
            .withProperty("key3", "view-key3")
            .withProperty("key5", "view-key5")
            .create();

    assertThat(view.properties().get("key1"))
        .as("Table defaults set for the catalog must be added to the view properties.")
        .isEqualTo("catalog-default-key1");
    assertThat(view.properties().get("key2"))
        .as("View property must override table default properties set at catalog level.")
        .isEqualTo("view-key2");
    assertThat(view.properties().get("key3"))
        .as(
            "View property override set at catalog level must override table default"
                + " properties set at catalog level and table property specified.")
        .isEqualTo("catalog-override-key3");
    assertThat(view.properties().get("key4"))
        .as("Table override not in table props or defaults should be added to view properties")
        .isEqualTo("catalog-override-key4");
    assertThat(view.properties().get("key5"))
        .as(
            "Table properties without any catalog level default or override should be added to view"
                + " properties.")
        .isEqualTo("view-key5");
  }

  public static class CustomMetricsReporter implements MetricsReporter {
    static final AtomicInteger COUNTER = new AtomicInteger(0);

    @Override
    public void report(MetricsReport report) {
      COUNTER.incrementAndGet();
    }
  }

  private String createMetadataLocationViaJdbcCatalog(TableIdentifier identifier)
      throws SQLException {
    // temporary connection just to actually create a concrete metadata location
    String jdbcUrl;
    try {
      java.nio.file.Path dbFile = Files.createTempFile("temp", "metadata");
      jdbcUrl = "jdbc:sqlite:" + dbFile.toAbsolutePath();
    } catch (IOException e) {
      throw new SQLException("Error while creating temp data", e);
    }

    Map<String, String> properties = Maps.newHashMap();

    properties.put(CatalogProperties.URI, jdbcUrl);

    warehouseLocation = this.tableDir.toAbsolutePath().toString();
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);
    properties.put("type", "jdbc");

    JdbcCatalog jdbcCatalog =
        (JdbcCatalog) CatalogUtil.buildIcebergCatalog("TEMP", properties, conf);
    jdbcCatalog.buildTable(identifier, SCHEMA).create();

    SQLiteDataSource dataSource = new SQLiteDataSource();
    dataSource.setUrl(jdbcUrl);

    try (Connection connection = dataSource.getConnection()) {
      ResultSet result =
          connection
              .prepareStatement("SELECT * FROM " + JdbcUtil.CATALOG_TABLE_VIEW_NAME)
              .executeQuery();
      result.next();
      return result.getString(JdbcTableOperations.METADATA_LOCATION_PROP);
    }
  }

  private void initLegacySchema(String jdbcUrl) throws SQLException {
    TableIdentifier table1 = TableIdentifier.of(Namespace.of("namespace1"), "table1");
    TableIdentifier table2 = TableIdentifier.of(Namespace.of("namespace2"), "table2");

    String table1MetadataLocation = createMetadataLocationViaJdbcCatalog(table1);
    String table2MetadataLocation = createMetadataLocationViaJdbcCatalog(table2);

    SQLiteDataSource dataSource = new SQLiteDataSource();
    dataSource.setUrl(jdbcUrl);

    try (Connection connection = dataSource.getConnection()) {
      // create "old style" SQL schema
      connection.prepareStatement(JdbcUtil.V0_CREATE_CATALOG_SQL).executeUpdate();
      connection
          .prepareStatement(
              "INSERT INTO "
                  + JdbcUtil.CATALOG_TABLE_VIEW_NAME
                  + "("
                  + JdbcUtil.CATALOG_NAME
                  + ","
                  + JdbcUtil.TABLE_NAMESPACE
                  + ","
                  + JdbcUtil.TABLE_NAME
                  + ","
                  + JdbcTableOperations.METADATA_LOCATION_PROP
                  + ","
                  + JdbcTableOperations.PREVIOUS_METADATA_LOCATION_PROP
                  + ") VALUES('TEST','namespace1','table1','"
                  + table1MetadataLocation
                  + "',null)")
          .execute();
      connection
          .prepareStatement(
              "INSERT INTO "
                  + JdbcUtil.CATALOG_TABLE_VIEW_NAME
                  + "("
                  + JdbcUtil.CATALOG_NAME
                  + ","
                  + JdbcUtil.TABLE_NAMESPACE
                  + ","
                  + JdbcUtil.TABLE_NAME
                  + ","
                  + JdbcTableOperations.METADATA_LOCATION_PROP
                  + ","
                  + JdbcTableOperations.PREVIOUS_METADATA_LOCATION_PROP
                  + ") VALUES('TEST','namespace2','table2','"
                  + table2MetadataLocation
                  + "',null)")
          .execute();
    }
  }

  private boolean catalogTablesExist(String jdbcUrl) throws SQLException {
    SQLiteDataSource dataSource = new SQLiteDataSource();
    dataSource.setUrl(jdbcUrl);

    boolean catalogTableExists = false;
    boolean namespacePropertiesTableExists = false;

    try (Connection connection = dataSource.getConnection()) {
      DatabaseMetaData metadata = connection.getMetaData();
      if (tableExists(metadata, JdbcUtil.CATALOG_TABLE_VIEW_NAME)) {
        catalogTableExists = true;
      }
      if (tableExists(metadata, JdbcUtil.NAMESPACE_PROPERTIES_TABLE_NAME)) {
        namespacePropertiesTableExists = true;
      }
    }

    return catalogTableExists && namespacePropertiesTableExists;
  }

  private boolean tableExists(DatabaseMetaData metadata, String tableName) throws SQLException {
    ResultSet resultSet = metadata.getTables(null, null, tableName, new String[] {"TABLE"});

    while (resultSet.next()) {
      if (tableName.equals(resultSet.getString("TABLE_NAME"))) {
        return true;
      }
    }

    return false;
  }
}
