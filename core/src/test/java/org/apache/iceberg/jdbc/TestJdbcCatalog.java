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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
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
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
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
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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
    properties.putAll(props);

    JdbcCatalog jdbcCatalog = new JdbcCatalog();
    jdbcCatalog.setConf(conf);
    jdbcCatalog.initialize(catalogName, properties);
    return jdbcCatalog;
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

    Assertions.assertThatThrownBy(
            () -> catalog.createTable(testTable, SCHEMA, PartitionSpec.unpartitioned()))
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

    Assertions.assertThatThrownBy(() -> table.newAppend().appendFile(dataFile2).commit())
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

    Assertions.assertThatThrownBy(() -> catalog.listTables(testTable2.namespace()))
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

    Assertions.assertThatThrownBy(
            () -> catalog.renameTable(TableIdentifier.of("db", "tbl-not-exists"), to))
        .isInstanceOf(NoSuchTableException.class)
        .hasMessage("Table does not exist: db.tbl-not-exists");

    // rename table to existing table name!
    TableIdentifier from2 = TableIdentifier.of("db", "tbl2");
    catalog.createTable(from2, SCHEMA, PartitionSpec.unpartitioned());
    Assertions.assertThatThrownBy(() -> catalog.renameTable(from2, to))
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

    Assertions.assertThatThrownBy(() -> catalog.listTables(Namespace.of("db", "ns1", "ns2")))
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

    Lists.newArrayList(tbl1, tbl2, tbl3, tbl4, tbl5, tbl6)
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
    assertThat(tblSet2).hasSize(3).contains("db", "db2", "");

    List<Namespace> nsp4 = catalog.listNamespaces();
    Set<String> tblSet3 = Sets.newHashSet(nsp4.stream().map(Namespace::toString).iterator());
    assertThat(tblSet3).hasSize(3).contains("db", "db2", "");

    Assertions.assertThatThrownBy(() -> catalog.listNamespaces(Namespace.of("db", "db2", "ns2")))
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

    Assertions.assertThatThrownBy(
            () -> catalog.loadNamespaceMetadata(Namespace.of("db", "db2", "ns2")))
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

    Assertions.assertThatThrownBy(() -> catalog.dropNamespace(tbl1.namespace()))
        .isInstanceOf(NamespaceNotEmptyException.class)
        .hasMessage("Namespace db.ns1.ns2 is not empty. 2 tables exist.");

    Assertions.assertThatThrownBy(() -> catalog.dropNamespace(tbl2.namespace()))
        .isInstanceOf(NamespaceNotEmptyException.class)
        .hasMessage("Namespace db.ns1 is not empty. 1 tables exist.");

    Assertions.assertThatThrownBy(() -> catalog.dropNamespace(tbl4.namespace()))
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
      Assertions.assertThat(jdbcCatalog.namespaceExists(namespace)).isFalse();
      Assertions.assertThat(jdbcCatalog.tableExists(identifier)).isFalse();

      // default=non-strict mode allows creating a table in a non-existing namespace
      jdbcCatalog.createTable(identifier, SCHEMA, PARTITION_SPEC);
      Assertions.assertThat(jdbcCatalog.loadTable(identifier)).isNotNull();
    }
  }

  @Test
  public void testCreateTableInNonExistingNamespaceStrictMode() {
    try (JdbcCatalog jdbcCatalog =
        initCatalog(
            "strict_jdbc_catalog", ImmutableMap.of(JdbcUtil.STRICT_MODE_PROPERTY, "true"))) {
      Namespace namespace = Namespace.of("testDb", "ns1", "ns2");
      TableIdentifier identifier = TableIdentifier.of(namespace, "someTable");
      Assertions.assertThat(jdbcCatalog.namespaceExists(namespace)).isFalse();
      Assertions.assertThat(jdbcCatalog.tableExists(identifier)).isFalse();
      Assertions.assertThatThrownBy(
              () -> jdbcCatalog.createTable(identifier, SCHEMA, PARTITION_SPEC))
          .isInstanceOf(NoSuchNamespaceException.class)
          .hasMessage(
              "Cannot create table testDb.ns1.ns2.someTable in catalog strict_jdbc_catalog. Namespace testDb.ns1.ns2 does not exist");

      Assertions.assertThat(jdbcCatalog.tableExists(identifier)).isFalse();

      jdbcCatalog.createNamespace(namespace);
      Assertions.assertThat(jdbcCatalog.tableExists(identifier)).isFalse();
      jdbcCatalog.createTable(identifier, SCHEMA, PARTITION_SPEC);
      Assertions.assertThat(jdbcCatalog.loadTable(identifier)).isNotNull();
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

    Assertions.assertThat(catalog.loadNamespaceMetadata(testNamespace)).containsKey("location");
  }

  @Test
  public void testNamespaceCustomLocation() {
    Namespace testNamespace = Namespace.of("testDb", "ns1", "ns2");
    String namespaceLocation = "file:///tmp/warehouse/ns/path";

    // Test with location
    Map<String, String> testMetadata = ImmutableMap.of("location", namespaceLocation);
    catalog.createNamespace(testNamespace, testMetadata);

    Assertions.assertThat(catalog.loadNamespaceMetadata(testNamespace))
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
        Assertions.assertThat(tasks.iterator()).hasNext();
      }
    } finally {
      catalogWithCustomReporter.dropTable(TABLE);
    }
    // counter of custom metrics reporter should have been increased
    // 1x for commit metrics / 1x for scan metrics
    Assertions.assertThat(CustomMetricsReporter.COUNTER.get()).isEqualTo(2);
  }

  public static class CustomMetricsReporter implements MetricsReporter {
    static final AtomicInteger COUNTER = new AtomicInteger(0);

    @Override
    public void report(MetricsReport report) {
      COUNTER.incrementAndGet();
    }
  }
}
