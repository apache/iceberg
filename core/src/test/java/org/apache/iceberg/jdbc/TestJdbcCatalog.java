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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.NullOrder.NULLS_FIRST;
import static org.apache.iceberg.SortDirection.ASC;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestJdbcCatalog {

  static final Schema SCHEMA = new Schema(
      required(1, "id", Types.IntegerType.get(), "unique ID"),
      required(2, "data", Types.StringType.get())
  );
  static final PartitionSpec PARTITION_SPEC = PartitionSpec.builderFor(SCHEMA)
      .bucket("data", 16)
      .build();

  static Configuration conf = new Configuration();
  private static JdbcCatalog catalog;
  private static String warehouseLocation;
  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  File tableDir = null;

  protected List<String> metadataVersionFiles(String location) {
    return Stream.of(new File(location).listFiles())
        .filter(file -> !file.isDirectory())
        .map(File::getName)
        .filter(fileName -> fileName.endsWith("metadata.json"))
        .collect(Collectors.toList())
        ;
  }

  protected List<String> manifestFiles(String location) {
    return Stream.of(new File(location).listFiles())
        .filter(file -> !file.isDirectory())
        .map(File::getName)
        .filter(fileName -> fileName.endsWith(".avro"))
        .collect(Collectors.toList())
        ;
  }

  @Before
  public void setupTable() throws Exception {
    this.tableDir = temp.newFolder();
    tableDir.delete(); // created by table create
    Map<String, String> properties = new HashMap<>();
    properties.put(CatalogProperties.URI,
        "jdbc:sqlite:file::memory:?ic" + UUID.randomUUID().toString().replace("-", ""));

    properties.put(JdbcCatalog.PROPERTY_PREFIX + "username", "user");
    properties.put(JdbcCatalog.PROPERTY_PREFIX + "password", "password");
    warehouseLocation = this.tableDir.getAbsolutePath();
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);
    catalog = new JdbcCatalog();
    catalog.setConf(conf);
    catalog.initialize("test_jdbc_catalog", properties);
  }

  @Test
  public void testInitialize() {
    Map<String, String> properties = new HashMap<>();
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, this.tableDir.getAbsolutePath());
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
    Table table = catalog.buildTable(tableIdent, SCHEMA)
        .withPartitionSpec(PARTITION_SPEC)
        .withProperties(null)
        .withProperty("key1", "value1")
        .withProperties(ImmutableMap.of("key2", "value2"))
        .create();

    Assert.assertEquals(SCHEMA.toString(), table.schema().toString());
    Assert.assertEquals(1, table.spec().fields().size());
    Assert.assertEquals("value1", table.properties().get("key1"));
    Assert.assertEquals("value2", table.properties().get("key2"));
  }

  @Test
  public void testCreateTableTxnBuilder() {
    TableIdentifier tableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    Transaction txn = catalog.buildTable(tableIdent, SCHEMA)
        .withPartitionSpec(null)
        .withProperty("key1", "testval1")
        .createTransaction();
    txn.commitTransaction();
    Table table = catalog.loadTable(tableIdent);

    Assert.assertEquals(SCHEMA.toString(), table.schema().toString());
    Assert.assertTrue(table.spec().isUnpartitioned());
    Assert.assertEquals("testval1", table.properties().get("key1"));
  }

  @Test
  public void testReplaceTxnBuilder() {
    TableIdentifier tableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl");

    final DataFile fileA = DataFiles.builder(PARTITION_SPEC)
        .withPath("/path/to/data-a.parquet")
        .withFileSizeInBytes(0)
        .withPartitionPath("data_bucket=0") // easy way to set partition data for now
        .withRecordCount(2) // needs at least one record or else metrics will filter it out
        .build();

    Transaction createTxn = catalog.buildTable(tableIdent, SCHEMA)
        .withPartitionSpec(PARTITION_SPEC)
        .withProperty("key1", "value1")
        .createOrReplaceTransaction();

    createTxn.newAppend()
        .appendFile(fileA)
        .commit();

    createTxn.commitTransaction();

    Table table = catalog.loadTable(tableIdent);
    Assert.assertNotNull(table.currentSnapshot());

    Transaction replaceTxn = catalog.buildTable(tableIdent, SCHEMA)
        .withProperty("key2", "value2")
        .replaceTransaction();
    replaceTxn.commitTransaction();

    table = catalog.loadTable(tableIdent);
    Assert.assertNull(table.currentSnapshot());
    PartitionSpec v1Expected = PartitionSpec.builderFor(table.schema())
        .alwaysNull("data", "data_bucket")
        .withSpecId(1)
        .build();
    Assert.assertEquals("Table should have a spec with one void field",
        v1Expected, table.spec());

    Assert.assertEquals("value1", table.properties().get("key1"));
    Assert.assertEquals("value2", table.properties().get("key2"));
  }

  @Test
  public void testCreateTableDefaultSortOrder() {
    TableIdentifier tableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    Table table = catalog.createTable(tableIdent, SCHEMA, PARTITION_SPEC);

    SortOrder sortOrder = table.sortOrder();
    Assert.assertEquals("Order ID must match", 0, sortOrder.orderId());
    Assert.assertTrue("Order must unsorted", sortOrder.isUnsorted());
  }

  @Test
  public void testCreateTableCustomSortOrder() {
    TableIdentifier tableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    SortOrder order = SortOrder.builderFor(SCHEMA)
        .asc("id", NULLS_FIRST)
        .build();
    Table table = catalog.buildTable(tableIdent, SCHEMA)
        .withPartitionSpec(PARTITION_SPEC)
        .withSortOrder(order)
        .create();

    SortOrder sortOrder = table.sortOrder();
    Assert.assertEquals("Order ID must match", 1, sortOrder.orderId());
    Assert.assertEquals("Order must have 1 field", 1, sortOrder.fields().size());
    Assert.assertEquals("Direction must match ", ASC, sortOrder.fields().get(0).direction());
    Assert.assertEquals("Null order must match ", NULLS_FIRST, sortOrder.fields().get(0).nullOrder());
    Transform<?, ?> transform = Transforms.identity(Types.IntegerType.get());
    Assert.assertEquals("Transform must match", transform, sortOrder.fields().get(0).transform());
  }

  @Test
  public void testBasicCatalog() throws Exception {
    TableIdentifier testTable = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    catalog.createTable(testTable, SCHEMA, PartitionSpec.unpartitioned());
    String metaLocation = catalog.defaultWarehouseLocation(testTable);

    FileSystem fs = Util.getFs(new Path(metaLocation), conf);
    Assert.assertTrue(fs.isDirectory(new Path(metaLocation)));

    AssertHelpers.assertThrows("should throw exception", AlreadyExistsException.class,
        "already exists", () ->
            catalog.createTable(testTable, SCHEMA, PartitionSpec.unpartitioned())
    );

    catalog.dropTable(testTable);
  }

  @Test
  public void testCreateAndDropTableWithoutNamespace() throws Exception {
    TableIdentifier testTable = TableIdentifier.of("tbl");
    Table table = catalog.createTable(testTable, SCHEMA, PartitionSpec.unpartitioned());

    Assert.assertEquals(table.schema().toString(), SCHEMA.toString());
    Assert.assertEquals(catalog.name() + ".tbl", table.name());
    String metaLocation = catalog.defaultWarehouseLocation(testTable);

    FileSystem fs = Util.getFs(new Path(metaLocation), conf);
    Assert.assertTrue(fs.isDirectory(new Path(metaLocation)));

    catalog.dropTable(testTable, true);
  }

  @Test
  public void testDefaultWarehouseLocation() throws Exception {
    TableIdentifier testTable = TableIdentifier.of("tbl");
    TableIdentifier testTable2 = TableIdentifier.of(Namespace.of("ns"), "tbl");
    Assert.assertEquals(catalog.defaultWarehouseLocation(testTable),
        warehouseLocation + "/" + testTable.name());
    Assert.assertEquals(catalog.defaultWarehouseLocation(testTable2),
        warehouseLocation + "/" + testTable2.namespace() + "/" + testTable2.name());
  }

  @Test
  public void testConcurrentCommit() throws IOException {
    TableIdentifier tableIdentifier = TableIdentifier.of("db", "table");
    Table table = catalog.createTable(tableIdentifier, SCHEMA, PartitionSpec.unpartitioned());
    // append file and commit!
    String data = temp.newFile("data.parquet").getPath();
    Files.write(Paths.get(data), new ArrayList<>(), StandardCharsets.UTF_8);
    DataFile dataFile = DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(data)
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .build();
    table.newAppend().appendFile(dataFile).commit();
    Assert.assertEquals(1, table.history().size());
    catalog.dropTable(tableIdentifier);
    data = temp.newFile("data2.parquet").getPath();
    Files.write(Paths.get(data), new ArrayList<>(), StandardCharsets.UTF_8);
    DataFile dataFile2 = DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(data)
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .build();

    AssertHelpers.assertThrows("Should fail", NoSuchTableException.class,
        "Failed to load table", () -> table.newAppend().appendFile(dataFile2).commit()
    );
  }

  @Test
  public void testCommitHistory() throws IOException {
    TableIdentifier testTable = TableIdentifier.of("db", "ns", "tbl");
    catalog.createTable(testTable, SCHEMA, PartitionSpec.unpartitioned());
    Table table = catalog.loadTable(testTable);

    String data = temp.newFile("data.parquet").getPath();
    Files.write(Paths.get(data), new ArrayList<>(), StandardCharsets.UTF_8);
    DataFile dataFile = DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(data)
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .build();
    table.newAppend().appendFile(dataFile).commit();
    Assert.assertEquals(1, table.history().size());

    data = temp.newFile("data2.parquet").getPath();
    Files.write(Paths.get(data), new ArrayList<>(), StandardCharsets.UTF_8);
    dataFile = DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(data)
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .build();
    table.newAppend().appendFile(dataFile).commit();
    Assert.assertEquals(2, table.history().size());

    data = temp.newFile("data3.parquet").getPath();
    Files.write(Paths.get(data), new ArrayList<>(), StandardCharsets.UTF_8);
    dataFile = DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(data)
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .build();
    table.newAppend().appendFile(dataFile).commit();
    Assert.assertEquals(3, table.history().size());
  }

  @Test
  public void testDropTable() {
    TableIdentifier testTable = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    TableIdentifier testTable2 = TableIdentifier.of("db", "ns1", "ns2", "tbl2");
    catalog.createTable(testTable, SCHEMA, PartitionSpec.unpartitioned());
    catalog.createTable(testTable2, SCHEMA, PartitionSpec.unpartitioned());
    catalog.dropTable(testTable);
    Assert.assertFalse(catalog.listTables(testTable.namespace()).contains(testTable));
    catalog.dropTable(testTable2);
    AssertHelpers.assertThrows("should throw exception", NoSuchNamespaceException.class,
        "not exist", () -> catalog.listTables(testTable2.namespace())
    );

    Assert.assertFalse(catalog.dropTable(TableIdentifier.of("db", "tbl-not-exists")));
  }

  @Test
  public void testRenameTable() {
    TableIdentifier from = TableIdentifier.of("db", "tbl1");
    TableIdentifier to = TableIdentifier.of("db", "tbl2-newtable");
    catalog.createTable(from, SCHEMA, PartitionSpec.unpartitioned());
    catalog.renameTable(from, to);
    Assert.assertTrue(catalog.listTables(to.namespace()).contains(to));
    Assert.assertFalse(catalog.listTables(to.namespace()).contains(from));
    Assert.assertTrue(catalog.loadTable(to).name().endsWith(to.name()));

    AssertHelpers.assertThrows("should throw exception", NoSuchTableException.class,
        "Table does not exist", () ->
            catalog.renameTable(TableIdentifier.of("db", "tbl-not-exists"), to)
    );

    // rename table to existing table name!
    TableIdentifier from2 = TableIdentifier.of("db", "tbl2");
    catalog.createTable(from2, SCHEMA, PartitionSpec.unpartitioned());
    AssertHelpers.assertThrows("should throw exception", UncheckedSQLException.class,
        "Failed to rename db.tbl2 to db.tbl2-newtable", () -> catalog.renameTable(from2, to)
    );
  }

  @Test
  public void testListTables() {
    TableIdentifier tbl1 = TableIdentifier.of("db", "tbl1");
    TableIdentifier tbl2 = TableIdentifier.of("db", "tbl2");
    TableIdentifier tbl3 = TableIdentifier.of("db", "tbl2", "subtbl2");
    TableIdentifier tbl4 = TableIdentifier.of("db", "ns1", "tbl3");
    TableIdentifier tbl5 = TableIdentifier.of("db", "metadata", "metadata");

    Lists.newArrayList(tbl1, tbl2, tbl3, tbl4, tbl5).forEach(t ->
        catalog.createTable(t, SCHEMA, PartitionSpec.unpartitioned())
    );

    List<TableIdentifier> tbls1 = catalog.listTables(Namespace.of("db"));
    Set<String> tblSet = Sets.newHashSet(tbls1.stream().map(TableIdentifier::name).iterator());
    Assert.assertEquals(tblSet.size(), 2);
    Assert.assertTrue(tblSet.contains("tbl1"));
    Assert.assertTrue(tblSet.contains("tbl2"));

    List<TableIdentifier> tbls2 = catalog.listTables(Namespace.of("db", "ns1"));
    Assert.assertEquals(tbls2.size(), 1);
    Assert.assertEquals("tbl3", tbls2.get(0).name());

    AssertHelpers.assertThrows("should throw exception", NoSuchNamespaceException.class,
        "does not exist", () -> catalog.listTables(Namespace.of("db", "ns1", "ns2")));
  }

  @Test
  public void testCallingLocationProviderWhenNoCurrentMetadata() {
    TableIdentifier tableIdent = TableIdentifier.of("ns1", "ns2", "table1");
    Transaction create = catalog.newCreateTableTransaction(tableIdent, SCHEMA);
    create.table().locationProvider();  // NPE triggered if not handled appropriately
    create.commitTransaction();

    Assert.assertEquals("1 table expected", 1, catalog.listTables(Namespace.of("ns1", "ns2")).size());
    catalog.dropTable(tableIdent, true);
  }

  @Test
  public void testExistingTableUpdate() {
    TableIdentifier tableIdent = TableIdentifier.of("ns1", "ns2", "table1");
    Transaction create = catalog.newCreateTableTransaction(tableIdent, SCHEMA);
    create.table().locationProvider();  // NPE triggered if not handled appropriately
    create.commitTransaction();
    Table icebergTable = catalog.loadTable(tableIdent);
    // add a column
    icebergTable.updateSchema().addColumn("Coll3", Types.LongType.get()).commit();
    icebergTable = catalog.loadTable(tableIdent);
    // Only 2 snapshotFile Should exist and no manifests should exist
    Assert.assertEquals(2, metadataVersionFiles(icebergTable.location() + "/metadata/").size());
    Assert.assertEquals(0, manifestFiles(icebergTable.location() + "/metadata/").size());
    Assert.assertNotEquals(SCHEMA.asStruct(), icebergTable.schema().asStruct());
    Assert.assertTrue(icebergTable.schema().asStruct().toString().contains("Coll3"));
  }

  @Test
  public void testTableName() {
    TableIdentifier tableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    catalog.buildTable(tableIdent, SCHEMA)
        .withPartitionSpec(PARTITION_SPEC)
        .create();
    Table table = catalog.loadTable(tableIdent);
    Assert.assertEquals("Name must match", catalog.name() + ".db.ns1.ns2.tbl", table.name());

    TableIdentifier snapshotsTableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl", "snapshots");
    Table snapshotsTable = catalog.loadTable(snapshotsTableIdent);
    Assert.assertEquals(
        "Name must match", catalog.name() + ".db.ns1.ns2.tbl.snapshots", snapshotsTable.name());
  }

  @Test
  public void testListNamespace() {
    TableIdentifier tbl1 = TableIdentifier.of("db", "ns1", "ns2", "metadata");
    TableIdentifier tbl2 = TableIdentifier.of("db", "ns2", "ns3", "tbl2");
    TableIdentifier tbl3 = TableIdentifier.of("db", "ns3", "tbl4");
    TableIdentifier tbl4 = TableIdentifier.of("db", "metadata");
    TableIdentifier tbl5 = TableIdentifier.of("db2", "metadata");
    TableIdentifier tbl6 = TableIdentifier.of("tbl6");

    Lists.newArrayList(tbl1, tbl2, tbl3, tbl4, tbl5, tbl6).forEach(t ->
        catalog.createTable(t, SCHEMA, PartitionSpec.unpartitioned())
    );

    List<Namespace> nsp1 = catalog.listNamespaces(Namespace.of("db"));
    Assert.assertEquals(nsp1.size(), 3);
    Set<String> tblSet = Sets.newHashSet(nsp1.stream().map(Namespace::toString).iterator());
    Assert.assertEquals(tblSet.size(), 3);
    Assert.assertTrue(tblSet.contains("db.ns1"));
    Assert.assertTrue(tblSet.contains("db.ns2"));
    Assert.assertTrue(tblSet.contains("db.ns3"));

    List<Namespace> nsp2 = catalog.listNamespaces(Namespace.of("db", "ns1"));
    Assert.assertEquals(nsp2.size(), 1);
    Assert.assertEquals("db.ns1.ns2", nsp2.get(0).toString());

    List<Namespace> nsp3 = catalog.listNamespaces();
    Set<String> tblSet2 = Sets.newHashSet(nsp3.stream().map(Namespace::toString).iterator());
    System.out.println(tblSet2.toString());
    Assert.assertEquals(tblSet2.size(), 3);
    Assert.assertTrue(tblSet2.contains("db"));
    Assert.assertTrue(tblSet2.contains("db2"));
    Assert.assertTrue(tblSet2.contains(""));

    List<Namespace> nsp4 = catalog.listNamespaces();
    Set<String> tblSet3 = Sets.newHashSet(nsp4.stream().map(Namespace::toString).iterator());
    Assert.assertEquals(tblSet3.size(), 3);
    Assert.assertTrue(tblSet3.contains("db"));
    Assert.assertTrue(tblSet3.contains("db2"));
    Assert.assertTrue(tblSet3.contains(""));

    AssertHelpers.assertThrows("Should fail to list namespace doesn't exist", NoSuchNamespaceException.class,
        "Namespace does not exist", () -> catalog.listNamespaces(Namespace.of("db", "db2", "ns2")
        ));
  }

  @Test
  public void testLoadNamespaceMeta() {
    TableIdentifier tbl1 = TableIdentifier.of("db", "ns1", "ns2", "metadata");
    TableIdentifier tbl2 = TableIdentifier.of("db", "ns2", "ns3", "tbl2");
    TableIdentifier tbl3 = TableIdentifier.of("db", "ns3", "tbl4");
    TableIdentifier tbl4 = TableIdentifier.of("db", "metadata");

    Lists.newArrayList(tbl1, tbl2, tbl3, tbl4).forEach(t ->
        catalog.createTable(t, SCHEMA, PartitionSpec.unpartitioned())
    );

    Assert.assertTrue(catalog.loadNamespaceMetadata(Namespace.of("db")).containsKey("location"));

    AssertHelpers.assertThrows("Should fail to load namespace doesn't exist",
        NoSuchNamespaceException.class, "Namespace does not exist", () ->
            catalog.loadNamespaceMetadata(Namespace.of("db", "db2", "ns2")));
  }

  @Test
  public void testNamespaceExists() {
    TableIdentifier tbl1 = TableIdentifier.of("db", "ns1", "ns2", "metadata");
    TableIdentifier tbl2 = TableIdentifier.of("db", "ns2", "ns3", "tbl2");
    TableIdentifier tbl3 = TableIdentifier.of("db", "ns3", "tbl4");
    TableIdentifier tbl4 = TableIdentifier.of("db", "metadata");

    Lists.newArrayList(tbl1, tbl2, tbl3, tbl4).forEach(t ->
        catalog.createTable(t, SCHEMA, PartitionSpec.unpartitioned())
    );
    Assert.assertTrue("Should true to namespace exist",
        catalog.namespaceExists(Namespace.of("db", "ns1", "ns2")));
    Assert.assertFalse("Should false to namespace doesn't exist",
        catalog.namespaceExists(Namespace.of("db", "db2", "not_exist")));
  }


  @Test
  public void testDropNamespace() {

    AssertHelpers.assertThrows("Should fail to drop namespace doesn't exist", NoSuchNamespaceException.class,
        "Namespace does not exist", () -> catalog.dropNamespace(Namespace.of("db", "ns1_not_exitss")));

    TableIdentifier tbl0 = TableIdentifier.of("db", "ns1", "ns2", "tbl2");
    TableIdentifier tbl1 = TableIdentifier.of("db", "ns1", "ns2", "tbl1");
    TableIdentifier tbl2 = TableIdentifier.of("db", "ns1", "tbl2");
    TableIdentifier tbl3 = TableIdentifier.of("db", "ns3", "tbl4");
    TableIdentifier tbl4 = TableIdentifier.of("db", "tbl");

    Lists.newArrayList(tbl0, tbl1, tbl2, tbl3, tbl4).forEach(t ->
        catalog.createTable(t, SCHEMA, PartitionSpec.unpartitioned())
    );

    AssertHelpers.assertThrows("Should fail to drop namespace has tables", NamespaceNotEmptyException.class,
        "is not empty. 2 tables exist.", () -> catalog.dropNamespace(tbl1.namespace()));
    AssertHelpers.assertThrows("Should fail to drop namespace has tables", NamespaceNotEmptyException.class,
        "is not empty. 1 tables exist.", () -> catalog.dropNamespace(tbl2.namespace()));
    AssertHelpers.assertThrows("Should fail to drop namespace has tables", NamespaceNotEmptyException.class,
        "is not empty. 1 tables exist.", () -> catalog.dropNamespace(tbl4.namespace()));
  }

  @Test
  public void testConversions() {
    Namespace ns = Namespace.of("db", "db2", "ns2");
    String nsString = JdbcUtil.namespaceToString(ns);
    Assert.assertEquals(ns, JdbcUtil.stringToNamespace(nsString));
  }

}
