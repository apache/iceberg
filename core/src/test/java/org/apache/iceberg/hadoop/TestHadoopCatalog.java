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
package org.apache.iceberg.hadoop;

import static org.apache.iceberg.NullOrder.NULLS_FIRST;
import static org.apache.iceberg.SortDirection.ASC;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

public class TestHadoopCatalog extends HadoopTableTestBase {
  private static ImmutableMap<String, String> meta = ImmutableMap.of();

  @Test
  public void testCreateTableBuilder() throws Exception {
    TableIdentifier tableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    Table table =
        hadoopCatalog()
            .buildTable(tableIdent, SCHEMA)
            .withPartitionSpec(SPEC)
            .withProperties(null)
            .withProperty("key1", "value1")
            .withProperties(ImmutableMap.of("key2", "value2"))
            .create();

    Assert.assertEquals(TABLE_SCHEMA.toString(), table.schema().toString());
    Assert.assertEquals(1, table.spec().fields().size());
    Assert.assertEquals("value1", table.properties().get("key1"));
    Assert.assertEquals("value2", table.properties().get("key2"));
  }

  @Test
  public void testCreateTableTxnBuilder() throws Exception {
    HadoopCatalog catalog = hadoopCatalog();
    TableIdentifier tableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    Transaction txn =
        catalog.buildTable(tableIdent, SCHEMA).withPartitionSpec(null).createTransaction();
    txn.commitTransaction();
    Table table = catalog.loadTable(tableIdent);

    Assert.assertEquals(TABLE_SCHEMA.toString(), table.schema().toString());
    Assert.assertTrue(table.spec().isUnpartitioned());
  }

  @Test
  public void testReplaceTxnBuilder() throws Exception {
    HadoopCatalog catalog = hadoopCatalog();
    TableIdentifier tableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl");

    Transaction createTxn =
        catalog
            .buildTable(tableIdent, SCHEMA)
            .withPartitionSpec(SPEC)
            .withProperty("key1", "value1")
            .createOrReplaceTransaction();

    createTxn.newAppend().appendFile(FILE_A).commit();

    createTxn.commitTransaction();

    Table table = catalog.loadTable(tableIdent);
    Assert.assertNotNull(table.currentSnapshot());

    Transaction replaceTxn =
        catalog.buildTable(tableIdent, SCHEMA).withProperty("key2", "value2").replaceTransaction();
    replaceTxn.commitTransaction();

    table = catalog.loadTable(tableIdent);
    Assert.assertNull(table.currentSnapshot());
    PartitionSpec v1Expected =
        PartitionSpec.builderFor(table.schema())
            .alwaysNull("data", "data_bucket")
            .withSpecId(1)
            .build();
    Assert.assertEquals("Table should have a spec with one void field", v1Expected, table.spec());

    Assert.assertEquals("value1", table.properties().get("key1"));
    Assert.assertEquals("value2", table.properties().get("key2"));
  }

  @Test
  public void testTableBuilderWithLocation() throws Exception {
    HadoopCatalog catalog = hadoopCatalog();
    TableIdentifier tableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl");

    AssertHelpers.assertThrows(
        "Should reject a custom location",
        IllegalArgumentException.class,
        "Cannot set a custom location for a path-based table",
        () -> catalog.buildTable(tableIdent, SCHEMA).withLocation("custom").create());

    AssertHelpers.assertThrows(
        "Should reject a custom location",
        IllegalArgumentException.class,
        "Cannot set a custom location for a path-based table",
        () -> catalog.buildTable(tableIdent, SCHEMA).withLocation("custom").createTransaction());

    AssertHelpers.assertThrows(
        "Should reject a custom location",
        IllegalArgumentException.class,
        "Cannot set a custom location for a path-based table",
        () ->
            catalog
                .buildTable(tableIdent, SCHEMA)
                .withLocation("custom")
                .createOrReplaceTransaction());
  }

  @Test
  public void testCreateTableDefaultSortOrder() throws Exception {
    TableIdentifier tableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    Table table = hadoopCatalog().createTable(tableIdent, SCHEMA, SPEC);

    SortOrder sortOrder = table.sortOrder();
    Assert.assertEquals("Order ID must match", 0, sortOrder.orderId());
    Assert.assertTrue("Order must unsorted", sortOrder.isUnsorted());
  }

  @Test
  public void testCreateTableCustomSortOrder() throws Exception {
    TableIdentifier tableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    SortOrder order = SortOrder.builderFor(SCHEMA).asc("id", NULLS_FIRST).build();
    Table table =
        hadoopCatalog()
            .buildTable(tableIdent, SCHEMA)
            .withPartitionSpec(SPEC)
            .withSortOrder(order)
            .create();

    SortOrder sortOrder = table.sortOrder();
    Assert.assertEquals("Order ID must match", 1, sortOrder.orderId());
    Assert.assertEquals("Order must have 1 field", 1, sortOrder.fields().size());
    Assert.assertEquals("Direction must match ", ASC, sortOrder.fields().get(0).direction());
    Assert.assertEquals(
        "Null order must match ", NULLS_FIRST, sortOrder.fields().get(0).nullOrder());
    Transform<?, ?> transform = Transforms.identity();
    Assert.assertEquals("Transform must match", transform, sortOrder.fields().get(0).transform());
  }

  @Test
  public void testBasicCatalog() throws Exception {
    HadoopCatalog catalog = hadoopCatalog();
    TableIdentifier testTable = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    catalog.createTable(testTable, SCHEMA, PartitionSpec.unpartitioned());
    String metaLocation = catalog.defaultWarehouseLocation(testTable);

    FileSystem fs = Util.getFs(new Path(metaLocation), catalog.getConf());
    Assert.assertTrue(fs.isDirectory(new Path(metaLocation)));

    catalog.dropTable(testTable);
    Assert.assertFalse(fs.isDirectory(new Path(metaLocation)));
  }

  @Test
  public void testCreateAndDropTableWithoutNamespace() throws Exception {
    HadoopCatalog catalog = hadoopCatalog();

    TableIdentifier testTable = TableIdentifier.of("tbl");
    Table table = catalog.createTable(testTable, SCHEMA, PartitionSpec.unpartitioned());

    Assert.assertEquals(table.schema().toString(), TABLE_SCHEMA.toString());
    Assert.assertEquals("hadoop.tbl", table.name());
    String metaLocation = catalog.defaultWarehouseLocation(testTable);

    FileSystem fs = Util.getFs(new Path(metaLocation), catalog.getConf());
    Assert.assertTrue(fs.isDirectory(new Path(metaLocation)));

    catalog.dropTable(testTable);
    Assert.assertFalse(fs.isDirectory(new Path(metaLocation)));
  }

  @Test
  public void testDropTable() throws Exception {
    HadoopCatalog catalog = hadoopCatalog();
    TableIdentifier testTable = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    catalog.createTable(testTable, SCHEMA, PartitionSpec.unpartitioned());
    String metaLocation = catalog.defaultWarehouseLocation(testTable);

    FileSystem fs = Util.getFs(new Path(metaLocation), catalog.getConf());
    Assert.assertTrue(fs.isDirectory(new Path(metaLocation)));

    catalog.dropTable(testTable);
    Assert.assertFalse(fs.isDirectory(new Path(metaLocation)));
  }

  @Test
  public void testDropNonIcebergTable() throws Exception {
    HadoopCatalog catalog = hadoopCatalog();
    TableIdentifier testTable = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    String metaLocation = catalog.defaultWarehouseLocation(testTable);
    // testing with non existent directory
    Assert.assertFalse(catalog.dropTable(testTable));

    FileSystem fs = Util.getFs(new Path(metaLocation), catalog.getConf());
    fs.mkdirs(new Path(metaLocation));
    Assert.assertTrue(fs.isDirectory(new Path(metaLocation)));

    Assert.assertFalse(catalog.dropTable(testTable));
    Assert.assertTrue(fs.isDirectory(new Path(metaLocation)));
  }

  @Test
  public void testRenameTable() throws Exception {
    HadoopCatalog catalog = hadoopCatalog();
    TableIdentifier testTable = TableIdentifier.of("db", "tbl1");
    catalog.createTable(testTable, SCHEMA, PartitionSpec.unpartitioned());
    AssertHelpers.assertThrows(
        "should throw exception",
        UnsupportedOperationException.class,
        "Cannot rename Hadoop tables",
        () -> {
          catalog.renameTable(testTable, TableIdentifier.of("db", "tbl2"));
        });
  }

  @Test
  public void testListTables() throws Exception {
    HadoopCatalog catalog = hadoopCatalog();

    TableIdentifier tbl1 = TableIdentifier.of("db", "tbl1");
    TableIdentifier tbl2 = TableIdentifier.of("db", "tbl2");
    TableIdentifier tbl3 = TableIdentifier.of("db", "ns1", "tbl3");
    TableIdentifier tbl4 = TableIdentifier.of("db", "metadata", "metadata");

    Lists.newArrayList(tbl1, tbl2, tbl3, tbl4)
        .forEach(t -> catalog.createTable(t, SCHEMA, PartitionSpec.unpartitioned()));

    List<TableIdentifier> tbls1 = catalog.listTables(Namespace.of("db"));
    Set<String> tblSet = Sets.newHashSet(tbls1.stream().map(t -> t.name()).iterator());
    Assert.assertEquals(2, tblSet.size());
    Assert.assertTrue(tblSet.contains("tbl1"));
    Assert.assertTrue(tblSet.contains("tbl2"));

    List<TableIdentifier> tbls2 = catalog.listTables(Namespace.of("db", "ns1"));
    Assert.assertEquals("table identifiers", 1, tbls2.size());
    Assert.assertEquals("table name", "tbl3", tbls2.get(0).name());

    AssertHelpers.assertThrows(
        "should throw exception",
        NoSuchNamespaceException.class,
        "Namespace does not exist: ",
        () -> {
          catalog.listTables(Namespace.of("db", "ns1", "ns2"));
        });
  }

  @Test
  public void testCallingLocationProviderWhenNoCurrentMetadata() throws IOException {
    HadoopCatalog catalog = hadoopCatalog();

    TableIdentifier tableIdent = TableIdentifier.of("ns1", "ns2", "table1");
    Transaction create = catalog.newCreateTableTransaction(tableIdent, SCHEMA);
    create.table().locationProvider(); // NPE triggered if not handled appropriately
    create.commitTransaction();

    Assert.assertEquals(
        "1 table expected", 1, catalog.listTables(Namespace.of("ns1", "ns2")).size());
    catalog.dropTable(tableIdent, true);
  }

  @Test
  public void testCreateNamespace() throws Exception {
    String warehouseLocation = temp.newFolder().getAbsolutePath();
    HadoopCatalog catalog = new HadoopCatalog();
    catalog.setConf(new Configuration());
    catalog.initialize(
        "hadoop", ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation));

    TableIdentifier tbl1 = TableIdentifier.of("db", "ns1", "ns2", "metadata");
    TableIdentifier tbl2 = TableIdentifier.of("db", "ns2", "ns3", "tbl2");

    Lists.newArrayList(tbl1, tbl2).forEach(t -> catalog.createNamespace(t.namespace(), meta));

    String metaLocation1 = warehouseLocation + "/" + "db/ns1/ns2";
    FileSystem fs1 = Util.getFs(new Path(metaLocation1), catalog.getConf());
    Assert.assertTrue(fs1.isDirectory(new Path(metaLocation1)));

    String metaLocation2 = warehouseLocation + "/" + "db/ns2/ns3";
    FileSystem fs2 = Util.getFs(new Path(metaLocation2), catalog.getConf());
    Assert.assertTrue(fs2.isDirectory(new Path(metaLocation2)));

    AssertHelpers.assertThrows(
        "Should fail to create when namespace already exist: " + tbl1.namespace(),
        org.apache.iceberg.exceptions.AlreadyExistsException.class,
        "Namespace already exists: " + tbl1.namespace(),
        () -> {
          catalog.createNamespace(tbl1.namespace());
        });
  }

  @Test
  public void testListNamespace() throws Exception {
    HadoopCatalog catalog = hadoopCatalog();

    TableIdentifier tbl1 = TableIdentifier.of("db", "ns1", "ns2", "metadata");
    TableIdentifier tbl2 = TableIdentifier.of("db", "ns2", "ns3", "tbl2");
    TableIdentifier tbl3 = TableIdentifier.of("db", "ns3", "tbl4");
    TableIdentifier tbl4 = TableIdentifier.of("db", "metadata");
    TableIdentifier tbl5 = TableIdentifier.of("db2", "metadata");

    Lists.newArrayList(tbl1, tbl2, tbl3, tbl4, tbl5)
        .forEach(t -> catalog.createTable(t, SCHEMA, PartitionSpec.unpartitioned()));

    List<Namespace> nsp1 = catalog.listNamespaces(Namespace.of("db"));
    Set<String> tblSet = Sets.newHashSet(nsp1.stream().map(t -> t.toString()).iterator());
    Assert.assertEquals(3, tblSet.size());
    Assert.assertTrue(tblSet.contains("db.ns1"));
    Assert.assertTrue(tblSet.contains("db.ns2"));
    Assert.assertTrue(tblSet.contains("db.ns3"));

    List<Namespace> nsp2 = catalog.listNamespaces(Namespace.of("db", "ns1"));
    Assert.assertEquals(1, nsp2.size());
    Assert.assertTrue(nsp2.get(0).toString().equals("db.ns1.ns2"));

    List<Namespace> nsp3 = catalog.listNamespaces();
    Set<String> tblSet2 = Sets.newHashSet(nsp3.stream().map(t -> t.toString()).iterator());
    Assert.assertEquals(2, tblSet2.size());
    Assert.assertTrue(tblSet2.contains("db"));
    Assert.assertTrue(tblSet2.contains("db2"));

    List<Namespace> nsp4 = catalog.listNamespaces();
    Set<String> tblSet3 = Sets.newHashSet(nsp4.stream().map(t -> t.toString()).iterator());
    Assert.assertEquals(2, tblSet3.size());
    Assert.assertTrue(tblSet3.contains("db"));
    Assert.assertTrue(tblSet3.contains("db2"));

    AssertHelpers.assertThrows(
        "Should fail to list namespace doesn't exist",
        NoSuchNamespaceException.class,
        "Namespace does not exist: ",
        () -> {
          catalog.listNamespaces(Namespace.of("db", "db2", "ns2"));
        });
  }

  @Test
  public void testLoadNamespaceMeta() throws IOException {
    HadoopCatalog catalog = hadoopCatalog();

    TableIdentifier tbl1 = TableIdentifier.of("db", "ns1", "ns2", "metadata");
    TableIdentifier tbl2 = TableIdentifier.of("db", "ns2", "ns3", "tbl2");
    TableIdentifier tbl3 = TableIdentifier.of("db", "ns3", "tbl4");
    TableIdentifier tbl4 = TableIdentifier.of("db", "metadata");

    Lists.newArrayList(tbl1, tbl2, tbl3, tbl4)
        .forEach(t -> catalog.createTable(t, SCHEMA, PartitionSpec.unpartitioned()));
    catalog.loadNamespaceMetadata(Namespace.of("db"));

    AssertHelpers.assertThrows(
        "Should fail to load namespace doesn't exist",
        NoSuchNamespaceException.class,
        "Namespace does not exist: ",
        () -> {
          catalog.loadNamespaceMetadata(Namespace.of("db", "db2", "ns2"));
        });
  }

  @Test
  public void testNamespaceExists() throws IOException {
    HadoopCatalog catalog = hadoopCatalog();

    TableIdentifier tbl1 = TableIdentifier.of("db", "ns1", "ns2", "metadata");
    TableIdentifier tbl2 = TableIdentifier.of("db", "ns2", "ns3", "tbl2");
    TableIdentifier tbl3 = TableIdentifier.of("db", "ns3", "tbl4");
    TableIdentifier tbl4 = TableIdentifier.of("db", "metadata");

    Lists.newArrayList(tbl1, tbl2, tbl3, tbl4)
        .forEach(t -> catalog.createTable(t, SCHEMA, PartitionSpec.unpartitioned()));
    Assert.assertTrue(
        "Should true to namespace exist",
        catalog.namespaceExists(Namespace.of("db", "ns1", "ns2")));
    Assert.assertTrue(
        "Should false to namespace doesn't exist",
        !catalog.namespaceExists(Namespace.of("db", "db2", "ns2")));
  }

  @Test
  public void testAlterNamespaceMeta() throws IOException {
    HadoopCatalog catalog = hadoopCatalog();
    AssertHelpers.assertThrows(
        "Should fail to change namespace",
        UnsupportedOperationException.class,
        "Cannot set namespace properties db.db2.ns2 : setProperties is not supported",
        () -> {
          catalog.setProperties(
              Namespace.of("db", "db2", "ns2"), ImmutableMap.of("property", "test"));
        });
  }

  @Test
  public void testDropNamespace() throws IOException {
    String warehouseLocation = temp.newFolder().getAbsolutePath();
    HadoopCatalog catalog = new HadoopCatalog();
    catalog.setConf(new Configuration());
    catalog.initialize(
        "hadoop", ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation));
    Namespace namespace1 = Namespace.of("db");
    Namespace namespace2 = Namespace.of("db", "ns1");

    TableIdentifier tbl1 = TableIdentifier.of(namespace1, "tbl1");
    TableIdentifier tbl2 = TableIdentifier.of(namespace2, "tbl1");

    Lists.newArrayList(tbl1, tbl2)
        .forEach(t -> catalog.createTable(t, SCHEMA, PartitionSpec.unpartitioned()));

    AssertHelpers.assertThrows(
        "Should fail to drop namespace is not empty " + namespace1,
        NamespaceNotEmptyException.class,
        "Namespace " + namespace1 + " is not empty.",
        () -> {
          catalog.dropNamespace(Namespace.of("db"));
        });
    Assert.assertFalse(
        "Should fail to drop namespace doesn't exist", catalog.dropNamespace(Namespace.of("db2")));
    Assert.assertTrue(catalog.dropTable(tbl1));
    Assert.assertTrue(catalog.dropTable(tbl2));
    Assert.assertTrue(catalog.dropNamespace(namespace2));
    Assert.assertTrue(catalog.dropNamespace(namespace1));
    String metaLocation = warehouseLocation + "/" + "db";
    FileSystem fs = Util.getFs(new Path(metaLocation), catalog.getConf());
    Assert.assertFalse(fs.isDirectory(new Path(metaLocation)));
  }

  @Test
  public void testVersionHintFileErrorWithFile() throws Exception {
    addVersionsToTable(table);

    HadoopTableOperations tableOperations =
        (HadoopTableOperations) TABLES.newTableOps(tableLocation);

    long secondSnapshotId = table.currentSnapshot().snapshotId();

    // Write old data to confirm that we are writing the correct file
    FileIO io = table.io();
    io.deleteFile(versionHintFile.getPath());
    try (PositionOutputStream stream = io.newOutputFile(versionHintFile.getPath()).create()) {
      stream.write("1".getBytes(StandardCharsets.UTF_8));
    }

    // Check the result of the findVersion(), and load the table and check the current snapshotId
    Assert.assertEquals(1, tableOperations.findVersion());
    Assert.assertEquals(
        secondSnapshotId, TABLES.load(tableLocation).currentSnapshot().snapshotId());

    // Write newer data to confirm that we are writing the correct file
    io.deleteFile(versionHintFile.getPath());
    try (PositionOutputStream stream = io.newOutputFile(versionHintFile.getPath()).create()) {
      stream.write("3".getBytes(StandardCharsets.UTF_8));
    }

    // Check the result of the findVersion(), and load the table and check the current snapshotId
    Assert.assertEquals(3, tableOperations.findVersion());
    Assert.assertEquals(
        secondSnapshotId, TABLES.load(tableLocation).currentSnapshot().snapshotId());

    // Write an empty version hint file
    io.deleteFile(versionHintFile.getPath());
    io.newOutputFile(versionHintFile.getPath()).create().close();

    // Check the result of the findVersion(), and load the table and check the current snapshotId
    Assert.assertEquals(3, tableOperations.findVersion());
    Assert.assertEquals(
        secondSnapshotId, TABLES.load(tableLocation).currentSnapshot().snapshotId());

    // Just delete the file
    io.deleteFile(versionHintFile.getPath());

    // Check the result of the versionHint(), and load the table and check the current snapshotId
    Assert.assertEquals(3, tableOperations.findVersion());
    Assert.assertEquals(
        secondSnapshotId, TABLES.load(tableLocation).currentSnapshot().snapshotId());
  }

  @Test
  public void testVersionHintFileMissingMetadata() throws Exception {
    addVersionsToTable(table);

    HadoopTableOperations tableOperations =
        (HadoopTableOperations) TABLES.newTableOps(tableLocation);

    long secondSnapshotId = table.currentSnapshot().snapshotId();

    // Write old data to confirm that we are writing the correct file
    FileIO io = table.io();
    io.deleteFile(versionHintFile.getPath());

    // Remove the first version file, and see if we can recover
    io.deleteFile(tableOperations.getMetadataFile(1).toString());

    // Check the result of the findVersion(), and load the table and check the current snapshotId
    Assert.assertEquals(3, tableOperations.findVersion());
    Assert.assertEquals(
        secondSnapshotId, TABLES.load(tableLocation).currentSnapshot().snapshotId());

    // Remove all the version files, and see if we can recover. Hint... not :)
    io.deleteFile(tableOperations.getMetadataFile(2).toString());
    io.deleteFile(tableOperations.getMetadataFile(3).toString());

    // Check that we got 0 findVersion, and a NoSuchTableException is thrown when trying to load the
    // table
    Assert.assertEquals(0, tableOperations.findVersion());
    AssertHelpers.assertThrows(
        "Should not be able to find the table",
        NoSuchTableException.class,
        "Table does not exist",
        () -> TABLES.load(tableLocation));
  }

  @Test
  public void testTableName() throws Exception {
    HadoopCatalog catalog = hadoopCatalog();
    TableIdentifier tableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    catalog.buildTable(tableIdent, SCHEMA).withPartitionSpec(SPEC).create();

    Table table = catalog.loadTable(tableIdent);
    Assert.assertEquals("Name must match", "hadoop.db.ns1.ns2.tbl", table.name());

    TableIdentifier snapshotsTableIdent =
        TableIdentifier.of("db", "ns1", "ns2", "tbl", "snapshots");
    Table snapshotsTable = catalog.loadTable(snapshotsTableIdent);
    Assert.assertEquals(
        "Name must match", "hadoop.db.ns1.ns2.tbl.snapshots", snapshotsTable.name());
  }

  private static void addVersionsToTable(Table table) {
    DataFile dataFile1 =
        DataFiles.builder(SPEC)
            .withPath("/a.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();

    DataFile dataFile2 =
        DataFiles.builder(SPEC)
            .withPath("/b.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();

    table.newAppend().appendFile(dataFile1).commit();
    table.newAppend().appendFile(dataFile2).commit();
  }

  @Test
  public void testTablePropsDefinedAtCatalogLevel() throws IOException {
    TableIdentifier tableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    ImmutableMap<String, String> catalogProps =
        ImmutableMap.of(
            "table-default.key1", "catalog-default-key1",
            "table-default.key2", "catalog-default-key2",
            "table-default.key3", "catalog-default-key3",
            "table-override.key3", "catalog-override-key3",
            "table-override.key4", "catalog-override-key4");

    Table table =
        hadoopCatalog(catalogProps)
            .buildTable(tableIdent, SCHEMA)
            .withPartitionSpec(SPEC)
            .withProperties(null)
            .withProperty("key2", "table-key2")
            .withProperty("key3", "table-key3")
            .withProperty("key5", "table-key5")
            .create();

    Assert.assertEquals(
        "Table defaults set for the catalog must be added to the table properties.",
        "catalog-default-key1",
        table.properties().get("key1"));
    Assert.assertEquals(
        "Table property must override table default properties set at catalog level.",
        "table-key2",
        table.properties().get("key2"));
    Assert.assertEquals(
        "Table property override set at catalog level must override table default"
            + " properties set at catalog level and table property specified.",
        "catalog-override-key3",
        table.properties().get("key3"));
    Assert.assertEquals(
        "Table override not in table props or defaults should be added to table properties",
        "catalog-override-key4",
        table.properties().get("key4"));
    Assert.assertEquals(
        "Table properties without any catalog level default or override should be added to table"
            + " properties.",
        "table-key5",
        table.properties().get("key5"));
  }

  @Test
  public void testRegisterTable() throws IOException {
    TableIdentifier identifier = TableIdentifier.of("a", "t1");
    TableIdentifier identifier2 = TableIdentifier.of("a", "t2");
    HadoopCatalog catalog = hadoopCatalog();
    catalog.createTable(identifier, SCHEMA);
    Table registeringTable = catalog.loadTable(identifier);
    TableOperations ops = ((HasTableOperations) registeringTable).operations();
    String metadataLocation = ((HadoopTableOperations) ops).current().metadataFileLocation();
    Assertions.assertThat(catalog.registerTable(identifier2, metadataLocation)).isNotNull();
    Assertions.assertThat(catalog.loadTable(identifier2)).isNotNull();
    Assertions.assertThat(catalog.dropTable(identifier)).isTrue();
    Assertions.assertThat(catalog.dropTable(identifier2)).isTrue();
  }

  @Test
  public void testRegisterExistingTable() throws IOException {
    TableIdentifier identifier = TableIdentifier.of("a", "t1");
    HadoopCatalog catalog = hadoopCatalog();
    catalog.createTable(identifier, SCHEMA);
    Table registeringTable = catalog.loadTable(identifier);
    TableOperations ops = ((HasTableOperations) registeringTable).operations();
    String metadataLocation = ((HadoopTableOperations) ops).current().metadataFileLocation();
    Assertions.assertThatThrownBy(() -> catalog.registerTable(identifier, metadataLocation))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessage("Table already exists: a.t1");
    Assertions.assertThat(catalog.dropTable(identifier)).isTrue();
  }
}
