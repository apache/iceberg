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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestHadoopCatalog extends HadoopTableTestBase {
  private static final ImmutableMap<String, String> meta = ImmutableMap.of();

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  public void testCreateTableBuilder(int formatVersion) throws Exception {
    TableIdentifier tableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    Table table =
        hadoopCatalog()
            .buildTable(tableIdent, SCHEMA)
            .withPartitionSpec(SPEC)
            .withProperties(null)
            .withProperty("key1", "value1")
            .withProperty(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion))
            .withProperties(ImmutableMap.of("key2", "value2"))
            .create();

    assertThat(table.schema().toString()).isEqualTo(TABLE_SCHEMA.toString());
    assertThat(table.spec().fields()).hasSize(1);
    assertThat(table.properties()).containsEntry("key1", "value1").containsEntry("key2", "value2");
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  public void testCreateTableTxnBuilder(int formatVersion) throws Exception {
    HadoopCatalog catalog = hadoopCatalog();
    TableIdentifier tableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    Transaction txn =
        catalog
            .buildTable(tableIdent, SCHEMA)
            .withPartitionSpec(null)
            .withProperty(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion))
            .createTransaction();
    txn.commitTransaction();
    Table table = catalog.loadTable(tableIdent);

    assertThat(table.schema().toString()).isEqualTo(TABLE_SCHEMA.toString());
    assertThat(table.spec().isUnpartitioned()).isTrue();
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  public void testReplaceTxnBuilder(int formatVersion) throws Exception {
    HadoopCatalog catalog = hadoopCatalog();
    TableIdentifier tableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl");

    Transaction createTxn =
        catalog
            .buildTable(tableIdent, SCHEMA)
            .withPartitionSpec(SPEC)
            .withProperty("key1", "value1")
            .withProperty(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion))
            .createOrReplaceTransaction();

    createTxn.newAppend().appendFile(FILE_A).commit();

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
      assertThat(table.spec().isUnpartitioned()).as("Table spec should be unpartitioned").isTrue();
    }

    assertThat(table.properties()).containsEntry("key1", "value1").containsEntry("key2", "value2");
  }

  @Test
  public void testTableBuilderWithLocation() throws Exception {
    HadoopCatalog catalog = hadoopCatalog();
    TableIdentifier tableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl");

    assertThatThrownBy(() -> catalog.buildTable(tableIdent, SCHEMA).withLocation("custom").create())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot set a custom location for a path-based table");

    assertThatThrownBy(
            () -> catalog.buildTable(tableIdent, SCHEMA).withLocation("custom").createTransaction())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot set a custom location for a path-based table");

    assertThatThrownBy(
            () ->
                catalog
                    .buildTable(tableIdent, SCHEMA)
                    .withLocation("custom")
                    .createOrReplaceTransaction())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot set a custom location for a path-based table");
  }

  @Test
  public void testCreateTableDefaultSortOrder() throws Exception {
    TableIdentifier tableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    Table table = hadoopCatalog().createTable(tableIdent, SCHEMA, SPEC);

    SortOrder sortOrder = table.sortOrder();
    assertThat(sortOrder.orderId()).as("Order ID must match").isEqualTo(0);
    assertThat(sortOrder.isUnsorted()).as("Order must be unsorted").isTrue();
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
    assertThat(sortOrder.orderId()).as("Order ID must match").isEqualTo(1);
    assertThat(sortOrder.fields().size()).as("Order must have 1 field").isEqualTo(1);
    assertThat(sortOrder.fields().get(0).direction()).as("Direction must match").isEqualTo(ASC);
    assertThat(sortOrder.fields().get(0).nullOrder())
        .as("Null order must match")
        .isEqualTo(NULLS_FIRST);
    Transform<?, ?> transform = Transforms.identity();
    assertThat(sortOrder.fields().get(0).transform())
        .as("Transform must match")
        .isEqualTo(transform);
  }

  @Test
  public void testBasicCatalog() throws Exception {
    HadoopCatalog catalog = hadoopCatalog();
    TableIdentifier testTable = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    catalog.createTable(testTable, SCHEMA, PartitionSpec.unpartitioned());
    String metaLocation = catalog.defaultWarehouseLocation(testTable);

    FileSystem fs = Util.getFs(new Path(metaLocation), catalog.getConf());
    assertThat(fs.isDirectory(new Path(metaLocation))).isTrue();

    catalog.dropTable(testTable);
    assertThat(fs.isDirectory(new Path(metaLocation))).isFalse();
  }

  @Test
  public void testHadoopFileIOProperties() {
    TableIdentifier tableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    ImmutableMap<String, String> catalogProps =
        ImmutableMap.of(
            "warehouse", "/hive/testwarehouse",
            "io.manifest.cache-enabled", "true");

    HadoopCatalog catalog = new HadoopCatalog();
    catalog.setConf(new Configuration());
    catalog.initialize("hadoop", catalogProps);
    FileIO fileIO = catalog.newTableOps(tableIdent).io();

    assertThat(fileIO.properties()).containsEntry("warehouse", "/hive/testwarehouse");
    assertThat(fileIO.properties()).containsEntry("io.manifest.cache-enabled", "true");
  }

  @Test
  public void testCreateAndDropTableWithoutNamespace() throws Exception {
    HadoopCatalog catalog = hadoopCatalog();

    TableIdentifier testTable = TableIdentifier.of("tbl");
    Table table = catalog.createTable(testTable, SCHEMA, PartitionSpec.unpartitioned());

    assertThat(table.schema().toString()).isEqualTo(TABLE_SCHEMA.toString());
    assertThat(table.name()).isEqualTo("hadoop.tbl");
    String metaLocation = catalog.defaultWarehouseLocation(testTable);

    FileSystem fs = Util.getFs(new Path(metaLocation), catalog.getConf());
    assertThat(fs.isDirectory(new Path(metaLocation))).isTrue();

    catalog.dropTable(testTable);
    assertThat(fs.isDirectory(new Path(metaLocation))).isFalse();
  }

  @Test
  public void testDropTable() throws Exception {
    HadoopCatalog catalog = hadoopCatalog();
    TableIdentifier testTable = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    catalog.createTable(testTable, SCHEMA, PartitionSpec.unpartitioned());
    String metaLocation = catalog.defaultWarehouseLocation(testTable);

    FileSystem fs = Util.getFs(new Path(metaLocation), catalog.getConf());
    assertThat(fs.isDirectory(new Path(metaLocation))).isTrue();

    catalog.dropTable(testTable);
    assertThat(fs.isDirectory(new Path(metaLocation))).isFalse();
  }

  @Test
  public void testDropNonIcebergTable() throws Exception {
    HadoopCatalog catalog = hadoopCatalog();
    TableIdentifier testTable = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    String metaLocation = catalog.defaultWarehouseLocation(testTable);
    // testing with non existent directory
    assertThat(catalog.dropTable(testTable)).isFalse();

    FileSystem fs = Util.getFs(new Path(metaLocation), catalog.getConf());
    fs.mkdirs(new Path(metaLocation));
    assertThat(fs.isDirectory(new Path(metaLocation))).isTrue();

    assertThat(catalog.dropTable(testTable)).isFalse();
    assertThat(fs.isDirectory(new Path(metaLocation))).isTrue();
  }

  @Test
  public void testRenameTable() throws Exception {
    HadoopCatalog catalog = hadoopCatalog();
    TableIdentifier testTable = TableIdentifier.of("db", "tbl1");
    catalog.createTable(testTable, SCHEMA, PartitionSpec.unpartitioned());
    assertThatThrownBy(() -> catalog.renameTable(testTable, TableIdentifier.of("db", "tbl2")))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot rename Hadoop tables");
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
    assertThat(tblSet).hasSize(2).contains("tbl1").contains("tbl2");

    List<TableIdentifier> tbls2 = catalog.listTables(Namespace.of("db", "ns1"));
    assertThat(tbls2).hasSize(1);
    assertThat(tbls2.get(0).name()).isEqualTo("tbl3");

    assertThatThrownBy(() -> catalog.listTables(Namespace.of("db", "ns1", "ns2")))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessage("Namespace does not exist: db.ns1.ns2");
  }

  @Test
  public void testCallingLocationProviderWhenNoCurrentMetadata() throws IOException {
    HadoopCatalog catalog = hadoopCatalog();

    TableIdentifier tableIdent = TableIdentifier.of("ns1", "ns2", "table1");
    Transaction create = catalog.newCreateTableTransaction(tableIdent, SCHEMA);
    create.table().locationProvider(); // NPE triggered if not handled appropriately
    create.commitTransaction();

    assertThat(catalog.listTables(Namespace.of("ns1", "ns2"))).as("1 table expected").hasSize(1);
    catalog.dropTable(tableIdent, true);
  }

  @Test
  public void testCreateNamespace() throws Exception {
    String warehouseLocation = tableDir.getAbsolutePath();
    HadoopCatalog catalog = new HadoopCatalog();
    catalog.setConf(new Configuration());
    catalog.initialize(
        "hadoop", ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation));

    TableIdentifier tbl1 = TableIdentifier.of("db", "ns1", "ns2", "metadata");
    TableIdentifier tbl2 = TableIdentifier.of("db", "ns2", "ns3", "tbl2");

    Lists.newArrayList(tbl1, tbl2).forEach(t -> catalog.createNamespace(t.namespace(), meta));

    String metaLocation1 = warehouseLocation + "/" + "db/ns1/ns2";
    FileSystem fs1 = Util.getFs(new Path(metaLocation1), catalog.getConf());
    assertThat(fs1.isDirectory(new Path(metaLocation1))).isTrue();

    String metaLocation2 = warehouseLocation + "/" + "db/ns2/ns3";
    FileSystem fs2 = Util.getFs(new Path(metaLocation2), catalog.getConf());
    assertThat(fs2.isDirectory(new Path(metaLocation2))).isTrue();

    assertThatThrownBy(() -> catalog.createNamespace(tbl1.namespace()))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessage("Namespace already exists: " + tbl1.namespace());
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
    assertThat(tblSet).hasSize(3).contains("db.ns1").contains("db.ns2").contains("db.ns3");

    List<Namespace> nsp2 = catalog.listNamespaces(Namespace.of("db", "ns1"));
    assertThat(nsp2).hasSize(1);
    assertThat(nsp2.get(0).toString()).isEqualTo("db.ns1.ns2");

    List<Namespace> nsp3 = catalog.listNamespaces();
    Set<String> tblSet2 = Sets.newHashSet(nsp3.stream().map(t -> t.toString()).iterator());
    assertThat(tblSet2).hasSize(2).contains("db").contains("db2");

    List<Namespace> nsp4 = catalog.listNamespaces();
    Set<String> tblSet3 = Sets.newHashSet(nsp4.stream().map(t -> t.toString()).iterator());
    assertThat(tblSet3).hasSize(2).contains("db").contains("db2");

    assertThatThrownBy(() -> catalog.listNamespaces(Namespace.of("db", "db2", "ns2")))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessage("Namespace does not exist: db.db2.ns2");
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

    assertThatThrownBy(() -> catalog.loadNamespaceMetadata(Namespace.of("db", "db2", "ns2")))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessage("Namespace does not exist: db.db2.ns2");
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
    assertThat(catalog.namespaceExists(Namespace.of("db", "ns1", "ns2")))
        .as("Should be true as namespace exists")
        .isTrue();
    assertThat(catalog.namespaceExists(Namespace.of("db", "db2", "ns2")))
        .as("Should be false as namespace doesn't exist")
        .isFalse();
  }

  @Test
  public void testAlterNamespaceMeta() throws IOException {
    HadoopCatalog catalog = hadoopCatalog();
    assertThatThrownBy(
            () ->
                catalog.setProperties(
                    Namespace.of("db", "db2", "ns2"), ImmutableMap.of("property", "test")))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot set namespace properties db.db2.ns2 : setProperties is not supported");
  }

  @Test
  public void testDropNamespace() throws IOException {
    String warehouseLocation = tableDir.getAbsolutePath();
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

    assertThatThrownBy(() -> catalog.dropNamespace(Namespace.of("db")))
        .isInstanceOf(NamespaceNotEmptyException.class)
        .hasMessage("Namespace " + namespace1 + " is not empty.");
    assertThat(catalog.dropNamespace(Namespace.of("db2")))
        .as("Should fail to drop namespace that doesn't exist")
        .isFalse();
    assertThat(catalog.dropTable(tbl1)).isTrue();
    assertThat(catalog.dropTable(tbl2)).isTrue();
    assertThat(catalog.dropNamespace(namespace2)).isTrue();
    assertThat(catalog.dropNamespace(namespace1)).isTrue();
    String metaLocation = warehouseLocation + "/" + "db";
    FileSystem fs = Util.getFs(new Path(metaLocation), catalog.getConf());
    assertThat(fs.isDirectory(new Path(metaLocation))).isFalse();
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
    assertThat(tableOperations.findVersion()).isEqualTo(1);
    assertThat(TABLES.load(tableLocation).currentSnapshot().snapshotId())
        .isEqualTo(secondSnapshotId);

    // Write newer data to confirm that we are writing the correct file
    io.deleteFile(versionHintFile.getPath());
    try (PositionOutputStream stream = io.newOutputFile(versionHintFile.getPath()).create()) {
      stream.write("3".getBytes(StandardCharsets.UTF_8));
    }

    // Check the result of the findVersion(), and load the table and check the current snapshotId
    assertThat(tableOperations.findVersion()).isEqualTo(3);
    assertThat(TABLES.load(tableLocation).currentSnapshot().snapshotId())
        .isEqualTo(secondSnapshotId);

    // Write an empty version hint file
    io.deleteFile(versionHintFile.getPath());
    io.newOutputFile(versionHintFile.getPath()).create().close();

    // Check the result of the findVersion(), and load the table and check the current snapshotId
    assertThat(tableOperations.findVersion()).isEqualTo(3);
    assertThat(TABLES.load(tableLocation).currentSnapshot().snapshotId())
        .isEqualTo(secondSnapshotId);

    // Just delete the file
    io.deleteFile(versionHintFile.getPath());

    // Check the result of the versionHint(), and load the table and check the current snapshotId
    assertThat(tableOperations.findVersion()).isEqualTo(3);
    assertThat(TABLES.load(tableLocation).currentSnapshot().snapshotId())
        .isEqualTo(secondSnapshotId);
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
    assertThat(tableOperations.findVersion()).isEqualTo(3);
    assertThat(TABLES.load(tableLocation).currentSnapshot().snapshotId())
        .isEqualTo(secondSnapshotId);

    // Remove all the version files, and see if we can recover. Hint... not :)
    io.deleteFile(tableOperations.getMetadataFile(2).toString());
    io.deleteFile(tableOperations.getMetadataFile(3).toString());

    // Check that we got 0 findVersion, and a NoSuchTableException is thrown when trying to load the
    // table
    assertThat(tableOperations.findVersion()).isEqualTo(0);
    assertThatThrownBy(() -> TABLES.load(tableLocation))
        .isInstanceOf(NoSuchTableException.class)
        .hasMessageStartingWith("Table does not exist");
  }

  @Test
  public void testTableName() throws Exception {
    HadoopCatalog catalog = hadoopCatalog();
    TableIdentifier tableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    catalog.buildTable(tableIdent, SCHEMA).withPartitionSpec(SPEC).create();

    Table table = catalog.loadTable(tableIdent);
    assertThat(table.name()).isEqualTo("hadoop.db.ns1.ns2.tbl");

    TableIdentifier snapshotsTableIdent =
        TableIdentifier.of("db", "ns1", "ns2", "tbl", "snapshots");
    Table snapshotsTable = catalog.loadTable(snapshotsTableIdent);
    assertThat(snapshotsTable.name()).isEqualTo("hadoop.db.ns1.ns2.tbl.snapshots");
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
  public void testRegisterTable() throws IOException {
    TableIdentifier identifier = TableIdentifier.of("a", "t1");
    TableIdentifier identifier2 = TableIdentifier.of("a", "t2");
    HadoopCatalog catalog = hadoopCatalog();
    catalog.createTable(identifier, SCHEMA);
    Table registeringTable = catalog.loadTable(identifier);
    TableOperations ops = ((HasTableOperations) registeringTable).operations();
    String metadataLocation = ((HadoopTableOperations) ops).current().metadataFileLocation();
    assertThat(catalog.registerTable(identifier2, metadataLocation)).isNotNull();
    assertThat(catalog.loadTable(identifier2)).isNotNull();
    assertThat(catalog.dropTable(identifier)).isTrue();
    assertThat(catalog.dropTable(identifier2)).isTrue();
  }

  @Test
  public void testRegisterExistingTable() throws IOException {
    TableIdentifier identifier = TableIdentifier.of("a", "t1");
    HadoopCatalog catalog = hadoopCatalog();
    catalog.createTable(identifier, SCHEMA);
    Table registeringTable = catalog.loadTable(identifier);
    TableOperations ops = ((HasTableOperations) registeringTable).operations();
    String metadataLocation = ((HadoopTableOperations) ops).current().metadataFileLocation();
    assertThatThrownBy(() -> catalog.registerTable(identifier, metadataLocation))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessage("Table already exists: a.t1");
    assertThat(catalog.dropTable(identifier)).isTrue();
  }
}
