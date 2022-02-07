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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

public class TestHadoopCatalog extends HadoopTableTestBase {
  private static final ImmutableMap<String, String> meta = ImmutableMap.of();

  @Test
  public void testTableBuilderWithLocation() throws Exception {
    HadoopCatalog catalog = hadoopCatalog();
    TableIdentifier tableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl");

    AssertHelpers.assertThrows("Should reject a custom location",
        IllegalArgumentException.class, "Cannot set a custom location for a path-based table",
        () -> catalog.buildTable(tableIdent, SCHEMA).withLocation("custom").create());

    AssertHelpers.assertThrows("Should reject a custom location",
        IllegalArgumentException.class, "Cannot set a custom location for a path-based table",
        () -> catalog.buildTable(tableIdent, SCHEMA).withLocation("custom").createTransaction());

    AssertHelpers.assertThrows("Should reject a custom location",
        IllegalArgumentException.class, "Cannot set a custom location for a path-based table",
        () -> catalog.buildTable(tableIdent, SCHEMA).withLocation("custom").createOrReplaceTransaction());
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
    AssertHelpers.assertThrows("should throw exception", UnsupportedOperationException.class,
        "Cannot rename Hadoop tables", () -> {
          catalog.renameTable(testTable, TableIdentifier.of("db", "tbl2"));
        }
    );
  }

  @Test
  public void testCreateNamespace() throws Exception {
    String warehouseLocation = temp.newFolder().getAbsolutePath();
    HadoopCatalog catalog = new HadoopCatalog();
    catalog.setConf(new Configuration());
    catalog.initialize("hadoop", ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation));

    TableIdentifier tbl1 = TableIdentifier.of("db", "ns1", "ns2", "metadata");
    TableIdentifier tbl2 = TableIdentifier.of("db", "ns2", "ns3", "tbl2");

    Lists.newArrayList(tbl1, tbl2).forEach(t ->
        catalog.createNamespace(t.namespace(), meta)
    );

    String metaLocation1 = warehouseLocation + "/" + "db/ns1/ns2";
    FileSystem fs1 = Util.getFs(new Path(metaLocation1), catalog.getConf());
    Assert.assertTrue(fs1.isDirectory(new Path(metaLocation1)));

    String metaLocation2 = warehouseLocation + "/" + "db/ns2/ns3";
    FileSystem fs2 = Util.getFs(new Path(metaLocation2), catalog.getConf());
    Assert.assertTrue(fs2.isDirectory(new Path(metaLocation2)));

    AssertHelpers.assertThrows("Should fail to create when namespace already exist: " + tbl1.namespace(),
        org.apache.iceberg.exceptions.AlreadyExistsException.class,
        "Namespace already exists: " + tbl1.namespace(), () -> {
          catalog.createNamespace(tbl1.namespace());
        });
  }

  @Test
  public void testAlterNamespaceMeta() throws IOException {
    HadoopCatalog catalog = hadoopCatalog();
    AssertHelpers.assertThrows("Should fail to change namespace", UnsupportedOperationException.class,
        "Cannot set namespace properties db.db2.ns2 : setProperties is not supported", () -> {
          catalog.setProperties(Namespace.of("db", "db2", "ns2"), ImmutableMap.of("property", "test"));
        });
  }

  @Test
  public void testDropNamespace() throws IOException {
    String warehouseLocation = temp.newFolder().getAbsolutePath();
    HadoopCatalog catalog = new HadoopCatalog();
    catalog.setConf(new Configuration());
    catalog.initialize("hadoop", ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation));
    Namespace namespace1 = Namespace.of("db");
    Namespace namespace2 = Namespace.of("db", "ns1");

    TableIdentifier tbl1 = TableIdentifier.of(namespace1, "tbl1");
    TableIdentifier tbl2 = TableIdentifier.of(namespace2, "tbl1");

    Lists.newArrayList(tbl1, tbl2).forEach(t ->
        catalog.createTable(t, SCHEMA, PartitionSpec.unpartitioned())
    );

    AssertHelpers.assertThrows("Should fail to drop namespace is not empty " + namespace1,
        NamespaceNotEmptyException.class,
        "Namespace " + namespace1 + " is not empty.", () -> {
          catalog.dropNamespace(Namespace.of("db"));
        });
    Assert.assertFalse("Should fail to drop namespace doesn't exist",
          catalog.dropNamespace(Namespace.of("db2")));
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

    HadoopTableOperations tableOperations = (HadoopTableOperations) TABLES.newTableOps(tableLocation);

    long secondSnapshotId = table.currentSnapshot().snapshotId();

    // Write old data to confirm that we are writing the correct file
    FileIO io = table.io();
    io.deleteFile(versionHintFile.getPath());
    try (PositionOutputStream stream = io.newOutputFile(versionHintFile.getPath()).create()) {
      stream.write("1".getBytes(StandardCharsets.UTF_8));
    }

    // Check the result of the findVersion(), and load the table and check the current snapshotId
    Assert.assertEquals(1, tableOperations.findVersion());
    Assert.assertEquals(secondSnapshotId, TABLES.load(tableLocation).currentSnapshot().snapshotId());

    // Write newer data to confirm that we are writing the correct file
    io.deleteFile(versionHintFile.getPath());
    try (PositionOutputStream stream = io.newOutputFile(versionHintFile.getPath()).create()) {
      stream.write("3".getBytes(StandardCharsets.UTF_8));
    }

    // Check the result of the findVersion(), and load the table and check the current snapshotId
    Assert.assertEquals(3, tableOperations.findVersion());
    Assert.assertEquals(secondSnapshotId, TABLES.load(tableLocation).currentSnapshot().snapshotId());

    // Write an empty version hint file
    io.deleteFile(versionHintFile.getPath());
    io.newOutputFile(versionHintFile.getPath()).create().close();

    // Check the result of the findVersion(), and load the table and check the current snapshotId
    Assert.assertEquals(3, tableOperations.findVersion());
    Assert.assertEquals(secondSnapshotId, TABLES.load(tableLocation).currentSnapshot().snapshotId());

    // Just delete the file
    io.deleteFile(versionHintFile.getPath());

    // Check the result of the versionHint(), and load the table and check the current snapshotId
    Assert.assertEquals(3, tableOperations.findVersion());
    Assert.assertEquals(secondSnapshotId, TABLES.load(tableLocation).currentSnapshot().snapshotId());
  }

  @Test
  public void testVersionHintFileMissingMetadata() throws Exception {
    addVersionsToTable(table);

    HadoopTableOperations tableOperations = (HadoopTableOperations) TABLES.newTableOps(tableLocation);

    long secondSnapshotId = table.currentSnapshot().snapshotId();

    // Write old data to confirm that we are writing the correct file
    FileIO io = table.io();
    io.deleteFile(versionHintFile.getPath());

    // Remove the first version file, and see if we can recover
    io.deleteFile(tableOperations.getMetadataFile(1).toString());

    // Check the result of the findVersion(), and load the table and check the current snapshotId
    Assert.assertEquals(3, tableOperations.findVersion());
    Assert.assertEquals(secondSnapshotId, TABLES.load(tableLocation).currentSnapshot().snapshotId());

    // Remove all the version files, and see if we can recover. Hint... not :)
    io.deleteFile(tableOperations.getMetadataFile(2).toString());
    io.deleteFile(tableOperations.getMetadataFile(3).toString());

    // Check that we got 0 findVersion, and a NoSuchTableException is thrown when trying to load the table
    Assert.assertEquals(0, tableOperations.findVersion());
    AssertHelpers.assertThrows(
        "Should not be able to find the table",
        NoSuchTableException.class,
        "Table does not exist",
        () -> TABLES.load(tableLocation));
  }

  private static void addVersionsToTable(Table table) {
    DataFile dataFile1 = DataFiles.builder(SPEC)
        .withPath("/a.parquet")
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .build();

    DataFile dataFile2 = DataFiles.builder(SPEC)
        .withPath("/b.parquet")
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .build();

    table.newAppend().appendFile(dataFile1).commit();
    table.newAppend().appendFile(dataFile2).commit();
  }
}
