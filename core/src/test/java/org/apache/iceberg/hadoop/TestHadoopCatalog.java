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
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

public class TestHadoopCatalog extends HadoopTableTestBase {
  private static ImmutableMap<String, String> meta = ImmutableMap.of();

  @Test
  public void testBasicCatalog() throws Exception {
    Configuration conf = new Configuration();
    String warehousePath = temp.newFolder().getAbsolutePath();
    HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);
    TableIdentifier testTable = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    catalog.createTable(testTable, SCHEMA, PartitionSpec.unpartitioned());
    String metaLocation = catalog.defaultWarehouseLocation(testTable);

    FileSystem fs = Util.getFs(new Path(metaLocation), conf);
    Assert.assertTrue(fs.isDirectory(new Path(metaLocation)));

    catalog.dropTable(testTable);
    Assert.assertFalse(fs.isDirectory(new Path(metaLocation)));
  }

  @Test
  public void testCreateAndDropTableWithoutNamespace() throws Exception {
    Configuration conf = new Configuration();
    String warehousePath = temp.newFolder().getAbsolutePath();
    HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);

    TableIdentifier testTable = TableIdentifier.of("tbl");
    Table table = catalog.createTable(testTable, SCHEMA, PartitionSpec.unpartitioned());

    Assert.assertEquals(table.schema().toString(), TABLE_SCHEMA.toString());
    Assert.assertEquals("hadoop.tbl", table.toString());
    String metaLocation = catalog.defaultWarehouseLocation(testTable);

    FileSystem fs = Util.getFs(new Path(metaLocation), conf);
    Assert.assertTrue(fs.isDirectory(new Path(metaLocation)));

    catalog.dropTable(testTable);
    Assert.assertFalse(fs.isDirectory(new Path(metaLocation)));
  }

  @Test
  public void testDropTable() throws Exception {
    Configuration conf = new Configuration();
    String warehousePath = temp.newFolder().getAbsolutePath();
    HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);
    TableIdentifier testTable = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    catalog.createTable(testTable, SCHEMA, PartitionSpec.unpartitioned());
    String metaLocation = catalog.defaultWarehouseLocation(testTable);

    FileSystem fs = Util.getFs(new Path(metaLocation), conf);
    Assert.assertTrue(fs.isDirectory(new Path(metaLocation)));

    catalog.dropTable(testTable);
    Assert.assertFalse(fs.isDirectory(new Path(metaLocation)));
  }

  @Test
  public void testRenameTable() throws Exception {
    Configuration conf = new Configuration();
    String warehousePath = temp.newFolder().getAbsolutePath();
    HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);
    TableIdentifier testTable = TableIdentifier.of("db", "tbl1");
    catalog.createTable(testTable, SCHEMA, PartitionSpec.unpartitioned());
    AssertHelpers.assertThrows("should throw exception", UnsupportedOperationException.class,
        "Cannot rename Hadoop tables", () -> {
          catalog.renameTable(testTable, TableIdentifier.of("db", "tbl2"));
        }
    );
  }

  @Test
  public void testListTables() throws Exception {
    Configuration conf = new Configuration();
    String warehousePath = temp.newFolder().getAbsolutePath();
    HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);

    TableIdentifier tbl1 = TableIdentifier.of("db", "tbl1");
    TableIdentifier tbl2 = TableIdentifier.of("db", "tbl2");
    TableIdentifier tbl3 = TableIdentifier.of("db", "ns1", "tbl3");
    TableIdentifier tbl4 = TableIdentifier.of("db", "metadata", "metadata");

    Lists.newArrayList(tbl1, tbl2, tbl3, tbl4).forEach(t ->
        catalog.createTable(t, SCHEMA, PartitionSpec.unpartitioned())
    );

    List<TableIdentifier> tbls1 = catalog.listTables(Namespace.of("db"));
    Set<String> tblSet = Sets.newHashSet(tbls1.stream().map(t -> t.name()).iterator());
    Assert.assertEquals(tblSet.size(), 2);
    Assert.assertTrue(tblSet.contains("tbl1"));
    Assert.assertTrue(tblSet.contains("tbl2"));

    List<TableIdentifier> tbls2 = catalog.listTables(Namespace.of("db", "ns1"));
    Assert.assertEquals(tbls2.size(), 1);
    Assert.assertTrue(tbls2.get(0).name().equals("tbl3"));

    AssertHelpers.assertThrows("should throw exception", NoSuchNamespaceException.class,
        "Namespace does not exist: ", () -> {
        catalog.listTables(Namespace.of("db", "ns1", "ns2"));
      });
  }

  @Test
  public void testCallingLocationProviderWhenNoCurrentMetadata() throws IOException {
    Configuration conf = new Configuration();
    String warehousePath = temp.newFolder().getAbsolutePath();
    HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);

    TableIdentifier tableIdent = TableIdentifier.of("ns1", "ns2", "table1");
    Transaction create = catalog.newCreateTableTransaction(tableIdent, SCHEMA);
    create.table().locationProvider();  // NPE triggered if not handled appropriately
    create.commitTransaction();

    Assert.assertEquals("1 table expected", 1, catalog.listTables(Namespace.of("ns1", "ns2")).size());
    catalog.dropTable(tableIdent, true);
  }

  @Test
  public void testCreateNamespace() throws Exception {
    Configuration conf = new Configuration();
    String warehousePath = temp.newFolder().getAbsolutePath();
    HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);

    TableIdentifier tbl1 = TableIdentifier.of("db", "ns1", "ns2", "metadata");
    TableIdentifier tbl2 = TableIdentifier.of("db", "ns2", "ns3", "tbl2");

    Lists.newArrayList(tbl1, tbl2).forEach(t ->
        catalog.createNamespace(t.namespace(), meta)
    );

    String metaLocation1 = warehousePath + "/" + "db/ns1/ns2";
    FileSystem fs1 = Util.getFs(new Path(metaLocation1), conf);
    Assert.assertTrue(fs1.isDirectory(new Path(metaLocation1)));

    String metaLocation2 = warehousePath + "/" + "db/ns2/ns3";
    FileSystem fs2 = Util.getFs(new Path(metaLocation2), conf);
    Assert.assertTrue(fs2.isDirectory(new Path(metaLocation2)));

    AssertHelpers.assertThrows("Should fail to create when namespace already exist: " + tbl1.namespace(),
        org.apache.iceberg.exceptions.AlreadyExistsException.class,
        "Namespace already exists: " + tbl1.namespace(), () -> {
          catalog.createNamespace(tbl1.namespace());
        });
  }

  @Test
  public void testListNamespace() throws Exception {
    Configuration conf = new Configuration();
    String warehousePath = temp.newFolder().getAbsolutePath();
    HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);

    TableIdentifier tbl1 = TableIdentifier.of("db", "ns1", "ns2", "metadata");
    TableIdentifier tbl2 = TableIdentifier.of("db", "ns2", "ns3", "tbl2");
    TableIdentifier tbl3 = TableIdentifier.of("db", "ns3", "tbl4");
    TableIdentifier tbl4 = TableIdentifier.of("db", "metadata");
    TableIdentifier tbl5 = TableIdentifier.of("db2", "metadata");

    Lists.newArrayList(tbl1, tbl2, tbl3, tbl4, tbl5).forEach(t ->
        catalog.createTable(t, SCHEMA, PartitionSpec.unpartitioned())
    );

    List<Namespace> nsp1 = catalog.listNamespaces(Namespace.of("db"));
    Set<String> tblSet = Sets.newHashSet(nsp1.stream().map(t -> t.toString()).iterator());
    Assert.assertEquals(tblSet.size(), 3);
    Assert.assertTrue(tblSet.contains("db.ns1"));
    Assert.assertTrue(tblSet.contains("db.ns2"));
    Assert.assertTrue(tblSet.contains("db.ns3"));

    List<Namespace> nsp2 = catalog.listNamespaces(Namespace.of("db", "ns1"));
    Assert.assertEquals(nsp2.size(), 1);
    Assert.assertTrue(nsp2.get(0).toString().equals("db.ns1.ns2"));

    List<Namespace> nsp3 = catalog.listNamespaces();
    Set<String> tblSet2 = Sets.newHashSet(nsp3.stream().map(t -> t.toString()).iterator());
    Assert.assertEquals(tblSet2.size(), 2);
    Assert.assertTrue(tblSet2.contains("db"));
    Assert.assertTrue(tblSet2.contains("db2"));

    List<Namespace> nsp4 = catalog.listNamespaces();
    Set<String> tblSet3 = Sets.newHashSet(nsp4.stream().map(t -> t.toString()).iterator());
    Assert.assertEquals(tblSet3.size(), 2);
    Assert.assertTrue(tblSet3.contains("db"));
    Assert.assertTrue(tblSet3.contains("db2"));

    AssertHelpers.assertThrows("Should fail to list namespace doesn't exist", NoSuchNamespaceException.class,
        "Namespace does not exist: ", () -> {
          catalog.listNamespaces(Namespace.of("db", "db2", "ns2"));
        });
  }

  @Test
  public void testLoadNamespaceMeta() throws IOException {
    Configuration conf = new Configuration();
    String warehousePath = temp.newFolder().getAbsolutePath();
    HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);

    TableIdentifier tbl1 = TableIdentifier.of("db", "ns1", "ns2", "metadata");
    TableIdentifier tbl2 = TableIdentifier.of("db", "ns2", "ns3", "tbl2");
    TableIdentifier tbl3 = TableIdentifier.of("db", "ns3", "tbl4");
    TableIdentifier tbl4 = TableIdentifier.of("db", "metadata");

    Lists.newArrayList(tbl1, tbl2, tbl3, tbl4).forEach(t ->
        catalog.createTable(t, SCHEMA, PartitionSpec.unpartitioned())
    );
    catalog.loadNamespaceMetadata(Namespace.of("db"));

    AssertHelpers.assertThrows("Should fail to load namespace doesn't exist", NoSuchNamespaceException.class,
        "Namespace does not exist: ", () -> {
          catalog.loadNamespaceMetadata(Namespace.of("db", "db2", "ns2"));
        });
  }

  @Test
  public void testNamespaceExists() throws IOException {
    Configuration conf = new Configuration();
    String warehousePath = temp.newFolder().getAbsolutePath();
    HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);

    TableIdentifier tbl1 = TableIdentifier.of("db", "ns1", "ns2", "metadata");
    TableIdentifier tbl2 = TableIdentifier.of("db", "ns2", "ns3", "tbl2");
    TableIdentifier tbl3 = TableIdentifier.of("db", "ns3", "tbl4");
    TableIdentifier tbl4 = TableIdentifier.of("db", "metadata");

    Lists.newArrayList(tbl1, tbl2, tbl3, tbl4).forEach(t ->
        catalog.createTable(t, SCHEMA, PartitionSpec.unpartitioned())
    );
    Assert.assertTrue("Should true to namespace exist",
        catalog.namespaceExists(Namespace.of("db", "ns1", "ns2")));
    Assert.assertTrue("Should false to namespace doesn't exist",
        !catalog.namespaceExists(Namespace.of("db", "db2", "ns2")));
  }

  @Test
  public void testAlterNamespaceMeta() throws IOException {
    Configuration conf = new Configuration();
    String warehousePath = temp.newFolder().getAbsolutePath();
    HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);
    AssertHelpers.assertThrows("Should fail to change namespace", UnsupportedOperationException.class,
        "Cannot set namespace properties db.db2.ns2 : setProperties is not supported", () -> {
          catalog.setProperties(Namespace.of("db", "db2", "ns2"), ImmutableMap.of("property", "test"));
        });
  }

  @Test
  public void testDropNamespace() throws IOException {
    Configuration conf = new Configuration();
    String warehousePath = temp.newFolder().getAbsolutePath();
    HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);
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
    String metaLocation = warehousePath + "/" + "db";
    FileSystem fs = Util.getFs(new Path(metaLocation), conf);
    Assert.assertFalse(fs.isDirectory(new Path(metaLocation)));
  }
}
