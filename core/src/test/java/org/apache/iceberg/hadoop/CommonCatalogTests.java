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
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.io.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.NullOrder.NULLS_FIRST;
import static org.apache.iceberg.SortDirection.ASC;

/**
 * Common tests for the Iceberg catalog.
 * The tests expect the catalog to support namespaces.
 * <p>
 * Currently, the following catalogs are tested:
 * <ol>
 *   <li>{@link HadoopCatalog}</li>
 *   <li>{@link InMemoryCatalog}</li>
 * </ol>
 * Catalog specific tests should be tested in a separate test,
 * e.g. {@link TestHadoopCatalog}.
 */
@RunWith(Parameterized.class)
public class CommonCatalogTests extends HadoopTableTestBase {

  private enum CatalogFactory {
    HADOOP("hadoop"),
    IN_MEMORY("mem");

    private final String catalogName;

    CatalogFactory(String catalogName) {
      this.catalogName = catalogName;
    }

    public String catalogName() {
      return catalogName;
    }
  }

  @Parameterized.Parameters(name = "catalogFactory = {0}")
  public static Object[][] parameters() {
    return new Object[][] { {CatalogFactory.HADOOP}, {CatalogFactory.IN_MEMORY} };
  }

  private final CatalogFactory catalogFactory;

  public CommonCatalogTests(CatalogFactory catalogFactory) {
    this.catalogFactory = catalogFactory;
  }

  private Catalog catalog() throws Exception {
    switch (catalogFactory) {
      case HADOOP: return hadoopCatalog();
      case IN_MEMORY: {
        Catalog catalog = new InMemoryCatalog();
        catalog.initialize("mem", ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION,
            temp.newFolder().getAbsolutePath()));
        return catalog;
      }
      default:
        throw new IllegalStateException("Unsupported catalog factory: " + catalogFactory);
    }
  }

  @Test
  public void testCreateTableBuilder() throws Exception {
    TableIdentifier tableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    Table table = catalog().buildTable(tableIdent, SCHEMA)
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
    Catalog catalog = catalog();
    TableIdentifier tableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    Transaction txn = catalog.buildTable(tableIdent, SCHEMA)
        .withPartitionSpec(null)
        .createTransaction();
    txn.commitTransaction();
    Table table = catalog.loadTable(tableIdent);

    Assert.assertEquals(TABLE_SCHEMA.toString(), table.schema().toString());
    Assert.assertTrue(table.spec().isUnpartitioned());
  }

  @Test
  public void testReplaceTxnBuilder() throws Exception {
    Catalog catalog = catalog();
    TableIdentifier tableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl");

    Transaction createTxn = catalog.buildTable(tableIdent, SCHEMA)
        .withPartitionSpec(SPEC)
        .withProperty("key1", "value1")
        .createOrReplaceTransaction();

    createTxn.newAppend()
        .appendFile(FILE_A)
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
  public void testCreateTableDefaultSortOrder() throws Exception {
    TableIdentifier tableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    Table table = catalog().createTable(tableIdent, SCHEMA, SPEC);

    SortOrder sortOrder = table.sortOrder();
    Assert.assertEquals("Order ID must match", 0, sortOrder.orderId());
    Assert.assertTrue("Order must unsorted", sortOrder.isUnsorted());
  }

  @Test
  public void testCreateTableCustomSortOrder() throws Exception {
    TableIdentifier tableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    SortOrder order = SortOrder.builderFor(SCHEMA)
        .asc("id", NULLS_FIRST)
        .build();
    Table table = catalog().buildTable(tableIdent, SCHEMA)
        .withPartitionSpec(SPEC)
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
    Catalog catalog = catalog();
    TableIdentifier testTable = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    catalog.createTable(testTable, SCHEMA, PartitionSpec.unpartitioned());
    Assert.assertTrue(catalog.tableExists(testTable));
  }

  @Test
  public void testCreateAndDropTableWithoutNamespace() throws Exception {
    Catalog catalog = catalog();

    TableIdentifier testTable = TableIdentifier.of("tbl");
    Table table = catalog.createTable(testTable, SCHEMA, PartitionSpec.unpartitioned());

    Assert.assertEquals(table.schema().toString(), TABLE_SCHEMA.toString());
    Assert.assertEquals(catalogFactory.catalogName() + ".tbl", table.name());
    Assert.assertTrue(catalog.tableExists(testTable));

    catalog.dropTable(testTable);
    Assert.assertFalse(catalog.tableExists(testTable));
  }

  @Test
  public void testDropTable() throws Exception {
    Catalog catalog = catalog();
    TableIdentifier testTable = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    catalog.createTable(testTable, SCHEMA, PartitionSpec.unpartitioned());
    Assert.assertTrue(catalog.tableExists(testTable));

    catalog.dropTable(testTable);
    Assert.assertFalse(catalog.tableExists(testTable));
  }

  @Test
  public void testDropNonIcebergTable() throws Exception {
    Catalog catalog = catalog();
    TableIdentifier testTable = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    Assert.assertFalse(catalog.dropTable(testTable));
  }

  @Test
  public void testListTables() throws Exception {
    Catalog catalog = catalog();

    TableIdentifier tbl1 = TableIdentifier.of("db", "tbl1");
    TableIdentifier tbl2 = TableIdentifier.of("db", "tbl2");
    TableIdentifier tbl3 = TableIdentifier.of("db", "ns1", "tbl3");
    TableIdentifier tbl4 = TableIdentifier.of("db", "metadata", "metadata");

    Lists.newArrayList(tbl1, tbl2, tbl3, tbl4).forEach(t ->
        catalog.createTable(t, SCHEMA, PartitionSpec.unpartitioned())
    );

    List<TableIdentifier> tbls1 = catalog.listTables(Namespace.of("db"));
    Set<String> tblSet = Sets.newHashSet(tbls1.stream().map(t -> t.name()).iterator());
    Assert.assertEquals(2, tblSet.size());
    Assert.assertTrue(tblSet.contains("tbl1"));
    Assert.assertTrue(tblSet.contains("tbl2"));

    List<TableIdentifier> tbls2 = catalog.listTables(Namespace.of("db", "ns1"));
    Assert.assertEquals("table identifiers", 1, tbls2.size());
    Assert.assertEquals("table name", "tbl3", tbls2.get(0).name());

    AssertHelpers.assertThrows("should throw exception", NoSuchNamespaceException.class,
        "Namespace does not exist: ", () -> {
        catalog.listTables(Namespace.of("db", "ns1", "ns2"));
      });
  }

  @Test
  public void testCallingLocationProviderWhenNoCurrentMetadata() throws Exception {
    Catalog catalog = catalog();

    TableIdentifier tableIdent = TableIdentifier.of("ns1", "ns2", "table1");
    Transaction create = catalog.newCreateTableTransaction(tableIdent, SCHEMA);
    create.table().locationProvider();  // NPE triggered if not handled appropriately
    create.commitTransaction();

    Assert.assertEquals("1 table expected", 1, catalog.listTables(Namespace.of("ns1", "ns2")).size());
    catalog.dropTable(tableIdent, true);
  }

  @Test
  public void testCreateNamespace() throws Exception {
    Catalog catalog = catalog();
    SupportsNamespaces catalogWithNamespaceSupport = castToSupportsNamespaces(catalog);

    TableIdentifier tbl1 = TableIdentifier.of("db", "ns1", "ns2", "metadata");
    TableIdentifier tbl2 = TableIdentifier.of("db", "ns2", "ns3", "tbl2");

    Lists.newArrayList(tbl1, tbl2).forEach(t ->
        catalogWithNamespaceSupport.createNamespace(t.namespace(), ImmutableMap.of())
    );

    Assert.assertTrue(catalogWithNamespaceSupport.namespaceExists(tbl1.namespace()));

    AssertHelpers.assertThrows("Should fail to create when namespace already exist: " + tbl1.namespace(),
        org.apache.iceberg.exceptions.AlreadyExistsException.class,
        "Namespace already exists: " + tbl1.namespace(), () -> {
          catalogWithNamespaceSupport.createNamespace(tbl1.namespace());
        });
  }

  @Test
  public void testListNamespace() throws Exception {
    Catalog catalog = catalog();
    SupportsNamespaces catalogWithNamespaceSupport = castToSupportsNamespaces(catalog);

    TableIdentifier tbl1 = TableIdentifier.of("db", "ns1", "ns2", "metadata");
    TableIdentifier tbl2 = TableIdentifier.of("db", "ns2", "ns3", "tbl2");
    TableIdentifier tbl3 = TableIdentifier.of("db", "ns3", "tbl4");
    TableIdentifier tbl4 = TableIdentifier.of("db", "metadata");
    TableIdentifier tbl5 = TableIdentifier.of("db2", "metadata");

    Lists.newArrayList(tbl1, tbl2, tbl3, tbl4, tbl5).forEach(t ->
        catalog.createTable(t, SCHEMA, PartitionSpec.unpartitioned())
    );

    List<Namespace> nsp1 = catalogWithNamespaceSupport.listNamespaces(Namespace.of("db"));
    Set<String> tblSet = Sets.newHashSet(nsp1.stream().map(t -> t.toString()).iterator());
    Assert.assertEquals(3, tblSet.size());
    Assert.assertTrue(tblSet.contains("db.ns1"));
    Assert.assertTrue(tblSet.contains("db.ns2"));
    Assert.assertTrue(tblSet.contains("db.ns3"));

    List<Namespace> nsp2 = catalogWithNamespaceSupport.listNamespaces(Namespace.of("db", "ns1"));
    Assert.assertEquals(1, nsp2.size());
    Assert.assertTrue(nsp2.get(0).toString().equals("db.ns1.ns2"));

    List<Namespace> nsp3 = catalogWithNamespaceSupport.listNamespaces();
    Set<String> tblSet2 = Sets.newHashSet(nsp3.stream().map(t -> t.toString()).iterator());
    Assert.assertEquals(2, tblSet2.size());
    Assert.assertTrue(tblSet2.contains("db"));
    Assert.assertTrue(tblSet2.contains("db2"));

    List<Namespace> nsp4 = catalogWithNamespaceSupport.listNamespaces();
    Set<String> tblSet3 = Sets.newHashSet(nsp4.stream().map(t -> t.toString()).iterator());
    Assert.assertEquals(2, tblSet3.size());
    Assert.assertTrue(tblSet3.contains("db"));
    Assert.assertTrue(tblSet3.contains("db2"));

    AssertHelpers.assertThrows("Should fail to list namespace doesn't exist", NoSuchNamespaceException.class,
        "Namespace does not exist: ", () -> {
          catalogWithNamespaceSupport.listNamespaces(Namespace.of("db", "db2", "ns2"));
        });
  }

  @Test
  public void testLoadNamespaceMeta() throws Exception {
    Catalog catalog = catalog();
    SupportsNamespaces catalogWithNamespaceSupport = castToSupportsNamespaces(catalog);

    TableIdentifier tbl1 = TableIdentifier.of("db", "ns1", "ns2", "metadata");
    TableIdentifier tbl2 = TableIdentifier.of("db", "ns2", "ns3", "tbl2");
    TableIdentifier tbl3 = TableIdentifier.of("db", "ns3", "tbl4");
    TableIdentifier tbl4 = TableIdentifier.of("db", "metadata");

    Lists.newArrayList(tbl1, tbl2, tbl3, tbl4).forEach(t ->
        catalog.createTable(t, SCHEMA, PartitionSpec.unpartitioned())
    );
    catalogWithNamespaceSupport.loadNamespaceMetadata(Namespace.of("db"));

    AssertHelpers.assertThrows("Should fail to load namespace doesn't exist", NoSuchNamespaceException.class,
        "Namespace does not exist: ", () -> {
          catalogWithNamespaceSupport.loadNamespaceMetadata(Namespace.of("db", "db2", "ns2"));
        });
  }

  @Test
  public void testNamespaceExists() throws Exception {
    Catalog catalog = catalog();
    SupportsNamespaces catalogWithNamespaceSupport = castToSupportsNamespaces(catalog);

    TableIdentifier tbl1 = TableIdentifier.of("db", "ns1", "ns2", "metadata");
    TableIdentifier tbl2 = TableIdentifier.of("db", "ns2", "ns3", "tbl2");
    TableIdentifier tbl3 = TableIdentifier.of("db", "ns3", "tbl4");
    TableIdentifier tbl4 = TableIdentifier.of("db", "metadata");

    Lists.newArrayList(tbl1, tbl2, tbl3, tbl4).forEach(t ->
        catalog.createTable(t, SCHEMA, PartitionSpec.unpartitioned())
    );
    Assert.assertTrue("Should true to namespace exist",
        catalogWithNamespaceSupport.namespaceExists(Namespace.of("db", "ns1", "ns2")));
    Assert.assertTrue("Should false to namespace doesn't exist",
        !catalogWithNamespaceSupport.namespaceExists(Namespace.of("db", "db2", "ns2")));
  }

  @Test
  public void testDropNamespace() throws Exception {
    Catalog catalog = catalog();
    SupportsNamespaces catalogWithNamespaceSupport = castToSupportsNamespaces(catalog);
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
          catalogWithNamespaceSupport.dropNamespace(Namespace.of("db"));
        });
    Assert.assertFalse("Should fail to drop namespace doesn't exist",
          catalogWithNamespaceSupport.dropNamespace(Namespace.of("db2")));
    Assert.assertTrue(catalog.dropTable(tbl1));
    Assert.assertTrue(catalog.dropTable(tbl2));
    Assert.assertTrue(catalogWithNamespaceSupport.dropNamespace(namespace2));
    Assert.assertTrue(catalogWithNamespaceSupport.dropNamespace(namespace1));
  }

  @Test
  public void testTableName() throws Exception {
    Catalog catalog = catalog();
    TableIdentifier tableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    catalog.buildTable(tableIdent, SCHEMA)
        .withPartitionSpec(SPEC)
        .create();

    Table table = catalog.loadTable(tableIdent);
    Assert.assertEquals("Name must match", catalogFactory.catalogName() + ".db.ns1.ns2.tbl", table.name());

    TableIdentifier snapshotsTableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl", "snapshots");
    Table snapshotsTable = catalog.loadTable(snapshotsTableIdent);
    Assert.assertEquals("Name must match",
        catalogFactory.catalogName() + ".db.ns1.ns2.tbl.snapshots", snapshotsTable.name());
  }

  private SupportsNamespaces castToSupportsNamespaces(Catalog catalog) {
    if (!(catalog instanceof SupportsNamespaces)) {
      throw new IllegalArgumentException(
          String.format("Catalog %s does not implement SupportsNamespaces", catalog.getClass()));
    }
    return (SupportsNamespaces) catalog;
  }
}
