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

package org.apache.iceberg.catalog;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReplaceSortOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.CharSequenceSet;
import org.assertj.core.util.Streams;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.required;

public abstract class CatalogTests<C extends Catalog & SupportsNamespaces> {
  private static final Namespace NS = Namespace.of("newdb");
  private static final TableIdentifier TABLE = TableIdentifier.of(NS, "table");

  // Schema passed to create tables
  private static final Schema SCHEMA = new Schema(
      required(3, "id", Types.IntegerType.get(), "unique ID"),
      required(4, "data", Types.StringType.get())
  );

  // This is the actual schema for the table, with column IDs reassigned
  private static final Schema TABLE_SCHEMA = new Schema(
      required(1, "id", Types.IntegerType.get(), "unique ID"),
      required(2, "data", Types.StringType.get())
  );

  // Partition spec used to create tables
  private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA)
      .bucket("id", 16)
      .build();

  private static final PartitionSpec TABLE_SPEC = PartitionSpec.builderFor(TABLE_SCHEMA)
      .bucket("id", 16)
      .build();

  // Partition spec used to create tables
  static final SortOrder WRITE_ORDER = SortOrder.builderFor(SCHEMA)
      .asc(Expressions.bucket("id", 16))
      .asc("id")
      .build();

  static final SortOrder TABLE_WRITE_ORDER = SortOrder.builderFor(TABLE_SCHEMA)
      .asc(Expressions.bucket("id", 16))
      .asc("id")
      .build();

  static final DataFile FILE_A = DataFiles.builder(SPEC)
      .withPath("/path/to/data-a.parquet")
      .withFileSizeInBytes(10)
      .withPartitionPath("id_bucket=0") // easy way to set partition data for now
      .withRecordCount(2) // needs at least one record or else metrics will filter it out
      .build();

  static final DataFile FILE_B = DataFiles.builder(SPEC)
      .withPath("/path/to/data-b.parquet")
      .withFileSizeInBytes(10)
      .withPartitionPath("id_bucket=1") // easy way to set partition data for now
      .withRecordCount(2) // needs at least one record or else metrics will filter it out
      .build();

  static final DataFile FILE_C = DataFiles.builder(SPEC)
      .withPath("/path/to/data-c.parquet")
      .withFileSizeInBytes(10)
      .withPartitionPath("id_bucket=2") // easy way to set partition data for now
      .withRecordCount(2) // needs at least one record or else metrics will filter it out
      .build();

  protected abstract C catalog();

  protected boolean supportsNamespaceProperties() {
    return true;
  }

  protected boolean requiresNamespaceCreate() {
    return false;
  }

  protected boolean supportsServerSideRetry() {
    return false;
  }

  @Test
  public void testCreateNamespace() {
    C catalog = catalog();

    Assert.assertFalse("Namespace should not exist", catalog.namespaceExists(NS));

    catalog.createNamespace(NS);
    Assert.assertEquals("Catalog should have the created namespace", ImmutableList.of(NS), catalog.listNamespaces());
    Assert.assertTrue("Namespace should exist", catalog.namespaceExists(NS));
  }

  @Test
  public void testCreateExistingNamespace() {
    C catalog = catalog();

    Assert.assertFalse("Namespace should not exist", catalog.namespaceExists(NS));

    catalog.createNamespace(NS);
    Assert.assertTrue("Namespace should exist", catalog.namespaceExists(NS));

    AssertHelpers.assertThrows("Should fail to create an existing database",
        AlreadyExistsException.class, "newdb", () -> catalog.createNamespace(NS));
    Assert.assertTrue("Namespace should still exist", catalog.namespaceExists(NS));
  }

  @Test
  public void testCreateNamespaceWithProperties() {
    Assume.assumeTrue(supportsNamespaceProperties());

    C catalog = catalog();

    Assert.assertFalse("Namespace should not exist", catalog.namespaceExists(NS));

    Map<String, String> createProps = ImmutableMap.of("prop", "val");
    catalog.createNamespace(NS, createProps);
    Assert.assertTrue("Namespace should exist", catalog.namespaceExists(NS));

    Map<String, String> props = catalog.loadNamespaceMetadata(NS);
    Assert.assertEquals("Create properties should be a subset of returned properties",
        createProps.entrySet(),
        Sets.intersection(createProps.entrySet(), props.entrySet()));
  }

  @Test
  public void testLoadNamespaceMetadata() {
    C catalog = catalog();

    Assert.assertFalse("Namespace should not exist", catalog.namespaceExists(NS));

    AssertHelpers.assertThrows("Should fail to load nonexistent namespace metadata",
        NoSuchNamespaceException.class, "newdb",
        () -> catalog.loadNamespaceMetadata(NS));

    catalog.createNamespace(NS);
    Assert.assertTrue("Namespace should exist", catalog.namespaceExists(NS));

    Map<String, String> props = catalog.loadNamespaceMetadata(NS);
    Assert.assertNotNull("Should return non-null property map", props);
    // note that there are no requirements for the properties returned by the catalog
  }

  @Test
  public void testSetNamespaceProperties() {
    Assume.assumeTrue(supportsNamespaceProperties());

    C catalog = catalog();

    Map<String, String> properties = ImmutableMap.of("owner", "user", "created-at", "sometime");

    catalog.createNamespace(NS);
    catalog.setProperties(NS, properties);

    Map<String, String> actualProperties = catalog.loadNamespaceMetadata(NS);
    Assert.assertEquals("Set properties should be a subset of returned properties",
        properties.entrySet(),
        Sets.intersection(properties.entrySet(), actualProperties.entrySet()));
  }

  @Test
  public void testUpdateNamespaceProperties() {
    Assume.assumeTrue(supportsNamespaceProperties());

    C catalog = catalog();

    Map<String, String> initialProperties = ImmutableMap.of("owner", "user");

    catalog.createNamespace(NS);
    catalog.setProperties(NS, initialProperties);

    Map<String, String> actualProperties = catalog.loadNamespaceMetadata(NS);
    Assert.assertEquals("Set properties should be a subset of returned properties",
        initialProperties.entrySet(),
        Sets.intersection(initialProperties.entrySet(), actualProperties.entrySet()));

    Map<String, String> updatedProperties = ImmutableMap.of("owner", "newuser");

    catalog.setProperties(NS, updatedProperties);

    Map<String, String> finalProperties = catalog.loadNamespaceMetadata(NS);
    Assert.assertEquals("Updated properties should be a subset of returned properties",
        updatedProperties.entrySet(),
        Sets.intersection(updatedProperties.entrySet(), finalProperties.entrySet()));
  }

  @Test
  public void testUpdateAndSetNamespaceProperties() {
    Assume.assumeTrue(supportsNamespaceProperties());

    C catalog = catalog();

    Map<String, String> initialProperties = ImmutableMap.of("owner", "user");

    catalog.createNamespace(NS);
    catalog.setProperties(NS, initialProperties);

    Map<String, String> actualProperties = catalog.loadNamespaceMetadata(NS);
    Assert.assertEquals("Set properties should be a subset of returned properties",
        initialProperties.entrySet(),
        Sets.intersection(initialProperties.entrySet(), actualProperties.entrySet()));

    Map<String, String> updatedProperties = ImmutableMap.of("owner", "newuser", "last-modified-at", "now");

    catalog.setProperties(NS, updatedProperties);

    Map<String, String> finalProperties = catalog.loadNamespaceMetadata(NS);
    Assert.assertEquals("Updated properties should be a subset of returned properties",
        updatedProperties.entrySet(),
        Sets.intersection(updatedProperties.entrySet(), finalProperties.entrySet()));
  }

  @Test
  public void testSetNamespacePropertiesNamespaceDoesNotExist() {
    Assume.assumeTrue(supportsNamespaceProperties());

    C catalog = catalog();

    AssertHelpers.assertThrows("setProperties should fail if the namespace does not exist",
        NoSuchNamespaceException.class, "does not exist",
        () -> catalog.setProperties(NS, ImmutableMap.of("test", "value")));
  }

  @Test
  public void testRemoveNamespaceProperties() {
    Assume.assumeTrue(supportsNamespaceProperties());

    C catalog = catalog();

    Map<String, String> properties = ImmutableMap.of("owner", "user", "created-at", "sometime");

    catalog.createNamespace(NS);
    catalog.setProperties(NS, properties);
    catalog.removeProperties(NS, ImmutableSet.of("created-at"));

    Map<String, String> actualProperties = catalog.loadNamespaceMetadata(NS);
    Assert.assertFalse("Should not contain deleted property key", actualProperties.containsKey("created-at"));
    Assert.assertEquals("Expected properties should be a subset of returned properties",
        ImmutableMap.of("owner", "user").entrySet(),
        Sets.intersection(properties.entrySet(), actualProperties.entrySet()));
  }

  @Test
  public void testRemoveNamespacePropertiesNamespaceDoesNotExist() {
    Assume.assumeTrue(supportsNamespaceProperties());

    C catalog = catalog();

    AssertHelpers.assertThrows("setProperties should fail if the namespace does not exist",
        NoSuchNamespaceException.class, "does not exist",
        () -> catalog.removeProperties(NS, ImmutableSet.of("a", "b")));
  }

  @Test
  public void testDropNamespace() {
    C catalog = catalog();

    Assert.assertFalse("Namespace should not exist", catalog.namespaceExists(NS));

    catalog.createNamespace(NS);
    Assert.assertTrue("Namespace should exist", catalog.namespaceExists(NS));

    Assert.assertTrue("Dropping an existing namespace should return true", catalog.dropNamespace(NS));
    Assert.assertFalse("Namespace should not exist", catalog.namespaceExists(NS));
  }

  @Test
  public void testDropNonexistentNamespace() {
    C catalog = catalog();

    Assert.assertFalse("Dropping a nonexistent namespace should return false", catalog.dropNamespace(NS));
  }

  @Test
  public void testListNamespaces() {
    C catalog = catalog();
    // the catalog may automatically create a default namespace
    List<Namespace> starting = catalog.listNamespaces();

    Namespace ns1 = Namespace.of("newdb_1");
    Namespace ns2 = Namespace.of("newdb_2");

    catalog.createNamespace(ns1);
    Assert.assertEquals("Should include newdb_1", concat(starting, ns1), catalog.listNamespaces());

    catalog.createNamespace(ns2);
    Assert.assertEquals("Should include newdb_1 and newdb_2", concat(starting, ns1, ns2), catalog.listNamespaces());

    catalog.dropNamespace(ns1);
    Assert.assertEquals("Should include newdb_2, not newdb_1", concat(starting, ns2), catalog.listNamespaces());

    catalog.dropNamespace(ns2);
    Assert.assertEquals("Should include only starting namespaces", starting, catalog.listNamespaces());
  }

  @Test
  public void testNamespaceWithSlash() {
    C catalog = catalog();

    Namespace withSlash = Namespace.of("new/db");

    Assert.assertFalse("Namespace should not exist", catalog.namespaceExists(withSlash));

    catalog.createNamespace(withSlash);
    Assert.assertTrue("Namespace should exist", catalog.namespaceExists(withSlash));

    Map<String, String> properties = catalog.loadNamespaceMetadata(withSlash);
    Assert.assertNotNull("Properties should be accessible", properties);

    Assert.assertTrue("Dropping the namespace should succeed", catalog.dropNamespace(withSlash));
    Assert.assertFalse("Namespace should not exist", catalog.namespaceExists(withSlash));
  }

  @Test
  public void testNamespaceWithDot() {
    C catalog = catalog();

    Namespace withDot = Namespace.of("new.db");

    Assert.assertFalse("Namespace should not exist", catalog.namespaceExists(withDot));

    catalog.createNamespace(withDot);
    Assert.assertTrue("Namespace should exist", catalog.namespaceExists(withDot));

    Map<String, String> properties = catalog.loadNamespaceMetadata(withDot);
    Assert.assertNotNull("Properties should be accessible", properties);

    Assert.assertTrue("Dropping the namespace should succeed", catalog.dropNamespace(withDot));
    Assert.assertFalse("Namespace should not exist", catalog.namespaceExists(withDot));
  }

  @Test
  public void testBasicCreateTable() {
    C catalog = catalog();

    TableIdentifier ident = TableIdentifier.of("ns", "table");

    Assert.assertFalse("Table should not exist", catalog.tableExists(ident));

    Table table = catalog.buildTable(ident, SCHEMA).create();
    Assert.assertTrue("Table should exist", catalog.tableExists(ident));

    // validate table settings
    Assert.assertEquals("Table name should report its full name", catalog.name() + "." + ident, table.name());
    Assert.assertEquals("Schema should match expected ID assignment",
        TABLE_SCHEMA.asStruct(), table.schema().asStruct());
    Assert.assertNotNull("Should have a location", table.location());
    Assert.assertTrue("Should be unpartitioned", table.spec().isUnpartitioned());
    Assert.assertTrue("Should be unsorted", table.sortOrder().isUnsorted());
    Assert.assertNotNull("Should have table properties", table.properties());
  }

  @Test
  public void testBasicCreateTableThatAlreadyExists() {
    C catalog = catalog();

    TableIdentifier ident = TableIdentifier.of("ns", "table");

    Assert.assertFalse("Table should not exist", catalog.tableExists(ident));

    catalog.buildTable(ident, SCHEMA).create();
    Assert.assertTrue("Table should exist", catalog.tableExists(ident));

    AssertHelpers.assertThrows("Should fail to create a table that already exists",
        AlreadyExistsException.class, "ns.table",
        () -> catalog.buildTable(ident, new Schema(required(1, "some_id", Types.IntegerType.get()))).create());

    Table table = catalog.loadTable(ident);
    Assert.assertEquals("Schema should match original table schema",
        TABLE_SCHEMA.asStruct(), table.schema().asStruct());
  }

  @Test
  public void testCompleteCreateTable() {
    C catalog = catalog();

    TableIdentifier ident = TableIdentifier.of("ns", "table");

    Assert.assertFalse("Table should not exist", catalog.tableExists(ident));

    Map<String, String> properties = ImmutableMap.of("user", "someone", "created-at", "2022-02-25T00:38:19");
    Table table = catalog.buildTable(ident, SCHEMA)
        .withLocation("file:/tmp/ns/table")
        .withPartitionSpec(SPEC)
        .withSortOrder(WRITE_ORDER)
        .withProperties(properties)
        .create();

    // validate table settings
    Assert.assertEquals("Table name should report its full name", catalog.name() + "." + ident, table.name());
    Assert.assertTrue("Table should exist", catalog.tableExists(ident));
    Assert.assertEquals("Schema should match expected ID assignment",
        TABLE_SCHEMA.asStruct(), table.schema().asStruct());
    Assert.assertNotNull("Should have a location", table.location());
    Assert.assertEquals("Should use requested partition spec", TABLE_SPEC, table.spec());
    Assert.assertEquals("Should use requested write order", TABLE_WRITE_ORDER, table.sortOrder());
    Assert.assertEquals("Table properties should be a superset of the requested properties",
        properties.entrySet(),
        Sets.intersection(properties.entrySet(), table.properties().entrySet()));
  }

  @Test
  public void testLoadTable() {
    C catalog = catalog();

    TableIdentifier ident = TableIdentifier.of("ns", "table");

    Assert.assertFalse("Table should not exist", catalog.tableExists(ident));

    Map<String, String> properties = ImmutableMap.of("user", "someone", "created-at", "2022-02-25T00:38:19");
    catalog.buildTable(ident, SCHEMA)
        .withLocation("file:/tmp/ns/table")
        .withPartitionSpec(SPEC)
        .withSortOrder(WRITE_ORDER)
        .withProperties(properties)
        .create();
    Assert.assertTrue("Table should exist", catalog.tableExists(ident));

    Table table = catalog.loadTable(ident);
    // validate table settings
    Assert.assertEquals("Table name should report its full name", catalog.name() + "." + ident, table.name());
    Assert.assertTrue("Table should exist", catalog.tableExists(ident));
    Assert.assertEquals("Schema should match expected ID assignment",
        TABLE_SCHEMA.asStruct(), table.schema().asStruct());
    Assert.assertNotNull("Should have a location", table.location());
    Assert.assertEquals("Should use requested partition spec", TABLE_SPEC, table.spec());
    Assert.assertEquals("Should use requested write order", TABLE_WRITE_ORDER, table.sortOrder());
    Assert.assertEquals("Table properties should be a superset of the requested properties",
        properties.entrySet(),
        Sets.intersection(properties.entrySet(), table.properties().entrySet()));
  }

  @Test
  public void testLoadMissingTable() {
    C catalog = catalog();

    TableIdentifier ident = TableIdentifier.of("ns", "table");

    Assert.assertFalse("Table should not exist", catalog.tableExists(ident));
    AssertHelpers.assertThrows("Should fail to load a nonexistent table",
        NoSuchTableException.class, ident.toString(),
        () -> catalog.loadTable(ident));
  }

  @Test
  public void testDropTable() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Assert.assertFalse("Table should not exist before create", catalog.tableExists(TABLE));

    catalog.buildTable(TABLE, SCHEMA).create();
    Assert.assertTrue("Table should exist after create", catalog.tableExists(TABLE));

    boolean dropped = catalog.dropTable(TABLE);
    Assert.assertTrue("Should drop a table that does exist", dropped);
    Assert.assertFalse("Table should not exist after drop", catalog.tableExists(TABLE));
  }

  @Test
  public void testDropMissingTable() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    TableIdentifier noSuchTableIdent = TableIdentifier.of(NS, "notable");
    Assert.assertFalse("Table should not exist", catalog.tableExists(noSuchTableIdent));
    Assert.assertFalse("Should not drop a table that does not exist", catalog.dropTable(noSuchTableIdent));
  }

  @Test
  public void testListTables() {
    C catalog = catalog();

    Namespace ns1 = Namespace.of("ns_1");
    Namespace ns2 = Namespace.of("ns_2");

    TableIdentifier ns1Table1 = TableIdentifier.of(ns1, "table_1");
    TableIdentifier ns1Table2 = TableIdentifier.of(ns1, "table_2");
    TableIdentifier ns2Table1 = TableIdentifier.of(ns2, "table_1");

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(ns1);
      catalog.createNamespace(ns2);
    }

    assertEmpty("Should not have tables in a new namespace, ns_1", catalog, ns1);
    assertEmpty("Should not have tables in a new namespace, ns_2", catalog, ns2);

    catalog.buildTable(ns1Table1, SCHEMA).create();

    Assert.assertEquals("Should contain ns_1.table_1 after create",
        ImmutableSet.of(ns1Table1), Sets.newHashSet(catalog.listTables(ns1)));

    catalog.buildTable(ns2Table1, SCHEMA).create();

    Assert.assertEquals("Should contain ns_2.table_1 after create",
        ImmutableSet.of(ns2Table1), Sets.newHashSet(catalog.listTables(ns2)));
    Assert.assertEquals("Should not show changes to ns_2 in ns_1",
        ImmutableSet.of(ns1Table1), Sets.newHashSet(catalog.listTables(ns1)));

    catalog.buildTable(ns1Table2, SCHEMA).create();

    Assert.assertEquals("Should not show changes to ns_1 in ns_2",
        ImmutableSet.of(ns2Table1), Sets.newHashSet(catalog.listTables(ns2)));
    Assert.assertEquals("Should contain ns_1.table_2 after create",
        ImmutableSet.of(ns1Table1, ns1Table2), Sets.newHashSet(catalog.listTables(ns1)));

    catalog.dropTable(ns1Table1);

    Assert.assertEquals("Should not show changes to ns_1 in ns_2",
        ImmutableSet.of(ns2Table1), Sets.newHashSet(catalog.listTables(ns2)));
    Assert.assertEquals("Should not contain ns_1.table_1 after drop",
        ImmutableSet.of(ns1Table2), Sets.newHashSet(catalog.listTables(ns1)));

    catalog.dropTable(ns1Table2);

    Assert.assertEquals("Should not show changes to ns_1 in ns_2",
        ImmutableSet.of(ns2Table1), Sets.newHashSet(catalog.listTables(ns2)));
    assertEmpty("Should not contain ns_1.table_2 after drop", catalog, ns1);

    catalog.dropTable(ns2Table1);
    assertEmpty("Should not contain ns_2.table_1 after drop", catalog, ns2);
  }

  @Test
  public void testUpdateTableSchema() {
    C catalog = catalog();

    Table table = catalog.buildTable(TABLE, SCHEMA).create();
    UpdateSchema update = table.updateSchema()
        .addColumn("new_col", Types.LongType.get());

    Schema expected = update.apply();

    update.commit();

    Table loaded = catalog.loadTable(TABLE);

    Assert.assertEquals("Loaded table should have expected schema", expected.asStruct(), loaded.schema().asStruct());
  }

  @Test
  public void testUUIDValidation() {
    C catalog = catalog();

    Table table = catalog.buildTable(TABLE, SCHEMA).create();
    UpdateSchema update = table.updateSchema()
        .addColumn("new_col", Types.LongType.get());

    Schema expected = new Schema(required(1, "some_id", Types.IntegerType.get()));
    Assert.assertTrue("Should successfully drop table", catalog.dropTable(TABLE));
    catalog.buildTable(TABLE, expected).create();

    String expectedMessage = supportsServerSideRetry() ? "Requirement failed: UUID does not match" : "Cannot commit";
    AssertHelpers.assertThrows("Should reject changes to tables that have been dropped and recreated",
        CommitFailedException.class, expectedMessage, update::commit);

    Table loaded = catalog.loadTable(TABLE);
    Assert.assertEquals("Loaded table should have expected schema", expected.asStruct(), loaded.schema().asStruct());
  }

  @Test
  public void testUpdateTableSchemaServerSideRetry() {
    Assume.assumeTrue("Schema update recovery is only supported with server-side retry", supportsServerSideRetry());
    C catalog = catalog();

    Table table = catalog.buildTable(TABLE, SCHEMA).create();

    UpdateSchema update = table.updateSchema()
        .addColumn("new_col", Types.LongType.get());
    Schema expected = update.apply();

    // update the spec concurrently so that the first update fails, but can succeed on retry
    catalog.loadTable(TABLE).updateSpec()
        .addField("shard", Expressions.bucket("id", 16))
        .commit();

    // commit the original update
    update.commit();

    Table loaded = catalog.loadTable(TABLE);
    Assert.assertEquals("Loaded table should have expected schema", expected.asStruct(), loaded.schema().asStruct());
  }

  @Test
  public void testUpdateTableSchemaConflict() {
    C catalog = catalog();

    Table table = catalog.buildTable(TABLE, SCHEMA).create();

    UpdateSchema update = table.updateSchema()
        .addColumn("new_col", Types.LongType.get());

    // update the schema concurrently so that the original update fails
    UpdateSchema concurrent = catalog.loadTable(TABLE).updateSchema()
        .addColumn("another_col", Types.StringType.get());
    Schema expected = concurrent.apply();
    concurrent.commit();

    // attempt to commit the original update
    String expectedMessage = supportsServerSideRetry() ? "Requirement failed: current schema changed" : "Cannot commit";
    AssertHelpers.assertThrows("Second schema update commit should fail because of a conflict",
        CommitFailedException.class, expectedMessage, update::commit);

    Table loaded = catalog.loadTable(TABLE);
    Assert.assertEquals("Loaded table should have expected schema", expected.asStruct(), loaded.schema().asStruct());
  }

  @Test
  public void testUpdateTableSpec() {
    C catalog = catalog();

    Table table = catalog.buildTable(TABLE, SCHEMA).create();
    UpdatePartitionSpec update = table.updateSpec()
        .addField("shard", Expressions.bucket("id", 16));

    PartitionSpec expected = update.apply();

    update.commit();

    Table loaded = catalog.loadTable(TABLE);

    // the spec ID may not match, so check equality of the fields
    Assert.assertEquals("Loaded table should have expected spec", expected.fields(), loaded.spec().fields());
  }

  @Test
  public void testUpdateTableSpecServerSideRetry() {
    Assume.assumeTrue("Spec update recovery is only supported with server-side retry", supportsServerSideRetry());
    C catalog = catalog();

    Table table = catalog.buildTable(TABLE, SCHEMA).create();

    UpdatePartitionSpec update = table.updateSpec()
        .addField("shard", Expressions.bucket("id", 16));
    PartitionSpec expected = update.apply();

    // update the schema concurrently so that the first update fails, but can succeed on retry
    catalog.loadTable(TABLE).updateSchema()
        .addColumn("another_col", Types.StringType.get())
        .commit();

    // commit the original update
    update.commit();

    Table loaded = catalog.loadTable(TABLE);

    // the spec ID may not match, so check equality of the fields
    Assert.assertEquals("Loaded table should have expected spec", expected.fields(), loaded.spec().fields());
  }

  @Test
  public void testUpdateTableSpecConflict() {
    C catalog = catalog();

    Table table = catalog.buildTable(TABLE, SCHEMA).create();

    UpdatePartitionSpec update = table.updateSpec()
        .addField("shard", Expressions.bucket("id", 16));

    // update the spec concurrently so that the original update fails
    UpdatePartitionSpec concurrent = catalog.loadTable(TABLE).updateSpec()
        .addField("shard", Expressions.truncate("id", 100));
    PartitionSpec expected = concurrent.apply();
    concurrent.commit();

    // attempt to commit the original update
    String expectedMessage = supportsServerSideRetry() ?
        "Requirement failed: default partition spec changed" : "Cannot commit";
    AssertHelpers.assertThrows("Second partition spec update commit should fail because of a conflict",
        CommitFailedException.class, expectedMessage, update::commit);

    Table loaded = catalog.loadTable(TABLE);

    // the spec ID may not match, so check equality of the fields
    Assert.assertEquals("Loaded table should have expected spec", expected.fields(), loaded.spec().fields());
  }

  @Test
  public void testUpdateTableSortOrder() {
    C catalog = catalog();

    Table table = catalog.buildTable(TABLE, SCHEMA).create();
    ReplaceSortOrder update = table.replaceSortOrder()
        .asc(Expressions.bucket("id", 16))
        .asc("id");

    SortOrder expected = update.apply();

    update.commit();

    Table loaded = catalog.loadTable(TABLE);

    // the sort order ID may not match, so check equality of the fields
    Assert.assertEquals("Loaded table should have expected order", expected.fields(), loaded.sortOrder().fields());
  }

  @Test
  public void testUpdateTableSortOrderServerSideRetry() {
    Assume.assumeTrue("Sort order update recovery is only supported with server-side retry", supportsServerSideRetry());
    C catalog = catalog();

    Table table = catalog.buildTable(TABLE, SCHEMA).create();

    ReplaceSortOrder update = table.replaceSortOrder()
        .asc(Expressions.bucket("id", 16))
        .asc("id");
    SortOrder expected = update.apply();

    // update the schema concurrently so that the first update fails, but can succeed on retry
    catalog.loadTable(TABLE).updateSchema()
        .addColumn("another_col", Types.StringType.get())
        .commit();

    // commit the original update
    update.commit();

    Table loaded = catalog.loadTable(TABLE);

    // the sort order ID may not match, so check equality of the fields
    Assert.assertEquals("Loaded table should have expected order", expected.fields(), loaded.sortOrder().fields());
  }

  @Test
  public void testAppend() throws IOException {
    C catalog = catalog();

    Table table = catalog.buildTable(TABLE, SCHEMA)
        .withPartitionSpec(SPEC)
        .create();

    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      Assert.assertFalse("Should contain no files", tasks.iterator().hasNext());
    }

    table.newFastAppend().appendFile(FILE_A).commit();

    assertFiles(table, FILE_A);
  }

  @Test
  public void testConcurrentAppendEmptyTable() throws IOException {
    C catalog = catalog();

    Table table = catalog.buildTable(TABLE, SCHEMA)
        .withPartitionSpec(SPEC)
        .create();

    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      Assert.assertFalse("Should contain no files", tasks.iterator().hasNext());
    }

    // create an uncommitted append
    AppendFiles append = table.newFastAppend().appendFile(FILE_A);
    append.apply(); // apply changes to eagerly write metadata

    catalog.loadTable(TABLE).newFastAppend().appendFile(FILE_B).commit();
    assertFiles(catalog.loadTable(TABLE), FILE_B);

    // the uncommitted append should retry and succeed
    append.commit();
    assertFiles(catalog.loadTable(TABLE), FILE_A, FILE_B);
  }

  @Test
  public void testConcurrentAppendNonEmptyTable() throws IOException {
    C catalog = catalog();

    Table table = catalog.buildTable(TABLE, SCHEMA)
        .withPartitionSpec(SPEC)
        .create();

    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      Assert.assertFalse("Should contain no files", tasks.iterator().hasNext());
    }

    // TODO: skip the initial refresh in FastAppend so that commits actually fail

    // create an initial snapshot
    catalog.loadTable(TABLE).newFastAppend().appendFile(FILE_C).commit();

    // create an uncommitted append
    AppendFiles append = table.newFastAppend().appendFile(FILE_A);
    append.apply(); // apply changes to eagerly write metadata

    catalog.loadTable(TABLE).newFastAppend().appendFile(FILE_B).commit();
    assertFiles(catalog.loadTable(TABLE), FILE_B, FILE_C);

    // the uncommitted append should retry and succeed
    append.commit();
    assertFiles(catalog.loadTable(TABLE), FILE_A, FILE_B, FILE_C);
  }

  public void assertFiles(Table table, DataFile... files) throws IOException {
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      List<CharSequence> paths = Streams.stream(tasks)
          .map(FileScanTask::file)
          .map(DataFile::path)
          .collect(Collectors.toList());
      Assert.assertEquals("Should contain expected number of data files", files.length, paths.size());
      Assert.assertEquals("Should contain correct file paths",
          CharSequenceSet.of(Iterables.transform(Arrays.asList(files), DataFile::path)),
          CharSequenceSet.of(paths));
    }
  }

  @Test
  public void testUpdateTransaction() {
    C catalog = catalog();

    Table table = catalog.buildTable(TABLE, SCHEMA).create();

    TableOperations ops = ((BaseTable) table).operations();
    ops.current().metadataFileLocation();

    Transaction transaction = table.newTransaction();

    UpdateSchema updateSchema = transaction.updateSchema()
        .addColumn("new_col", Types.LongType.get());
    Schema expectedSchema = updateSchema.apply();
    updateSchema.commit();

    UpdatePartitionSpec updateSpec = transaction.updateSpec()
        .addField("shard", Expressions.bucket("id", 16));
    PartitionSpec expectedSpec = updateSpec.apply();
    updateSpec.commit();

    transaction.commitTransaction();

    Table loaded = catalog.loadTable(TABLE);

    Assert.assertEquals("Loaded table should have expected schema",
        expectedSchema.asStruct(), loaded.schema().asStruct());
    Assert.assertEquals("Loaded table should have expected spec",
        expectedSpec.fields(), loaded.spec().fields());
    Assert.assertEquals("Table should have one previous metadata location",
        1, ops.current().previousFiles().size());
  }

  private static void assertEmpty(String context, Catalog catalog, Namespace ns) {
    try {
      Assert.assertEquals(context, 0, catalog.listTables(ns).size());
    } catch (NoSuchNamespaceException e) {
      // it is okay if the catalog throws NoSuchNamespaceException when it is empty
    }
  }

  private List<Namespace> concat(List<Namespace> starting, Namespace... additional) {
    List<Namespace> namespaces = Lists.newArrayList();
    namespaces.addAll(starting);
    namespaces.addAll(Arrays.asList(additional));
    return namespaces;
  }
}
