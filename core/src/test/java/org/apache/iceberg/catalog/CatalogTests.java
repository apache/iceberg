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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.required;

public abstract class CatalogTests<C extends Catalog & SupportsNamespaces> {
  private static final Namespace NS = Namespace.of("newdb");
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
      .withFileSizeInBytes(0)
      .withPartitionPath("id_bucket=0") // easy way to set partition data for now
      .withRecordCount(2) // needs at least one record or else metrics will filter it out
      .build();

  protected abstract C catalog();
  protected abstract boolean supportsNamespaceProperties();

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

  @Ignore
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

    Table table = catalog.buildTable(ident, SCHEMA).create();
    Assert.assertTrue("Table should exist", catalog.tableExists(ident));
    Assert.assertEquals("Schema should match expected ID assignment",
        TABLE_SCHEMA.asStruct(), table.schema().asStruct());
  }

  private List<Namespace> concat(List<Namespace> starting, Namespace... additional) {
    List<Namespace> namespaces = Lists.newArrayList();
    namespaces.addAll(starting);
    namespaces.addAll(Arrays.asList(additional));
    return namespaces;
  }
}
