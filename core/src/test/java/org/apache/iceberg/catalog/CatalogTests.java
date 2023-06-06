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

import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.FilesTable;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReachableFileUtil;
import org.apache.iceberg.ReplaceSortOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
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
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.CharSequenceSet;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

public abstract class CatalogTests<C extends Catalog & SupportsNamespaces> {
  private static final Namespace NS = Namespace.of("newdb");
  protected static final TableIdentifier TABLE = TableIdentifier.of(NS, "table");
  private static final TableIdentifier RENAMED_TABLE = TableIdentifier.of(NS, "table_renamed");

  // Schema passed to create tables
  protected static final Schema SCHEMA =
      new Schema(
          required(3, "id", Types.IntegerType.get(), "unique ID"),
          required(4, "data", Types.StringType.get()));

  // This is the actual schema for the table, with column IDs reassigned
  private static final Schema TABLE_SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get(), "unique ID"),
          required(2, "data", Types.StringType.get()));

  // This is the actual schema for the table, with column IDs reassigned
  private static final Schema REPLACE_SCHEMA =
      new Schema(
          required(2, "id", Types.IntegerType.get(), "unique ID"),
          required(3, "data", Types.StringType.get()));

  // another schema that is not the same
  private static final Schema OTHER_SCHEMA =
      new Schema(required(1, "some_id", Types.IntegerType.get()));

  // Partition spec used to create tables
  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).bucket("id", 16).build();

  private static final PartitionSpec TABLE_SPEC =
      PartitionSpec.builderFor(TABLE_SCHEMA).bucket("id", 16).build();

  private static final PartitionSpec REPLACE_SPEC =
      PartitionSpec.builderFor(REPLACE_SCHEMA).bucket("id", 16).withSpecId(1).build();

  // Partition spec used to create tables
  static final SortOrder WRITE_ORDER =
      SortOrder.builderFor(SCHEMA).asc(Expressions.bucket("id", 16)).asc("id").build();

  static final SortOrder TABLE_WRITE_ORDER =
      SortOrder.builderFor(TABLE_SCHEMA).asc(Expressions.bucket("id", 16)).asc("id").build();

  static final SortOrder REPLACE_WRITE_ORDER =
      SortOrder.builderFor(REPLACE_SCHEMA).asc(Expressions.bucket("id", 16)).asc("id").build();

  static final DataFile FILE_A =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-a.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("id_bucket=0") // easy way to set partition data for now
          .withRecordCount(2) // needs at least one record or else metrics will filter it out
          .build();

  static final DataFile FILE_B =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-b.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("id_bucket=1") // easy way to set partition data for now
          .withRecordCount(2) // needs at least one record or else metrics will filter it out
          .build();

  static final DataFile FILE_C =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-c.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("id_bucket=2") // easy way to set partition data for now
          .withRecordCount(2) // needs at least one record or else metrics will filter it out
          .build();

  protected abstract C catalog();

  protected boolean supportsNamespaceProperties() {
    return true;
  }

  protected boolean supportsNestedNamespaces() {
    return false;
  }

  protected boolean requiresNamespaceCreate() {
    return false;
  }

  protected boolean supportsServerSideRetry() {
    return false;
  }

  protected boolean overridesRequestedLocation() {
    return false;
  }

  protected boolean supportsNamesWithSlashes() {
    return true;
  }

  @Test
  public void testCreateNamespace() {
    C catalog = catalog();

    Assert.assertFalse("Namespace should not exist", catalog.namespaceExists(NS));

    catalog.createNamespace(NS);
    Assert.assertTrue(
        "Catalog should have the created namespace", catalog.listNamespaces().contains(NS));
    Assert.assertTrue("Namespace should exist", catalog.namespaceExists(NS));
  }

  @Test
  public void testCreateExistingNamespace() {
    C catalog = catalog();

    Assert.assertFalse("Namespace should not exist", catalog.namespaceExists(NS));

    catalog.createNamespace(NS);
    Assert.assertTrue("Namespace should exist", catalog.namespaceExists(NS));

    Assertions.assertThatThrownBy(() -> catalog.createNamespace(NS))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("Namespace already exists");

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
    Assert.assertEquals(
        "Create properties should be a subset of returned properties",
        createProps.entrySet(),
        Sets.intersection(createProps.entrySet(), props.entrySet()));
  }

  @Test
  public void testLoadNamespaceMetadata() {
    C catalog = catalog();

    Assert.assertFalse("Namespace should not exist", catalog.namespaceExists(NS));

    Assertions.assertThatThrownBy(() -> catalog.loadNamespaceMetadata(NS))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessageStartingWith("Namespace does not exist: newdb");

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
    Assert.assertEquals(
        "Set properties should be a subset of returned properties",
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
    Assert.assertEquals(
        "Set properties should be a subset of returned properties",
        initialProperties.entrySet(),
        Sets.intersection(initialProperties.entrySet(), actualProperties.entrySet()));

    Map<String, String> updatedProperties = ImmutableMap.of("owner", "newuser");

    catalog.setProperties(NS, updatedProperties);

    Map<String, String> finalProperties = catalog.loadNamespaceMetadata(NS);
    Assert.assertEquals(
        "Updated properties should be a subset of returned properties",
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
    Assert.assertEquals(
        "Set properties should be a subset of returned properties",
        initialProperties.entrySet(),
        Sets.intersection(initialProperties.entrySet(), actualProperties.entrySet()));

    Map<String, String> updatedProperties =
        ImmutableMap.of("owner", "newuser", "last-modified-at", "now");

    catalog.setProperties(NS, updatedProperties);

    Map<String, String> finalProperties = catalog.loadNamespaceMetadata(NS);
    Assert.assertEquals(
        "Updated properties should be a subset of returned properties",
        updatedProperties.entrySet(),
        Sets.intersection(updatedProperties.entrySet(), finalProperties.entrySet()));
  }

  @Test
  public void testSetNamespacePropertiesNamespaceDoesNotExist() {
    Assume.assumeTrue(supportsNamespaceProperties());

    C catalog = catalog();

    Assertions.assertThatThrownBy(() -> catalog.setProperties(NS, ImmutableMap.of("test", "value")))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessageStartingWith("Namespace does not exist: newdb");
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
    Assert.assertFalse(
        "Should not contain deleted property key", actualProperties.containsKey("created-at"));
    Assert.assertEquals(
        "Expected properties should be a subset of returned properties",
        ImmutableMap.of("owner", "user").entrySet(),
        Sets.intersection(properties.entrySet(), actualProperties.entrySet()));
  }

  @Test
  public void testRemoveNamespacePropertiesNamespaceDoesNotExist() {
    Assume.assumeTrue(supportsNamespaceProperties());

    C catalog = catalog();

    Assertions.assertThatThrownBy(() -> catalog.removeProperties(NS, ImmutableSet.of("a", "b")))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessageStartingWith("Namespace does not exist: newdb");
  }

  @Test
  public void testDropNamespace() {
    C catalog = catalog();

    Assert.assertFalse("Namespace should not exist", catalog.namespaceExists(NS));

    catalog.createNamespace(NS);
    Assert.assertTrue("Namespace should exist", catalog.namespaceExists(NS));

    Assert.assertTrue(
        "Dropping an existing namespace should return true", catalog.dropNamespace(NS));
    Assert.assertFalse("Namespace should not exist", catalog.namespaceExists(NS));
  }

  @Test
  public void testDropNonexistentNamespace() {
    C catalog = catalog();

    Assert.assertFalse(
        "Dropping a nonexistent namespace should return false", catalog.dropNamespace(NS));
  }

  @Test
  public void testListNamespaces() {
    C catalog = catalog();
    // the catalog may automatically create a default namespace
    List<Namespace> starting = catalog.listNamespaces();

    Namespace ns1 = Namespace.of("newdb_1");
    Namespace ns2 = Namespace.of("newdb_2");

    catalog.createNamespace(ns1);
    Assertions.assertThat(catalog.listNamespaces())
        .withFailMessage("Should include newdb_1")
        .hasSameElementsAs(concat(starting, ns1));

    catalog.createNamespace(ns2);
    Assertions.assertThat(catalog.listNamespaces())
        .withFailMessage("Should include newdb_1 and newdb_2")
        .hasSameElementsAs(concat(starting, ns1, ns2));

    catalog.dropNamespace(ns1);
    Assertions.assertThat(catalog.listNamespaces())
        .withFailMessage("Should include newdb_2, not newdb_1")
        .hasSameElementsAs(concat(starting, ns2));

    catalog.dropNamespace(ns2);
    Assert.assertTrue(
        "Should include only starting namespaces", catalog.listNamespaces().containsAll(starting));
  }

  @Test
  public void testListNestedNamespaces() {
    Assume.assumeTrue(
        "Only valid when the catalog supports nested namespaces", supportsNestedNamespaces());

    C catalog = catalog();

    // the catalog may automatically create a default namespace
    List<Namespace> starting = catalog.listNamespaces();

    Namespace parent = Namespace.of("parent");
    Namespace child1 = Namespace.of("parent", "child1");
    Namespace child2 = Namespace.of("parent", "child2");

    catalog.createNamespace(parent);
    Assertions.assertThat(catalog.listNamespaces())
        .withFailMessage("Should include parent")
        .hasSameElementsAs(concat(starting, parent));

    Assertions.assertThat(catalog.listNamespaces(parent))
        .withFailMessage("Should have no children in newly created parent namespace")
        .isEmpty();

    catalog.createNamespace(child1);
    Assertions.assertThat(catalog.listNamespaces(parent))
        .withFailMessage("Should include child1")
        .hasSameElementsAs(ImmutableList.of(child1));

    catalog.createNamespace(child2);
    Assertions.assertThat(catalog.listNamespaces(parent))
        .withFailMessage("Should include child1 and child2")
        .hasSameElementsAs(ImmutableList.of(child1, child2));

    Assertions.assertThat(catalog.listNamespaces())
        .withFailMessage("Should not change listing the root")
        .hasSameElementsAs(concat(starting, parent));

    catalog.dropNamespace(child1);
    Assertions.assertThat(catalog.listNamespaces(parent))
        .withFailMessage("Should include only child2")
        .hasSameElementsAs(ImmutableList.of(child2));

    catalog.dropNamespace(child2);
    Assertions.assertThat(catalog.listNamespaces(parent))
        .withFailMessage("Should be empty")
        .isEmpty();
  }

  @Test
  public void testNamespaceWithSlash() {
    Assume.assumeTrue(supportsNamesWithSlashes());

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

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(ident.namespace());
    }

    Table table = catalog.buildTable(ident, SCHEMA).create();
    Assert.assertTrue("Table should exist", catalog.tableExists(ident));

    // validate table settings
    Assert.assertEquals(
        "Table name should report its full name", catalog.name() + "." + ident, table.name());
    Assert.assertEquals(
        "Schema should match expected ID assignment",
        TABLE_SCHEMA.asStruct(),
        table.schema().asStruct());
    Assert.assertNotNull("Should have a location", table.location());
    Assert.assertTrue("Should be unpartitioned", table.spec().isUnpartitioned());
    Assert.assertTrue("Should be unsorted", table.sortOrder().isUnsorted());
    Assert.assertNotNull("Should have table properties", table.properties());
  }

  @Test
  public void testTableNameWithSlash() {
    Assume.assumeTrue(supportsNamesWithSlashes());

    C catalog = catalog();

    TableIdentifier ident = TableIdentifier.of("ns", "tab/le");
    if (requiresNamespaceCreate()) {
      catalog.createNamespace(Namespace.of("ns"));
    }

    Assert.assertFalse("Table should not exist", catalog.tableExists(ident));

    catalog.buildTable(ident, SCHEMA).create();
    Assert.assertTrue("Table should exist", catalog.tableExists(ident));

    Table loaded = catalog.loadTable(ident);
    Assert.assertEquals(
        "Schema should match expected ID assignment",
        TABLE_SCHEMA.asStruct(),
        loaded.schema().asStruct());

    catalog.dropTable(ident);

    Assert.assertFalse("Table should not exist", catalog.tableExists(ident));
  }

  @Test
  public void testTableNameWithDot() {
    C catalog = catalog();

    TableIdentifier ident = TableIdentifier.of("ns", "ta.ble");
    if (requiresNamespaceCreate()) {
      catalog.createNamespace(Namespace.of("ns"));
    }

    Assert.assertFalse("Table should not exist", catalog.tableExists(ident));

    catalog.buildTable(ident, SCHEMA).create();
    Assert.assertTrue("Table should exist", catalog.tableExists(ident));

    Table loaded = catalog.loadTable(ident);
    Assert.assertEquals(
        "Schema should match expected ID assignment",
        TABLE_SCHEMA.asStruct(),
        loaded.schema().asStruct());

    catalog.dropTable(ident);

    Assert.assertFalse("Table should not exist", catalog.tableExists(ident));
  }

  @Test
  public void testBasicCreateTableThatAlreadyExists() {
    C catalog = catalog();

    TableIdentifier ident = TableIdentifier.of("ns", "table");

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(ident.namespace());
    }

    Assert.assertFalse("Table should not exist", catalog.tableExists(ident));

    catalog.buildTable(ident, SCHEMA).create();
    Assert.assertTrue("Table should exist", catalog.tableExists(ident));

    Assertions.assertThatThrownBy(() -> catalog.buildTable(ident, OTHER_SCHEMA).create())
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageStartingWith("Table already exists: ns.table");

    Table table = catalog.loadTable(ident);
    Assert.assertEquals(
        "Schema should match original table schema",
        TABLE_SCHEMA.asStruct(),
        table.schema().asStruct());
  }

  @Test
  public void testCompleteCreateTable() {
    C catalog = catalog();

    TableIdentifier ident = TableIdentifier.of("ns", "table");

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(ident.namespace());
    }

    Assert.assertFalse("Table should not exist", catalog.tableExists(ident));

    Map<String, String> properties =
        ImmutableMap.of("user", "someone", "created-at", "2022-02-25T00:38:19");
    Table table =
        catalog
            .buildTable(ident, SCHEMA)
            .withLocation("file:/tmp/ns/table")
            .withPartitionSpec(SPEC)
            .withSortOrder(WRITE_ORDER)
            .withProperties(properties)
            .create();

    // validate table settings
    Assert.assertEquals(
        "Table name should report its full name", catalog.name() + "." + ident, table.name());
    Assert.assertTrue("Table should exist", catalog.tableExists(ident));
    Assert.assertEquals(
        "Schema should match expected ID assignment",
        TABLE_SCHEMA.asStruct(),
        table.schema().asStruct());
    Assert.assertNotNull("Should have a location", table.location());
    Assert.assertEquals("Should use requested partition spec", TABLE_SPEC, table.spec());
    Assert.assertEquals("Should use requested write order", TABLE_WRITE_ORDER, table.sortOrder());
    Assert.assertEquals(
        "Table properties should be a superset of the requested properties",
        properties.entrySet(),
        Sets.intersection(properties.entrySet(), table.properties().entrySet()));
  }

  @Test
  public void testLoadTable() {
    C catalog = catalog();

    TableIdentifier ident = TableIdentifier.of("ns", "table");

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(ident.namespace());
    }

    Assert.assertFalse("Table should not exist", catalog.tableExists(ident));

    Map<String, String> properties =
        ImmutableMap.of("user", "someone", "created-at", "2022-02-25T00:38:19");
    catalog
        .buildTable(ident, SCHEMA)
        .withLocation("file:/tmp/ns/table")
        .withPartitionSpec(SPEC)
        .withSortOrder(WRITE_ORDER)
        .withProperties(properties)
        .create();
    Assert.assertTrue("Table should exist", catalog.tableExists(ident));

    Table table = catalog.loadTable(ident);
    // validate table settings
    Assert.assertEquals(
        "Table name should report its full name", catalog.name() + "." + ident, table.name());
    Assert.assertTrue("Table should exist", catalog.tableExists(ident));
    Assert.assertEquals(
        "Schema should match expected ID assignment",
        TABLE_SCHEMA.asStruct(),
        table.schema().asStruct());
    Assert.assertNotNull("Should have a location", table.location());
    Assert.assertEquals("Should use requested partition spec", TABLE_SPEC, table.spec());
    Assert.assertEquals("Should use requested write order", TABLE_WRITE_ORDER, table.sortOrder());
    Assert.assertEquals(
        "Table properties should be a superset of the requested properties",
        properties.entrySet(),
        Sets.intersection(properties.entrySet(), table.properties().entrySet()));
  }

  @Test
  public void testLoadMetadataTable() {
    C catalog = catalog();

    TableIdentifier tableIdent = TableIdentifier.of("ns", "table");
    TableIdentifier metaIdent = TableIdentifier.of("ns", "table", "files");

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(tableIdent.namespace());
    }

    catalog.buildTable(tableIdent, SCHEMA).create();

    Table table = catalog.loadTable(metaIdent);
    Assertions.assertThat(table).isNotNull();
    Assertions.assertThat(table).isInstanceOf(FilesTable.class);

    // check that the table metadata can be refreshed
    table.refresh();

    Assertions.assertThat(table.name()).isEqualTo(catalog.name() + "." + metaIdent);
  }

  @Test
  public void testLoadMissingTable() {
    C catalog = catalog();

    TableIdentifier ident = TableIdentifier.of("ns", "table");

    Assert.assertFalse("Table should not exist", catalog.tableExists(ident));
    Assertions.assertThatThrownBy(() -> catalog.loadTable(ident))
        .isInstanceOf(NoSuchTableException.class)
        .hasMessageStartingWith("Table does not exist: ns.table");
  }

  @Test
  public void testRenameTable() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Assert.assertFalse("Source table should not exist before create", catalog.tableExists(TABLE));

    catalog.buildTable(TABLE, SCHEMA).create();
    Assert.assertTrue("Table should exist after create", catalog.tableExists(TABLE));

    Assert.assertFalse(
        "Destination table should not exist before rename", catalog.tableExists(RENAMED_TABLE));

    catalog.renameTable(TABLE, RENAMED_TABLE);
    Assert.assertTrue("Table should exist with new name", catalog.tableExists(RENAMED_TABLE));
    Assert.assertFalse("Original table should no longer exist", catalog.tableExists(TABLE));

    catalog.dropTable(RENAMED_TABLE);
    assertEmpty("Should not contain table after drop", catalog, NS);
  }

  @Test
  public void testRenameTableMissingSourceTable() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Assert.assertFalse("Source table should not exist before rename", catalog.tableExists(TABLE));
    Assert.assertFalse(
        "Destination table should not exist before rename", catalog.tableExists(RENAMED_TABLE));

    Assertions.assertThatThrownBy(() -> catalog.renameTable(TABLE, RENAMED_TABLE))
        .isInstanceOf(NoSuchTableException.class)
        .hasMessageContaining("Table does not exist");

    Assert.assertFalse(
        "Destination table should not exist after failed rename",
        catalog.tableExists(RENAMED_TABLE));
  }

  @Test
  public void testRenameTableDestinationTableAlreadyExists() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Assert.assertFalse("Source table should not exist before create", catalog.tableExists(TABLE));
    catalog.buildTable(TABLE, SCHEMA).create();
    Assert.assertTrue("Source table should exist after create", catalog.tableExists(TABLE));

    Assert.assertFalse(
        "Destination table should not exist before create", catalog.tableExists(RENAMED_TABLE));
    catalog.buildTable(RENAMED_TABLE, SCHEMA).create();
    Assert.assertTrue(
        "Destination table should exist after create", catalog.tableExists(RENAMED_TABLE));

    Assertions.assertThatThrownBy(() -> catalog.renameTable(TABLE, RENAMED_TABLE))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("Table already exists");

    Assert.assertTrue(
        "Source table should still exist after failed rename", catalog.tableExists(TABLE));
    Assert.assertTrue(
        "Destination table should still exist after failed rename",
        catalog.tableExists(RENAMED_TABLE));

    String sourceTableUUID =
        ((HasTableOperations) catalog.loadTable(TABLE)).operations().current().uuid();
    String destinationTableUUID =
        ((HasTableOperations) catalog.loadTable(RENAMED_TABLE)).operations().current().uuid();
    Assert.assertNotEquals(
        "Source and destination table should remain distinct after failed rename",
        sourceTableUUID,
        destinationTableUUID);
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
  public void testDropTableWithPurge() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Assert.assertFalse("Table should not exist before create", catalog.tableExists(TABLE));

    catalog.buildTable(TABLE, SCHEMA).create();
    Assert.assertTrue("Table should exist after create", catalog.tableExists(TABLE));

    boolean dropped = catalog.dropTable(TABLE, true);
    Assert.assertTrue("Should drop a table that does exist", dropped);
    Assert.assertFalse("Table should not exist after drop", catalog.tableExists(TABLE));
  }

  @Test
  public void testDropTableWithoutPurge() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Assert.assertFalse("Table should not exist before create", catalog.tableExists(TABLE));

    Table table = catalog.buildTable(TABLE, SCHEMA).create();
    Assert.assertTrue("Table should exist after create", catalog.tableExists(TABLE));
    Set<String> actualMetadataFileLocations = ReachableFileUtil.metadataFileLocations(table, false);

    boolean dropped = catalog.dropTable(TABLE, false);
    Assert.assertTrue("Should drop a table that does exist", dropped);
    Assert.assertFalse("Table should not exist after drop", catalog.tableExists(TABLE));
    Set<String> expectedMetadataFileLocations =
        ReachableFileUtil.metadataFileLocations(table, false);
    Assertions.assertThat(actualMetadataFileLocations)
        .hasSameElementsAs(expectedMetadataFileLocations)
        .hasSize(1)
        .as("Should have one metadata file");
  }

  @Test
  public void testDropMissingTable() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    TableIdentifier noSuchTableIdent = TableIdentifier.of(NS, "notable");
    Assert.assertFalse("Table should not exist", catalog.tableExists(noSuchTableIdent));
    Assert.assertFalse(
        "Should not drop a table that does not exist", catalog.dropTable(noSuchTableIdent));
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

    Assert.assertEquals(
        "Should contain ns_1.table_1 after create",
        ImmutableSet.of(ns1Table1),
        Sets.newHashSet(catalog.listTables(ns1)));

    catalog.buildTable(ns2Table1, SCHEMA).create();

    Assert.assertEquals(
        "Should contain ns_2.table_1 after create",
        ImmutableSet.of(ns2Table1),
        Sets.newHashSet(catalog.listTables(ns2)));
    Assert.assertEquals(
        "Should not show changes to ns_2 in ns_1",
        ImmutableSet.of(ns1Table1),
        Sets.newHashSet(catalog.listTables(ns1)));

    catalog.buildTable(ns1Table2, SCHEMA).create();

    Assert.assertEquals(
        "Should not show changes to ns_1 in ns_2",
        ImmutableSet.of(ns2Table1),
        Sets.newHashSet(catalog.listTables(ns2)));
    Assert.assertEquals(
        "Should contain ns_1.table_2 after create",
        ImmutableSet.of(ns1Table1, ns1Table2),
        Sets.newHashSet(catalog.listTables(ns1)));

    catalog.dropTable(ns1Table1);

    Assert.assertEquals(
        "Should not show changes to ns_1 in ns_2",
        ImmutableSet.of(ns2Table1),
        Sets.newHashSet(catalog.listTables(ns2)));
    Assert.assertEquals(
        "Should not contain ns_1.table_1 after drop",
        ImmutableSet.of(ns1Table2),
        Sets.newHashSet(catalog.listTables(ns1)));

    catalog.dropTable(ns1Table2);

    Assert.assertEquals(
        "Should not show changes to ns_1 in ns_2",
        ImmutableSet.of(ns2Table1),
        Sets.newHashSet(catalog.listTables(ns2)));
    assertEmpty("Should not contain ns_1.table_2 after drop", catalog, ns1);

    catalog.dropTable(ns2Table1);
    assertEmpty("Should not contain ns_2.table_1 after drop", catalog, ns2);
  }

  @Test
  public void testUpdateTableSchema() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Table table = catalog.buildTable(TABLE, SCHEMA).create();
    UpdateSchema update = table.updateSchema().addColumn("new_col", Types.LongType.get());

    Schema expected = update.apply();

    update.commit();

    Table loaded = catalog.loadTable(TABLE);

    Assert.assertEquals(
        "Loaded table should have expected schema",
        expected.asStruct(),
        loaded.schema().asStruct());
  }

  @Test
  public void testUUIDValidation() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Table table = catalog.buildTable(TABLE, SCHEMA).create();
    UpdateSchema update = table.updateSchema().addColumn("new_col", Types.LongType.get());

    Assert.assertTrue("Should successfully drop table", catalog.dropTable(TABLE));
    catalog.buildTable(TABLE, OTHER_SCHEMA).create();

    String expectedMessage =
        supportsServerSideRetry() ? "Requirement failed: UUID does not match" : "Cannot commit";
    Assertions.assertThatThrownBy(update::commit)
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining(expectedMessage);

    Table loaded = catalog.loadTable(TABLE);
    Assert.assertEquals(
        "Loaded table should have expected schema",
        OTHER_SCHEMA.asStruct(),
        loaded.schema().asStruct());
  }

  @Test
  public void testUpdateTableSchemaServerSideRetry() {
    Assume.assumeTrue(
        "Schema update recovery is only supported with server-side retry",
        supportsServerSideRetry());
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Table table = catalog.buildTable(TABLE, SCHEMA).create();

    UpdateSchema update = table.updateSchema().addColumn("new_col", Types.LongType.get());
    Schema expected = update.apply();

    // update the spec concurrently so that the first update fails, but can succeed on retry
    catalog.loadTable(TABLE).updateSpec().addField("shard", Expressions.bucket("id", 16)).commit();

    // commit the original update
    update.commit();

    Table loaded = catalog.loadTable(TABLE);
    Assert.assertEquals(
        "Loaded table should have expected schema",
        expected.asStruct(),
        loaded.schema().asStruct());
  }

  @Test
  public void testUpdateTableSchemaConflict() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Table table = catalog.buildTable(TABLE, SCHEMA).create();

    UpdateSchema update = table.updateSchema().addColumn("new_col", Types.LongType.get());

    // update the schema concurrently so that the original update fails
    UpdateSchema concurrent = catalog.loadTable(TABLE).updateSchema().deleteColumn("data");
    Schema expected = concurrent.apply();
    concurrent.commit();

    // attempt to commit the original update
    String expectedMessage =
        supportsServerSideRetry() ? "Requirement failed: current schema changed" : "Cannot commit";
    Assertions.assertThatThrownBy(update::commit)
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining(expectedMessage);

    Table loaded = catalog.loadTable(TABLE);
    Assert.assertEquals(
        "Loaded table should have expected schema",
        expected.asStruct(),
        loaded.schema().asStruct());
  }

  @Test
  public void testUpdateTableSchemaAssignmentConflict() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Table table = catalog.buildTable(TABLE, SCHEMA).create();

    UpdateSchema update = table.updateSchema().addColumn("new_col", Types.LongType.get());

    // update the schema concurrently so that the original update fails
    UpdateSchema concurrent =
        catalog.loadTable(TABLE).updateSchema().addColumn("another_col", Types.StringType.get());
    Schema expected = concurrent.apply();
    concurrent.commit();

    // attempt to commit the original update
    String expectedMessage =
        supportsServerSideRetry()
            ? "Requirement failed: last assigned field id changed"
            : "Cannot commit";
    Assertions.assertThatThrownBy(update::commit)
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining(expectedMessage);

    Table loaded = catalog.loadTable(TABLE);
    Assert.assertEquals(
        "Loaded table should have expected schema",
        expected.asStruct(),
        loaded.schema().asStruct());
  }

  @Test
  public void testUpdateTableSchemaThenRevert() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Table table = catalog.buildTable(TABLE, SCHEMA).create();

    table
        .updateSchema()
        .addColumn("col1", Types.StringType.get())
        .addColumn("col2", Types.StringType.get())
        .addColumn("col3", Types.StringType.get())
        .commit();

    table.updateSchema().deleteColumn("col1").deleteColumn("col2").deleteColumn("col3").commit();

    Assert.assertEquals(
        "Loaded table should have expected schema",
        TABLE_SCHEMA.asStruct(),
        table.schema().asStruct());
  }

  @Test
  public void testUpdateTableSpec() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Table table = catalog.buildTable(TABLE, SCHEMA).create();
    UpdatePartitionSpec update = table.updateSpec().addField("shard", Expressions.bucket("id", 16));

    PartitionSpec expected = update.apply();

    update.commit();

    Table loaded = catalog.loadTable(TABLE);

    // the spec ID may not match, so check equality of the fields
    Assert.assertEquals(
        "Loaded table should have expected spec", expected.fields(), loaded.spec().fields());
  }

  @Test
  public void testUpdateTableSpecServerSideRetry() {
    Assume.assumeTrue(
        "Spec update recovery is only supported with server-side retry", supportsServerSideRetry());
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Table table = catalog.buildTable(TABLE, SCHEMA).create();

    UpdatePartitionSpec update = table.updateSpec().addField("shard", Expressions.bucket("id", 16));
    PartitionSpec expected = update.apply();

    // update the schema concurrently so that the first update fails, but can succeed on retry
    catalog
        .loadTable(TABLE)
        .updateSchema()
        .addColumn("another_col", Types.StringType.get())
        .commit();

    // commit the original update
    update.commit();

    Table loaded = catalog.loadTable(TABLE);

    // the spec ID may not match, so check equality of the fields
    Assert.assertEquals(
        "Loaded table should have expected spec", expected.fields(), loaded.spec().fields());
  }

  @Test
  public void testUpdateTableSpecConflict() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Table table = catalog.buildTable(TABLE, SCHEMA).withPartitionSpec(SPEC).create();

    UpdatePartitionSpec update =
        table.updateSpec().addField("shard", Expressions.bucket("data", 16));

    // update the spec concurrently so that the original update fails
    UpdatePartitionSpec concurrent =
        catalog.loadTable(TABLE).updateSpec().removeField(Expressions.bucket("id", 16));
    PartitionSpec expected = concurrent.apply();
    concurrent.commit();

    // attempt to commit the original update
    String expectedMessage =
        supportsServerSideRetry()
            ? "Requirement failed: default partition spec changed"
            : "Cannot commit";
    Assertions.assertThatThrownBy(update::commit)
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining(expectedMessage);

    Table loaded = catalog.loadTable(TABLE);

    // the spec ID may not match, so check equality of the fields
    Assert.assertEquals(
        "Loaded table should have expected spec", expected.fields(), loaded.spec().fields());
  }

  @Test
  public void testUpdateTableAssignmentSpecConflict() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Table table = catalog.buildTable(TABLE, SCHEMA).create();

    UpdatePartitionSpec update = table.updateSpec().addField("shard", Expressions.bucket("id", 16));

    // update the spec concurrently so that the original update fails
    UpdatePartitionSpec concurrent =
        catalog.loadTable(TABLE).updateSpec().addField("shard", Expressions.truncate("id", 100));
    PartitionSpec expected = concurrent.apply();
    concurrent.commit();

    // attempt to commit the original update
    String expectedMessage =
        supportsServerSideRetry()
            ? "Requirement failed: last assigned partition id changed"
            : "Cannot commit";
    Assertions.assertThatThrownBy(update::commit)
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining(expectedMessage);

    Table loaded = catalog.loadTable(TABLE);

    // the spec ID may not match, so check equality of the fields
    Assert.assertEquals(
        "Loaded table should have expected spec", expected.fields(), loaded.spec().fields());
  }

  @Test
  public void testUpdateTableSpecThenRevert() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    // create a v2 table. otherwise the spec update would produce a different spec with a void
    // partition field
    Table table =
        catalog
            .buildTable(TABLE, SCHEMA)
            .withPartitionSpec(SPEC)
            .withProperty("format-version", "2")
            .create();
    Assert.assertEquals(
        "Should be a v2 table", 2, ((BaseTable) table).operations().current().formatVersion());

    table.updateSpec().addField("id").commit();

    table.updateSpec().removeField("id").commit();

    Assert.assertEquals("Loaded table should have expected spec", TABLE_SPEC, table.spec());
  }

  @Test
  public void testUpdateTableSortOrder() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Table table = catalog.buildTable(TABLE, SCHEMA).create();
    ReplaceSortOrder update = table.replaceSortOrder().asc(Expressions.bucket("id", 16)).asc("id");

    SortOrder expected = update.apply();

    update.commit();

    Table loaded = catalog.loadTable(TABLE);

    // the sort order ID may not match, so check equality of the fields
    Assert.assertEquals(
        "Loaded table should have expected order", expected.fields(), loaded.sortOrder().fields());
  }

  @Test
  public void testUpdateTableSortOrderServerSideRetry() {
    Assume.assumeTrue(
        "Sort order update recovery is only supported with server-side retry",
        supportsServerSideRetry());
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Table table = catalog.buildTable(TABLE, SCHEMA).create();

    ReplaceSortOrder update = table.replaceSortOrder().asc(Expressions.bucket("id", 16)).asc("id");
    SortOrder expected = update.apply();

    // update the schema concurrently so that the first update fails, but can succeed on retry
    catalog
        .loadTable(TABLE)
        .updateSchema()
        .addColumn("another_col", Types.StringType.get())
        .commit();

    // commit the original update
    update.commit();

    Table loaded = catalog.loadTable(TABLE);

    // the sort order ID may not match, so check equality of the fields
    Assert.assertEquals(
        "Loaded table should have expected order", expected.fields(), loaded.sortOrder().fields());
  }

  @Test
  public void testUpdateTableOrderThenRevert() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Table table = catalog.buildTable(TABLE, SCHEMA).withSortOrder(WRITE_ORDER).create();

    table.replaceSortOrder().asc("id").commit();

    table.replaceSortOrder().asc(Expressions.bucket("id", 16)).asc("id").commit();

    Assert.assertEquals(
        "Loaded table should have expected order", TABLE_WRITE_ORDER, table.sortOrder());
  }

  @Test
  public void testAppend() throws IOException {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Table table = catalog.buildTable(TABLE, SCHEMA).withPartitionSpec(SPEC).create();

    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      Assert.assertFalse("Should contain no files", tasks.iterator().hasNext());
    }

    table.newFastAppend().appendFile(FILE_A).commit();

    assertFiles(table, FILE_A);
  }

  @Test
  public void testConcurrentAppendEmptyTable() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Table table = catalog.buildTable(TABLE, SCHEMA).withPartitionSpec(SPEC).create();

    assertNoFiles(table);

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
  public void testConcurrentAppendNonEmptyTable() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Table table = catalog.buildTable(TABLE, SCHEMA).withPartitionSpec(SPEC).create();

    assertNoFiles(table);

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

  @Test
  public void testUpdateTransaction() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Table table = catalog.buildTable(TABLE, SCHEMA).create();

    Transaction transaction = table.newTransaction();

    UpdateSchema updateSchema =
        transaction.updateSchema().addColumn("new_col", Types.LongType.get());
    Schema expectedSchema = updateSchema.apply();
    updateSchema.commit();

    UpdatePartitionSpec updateSpec =
        transaction.updateSpec().addField("shard", Expressions.bucket("id", 16));
    PartitionSpec expectedSpec = updateSpec.apply();
    updateSpec.commit();

    transaction.commitTransaction();

    Table loaded = catalog.loadTable(TABLE);

    Assert.assertEquals(
        "Loaded table should have expected schema",
        expectedSchema.asStruct(),
        loaded.schema().asStruct());
    Assert.assertEquals(
        "Loaded table should have expected spec", expectedSpec.fields(), loaded.spec().fields());

    assertPreviousMetadataFileCount(loaded, 1);
  }

  @Test
  public void testCreateTransaction() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Transaction create = catalog.buildTable(TABLE, SCHEMA).createTransaction();

    Assert.assertFalse(
        "Table should not exist after createTransaction", catalog.tableExists(TABLE));

    create.newFastAppend().appendFile(FILE_A).commit();

    Assert.assertFalse("Table should not exist after append commit", catalog.tableExists(TABLE));

    create.commitTransaction();

    Assert.assertTrue("Table should exist after append commit", catalog.tableExists(TABLE));
    Table table = catalog.loadTable(TABLE);
    assertFiles(table, FILE_A);
    assertPreviousMetadataFileCount(table, 0);
  }

  @Test
  public void testCompleteCreateTransaction() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Map<String, String> properties =
        ImmutableMap.of("user", "someone", "created-at", "2022-02-25T00:38:19");
    Transaction create =
        catalog
            .buildTable(TABLE, SCHEMA)
            .withLocation("file:/tmp/ns/table")
            .withPartitionSpec(SPEC)
            .withSortOrder(WRITE_ORDER)
            .withProperties(properties)
            .createTransaction();

    Assert.assertFalse(
        "Table should not exist after createTransaction", catalog.tableExists(TABLE));

    create.newFastAppend().appendFile(FILE_A).commit();

    Assert.assertFalse("Table should not exist after append commit", catalog.tableExists(TABLE));

    create.commitTransaction();

    Assert.assertTrue("Table should exist after append commit", catalog.tableExists(TABLE));
    Table table = catalog.loadTable(TABLE);

    Assert.assertEquals(
        "Table schema should match the new schema",
        TABLE_SCHEMA.asStruct(),
        table.schema().asStruct());
    Assert.assertEquals(
        "Table should have create partition spec", TABLE_SPEC.fields(), table.spec().fields());
    Assert.assertEquals(
        "Table should have create sort order", TABLE_WRITE_ORDER, table.sortOrder());
    Assert.assertEquals(
        "Table properties should be a superset of the requested properties",
        properties.entrySet(),
        Sets.intersection(properties.entrySet(), table.properties().entrySet()));
    if (!overridesRequestedLocation()) {
      Assert.assertEquals(
          "Table location should match requested", "file:/tmp/ns/table", table.location());
    }
    assertFiles(table, FILE_A);
    assertFilesPartitionSpec(table);
    assertPreviousMetadataFileCount(table, 0);
  }

  @Test
  public void testCompleteCreateTransactionMultipleSchemas() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Map<String, String> properties =
        ImmutableMap.of("user", "someone", "created-at", "2022-02-25T00:38:19");
    Transaction create =
        catalog
            .buildTable(TABLE, SCHEMA)
            .withLocation("file:/tmp/ns/table")
            .withPartitionSpec(SPEC)
            .withSortOrder(WRITE_ORDER)
            .withProperties(properties)
            .createTransaction();

    Assert.assertFalse(
        "Table should not exist after createTransaction", catalog.tableExists(TABLE));

    create.newFastAppend().appendFile(FILE_A).commit();

    UpdateSchema updateSchema = create.updateSchema().addColumn("new_col", Types.LongType.get());
    Schema newSchema = updateSchema.apply();
    updateSchema.commit();

    UpdatePartitionSpec updateSpec = create.updateSpec().addField("new_col");
    PartitionSpec newSpec = updateSpec.apply();
    updateSpec.commit();

    ReplaceSortOrder replaceSortOrder = create.replaceSortOrder().asc("new_col");
    SortOrder newSortOrder = replaceSortOrder.apply();
    replaceSortOrder.commit();

    DataFile anotherFile =
        DataFiles.builder(newSpec)
            .withPath("/path/to/data-b.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("id_bucket=0/new_col=0") // easy way to set partition data for now
            .withRecordCount(2) // needs at least one record or else metrics will filter it out
            .build();

    create.newFastAppend().appendFile(anotherFile).commit();

    Assert.assertFalse("Table should not exist after append commit", catalog.tableExists(TABLE));

    create.commitTransaction();

    Assert.assertTrue("Table should exist after append commit", catalog.tableExists(TABLE));
    Table table = catalog.loadTable(TABLE);

    // initial IDs taken from TableMetadata constants
    final int initialSchemaId = 0;
    final int initialSpecId = 0;
    final int initialOrderId = 1;
    final int updateSchemaId = initialSchemaId + 1;
    final int updateSpecId = initialSpecId + 1;
    final int updateOrderId = initialOrderId + 1;

    Assert.assertEquals(
        "Table schema should match the new schema",
        newSchema.asStruct(),
        table.schema().asStruct());
    Assert.assertEquals(
        "Table schema should match the new schema ID", updateSchemaId, table.schema().schemaId());
    Assert.assertEquals(
        "Table should have updated partition spec", newSpec.fields(), table.spec().fields());
    Assert.assertEquals(
        "Table should have updated partition spec ID", updateSpecId, table.spec().specId());
    Assert.assertEquals(
        "Table should have updated sort order", newSortOrder.fields(), table.sortOrder().fields());
    Assert.assertEquals(
        "Table should have updated sort order ID", updateOrderId, table.sortOrder().orderId());
    Assert.assertEquals(
        "Table properties should be a superset of the requested properties",
        properties.entrySet(),
        Sets.intersection(properties.entrySet(), table.properties().entrySet()));
    if (!overridesRequestedLocation()) {
      Assert.assertEquals(
          "Table location should match requested", "file:/tmp/ns/table", table.location());
    }
    assertFiles(table, FILE_A, anotherFile);
    assertFilePartitionSpec(table, FILE_A, initialSpecId);
    assertFilePartitionSpec(table, anotherFile, updateSpecId);
    assertPreviousMetadataFileCount(table, 0);
  }

  @Test
  public void testCompleteCreateTransactionV2() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Map<String, String> properties =
        ImmutableMap.of(
            "user", "someone", "created-at", "2022-02-25T00:38:19", "format-version", "2");

    Transaction create =
        catalog
            .buildTable(TABLE, SCHEMA)
            .withLocation("file:/tmp/ns/table")
            .withPartitionSpec(SPEC)
            .withSortOrder(WRITE_ORDER)
            .withProperties(properties)
            .createTransaction();

    Assert.assertFalse(
        "Table should not exist after createTransaction", catalog.tableExists(TABLE));

    create.newFastAppend().appendFile(FILE_A).commit();

    Assert.assertFalse("Table should not exist after append commit", catalog.tableExists(TABLE));

    create.commitTransaction();

    Assert.assertTrue("Table should exist after append commit", catalog.tableExists(TABLE));
    Table table = catalog.loadTable(TABLE);

    Map<String, String> expectedProps = Maps.newHashMap(properties);
    expectedProps.remove("format-version");

    Assert.assertEquals(
        "Table schema should match the new schema",
        TABLE_SCHEMA.asStruct(),
        table.schema().asStruct());
    Assert.assertEquals(
        "Table should have create partition spec", TABLE_SPEC.fields(), table.spec().fields());
    Assert.assertEquals(
        "Table should have create sort order", TABLE_WRITE_ORDER, table.sortOrder());
    Assert.assertEquals(
        "Table properties should be a superset of the requested properties",
        expectedProps.entrySet(),
        Sets.intersection(properties.entrySet(), table.properties().entrySet()));
    Assert.assertEquals(
        "Sequence number should start at 1 for v2 format",
        1,
        table.currentSnapshot().sequenceNumber());
    if (!overridesRequestedLocation()) {
      Assert.assertEquals(
          "Table location should match requested", "file:/tmp/ns/table", table.location());
    }
    assertFiles(table, FILE_A);
    assertFilesPartitionSpec(table);
    assertPreviousMetadataFileCount(table, 0);
  }

  @Test
  public void testConcurrentCreateTransaction() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Transaction create = catalog.buildTable(TABLE, SCHEMA).createTransaction();

    Assert.assertFalse(
        "Table should not exist after createTransaction", catalog.tableExists(TABLE));

    create.newFastAppend().appendFile(FILE_A).commit();

    Assert.assertFalse("Table should not exist after append commit", catalog.tableExists(TABLE));

    catalog.buildTable(TABLE, OTHER_SCHEMA).create();

    Assertions.setMaxStackTraceElementsDisplayed(Integer.MAX_VALUE);
    String expectedMessage =
        supportsServerSideRetry()
            ? "Requirement failed: table already exists"
            : "Table already exists";
    Assertions.assertThatThrownBy(create::commitTransaction)
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageStartingWith(expectedMessage);

    // validate the concurrently created table is unmodified
    Table table = catalog.loadTable(TABLE);
    Assert.assertEquals(
        "Table schema should match concurrent create",
        OTHER_SCHEMA.asStruct(),
        table.schema().asStruct());
    assertNoFiles(table);
  }

  @Test
  public void testCreateOrReplaceTransactionCreate() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Transaction create = catalog.buildTable(TABLE, SCHEMA).createOrReplaceTransaction();

    Assert.assertFalse(
        "Table should not exist after createTransaction", catalog.tableExists(TABLE));

    create.newFastAppend().appendFile(FILE_A).commit();

    Assert.assertFalse("Table should not exist after append commit", catalog.tableExists(TABLE));

    create.commitTransaction();

    Assert.assertTrue("Table should exist after append commit", catalog.tableExists(TABLE));
    Table table = catalog.loadTable(TABLE);
    assertFiles(table, FILE_A);
    assertPreviousMetadataFileCount(table, 0);
  }

  @Test
  public void testCompleteCreateOrReplaceTransactionCreate() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Map<String, String> properties =
        ImmutableMap.of("user", "someone", "created-at", "2022-02-25T00:38:19");
    Transaction createOrReplace =
        catalog
            .buildTable(TABLE, SCHEMA)
            .withLocation("file:/tmp/ns/table")
            .withPartitionSpec(SPEC)
            .withSortOrder(WRITE_ORDER)
            .withProperties(properties)
            .createOrReplaceTransaction();

    Assert.assertFalse(
        "Table should not exist after createTransaction", catalog.tableExists(TABLE));

    createOrReplace.newFastAppend().appendFile(FILE_A).commit();

    Assert.assertFalse("Table should not exist after append commit", catalog.tableExists(TABLE));

    createOrReplace.commitTransaction();

    Assert.assertTrue("Table should exist after append commit", catalog.tableExists(TABLE));
    Table table = catalog.loadTable(TABLE);

    Assert.assertEquals(
        "Table schema should match the new schema",
        TABLE_SCHEMA.asStruct(),
        table.schema().asStruct());
    Assert.assertEquals(
        "Table should have create partition spec", TABLE_SPEC.fields(), table.spec().fields());
    Assert.assertEquals(
        "Table should have create sort order", TABLE_WRITE_ORDER, table.sortOrder());
    Assert.assertEquals(
        "Table properties should be a superset of the requested properties",
        properties.entrySet(),
        Sets.intersection(properties.entrySet(), table.properties().entrySet()));
    if (!overridesRequestedLocation()) {
      Assert.assertEquals(
          "Table location should match requested", "file:/tmp/ns/table", table.location());
    }
    assertFiles(table, FILE_A);
    assertFilesPartitionSpec(table);
    assertPreviousMetadataFileCount(table, 0);
  }

  @Test
  public void testCreateOrReplaceReplaceTransactionReplace() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Table original = catalog.buildTable(TABLE, OTHER_SCHEMA).create();

    Assert.assertTrue("Table should exist before replaceTransaction", catalog.tableExists(TABLE));

    Transaction createOrReplace = catalog.buildTable(TABLE, SCHEMA).createOrReplaceTransaction();

    Assert.assertTrue(
        "Table should still exist after replaceTransaction", catalog.tableExists(TABLE));

    createOrReplace.newFastAppend().appendFile(FILE_A).commit();

    // validate table has not changed
    Table table = catalog.loadTable(TABLE);
    Assert.assertEquals(
        "Table schema should match concurrent create",
        OTHER_SCHEMA.asStruct(),
        table.schema().asStruct());
    assertUUIDsMatch(original, table);
    assertNoFiles(table);

    createOrReplace.commitTransaction();

    // validate the table after replace
    Assert.assertTrue("Table should exist after append commit", catalog.tableExists(TABLE));
    table.refresh(); // refresh should work with UUID validation

    Table loaded = catalog.loadTable(TABLE);

    Assert.assertEquals(
        "Table schema should match the new schema",
        REPLACE_SCHEMA.asStruct(),
        loaded.schema().asStruct());
    assertUUIDsMatch(original, loaded);
    assertFiles(loaded, FILE_A);
    assertPreviousMetadataFileCount(loaded, 1);
  }

  @Test
  public void testCompleteCreateOrReplaceTransactionReplace() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Table original = catalog.buildTable(TABLE, OTHER_SCHEMA).create();

    Assert.assertTrue("Table should exist before replaceTransaction", catalog.tableExists(TABLE));

    Map<String, String> properties =
        ImmutableMap.of("user", "someone", "created-at", "2022-02-25T00:38:19");
    Transaction createOrReplace =
        catalog
            .buildTable(TABLE, SCHEMA)
            .withLocation("file:/tmp/ns/table")
            .withPartitionSpec(SPEC)
            .withSortOrder(WRITE_ORDER)
            .withProperties(properties)
            .createOrReplaceTransaction();

    Assert.assertTrue(
        "Table should still exist after replaceTransaction", catalog.tableExists(TABLE));

    createOrReplace.newFastAppend().appendFile(FILE_A).commit();

    // validate table has not changed
    Table table = catalog.loadTable(TABLE);
    Assert.assertEquals(
        "Table schema should match concurrent create",
        OTHER_SCHEMA.asStruct(),
        table.schema().asStruct());
    Assert.assertTrue("Table should be unpartitioned", table.spec().isUnpartitioned());
    Assert.assertTrue("Table should be unsorted", table.sortOrder().isUnsorted());
    Assert.assertNotEquals(
        "Created at should not match", table.properties().get("created-at"), "2022-02-25T00:38:19");
    assertUUIDsMatch(original, table);
    assertNoFiles(table);

    createOrReplace.commitTransaction();

    // validate the table after replace
    Assert.assertTrue("Table should exist after append commit", catalog.tableExists(TABLE));
    table.refresh(); // refresh should work with UUID validation

    Table loaded = catalog.loadTable(TABLE);

    Assert.assertEquals(
        "Table schema should match the new schema",
        REPLACE_SCHEMA.asStruct(),
        loaded.schema().asStruct());
    Assert.assertEquals("Table should have replace partition spec", REPLACE_SPEC, loaded.spec());
    Assert.assertEquals(
        "Table should have replace sort order", REPLACE_WRITE_ORDER, loaded.sortOrder());
    Assert.assertEquals(
        "Table properties should be a superset of the requested properties",
        properties.entrySet(),
        Sets.intersection(properties.entrySet(), loaded.properties().entrySet()));
    if (!overridesRequestedLocation()) {
      Assert.assertEquals(
          "Table location should be replaced", "file:/tmp/ns/table", table.location());
    }
    assertUUIDsMatch(original, loaded);
    assertFiles(loaded, FILE_A);
    assertPreviousMetadataFileCount(loaded, 1);
  }

  @Test
  public void testCreateOrReplaceTransactionConcurrentCreate() {
    Assume.assumeTrue(
        "Conversion to replace transaction is not supported by REST catalog",
        supportsServerSideRetry());

    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Transaction createOrReplace = catalog.buildTable(TABLE, SCHEMA).createOrReplaceTransaction();

    Assert.assertFalse(
        "Table should not exist after createTransaction", catalog.tableExists(TABLE));

    createOrReplace.newFastAppend().appendFile(FILE_A).commit();

    Assert.assertFalse("Table should not exist after append commit", catalog.tableExists(TABLE));

    catalog.buildTable(TABLE, OTHER_SCHEMA).create();

    String expectedMessage =
        supportsServerSideRetry()
            ? "Requirement failed: table already exists"
            : "Table already exists";
    Assertions.assertThatThrownBy(createOrReplace::commitTransaction)
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessage(expectedMessage);

    // validate the concurrently created table is unmodified
    Table table = catalog.loadTable(TABLE);
    Assert.assertEquals(
        "Table schema should match concurrent create",
        OTHER_SCHEMA.asStruct(),
        table.schema().asStruct());
    assertNoFiles(table);
  }

  @Test
  public void testReplaceTransaction() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Table original = catalog.buildTable(TABLE, OTHER_SCHEMA).create();

    Assert.assertTrue("Table should exist before replaceTransaction", catalog.tableExists(TABLE));

    Transaction replace = catalog.buildTable(TABLE, SCHEMA).replaceTransaction();

    Assert.assertTrue(
        "Table should still exist after replaceTransaction", catalog.tableExists(TABLE));

    replace.newFastAppend().appendFile(FILE_A).commit();

    // validate table has not changed
    Table table = catalog.loadTable(TABLE);
    Assert.assertEquals(
        "Table schema should match concurrent create",
        OTHER_SCHEMA.asStruct(),
        table.schema().asStruct());
    assertUUIDsMatch(original, table);
    assertNoFiles(table);

    replace.commitTransaction();

    // validate the table after replace
    Assert.assertTrue("Table should exist after append commit", catalog.tableExists(TABLE));
    table.refresh(); // refresh should work with UUID validation

    Table loaded = catalog.loadTable(TABLE);

    Assert.assertEquals(
        "Table schema should match the new schema",
        REPLACE_SCHEMA.asStruct(),
        loaded.schema().asStruct());
    assertUUIDsMatch(original, loaded);
    assertFiles(loaded, FILE_A);
    assertPreviousMetadataFileCount(loaded, 1);
  }

  @Test
  public void testCompleteReplaceTransaction() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Table original = catalog.buildTable(TABLE, OTHER_SCHEMA).create();

    Assert.assertTrue("Table should exist before replaceTransaction", catalog.tableExists(TABLE));

    Map<String, String> properties =
        ImmutableMap.of("user", "someone", "created-at", "2022-02-25T00:38:19");
    Transaction replace =
        catalog
            .buildTable(TABLE, SCHEMA)
            .withLocation("file:/tmp/ns/table")
            .withPartitionSpec(SPEC)
            .withSortOrder(WRITE_ORDER)
            .withProperties(properties)
            .replaceTransaction();

    Assert.assertTrue(
        "Table should still exist after replaceTransaction", catalog.tableExists(TABLE));

    replace.newFastAppend().appendFile(FILE_A).commit();

    // validate table has not changed
    Table table = catalog.loadTable(TABLE);
    Assert.assertEquals(
        "Table schema should match concurrent create",
        OTHER_SCHEMA.asStruct(),
        table.schema().asStruct());
    Assert.assertTrue("Table should be unpartitioned", table.spec().isUnpartitioned());
    Assert.assertTrue("Table should be unsorted", table.sortOrder().isUnsorted());
    Assert.assertNotEquals(
        "Created at should not match", table.properties().get("created-at"), "2022-02-25T00:38:19");
    assertUUIDsMatch(original, table);
    assertNoFiles(table);

    replace.commitTransaction();

    // validate the table after replace
    Assert.assertTrue("Table should exist after append commit", catalog.tableExists(TABLE));
    table.refresh(); // refresh should work with UUID validation

    Table loaded = catalog.loadTable(TABLE);

    Assert.assertEquals(
        "Table schema should match the new schema",
        REPLACE_SCHEMA.asStruct(),
        loaded.schema().asStruct());
    Assert.assertEquals("Table should have replace partition spec", REPLACE_SPEC, loaded.spec());
    Assert.assertEquals(
        "Table should have replace sort order", REPLACE_WRITE_ORDER, loaded.sortOrder());
    Assert.assertEquals(
        "Table properties should be a superset of the requested properties",
        properties.entrySet(),
        Sets.intersection(properties.entrySet(), loaded.properties().entrySet()));
    if (!overridesRequestedLocation()) {
      Assert.assertEquals(
          "Table location should be replaced", "file:/tmp/ns/table", table.location());
    }
    assertUUIDsMatch(original, loaded);
    assertFiles(loaded, FILE_A);
    assertPreviousMetadataFileCount(loaded, 1);
  }

  @Test
  public void testReplaceTransactionRequiresTableExists() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Assertions.assertThatThrownBy(() -> catalog.buildTable(TABLE, SCHEMA).replaceTransaction())
        .isInstanceOf(NoSuchTableException.class)
        .hasMessageStartingWith("Table does not exist: newdb.table");
  }

  @Test
  public void testConcurrentReplaceTransactions() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Transaction transaction = catalog.buildTable(TABLE, SCHEMA).createTransaction();
    transaction.newFastAppend().appendFile(FILE_A).commit();
    transaction.commitTransaction();

    Table original = catalog.loadTable(TABLE);
    assertFiles(original, FILE_A);

    Transaction secondReplace = catalog.buildTable(TABLE, SCHEMA).replaceTransaction();
    secondReplace.newFastAppend().appendFile(FILE_C).commit();

    Transaction firstReplace = catalog.buildTable(TABLE, SCHEMA).replaceTransaction();
    firstReplace.newFastAppend().appendFile(FILE_B).commit();
    firstReplace.commitTransaction();

    Table afterFirstReplace = catalog.loadTable(TABLE);
    Assert.assertEquals(
        "Table schema should match the original schema",
        original.schema().asStruct(),
        afterFirstReplace.schema().asStruct());
    Assert.assertTrue("Table should be unpartitioned", afterFirstReplace.spec().isUnpartitioned());
    Assert.assertTrue("Table should be unsorted", afterFirstReplace.sortOrder().isUnsorted());
    assertUUIDsMatch(original, afterFirstReplace);
    assertFiles(afterFirstReplace, FILE_B);

    secondReplace.commitTransaction();

    Table afterSecondReplace = catalog.loadTable(TABLE);
    Assert.assertEquals(
        "Table schema should match the original schema",
        original.schema().asStruct(),
        afterSecondReplace.schema().asStruct());
    Assert.assertTrue("Table should be unpartitioned", afterSecondReplace.spec().isUnpartitioned());
    Assert.assertTrue("Table should be unsorted", afterSecondReplace.sortOrder().isUnsorted());
    assertUUIDsMatch(original, afterSecondReplace);
    assertFiles(afterSecondReplace, FILE_C);
  }

  @Test
  public void testConcurrentReplaceTransactionSchema() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Transaction transaction = catalog.buildTable(TABLE, OTHER_SCHEMA).createTransaction();
    transaction.newFastAppend().appendFile(FILE_A).commit();
    transaction.commitTransaction();

    Table original = catalog.loadTable(TABLE);
    assertFiles(original, FILE_A);

    Transaction secondReplace = catalog.buildTable(TABLE, OTHER_SCHEMA).replaceTransaction();
    secondReplace.newFastAppend().appendFile(FILE_C).commit();

    Transaction firstReplace = catalog.buildTable(TABLE, SCHEMA).replaceTransaction();
    firstReplace.newFastAppend().appendFile(FILE_B).commit();
    firstReplace.commitTransaction();

    Table afterFirstReplace = catalog.loadTable(TABLE);
    Assert.assertEquals(
        "Table schema should match the new schema",
        REPLACE_SCHEMA.asStruct(),
        afterFirstReplace.schema().asStruct());
    assertUUIDsMatch(original, afterFirstReplace);
    assertFiles(afterFirstReplace, FILE_B);

    secondReplace.commitTransaction();

    Table afterSecondReplace = catalog.loadTable(TABLE);
    Assert.assertEquals(
        "Table schema should match the original schema",
        original.schema().asStruct(),
        afterSecondReplace.schema().asStruct());
    assertUUIDsMatch(original, afterSecondReplace);
    assertFiles(afterSecondReplace, FILE_C);
  }

  @Test
  public void testConcurrentReplaceTransactionSchema2() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Transaction transaction = catalog.buildTable(TABLE, OTHER_SCHEMA).createTransaction();
    transaction.newFastAppend().appendFile(FILE_A).commit();
    transaction.commitTransaction();

    Table original = catalog.loadTable(TABLE);
    assertFiles(original, FILE_A);

    Transaction secondReplace = catalog.buildTable(TABLE, SCHEMA).replaceTransaction();
    secondReplace.newFastAppend().appendFile(FILE_C).commit();

    Transaction firstReplace = catalog.buildTable(TABLE, OTHER_SCHEMA).replaceTransaction();
    firstReplace.newFastAppend().appendFile(FILE_B).commit();
    firstReplace.commitTransaction();

    Table afterFirstReplace = catalog.loadTable(TABLE);
    Assert.assertEquals(
        "Table schema should match the original schema",
        original.schema().asStruct(),
        afterFirstReplace.schema().asStruct());
    assertUUIDsMatch(original, afterFirstReplace);
    assertFiles(afterFirstReplace, FILE_B);

    secondReplace.commitTransaction();

    Table afterSecondReplace = catalog.loadTable(TABLE);
    Assert.assertEquals(
        "Table schema should match the new schema",
        REPLACE_SCHEMA.asStruct(),
        afterSecondReplace.schema().asStruct());
    assertUUIDsMatch(original, afterSecondReplace);
    assertFiles(afterSecondReplace, FILE_C);
  }

  @Test
  public void testConcurrentReplaceTransactionSchemaConflict() {
    Assume.assumeTrue("Schema conflicts are detected server-side", supportsServerSideRetry());

    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Transaction transaction = catalog.buildTable(TABLE, OTHER_SCHEMA).createTransaction();
    transaction.newFastAppend().appendFile(FILE_A).commit();
    transaction.commitTransaction();

    Table original = catalog.loadTable(TABLE);
    assertFiles(original, FILE_A);

    Transaction secondReplace = catalog.buildTable(TABLE, SCHEMA).replaceTransaction();
    secondReplace.newFastAppend().appendFile(FILE_C).commit();

    Transaction firstReplace = catalog.buildTable(TABLE, SCHEMA).replaceTransaction();
    firstReplace.newFastAppend().appendFile(FILE_B).commit();
    firstReplace.commitTransaction();

    Table afterFirstReplace = catalog.loadTable(TABLE);
    Assert.assertEquals(
        "Table schema should match the original schema",
        REPLACE_SCHEMA.asStruct(),
        afterFirstReplace.schema().asStruct());
    assertUUIDsMatch(original, afterFirstReplace);
    assertFiles(afterFirstReplace, FILE_B);

    // even though the new schema is identical, the assertion that the last assigned id has not
    // changed will fail
    Assertions.assertThatThrownBy(secondReplace::commitTransaction)
        .isInstanceOf(CommitFailedException.class)
        .hasMessageStartingWith(
            "Commit failed: Requirement failed: last assigned field id changed");
  }

  @Test
  public void testConcurrentReplaceTransactionPartitionSpec() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Transaction transaction = catalog.buildTable(TABLE, SCHEMA).createTransaction();
    transaction.newFastAppend().appendFile(FILE_A).commit();
    transaction.commitTransaction();

    Table original = catalog.loadTable(TABLE);
    assertFiles(original, FILE_A);

    Transaction secondReplace = catalog.buildTable(TABLE, SCHEMA).replaceTransaction();
    secondReplace.newFastAppend().appendFile(FILE_C).commit();

    Transaction firstReplace =
        catalog.buildTable(TABLE, SCHEMA).withPartitionSpec(SPEC).replaceTransaction();
    firstReplace.newFastAppend().appendFile(FILE_B).commit();
    firstReplace.commitTransaction();

    Table afterFirstReplace = catalog.loadTable(TABLE);
    Assert.assertEquals(
        "Table spec should match the new spec",
        TABLE_SPEC.fields(),
        afterFirstReplace.spec().fields());
    assertUUIDsMatch(original, afterFirstReplace);
    assertFiles(afterFirstReplace, FILE_B);

    secondReplace.commitTransaction();

    Table afterSecondReplace = catalog.loadTable(TABLE);
    Assert.assertTrue("Table should be unpartitioned", afterSecondReplace.spec().isUnpartitioned());
    assertUUIDsMatch(original, afterSecondReplace);
    assertFiles(afterSecondReplace, FILE_C);
  }

  @Test
  public void testConcurrentReplaceTransactionPartitionSpec2() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Transaction transaction = catalog.buildTable(TABLE, SCHEMA).createTransaction();
    transaction.newFastAppend().appendFile(FILE_A).commit();
    transaction.commitTransaction();

    Table original = catalog.loadTable(TABLE);
    assertFiles(original, FILE_A);

    Transaction secondReplace =
        catalog.buildTable(TABLE, SCHEMA).withPartitionSpec(SPEC).replaceTransaction();
    secondReplace.newFastAppend().appendFile(FILE_C).commit();

    Transaction firstReplace = catalog.buildTable(TABLE, SCHEMA).replaceTransaction();
    firstReplace.newFastAppend().appendFile(FILE_B).commit();
    firstReplace.commitTransaction();

    Table afterFirstReplace = catalog.loadTable(TABLE);
    Assert.assertTrue("Table should be unpartitioned", afterFirstReplace.spec().isUnpartitioned());
    assertUUIDsMatch(original, afterFirstReplace);
    assertFiles(afterFirstReplace, FILE_B);

    secondReplace.commitTransaction();

    Table afterSecondReplace = catalog.loadTable(TABLE);
    Assert.assertEquals(
        "Table spec should match the new spec",
        TABLE_SPEC.fields(),
        afterSecondReplace.spec().fields());
    assertUUIDsMatch(original, afterSecondReplace);
    assertFiles(afterSecondReplace, FILE_C);
  }

  @Test
  public void testConcurrentReplaceTransactionPartitionSpecConflict() {
    Assume.assumeTrue("Spec conflicts are detected server-side", supportsServerSideRetry());
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Transaction transaction = catalog.buildTable(TABLE, SCHEMA).createTransaction();
    transaction.newFastAppend().appendFile(FILE_A).commit();
    transaction.commitTransaction();

    Table original = catalog.loadTable(TABLE);
    assertFiles(original, FILE_A);

    Transaction secondReplace =
        catalog.buildTable(TABLE, SCHEMA).withPartitionSpec(SPEC).replaceTransaction();
    secondReplace.newFastAppend().appendFile(FILE_C).commit();

    Transaction firstReplace =
        catalog.buildTable(TABLE, SCHEMA).withPartitionSpec(SPEC).replaceTransaction();
    firstReplace.newFastAppend().appendFile(FILE_B).commit();
    firstReplace.commitTransaction();

    Table afterFirstReplace = catalog.loadTable(TABLE);
    Assert.assertEquals(
        "Table spec should match the new spec",
        TABLE_SPEC.fields(),
        afterFirstReplace.spec().fields());
    assertUUIDsMatch(original, afterFirstReplace);
    assertFiles(afterFirstReplace, FILE_B);

    // even though the new spec is identical, the assertion that the last assigned id has not
    // changed will fail
    Assertions.assertThatThrownBy(secondReplace::commitTransaction)
        .isInstanceOf(CommitFailedException.class)
        .hasMessageStartingWith(
            "Commit failed: Requirement failed: last assigned partition id changed");
  }

  @Test
  public void testConcurrentReplaceTransactionSortOrder() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Transaction transaction = catalog.buildTable(TABLE, SCHEMA).createTransaction();
    transaction.newFastAppend().appendFile(FILE_A).commit();
    transaction.commitTransaction();

    Table original = catalog.loadTable(TABLE);
    assertFiles(original, FILE_A);

    Transaction secondReplace = catalog.buildTable(TABLE, SCHEMA).replaceTransaction();
    secondReplace.newFastAppend().appendFile(FILE_C).commit();

    Transaction firstReplace =
        catalog.buildTable(TABLE, SCHEMA).withSortOrder(WRITE_ORDER).replaceTransaction();
    firstReplace.newFastAppend().appendFile(FILE_B).commit();
    firstReplace.commitTransaction();

    Table afterFirstReplace = catalog.loadTable(TABLE);
    Assert.assertEquals(
        "Table order should match the new order", TABLE_WRITE_ORDER, afterFirstReplace.sortOrder());
    assertUUIDsMatch(original, afterFirstReplace);
    assertFiles(afterFirstReplace, FILE_B);

    secondReplace.commitTransaction();

    Table afterSecondReplace = catalog.loadTable(TABLE);
    Assert.assertTrue("Table should be unsorted", afterSecondReplace.sortOrder().isUnsorted());
    assertUUIDsMatch(original, afterSecondReplace);
    assertFiles(afterSecondReplace, FILE_C);
  }

  @Test
  public void testConcurrentReplaceTransactionSortOrderConflict() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Transaction transaction = catalog.buildTable(TABLE, SCHEMA).createTransaction();
    transaction.newFastAppend().appendFile(FILE_A).commit();
    transaction.commitTransaction();

    Table original = catalog.loadTable(TABLE);
    assertFiles(original, FILE_A);

    Transaction secondReplace =
        catalog.buildTable(TABLE, SCHEMA).withSortOrder(WRITE_ORDER).replaceTransaction();
    secondReplace.newFastAppend().appendFile(FILE_C).commit();

    Transaction firstReplace =
        catalog
            .buildTable(TABLE, SCHEMA)
            .withSortOrder(
                SortOrder.builderFor(SCHEMA).desc(Expressions.bucket("id", 16)).desc("id").build())
            .replaceTransaction();
    firstReplace.newFastAppend().appendFile(FILE_B).commit();
    firstReplace.commitTransaction();

    Table afterFirstReplace = catalog.loadTable(TABLE);
    Assert.assertTrue("Table order should be set", afterFirstReplace.sortOrder().isSorted());
    assertUUIDsMatch(original, afterFirstReplace);
    assertFiles(afterFirstReplace, FILE_B);

    secondReplace.commitTransaction();

    Table afterSecondReplace = catalog.loadTable(TABLE);
    Assert.assertEquals(
        "Table order should match the new order",
        TABLE_WRITE_ORDER.fields(),
        afterSecondReplace.sortOrder().fields());
    assertUUIDsMatch(original, afterSecondReplace);
    assertFiles(afterSecondReplace, FILE_C);
  }

  @Test
  public void testMetadataFileLocationsRemovalAfterCommit() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Table table = catalog.buildTable(TABLE, SCHEMA).create();
    table.updateSchema().addColumn("a", Types.LongType.get()).commit();
    table.updateSchema().addColumn("b", Types.LongType.get()).commit();
    table.updateSchema().addColumn("c", Types.LongType.get()).commit();

    Set<String> metadataFileLocations = ReachableFileUtil.metadataFileLocations(table, false);
    Assertions.assertThat(metadataFileLocations).hasSize(4);

    int maxPreviousVersionsToKeep = 2;
    table
        .updateProperties()
        .set(TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, "true")
        .set(
            TableProperties.METADATA_PREVIOUS_VERSIONS_MAX,
            Integer.toString(maxPreviousVersionsToKeep))
        .commit();

    metadataFileLocations = ReachableFileUtil.metadataFileLocations(table, false);
    Assertions.assertThat(metadataFileLocations).hasSize(maxPreviousVersionsToKeep + 1);

    // for each new commit, the amount of metadata files should stay the same and old files should
    // be deleted
    for (int i = 1; i <= 5; i++) {
      table.updateSchema().addColumn("d" + i, Types.LongType.get()).commit();
      metadataFileLocations = ReachableFileUtil.metadataFileLocations(table, false);
      Assertions.assertThat(metadataFileLocations).hasSize(maxPreviousVersionsToKeep + 1);
    }

    maxPreviousVersionsToKeep = 4;
    table
        .updateProperties()
        .set(
            TableProperties.METADATA_PREVIOUS_VERSIONS_MAX,
            Integer.toString(maxPreviousVersionsToKeep))
        .commit();

    // for each new commit, the amount of metadata files should stay the same and old files should
    // be deleted
    for (int i = 1; i <= 10; i++) {
      table.updateSchema().addColumn("e" + i, Types.LongType.get()).commit();
      metadataFileLocations = ReachableFileUtil.metadataFileLocations(table, false);
      Assertions.assertThat(metadataFileLocations).hasSize(maxPreviousVersionsToKeep + 1);
    }
  }

  @Test
  public void tableCreationWithoutNamespace() {
    Assumptions.assumeTrue(requiresNamespaceCreate());

    Assertions.assertThatThrownBy(
            () ->
                catalog().buildTable(TableIdentifier.of("non-existing", "table"), SCHEMA).create())
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessageEndingWith("Namespace does not exist: non-existing");
  }

  private static void assertEmpty(String context, Catalog catalog, Namespace ns) {
    try {
      Assert.assertEquals(context, 0, catalog.listTables(ns).size());
    } catch (NoSuchNamespaceException e) {
      // it is okay if the catalog throws NoSuchNamespaceException when it is empty
    }
  }

  public void assertUUIDsMatch(Table expected, Table actual) {
    Assert.assertEquals(
        "Table UUID should not change",
        ((BaseTable) expected).operations().current().uuid(),
        ((BaseTable) actual).operations().current().uuid());
  }

  public void assertPreviousMetadataFileCount(Table table, int metadataFileCount) {
    TableOperations ops = ((BaseTable) table).operations();
    Assert.assertEquals(
        "Table should have correct number of previous metadata locations",
        metadataFileCount,
        ops.current().previousFiles().size());
  }

  public void assertNoFiles(Table table) {
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      Assert.assertFalse("Should contain no files", tasks.iterator().hasNext());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public void assertFiles(Table table, DataFile... files) {
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      List<CharSequence> paths =
          Streams.stream(tasks)
              .map(FileScanTask::file)
              .map(DataFile::path)
              .collect(Collectors.toList());
      Assert.assertEquals(
          "Should contain expected number of data files", files.length, paths.size());
      Assert.assertEquals(
          "Should contain correct file paths",
          CharSequenceSet.of(Iterables.transform(Arrays.asList(files), DataFile::path)),
          CharSequenceSet.of(paths));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public void assertFilePartitionSpec(Table table, DataFile dataFile, int specId) {
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      Streams.stream(tasks)
          .map(FileScanTask::file)
          .filter(file -> file.path().equals(dataFile.path()))
          .forEach(file -> Assert.assertEquals(specId, file.specId()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public void assertFilesPartitionSpec(Table table) {
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      Streams.stream(tasks)
          .map(FileScanTask::file)
          .forEach(file -> Assert.assertEquals(table.spec().specId(), file.specId()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private List<Namespace> concat(List<Namespace> starting, Namespace... additional) {
    List<Namespace> namespaces = Lists.newArrayList();
    namespaces.addAll(starting);
    namespaces.addAll(Arrays.asList(additional));
    return namespaces;
  }
}
