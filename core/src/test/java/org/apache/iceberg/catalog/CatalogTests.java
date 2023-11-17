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
import java.util.UUID;
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
import org.apache.iceberg.TestHelpers;
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
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

public abstract class CatalogTests<C extends Catalog & SupportsNamespaces> {
  private static final Namespace NS = Namespace.of("newdb");
  protected static final TableIdentifier TABLE = TableIdentifier.of(NS, "table");
  private static final TableIdentifier RENAMED_TABLE = TableIdentifier.of(NS, "table_renamed");

  // Schema passed to create tables
  protected static final Schema SCHEMA =
      new Schema(
          required(3, "id", Types.IntegerType.get(), "unique ID 🤪"),
          required(4, "data", Types.StringType.get()));

  // This is the actual schema for the table, with column IDs reassigned
  private static final Schema TABLE_SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get(), "unique ID 🤪"),
          required(2, "data", Types.StringType.get()));

  // This is the actual schema for the table, with column IDs reassigned
  private static final Schema REPLACE_SCHEMA =
      new Schema(
          required(2, "id", Types.IntegerType.get(), "unique ID 🤪"),
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

  protected static final DataFile FILE_A =
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

    Assertions.assertThat(catalog.namespaceExists(NS)).as("Namespace should not exist").isFalse();

    catalog.createNamespace(NS);
    Assertions.assertThat(catalog.listNamespaces())
        .as("Catalog should have the created namespace")
        .contains(NS);
    Assertions.assertThat(catalog.namespaceExists(NS)).as("Namespace should exist").isTrue();
  }

  @Test
  public void testCreateExistingNamespace() {
    C catalog = catalog();

    Assertions.assertThat(catalog.namespaceExists(NS)).as("Namespace should not exist").isFalse();

    catalog.createNamespace(NS);
    Assertions.assertThat(catalog.namespaceExists(NS)).as("Namespace should exist").isTrue();

    Assertions.assertThatThrownBy(() -> catalog.createNamespace(NS))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("Namespace already exists");

    Assertions.assertThat(catalog.namespaceExists(NS)).as("Namespace should still exist").isTrue();
  }

  @Test
  public void testCreateNamespaceWithProperties() {
    Assumptions.assumeTrue(supportsNamespaceProperties());

    C catalog = catalog();

    Assertions.assertThat(catalog.namespaceExists(NS)).as("Namespace should not exist").isFalse();

    Map<String, String> createProps = ImmutableMap.of("prop", "val");
    catalog.createNamespace(NS, createProps);
    Assertions.assertThat(catalog.namespaceExists(NS)).as("Namespace should exist").isTrue();

    Map<String, String> props = catalog.loadNamespaceMetadata(NS);

    Assertions.assertThat(Sets.intersection(createProps.entrySet(), props.entrySet()))
        .as("Create properties should be a subset of returned properties")
        .containsExactlyInAnyOrderElementsOf(createProps.entrySet());
  }

  @Test
  public void testLoadNamespaceMetadata() {
    C catalog = catalog();

    Assertions.assertThat(catalog.namespaceExists(NS)).as("Namespace should not exist").isFalse();

    Assertions.assertThatThrownBy(() -> catalog.loadNamespaceMetadata(NS))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessageStartingWith("Namespace does not exist: newdb");

    catalog.createNamespace(NS);
    Assertions.assertThat(catalog.namespaceExists(NS)).as("Namespace should exist").isTrue();
    Map<String, String> props = catalog.loadNamespaceMetadata(NS);
    Assertions.assertThat(props).as("Should return non-null property map").isNotNull();
    // note that there are no requirements for the properties returned by the catalog
  }

  @Test
  public void testSetNamespaceProperties() {
    Assumptions.assumeTrue(supportsNamespaceProperties());

    C catalog = catalog();

    Map<String, String> properties = ImmutableMap.of("owner", "user", "created-at", "sometime");

    catalog.createNamespace(NS);
    catalog.setProperties(NS, properties);

    Map<String, String> actualProperties = catalog.loadNamespaceMetadata(NS);
    Assertions.assertThat(actualProperties.entrySet())
        .as("Set properties should be a subset of returned properties")
        .containsAll(properties.entrySet());
  }

  @Test
  public void testUpdateNamespaceProperties() {
    Assumptions.assumeTrue(supportsNamespaceProperties());

    C catalog = catalog();

    Map<String, String> initialProperties = ImmutableMap.of("owner", "user");

    catalog.createNamespace(NS);
    catalog.setProperties(NS, initialProperties);

    Map<String, String> actualProperties = catalog.loadNamespaceMetadata(NS);
    Assertions.assertThat(actualProperties.entrySet())
        .as("Set properties should be a subset of returned properties")
        .containsAll(initialProperties.entrySet());

    Map<String, String> updatedProperties = ImmutableMap.of("owner", "newuser");

    catalog.setProperties(NS, updatedProperties);

    Map<String, String> finalProperties = catalog.loadNamespaceMetadata(NS);
    Assertions.assertThat(finalProperties.entrySet())
        .as("Updated properties should be a subset of returned properties")
        .containsAll(updatedProperties.entrySet());
  }

  @Test
  public void testUpdateAndSetNamespaceProperties() {
    Assumptions.assumeTrue(supportsNamespaceProperties());

    C catalog = catalog();

    Map<String, String> initialProperties = ImmutableMap.of("owner", "user");

    catalog.createNamespace(NS);
    catalog.setProperties(NS, initialProperties);

    Map<String, String> actualProperties = catalog.loadNamespaceMetadata(NS);
    Assertions.assertThat(actualProperties.entrySet())
        .as("Set properties should be a subset of returned properties")
        .containsAll(initialProperties.entrySet());

    Map<String, String> updatedProperties =
        ImmutableMap.of("owner", "newuser", "last-modified-at", "now");

    catalog.setProperties(NS, updatedProperties);

    Map<String, String> finalProperties = catalog.loadNamespaceMetadata(NS);
    Assertions.assertThat(finalProperties.entrySet())
        .as("Updated properties should be a subset of returned properties")
        .containsAll(updatedProperties.entrySet());
  }

  @Test
  public void testSetNamespacePropertiesNamespaceDoesNotExist() {
    Assumptions.assumeTrue(supportsNamespaceProperties());

    C catalog = catalog();

    Assertions.assertThatThrownBy(() -> catalog.setProperties(NS, ImmutableMap.of("test", "value")))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessageStartingWith("Namespace does not exist: newdb");
  }

  @Test
  public void testRemoveNamespaceProperties() {
    Assumptions.assumeTrue(supportsNamespaceProperties());

    C catalog = catalog();

    Map<String, String> properties = ImmutableMap.of("owner", "user", "created-at", "sometime");

    catalog.createNamespace(NS);
    catalog.setProperties(NS, properties);
    catalog.removeProperties(NS, ImmutableSet.of("created-at"));

    Map<String, String> actualProperties = catalog.loadNamespaceMetadata(NS);
    Assertions.assertThat(actualProperties.containsKey("created-at"))
        .as("Should not contain deleted property key")
        .isFalse();
    Assertions.assertThat(Sets.intersection(properties.entrySet(), actualProperties.entrySet()))
        .as("Expected properties should be a subset of returned properties")
        .containsExactlyInAnyOrderElementsOf(ImmutableMap.of("owner", "user").entrySet());
  }

  @Test
  public void testRemoveNamespacePropertiesNamespaceDoesNotExist() {
    Assumptions.assumeTrue(supportsNamespaceProperties());

    C catalog = catalog();

    Assertions.assertThatThrownBy(() -> catalog.removeProperties(NS, ImmutableSet.of("a", "b")))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessageStartingWith("Namespace does not exist: newdb");
  }

  @Test
  public void testDropNamespace() {
    C catalog = catalog();

    Assertions.assertThat(catalog.namespaceExists(NS)).as("Namespace should not exist").isFalse();

    catalog.createNamespace(NS);
    Assertions.assertThat(catalog.namespaceExists(NS)).as("Namespace should exist").isTrue();
    Assertions.assertThat(catalog.dropNamespace(NS))
        .as("Dropping an existing namespace should return true")
        .isTrue();
    Assertions.assertThat(catalog.namespaceExists(NS)).as("Namespace should not exist").isFalse();
  }

  @Test
  public void testDropNonexistentNamespace() {
    C catalog = catalog();

    Assertions.assertThat(catalog.dropNamespace(NS))
        .as("Dropping a nonexistent namespace should return false")
        .isFalse();
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
    Assertions.assertThat(catalog.listNamespaces().containsAll(starting))
        .as("Should include only starting namespaces")
        .isTrue();
  }

  @Test
  public void testListNestedNamespaces() {
    Assumptions.assumeTrue(
        supportsNestedNamespaces(), "Only valid when the catalog supports nested namespaces");

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
    Assumptions.assumeTrue(supportsNamesWithSlashes());

    C catalog = catalog();

    Namespace withSlash = Namespace.of("new/db");

    Assertions.assertThat(catalog.namespaceExists(withSlash))
        .as("Namespace should not exist")
        .isFalse();

    catalog.createNamespace(withSlash);
    Assertions.assertThat(catalog.namespaceExists(withSlash)).as("Namespace should exist").isTrue();

    Map<String, String> properties = catalog.loadNamespaceMetadata(withSlash);
    Assertions.assertThat(properties).as("Properties should be accessible").isNotNull();
    Assertions.assertThat(catalog.dropNamespace(withSlash))
        .as("Dropping the namespace should succeed")
        .isTrue();
    Assertions.assertThat(catalog.namespaceExists(withSlash))
        .as("Namespace should not exist")
        .isFalse();
  }

  @Test
  public void testNamespaceWithDot() {
    C catalog = catalog();

    Namespace withDot = Namespace.of("new.db");

    Assertions.assertThat(catalog.namespaceExists(withDot))
        .as("Namespace should not exist")
        .isFalse();

    catalog.createNamespace(withDot);
    Assertions.assertThat(catalog.namespaceExists(withDot)).as("Namespace should exist").isTrue();

    Map<String, String> properties = catalog.loadNamespaceMetadata(withDot);
    Assertions.assertThat(properties).as("Properties should be accessible").isNotNull();
    Assertions.assertThat(catalog.dropNamespace(withDot))
        .as("Dropping the namespace should succeed")
        .isTrue();
    Assertions.assertThat(catalog.namespaceExists(withDot))
        .as("Namespace should not exist")
        .isFalse();
  }

  @Test
  public void testBasicCreateTable() {
    C catalog = catalog();

    TableIdentifier ident = TableIdentifier.of("ns", "table");

    Assertions.assertThat(catalog.tableExists(ident)).as("Table should not exist").isFalse();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(ident.namespace());
    }

    Table table = catalog.buildTable(ident, SCHEMA).create();
    Assertions.assertThat(catalog.tableExists(ident)).as("Table should exist").isTrue();

    // validate table settings
    Assertions.assertThat(table.name())
        .as("Table name should report its full name")
        .isEqualTo(catalog.name() + "." + ident);
    Assertions.assertThat(table.schema().asStruct())
        .as("Schema should match expected ID assignment")
        .isEqualTo(TABLE_SCHEMA.asStruct());
    Assertions.assertThat(table.location()).as("Should have a location").isNotNull();
    Assertions.assertThat(table.spec().isUnpartitioned()).as("Should be unpartitioned").isTrue();
    Assertions.assertThat(table.sortOrder().isUnsorted()).as("Should be unsorted").isTrue();
    Assertions.assertThat(table.properties()).as("Should have table properties").isNotNull();
  }

  @Test
  public void testTableNameWithSlash() {
    Assumptions.assumeTrue(supportsNamesWithSlashes());

    C catalog = catalog();

    TableIdentifier ident = TableIdentifier.of("ns", "tab/le");
    if (requiresNamespaceCreate()) {
      catalog.createNamespace(Namespace.of("ns"));
    }

    Assertions.assertThat(catalog.tableExists(ident)).as("Table should not exist").isFalse();

    catalog.buildTable(ident, SCHEMA).create();
    Assertions.assertThat(catalog.tableExists(ident)).as("Table should exist").isTrue();

    Table loaded = catalog.loadTable(ident);
    Assertions.assertThat(loaded.schema().asStruct())
        .as("Schema should match expected ID assignment")
        .isEqualTo(TABLE_SCHEMA.asStruct());

    catalog.dropTable(ident);

    Assertions.assertThat(catalog.tableExists(ident)).as("Table should not exist").isFalse();
  }

  @Test
  public void testTableNameWithDot() {
    C catalog = catalog();

    TableIdentifier ident = TableIdentifier.of("ns", "ta.ble");
    if (requiresNamespaceCreate()) {
      catalog.createNamespace(Namespace.of("ns"));
    }

    Assertions.assertThat(catalog.tableExists(ident)).as("Table should not exist").isFalse();

    catalog.buildTable(ident, SCHEMA).create();
    Assertions.assertThat(catalog.tableExists(ident)).as("Table should exist").isTrue();

    Table loaded = catalog.loadTable(ident);
    Assertions.assertThat(loaded.schema().asStruct())
        .as("Schema should match expected ID assignment")
        .isEqualTo(TABLE_SCHEMA.asStruct());

    catalog.dropTable(ident);

    Assertions.assertThat(catalog.tableExists(ident)).as("Table should not exist").isFalse();
  }

  @Test
  public void testBasicCreateTableThatAlreadyExists() {
    C catalog = catalog();

    TableIdentifier ident = TableIdentifier.of("ns", "table");

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(ident.namespace());
    }

    Assertions.assertThat(catalog.tableExists(ident)).as("Table should not exist").isFalse();

    catalog.buildTable(ident, SCHEMA).create();
    Assertions.assertThat(catalog.tableExists(ident)).as("Table should exist").isTrue();

    Assertions.assertThatThrownBy(() -> catalog.buildTable(ident, OTHER_SCHEMA).create())
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageStartingWith("Table already exists: ns.table");

    Table table = catalog.loadTable(ident);
    Assertions.assertThat(table.schema().asStruct())
        .as("Schema should match original table schema")
        .isEqualTo(TABLE_SCHEMA.asStruct());
  }

  @Test
  public void testCompleteCreateTable() {
    C catalog = catalog();

    TableIdentifier ident = TableIdentifier.of("ns", "table");

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(ident.namespace());
    }

    Assertions.assertThat(catalog.tableExists(ident)).as("Table should not exist").isFalse();

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
    Assertions.assertThat(table.name())
        .as("Table name should report its full name")
        .isEqualTo(catalog.name() + "." + ident);
    Assertions.assertThat(catalog.tableExists(ident)).as("Table should exist").isTrue();
    Assertions.assertThat(table.schema().asStruct())
        .as("Schema should match expected ID assignment")
        .isEqualTo(TABLE_SCHEMA.asStruct());
    Assertions.assertThat(table.location()).as("Should have a location").isNotNull();
    Assertions.assertThat(table.spec())
        .as("Should use requested partition spec")
        .isEqualTo(TABLE_SPEC);
    Assertions.assertThat(table.sortOrder())
        .as("Should use requested write order")
        .isEqualTo(TABLE_WRITE_ORDER);
    Assertions.assertThat(table.properties().entrySet())
        .as("Table properties should be a superset of the requested properties")
        .containsAll(properties.entrySet());
    Assertions.assertThat(table.uuid())
        .isEqualTo(UUID.fromString(((BaseTable) table).operations().current().uuid()));
  }

  @Test
  public void testLoadTable() {
    C catalog = catalog();

    TableIdentifier ident = TableIdentifier.of("ns", "table");

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(ident.namespace());
    }

    Assertions.assertThat(catalog.tableExists(ident)).as("Table should not exist").isFalse();

    Map<String, String> properties =
        ImmutableMap.of("user", "someone", "created-at", "2022-02-25T00:38:19");
    catalog
        .buildTable(ident, SCHEMA)
        .withLocation("file:/tmp/ns/table")
        .withPartitionSpec(SPEC)
        .withSortOrder(WRITE_ORDER)
        .withProperties(properties)
        .create();
    Assertions.assertThat(catalog.tableExists(ident)).as("Table should exist").isTrue();

    Table table = catalog.loadTable(ident);
    // validate table settings
    Assertions.assertThat(table.name())
        .as("Table name should report its full name")
        .isEqualTo(catalog.name() + "." + ident);
    Assertions.assertThat(catalog.tableExists(ident)).as("Table should exist").isTrue();
    Assertions.assertThat(table.schema().asStruct())
        .as("Schema should match expected ID assignment")
        .isEqualTo(TABLE_SCHEMA.asStruct());
    Assertions.assertThat(table.location()).as("Should have a location").isNotNull();
    Assertions.assertThat(table.spec())
        .as("Should use requested partition spec")
        .isEqualTo(TABLE_SPEC);
    Assertions.assertThat(table.sortOrder())
        .as("Should use requested write order")
        .isEqualTo(TABLE_WRITE_ORDER);
    Assertions.assertThat(table.properties().entrySet())
        .as("Table properties should be a superset of the requested properties")
        .containsAll(properties.entrySet());
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

    Assertions.assertThat(catalog.tableExists(ident)).as("Table should not exist").isFalse();
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

    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Source table should not exist before create")
        .isFalse();

    catalog.buildTable(TABLE, SCHEMA).create();
    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should exist after create")
        .isTrue();

    Assertions.assertThat(catalog.tableExists(RENAMED_TABLE))
        .as("Destination table should not exist before rename")
        .isFalse();

    catalog.renameTable(TABLE, RENAMED_TABLE);
    Assertions.assertThat(catalog.tableExists(RENAMED_TABLE))
        .as("Table should exist with new name")
        .isTrue();
    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Original table should no longer exist")
        .isFalse();

    catalog.dropTable(RENAMED_TABLE);
    assertEmpty("Should not contain table after drop", catalog, NS);
  }

  @Test
  public void testRenameTableMissingSourceTable() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Source table should not exist before rename")
        .isFalse();
    Assertions.assertThat(catalog.tableExists(RENAMED_TABLE))
        .as("Destination table should not exist before rename")
        .isFalse();

    Assertions.assertThatThrownBy(() -> catalog.renameTable(TABLE, RENAMED_TABLE))
        .isInstanceOf(NoSuchTableException.class)
        .hasMessageContaining("Table does not exist");

    Assertions.assertThat(catalog.tableExists(RENAMED_TABLE))
        .as("Destination table should not exist after failed rename")
        .isFalse();
  }

  @Test
  public void testRenameTableDestinationTableAlreadyExists() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Source table should not exist before create")
        .isFalse();

    catalog.buildTable(TABLE, SCHEMA).create();
    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Source table should exist after create")
        .isTrue();

    Assertions.assertThat(catalog.tableExists(RENAMED_TABLE))
        .as("Destination table should not exist before create")
        .isFalse();

    catalog.buildTable(RENAMED_TABLE, SCHEMA).create();
    Assertions.assertThat(catalog.tableExists(RENAMED_TABLE))
        .as("Destination table should exist after create")
        .isTrue();
    Assertions.assertThatThrownBy(() -> catalog.renameTable(TABLE, RENAMED_TABLE))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("Table already exists");
    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Source table should still exist after failed rename")
        .isTrue();
    Assertions.assertThat(catalog.tableExists(RENAMED_TABLE))
        .as("Destination table should still exist after failed rename")
        .isTrue();

    String sourceTableUUID =
        ((HasTableOperations) catalog.loadTable(TABLE)).operations().current().uuid();
    String destinationTableUUID =
        ((HasTableOperations) catalog.loadTable(RENAMED_TABLE)).operations().current().uuid();
    Assertions.assertThat(sourceTableUUID)
        .as("Source and destination table should remain distinct after failed rename")
        .isNotEqualTo(destinationTableUUID);
  }

  @Test
  public void testDropTable() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should not exist before create")
        .isFalse();

    catalog.buildTable(TABLE, SCHEMA).create();
    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should exist after create")
        .isTrue();

    boolean dropped = catalog.dropTable(TABLE);
    Assertions.assertThat(dropped).as("Should drop a table that does exist").isTrue();
    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should not exist after drop")
        .isFalse();
  }

  @Test
  public void testDropTableWithPurge() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should not exist before create")
        .isFalse();

    catalog.buildTable(TABLE, SCHEMA).create();
    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should exist after create")
        .isTrue();

    boolean dropped = catalog.dropTable(TABLE, true);
    Assertions.assertThat(dropped).as("Should drop a table that does exist").isTrue();
    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should not exist after drop")
        .isFalse();
  }

  @Test
  public void testDropTableWithoutPurge() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should not exist before create")
        .isFalse();

    Table table = catalog.buildTable(TABLE, SCHEMA).create();
    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should exist after create")
        .isTrue();
    Set<String> actualMetadataFileLocations = ReachableFileUtil.metadataFileLocations(table, false);

    boolean dropped = catalog.dropTable(TABLE, false);
    Assertions.assertThat(dropped).as("Should drop a table that does exist").isTrue();
    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should not exist after drop")
        .isFalse();
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
    Assertions.assertThat(catalog.tableExists(noSuchTableIdent))
        .as("Table should not exist")
        .isFalse();
    Assertions.assertThat(catalog.dropTable(noSuchTableIdent))
        .as("Should not drop a table that does not exist")
        .isFalse();
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

    Assertions.assertThat(catalog.listTables(ns1))
        .as("Should contain ns_1.table_1 after create")
        .containsExactlyInAnyOrder(ns1Table1);

    catalog.buildTable(ns2Table1, SCHEMA).create();

    Assertions.assertThat(catalog.listTables(ns2))
        .as("Should contain ns_2.table_1 after create")
        .containsExactlyInAnyOrder(ns2Table1);
    Assertions.assertThat(catalog.listTables(ns1))
        .as("Should not show changes to ns_2 in ns_1")
        .containsExactlyInAnyOrder(ns1Table1);

    catalog.buildTable(ns1Table2, SCHEMA).create();

    Assertions.assertThat(catalog.listTables(ns2))
        .as("Should not show changes to ns_1 in ns_2")
        .containsExactlyInAnyOrder(ns2Table1);
    Assertions.assertThat(catalog.listTables(ns1))
        .as("Should contain ns_1.table_2 after create")
        .containsExactlyInAnyOrder(ns1Table1, ns1Table2);

    catalog.dropTable(ns1Table1);

    Assertions.assertThat(catalog.listTables(ns2))
        .as("Should not show changes to ns_1 in ns_2")
        .containsExactlyInAnyOrder(ns2Table1);
    Assertions.assertThat(catalog.listTables(ns1))
        .as("Should not contain ns_1.table_1 after drop")
        .containsExactlyInAnyOrder(ns1Table2);

    catalog.dropTable(ns1Table2);

    Assertions.assertThat(catalog.listTables(ns2))
        .as("Should not show changes to ns_1 in ns_2")
        .containsExactlyInAnyOrder(ns2Table1);

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

    Assertions.assertThat(loaded.schema().asStruct())
        .as("Loaded table should have expected schema")
        .isEqualTo(expected.asStruct());
  }

  @Test
  public void testUUIDValidation() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Table table = catalog.buildTable(TABLE, SCHEMA).create();
    UpdateSchema update = table.updateSchema().addColumn("new_col", Types.LongType.get());

    Assertions.assertThat(catalog.dropTable(TABLE)).as("Should successfully drop table").isTrue();
    catalog.buildTable(TABLE, OTHER_SCHEMA).create();

    String expectedMessage =
        supportsServerSideRetry() ? "Requirement failed: UUID does not match" : "Cannot commit";
    Assertions.assertThatThrownBy(update::commit)
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining(expectedMessage);

    Table loaded = catalog.loadTable(TABLE);
    Assertions.assertThat(loaded.schema().asStruct())
        .as("Loaded table should have expected schema")
        .isEqualTo(OTHER_SCHEMA.asStruct());
  }

  @Test
  public void testUpdateTableSchemaServerSideRetry() {
    Assumptions.assumeTrue(
        supportsServerSideRetry(),
        "Schema update recovery is only supported with server-side retry");
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
    Assertions.assertThat(loaded.schema().asStruct())
        .as("Loaded table should have expected schema")
        .isEqualTo(expected.asStruct());
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
    Assertions.assertThat(loaded.schema().asStruct())
        .as("Loaded table should have expected schema")
        .isEqualTo(expected.asStruct());
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
    Assertions.assertThat(loaded.schema().asStruct())
        .as("Loaded table should have expected schema")
        .isEqualTo(expected.asStruct());
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

    Assertions.assertThat(table.schema().asStruct())
        .as("Loaded table should have expected schema")
        .isEqualTo(TABLE_SCHEMA.asStruct());
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
    Assertions.assertThat(loaded.spec().fields())
        .as("Loaded table should have expected spec")
        .isEqualTo(expected.fields());
  }

  @Test
  public void testUpdateTableSpecServerSideRetry() {
    Assumptions.assumeTrue(
        supportsServerSideRetry(), "Spec update recovery is only supported with server-side retry");
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
    Assertions.assertThat(loaded.spec().fields())
        .as("Loaded table should have expected spec")
        .isEqualTo(expected.fields());
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
    Assertions.assertThat(loaded.spec().fields())
        .as("Loaded table should have expected spec")
        .isEqualTo(expected.fields());
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
    Assertions.assertThat(loaded.spec().fields())
        .as("Loaded table should have expected spec")
        .isEqualTo(expected.fields());
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
    Assertions.assertThat(((BaseTable) table).operations().current().formatVersion())
        .as("Should be a v2 table")
        .isEqualTo(2);

    table.updateSpec().addField("id").commit();

    table.updateSpec().removeField("id").commit();

    Assertions.assertThat(table.spec())
        .as("Loaded table should have expected spec")
        .isEqualTo(TABLE_SPEC);
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
    Assertions.assertThat(loaded.sortOrder().fields())
        .as("Loaded table should have expected order")
        .isEqualTo(expected.fields());
  }

  @Test
  public void testUpdateTableSortOrderServerSideRetry() {
    Assumptions.assumeTrue(
        supportsServerSideRetry(),
        "Sort order update recovery is only supported with server-side retry");
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
    Assertions.assertThat(loaded.sortOrder().fields())
        .as("Loaded table should have expected order")
        .isEqualTo(expected.fields());
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

    Assertions.assertThat(table.sortOrder())
        .as("Loaded table should have expected order")
        .isEqualTo(TABLE_WRITE_ORDER);
  }

  @Test
  public void testAppend() throws IOException {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Table table = catalog.buildTable(TABLE, SCHEMA).withPartitionSpec(SPEC).create();

    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      Assertions.assertThat(tasks.iterator().hasNext()).as("Should contain no files").isFalse();
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

    Assertions.assertThat(loaded.schema().asStruct())
        .as("Loaded table should have expected schema")
        .isEqualTo(expectedSchema.asStruct());
    Assertions.assertThat(loaded.spec().fields())
        .as("Loaded table should have expected spec")
        .isEqualTo(expectedSpec.fields());

    assertPreviousMetadataFileCount(loaded, 1);
  }

  @Test
  public void testCreateTransaction() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Transaction create = catalog.buildTable(TABLE, SCHEMA).createTransaction();

    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should not exist after createTransaction")
        .isFalse();

    create.newFastAppend().appendFile(FILE_A).commit();

    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should not exist after append commit")
        .isFalse();

    create.commitTransaction();

    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should exist after append commit")
        .isTrue();
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

    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should not exist after createTransaction")
        .isFalse();

    create.newFastAppend().appendFile(FILE_A).commit();

    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should not exist after append commit")
        .isFalse();

    create.commitTransaction();

    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should exist after append commit")
        .isTrue();

    Table table = catalog.loadTable(TABLE);
    Assertions.assertThat(table.schema().asStruct())
        .as("Table schema should match the new schema")
        .isEqualTo(TABLE_SCHEMA.asStruct());
    Assertions.assertThat(table.spec().fields())
        .as("Table should have create partition spec")
        .isEqualTo(TABLE_SPEC.fields());
    Assertions.assertThat(table.sortOrder())
        .as("Table should have create sort order")
        .isEqualTo(TABLE_WRITE_ORDER);
    Assertions.assertThat(table.properties().entrySet())
        .as("Table properties should be a superset of the requested properties")
        .containsAll(properties.entrySet());
    if (!overridesRequestedLocation()) {
      Assertions.assertThat(table.location())
          .as("Table location should match requested")
          .isEqualTo("file:/tmp/ns/table");
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

    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should not exist after createTransaction")
        .isFalse();

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

    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should not exist after append commit")
        .isFalse();

    create.commitTransaction();

    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should exist after append commit")
        .isTrue();

    Table table = catalog.loadTable(TABLE);

    // initial IDs taken from TableMetadata constants
    final int initialSchemaId = 0;
    final int initialSpecId = 0;
    final int initialOrderId = 1;
    final int updateSchemaId = initialSchemaId + 1;
    final int updateSpecId = initialSpecId + 1;
    final int updateOrderId = initialOrderId + 1;

    Assertions.assertThat(table.schema().asStruct())
        .as("Table schema should match the new schema")
        .isEqualTo(newSchema.asStruct());
    Assertions.assertThat(table.schema().schemaId())
        .as("Table schema should match the new schema ID")
        .isEqualTo(updateSchemaId);
    Assertions.assertThat(table.spec().fields())
        .as("Table should have updated partition spec")
        .isEqualTo(newSpec.fields());
    Assertions.assertThat(table.spec().specId())
        .as("Table should have updated partition spec ID")
        .isEqualTo(updateSpecId);
    Assertions.assertThat(table.sortOrder().fields())
        .as("Table should have updated sort order")
        .isEqualTo(newSortOrder.fields());
    Assertions.assertThat(table.sortOrder().orderId())
        .as("Table should have updated sort order ID")
        .isEqualTo(updateOrderId);
    Assertions.assertThat(table.properties().entrySet())
        .as("Table properties should be a superset of the requested properties")
        .containsAll(properties.entrySet());
    if (!overridesRequestedLocation()) {
      Assertions.assertThat(table.location())
          .as("Table location should match requested")
          .isEqualTo("file:/tmp/ns/table");
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

    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should not exist after createTransaction")
        .isFalse();

    create.newFastAppend().appendFile(FILE_A).commit();

    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should not exist after append commit")
        .isFalse();

    create.commitTransaction();

    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should exist after append commit")
        .isTrue();
    Table table = catalog.loadTable(TABLE);

    Map<String, String> expectedProps = Maps.newHashMap(properties);

    expectedProps.remove("format-version");

    Assertions.assertThat(table.schema().asStruct())
        .as("Table schema should match the new schema")
        .isEqualTo(TABLE_SCHEMA.asStruct());
    Assertions.assertThat(table.spec().fields())
        .as("Table should have create partition spec")
        .isEqualTo(TABLE_SPEC.fields());
    Assertions.assertThat(table.sortOrder())
        .as("Table should have create sort order")
        .isEqualTo(TABLE_WRITE_ORDER);
    Assertions.assertThat(Sets.intersection(properties.entrySet(), table.properties().entrySet()))
        .as("Table properties should be a superset of the requested properties")
        .containsExactlyInAnyOrderElementsOf(expectedProps.entrySet());
    Assertions.assertThat(table.currentSnapshot().sequenceNumber())
        .as("Sequence number should start at 1 for v2 format")
        .isEqualTo(1);
    if (!overridesRequestedLocation()) {
      Assertions.assertThat(table.location())
          .as("Table location should match requested")
          .isEqualTo("file:/tmp/ns/table");
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

    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should not exist after createTransaction")
        .isFalse();

    create.newFastAppend().appendFile(FILE_A).commit();

    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should not exist after append commit")
        .isFalse();

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
    Assertions.assertThat(table.schema().asStruct())
        .as("Table schema should match concurrent create")
        .isEqualTo(OTHER_SCHEMA.asStruct());
    assertNoFiles(table);
  }

  @Test
  public void testCreateOrReplaceTransactionCreate() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Transaction create = catalog.buildTable(TABLE, SCHEMA).createOrReplaceTransaction();

    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should not exist after createTransaction")
        .isFalse();

    create.newFastAppend().appendFile(FILE_A).commit();

    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should not exist after append commit")
        .isFalse();

    create.commitTransaction();

    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should exist after append commit")
        .isTrue();

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

    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should not exist after createTransaction")
        .isFalse();

    createOrReplace.newFastAppend().appendFile(FILE_A).commit();

    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should not exist after append commit")
        .isFalse();

    createOrReplace.commitTransaction();

    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should exist after append commit")
        .isTrue();

    Table table = catalog.loadTable(TABLE);

    Assertions.assertThat(table.schema().asStruct())
        .as("Table schema should match the new schema")
        .isEqualTo(TABLE_SCHEMA.asStruct());
    Assertions.assertThat(table.spec().fields())
        .as("Table should have create partition spec")
        .isEqualTo(TABLE_SPEC.fields());
    Assertions.assertThat(table.sortOrder())
        .as("Table should have create sort order")
        .isEqualTo(TABLE_WRITE_ORDER);
    Assertions.assertThat(table.properties().entrySet())
        .as("Table properties should be a superset of the requested properties")
        .containsAll(properties.entrySet());
    if (!overridesRequestedLocation()) {
      Assertions.assertThat(table.location())
          .as("Table location should match requested")
          .isEqualTo("file:/tmp/ns/table");
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

    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should exist before replaceTransaction")
        .isTrue();

    Transaction createOrReplace = catalog.buildTable(TABLE, SCHEMA).createOrReplaceTransaction();

    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should still exist after replaceTransaction")
        .isTrue();

    createOrReplace.newFastAppend().appendFile(FILE_A).commit();

    // validate table has not changed
    Table table = catalog.loadTable(TABLE);

    Assertions.assertThat(table.schema().asStruct())
        .as("Table schema should match concurrent create")
        .isEqualTo(OTHER_SCHEMA.asStruct());

    assertUUIDsMatch(original, table);
    assertNoFiles(table);

    createOrReplace.commitTransaction();

    // validate the table after replace
    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should exist after append commit")
        .isTrue();
    table.refresh(); // refresh should work with UUID validation

    Table loaded = catalog.loadTable(TABLE);

    Assertions.assertThat(loaded.schema().asStruct())
        .as("Table schema should match the new schema")
        .isEqualTo(REPLACE_SCHEMA.asStruct());
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

    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should exist before replaceTransaction")
        .isTrue();

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

    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should still exist after replaceTransaction")
        .isTrue();

    createOrReplace.newFastAppend().appendFile(FILE_A).commit();

    // validate table has not changed
    Table table = catalog.loadTable(TABLE);
    Assertions.assertThat(table.schema().asStruct())
        .as("Table schema should match concurrent create")
        .isEqualTo(OTHER_SCHEMA.asStruct());
    Assertions.assertThat(table.spec().isUnpartitioned())
        .as("Table should be unpartitioned")
        .isTrue();
    Assertions.assertThat(table.sortOrder().isUnsorted()).as("Table should be unsorted").isTrue();
    Assertions.assertThat(table.properties().get("created-at"))
        .as("Created at should not match")
        .isNotEqualTo("2022-02-25T00:38:19");
    assertUUIDsMatch(original, table);
    assertNoFiles(table);

    createOrReplace.commitTransaction();

    // validate the table after replace
    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should exist after append commit")
        .isTrue();
    table.refresh(); // refresh should work with UUID validation

    Table loaded = catalog.loadTable(TABLE);

    Assertions.assertThat(loaded.schema().asStruct())
        .as("Table schema should match the new schema")
        .isEqualTo(REPLACE_SCHEMA.asStruct());
    Assertions.assertThat(loaded.spec())
        .as("Table should have replace partition spec")
        .isEqualTo(REPLACE_SPEC);
    Assertions.assertThat(loaded.sortOrder())
        .as("Table should have replace sort order")
        .isEqualTo(REPLACE_WRITE_ORDER);
    Assertions.assertThat(loaded.properties().entrySet())
        .as("Table properties should be a superset of the requested properties")
        .containsAll(properties.entrySet());
    if (!overridesRequestedLocation()) {
      Assertions.assertThat(table.location())
          .as("Table location should be replaced")
          .isEqualTo("file:/tmp/ns/table");
    }

    assertUUIDsMatch(original, loaded);
    assertFiles(loaded, FILE_A);
    assertPreviousMetadataFileCount(loaded, 1);
  }

  @Test
  public void testCreateOrReplaceTransactionConcurrentCreate() {
    Assumptions.assumeTrue(
        supportsServerSideRetry(),
        "Conversion to replace transaction is not supported by REST catalog");

    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Transaction createOrReplace = catalog.buildTable(TABLE, SCHEMA).createOrReplaceTransaction();

    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should not exist after createTransaction")
        .isFalse();

    createOrReplace.newFastAppend().appendFile(FILE_A).commit();

    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should not exist after append commit")
        .isFalse();

    catalog.buildTable(TABLE, OTHER_SCHEMA).create();

    String expectedMessage =
        supportsServerSideRetry()
            ? "Requirement failed: table already exists"
            : "Table already exists";
    Assertions.assertThatThrownBy(createOrReplace::commitTransaction)
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageStartingWith(expectedMessage);

    // validate the concurrently created table is unmodified
    Table table = catalog.loadTable(TABLE);
    Assertions.assertThat(table.schema().asStruct())
        .as("Table schema should match concurrent create")
        .isEqualTo(OTHER_SCHEMA.asStruct());
    assertNoFiles(table);
  }

  @Test
  public void testReplaceTransaction() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Table original = catalog.buildTable(TABLE, OTHER_SCHEMA).create();

    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should exist before replaceTransaction")
        .isTrue();

    Transaction replace = catalog.buildTable(TABLE, SCHEMA).replaceTransaction();

    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should still exist after replaceTransaction")
        .isTrue();

    replace.newFastAppend().appendFile(FILE_A).commit();

    // validate table has not changed
    Table table = catalog.loadTable(TABLE);
    Assertions.assertThat(table.schema().asStruct())
        .as("Table schema should match concurrent create")
        .isEqualTo(OTHER_SCHEMA.asStruct());
    assertUUIDsMatch(original, table);
    assertNoFiles(table);

    replace.commitTransaction();

    // validate the table after replace
    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should exist after append commit")
        .isTrue();
    table.refresh(); // refresh should work with UUID validation

    Table loaded = catalog.loadTable(TABLE);

    Assertions.assertThat(loaded.schema().asStruct())
        .as("Table schema should match the new schema")
        .isEqualTo(REPLACE_SCHEMA.asStruct());

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

    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should exist before replaceTransaction")
        .isTrue();

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

    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should still exist after replaceTransaction")
        .isTrue();

    replace.newFastAppend().appendFile(FILE_A).commit();

    // validate table has not changed
    Table table = catalog.loadTable(TABLE);

    Assertions.assertThat(table.schema().asStruct())
        .as("Table schema should match concurrent create")
        .isEqualTo(OTHER_SCHEMA.asStruct());
    Assertions.assertThat(table.spec().isUnpartitioned())
        .as("Table should be unpartitioned")
        .isTrue();
    Assertions.assertThat(table.sortOrder().isUnsorted()).as("Table should be unsorted").isTrue();
    Assertions.assertThat(table.properties().get("created-at"))
        .as("Created at should not match")
        .isNotEqualTo("2022-02-25T00:38:19");

    assertUUIDsMatch(original, table);
    assertNoFiles(table);

    replace.commitTransaction();

    // validate the table after replace
    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should exist after append commit")
        .isTrue();
    table.refresh(); // refresh should work with UUID validation

    Table loaded = catalog.loadTable(TABLE);

    Assertions.assertThat(loaded.schema().asStruct())
        .as("Table schema should match the new schema")
        .isEqualTo(REPLACE_SCHEMA.asStruct());
    Assertions.assertThat(loaded.spec())
        .as("Table should have replace partition spec")
        .isEqualTo(REPLACE_SPEC);
    Assertions.assertThat(loaded.sortOrder())
        .as("Table should have replace sort order")
        .isEqualTo(REPLACE_WRITE_ORDER);
    Assertions.assertThat(loaded.properties().entrySet())
        .as("Table properties should be a superset of the requested properties")
        .containsAll(properties.entrySet());
    if (!overridesRequestedLocation()) {
      Assertions.assertThat(table.location())
          .as("Table location should be replaced")
          .isEqualTo("file:/tmp/ns/table");
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
    Assertions.assertThat(afterFirstReplace.schema().asStruct())
        .as("Table schema should match the original schema")
        .isEqualTo(original.schema().asStruct());
    Assertions.assertThat(afterFirstReplace.spec().isUnpartitioned())
        .as("Table should be unpartitioned")
        .isTrue();
    Assertions.assertThat(afterFirstReplace.sortOrder().isUnsorted())
        .as("Table should be unsorted")
        .isTrue();
    assertUUIDsMatch(original, afterFirstReplace);
    assertFiles(afterFirstReplace, FILE_B);

    secondReplace.commitTransaction();

    Table afterSecondReplace = catalog.loadTable(TABLE);
    Assertions.assertThat(afterSecondReplace.schema().asStruct())
        .as("Table schema should match the original schema")
        .isEqualTo(original.schema().asStruct());
    Assertions.assertThat(afterSecondReplace.spec().isUnpartitioned())
        .as("Table should be unpartitioned")
        .isTrue();
    Assertions.assertThat(afterSecondReplace.sortOrder().isUnsorted())
        .as("Table should be unsorted")
        .isTrue();
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
    Assertions.assertThat(afterFirstReplace.schema().asStruct())
        .as("Table schema should match the new schema")
        .isEqualTo(REPLACE_SCHEMA.asStruct());
    assertUUIDsMatch(original, afterFirstReplace);
    assertFiles(afterFirstReplace, FILE_B);

    secondReplace.commitTransaction();

    Table afterSecondReplace = catalog.loadTable(TABLE);
    Assertions.assertThat(afterSecondReplace.schema().asStruct())
        .as("Table schema should match the original schema")
        .isEqualTo(original.schema().asStruct());
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
    Assertions.assertThat(afterFirstReplace.schema().asStruct())
        .as("Table schema should match the original schema")
        .isEqualTo(original.schema().asStruct());
    assertUUIDsMatch(original, afterFirstReplace);
    assertFiles(afterFirstReplace, FILE_B);

    secondReplace.commitTransaction();

    Table afterSecondReplace = catalog.loadTable(TABLE);
    Assertions.assertThat(afterSecondReplace.schema().asStruct())
        .as("Table schema should match the new schema")
        .isEqualTo(REPLACE_SCHEMA.asStruct());
    assertUUIDsMatch(original, afterSecondReplace);
    assertFiles(afterSecondReplace, FILE_C);
  }

  @Test
  public void testConcurrentReplaceTransactionSchemaConflict() {
    Assumptions.assumeTrue(supportsServerSideRetry(), "Schema conflicts are detected server-side");

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
    Assertions.assertThat(afterFirstReplace.schema().asStruct())
        .as("Table schema should match the original schema")
        .isEqualTo(REPLACE_SCHEMA.asStruct());

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
    Assertions.assertThat(afterFirstReplace.spec().fields())
        .as("Table spec should match the new spec")
        .isEqualTo(TABLE_SPEC.fields());
    assertUUIDsMatch(original, afterFirstReplace);
    assertFiles(afterFirstReplace, FILE_B);

    secondReplace.commitTransaction();

    Table afterSecondReplace = catalog.loadTable(TABLE);
    Assertions.assertThat(afterSecondReplace.spec().isUnpartitioned())
        .as("Table should be unpartitioned")
        .isTrue();
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
    Assertions.assertThat(afterFirstReplace.spec().isUnpartitioned())
        .as("Table should be unpartitioned")
        .isTrue();
    assertUUIDsMatch(original, afterFirstReplace);
    assertFiles(afterFirstReplace, FILE_B);

    secondReplace.commitTransaction();

    Table afterSecondReplace = catalog.loadTable(TABLE);
    Assertions.assertThat(afterSecondReplace.spec().fields())
        .as("Table spec should match the new spec")
        .isEqualTo(TABLE_SPEC.fields());
    assertUUIDsMatch(original, afterSecondReplace);
    assertFiles(afterSecondReplace, FILE_C);
  }

  @Test
  public void testConcurrentReplaceTransactionPartitionSpecConflict() {
    Assumptions.assumeTrue(supportsServerSideRetry(), "Spec conflicts are detected server-side");
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
    Assertions.assertThat(afterFirstReplace.spec().fields())
        .as("Table spec should match the new spec")
        .isEqualTo(TABLE_SPEC.fields());
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
    Assertions.assertThat(afterFirstReplace.sortOrder())
        .as("Table order should match the new order")
        .isEqualTo(TABLE_WRITE_ORDER);
    assertUUIDsMatch(original, afterFirstReplace);
    assertFiles(afterFirstReplace, FILE_B);

    secondReplace.commitTransaction();

    Table afterSecondReplace = catalog.loadTable(TABLE);
    Assertions.assertThat(afterSecondReplace.sortOrder().isUnsorted())
        .as("Table should be unsorted")
        .isTrue();
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
    Assertions.assertThat(afterFirstReplace.sortOrder().isSorted())
        .as("Table order should be set")
        .isTrue();
    assertUUIDsMatch(original, afterFirstReplace);
    assertFiles(afterFirstReplace, FILE_B);

    secondReplace.commitTransaction();

    Table afterSecondReplace = catalog.loadTable(TABLE);
    Assertions.assertThat(afterSecondReplace.sortOrder().fields())
        .as("Table order should match the new order")
        .isEqualTo(TABLE_WRITE_ORDER.fields());
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
        .hasMessageContaining("Namespace does not exist: non-existing");
  }

  @Test
  public void testRegisterTable() {
    C catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(TABLE.namespace());
    }

    Map<String, String> properties =
        ImmutableMap.of("user", "someone", "created-at", "2023-01-15T00:00:01");
    Table originalTable =
        catalog
            .buildTable(TABLE, SCHEMA)
            .withPartitionSpec(SPEC)
            .withSortOrder(WRITE_ORDER)
            .withProperties(properties)
            .create();

    originalTable.newFastAppend().appendFile(FILE_A).commit();
    originalTable.newFastAppend().appendFile(FILE_B).commit();
    originalTable.newDelete().deleteFile(FILE_A).commit();
    originalTable.newFastAppend().appendFile(FILE_C).commit();

    TableOperations ops = ((BaseTable) originalTable).operations();
    String metadataLocation = ops.current().metadataFileLocation();

    catalog.dropTable(TABLE, false /* do not purge */);

    Table registeredTable = catalog.registerTable(TABLE, metadataLocation);

    Assertions.assertThat(registeredTable).isNotNull();
    Assertions.assertThat(catalog.tableExists(TABLE)).as("Table must exist").isTrue();
    Assertions.assertThat(registeredTable.properties())
        .as("Props must match")
        .containsAllEntriesOf(properties);
    Assertions.assertThat(registeredTable.schema().asStruct())
        .as("Schema must match")
        .isEqualTo(originalTable.schema().asStruct());
    Assertions.assertThat(registeredTable.specs())
        .as("Specs must match")
        .isEqualTo(originalTable.specs());
    Assertions.assertThat(registeredTable.sortOrders())
        .as("Sort orders must match")
        .isEqualTo(originalTable.sortOrders());
    Assertions.assertThat(registeredTable.currentSnapshot())
        .as("Current snapshot must match")
        .isEqualTo(originalTable.currentSnapshot());
    Assertions.assertThat(registeredTable.snapshots())
        .as("Snapshots must match")
        .isEqualTo(originalTable.snapshots());
    Assertions.assertThat(registeredTable.history())
        .as("History must match")
        .isEqualTo(originalTable.history());

    TestHelpers.assertSameSchemaMap(registeredTable.schemas(), originalTable.schemas());
    assertFiles(registeredTable, FILE_B, FILE_C);

    registeredTable.newFastAppend().appendFile(FILE_A).commit();
    assertFiles(registeredTable, FILE_B, FILE_C, FILE_A);

    Assertions.assertThat(catalog.loadTable(TABLE)).isNotNull();
    Assertions.assertThat(catalog.dropTable(TABLE)).isTrue();
    Assertions.assertThat(catalog.tableExists(TABLE)).isFalse();
  }

  @Test
  public void testRegisterExistingTable() {
    C catalog = catalog();

    TableIdentifier identifier = TableIdentifier.of("a", "t1");

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(identifier.namespace());
    }

    catalog.createTable(identifier, SCHEMA);
    Table table = catalog.loadTable(identifier);
    TableOperations ops = ((BaseTable) table).operations();
    String metadataLocation = ops.current().metadataFileLocation();
    Assertions.assertThatThrownBy(() -> catalog.registerTable(identifier, metadataLocation))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageStartingWith("Table already exists: a.t1");
    Assertions.assertThat(catalog.dropTable(identifier)).isTrue();
  }

  private static void assertEmpty(String context, Catalog catalog, Namespace ns) {
    try {
      Assertions.assertThat(catalog.listTables(ns)).as(context).isEmpty();
    } catch (NoSuchNamespaceException e) {
      // it is okay if the catalog throws NoSuchNamespaceException when it is empty
    }
  }

  public void assertUUIDsMatch(Table expected, Table actual) {
    Assertions.assertThat(((BaseTable) actual).operations().current().uuid())
        .as("Table UUID should not change")
        .isEqualTo(((BaseTable) expected).operations().current().uuid());
  }

  public void assertPreviousMetadataFileCount(Table table, int metadataFileCount) {
    TableOperations ops = ((BaseTable) table).operations();
    Assertions.assertThat(ops.current().previousFiles().size())
        .as("Table should have correct number of previous metadata locations")
        .isEqualTo(metadataFileCount);
  }

  public void assertNoFiles(Table table) {
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      Assertions.assertThat(tasks).as("Should contain no files").isEmpty();
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
      Assertions.assertThat(paths.size())
          .as("Should contain expected number of data files")
          .isEqualTo(files.length);
      Assertions.assertThat(CharSequenceSet.of(paths))
          .as("Should contain correct file paths")
          .isEqualTo(CharSequenceSet.of(Iterables.transform(Arrays.asList(files), DataFile::path)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public void assertFilePartitionSpec(Table table, DataFile dataFile, int specId) {
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      Streams.stream(tasks)
          .map(FileScanTask::file)
          .filter(file -> file.path().equals(dataFile.path()))
          .forEach(
              file ->
                  Assertions.assertThat(file.specId())
                      .as("Spec ID should match")
                      .isEqualTo(specId));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public void assertFilesPartitionSpec(Table table) {
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      Streams.stream(tasks)
          .map(FileScanTask::file)
          .forEach(
              file ->
                  Assertions.assertThat(file.specId())
                      .as("Spec ID should match")
                      .isEqualTo(table.spec().specId()));
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
