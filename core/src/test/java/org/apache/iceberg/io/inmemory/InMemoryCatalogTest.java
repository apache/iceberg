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

package org.apache.iceberg.io.inmemory;

import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.CommonCatalogTests;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.IntegerType;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests specific to {@link InMemoryCatalog}, e.g. rename table, alter namespace properties, etc.
 * Common catalog tests can be found in {@link CommonCatalogTests}.
 */
public class InMemoryCatalogTest {

  private InMemoryCatalog catalog;

  @Before
  public void before() {
    catalog = new InMemoryCatalog();
    catalog.initialize("in-memory-catalog", ImmutableMap.of());
  }

  @Test
  public void testSetProperties() {
    Namespace namespace = Namespace.of("a");

    // Create a namespace with properties: {'k1': 'v1', 'k2': 'v2'}
    catalog.createNamespace(namespace, ImmutableMap.of("k1", "v1", "k2", "v2"));
    // Verify the properties
    assertEquals(ImmutableMap.of("k1", "v1", "k2", "v2"), catalog.loadNamespaceMetadata(namespace));

    // Set properties such that 'k1' is overwritten and a new key 'k3' is added.
    catalog.setProperties(namespace, ImmutableMap.of("k1", "v1'", "k3", "v3"));
    // Verify the result of set properties.
    assertEquals(ImmutableMap.of("k1", "v1'", "k2", "v2", "k3", "v3"), catalog.loadNamespaceMetadata(namespace));
  }

  @Test
  public void testRemoveProperties() {
    Namespace namespace = Namespace.of("a");

    // Create a namespace with properties: {'k1': 'v1', 'k2': 'v2'}
    catalog.createNamespace(namespace, ImmutableMap.of("k1", "v1", "k2", "v2"));
    // Verify the properties
    assertEquals(ImmutableMap.of("k1", "v1", "k2", "v2"), catalog.loadNamespaceMetadata(namespace));

    // Remove properties 'k1'
    catalog.removeProperties(namespace, ImmutableSet.of("k1", "k3"));
    // Verify the result of set properties.
    assertEquals(ImmutableMap.of("k2", "v2"), catalog.loadNamespaceMetadata(namespace));
  }

  @Test
  public void testCreateTable() {
    TableIdentifier tableIdentifier = TableIdentifier.of("db1", "table1");

    // Verify table does not already exist
    assertFalse(catalog.tableExists(tableIdentifier));

    // Create table
    catalog.createTable(tableIdentifier, new Schema());

    // Verify table exists now and can be loaded
    assertTrue(catalog.tableExists(tableIdentifier));
    assertNotNull(catalog.loadTable(tableIdentifier));
  }

  @Test
  public void testAlterTable() {
    TableIdentifier tableIdentifier = TableIdentifier.of("db1", "table1");

    // Create table
    catalog.createTable(tableIdentifier, new Schema());
    Table table = catalog.loadTable(tableIdentifier);
    assertTrue(table.schema().columns().isEmpty());

    // Alter the schema and verify updated schema
    table.updateSchema().addColumn("column", IntegerType.get()).commit();
    Table alteredTable = catalog.loadTable(tableIdentifier);
    assertEquals(1, alteredTable.schema().columns().size());
    assertEquals(ImmutableList.of(Types.NestedField.of(1, true, "column", IntegerType.get())),
        alteredTable.schema().columns());
  }

  @Test
  public void testDropTable_doesNotExist() {
    assertFalse(catalog.dropTable(TableIdentifier.of("db1", "table1"), false));
    assertFalse(catalog.dropTable(TableIdentifier.of("db1", "table1"), true));
  }

  @Test
  public void testRenameTable() {
    TableIdentifier fromTableIdentifier = TableIdentifier.of("db1", "table1");
    TableIdentifier toTableIdentifier = TableIdentifier.of("db1", "table2");

    // Verify that either table does not exist
    assertFalse(catalog.tableExists(fromTableIdentifier));
    assertFalse(catalog.tableExists(toTableIdentifier));

    // Create table 'from'
    catalog.createTable(fromTableIdentifier, new Schema());

    // Verify that 'db1.table1' exists and 'db1.table2' does not
    assertTrue(catalog.tableExists(fromTableIdentifier));
    assertFalse(catalog.tableExists(toTableIdentifier));

    // Rename table 'db1.table1' -> 'db1.table2'
    catalog.renameTable(fromTableIdentifier, toTableIdentifier);

    // Verify that rename worked
    assertFalse(catalog.tableExists(fromTableIdentifier));
    assertTrue(catalog.tableExists(toTableIdentifier));
  }

  @Test
  public void testRenameTable_fromDoesNotExist() {
    TableIdentifier fromTableIdentifier = TableIdentifier.of("db1", "table1");
    TableIdentifier toTableIdentifier = TableIdentifier.of("db2", "table2");

    catalog.createNamespace(Namespace.of("db2"));

    // Rename should fail because 'db1.table1' does not exist
    AssertHelpers.assertThrows(
        "Renaming a table that does not exist should fail",
        NoSuchTableException.class,
        "Cannot rename db1.table1 to db2.table2 because the table db1.table1 does not exist",
        () -> catalog.renameTable(fromTableIdentifier, toTableIdentifier));
  }

  @Test
  public void testRenameTable_toAlreadyExists() {
    TableIdentifier fromTableIdentifier = TableIdentifier.of("db1", "table1");
    TableIdentifier toTableIdentifier = TableIdentifier.of("db2", "table2");

    // Create both 'db1.table1' and 'db1.table2'
    catalog.createTable(fromTableIdentifier, new Schema());
    catalog.createTable(toTableIdentifier, new Schema());

    // Rename should fail because 'db2.table2' already exists
    AssertHelpers.assertThrows(
        "Renaming a table to an existing table should fail",
        AlreadyExistsException.class,
        "Cannot rename db1.table1 to db2.table2 because the table db2.table2 already exists",
        () -> catalog.renameTable(fromTableIdentifier, toTableIdentifier));
  }
}
