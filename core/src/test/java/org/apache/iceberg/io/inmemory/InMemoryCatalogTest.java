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

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchTableException;
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

public class InMemoryCatalogTest {

  private InMemoryCatalog catalog;

  @Before
  public void before() {
    catalog = new InMemoryCatalog();
    catalog.initialize("in-memory-catalog", ImmutableMap.of());
  }

  @Test
  public void testCreateNamespace() {
    Namespace namespace = Namespace.of("level1", "level2");

    // At the start of the test, ensure that
    // namespace does not exist and list namespace is empty.
    assertFalse(catalog.namespaceExists(namespace));
    assertTrue(catalog.listNamespaces().isEmpty());

    // Create a namespace.
    catalog.createNamespace(namespace);

    // Verify that namespace exists and
    // list namespace returns the created namespace.
    assertTrue(catalog.namespaceExists(namespace));
  }

  @Test
  public void testCreateNamespace_properties() {
    Namespace namespace = Namespace.of("level1", "level2");

    // Create a namespace with properties.
    catalog.createNamespace(namespace, ImmutableMap.of("k1", "v1", "k2", "v2"));

    // Verify namespace properties.
    assertEquals(ImmutableMap.of("k1", "v1", "k2", "v2"), catalog.loadNamespaceMetadata(namespace));
  }

  @Test
  public void testListNamespace_level0() {
    // Make sure list of namespaces is empty
    assertEquals(ImmutableList.of(), catalog.listNamespaces());

    // Create two namespaces, a.b and a.c
    catalog.createNamespace(Namespace.of("a", "b"));
    catalog.createNamespace(Namespace.of("a", "c"));

    // Verify that list namespace returns ["a"].
    assertEquals(ImmutableList.of(Namespace.of("a")), catalog.listNamespaces());
  }

  @Test
  public void testListNamespace_underANamespace() {
    // Make sure list of namespaces is empty
    assertEquals(ImmutableList.of(), catalog.listNamespaces());

    // Create two namespaces, a.b and a.c
    catalog.createNamespace(Namespace.of("a", "1"));
    catalog.createNamespace(Namespace.of("a", "2"));
    catalog.createNamespace(Namespace.of("b", "3"));

    // Verify that list namespace returns ["a", "1"] and ["a", "2"].
    assertEquals(ImmutableList.of(Namespace.of("a", "1"), Namespace.of("a", "2")),
        catalog.listNamespaces(Namespace.of("a")));
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
  public void dropNamespace() {
    Namespace namespace = Namespace.of("a");
    catalog.createNamespace(namespace);
    assertTrue(catalog.dropNamespace(namespace));
  }

  @Test(expected = NamespaceNotEmptyException.class)
  public void dropNamespace_notEmpty() {
    Namespace namespace = Namespace.of("a");
    catalog.createNamespace(namespace);
    catalog.createTable(TableIdentifier.of(namespace, "table1"), new Schema());
    assertTrue(catalog.dropNamespace(namespace));
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
  public void testDropTable() {
    TableIdentifier tableIdentifier = TableIdentifier.of("db1", "table1");

    // Create table
    catalog.createTable(tableIdentifier, new Schema());

    // Drop table should succeed
    assertTrue(catalog.dropTable(tableIdentifier, true));
    // Drop table should fail because it has been dropped
    assertFalse(catalog.dropTable(tableIdentifier, true));
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

  @Test(expected = NoSuchTableException.class)
  public void testRenameTable_fromDoesNotExist() {
    TableIdentifier fromTableIdentifier = TableIdentifier.of("db1", "table1");
    TableIdentifier toTableIdentifier = TableIdentifier.of("db1", "table2");

    // Rename should fail because 'db2.table1' does not exist
    catalog.renameTable(fromTableIdentifier, toTableIdentifier);
  }

  @Test(expected = AlreadyExistsException.class)
  public void testRenameTable_toAlreadyExists() {
    TableIdentifier fromTableIdentifier = TableIdentifier.of("db1", "table1");
    TableIdentifier toTableIdentifier = TableIdentifier.of("db1", "table2");

    // Create both 'db1.table1' and 'db2.table2'
    catalog.createTable(fromTableIdentifier, new Schema());
    catalog.createTable(toTableIdentifier, new Schema());

    // Rename should fail because 'db2.table2' already exists
    catalog.renameTable(fromTableIdentifier, toTableIdentifier);
  }

  @Test
  public void testListTables() {
    TableIdentifier db1Table1 = TableIdentifier.of("db1", "table1");
    TableIdentifier db1Table2 = TableIdentifier.of("db1", "table2");
    TableIdentifier db2Table1 = TableIdentifier.of("db2", "table1");

    catalog.createTable(db1Table1, new Schema());
    catalog.createTable(db1Table2, new Schema());
    catalog.createTable(db2Table1, new Schema());

    // list "" should return ["db1.table1", "db1.table2", "db2.table1"]
    assertEquals(ImmutableList.of(db1Table1, db1Table2, db2Table1), catalog.listTables(Namespace.of()));
    // list "db1" should return ["db1.table1", "db1.table2"]
    assertEquals(ImmutableList.of(db1Table1, db1Table2), catalog.listTables(Namespace.of("db1")));
    // list "db2" should return ["db2.table1"]
    assertEquals(ImmutableList.of(db2Table1), catalog.listTables(Namespace.of("db2")));
  }
}
