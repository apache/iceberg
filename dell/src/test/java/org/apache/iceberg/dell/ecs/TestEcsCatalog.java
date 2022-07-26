/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.dell.ecs;

import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.dell.mock.ecs.EcsS3MockRule;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.required;

public class TestEcsCatalog {

  static final Schema SCHEMA = new Schema(
      required(1, "id", Types.IntegerType.get()));

  @Rule
  public EcsS3MockRule rule = EcsS3MockRule.create();

  private EcsCatalog ecsCatalog;

  @Before
  public void before() {
    ecsCatalog = new EcsCatalog();
    Map<String, String> properties = Maps.newHashMap();
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, new EcsURI(rule.bucket(), "").location());
    properties.putAll(rule.clientProperties());
    ecsCatalog.initialize("test", properties);
  }

  @After
  public void after() throws IOException {
    ecsCatalog.close();
  }

  @Test
  public void testListTablesAndNamespaces() {
    ecsCatalog.createNamespace(Namespace.of("a"));
    ecsCatalog.createNamespace(Namespace.of("a", "b1"));
    ecsCatalog.createNamespace(Namespace.of("a", "b2"));
    ecsCatalog.createTable(TableIdentifier.of("t1"), SCHEMA);
    ecsCatalog.createTable(TableIdentifier.of("t2"), SCHEMA);
    ecsCatalog.createTable(TableIdentifier.of("a", "t3"), SCHEMA);
    ecsCatalog.createTable(TableIdentifier.of("a", "t4"), SCHEMA);
    ecsCatalog.createTable(TableIdentifier.of("a", "b1", "t5"), SCHEMA);

    Assert.assertEquals(
        "List namespaces with empty namespace",
        ImmutableList.of(Namespace.of("a")),
        ecsCatalog.listNamespaces());
    Assert.assertEquals(
        "List tables with empty namespace",
        ImmutableList.of(TableIdentifier.of("t1"), TableIdentifier.of("t2")),
        ecsCatalog.listTables(Namespace.empty()));
    Assert.assertEquals(
        "List namespaces in [a]",
        ImmutableList.of(Namespace.of("a", "b1"), Namespace.of("a", "b2")),
        ecsCatalog.listNamespaces(Namespace.of("a")));
    Assert.assertEquals(
        "List tables in [a]",
        ImmutableList.of(TableIdentifier.of("a", "t3"), TableIdentifier.of("a", "t4")),
        ecsCatalog.listTables(Namespace.of("a")));
  }

  @Test
  public void testNamespaceProperties() {
    ecsCatalog.createNamespace(Namespace.of("a"), ImmutableMap.of("a", "a"));

    Assert.assertEquals("The initial properties", ImmutableMap.of("a", "a"),
        ecsCatalog.loadNamespaceMetadata(Namespace.of("a")));

    ecsCatalog.setProperties(Namespace.of("a"), ImmutableMap.of("b", "b"));

    Assert.assertEquals("Update properties", ImmutableMap.of("a", "a", "b", "b"),
        ecsCatalog.loadNamespaceMetadata(Namespace.of("a")));

    ecsCatalog.removeProperties(Namespace.of("a"), ImmutableSet.of("a"));

    Assert.assertEquals("Remove properties", ImmutableMap.of("b", "b"),
        ecsCatalog.loadNamespaceMetadata(Namespace.of("a")));
  }

  @Test
  public void testDropNamespace() {
    ecsCatalog.createNamespace(Namespace.of("a"));
    ecsCatalog.createNamespace(Namespace.of("a", "b1"));
    ecsCatalog.createTable(TableIdentifier.of("a", "t1"), SCHEMA);

    AssertHelpers.assertThrows(
        "Drop an unknown namespace should throw exception",
        NoSuchNamespaceException.class,
        () -> ecsCatalog.dropNamespace(Namespace.of("unknown")));

    AssertHelpers.assertThrows(
        "Drop not empty namespace should throw exception",
        NamespaceNotEmptyException.class,
        () -> ecsCatalog.dropNamespace(Namespace.of("a")));

    Assert.assertTrue("Drop namespace [a, b1]", ecsCatalog.dropNamespace(Namespace.of("a", "b1")));

    Assert.assertFalse("The [a, b1] is absent", ecsCatalog.namespaceExists(Namespace.of("a", "b1")));
    Assert.assertTrue(
        "The [a, b1] is not in list result of [a]",
        ecsCatalog.listNamespaces(Namespace.of("a")).isEmpty());
  }

  @Test
  public void testDropTable() {
    ecsCatalog.createTable(TableIdentifier.of("a"), SCHEMA);

    Assert.assertFalse(
        "Drop an unknown table return false",
        ecsCatalog.dropTable(TableIdentifier.of("unknown")));

    Assert.assertTrue("Drop a table", ecsCatalog.dropTable(TableIdentifier.of("a"), true));
  }

  @Test
  public void testRenameTable() {
    ecsCatalog.createNamespace(Namespace.of("a"));
    ecsCatalog.createTable(TableIdentifier.of("a", "t1"), SCHEMA);
    ecsCatalog.createNamespace(Namespace.of("b"));

    AssertHelpers.assertThrows(
        "Rename an unknown table should throw exception",
        NoSuchTableException.class,
        () -> ecsCatalog.renameTable(TableIdentifier.of("unknown"), TableIdentifier.of("b", "t2")));

    AssertHelpers.assertThrows(
        "Rename to an unknown namespace should throw exception",
        NoSuchNamespaceException.class,
        () -> ecsCatalog.renameTable(TableIdentifier.of("a", "t1"), TableIdentifier.of("unknown", "t2")));

    ecsCatalog.renameTable(TableIdentifier.of("a", "t1"), TableIdentifier.of("b", "t2"));

    Assert.assertFalse("Old table does not exist", ecsCatalog.tableExists(TableIdentifier.of("a", "t1")));
    Assert.assertTrue("New table exists", ecsCatalog.tableExists(TableIdentifier.of("b", "t2")));
  }

  @Test
  public void testRegisterTable() {
    TableIdentifier identifier = TableIdentifier.of("a", "t1");
    ecsCatalog.createTable(identifier, SCHEMA);
    Table registeringTable = ecsCatalog.loadTable(identifier);
    ecsCatalog.dropTable(identifier, false);
    TableOperations ops = ((HasTableOperations) registeringTable).operations();
    String metadataLocation = ((EcsTableOperations) ops).currentMetadataLocation();
    Assertions.assertThat(ecsCatalog.registerTable(identifier, metadataLocation)).isNotNull();
    Assertions.assertThat(ecsCatalog.loadTable(identifier)).isNotNull();
    Assertions.assertThat(ecsCatalog.dropTable(identifier, true)).isTrue();
  }

  @Test
  public void testRegisterExistingTable() {
    TableIdentifier identifier = TableIdentifier.of("a", "t1");
    ecsCatalog.createTable(identifier, SCHEMA);
    Table registeringTable = ecsCatalog.loadTable(identifier);
    TableOperations ops = ((HasTableOperations) registeringTable).operations();
    String metadataLocation = ((EcsTableOperations) ops).currentMetadataLocation();
    Assertions.assertThatThrownBy(() -> ecsCatalog.registerTable(identifier, metadataLocation))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessage("Table already exists: a.t1");
    Assertions.assertThat(ecsCatalog.dropTable(identifier, true)).isTrue();
  }
}
