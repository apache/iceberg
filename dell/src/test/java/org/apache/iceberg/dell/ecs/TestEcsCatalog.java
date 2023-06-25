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
package org.apache.iceberg.dell.ecs;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.common.testutils.PerTestCallbackWrapper;
import org.apache.iceberg.dell.mock.ecs.EcsS3MockExtension;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class TestEcsCatalog {

  static final Schema SCHEMA = new Schema(required(1, "id", Types.IntegerType.get()));

  @RegisterExtension
  public PerTestCallbackWrapper<EcsS3MockExtension> extensionWrapper =
      new PerTestCallbackWrapper<>(EcsS3MockExtension.create());

  private EcsCatalog ecsCatalog;

  @BeforeEach
  void before() {
    ecsCatalog = new EcsCatalog();
    Map<String, String> properties = Maps.newHashMap();
    properties.put(
        CatalogProperties.WAREHOUSE_LOCATION,
        new EcsURI(extensionWrapper.getExtension().bucket(), "").location());
    properties.putAll(extensionWrapper.getExtension().clientProperties());
    ecsCatalog.initialize("test", properties);
  }

  @AfterEach
  void after() throws IOException {
    ecsCatalog.close();
  }

  @Test
  void testListTablesAndNamespaces() {
    ecsCatalog.createNamespace(Namespace.of("a"));
    ecsCatalog.createNamespace(Namespace.of("a", "b1"));
    ecsCatalog.createNamespace(Namespace.of("a", "b2"));
    ecsCatalog.createTable(TableIdentifier.of("t1"), SCHEMA);
    ecsCatalog.createTable(TableIdentifier.of("t2"), SCHEMA);
    ecsCatalog.createTable(TableIdentifier.of("a", "t3"), SCHEMA);
    ecsCatalog.createTable(TableIdentifier.of("a", "t4"), SCHEMA);
    ecsCatalog.createTable(TableIdentifier.of("a", "b1", "t5"), SCHEMA);

    Assertions.assertThat(ImmutableList.of(Namespace.of("a")))
        .as("List namespaces with empty namespace")
        .isEqualTo(ecsCatalog.listNamespaces());
    Assertions.assertThat(ImmutableList.of(TableIdentifier.of("t1"), TableIdentifier.of("t2")))
        .as("List tables with empty namespace")
        .isEqualTo(ecsCatalog.listTables(Namespace.empty()));
    Assertions.assertThat(ImmutableList.of(Namespace.of("a", "b1"), Namespace.of("a", "b2")))
        .as("List namespaces in [a]")
        .isEqualTo(ecsCatalog.listNamespaces(Namespace.of("a")));
    Assertions.assertThat(
            ImmutableList.of(TableIdentifier.of("a", "t3"), TableIdentifier.of("a", "t4")))
        .as("List tables in [a]")
        .isEqualTo(ecsCatalog.listTables(Namespace.of("a")));
  }

  @Test
  void testNamespaceProperties() {
    ecsCatalog.createNamespace(Namespace.of("a"), ImmutableMap.of("a", "a"));

    Assertions.assertThat(ImmutableMap.of("a", "a"))
        .as("The initial properties")
        .isEqualTo(ecsCatalog.loadNamespaceMetadata(Namespace.of("a")));

    ecsCatalog.setProperties(Namespace.of("a"), ImmutableMap.of("b", "b"));

    Assertions.assertThat(ImmutableMap.of("a", "a", "b", "b"))
        .as("Update properties")
        .isEqualTo(ecsCatalog.loadNamespaceMetadata(Namespace.of("a")));

    ecsCatalog.removeProperties(Namespace.of("a"), ImmutableSet.of("a"));

    Assertions.assertThat(ImmutableMap.of("b", "b"))
        .as("Remove properties")
        .isEqualTo(ecsCatalog.loadNamespaceMetadata(Namespace.of("a")));
  }

  @Test
  void testDropNamespace() {
    ecsCatalog.createNamespace(Namespace.of("a"));
    ecsCatalog.createNamespace(Namespace.of("a", "b1"));
    ecsCatalog.createTable(TableIdentifier.of("a", "t1"), SCHEMA);

    Assertions.assertThatThrownBy(() -> ecsCatalog.dropNamespace(Namespace.of("unknown")))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessage("Namespace unknown does not exist");

    Assertions.assertThatThrownBy(() -> ecsCatalog.dropNamespace(Namespace.of("a")))
        .isInstanceOf(NamespaceNotEmptyException.class)
        .hasMessage("Namespace a is not empty");

    assertThat(ecsCatalog.dropNamespace(Namespace.of("a", "b1")))
        .as("Drop namespace [a, b1]")
        .isTrue();

    assertThat(ecsCatalog.namespaceExists(Namespace.of("a", "b1")))
        .as("The [a, b1] is absent")
        .isFalse();
    assertThat(ecsCatalog.listNamespaces(Namespace.of("a")))
        .as("The [a, b1] is not in list result of [a]")
        .isEmpty();
  }

  @Test
  void testDropTable() {
    ecsCatalog.createTable(TableIdentifier.of("a"), SCHEMA);

    assertThat(ecsCatalog.dropTable(TableIdentifier.of("unknown")))
        .as("Drop an unknown table return false")
        .isFalse();

    assertThat(ecsCatalog.dropTable(TableIdentifier.of("a"), true)).as("Drop a table").isTrue();
  }

  @Test
  void testRenameTable() {
    ecsCatalog.createNamespace(Namespace.of("a"));
    ecsCatalog.createTable(TableIdentifier.of("a", "t1"), SCHEMA);
    ecsCatalog.createNamespace(Namespace.of("b"));

    Assertions.assertThatThrownBy(
            () ->
                ecsCatalog.renameTable(
                    TableIdentifier.of("unknown"), TableIdentifier.of("b", "t2")))
        .isInstanceOf(NoSuchTableException.class)
        .hasMessage("Cannot rename table because table unknown does not exist");

    Assertions.assertThatThrownBy(
            () ->
                ecsCatalog.renameTable(
                    TableIdentifier.of("a", "t1"), TableIdentifier.of("unknown", "t2")))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessage("Cannot rename a.t1 to unknown.t2 because namespace unknown does not exist");

    ecsCatalog.renameTable(TableIdentifier.of("a", "t1"), TableIdentifier.of("b", "t2"));

    assertThat(ecsCatalog.tableExists(TableIdentifier.of("a", "t1")))
        .as("Old table does not exist")
        .isFalse();
    assertThat(ecsCatalog.tableExists(TableIdentifier.of("b", "t2")))
        .as("New table exists")
        .isTrue();
  }

  @Test
  void testRegisterTable() {
    TableIdentifier identifier = TableIdentifier.of("a", "t1");
    ecsCatalog.createTable(identifier, SCHEMA);
    Table registeringTable = ecsCatalog.loadTable(identifier);
    ecsCatalog.dropTable(identifier, false);
    TableOperations ops = ((HasTableOperations) registeringTable).operations();
    String metadataLocation = ((EcsTableOperations) ops).currentMetadataLocation();
    Table registeredTable = ecsCatalog.registerTable(identifier, metadataLocation);
    Assertions.assertThat(registeredTable).isNotNull();
    String expectedMetadataLocation =
        ((HasTableOperations) registeredTable).operations().current().metadataFileLocation();
    Assertions.assertThat(metadataLocation).isEqualTo(expectedMetadataLocation);
    Assertions.assertThat(ecsCatalog.loadTable(identifier)).isNotNull();
    Assertions.assertThat(ecsCatalog.dropTable(identifier, true)).isTrue();
  }

  @Test
  void testRegisterExistingTable() {
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
