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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.Map;
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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class TestEcsCatalog {

  static final Schema SCHEMA = new Schema(required(1, "id", Types.IntegerType.get()));

  @RegisterExtension public EcsS3MockRule rule = EcsS3MockRule.create();

  private EcsCatalog ecsCatalog;

  @BeforeEach
  public void before() {
    ecsCatalog = new EcsCatalog();
    Map<String, String> properties = Maps.newHashMap();
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, new EcsURI(rule.bucket(), "").location());
    properties.putAll(rule.clientProperties());
    ecsCatalog.initialize("test", properties);
  }

  @AfterEach
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

    assertThat(ecsCatalog.listNamespaces()).containsExactly(Namespace.of("a"));
    assertThat(ecsCatalog.listTables(Namespace.empty()))
        .containsExactly(TableIdentifier.of("t1"), TableIdentifier.of("t2"));
    assertThat(ecsCatalog.listNamespaces(Namespace.of("a")))
        .containsExactly(Namespace.of("a", "b1"), Namespace.of("a", "b2"));
    assertThat(ecsCatalog.listTables(Namespace.of("a")))
        .containsExactly(TableIdentifier.of("a", "t3"), TableIdentifier.of("a", "t4"));
  }

  @Test
  public void testNamespaceProperties() {
    ecsCatalog.createNamespace(Namespace.of("a"), ImmutableMap.of("a", "a"));

    assertThat(ecsCatalog.loadNamespaceMetadata(Namespace.of("a")))
        .isEqualTo(ImmutableMap.of("a", "a"));

    ecsCatalog.setProperties(Namespace.of("a"), ImmutableMap.of("b", "b"));

    assertThat(ecsCatalog.loadNamespaceMetadata(Namespace.of("a")))
        .isEqualTo(ImmutableMap.of("a", "a", "b", "b"));

    ecsCatalog.removeProperties(Namespace.of("a"), ImmutableSet.of("a"));

    assertThat(ecsCatalog.loadNamespaceMetadata(Namespace.of("a")))
        .isEqualTo(ImmutableMap.of("b", "b"));
  }

  @Test
  public void testDropNamespace() {
    ecsCatalog.createNamespace(Namespace.of("a"));
    ecsCatalog.createNamespace(Namespace.of("a", "b1"));
    ecsCatalog.createTable(TableIdentifier.of("a", "t1"), SCHEMA);

    assertThatThrownBy(() -> ecsCatalog.dropNamespace(Namespace.of("unknown")))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessage("Namespace unknown does not exist");

    assertThatThrownBy(() -> ecsCatalog.dropNamespace(Namespace.of("a")))
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
  public void testDropTable() {
    ecsCatalog.createTable(TableIdentifier.of("a"), SCHEMA);

    assertThat(ecsCatalog.dropTable(TableIdentifier.of("unknown")))
        .as("Drop an unknown table return false")
        .isFalse();

    assertThat(ecsCatalog.dropTable(TableIdentifier.of("a"), true)).as("Drop a table").isTrue();
  }

  @Test
  public void testRenameTable() {
    ecsCatalog.createNamespace(Namespace.of("a"));
    ecsCatalog.createTable(TableIdentifier.of("a", "t1"), SCHEMA);
    ecsCatalog.createNamespace(Namespace.of("b"));

    assertThatThrownBy(
            () ->
                ecsCatalog.renameTable(
                    TableIdentifier.of("unknown"), TableIdentifier.of("b", "t2")))
        .isInstanceOf(NoSuchTableException.class)
        .hasMessage("Cannot rename table because table unknown does not exist");

    assertThatThrownBy(
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
  public void testRegisterTable() {
    TableIdentifier identifier = TableIdentifier.of("a", "t1");
    ecsCatalog.createTable(identifier, SCHEMA);
    Table registeringTable = ecsCatalog.loadTable(identifier);
    ecsCatalog.dropTable(identifier, false);
    TableOperations ops = ((HasTableOperations) registeringTable).operations();
    String metadataLocation = ((EcsTableOperations) ops).currentMetadataLocation();
    Table registeredTable = ecsCatalog.registerTable(identifier, metadataLocation);
    assertThat(registeredTable).isNotNull();
    String expectedMetadataLocation =
        ((HasTableOperations) registeredTable).operations().current().metadataFileLocation();
    assertThat(metadataLocation).isEqualTo(expectedMetadataLocation);
    assertThat(ecsCatalog.loadTable(identifier)).isNotNull();
    assertThat(ecsCatalog.dropTable(identifier, true)).isTrue();
  }

  @Test
  public void testRegisterExistingTable() {
    TableIdentifier identifier = TableIdentifier.of("a", "t1");
    ecsCatalog.createTable(identifier, SCHEMA);
    Table registeringTable = ecsCatalog.loadTable(identifier);
    TableOperations ops = ((HasTableOperations) registeringTable).operations();
    String metadataLocation = ((EcsTableOperations) ops).currentMetadataLocation();
    assertThatThrownBy(() -> ecsCatalog.registerTable(identifier, metadataLocation))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessage("Table already exists: a.t1");
    assertThat(ecsCatalog.dropTable(identifier, true)).isTrue();
  }
}
