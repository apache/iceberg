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
package org.apache.iceberg.nessie;

import static org.apache.iceberg.types.Types.NestedField.required;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;

public class TestNamespace extends BaseTestIceberg {
  private static final String BRANCH = "test-namespace";

  public TestNamespace() {
    super(BRANCH);
  }

  @Test
  public void testListNamespaces() {
    Namespace nsA = Namespace.of("a");
    Namespace nsAB = Namespace.of("a", "b");
    Namespace nsABC = Namespace.of("a", "b", "c");
    Namespace nsB = Namespace.of("b");
    Namespace nsBC = Namespace.of("b", "c");

    createTable(TableIdentifier.of(nsABC, "t1"));
    createTable(TableIdentifier.of(nsAB, "t2"));
    createTable(TableIdentifier.of(nsA, "t3"));
    createTable(TableIdentifier.of(nsBC, "t4"));
    createTable(TableIdentifier.of(nsB, "t5"));
    createTable(TableIdentifier.of("t6"));

    List<TableIdentifier> tables = catalog.listTables(nsABC);
    Assertions.assertThat(tables).isNotNull().hasSize(1);
    tables = catalog.listTables(nsAB);
    Assertions.assertThat(tables).isNotNull().hasSize(2);
    tables = catalog.listTables(nsA);
    Assertions.assertThat(tables).isNotNull().hasSize(3);
    tables = catalog.listTables(null);
    Assertions.assertThat(tables).isNotNull().hasSize(6);

    List<Namespace> namespaces = catalog.listNamespaces();
    Assertions.assertThat(namespaces).containsExactly(nsA, nsB);
    namespaces = catalog.listNamespaces(nsA);
    Assertions.assertThat(namespaces).containsExactly(nsAB);
    namespaces = catalog.listNamespaces(nsAB);
    Assertions.assertThat(namespaces).containsExactly(nsABC);
    namespaces = catalog.listNamespaces(nsB);
    Assertions.assertThat(namespaces).containsExactly(nsBC);
  }

  @Test
  public void testCreatingAndDroppingNamespace() {
    Namespace namespace = Namespace.of("test");
    catalog.createNamespace(namespace, ImmutableMap.of());
    Assertions.assertThat(catalog.namespaceExists(namespace)).isTrue();
    catalog.dropNamespace(namespace);
    Assertions.assertThat(catalog.namespaceExists(namespace)).isFalse();
  }

  @Test
  public void testCreatingAndDroppingNamespaceWithContent() throws NessieNotFoundException {
    Namespace namespace = Namespace.of("test");
    catalog.createNamespace(namespace, ImmutableMap.of());
    Assertions.assertThat(catalog.namespaceExists(namespace)).isTrue();
    TableIdentifier identifier = TableIdentifier.of(namespace, "tbl");

    Schema schema =
        new Schema(Types.StructType.of(required(1, "id", Types.LongType.get())).fields());
    Assertions.assertThat(catalog.createTable(identifier, schema)).isNotNull();

    ContentKey key = NessieUtil.toKey(identifier);
    Assertions.assertThat(
            api.getContent().key(key).refName(BRANCH).get().get(key).unwrap(IcebergTable.class))
        .isPresent();

    Assertions.assertThatThrownBy(() -> catalog.dropNamespace(namespace))
        .isInstanceOf(NamespaceNotEmptyException.class)
        .hasMessage("Namespace 'test' is not empty. One or more tables exist.");

    catalog.dropTable(identifier, true);
    catalog.dropNamespace(namespace);
    Assertions.assertThat(catalog.namespaceExists(namespace)).isFalse();
  }

  @Test
  public void testSettingProperties() {
    Map<String, String> properties = ImmutableMap.of("prop", "val");
    Namespace namespace = Namespace.of("withProperties");
    catalog.createNamespace(namespace, properties);
    Assertions.assertThat(catalog.namespaceExists(namespace)).isTrue();
    Assertions.assertThat(catalog.loadNamespaceMetadata(namespace)).isEqualTo(properties);

    ImmutableMap<String, String> updatedProperties =
        ImmutableMap.of("prop2", "val2", "prop", "new_val");
    catalog.setProperties(namespace, updatedProperties);
    Assertions.assertThat(catalog.loadNamespaceMetadata(namespace)).isEqualTo(updatedProperties);

    Assertions.assertThatThrownBy(
            () -> catalog.setProperties(Namespace.of("unknown"), updatedProperties))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessage("Namespace does not exist: unknown");
  }

  @Test
  public void testRemovingProperties() {
    Map<String, String> properties = ImmutableMap.of("prop2", "val2", "prop", "val");
    Namespace namespace = Namespace.of("withPropertyDeletes");
    catalog.createNamespace(namespace, properties);
    Assertions.assertThat(catalog.namespaceExists(namespace)).isTrue();
    Assertions.assertThat(catalog.loadNamespaceMetadata(namespace)).isEqualTo(properties);

    Set<String> toRemove = Sets.newHashSet(Arrays.asList("prop1", "prop2", "prop3"));
    catalog.removeProperties(namespace, toRemove);
    Assertions.assertThat(catalog.loadNamespaceMetadata(namespace))
        .isEqualTo(ImmutableMap.of("prop", "val"));

    Assertions.assertThatThrownBy(() -> catalog.removeProperties(Namespace.of("unknown"), toRemove))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessage("Namespace does not exist: unknown");
  }

  @Test
  public void testCustomLocation() {
    Map<String, String> properties = ImmutableMap.of("location", "/custom/location");
    Namespace namespaceWithLocation = Namespace.of("withLocation");
    catalog.createNamespace(namespaceWithLocation, properties);
    Assertions.assertThat(catalog.namespaceExists(namespaceWithLocation)).isTrue();
    Assertions.assertThat(
            catalog.defaultWarehouseLocation(TableIdentifier.of("withLocation", "testTable")))
        .startsWith("/custom/location/testTable");
    Namespace namespaceWithoutLocation = Namespace.of("withoutLocation");
    catalog.createNamespace(namespaceWithoutLocation, ImmutableMap.of());
    Assertions.assertThat(catalog.namespaceExists(namespaceWithoutLocation)).isTrue();
    Assertions.assertThat(
            catalog.defaultWarehouseLocation(TableIdentifier.of("withoutLocation", "testTable")))
        .contains("/withoutLocation/testTable");
    Assertions.assertThat(
            catalog.defaultWarehouseLocation(TableIdentifier.of("badNamespace", "testTable")))
        .contains("/badNamespace/testTable");
  }
}
