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

import java.util.Collections;
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;

import static org.apache.iceberg.types.Types.NestedField.required;

public class TestNamespace extends BaseTestIceberg {
  private static final String BRANCH = "test-namespace";

  public TestNamespace() {
    super(BRANCH);
  }

  @Test
  public void testListNamespaces() {
    createTable(TableIdentifier.parse("a.b.c.t1"));
    createTable(TableIdentifier.parse("a.b.t2"));
    createTable(TableIdentifier.parse("a.t3"));
    createTable(TableIdentifier.parse("b.c.t4"));
    createTable(TableIdentifier.parse("b.t5"));
    createTable(TableIdentifier.parse("t6"));

    List<TableIdentifier> tables = catalog.listTables(Namespace.of("a", "b", "c"));
    Assertions.assertThat(tables).isNotNull().hasSize(1);
    tables = catalog.listTables(Namespace.of("a", "b"));
    Assertions.assertThat(tables).isNotNull().hasSize(2);
    tables = catalog.listTables(Namespace.of("a"));
    Assertions.assertThat(tables).isNotNull().hasSize(3);
    tables = catalog.listTables(null);
    Assertions.assertThat(tables).isNotNull().hasSize(6);

    List<Namespace> namespaces = catalog.listNamespaces();
    Assertions.assertThat(namespaces).isNotNull().hasSize(5);
    namespaces = catalog.listNamespaces(Namespace.of("a"));
    Assertions.assertThat(namespaces).isNotNull().hasSize(3);
    namespaces = catalog.listNamespaces(Namespace.of("a", "b"));
    Assertions.assertThat(namespaces).isNotNull().hasSize(2);
    namespaces = catalog.listNamespaces(Namespace.of("b"));
    Assertions.assertThat(namespaces).isNotNull().hasSize(2);
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

    Schema schema = new Schema(Types.StructType.of(required(1, "id", Types.LongType.get())).fields());
    Assertions.assertThat(catalog.createTable(identifier, schema)).isNotNull();

    ContentKey key = NessieUtil.toKey(identifier);
    Assertions.assertThat(api.getContent().key(key).refName(BRANCH).get().get(key).unwrap(IcebergTable.class))
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
    Assertions.assertThatThrownBy(() -> catalog.setProperties(Namespace.of("test"), Collections.emptyMap()))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot set properties for namespace 'test': setProperties is not supported by the NessieCatalog");
  }

  @Test
  public void testRemovingProperties() {
    Assertions.assertThatThrownBy(() -> catalog.removeProperties(Namespace.of("test"), Collections.emptySet()))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage(
            "Cannot remove properties for namespace 'test': removeProperties is not supported by the NessieCatalog");
  }
}
