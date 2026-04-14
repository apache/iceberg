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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class TestCatalogObjectIdentifier {

  @Test
  void testNullNamespace() {
    assertThatThrownBy(() -> CatalogObjectIdentifier.of(null, "table"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid namespace: null");
  }

  @Test
  void testNullName() {
    assertThatThrownBy(() -> CatalogObjectIdentifier.of(Namespace.of("ns"), null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid object name: null or empty");
  }

  @Test
  void testEmptyName() {
    assertThatThrownBy(() -> CatalogObjectIdentifier.of(Namespace.of("ns"), ""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid object name: null or empty");
  }

  @Test
  void testNullTableIdentifier() {
    assertThatThrownBy(() -> CatalogObjectIdentifier.of((TableIdentifier) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot create CatalogObjectIdentifier from null identifier");
  }

  @Test
  void testWithNamespace() {
    CatalogObjectIdentifier id =
        CatalogObjectIdentifier.of(Namespace.of("db", "schema"), "my_table");
    assertThat(id.namespace()).isEqualTo(Namespace.of("db", "schema"));
    assertThat(id.name()).isEqualTo("my_table");
    assertThat(id.hasNamespace()).isTrue();
    assertThat(id).hasToString("db.schema.my_table");
  }

  @Test
  void testWithEmptyNamespace() {
    CatalogObjectIdentifier id = CatalogObjectIdentifier.of(Namespace.empty(), "my_table");
    assertThat(id.namespace()).isEqualTo(Namespace.empty());
    assertThat(id.name()).isEqualTo("my_table");
    assertThat(id.hasNamespace()).isFalse();
    assertThat(id).hasToString("my_table");
  }

  @Test
  void testNamespaceObject() {
    CatalogObjectIdentifier id = CatalogObjectIdentifier.of(Namespace.of("ns1"), "ns2");
    assertThat(id.namespace()).isEqualTo(Namespace.of("ns1"));
    assertThat(id.name()).isEqualTo("ns2");
  }

  @Test
  void testFromTableIdentifier() {
    TableIdentifier tableId = TableIdentifier.of(Namespace.of("db"), "table");
    CatalogObjectIdentifier id = CatalogObjectIdentifier.of(tableId);
    assertThat(id.namespace()).isEqualTo(Namespace.of("db"));
    assertThat(id.name()).isEqualTo("table");
  }

  @Test
  void testToTableIdentifier() {
    CatalogObjectIdentifier id = CatalogObjectIdentifier.of(Namespace.of("db"), "table");
    TableIdentifier tableId = id.toTableIdentifier();
    assertThat(tableId.namespace()).isEqualTo(Namespace.of("db"));
    assertThat(tableId.name()).isEqualTo("table");
  }

  @Test
  void testEquality() {
    CatalogObjectIdentifier id1 = CatalogObjectIdentifier.of(Namespace.of("a"), "b");
    CatalogObjectIdentifier id2 = CatalogObjectIdentifier.of(Namespace.of("a"), "b");
    assertThat(id1).isEqualTo(id2);
    assertThat(id1.hashCode()).isEqualTo(id2.hashCode());
  }

  @Test
  void testInequality() {
    CatalogObjectIdentifier id1 = CatalogObjectIdentifier.of(Namespace.of("a"), "b");
    CatalogObjectIdentifier id2 = CatalogObjectIdentifier.of(Namespace.of("a"), "c");
    CatalogObjectIdentifier id3 = CatalogObjectIdentifier.of(Namespace.of("x"), "b");
    assertThat(id1).isNotEqualTo(id2);
    assertThat(id1).isNotEqualTo(id3);
  }
}
