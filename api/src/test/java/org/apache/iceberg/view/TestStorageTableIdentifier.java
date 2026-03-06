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
package org.apache.iceberg.view;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Test;

public class TestStorageTableIdentifier {

  @Test
  public void testOfWithoutCatalog() {
    Namespace ns = Namespace.of("db", "schema");
    StorageTableIdentifier id = StorageTableIdentifier.of(ns, "mv_storage");

    assertThat(id.namespace()).isEqualTo(ns);
    assertThat(id.name()).isEqualTo("mv_storage");
    assertThat(id.catalog()).isNull();
    assertThat(id.hasCatalog()).isFalse();
  }

  @Test
  public void testOfWithCatalog() {
    Namespace ns = Namespace.of("db");
    StorageTableIdentifier id = StorageTableIdentifier.of("my_catalog", ns, "mv_storage");

    assertThat(id.namespace()).isEqualTo(ns);
    assertThat(id.name()).isEqualTo("mv_storage");
    assertThat(id.catalog()).isEqualTo("my_catalog");
    assertThat(id.hasCatalog()).isTrue();
  }

  @Test
  public void testFromTableIdentifier() {
    TableIdentifier tableId = TableIdentifier.of(Namespace.of("db"), "tbl");
    StorageTableIdentifier id = StorageTableIdentifier.from(tableId);

    assertThat(id.namespace()).isEqualTo(tableId.namespace());
    assertThat(id.name()).isEqualTo(tableId.name());
    assertThat(id.catalog()).isNull();
    assertThat(id.hasCatalog()).isFalse();
  }

  @Test
  public void testToTableIdentifier() {
    Namespace ns = Namespace.of("db");
    StorageTableIdentifier id = StorageTableIdentifier.of("my_catalog", ns, "mv_storage");
    TableIdentifier tableId = id.toTableIdentifier();

    assertThat(tableId.namespace()).isEqualTo(ns);
    assertThat(tableId.name()).isEqualTo("mv_storage");
  }

  @Test
  public void testToStringWithoutCatalog() {
    StorageTableIdentifier id = StorageTableIdentifier.of(Namespace.of("db", "schema"), "mv_storage");
    assertThat(id.toString()).isEqualTo("db.schema.mv_storage");
  }

  @Test
  public void testToStringWithCatalog() {
    StorageTableIdentifier id =
        StorageTableIdentifier.of("my_catalog", Namespace.of("db"), "mv_storage");
    assertThat(id.toString()).isEqualTo("my_catalog.db.mv_storage");
  }

  @Test
  public void testToStringEmptyNamespace() {
    StorageTableIdentifier id = StorageTableIdentifier.of(Namespace.empty(), "mv_storage");
    assertThat(id.toString()).isEqualTo("mv_storage");
  }

  @Test
  public void testEquality() {
    Namespace ns = Namespace.of("db");
    StorageTableIdentifier id1 = StorageTableIdentifier.of(ns, "mv_storage");
    StorageTableIdentifier id2 = StorageTableIdentifier.of(ns, "mv_storage");
    StorageTableIdentifier id3 = StorageTableIdentifier.of("cat", ns, "mv_storage");

    assertThat(id1).isEqualTo(id2);
    assertThat(id1).isNotEqualTo(id3);
    assertThat(id1.hashCode()).isEqualTo(id2.hashCode());
  }

  @Test
  public void testInvalidNullName() {
    assertThatThrownBy(() -> StorageTableIdentifier.of(Namespace.empty(), null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid storage table name: null or empty");
  }

  @Test
  public void testInvalidEmptyName() {
    assertThatThrownBy(() -> StorageTableIdentifier.of(Namespace.empty(), ""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid storage table name: null or empty");
  }

  @Test
  public void testInvalidNullNamespace() {
    assertThatThrownBy(() -> StorageTableIdentifier.of(null, "mv_storage"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid namespace: null");
  }

  @Test
  public void testFromNullTableIdentifier() {
    assertThatThrownBy(() -> StorageTableIdentifier.from(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot create StorageTableIdentifier from null TableIdentifier");
  }
}
