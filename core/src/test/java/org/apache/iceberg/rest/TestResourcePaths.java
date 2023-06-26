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
package org.apache.iceberg.rest;

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestResourcePaths {
  private final String prefix = "ws/catalog";
  private final ResourcePaths withPrefix =
      ResourcePaths.forCatalogProperties(ImmutableMap.of("prefix", prefix));
  private final ResourcePaths withoutPrefix = ResourcePaths.forCatalogProperties(ImmutableMap.of());

  @Test
  public void testConfigPath() {
    // prefix does not affect the config route because config is merged into catalog properties
    Assertions.assertThat(ResourcePaths.config()).isEqualTo("v1/config");
  }

  @Test
  public void testNamespaces() {
    Assertions.assertThat(withPrefix.namespaces()).isEqualTo("v1/ws/catalog/namespaces");
    Assertions.assertThat(withoutPrefix.namespaces()).isEqualTo("v1/namespaces");
  }

  @Test
  public void testNamespace() {
    Namespace ns = Namespace.of("ns");
    Assertions.assertThat(withPrefix.namespace(ns)).isEqualTo("v1/ws/catalog/namespaces/ns");
    Assertions.assertThat(withoutPrefix.namespace(ns)).isEqualTo("v1/namespaces/ns");
  }

  @Test
  public void testNamespaceWithSlash() {
    Namespace ns = Namespace.of("n/s");
    Assertions.assertThat(withPrefix.namespace(ns)).isEqualTo("v1/ws/catalog/namespaces/n%2Fs");
    Assertions.assertThat(withoutPrefix.namespace(ns)).isEqualTo("v1/namespaces/n%2Fs");
  }

  @Test
  public void testNamespaceWithMultipartNamespace() {
    Namespace ns = Namespace.of("n", "s");
    Assertions.assertThat(withPrefix.namespace(ns)).isEqualTo("v1/ws/catalog/namespaces/n%1Fs");
    Assertions.assertThat(withoutPrefix.namespace(ns)).isEqualTo("v1/namespaces/n%1Fs");
  }

  @Test
  public void testNamespaceProperties() {
    Namespace ns = Namespace.of("ns");
    Assertions.assertThat(withPrefix.namespaceProperties(ns))
        .isEqualTo("v1/ws/catalog/namespaces/ns/properties");
    Assertions.assertThat(withoutPrefix.namespaceProperties(ns))
        .isEqualTo("v1/namespaces/ns/properties");
  }

  @Test
  public void testNamespacePropertiesWithSlash() {
    Namespace ns = Namespace.of("n/s");
    Assertions.assertThat(withPrefix.namespaceProperties(ns))
        .isEqualTo("v1/ws/catalog/namespaces/n%2Fs/properties");
    Assertions.assertThat(withoutPrefix.namespaceProperties(ns))
        .isEqualTo("v1/namespaces/n%2Fs/properties");
  }

  @Test
  public void testNamespacePropertiesWithMultipartNamespace() {
    Namespace ns = Namespace.of("n", "s");
    Assertions.assertThat(withPrefix.namespaceProperties(ns))
        .isEqualTo("v1/ws/catalog/namespaces/n%1Fs/properties");
    Assertions.assertThat(withoutPrefix.namespaceProperties(ns))
        .isEqualTo("v1/namespaces/n%1Fs/properties");
  }

  @Test
  public void testTables() {
    Namespace ns = Namespace.of("ns");
    Assertions.assertThat(withPrefix.tables(ns)).isEqualTo("v1/ws/catalog/namespaces/ns/tables");
    Assertions.assertThat(withoutPrefix.tables(ns)).isEqualTo("v1/namespaces/ns/tables");
  }

  @Test
  public void testTablesWithSlash() {
    Namespace ns = Namespace.of("n/s");
    Assertions.assertThat(withPrefix.tables(ns)).isEqualTo("v1/ws/catalog/namespaces/n%2Fs/tables");
    Assertions.assertThat(withoutPrefix.tables(ns)).isEqualTo("v1/namespaces/n%2Fs/tables");
  }

  @Test
  public void testTablesWithMultipartNamespace() {
    Namespace ns = Namespace.of("n", "s");
    Assertions.assertThat(withPrefix.tables(ns)).isEqualTo("v1/ws/catalog/namespaces/n%1Fs/tables");
    Assertions.assertThat(withoutPrefix.tables(ns)).isEqualTo("v1/namespaces/n%1Fs/tables");
  }

  @Test
  public void testTable() {
    TableIdentifier ident = TableIdentifier.of("ns", "table");
    Assertions.assertThat(withPrefix.table(ident))
        .isEqualTo("v1/ws/catalog/namespaces/ns/tables/table");
    Assertions.assertThat(withoutPrefix.table(ident)).isEqualTo("v1/namespaces/ns/tables/table");
  }

  @Test
  public void testTableWithSlash() {
    TableIdentifier ident = TableIdentifier.of("n/s", "tab/le");
    Assertions.assertThat(withPrefix.table(ident))
        .isEqualTo("v1/ws/catalog/namespaces/n%2Fs/tables/tab%2Fle");
    Assertions.assertThat(withoutPrefix.table(ident))
        .isEqualTo("v1/namespaces/n%2Fs/tables/tab%2Fle");
  }

  @Test
  public void testTableWithMultipartNamespace() {
    TableIdentifier ident = TableIdentifier.of("n", "s", "table");
    Assertions.assertThat(withPrefix.table(ident))
        .isEqualTo("v1/ws/catalog/namespaces/n%1Fs/tables/table");
    Assertions.assertThat(withoutPrefix.table(ident)).isEqualTo("v1/namespaces/n%1Fs/tables/table");
  }
}
