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
import org.junit.Assert;
import org.junit.Test;

public class TestResourcePaths {
  private final String prefix = "ws/catalog";
  private final ResourcePaths withPrefix =
      ResourcePaths.forCatalogProperties(ImmutableMap.of("prefix", prefix));
  private final ResourcePaths withoutPrefix = ResourcePaths.forCatalogProperties(ImmutableMap.of());

  @Test
  public void testConfigPath() {
    // prefix does not affect the config route because config is merged into catalog properties
    Assert.assertEquals(ResourcePaths.config(), "v1/config");
  }

  @Test
  public void testNamespaces() {
    Assert.assertEquals("v1/ws/catalog/namespaces", withPrefix.namespaces());
    Assert.assertEquals("v1/namespaces", withoutPrefix.namespaces());
  }

  @Test
  public void testNamespace() {
    Namespace ns = Namespace.of("ns");
    Assert.assertEquals("v1/ws/catalog/namespaces/ns", withPrefix.namespace(ns));
    Assert.assertEquals("v1/namespaces/ns", withoutPrefix.namespace(ns));
  }

  @Test
  public void testNamespaceWithSlash() {
    Namespace ns = Namespace.of("n/s");
    Assert.assertEquals("v1/ws/catalog/namespaces/n%2Fs", withPrefix.namespace(ns));
    Assert.assertEquals("v1/namespaces/n%2Fs", withoutPrefix.namespace(ns));
  }

  @Test
  public void testNamespaceWithMultipartNamespace() {
    Namespace ns = Namespace.of("n", "s");
    Assert.assertEquals("v1/ws/catalog/namespaces/n%1Fs", withPrefix.namespace(ns));
    Assert.assertEquals("v1/namespaces/n%1Fs", withoutPrefix.namespace(ns));
  }

  @Test
  public void testNamespaceProperties() {
    Namespace ns = Namespace.of("ns");
    Assert.assertEquals(
        "v1/ws/catalog/namespaces/ns/properties", withPrefix.namespaceProperties(ns));
    Assert.assertEquals("v1/namespaces/ns/properties", withoutPrefix.namespaceProperties(ns));
  }

  @Test
  public void testNamespacePropertiesWithSlash() {
    Namespace ns = Namespace.of("n/s");
    Assert.assertEquals(
        "v1/ws/catalog/namespaces/n%2Fs/properties", withPrefix.namespaceProperties(ns));
    Assert.assertEquals("v1/namespaces/n%2Fs/properties", withoutPrefix.namespaceProperties(ns));
  }

  @Test
  public void testNamespacePropertiesWithMultipartNamespace() {
    Namespace ns = Namespace.of("n", "s");
    Assert.assertEquals(
        "v1/ws/catalog/namespaces/n%1Fs/properties", withPrefix.namespaceProperties(ns));
    Assert.assertEquals("v1/namespaces/n%1Fs/properties", withoutPrefix.namespaceProperties(ns));
  }

  @Test
  public void testTables() {
    Namespace ns = Namespace.of("ns");
    Assert.assertEquals("v1/ws/catalog/namespaces/ns/tables", withPrefix.tables(ns));
    Assert.assertEquals("v1/namespaces/ns/tables", withoutPrefix.tables(ns));
  }

  @Test
  public void testTablesWithSlash() {
    Namespace ns = Namespace.of("n/s");
    Assert.assertEquals("v1/ws/catalog/namespaces/n%2Fs/tables", withPrefix.tables(ns));
    Assert.assertEquals("v1/namespaces/n%2Fs/tables", withoutPrefix.tables(ns));
  }

  @Test
  public void testTablesWithMultipartNamespace() {
    Namespace ns = Namespace.of("n", "s");
    Assert.assertEquals("v1/ws/catalog/namespaces/n%1Fs/tables", withPrefix.tables(ns));
    Assert.assertEquals("v1/namespaces/n%1Fs/tables", withoutPrefix.tables(ns));
  }

  @Test
  public void testTable() {
    TableIdentifier ident = TableIdentifier.of("ns", "table");
    Assert.assertEquals("v1/ws/catalog/namespaces/ns/tables/table", withPrefix.table(ident));
    Assert.assertEquals("v1/namespaces/ns/tables/table", withoutPrefix.table(ident));
  }

  @Test
  public void testTableWithSlash() {
    TableIdentifier ident = TableIdentifier.of("n/s", "tab/le");
    Assert.assertEquals("v1/ws/catalog/namespaces/n%2Fs/tables/tab%2Fle", withPrefix.table(ident));
    Assert.assertEquals("v1/namespaces/n%2Fs/tables/tab%2Fle", withoutPrefix.table(ident));
  }

  @Test
  public void testTableWithMultipartNamespace() {
    TableIdentifier ident = TableIdentifier.of("n", "s", "table");
    Assert.assertEquals("v1/ws/catalog/namespaces/n%1Fs/tables/table", withPrefix.table(ident));
    Assert.assertEquals("v1/namespaces/n%1Fs/tables/table", withoutPrefix.table(ident));
  }
}
