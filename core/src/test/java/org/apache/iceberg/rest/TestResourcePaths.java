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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestResourcePaths {
  private final String prefix = "ws/catalog";
  private final ResourcePaths withPrefix =
      ResourcePaths.forCatalogProperties(ImmutableMap.of("prefix", prefix));
  private final ResourcePaths withoutPrefix = ResourcePaths.forCatalogProperties(ImmutableMap.of());

  @Test
  public void testConfigPath() {
    // prefix does not affect the config route because config is merged into catalog properties
    assertThat(ResourcePaths.config()).isEqualTo("v1/config");
  }

  @Test
  public void testNamespaces() {
    assertThat(withPrefix.namespaces()).isEqualTo("v1/ws/catalog/namespaces");
    assertThat(withoutPrefix.namespaces()).isEqualTo("v1/namespaces");
  }

  @Test
  public void testNamespace() {
    Namespace ns = Namespace.of("ns");
    assertThat(withPrefix.namespace(ns)).isEqualTo("v1/ws/catalog/namespaces/ns");
    assertThat(withoutPrefix.namespace(ns)).isEqualTo("v1/namespaces/ns");
  }

  @Test
  public void testNamespaceWithSlash() {
    Namespace ns = Namespace.of("n/s");
    assertThat(withPrefix.namespace(ns)).isEqualTo("v1/ws/catalog/namespaces/n%2Fs");
    assertThat(withoutPrefix.namespace(ns)).isEqualTo("v1/namespaces/n%2Fs");
  }

  @Test
  public void testNamespaceWithMultipartNamespace() {
    Namespace ns = Namespace.of("n", "s");
    assertThat(withPrefix.namespace(ns)).isEqualTo("v1/ws/catalog/namespaces/n%1Fs");
    assertThat(withoutPrefix.namespace(ns)).isEqualTo("v1/namespaces/n%1Fs");
  }

  @ParameterizedTest
  @ValueSource(strings = {"%1F", "%2D", "%2E"})
  public void testNamespaceWithMultipartNamespace(String namespaceSeparator) {
    Namespace ns = Namespace.of("n", "s");
    String namespace = String.format("n%ss", namespaceSeparator);
    assertThat(
            ResourcePaths.forCatalogProperties(
                    ImmutableMap.of(
                        "prefix",
                        prefix,
                        RESTCatalogProperties.NAMESPACE_SEPARATOR,
                        namespaceSeparator))
                .namespace(ns))
        .isEqualTo("v1/ws/catalog/namespaces/" + namespace);

    assertThat(
            ResourcePaths.forCatalogProperties(
                    ImmutableMap.of(RESTCatalogProperties.NAMESPACE_SEPARATOR, namespaceSeparator))
                .namespace(ns))
        .isEqualTo("v1/namespaces/" + namespace);
  }

  @ParameterizedTest
  @ValueSource(strings = {"%1F", "%2D", "%2E"})
  public void testNamespaceWithDot(String namespaceSeparator) {
    Namespace ns = Namespace.of("n.s", "a.b");
    String namespace = String.format("n.s%sa.b", namespaceSeparator);
    assertThat(
            ResourcePaths.forCatalogProperties(
                    ImmutableMap.of(
                        "prefix",
                        prefix,
                        RESTCatalogProperties.NAMESPACE_SEPARATOR,
                        namespaceSeparator))
                .namespace(ns))
        .isEqualTo("v1/ws/catalog/namespaces/" + namespace);

    assertThat(
            ResourcePaths.forCatalogProperties(
                    ImmutableMap.of(RESTCatalogProperties.NAMESPACE_SEPARATOR, namespaceSeparator))
                .namespace(ns))
        .isEqualTo("v1/namespaces/" + namespace);
  }

  @Test
  public void nestedNamespaceWithLegacySeparator() {
    Namespace namespace = Namespace.of("first", "second", "third");
    String legacySeparator = RESTUtil.NAMESPACE_SEPARATOR_URLENCODED_UTF_8;
    String newSeparator = RESTCatalogAdapter.NAMESPACE_SEPARATOR_URLENCODED_UTF_8;

    // legacy separator is always used by default, so no need to configure it
    ResourcePaths pathsWithLegacySeparator = ResourcePaths.forCatalogProperties(ImmutableMap.of());

    // Encode namespace using legacy separator. No need to provide the separator to encodeNamespace
    String legacyEncodedNamespace = RESTUtil.encodeNamespace(namespace);
    assertThat(pathsWithLegacySeparator.namespace(namespace))
        .contains(legacyEncodedNamespace)
        .contains(legacySeparator);

    // Decode the namespace containing legacy separator without providing the separator
    assertThat(RESTUtil.decodeNamespace(legacyEncodedNamespace)).isEqualTo(namespace);

    // Decode the namespace containing legacy separator with providing the new separator
    assertThat(RESTUtil.decodeNamespace(legacyEncodedNamespace, newSeparator)).isEqualTo(namespace);
  }

  @Test
  public void nestedNamespaceWithNewSeparator() {
    Namespace namespace = Namespace.of("first", "second", "third");
    String newSeparator = RESTCatalogAdapter.NAMESPACE_SEPARATOR_URLENCODED_UTF_8;

    ResourcePaths pathsWithNewSeparator =
        ResourcePaths.forCatalogProperties(
            ImmutableMap.of(RESTCatalogProperties.NAMESPACE_SEPARATOR, newSeparator));

    // Encode namespace using new separator
    String newEncodedSeparator = RESTUtil.encodeNamespace(namespace, newSeparator);
    assertThat(pathsWithNewSeparator.namespace(namespace))
        .contains(newEncodedSeparator)
        .contains(newSeparator);

    // Decode the namespace containing new separator with explicitly providing the separator
    assertThat(RESTUtil.decodeNamespace(newEncodedSeparator, newSeparator)).isEqualTo(namespace);
  }

  @Test
  public void nestedNamespaceAsPathSegmentWithCustomSeparator() {
    Namespace namespace = Namespace.of("first second", "third");
    String separator = RESTCatalogAdapter.NAMESPACE_SEPARATOR_URLENCODED_UTF_8;

    ResourcePaths pathsWithCustomSeparator =
        ResourcePaths.forCatalogProperties(
            ImmutableMap.of(RESTCatalogProperties.NAMESPACE_SEPARATOR, separator));

    String actual = pathsWithCustomSeparator.namespace(namespace);
    assertThat(actual)
        .contains(RESTUtil.encodeNamespaceAsPathSegment(namespace, separator))
        .contains(separator)
        .contains("%20")
        .doesNotContain("+");
  }

  @Test
  public void testNamespaceProperties() {
    Namespace ns = Namespace.of("ns");
    assertThat(withPrefix.namespaceProperties(ns))
        .isEqualTo("v1/ws/catalog/namespaces/ns/properties");
    assertThat(withoutPrefix.namespaceProperties(ns)).isEqualTo("v1/namespaces/ns/properties");
  }

  @Test
  public void testNamespacePropertiesWithSlash() {
    Namespace ns = Namespace.of("n/s");
    assertThat(withPrefix.namespaceProperties(ns))
        .isEqualTo("v1/ws/catalog/namespaces/n%2Fs/properties");
    assertThat(withoutPrefix.namespaceProperties(ns)).isEqualTo("v1/namespaces/n%2Fs/properties");
  }

  @Test
  public void testNamespacePropertiesWithMultipartNamespace() {
    Namespace ns = Namespace.of("n", "s");
    assertThat(withPrefix.namespaceProperties(ns))
        .isEqualTo("v1/ws/catalog/namespaces/n%1Fs/properties");
    assertThat(withoutPrefix.namespaceProperties(ns)).isEqualTo("v1/namespaces/n%1Fs/properties");
  }

  @Test
  public void testTables() {
    Namespace ns = Namespace.of("ns");
    assertThat(withPrefix.tables(ns)).isEqualTo("v1/ws/catalog/namespaces/ns/tables");
    assertThat(withoutPrefix.tables(ns)).isEqualTo("v1/namespaces/ns/tables");
  }

  @Test
  public void testTablesWithSlash() {
    Namespace ns = Namespace.of("n/s");
    assertThat(withPrefix.tables(ns)).isEqualTo("v1/ws/catalog/namespaces/n%2Fs/tables");
    assertThat(withoutPrefix.tables(ns)).isEqualTo("v1/namespaces/n%2Fs/tables");
  }

  @Test
  public void testTablesWithMultipartNamespace() {
    Namespace ns = Namespace.of("n", "s");
    assertThat(withPrefix.tables(ns)).isEqualTo("v1/ws/catalog/namespaces/n%1Fs/tables");
    assertThat(withoutPrefix.tables(ns)).isEqualTo("v1/namespaces/n%1Fs/tables");
  }

  @Test
  public void testTable() {
    TableIdentifier ident = TableIdentifier.of("ns", "table");
    assertThat(withPrefix.table(ident)).isEqualTo("v1/ws/catalog/namespaces/ns/tables/table");
    assertThat(withoutPrefix.table(ident)).isEqualTo("v1/namespaces/ns/tables/table");
  }

  @Test
  public void testTableWithSlash() {
    TableIdentifier ident = TableIdentifier.of("n/s", "tab/le");
    assertThat(withPrefix.table(ident)).isEqualTo("v1/ws/catalog/namespaces/n%2Fs/tables/tab%2Fle");
    assertThat(withoutPrefix.table(ident)).isEqualTo("v1/namespaces/n%2Fs/tables/tab%2Fle");
  }

  @Test
  public void testTableWithMultipartNamespace() {
    TableIdentifier ident = TableIdentifier.of("n", "s", "table");
    assertThat(withPrefix.table(ident)).isEqualTo("v1/ws/catalog/namespaces/n%1Fs/tables/table");
    assertThat(withoutPrefix.table(ident)).isEqualTo("v1/namespaces/n%1Fs/tables/table");
  }

  @Test
  public void testNamespaceWithSpace() {
    Namespace ns = Namespace.of("n s");
    assertThat(withPrefix.namespace(ns)).isEqualTo("v1/ws/catalog/namespaces/n%20s");
    assertThat(withoutPrefix.namespace(ns)).isEqualTo("v1/namespaces/n%20s");
  }

  @Test
  public void testMultipartNamespaceWithSpace() {
    Namespace ns = Namespace.of("n s", "a b");
    assertThat(withPrefix.namespace(ns)).isEqualTo("v1/ws/catalog/namespaces/n%20s%1Fa%20b");
    assertThat(withoutPrefix.namespace(ns)).isEqualTo("v1/namespaces/n%20s%1Fa%20b");
  }

  @Test
  public void testNamespaceWithPlusSign() {
    Namespace ns = Namespace.of("n+s");
    assertThat(withPrefix.namespace(ns)).isEqualTo("v1/ws/catalog/namespaces/n%2Bs");
    assertThat(withoutPrefix.namespace(ns)).isEqualTo("v1/namespaces/n%2Bs");
  }

  @Test
  public void testMultipartNamespaceWithPlusSign() {
    Namespace ns = Namespace.of("n+s", "a+b");
    assertThat(withPrefix.namespace(ns)).isEqualTo("v1/ws/catalog/namespaces/n%2Bs%1Fa%2Bb");
    assertThat(withoutPrefix.namespace(ns)).isEqualTo("v1/namespaces/n%2Bs%1Fa%2Bb");
  }

  @Test
  public void testTableWithSpace() {
    TableIdentifier ident = TableIdentifier.of("ns", "my table");
    assertThat(withPrefix.table(ident)).isEqualTo("v1/ws/catalog/namespaces/ns/tables/my%20table");
    assertThat(withoutPrefix.table(ident)).isEqualTo("v1/namespaces/ns/tables/my%20table");
  }

  @Test
  public void testTableWithPlusSign() {
    TableIdentifier ident = TableIdentifier.of("ns", "a+b");
    assertThat(withPrefix.table(ident)).isEqualTo("v1/ws/catalog/namespaces/ns/tables/a%2Bb");
    assertThat(withoutPrefix.table(ident)).isEqualTo("v1/namespaces/ns/tables/a%2Bb");
  }

  @Test
  public void testViewWithSpace() {
    TableIdentifier ident = TableIdentifier.of("ns", "my view");
    assertThat(withPrefix.view(ident)).isEqualTo("v1/ws/catalog/namespaces/ns/views/my%20view");
    assertThat(withoutPrefix.view(ident)).isEqualTo("v1/namespaces/ns/views/my%20view");
  }

  @Test
  public void testViewWithPlusSign() {
    TableIdentifier ident = TableIdentifier.of("ns", "a+b");
    assertThat(withPrefix.view(ident)).isEqualTo("v1/ws/catalog/namespaces/ns/views/a%2Bb");
    assertThat(withoutPrefix.view(ident)).isEqualTo("v1/namespaces/ns/views/a%2Bb");
  }

  @Test
  public void testRegister() {
    Namespace ns = Namespace.of("ns");
    assertThat(withPrefix.register(ns)).isEqualTo("v1/ws/catalog/namespaces/ns/register");
    assertThat(withoutPrefix.register(ns)).isEqualTo("v1/namespaces/ns/register");
  }

  @Test
  public void views() {
    Namespace ns = Namespace.of("ns");
    assertThat(withPrefix.views(ns)).isEqualTo("v1/ws/catalog/namespaces/ns/views");
    assertThat(withoutPrefix.views(ns)).isEqualTo("v1/namespaces/ns/views");
  }

  @Test
  public void viewsWithSlash() {
    Namespace ns = Namespace.of("n/s");
    assertThat(withPrefix.views(ns)).isEqualTo("v1/ws/catalog/namespaces/n%2Fs/views");
    assertThat(withoutPrefix.views(ns)).isEqualTo("v1/namespaces/n%2Fs/views");
  }

  @Test
  public void viewsWithMultipartNamespace() {
    Namespace ns = Namespace.of("n", "s");
    assertThat(withPrefix.views(ns)).isEqualTo("v1/ws/catalog/namespaces/n%1Fs/views");
    assertThat(withoutPrefix.views(ns)).isEqualTo("v1/namespaces/n%1Fs/views");
  }

  @Test
  public void view() {
    TableIdentifier ident = TableIdentifier.of("ns", "view-name");
    assertThat(withPrefix.view(ident)).isEqualTo("v1/ws/catalog/namespaces/ns/views/view-name");
    assertThat(withoutPrefix.view(ident)).isEqualTo("v1/namespaces/ns/views/view-name");
  }

  @Test
  public void viewWithSlash() {
    TableIdentifier ident = TableIdentifier.of("n/s", "vi/ew-name");
    assertThat(withPrefix.view(ident))
        .isEqualTo("v1/ws/catalog/namespaces/n%2Fs/views/vi%2Few-name");
    assertThat(withoutPrefix.view(ident)).isEqualTo("v1/namespaces/n%2Fs/views/vi%2Few-name");
  }

  @Test
  public void viewWithMultipartNamespace() {
    TableIdentifier ident = TableIdentifier.of("n", "s", "view-name");
    assertThat(withPrefix.view(ident)).isEqualTo("v1/ws/catalog/namespaces/n%1Fs/views/view-name");
    assertThat(withoutPrefix.view(ident)).isEqualTo("v1/namespaces/n%1Fs/views/view-name");
  }

  @Test
  public void testRegisterView() {
    Namespace ns = Namespace.of("ns");
    assertThat(withPrefix.registerView(ns)).isEqualTo("v1/ws/catalog/namespaces/ns/register-view");
    assertThat(withoutPrefix.registerView(ns)).isEqualTo("v1/namespaces/ns/register-view");
  }

  @Test
  public void planEndpointPath() {
    TableIdentifier tableId = TableIdentifier.of("test_namespace", "test_table");

    assertThat(withPrefix.planTableScan(tableId))
        .isEqualTo("v1/ws/catalog/namespaces/test_namespace/tables/test_table/plan");
    assertThat(withoutPrefix.planTableScan(tableId))
        .isEqualTo("v1/namespaces/test_namespace/tables/test_table/plan");

    // Test with different identifiers
    TableIdentifier complexId = TableIdentifier.of(Namespace.of("db", "schema"), "my_table");
    assertThat(withPrefix.planTableScan(complexId))
        .isEqualTo("v1/ws/catalog/namespaces/db%1Fschema/tables/my_table/plan");
    assertThat(withoutPrefix.planTableScan(complexId))
        .isEqualTo("v1/namespaces/db%1Fschema/tables/my_table/plan");
  }

  @Test
  public void fetchScanTasksPath() {
    TableIdentifier tableId = TableIdentifier.of("test_namespace", "test_table");

    assertThat(withPrefix.fetchScanTasks(tableId))
        .isEqualTo("v1/ws/catalog/namespaces/test_namespace/tables/test_table/tasks");
    assertThat(withoutPrefix.fetchScanTasks(tableId))
        .isEqualTo("v1/namespaces/test_namespace/tables/test_table/tasks");

    // Test with different identifiers
    TableIdentifier complexId = TableIdentifier.of(Namespace.of("db", "schema"), "my_table");
    assertThat(withPrefix.fetchScanTasks(complexId))
        .isEqualTo("v1/ws/catalog/namespaces/db%1Fschema/tables/my_table/tasks");
    assertThat(withoutPrefix.fetchScanTasks(complexId))
        .isEqualTo("v1/namespaces/db%1Fschema/tables/my_table/tasks");
  }

  @Test
  public void cancelPlanEndpointPath() {
    TableIdentifier tableId = TableIdentifier.of("test_namespace", "test_table");
    String planId = "plan-abc-123";

    assertThat(withPrefix.plan(tableId, planId))
        .isEqualTo("v1/ws/catalog/namespaces/test_namespace/tables/test_table/plan/plan-abc-123");
    assertThat(withoutPrefix.plan(tableId, planId))
        .isEqualTo("v1/namespaces/test_namespace/tables/test_table/plan/plan-abc-123");

    // The planId contains a space which needs to be encoded
    String spaceSeparatedPlanId = "plan with spaces";
    // The expected encoded version of the planId (RFC 3986: space -> %20)
    String encodedPlanId = "plan%20with%20spaces";

    assertThat(withPrefix.plan(tableId, spaceSeparatedPlanId))
        .isEqualTo(
            "v1/ws/catalog/namespaces/test_namespace/tables/test_table/plan/" + encodedPlanId);
    assertThat(withoutPrefix.plan(tableId, spaceSeparatedPlanId))
        .isEqualTo("v1/namespaces/test_namespace/tables/test_table/plan/" + encodedPlanId);

    // Test with different identifiers
    TableIdentifier complexId = TableIdentifier.of(Namespace.of("db", "schema"), "my_table");
    assertThat(withPrefix.plan(complexId, "plan-xyz-789"))
        .isEqualTo("v1/ws/catalog/namespaces/db%1Fschema/tables/my_table/plan/plan-xyz-789");
    assertThat(withoutPrefix.plan(complexId, "plan-xyz-789"))
        .isEqualTo("v1/namespaces/db%1Fschema/tables/my_table/plan/plan-xyz-789");
  }

  @Test
  public void testRemoteSign() {
    TableIdentifier tableId = TableIdentifier.of("test_namespace", "test_table");
    assertThat(withPrefix.remoteSign(tableId))
        .isEqualTo("v1/ws/catalog/namespaces/test_namespace/tables/test_table/sign");
    assertThat(withoutPrefix.remoteSign(tableId))
        .isEqualTo("v1/namespaces/test_namespace/tables/test_table/sign");

    // Test with different identifiers
    TableIdentifier complexId = TableIdentifier.of(Namespace.of("db", "schema"), "my_table");
    assertThat(withPrefix.remoteSign(complexId))
        .isEqualTo("v1/ws/catalog/namespaces/db%1Fschema/tables/my_table/sign");
    assertThat(withoutPrefix.remoteSign(complexId))
        .isEqualTo("v1/namespaces/db%1Fschema/tables/my_table/sign");
  }
}
