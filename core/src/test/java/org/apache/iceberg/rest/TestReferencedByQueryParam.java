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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.ContextAwareTableCatalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class TestReferencedByQueryParam {

  private final RESTSessionCatalog catalog = new RESTSessionCatalog(config -> null, null);

  @Test
  public void testSingleViewSimpleNamespace() {
    List<TableIdentifier> chain =
        ImmutableList.of(TableIdentifier.of(Namespace.of("ns"), "viewName"));
    Map<String, Object> context =
        ImmutableMap.of(ContextAwareTableCatalog.VIEW_IDENTIFIER_KEY, chain);

    Map<String, String> result = catalog.referencedByToQueryParam(Map.of(), context);

    assertThat(result).containsEntry("referenced-by", "ns%1FviewName");
  }

  @Test
  public void testSingleViewNestedNamespace() {
    List<TableIdentifier> chain =
        ImmutableList.of(TableIdentifier.of(Namespace.of("prod", "analytics"), "quarterly_view"));
    Map<String, Object> context =
        ImmutableMap.of(ContextAwareTableCatalog.VIEW_IDENTIFIER_KEY, chain);

    Map<String, String> result = catalog.referencedByToQueryParam(Map.of(), context);

    assertThat(result).containsEntry("referenced-by", "prod%1Fanalytics%1Fquarterly_view");
  }

  @Test
  public void testNestedViewChain() {
    List<TableIdentifier> chain =
        ImmutableList.of(
            TableIdentifier.of(Namespace.of("outer_ns"), "outer_view"),
            TableIdentifier.of(Namespace.of("inner_ns"), "inner_view"));
    Map<String, Object> context =
        ImmutableMap.of(ContextAwareTableCatalog.VIEW_IDENTIFIER_KEY, chain);

    Map<String, String> result = catalog.referencedByToQueryParam(Map.of(), context);

    assertThat(result)
        .containsEntry("referenced-by", "outer_ns%1Fouter_view,inner_ns%1Finner_view");
  }

  @Test
  public void testViewNameWithCommaIsEncoded() {
    List<TableIdentifier> chain =
        ImmutableList.of(TableIdentifier.of(Namespace.of("ns"), "view,name"));
    Map<String, Object> context =
        ImmutableMap.of(ContextAwareTableCatalog.VIEW_IDENTIFIER_KEY, chain);

    Map<String, String> result = catalog.referencedByToQueryParam(Map.of(), context);

    // Comma in view name should be encoded as %2C
    assertThat(result).containsEntry("referenced-by", "ns%1Fview%2Cname");
  }

  @Test
  public void testEmptyContextReturnsOriginalParams() {
    Map<String, String> original = ImmutableMap.of("key", "value");

    Map<String, String> result = catalog.referencedByToQueryParam(original, Map.of());

    assertThat(result).isSameAs(original);
  }

  @Test
  public void testContextWithoutViewKeyReturnsOriginalParams() {
    Map<String, String> original = ImmutableMap.of("key", "value");
    Map<String, Object> context = ImmutableMap.of("other-key", "other-value");

    Map<String, String> result = catalog.referencedByToQueryParam(original, context);

    assertThat(result).isSameAs(original);
  }

  @Test
  public void testInvalidContextValueType() {
    Map<String, Object> context =
        ImmutableMap.of(ContextAwareTableCatalog.VIEW_IDENTIFIER_KEY, "not-a-list");

    assertThatThrownBy(() -> catalog.referencedByToQueryParam(Map.of(), context))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("expected List<TableIdentifier>");
  }

  @Test
  public void testEmptyChainReturnsOriginalParams() {
    List<TableIdentifier> chain = ImmutableList.of();
    Map<String, Object> context =
        ImmutableMap.of(ContextAwareTableCatalog.VIEW_IDENTIFIER_KEY, chain);

    Map<String, String> result =
        catalog.referencedByToQueryParam(ImmutableMap.of("key", "value"), context);

    // Should return params without referenced-by since chain is empty
    assertThat(result).containsEntry("key", "value").doesNotContainKey("referenced-by");
  }

  @Test
  public void testExistingParamsArePreserved() {
    List<TableIdentifier> chain = ImmutableList.of(TableIdentifier.of(Namespace.of("ns"), "view"));
    Map<String, Object> context =
        ImmutableMap.of(ContextAwareTableCatalog.VIEW_IDENTIFIER_KEY, chain);
    Map<String, String> original = ImmutableMap.of("snapshots", "all");

    Map<String, String> result = catalog.referencedByToQueryParam(original, context);

    assertThat(result)
        .containsEntry("snapshots", "all")
        .containsEntry("referenced-by", "ns%1Fview");
  }

  @Test
  public void testNamespaceWithSpecialCharsEncoded() {
    List<TableIdentifier> chain =
        ImmutableList.of(TableIdentifier.of(Namespace.of("ns with spaces"), "view/name"));
    Map<String, Object> context =
        ImmutableMap.of(ContextAwareTableCatalog.VIEW_IDENTIFIER_KEY, chain);

    Map<String, String> result = catalog.referencedByToQueryParam(Map.of(), context);

    // Spaces encode to + and / encodes to %2F in URL encoding
    assertThat(result).containsEntry("referenced-by", "ns+with+spaces%1Fview%2Fname");
  }
}
