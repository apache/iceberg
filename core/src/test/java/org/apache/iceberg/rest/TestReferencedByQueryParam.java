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

import static org.apache.iceberg.rest.RequestMatcher.matches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.ContextAwareCatalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.HTTPRequest.HTTPMethod;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestReferencedByQueryParam {

  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

  private static final Namespace NS = Namespace.of("ns");
  private static final TableIdentifier TABLE_IDENT = TableIdentifier.of(NS, "test_table");

  private final RESTSessionCatalog catalog = new RESTSessionCatalog(config -> null, null);

  private RESTCatalogAdapter adapter;
  private RESTCatalog restCatalog;

  @BeforeEach
  public void setupE2E() {
    InMemoryCatalog backendCatalog = new InMemoryCatalog();
    backendCatalog.initialize("test", ImmutableMap.of());

    adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));
    restCatalog = new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    restCatalog.initialize("test", ImmutableMap.of());

    restCatalog.createNamespace(NS);
    restCatalog.buildTable(TABLE_IDENT, SCHEMA).create();
    Mockito.clearInvocations(adapter);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (restCatalog != null) {
      restCatalog.close();
    }
  }

  @Test
  public void singleViewSimpleNamespace() {
    List<TableIdentifier> chain =
        ImmutableList.of(TableIdentifier.of(Namespace.of("ns"), "viewName"));
    Map<String, Object> context = ImmutableMap.of(ContextAwareCatalog.VIEW_IDENTIFIER_KEY, chain);

    Map<String, String> result = catalog.referencedByToQueryParam(context);

    assertThat(result)
        .containsEntry(RESTCatalogProperties.REFERENCED_BY_QUERY_PARAMETER, "ns%1FviewName");
  }

  @Test
  public void singleViewNestedNamespace() {
    List<TableIdentifier> chain =
        ImmutableList.of(TableIdentifier.of(Namespace.of("prod", "analytics"), "quarterly_view"));
    Map<String, Object> context = ImmutableMap.of(ContextAwareCatalog.VIEW_IDENTIFIER_KEY, chain);

    Map<String, String> result = catalog.referencedByToQueryParam(context);

    assertThat(result)
        .containsEntry(
            RESTCatalogProperties.REFERENCED_BY_QUERY_PARAMETER,
            "prod%1Fanalytics%1Fquarterly_view");
  }

  @Test
  public void nestedViewChain() {
    List<TableIdentifier> chain =
        ImmutableList.of(
            TableIdentifier.of(Namespace.of("outer_ns"), "outer_view"),
            TableIdentifier.of(Namespace.of("inner_ns"), "inner_view"));
    Map<String, Object> context = ImmutableMap.of(ContextAwareCatalog.VIEW_IDENTIFIER_KEY, chain);

    Map<String, String> result = catalog.referencedByToQueryParam(context);

    assertThat(result)
        .containsEntry(
            RESTCatalogProperties.REFERENCED_BY_QUERY_PARAMETER,
            "outer_ns%1Fouter_view,inner_ns%1Finner_view");
  }

  @Test
  public void viewNameWithCommaIsEncoded() {
    List<TableIdentifier> chain =
        ImmutableList.of(TableIdentifier.of(Namespace.of("ns"), "view,name"));
    Map<String, Object> context = ImmutableMap.of(ContextAwareCatalog.VIEW_IDENTIFIER_KEY, chain);

    Map<String, String> result = catalog.referencedByToQueryParam(context);

    // Comma in view name should be encoded as %2C
    assertThat(result)
        .containsEntry(RESTCatalogProperties.REFERENCED_BY_QUERY_PARAMETER, "ns%1Fview%2Cname");
  }

  @Test
  public void invalidContextValueType() {
    Map<String, Object> context =
        ImmutableMap.of(ContextAwareCatalog.VIEW_IDENTIFIER_KEY, "not-a-list");

    assertThatThrownBy(() -> catalog.referencedByToQueryParam(context))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("expected List<TableIdentifier>");
  }

  @Test
  public void paramsForLoadTableMergesSnapshotModeAndReferencedBy() {
    List<TableIdentifier> chain = ImmutableList.of(TableIdentifier.of(Namespace.of("ns"), "view"));
    Map<String, Object> context = ImmutableMap.of(ContextAwareCatalog.VIEW_IDENTIFIER_KEY, chain);

    Map<String, String> result =
        catalog.paramsForLoadTable(RESTCatalogProperties.SnapshotMode.ALL, context);

    assertThat(result)
        .containsEntry("snapshots", "all")
        .containsEntry(RESTCatalogProperties.REFERENCED_BY_QUERY_PARAMETER, "ns%1Fview");
  }

  @Test
  public void namespaceWithSpecialCharsEncoded() {
    List<TableIdentifier> chain =
        ImmutableList.of(TableIdentifier.of(Namespace.of("ns with spaces"), "view/name"));
    Map<String, Object> context = ImmutableMap.of(ContextAwareCatalog.VIEW_IDENTIFIER_KEY, chain);

    Map<String, String> result = catalog.referencedByToQueryParam(context);

    // Spaces encode to + and / encodes to %2F in URL encoding
    assertThat(result)
        .containsEntry(
            RESTCatalogProperties.REFERENCED_BY_QUERY_PARAMETER, "ns+with+spaces%1Fview%2Fname");
  }

  @Test
  public void loadTableWithReferencedByQueryParam() {
    List<TableIdentifier> viewChain = ImmutableList.of(TableIdentifier.of(NS, "outer_view"));
    Map<String, Object> loadingContext =
        ImmutableMap.of(ContextAwareCatalog.VIEW_IDENTIFIER_KEY, viewChain);

    restCatalog.loadTable(TABLE_IDENT, loadingContext);

    // The test adapter uses %2E as the namespace separator
    Mockito.verify(adapter)
        .execute(
            matches(
                HTTPMethod.GET,
                "v1/namespaces/ns/tables/test_table",
                Map.of(),
                ImmutableMap.of(
                    "snapshots",
                    "all",
                    RESTCatalogProperties.REFERENCED_BY_QUERY_PARAMETER,
                    "ns%2Eouter_view")),
            eq(LoadTableResponse.class),
            any(),
            any());
  }

  @Test
  public void loadTableWithoutContextHasNoReferencedByParam() {
    restCatalog.loadTable(TABLE_IDENT);

    Mockito.verify(adapter)
        .execute(
            matches(
                HTTPMethod.GET,
                "v1/namespaces/ns/tables/test_table",
                Map.of(),
                ImmutableMap.of("snapshots", "all")),
            eq(LoadTableResponse.class),
            any(),
            any());
  }

  @Test
  public void loadTableWithNestedViewChainReferencedBy() {
    List<TableIdentifier> viewChain =
        ImmutableList.of(
            TableIdentifier.of(NS, "outer_view"), TableIdentifier.of(NS, "inner_view"));
    Map<String, Object> loadingContext =
        ImmutableMap.of(ContextAwareCatalog.VIEW_IDENTIFIER_KEY, viewChain);

    restCatalog.loadTable(TABLE_IDENT, loadingContext);

    // The test adapter uses %2E as the namespace separator
    Mockito.verify(adapter)
        .execute(
            matches(
                HTTPMethod.GET,
                "v1/namespaces/ns/tables/test_table",
                Map.of(),
                ImmutableMap.of(
                    "snapshots",
                    "all",
                    RESTCatalogProperties.REFERENCED_BY_QUERY_PARAMETER,
                    "ns%2Eouter_view,ns%2Einner_view")),
            eq(LoadTableResponse.class),
            any(),
            any());
  }
}
