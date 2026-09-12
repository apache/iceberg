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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.LoadContext;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.HTTPRequest.HTTPMethod;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
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

  private RESTCatalogAdapter adapter;
  private RESTCatalog restCatalog;

  @BeforeEach
  public void before() {
    InMemoryCatalog backendCatalog = new InMemoryCatalog();
    backendCatalog.initialize("test", ImmutableMap.of());

    adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));
    restCatalog = new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    restCatalog.initialize(
        "test",
        ImmutableMap.of(
            CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO"));

    restCatalog.createNamespace(NS);
    restCatalog.buildTable(TABLE_IDENT, SCHEMA).create();
    Mockito.clearInvocations(adapter);
  }

  @AfterEach
  public void after() throws Exception {
    if (restCatalog != null) {
      restCatalog.close();
    }
  }

  @Test
  public void loadTableSendsReferencedBy() {
    restCatalog.loadTable(TABLE_IDENT, referencedBy("outer_view"));

    // the test adapter uses %2E as the namespace separator
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
  public void loadViewSendsReferencedBy() {
    TableIdentifier viewIdent = createView();

    restCatalog.loadView(viewIdent, referencedBy("outer_view"));

    Mockito.verify(adapter)
        .execute(
            matches(
                HTTPMethod.GET,
                "v1/namespaces/ns/views/test_view",
                Map.of(),
                ImmutableMap.of(
                    RESTCatalogProperties.REFERENCED_BY_QUERY_PARAMETER, "ns%2Eouter_view")),
            eq(LoadViewResponse.class),
            any(),
            any());
  }

  @Test
  public void loadViewWithoutContextHasNoReferencedByParam() {
    TableIdentifier viewIdent = createView();

    restCatalog.loadView(viewIdent);

    Mockito.verify(adapter)
        .execute(
            matches(HTTPMethod.GET, "v1/namespaces/ns/views/test_view", Map.of(), Map.of()),
            eq(LoadViewResponse.class),
            any(),
            any());
  }

  @Test
  public void loadViewThroughViewCatalogBridgeSendsReferencedBy() {
    TableIdentifier viewIdent = createView();

    // the ViewCatalog returned by asViewCatalog must forward the load context, otherwise
    // ViewCatalog's default implementation silently drops the view chain
    SessionCatalog.SessionContext session = SessionCatalog.SessionContext.createEmpty();
    ViewCatalog viewCatalog = restCatalog.sessionCatalog().asViewCatalog(session);

    viewCatalog.loadView(viewIdent, referencedBy("outer_view"));

    Mockito.verify(adapter)
        .execute(
            matches(
                HTTPMethod.GET,
                "v1/namespaces/ns/views/test_view",
                Map.of(),
                ImmutableMap.of(
                    RESTCatalogProperties.REFERENCED_BY_QUERY_PARAMETER, "ns%2Eouter_view")),
            eq(LoadViewResponse.class),
            any(),
            any());
  }

  @Test
  public void referencedByReachesTableFileIOProperties() {
    // a non-empty table config forces a table-level FileIO; that FileIO's properties are what
    // credential providers read the chain back out of
    Mockito.doAnswer(
            invocation -> {
              LoadTableResponse response = (LoadTableResponse) invocation.callRealMethod();
              return LoadTableResponse.builder()
                  .withTableMetadata(response.tableMetadata())
                  .addAllConfig(response.config())
                  .addAllConfig(ImmutableMap.of("table-scoped", "config"))
                  .build();
            })
        .when(adapter)
        .execute(
            matches(HTTPMethod.GET, "v1/namespaces/ns/tables/test_table"),
            eq(LoadTableResponse.class),
            any(),
            any());

    Table table = restCatalog.loadTable(TABLE_IDENT, referencedBy("outer_view"));

    assertThat(table.io().properties())
        .containsEntry(RESTCatalogProperties.REST_REFERENCED_BY, "ns%2Eouter_view");
  }

  @Test
  public void referencedByLoadStillReusesCatalogFileIO() {
    // with no table-scoped config from the server there are no per-table credentials to scope, so
    // the chain must not force a new FileIO per load
    Table plain = restCatalog.loadTable(TABLE_IDENT);
    Table viaView = restCatalog.loadTable(TABLE_IDENT, referencedBy("outer_view"));

    assertThat(viaView.io()).isSameAs(plain.io());
  }

  @Test
  public void loadMetadataTableSendsReferencedBy() {
    // the first GET 404s, and the retry against the base table must still carry the chain
    TableIdentifier metadataIdent =
        TableIdentifier.of(Namespace.of("ns", "test_table"), "snapshots");

    restCatalog.loadTable(metadataIdent, referencedBy("outer_view"));

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

  private static LoadContext referencedBy(String viewName) {
    return LoadContext.builder()
        .referencedBy(ImmutableList.of(TableIdentifier.of(NS, viewName)))
        .build();
  }

  private TableIdentifier createView() {
    TableIdentifier viewIdent = TableIdentifier.of(NS, "test_view");
    restCatalog
        .buildView(viewIdent)
        .withSchema(SCHEMA)
        .withDefaultNamespace(NS)
        .withQuery("spark", "select * from ns.test_table")
        .create();
    Mockito.clearInvocations(adapter);
    return viewIdent;
  }
}
