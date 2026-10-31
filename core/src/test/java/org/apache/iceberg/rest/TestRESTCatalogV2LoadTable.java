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

import static org.apache.iceberg.TestBase.FILE_A;
import static org.apache.iceberg.TestBase.FILE_B;
import static org.apache.iceberg.TestBase.SCHEMA;
import static org.apache.iceberg.rest.RequestMatcher.matches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.HTTPRequest.HTTPMethod;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

class TestRESTCatalogV2LoadTable extends TestBaseWithRESTServer {
  private static final ResourcePaths RESOURCE_PATHS =
      ResourcePaths.forCatalogProperties(
          ImmutableMap.of(
              RESTCatalogProperties.NAMESPACE_SEPARATOR,
              RESTCatalogAdapter.NAMESPACE_SEPARATOR_URLENCODED_UTF_8));
  private static final TableIdentifier TBL = TableIdentifier.of(NS, "tbl");
  private static final String TBL_LOCATION = "memory://ns/tbl";

  @Override
  protected String catalogName() {
    return "catalog-v2-load-table";
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 4})
  void loadTableUsesV2Endpoint(int formatVersion) {
    restCatalog.createNamespace(NS);
    restCatalog
        .buildTable(TBL, SCHEMA)
        .withLocation(TBL_LOCATION)
        .withProperty(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion))
        .create()
        .newAppend()
        .appendFile(FILE_A)
        .commit();

    Mockito.clearInvocations(adapterForRESTServer);
    restCatalog.loadTable(TBL);

    verifyLoad(adapterForRESTServer, RESOURCE_PATHS.tableV2(TBL));
    Mockito.verify(adapterForRESTServer, never())
        .execute(matches(HTTPMethod.GET, RESOURCE_PATHS.table(TBL)), any(), any(), any());
  }

  @Test
  void refreshUsesV2EndpointAndCommitUsesV1() {
    restCatalog.createNamespace(NS);
    restCatalog.createTable(TBL, SCHEMA);
    Table table = restCatalog.loadTable(TBL);

    Mockito.clearInvocations(adapterForRESTServer);
    table.refresh();
    verifyLoad(adapterForRESTServer, RESOURCE_PATHS.tableV2(TBL));

    Mockito.clearInvocations(adapterForRESTServer);
    table.newAppend().appendFile(FILE_B).commit();
    Mockito.verify(adapterForRESTServer)
        .execute(
            matches(HTTPMethod.POST, RESOURCE_PATHS.table(TBL)),
            eq(LoadTableResponse.class),
            any(),
            any());
  }

  @Test
  void serverWithoutV1LoadTableUsesV2() {
    RESTCatalogAdapter adapter = Mockito.spy(adapterAdvertising(r -> r != Route.LOAD_TABLE));
    RESTCatalog catalog = catalog(adapter);
    catalog.createNamespace(NS);
    catalog.createTable(TBL, SCHEMA);

    Mockito.clearInvocations(adapter);
    catalog.loadTable(TBL).refresh();

    Mockito.verify(adapter, Mockito.times(2))
        .execute(
            matches(HTTPMethod.GET, RESOURCE_PATHS.tableV2(TBL)),
            eq(LoadTableResponse.class),
            any(),
            any());
  }

  @Test
  void v1LoadRejectsFormatVersion4Table() {
    backendCatalog.createNamespace(NS);
    backendCatalog
        .buildTable(TBL, SCHEMA)
        .withLocation(TBL_LOCATION)
        .withProperty(TableProperties.FORMAT_VERSION, "4")
        .create();
    RESTCatalog v1OnlyCatalog = catalog(adapterAdvertising(r -> r != Route.LOAD_TABLE_V2));

    assertThatThrownBy(() -> v1OnlyCatalog.loadTable(TBL))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining(
            "Cannot load table ns.tbl with format version 4 using the v1 endpoint; use "
                + Endpoint.V2_LOAD_TABLE);
    assertThat(restCatalog.loadTable(TBL)).isNotNull();
  }

  @Test
  void serverWithoutV2LoadTableUsesV1() {
    RESTCatalogAdapter adapter = Mockito.spy(adapterAdvertising(r -> r != Route.LOAD_TABLE_V2));
    RESTCatalog catalog = catalog(adapter);
    catalog.createNamespace(NS);
    catalog.createTable(TBL, SCHEMA);

    Mockito.clearInvocations(adapter);
    catalog.loadTable(TBL).refresh();

    Mockito.verify(adapter, Mockito.times(2))
        .execute(
            matches(HTTPMethod.GET, RESOURCE_PATHS.table(TBL)),
            eq(LoadTableResponse.class),
            any(),
            any());
    Mockito.verify(adapter, never())
        .execute(matches(HTTPMethod.GET, RESOURCE_PATHS.tableV2(TBL)), any(), any(), any());
  }

  @Test
  void refreshUsesV2PathWhenAdvertised() {
    RESTClient client = clientReturning(newTableMetadata());
    opsWithPath(client, RESOURCE_PATHS.table(TBL)).refresh();

    Mockito.verify(client)
        .get(
            eq(RESOURCE_PATHS.tableV2(TBL)),
            anyMap(),
            eq(LoadTableResponse.class),
            any(Supplier.class),
            any());
  }

  @Test
  void refreshFallsBackToCustomPathWhenV2Advertised() {
    String customPath = "custom/tables/tbl";
    RESTClient client = clientReturning(newTableMetadata());
    opsWithPath(client, customPath).refresh();

    Mockito.verify(client)
        .get(eq(customPath), anyMap(), eq(LoadTableResponse.class), any(Supplier.class), any());
  }

  private RESTCatalogAdapter adapterAdvertising(Predicate<Route> advertised) {
    return new RESTCatalogAdapter(backendCatalog) {
      @Override
      public <T extends RESTResponse> T execute(
          HTTPRequest request,
          Class<T> responseType,
          Consumer<ErrorResponse> errorHandler,
          Consumer<Map<String, String>> responseHeaders) {
        if (ResourcePaths.config().equals(request.path())) {
          return castResponse(
              responseType,
              ConfigResponse.builder()
                  .withEndpoints(
                      Arrays.stream(Route.values())
                          .filter(advertised)
                          .map(r -> Endpoint.create(r.method().name(), r.resourcePath()))
                          .collect(Collectors.toList()))
                  .withOverride(
                      RESTCatalogProperties.NAMESPACE_SEPARATOR,
                      NAMESPACE_SEPARATOR_URLENCODED_UTF_8)
                  .build());
        }

        return super.execute(request, responseType, errorHandler, responseHeaders);
      }
    };
  }

  private static RESTCatalog catalog(RESTCatalogAdapter adapter) {
    RESTCatalog catalog =
        new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    catalog.initialize("test", ImmutableMap.of(CatalogProperties.URI, "ignored"));
    return catalog;
  }

  private static void verifyLoad(RESTCatalogAdapter adapter, String path) {
    Mockito.verify(adapter)
        .execute(matches(HTTPMethod.GET, path), eq(LoadTableResponse.class), any(), any());
  }

  @SuppressWarnings("unchecked")
  private static RESTClient clientReturning(TableMetadata metadata) {
    RESTClient client = Mockito.mock(RESTClient.class);
    Mockito.when(
            client.get(
                anyString(), anyMap(), eq(LoadTableResponse.class), any(Supplier.class), any()))
        .thenReturn(LoadTableResponse.builder().withTableMetadata(metadata).build());
    return client;
  }

  private static RESTTableOperations opsWithPath(RESTClient client, String path) {
    return new RESTTableOperations(
        client,
        path,
        Map::of,
        null,
        newTableMetadata(),
        Set.of(Endpoint.V1_LOAD_TABLE, Endpoint.V2_LOAD_TABLE));
  }

  private static TableMetadata newTableMetadata() {
    return TableMetadata.newTableMetadata(
        SCHEMA, PartitionSpec.unpartitioned(), "file:/tmp/tbl", Map.of());
  }
}
