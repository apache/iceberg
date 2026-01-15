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
import static org.apache.iceberg.TestBase.SCHEMA;
import static org.apache.iceberg.rest.RESTTableCache.SessionIdTableId;
import static org.apache.iceberg.rest.RESTTableCache.TableWithETag;
import static org.apache.iceberg.rest.RequestMatcher.matches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.github.benmanes.caffeine.cache.Cache;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.http.HttpHeaders;
import org.apache.iceberg.BaseMetadataTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.FakeTicker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

public class TestFreshnessAwareLoading extends TestBaseWithRESTServer {
  private static final ResourcePaths RESOURCE_PATHS =
      ResourcePaths.forCatalogProperties(
          ImmutableMap.of(
              RESTCatalogProperties.NAMESPACE_SEPARATOR,
              RESTCatalogAdapter.NAMESPACE_SEPARATOR_URLENCODED_UTF_8));
  private static final TableIdentifier TABLE = TableIdentifier.of(NS, "newtable");
  private static final Duration TABLE_EXPIRATION =
      Duration.ofMillis(RESTCatalogProperties.TABLE_CACHE_EXPIRE_AFTER_WRITE_MS_DEFAULT);
  private static final Duration HALF_OF_TABLE_EXPIRATION = TABLE_EXPIRATION.dividedBy(2);

  @Override
  protected String catalogName() {
    return "catalog-freshness-aware-loading";
  }

  @Test
  public void eTagWithCreateAndLoadTable() {
    Map<String, String> respHeaders = Maps.newHashMap();
    RESTCatalog catalog = catalogWithResponseHeaders(respHeaders);

    catalog.createNamespace(TABLE.namespace());
    catalog.createTable(TABLE, SCHEMA);

    assertThat(respHeaders).containsKey(HttpHeaders.ETAG);
    String eTag = respHeaders.get(HttpHeaders.ETAG);
    respHeaders.clear();

    catalog.loadTable(TABLE);

    assertThat(respHeaders).containsEntry(HttpHeaders.ETAG, eTag);
  }

  @Test
  public void eTagWithDifferentTables() {
    Map<String, String> respHeaders = Maps.newHashMap();
    RESTCatalog catalog = catalogWithResponseHeaders(respHeaders);

    catalog.createNamespace(TABLE.namespace());
    catalog.createTable(TABLE, SCHEMA);

    assertThat(respHeaders).containsKey(HttpHeaders.ETAG);
    String eTagTbl1 = respHeaders.get(HttpHeaders.ETAG);
    respHeaders.clear();

    catalog.createTable(TableIdentifier.of(TABLE.namespace(), "table2"), SCHEMA);

    assertThat(respHeaders).containsKey(HttpHeaders.ETAG);
    assertThat(eTagTbl1).isNotEqualTo(respHeaders.get(HttpHeaders.ETAG));
  }

  @Test
  public void eTagAfterDataUpdate() {
    Map<String, String> respHeaders = Maps.newHashMap();
    RESTCatalog catalog = catalogWithResponseHeaders(respHeaders);

    catalog.createNamespace(TABLE.namespace());
    Table tbl = catalog.createTable(TABLE, SCHEMA);

    assertThat(respHeaders).containsKey(HttpHeaders.ETAG);
    String eTag = respHeaders.get(HttpHeaders.ETAG);

    respHeaders.clear();
    tbl.newAppend().appendFile(FILE_A).commit();

    assertThat(respHeaders).containsKey(HttpHeaders.ETAG);
    assertThat(eTag).isNotEqualTo(respHeaders.get(HttpHeaders.ETAG));
  }

  @Test
  public void eTagAfterMetadataOnlyUpdate() {
    Map<String, String> respHeaders = Maps.newHashMap();
    RESTCatalog catalog = catalogWithResponseHeaders(respHeaders);

    catalog.createNamespace(TABLE.namespace());
    Table tbl = catalog.createTable(TABLE, SCHEMA);

    assertThat(respHeaders).containsKey(HttpHeaders.ETAG);
    String eTag = respHeaders.get(HttpHeaders.ETAG);

    respHeaders.clear();
    tbl.updateSchema().addColumn("extra", Types.IntegerType.get()).commit();

    assertThat(respHeaders).containsKey(HttpHeaders.ETAG);
    assertThat(eTag).isNotEqualTo(respHeaders.get(HttpHeaders.ETAG));
  }

  @Test
  public void eTagWithRegisterTable() {
    Map<String, String> respHeaders = Maps.newHashMap();
    RESTCatalog catalog = catalogWithResponseHeaders(respHeaders);

    catalog.createNamespace(TABLE.namespace());
    Table tbl = catalog.createTable(TABLE, SCHEMA);

    assertThat(respHeaders).containsKey(HttpHeaders.ETAG);
    String eTag = respHeaders.get(HttpHeaders.ETAG);

    respHeaders.clear();
    catalog.registerTable(
        TableIdentifier.of(TABLE.namespace(), "other_table"),
        ((BaseTable) tbl).operations().current().metadataFileLocation());

    assertThat(respHeaders).containsEntry(HttpHeaders.ETAG, eTag);
  }

  @Test
  public void notModifiedResponse() {
    restCatalog.createNamespace(TABLE.namespace());
    restCatalog.createTable(TABLE, SCHEMA);
    Table table = restCatalog.loadTable(TABLE);

    String eTag =
        ETagProvider.of(
            ((BaseTable) table).operations().current().metadataFileLocation(),
            RESTCatalogAdapter.defaultQueryParams());

    Mockito.doAnswer(
            invocation -> {
              HTTPRequest originalRequest = invocation.getArgument(0);

              assertThat(originalRequest.headers().contains(HttpHeaders.IF_NONE_MATCH));
              assertThat(
                      originalRequest.headers().firstEntry(HttpHeaders.IF_NONE_MATCH).get().value())
                  .isEqualTo(eTag);

              assertThat(
                      adapterForRESTServer.execute(
                          originalRequest,
                          LoadTableResponse.class,
                          invocation.getArgument(2),
                          invocation.getArgument(3),
                          ParserContext.builder().build()))
                  .isNull();

              return null;
            })
        .when(adapterForRESTServer)
        .execute(
            matches(HTTPRequest.HTTPMethod.GET, RESOURCE_PATHS.table(TABLE)),
            eq(LoadTableResponse.class),
            any(),
            any());

    assertThat(restCatalog.loadTable(TABLE)).isNotNull();

    TableIdentifier metadataTableIdentifier =
        TableIdentifier.of(NS.toString(), TABLE.name(), "partitions");

    assertThat(restCatalog.loadTable(metadataTableIdentifier)).isNotNull();

    Mockito.verify(adapterForRESTServer, times(3))
        .execute(
            matches(HTTPRequest.HTTPMethod.GET, RESOURCE_PATHS.table(TABLE)),
            eq(LoadTableResponse.class),
            any(),
            any());

    verify(adapterForRESTServer)
        .execute(
            matches(HTTPRequest.HTTPMethod.GET, RESOURCE_PATHS.table(metadataTableIdentifier)),
            any(),
            any(),
            any());
  }

  @Test
  public void freshnessAwareLoading() {
    restCatalog.createNamespace(TABLE.namespace());
    restCatalog.createTable(TABLE, SCHEMA);

    Cache<SessionIdTableId, TableWithETag> tableCache =
        restCatalog.sessionCatalog().tableCache().cache();
    assertThat(tableCache.estimatedSize()).isZero();

    expectFullTableLoadForLoadTable(TABLE, adapterForRESTServer);
    BaseTable tableAfterFirstLoad = (BaseTable) restCatalog.loadTable(TABLE);

    assertThat(tableCache.stats().hitCount()).isZero();
    assertThat(tableCache.asMap())
        .containsOnlyKeys(SessionIdTableId.of(DEFAULT_SESSION_CONTEXT.sessionId(), TABLE));

    expectNotModifiedResponseForLoadTable(TABLE, adapterForRESTServer);
    BaseTable tableAfterSecondLoad = (BaseTable) restCatalog.loadTable(TABLE);

    assertThat(tableAfterFirstLoad).isNotSameAs(tableAfterSecondLoad);
    assertThat(tableAfterFirstLoad.operations().current().location())
        .isEqualTo(tableAfterSecondLoad.operations().current().location());
    assertThat(
            tableCache
                .asMap()
                .get(SessionIdTableId.of(DEFAULT_SESSION_CONTEXT.sessionId(), TABLE))
                .supplier()
                .get()
                .operations()
                .current()
                .metadataFileLocation())
        .isEqualTo(tableAfterFirstLoad.operations().current().metadataFileLocation());

    Mockito.verify(adapterForRESTServer, times(2))
        .execute(
            matches(HTTPRequest.HTTPMethod.GET, RESOURCE_PATHS.table(TABLE)), any(), any(), any());
  }

  @Test
  public void freshnessAwareLoadingMetadataTables() {
    restCatalog.createNamespace(TABLE.namespace());
    restCatalog.createTable(TABLE, SCHEMA);

    Cache<SessionIdTableId, TableWithETag> tableCache =
        restCatalog.sessionCatalog().tableCache().cache();
    assertThat(tableCache.estimatedSize()).isZero();

    BaseTable table = (BaseTable) restCatalog.loadTable(TABLE);

    assertThat(tableCache.stats().hitCount()).isZero();
    assertThat(tableCache.asMap())
        .containsOnlyKeys(SessionIdTableId.of(DEFAULT_SESSION_CONTEXT.sessionId(), TABLE));

    TableIdentifier metadataTableIdentifier =
        TableIdentifier.of(TABLE.namespace().toString(), TABLE.name(), "partitions");

    BaseMetadataTable metadataTable =
        (BaseMetadataTable) restCatalog.loadTable(metadataTableIdentifier);

    assertThat(tableCache.stats().hitCount()).isEqualTo(1);
    assertThat(tableCache.asMap())
        .containsOnlyKeys(SessionIdTableId.of(DEFAULT_SESSION_CONTEXT.sessionId(), TABLE));

    assertThat(table).isNotSameAs(metadataTable.table());
    assertThat(table.operations().current().metadataFileLocation())
        .isEqualTo(metadataTable.table().operations().current().metadataFileLocation());

    ResourcePaths paths =
        ResourcePaths.forCatalogProperties(
            ImmutableMap.of(RESTCatalogProperties.NAMESPACE_SEPARATOR, "%2E"));

    Mockito.verify(adapterForRESTServer, times(2))
        .execute(matches(HTTPRequest.HTTPMethod.GET, paths.table(TABLE)), any(), any(), any());

    Mockito.verify(adapterForRESTServer)
        .execute(
            matches(HTTPRequest.HTTPMethod.GET, paths.table(metadataTableIdentifier)),
            any(),
            any(),
            any());
  }

  @Test
  public void renameTableInvalidatesTable() {
    runTableInvalidationTest(
        restCatalog,
        adapterForRESTServer,
        catalog -> catalog.renameTable(TABLE, TableIdentifier.of(TABLE.namespace(), "other_table")),
        0);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void dropTableInvalidatesTable(boolean purge) {
    runTableInvalidationTest(
        restCatalog, adapterForRESTServer, catalog -> catalog.dropTable(TABLE, purge), 0);
  }

  @Test
  public void tableExistViaHeadRequestInvalidatesTable() {
    runTableInvalidationTest(
        restCatalog,
        adapterForRESTServer,
        (catalog -> {
          // Use a different catalog to drop the table
          catalog(new RESTCatalogAdapter(backendCatalog)).dropTable(TABLE, true);

          // The main catalog still has the table in cache
          assertThat(catalog.sessionCatalog().tableCache().cache().asMap())
              .containsOnlyKeys(SessionIdTableId.of(DEFAULT_SESSION_CONTEXT.sessionId(), TABLE));

          catalog.tableExists(TABLE);
        }),
        0);
  }

  @Test
  public void tableExistViaGetRequestInvalidatesTable() {
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    // Configure REST server to answer tableExists query via GET
    Mockito.doAnswer(
            invocation ->
                ConfigResponse.builder()
                    .withEndpoints(
                        ImmutableList.of(
                            Endpoint.V1_LOAD_TABLE,
                            Endpoint.V1_CREATE_NAMESPACE,
                            Endpoint.V1_CREATE_TABLE))
                    .build())
        .when(adapter)
        .execute(
            matches(HTTPRequest.HTTPMethod.GET, ResourcePaths.config()),
            eq(ConfigResponse.class),
            any(),
            any());

    RESTCatalog catalog = new RESTCatalog(DEFAULT_SESSION_CONTEXT, config -> adapter);
    catalog.initialize(
        "catalog",
        ImmutableMap.of(
            CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO"));

    runTableInvalidationTest(
        catalog,
        adapter,
        cat -> {
          // Use a different catalog to drop the table
          catalog(new RESTCatalogAdapter(backendCatalog)).dropTable(TABLE, true);

          // The main catalog still has the table in cache
          assertThat(cat.sessionCatalog().tableCache().cache().asMap())
              .containsOnlyKeys(SessionIdTableId.of(DEFAULT_SESSION_CONTEXT.sessionId(), TABLE));

          cat.tableExists(TABLE);
        },
        1);
  }

  @Test
  public void loadTableInvalidatesCache() {
    runTableInvalidationTest(
        restCatalog,
        adapterForRESTServer,
        catalog -> {
          // Use a different catalog to drop the table
          catalog(new RESTCatalogAdapter(backendCatalog)).dropTable(TABLE, true);

          // The main catalog still has the table in cache
          assertThat(catalog.sessionCatalog().tableCache().cache().asMap())
              .containsOnlyKeys(SessionIdTableId.of(DEFAULT_SESSION_CONTEXT.sessionId(), TABLE));

          assertThatThrownBy(() -> catalog.loadTable(TABLE))
              .isInstanceOf(NoSuchTableException.class)
              .hasMessage("Table does not exist: %s", TABLE);
        },
        1);
  }

  @Test
  public void loadTableWithMetadataTableNameInvalidatesCache() {
    TableIdentifier metadataTableIdentifier =
        TableIdentifier.of(TABLE.namespace().toString(), TABLE.name(), "partitions");

    runTableInvalidationTest(
        restCatalog,
        adapterForRESTServer,
        catalog -> {
          // Use a different catalog to drop the table
          catalog(new RESTCatalogAdapter(backendCatalog)).dropTable(TABLE, true);

          // The main catalog still has the table in cache
          assertThat(catalog.sessionCatalog().tableCache().cache().asMap())
              .containsOnlyKeys(SessionIdTableId.of(DEFAULT_SESSION_CONTEXT.sessionId(), TABLE));

          assertThatThrownBy(() -> catalog.loadTable(metadataTableIdentifier))
              .isInstanceOf(NoSuchTableException.class)
              .hasMessage("Table does not exist: %s", TABLE);
        },
        1);

    ResourcePaths paths =
        ResourcePaths.forCatalogProperties(
            ImmutableMap.of(RESTCatalogProperties.NAMESPACE_SEPARATOR, "%2E"));

    Mockito.verify(adapterForRESTServer)
        .execute(
            matches(HTTPRequest.HTTPMethod.GET, paths.table(metadataTableIdentifier)),
            any(),
            any(),
            any());
  }

  private void runTableInvalidationTest(
      RESTCatalog catalog,
      RESTCatalogAdapter adapterToVerify,
      Consumer<RESTCatalog> action,
      int loadTableCountFromAction) {
    catalog.createNamespace(TABLE.namespace());
    catalog.createTable(TABLE, SCHEMA);
    BaseTable originalTable = (BaseTable) catalog.loadTable(TABLE);

    Cache<SessionIdTableId, TableWithETag> tableCache =
        catalog.sessionCatalog().tableCache().cache();
    assertThat(tableCache.stats().hitCount()).isZero();
    assertThat(tableCache.asMap())
        .containsOnlyKeys(SessionIdTableId.of(DEFAULT_SESSION_CONTEXT.sessionId(), TABLE));

    action.accept(catalog);

    // Check that 'action' invalidates cache
    assertThat(tableCache.estimatedSize()).isZero();

    assertThatThrownBy(() -> catalog.loadTable(TABLE))
        .isInstanceOf(NoSuchTableException.class)
        .hasMessageContaining("Table does not exist: %s", TABLE);

    catalog.createTable(TABLE, SCHEMA);
    expectFullTableLoadForLoadTable(TABLE, adapterToVerify);
    BaseTable newTableWithSameName = (BaseTable) catalog.loadTable(TABLE);

    assertThat(tableCache.stats().hitCount()).isEqualTo(loadTableCountFromAction);
    assertThat(tableCache.asMap())
        .containsOnlyKeys(SessionIdTableId.of(DEFAULT_SESSION_CONTEXT.sessionId(), TABLE));

    assertThat(newTableWithSameName).isNotEqualTo(originalTable);
    assertThat(newTableWithSameName.operations().current().metadataFileLocation())
        .isNotEqualTo(originalTable.operations().current().metadataFileLocation());

    Mockito.verify(adapterToVerify, times(3 + loadTableCountFromAction))
        .execute(
            matches(HTTPRequest.HTTPMethod.GET, RESOURCE_PATHS.table(TABLE)), any(), any(), any());
  }

  @Test
  public void tableCacheWithMultiSessions() {
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    RESTSessionCatalog sessionCatalog = new RESTSessionCatalog(config -> adapter, null);
    sessionCatalog.initialize("test_session_catalog", Map.of());

    SessionCatalog.SessionContext otherSessionContext =
        new SessionCatalog.SessionContext(
            "session_id_2", "user", ImmutableMap.of("credential", "user:12345"), ImmutableMap.of());

    sessionCatalog.createNamespace(DEFAULT_SESSION_CONTEXT, TABLE.namespace());
    sessionCatalog.buildTable(DEFAULT_SESSION_CONTEXT, TABLE, SCHEMA).create();
    expectFullTableLoadForLoadTable(TABLE, adapter);
    BaseTable tableSession1 = (BaseTable) sessionCatalog.loadTable(DEFAULT_SESSION_CONTEXT, TABLE);

    Cache<SessionIdTableId, TableWithETag> tableCache = sessionCatalog.tableCache().cache();
    assertThat(tableCache.stats().hitCount()).isZero();
    assertThat(tableCache.asMap())
        .containsOnlyKeys(SessionIdTableId.of(DEFAULT_SESSION_CONTEXT.sessionId(), TABLE));

    expectFullTableLoadForLoadTable(TABLE, adapter);
    BaseTable tableSession2 = (BaseTable) sessionCatalog.loadTable(otherSessionContext, TABLE);

    assertThat(tableSession1).isNotEqualTo(tableSession2);
    assertThat(tableSession1.operations().current().metadataFileLocation())
        .isEqualTo(tableSession2.operations().current().metadataFileLocation());
    assertThat(tableCache.asMap())
        .containsOnlyKeys(
            SessionIdTableId.of(DEFAULT_SESSION_CONTEXT.sessionId(), TABLE),
            SessionIdTableId.of(otherSessionContext.sessionId(), TABLE));
  }

  @Test
  public void notModified304ResponseWithEmptyTableCache() {
    Mockito.doAnswer(invocation -> null)
        .when(adapterForRESTServer)
        .execute(
            matches(HTTPRequest.HTTPMethod.GET, RESOURCE_PATHS.table(TABLE)),
            eq(LoadTableResponse.class),
            any(),
            any());

    restCatalog.createNamespace(TABLE.namespace());
    restCatalog.createTable(TABLE, SCHEMA);
    restCatalog.invalidateTable(TABLE);

    // Table is not in the cache and null LoadTableResponse is received
    assertThatThrownBy(() -> restCatalog.loadTable(TABLE))
        .isInstanceOf(RESTException.class)
        .hasMessage(
            "Invalid (NOT_MODIFIED) response for request: method=%s, path=%s",
            HTTPRequest.HTTPMethod.GET, RESOURCE_PATHS.table(TABLE));
  }

  @Test
  public void tableCacheNotUpdatedWithoutETag() {
    RESTCatalogAdapter adapter =
        Mockito.spy(
            new RESTCatalogAdapter(backendCatalog) {
              @Override
              public <T extends RESTResponse> T execute(
                  HTTPRequest request,
                  Class<T> responseType,
                  Consumer<ErrorResponse> errorHandler,
                  Consumer<Map<String, String>> responseHeaders) {
                // Wrap the original responseHeaders to not accept ETag.
                Consumer<Map<String, String>> noETagConsumer =
                    headers -> {
                      if (!headers.containsKey(HttpHeaders.ETAG)) {
                        responseHeaders.accept(headers);
                      }
                    };
                return super.execute(request, responseType, errorHandler, noETagConsumer);
              }
            });

    RESTCatalog catalog = new RESTCatalog(DEFAULT_SESSION_CONTEXT, config -> adapter);
    catalog.initialize(
        "catalog",
        ImmutableMap.of(
            CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO"));

    catalog.createNamespace(TABLE.namespace());
    catalog.createTable(TABLE, SCHEMA);
    assertThat(catalog.loadTable(TABLE)).isNotNull();

    assertThat(catalog.sessionCatalog().tableCache().cache().estimatedSize()).isZero();
  }

  @Test
  public void tableCacheDisabled() {
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    RESTCatalog catalog = new RESTCatalog(DEFAULT_SESSION_CONTEXT, config -> adapter);
    catalog.initialize(
        "catalog",
        ImmutableMap.of(
            CatalogProperties.FILE_IO_IMPL,
            "org.apache.iceberg.inmemory.InMemoryFileIO",
            RESTCatalogProperties.TABLE_CACHE_MAX_ENTRIES,
            "0"));

    catalog.createNamespace(TABLE.namespace());
    catalog.createTable(TABLE, SCHEMA);

    assertThat(catalog.sessionCatalog().tableCache().cache().estimatedSize()).isZero();

    expectFullTableLoadForLoadTable(TABLE, adapter);
    assertThat(catalog.loadTable(TABLE)).isNotNull();
    catalog.sessionCatalog().tableCache().cache().cleanUp();

    assertThat(catalog.sessionCatalog().tableCache().cache().estimatedSize()).isZero();
  }

  @Test
  public void fullTableLoadAfterExpiryFromCache() {
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    FakeTicker ticker = new FakeTicker();

    TestableRESTCatalog catalog =
        new TestableRESTCatalog(DEFAULT_SESSION_CONTEXT, config -> adapter, ticker);
    catalog.initialize("catalog", Map.of());

    catalog.createNamespace(TABLE.namespace());
    catalog.createTable(TABLE, SCHEMA);
    catalog.loadTable(TABLE);

    Cache<SessionIdTableId, TableWithETag> tableCache =
        catalog.sessionCatalog().tableCache().cache();
    SessionIdTableId tableCacheKey =
        SessionIdTableId.of(DEFAULT_SESSION_CONTEXT.sessionId(), TABLE);

    assertThat(tableCache.asMap()).containsOnlyKeys(tableCacheKey);
    assertThat(tableCache.policy().expireAfterWrite().get().ageOf(tableCacheKey))
        .isPresent()
        .get()
        .isEqualTo(Duration.ZERO);

    ticker.advance(HALF_OF_TABLE_EXPIRATION);

    assertThat(tableCache.asMap()).containsOnlyKeys(tableCacheKey);
    assertThat(tableCache.policy().expireAfterWrite().get().ageOf(tableCacheKey))
        .isPresent()
        .get()
        .isEqualTo(HALF_OF_TABLE_EXPIRATION);

    ticker.advance(HALF_OF_TABLE_EXPIRATION.plus(Duration.ofSeconds(10)));

    assertThat(tableCache.asMap()).doesNotContainKey(tableCacheKey);

    expectFullTableLoadForLoadTable(TABLE, adapter);
    assertThat(catalog.loadTable(TABLE)).isNotNull();

    assertThat(tableCache.stats().hitCount()).isEqualTo(0);
    assertThat(tableCache.asMap()).containsOnlyKeys(tableCacheKey);
    assertThat(tableCache.policy().expireAfterWrite().get().ageOf(tableCacheKey))
        .isPresent()
        .get()
        .isEqualTo(Duration.ZERO);
  }

  @Test
  public void tableCacheAgeNotRefreshedAfterAccess() {
    FakeTicker ticker = new FakeTicker();

    TestableRESTCatalog catalog =
        new TestableRESTCatalog(
            DEFAULT_SESSION_CONTEXT, config -> new RESTCatalogAdapter(backendCatalog), ticker);
    catalog.initialize("catalog", Map.of());

    catalog.createNamespace(TABLE.namespace());
    catalog.createTable(TABLE, SCHEMA);
    catalog.loadTable(TABLE);

    ticker.advance(HALF_OF_TABLE_EXPIRATION);

    Cache<SessionIdTableId, TableWithETag> tableCache =
        catalog.sessionCatalog().tableCache().cache();
    SessionIdTableId tableCacheKey =
        SessionIdTableId.of(DEFAULT_SESSION_CONTEXT.sessionId(), TABLE);

    assertThat(tableCache.policy().expireAfterWrite().get().ageOf(tableCacheKey))
        .isPresent()
        .get()
        .isEqualTo(HALF_OF_TABLE_EXPIRATION);

    assertThat(catalog.loadTable(TABLE)).isNotNull();

    assertThat(tableCache.policy().expireAfterWrite().get().ageOf(tableCacheKey))
        .isPresent()
        .get()
        .isEqualTo(HALF_OF_TABLE_EXPIRATION);
  }

  @Test
  public void customTableOperationsWithFreshnessAwareLoading() {
    class CustomTableOps extends RESTTableOperations {
      CustomTableOps(
          RESTClient client,
          String path,
          Supplier<Map<String, String>> readHeaders,
          Supplier<Map<String, String>> mutationHeaders,
          FileIO io,
          TableMetadata current,
          Set<Endpoint> endpoints) {
        super(client, path, readHeaders, mutationHeaders, io, current, endpoints);
      }
    }

    class CustomRESTSessionCatalog extends RESTSessionCatalog {
      CustomRESTSessionCatalog(
          Function<Map<String, String>, RESTClient> clientBuilder,
          BiFunction<SessionContext, Map<String, String>, FileIO> ioBuilder) {
        super(clientBuilder, ioBuilder);
      }

      @Override
      protected RESTTableOperations newTableOps(
          RESTClient restClient,
          String path,
          Supplier<Map<String, String>> readHeaders,
          Supplier<Map<String, String>> mutationHeaders,
          FileIO fileIO,
          TableMetadata current,
          Set<Endpoint> supportedEndpoints) {
        return new CustomTableOps(
            restClient, path, readHeaders, mutationHeaders, fileIO, current, supportedEndpoints);
      }
    }

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));
    RESTCatalog catalog =
        catalog(adapter, clientBuilder -> new CustomRESTSessionCatalog(clientBuilder, null));

    catalog.createNamespace(NS);
    catalog.createTable(TABLE, SCHEMA);

    expectFullTableLoadForLoadTable(TABLE, adapter);
    BaseTable table = (BaseTable) catalog.loadTable(TABLE);
    assertThat(table.operations()).isInstanceOf(CustomTableOps.class);

    // When answering loadTable from table cache we still get the injected ops.
    expectNotModifiedResponseForLoadTable(TABLE, adapter);
    table = (BaseTable) catalog.loadTable(TABLE);
    assertThat(table.operations()).isInstanceOf(CustomTableOps.class);
  }

  private RESTCatalog catalogWithResponseHeaders(Map<String, String> respHeaders) {
    RESTCatalogAdapter adapter =
        new RESTCatalogAdapter(backendCatalog) {
          @Override
          public <T extends RESTResponse> T execute(
              HTTPRequest request,
              Class<T> responseType,
              Consumer<ErrorResponse> errorHandler,
              Consumer<Map<String, String>> responseHeaders) {
            return super.execute(request, responseType, errorHandler, respHeaders::putAll);
          }
        };

    return catalog(adapter);
  }

  private RESTCatalog catalog(RESTCatalogAdapter adapter) {
    RESTCatalog catalog =
        new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    catalog.initialize(
        "test",
        ImmutableMap.of(
            CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO"));
    return catalog;
  }

  private RESTCatalog catalog(
      RESTCatalogAdapter adapter,
      Function<Function<Map<String, String>, RESTClient>, RESTSessionCatalog>
          sessionCatalogFactory) {
    RESTCatalog catalog =
        new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter) {
          @Override
          protected RESTSessionCatalog newSessionCatalog(
              Function<Map<String, String>, RESTClient> clientBuilder) {
            return sessionCatalogFactory.apply(clientBuilder);
          }
        };
    catalog.initialize(
        "test",
        ImmutableMap.of(
            CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO"));
    return catalog;
  }

  private void expectFullTableLoadForLoadTable(TableIdentifier ident, RESTCatalogAdapter adapter) {
    Answer<LoadTableResponse> invocationAssertsFullLoad =
        invocation ->
            assertThat((LoadTableResponse) invocation.callRealMethod()).isNotEqualTo(null).actual();

    Mockito.doAnswer(invocationAssertsFullLoad)
        .when(adapter)
        .execute(
            matches(HTTPRequest.HTTPMethod.GET, RESOURCE_PATHS.table(ident)),
            eq(LoadTableResponse.class),
            any(),
            any());
  }

  private void expectNotModifiedResponseForLoadTable(
      TableIdentifier ident, RESTCatalogAdapter adapter) {
    Answer<LoadTableResponse> invocationAssertsFullLoad =
        invocation ->
            assertThat((LoadTableResponse) invocation.callRealMethod()).isEqualTo(null).actual();

    Mockito.doAnswer(invocationAssertsFullLoad)
        .when(adapter)
        .execute(
            matches(HTTPRequest.HTTPMethod.GET, RESOURCE_PATHS.table(ident)),
            eq(LoadTableResponse.class),
            any(),
            any());
  }
}
