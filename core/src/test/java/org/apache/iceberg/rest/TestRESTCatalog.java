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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.CatalogTests;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.RESTCatalogAdapter.HTTPMethod;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;

public class TestRESTCatalog extends CatalogTests<RESTCatalog> {
  private static final ObjectMapper MAPPER = RESTObjectMapper.mapper();

  @TempDir
  public Path temp;

  private RESTCatalog restCatalog;
  private JdbcCatalog backendCatalog;

  @BeforeEach
  public void createCatalog() {
    File warehouse = temp.toFile();
    Configuration conf = new Configuration();

    this.backendCatalog = new JdbcCatalog();
    backendCatalog.setConf(conf);
    Map<String, String> backendCatalogProperties = ImmutableMap.of(
        CatalogProperties.WAREHOUSE_LOCATION, warehouse.getAbsolutePath(),
        CatalogProperties.URI, "jdbc:sqlite:file::memory:?ic" + UUID.randomUUID().toString().replace("-", ""),
        JdbcCatalog.PROPERTY_PREFIX + "username", "user",
        JdbcCatalog.PROPERTY_PREFIX + "password", "password");
    backendCatalog.initialize("backend", backendCatalogProperties);

    Map<String, String> catalogHeaders = ImmutableMap.of(
        "Authorization", "Bearer client-credentials-token:sub=catalog");
    Map<String, String> contextHeaders = ImmutableMap.of(
        "Authorization", "Bearer client-credentials-token:sub=user");

    RESTCatalogAdapter adaptor = new RESTCatalogAdapter(backendCatalog) {
      @Override
      public <T extends RESTResponse> T execute(RESTCatalogAdapter.HTTPMethod method, String path,
                                                Map<String, String> queryParams, Object body, Class<T> responseType,
                                                Map<String, String> headers, Consumer<ErrorResponse> errorHandler) {
        // this doesn't use a Mockito spy because this is used for catalog tests, which have different method calls
        if (!"v1/oauth/tokens".equals(path)) {
          if ("v1/config".equals(path)) {
            Assertions.assertEquals(catalogHeaders, headers, "Headers did not match for path: " + path);
          } else {
            Assertions.assertEquals(contextHeaders, headers, "Headers did not match for path: " + path);
          }
        }
        Object request = roundTripSerialize(body, "request");
        T response = super.execute(method, path, queryParams, request, responseType, headers, errorHandler);
        T responseAfterSerialization = roundTripSerialize(response, "response");
        return responseAfterSerialization;
      }
    };

    SessionCatalog.SessionContext context = new SessionCatalog.SessionContext(
        UUID.randomUUID().toString(), "user", ImmutableMap.of("credential", "user:12345"), ImmutableMap.of());

    this.restCatalog = new RESTCatalog(context, (config) -> adaptor);
    restCatalog.setConf(conf);
    restCatalog.initialize("prod", ImmutableMap.of(CatalogProperties.URI, "ignored", "credential", "catalog:12345"));
  }

  @SuppressWarnings("unchecked")
  public static <T> T roundTripSerialize(T payload, String description) {
    if (payload != null) {
      try {
        if (payload instanceof RESTMessage) {
          return (T) MAPPER.readValue(MAPPER.writeValueAsString(payload), payload.getClass());
        } else {
          // use Map so that Jackson doesn't try to instantiate ImmutableMap from payload.getClass()
          return (T) MAPPER.readValue(MAPPER.writeValueAsString(payload), Map.class);
        }
      } catch (JsonProcessingException e) {
        throw new RuntimeException(
            String.format("Failed to serialize and deserialize %s: %s", description, payload), e);
      }
    }
    return null;
  }

  @AfterEach
  public void closeCatalog() throws IOException {
    if (restCatalog != null) {
      restCatalog.close();
    }

    if (backendCatalog != null) {
      backendCatalog.close();
    }
  }

  @Override
  protected RESTCatalog catalog() {
    return restCatalog;
  }

  @Override
  protected boolean supportsNamespaceProperties() {
    return true;
  }

  @Override
  protected boolean supportsServerSideRetry() {
    return true;
  }

  @Override
  protected boolean supportsNestedNamespaces() {
    return true;
  }

  /* RESTCatalog specific tests */

  @Test
  public void testConfigRoute() throws IOException {
    RESTClient testClient = new RESTCatalogAdapter(backendCatalog) {
      @Override
      public <T extends RESTResponse> T get(String path, Map<String, String> queryParams, Class<T> responseType,
                                            Map<String, String> headers, Consumer<ErrorResponse> errorHandler) {
        if ("v1/config".equals(path)) {
          return castResponse(responseType, ConfigResponse
              .builder()
              .withDefaults(ImmutableMap.of(CatalogProperties.CLIENT_POOL_SIZE, "1"))
              .withOverrides(ImmutableMap.of(CatalogProperties.CACHE_ENABLED, "false"))
              .build());
        }
        return super.get(path, queryParams, responseType, headers, errorHandler);
      }
    };

    RESTCatalog restCat = new RESTCatalog((config) -> testClient);
    Map<String, String> initialConfig = ImmutableMap.of(
        CatalogProperties.URI, "http://localhost:8080",
        CatalogProperties.CACHE_ENABLED, "true");

    restCat.setConf(new Configuration());
    restCat.initialize("prod", initialConfig);

    Assert.assertEquals("Catalog properties after initialize should use the server's override properties",
        "false", restCat.properties().get(CatalogProperties.CACHE_ENABLED));

    Assert.assertEquals("Catalog after initialize should use the server's default properties if not specified",
        "1", restCat.properties().get(CatalogProperties.CLIENT_POOL_SIZE));
    restCat.close();
  }

  @Test
  public void testInitializeWithBadArguments() throws IOException {
    RESTCatalog restCat = new RESTCatalog();
    AssertHelpers.assertThrows("Configuration passed to initialize cannot be null",
        IllegalArgumentException.class,
        "Invalid configuration: null",
        () -> restCat.initialize("prod", null));

    AssertHelpers.assertThrows("Configuration passed to initialize must have uri",
        IllegalArgumentException.class,
        "REST Catalog server URI is required",
        () -> restCat.initialize("prod", ImmutableMap.of()));

    restCat.close();
  }

  @Test
  public void testCatalogBasicBearerToken() {
    Map<String, String> catalogHeaders = ImmutableMap.of(
        "Authorization", "Bearer bearer-token");

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    RESTCatalog catalog = new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    catalog.initialize("prod", ImmutableMap.of(
        CatalogProperties.URI, "ignored",
        "token", "bearer-token"));

    Assertions.assertFalse(catalog.tableExists(TableIdentifier.of("ns", "table")));

    // the bearer token should be used for all interactions
    Mockito.verify(adapter).execute(
        eq(HTTPMethod.GET), eq("v1/config"), any(), any(), eq(ConfigResponse.class), eq(catalogHeaders), any());
    Mockito.verify(adapter).execute(
        eq(HTTPMethod.GET),
        eq("v1/namespaces/ns/tables/table"),
        any(),
        any(),
        eq(LoadTableResponse.class),
        eq(catalogHeaders),
        any());
  }

  @Test
  public void testCatalogCredential() {
    Map<String, String> emptyHeaders = ImmutableMap.of();
    Map<String, String> catalogHeaders = ImmutableMap.of(
        "Authorization", "Bearer client-credentials-token:sub=catalog");

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    RESTCatalog catalog = new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    catalog.initialize("prod", ImmutableMap.of(
        CatalogProperties.URI, "ignored",
        "credential", "catalog:secret"));

    Assertions.assertFalse(catalog.tableExists(TableIdentifier.of("ns", "table")));

    // no token or credential for catalog token exchange
    Mockito.verify(adapter).execute(
        eq(HTTPMethod.POST),
        eq("v1/oauth/tokens"),
        any(),
        any(),
        eq(OAuthTokenResponse.class),
        eq(emptyHeaders),
        any());
    // no token or credential for config
    Mockito.verify(adapter).execute(
        eq(HTTPMethod.GET), eq("v1/config"), any(), any(), eq(ConfigResponse.class), eq(catalogHeaders), any());
    // use the catalog token for all interactions
    Mockito.verify(adapter).execute(
        eq(HTTPMethod.GET),
        eq("v1/namespaces/ns/tables/table"),
        any(),
        any(),
        eq(LoadTableResponse.class),
        eq(catalogHeaders),
        any());
  }

  @Test
  public void testCatalogBearerTokenWithClientCredential() {
    Map<String, String> contextHeaders = ImmutableMap.of(
        "Authorization", "Bearer client-credentials-token:sub=user");
    Map<String, String> catalogHeaders = ImmutableMap.of(
        "Authorization", "Bearer bearer-token");

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    SessionCatalog.SessionContext context = new SessionCatalog.SessionContext(
        UUID.randomUUID().toString(), "user", ImmutableMap.of("credential", "user:secret"), ImmutableMap.of());

    RESTCatalog catalog = new RESTCatalog(context, (config) -> adapter);
    catalog.initialize("prod", ImmutableMap.of(
        CatalogProperties.URI, "ignored",
        "token", "bearer-token"));

    Assertions.assertFalse(catalog.tableExists(TableIdentifier.of("ns", "table")));

    // use the bearer token for config
    Mockito.verify(adapter).execute(
        eq(HTTPMethod.GET), eq("v1/config"), any(), any(), eq(ConfigResponse.class), eq(catalogHeaders), any());
    // use the bearer token to fetch the context token
    Mockito.verify(adapter).execute(
        eq(HTTPMethod.POST),
        eq("v1/oauth/tokens"),
        any(),
        any(),
        eq(OAuthTokenResponse.class),
        eq(catalogHeaders),
        any());
    // use the context token for table load
    Mockito.verify(adapter).execute(
        eq(HTTPMethod.GET),
        eq("v1/namespaces/ns/tables/table"),
        any(),
        any(),
        eq(LoadTableResponse.class),
        eq(contextHeaders),
        any());
  }

  @Test
  public void testCatalogCredentialWithClientCredential() {
    Map<String, String> emptyHeaders = ImmutableMap.of();
    Map<String, String> contextHeaders = ImmutableMap.of(
        "Authorization", "Bearer client-credentials-token:sub=user");
    Map<String, String> catalogHeaders = ImmutableMap.of(
        "Authorization", "Bearer client-credentials-token:sub=catalog");

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    SessionCatalog.SessionContext context = new SessionCatalog.SessionContext(
        UUID.randomUUID().toString(), "user", ImmutableMap.of("credential", "user:secret"), ImmutableMap.of());

    RESTCatalog catalog = new RESTCatalog(context, (config) -> adapter);
    catalog.initialize("prod", ImmutableMap.of(
        CatalogProperties.URI, "ignored",
        "credential", "catalog:secret"));

    Assertions.assertFalse(catalog.tableExists(TableIdentifier.of("ns", "table")));

    // call client credentials with no initial auth
    Mockito.verify(adapter).execute(
        eq(HTTPMethod.POST),
        eq("v1/oauth/tokens"),
        any(),
        any(),
        eq(OAuthTokenResponse.class),
        eq(emptyHeaders),
        any());
    // use the client credential token for config
    Mockito.verify(adapter).execute(
        eq(HTTPMethod.GET), eq("v1/config"), any(), any(), eq(ConfigResponse.class), eq(catalogHeaders), any());
    // use the client credential to fetch the context token
    Mockito.verify(adapter).execute(
        eq(HTTPMethod.POST),
        eq("v1/oauth/tokens"),
        any(),
        any(),
        eq(OAuthTokenResponse.class),
        eq(catalogHeaders),
        any());
    // use the context token for table load
    Mockito.verify(adapter).execute(
        eq(HTTPMethod.GET),
        eq("v1/namespaces/ns/tables/table"),
        any(),
        any(),
        eq(LoadTableResponse.class),
        eq(contextHeaders),
        any());
  }

  @Test
  public void testCatalogBearerTokenAndCredentialWithClientCredential() {
    Map<String, String> contextHeaders = ImmutableMap.of(
        "Authorization", "Bearer client-credentials-token:sub=user");
    Map<String, String> initHeaders = ImmutableMap.of(
        "Authorization", "Bearer bearer-token");
    Map<String, String> catalogHeaders = ImmutableMap.of(
        "Authorization", "Bearer client-credentials-token:sub=catalog");

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    SessionCatalog.SessionContext context = new SessionCatalog.SessionContext(
        UUID.randomUUID().toString(), "user", ImmutableMap.of("credential", "user:secret"), ImmutableMap.of());

    RESTCatalog catalog = new RESTCatalog(context, (config) -> adapter);
    catalog.initialize("prod", ImmutableMap.of(
        CatalogProperties.URI, "ignored",
        "credential", "catalog:secret",
        "token", "bearer-token"));

    Assertions.assertFalse(catalog.tableExists(TableIdentifier.of("ns", "table")));

    // use the bearer token for client credentials
    Mockito.verify(adapter).execute(
        eq(HTTPMethod.POST), eq("v1/oauth/tokens"), any(), any(), eq(OAuthTokenResponse.class), eq(initHeaders), any());
    // use the client credential token for config
    Mockito.verify(adapter).execute(
        eq(HTTPMethod.GET), eq("v1/config"), any(), any(), eq(ConfigResponse.class), eq(catalogHeaders), any());
    // use the client credential to fetch the context token
    Mockito.verify(adapter).execute(
        eq(HTTPMethod.POST),
        eq("v1/oauth/tokens"),
        any(),
        any(),
        eq(OAuthTokenResponse.class),
        eq(catalogHeaders),
        any());
    // use the context token for table load
    Mockito.verify(adapter).execute(
        eq(HTTPMethod.GET),
        eq("v1/namespaces/ns/tables/table"),
        any(),
        any(),
        eq(LoadTableResponse.class),
        eq(contextHeaders),
        any());
  }

  @Test
  public void testClientBearerToken() {
    testClientAuth("bearer-token",
        ImmutableMap.of(
            "token", "client-bearer-token",
            "credential", "user:secret",
            "urn:ietf:params:oauth:token-type:id_token", "id-token",
            "urn:ietf:params:oauth:token-type:access_token", "access-token",
            "urn:ietf:params:oauth:token-type:jwt", "jwt-token",
            "urn:ietf:params:oauth:token-type:saml2", "saml2-token",
            "urn:ietf:params:oauth:token-type:saml1", "saml1-token"),
        ImmutableMap.of("Authorization", "Bearer client-bearer-token"));
  }

  @Test
  public void testClientCredential() {
    testClientAuth("bearer-token",
        ImmutableMap.of(
            "credential", "user:secret",
            "urn:ietf:params:oauth:token-type:id_token", "id-token",
            "urn:ietf:params:oauth:token-type:access_token", "access-token",
            "urn:ietf:params:oauth:token-type:jwt", "jwt-token",
            "urn:ietf:params:oauth:token-type:saml2", "saml2-token",
            "urn:ietf:params:oauth:token-type:saml1", "saml1-token"),
        ImmutableMap.of("Authorization", "Bearer client-credentials-token:sub=user"));
  }

  @Test
  public void testClientIDToken() {
    testClientAuth("bearer-token",
        ImmutableMap.of(
            "urn:ietf:params:oauth:token-type:id_token", "id-token",
            "urn:ietf:params:oauth:token-type:access_token", "access-token",
            "urn:ietf:params:oauth:token-type:jwt", "jwt-token",
            "urn:ietf:params:oauth:token-type:saml2", "saml2-token",
            "urn:ietf:params:oauth:token-type:saml1", "saml1-token"),
        ImmutableMap.of("Authorization", "Bearer token-exchange-token:sub=id-token,act=bearer-token"));
  }

  @Test
  public void testClientAccessToken() {
    testClientAuth("bearer-token",
        ImmutableMap.of(
            "urn:ietf:params:oauth:token-type:access_token", "access-token",
            "urn:ietf:params:oauth:token-type:jwt", "jwt-token",
            "urn:ietf:params:oauth:token-type:saml2", "saml2-token",
            "urn:ietf:params:oauth:token-type:saml1", "saml1-token"),
        ImmutableMap.of("Authorization", "Bearer token-exchange-token:sub=access-token,act=bearer-token"));
  }

  @Test
  public void testClientJWTToken() {
    testClientAuth("bearer-token",
        ImmutableMap.of(
            "urn:ietf:params:oauth:token-type:jwt", "jwt-token",
            "urn:ietf:params:oauth:token-type:saml2", "saml2-token",
            "urn:ietf:params:oauth:token-type:saml1", "saml1-token"),
        ImmutableMap.of("Authorization", "Bearer token-exchange-token:sub=jwt-token,act=bearer-token"));
  }

  @Test
  public void testClientSAML2Token() {
    testClientAuth("bearer-token",
        ImmutableMap.of(
            "urn:ietf:params:oauth:token-type:saml2", "saml2-token",
            "urn:ietf:params:oauth:token-type:saml1", "saml1-token"),
        ImmutableMap.of("Authorization", "Bearer token-exchange-token:sub=saml2-token,act=bearer-token"));
  }

  @Test
  public void testClientSAML1Token() {
    testClientAuth("bearer-token",
        ImmutableMap.of("urn:ietf:params:oauth:token-type:saml1", "saml1-token"),
        ImmutableMap.of("Authorization", "Bearer token-exchange-token:sub=saml1-token,act=bearer-token"));
  }

  private void testClientAuth(String catalogToken, Map<String, String> credentials,
                              Map<String, String> expectedHeaders) {
    Map<String, String> catalogHeaders = ImmutableMap.of(
        "Authorization", "Bearer " + catalogToken);

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    SessionCatalog.SessionContext context = new SessionCatalog.SessionContext(
        UUID.randomUUID().toString(), "user", credentials, ImmutableMap.of());

    RESTCatalog catalog = new RESTCatalog(context, (config) -> adapter);
    catalog.initialize("prod", ImmutableMap.of(CatalogProperties.URI, "ignored", "token", catalogToken));

    Assertions.assertFalse(catalog.tableExists(TableIdentifier.of("ns", "table")));

    Mockito.verify(adapter).execute(
        eq(HTTPMethod.GET), eq("v1/config"), any(), any(), eq(ConfigResponse.class), eq(catalogHeaders), any());

    // token passes a static token. otherwise, validate a client credentials or token exchange request
    if (!credentials.containsKey("token")) {
      Mockito.verify(adapter).execute(
          eq(HTTPMethod.POST),
          eq("v1/oauth/tokens"),
          any(),
          any(),
          eq(OAuthTokenResponse.class),
          eq(catalogHeaders),
          any());
    }

    Mockito.verify(adapter).execute(
        eq(HTTPMethod.GET),
        eq("v1/namespaces/ns/tables/table"),
        any(),
        any(),
        eq(LoadTableResponse.class),
        eq(expectedHeaders),
        any());
  }

  @Test
  public void testTableBearerToken() {
    testTableAuth("catalog",
        ImmutableMap.of("urn:ietf:params:oauth:token-type:id_token", "id-token"),
        ImmutableMap.of("token", "table-bearer-token"),
        ImmutableMap.of("Authorization", "Bearer token-exchange-token:sub=id-token,act=catalog"),
        ImmutableMap.of("Authorization", "Bearer table-bearer-token"));
  }

  @Test
  public void testTableIDToken() {
    testTableAuth("catalog",
        ImmutableMap.of("urn:ietf:params:oauth:token-type:id_token", "id-token"),
        ImmutableMap.of("urn:ietf:params:oauth:token-type:id_token", "table-id-token"),
        ImmutableMap.of("Authorization", "Bearer token-exchange-token:sub=id-token,act=catalog"),
        ImmutableMap.of("Authorization",
            "Bearer token-exchange-token:sub=table-id-token,act=token-exchange-token:sub=id-token,act=catalog"));
  }

  @Test
  public void testTableCredential() {
    testTableAuth("catalog",
        ImmutableMap.of("urn:ietf:params:oauth:token-type:id_token", "id-token"),
        ImmutableMap.of("credential", "table-user:secret"),
        ImmutableMap.of("Authorization", "Bearer token-exchange-token:sub=id-token,act=catalog"),
        ImmutableMap.of("Authorization", "Bearer client-credentials-token:sub=table-user"));
  }

  public void testTableAuth(String catalogToken, Map<String, String> credentials, Map<String, String> tableConfig,
                            Map<String, String> expectedContextHeaders, Map<String, String> expectedTableHeaders) {
    TableIdentifier ident = TableIdentifier.of("ns", "table");
    Map<String, String> catalogHeaders = ImmutableMap.of(
        "Authorization", "Bearer " + catalogToken);

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    // inject the expected table config
    Answer<LoadTableResponse> addTableConfig = invocation -> {
      LoadTableResponse loadTable = (LoadTableResponse) invocation.callRealMethod();
      return LoadTableResponse.builder()
          .withTableMetadata(loadTable.tableMetadata())
          .addAllConfig(loadTable.config())
          .addAllConfig(tableConfig)
          .build();
    };

    Mockito.doAnswer(addTableConfig).when(adapter).execute(
        eq(HTTPMethod.POST),
        eq("v1/namespaces/ns/tables"),
        any(),
        any(),
        eq(LoadTableResponse.class),
        eq(expectedContextHeaders),
        any());

    Mockito.doAnswer(addTableConfig).when(adapter).execute(
        eq(HTTPMethod.GET),
        eq("v1/namespaces/ns/tables/table"),
        any(),
        any(),
        eq(LoadTableResponse.class),
        eq(expectedContextHeaders),
        any());

    SessionCatalog.SessionContext context = new SessionCatalog.SessionContext(
        UUID.randomUUID().toString(), "user", credentials, ImmutableMap.of());

    RESTCatalog catalog = new RESTCatalog(context, (config) -> adapter);
    catalog.initialize("prod", ImmutableMap.of(CatalogProperties.URI, "ignored", "token", catalogToken));

    Schema expectedSchema = new Schema(
        required(1, "id", Types.IntegerType.get(), "unique ID"),
        required(2, "data", Types.StringType.get())
    );

    Table table = catalog.createTable(ident, expectedSchema);
    Assertions.assertEquals(expectedSchema.asStruct(), table.schema().asStruct(), "Schema should match");

    Table loaded = catalog.loadTable(ident); // the first load will send the token
    Assertions.assertEquals(expectedSchema.asStruct(), loaded.schema().asStruct(), "Schema should match");

    loaded.refresh(); // refresh to force reload

    Mockito.verify(adapter).execute(
        eq(HTTPMethod.GET), eq("v1/config"), any(), any(), eq(ConfigResponse.class), eq(catalogHeaders), any());
    // session client credentials flow
    Mockito.verify(adapter).execute(
        eq(HTTPMethod.POST),
        eq("v1/oauth/tokens"),
        any(),
        any(),
        eq(OAuthTokenResponse.class),
        eq(catalogHeaders),
        any());

    // create table request
    Mockito.verify(adapter).execute(
        eq(HTTPMethod.POST),
        eq("v1/namespaces/ns/tables"),
        any(),
        any(),
        eq(LoadTableResponse.class),
        eq(expectedContextHeaders),
        any());

    // if the table returned a bearer token, there will be no token request
    if (!tableConfig.containsKey("token")) {
      // client credentials or token exchange to get a table token
      Mockito.verify(adapter, times(2)).execute(
          eq(HTTPMethod.POST),
          eq("v1/oauth/tokens"),
          any(),
          any(),
          eq(OAuthTokenResponse.class),
          eq(expectedContextHeaders),
          any());
    }

    // automatic refresh when metadata is accessed after commit
    Mockito.verify(adapter).execute(
        eq(HTTPMethod.GET),
        eq("v1/namespaces/ns/tables/table"),
        any(),
        any(),
        eq(LoadTableResponse.class),
        eq(expectedTableHeaders),
        any());

    // load table from catalog
    Mockito.verify(adapter).execute(
        eq(HTTPMethod.GET),
        eq("v1/namespaces/ns/tables/table"),
        any(),
        any(),
        eq(LoadTableResponse.class),
        eq(expectedContextHeaders),
        any());

    // refresh loaded table
    Mockito.verify(adapter).execute(
        eq(HTTPMethod.GET),
        eq("v1/namespaces/ns/tables/table"),
        any(),
        any(),
        eq(LoadTableResponse.class),
        eq(expectedTableHeaders),
        any());
  }

  @Test
  public void testCatalogTokenRefresh() throws Exception {
    Map<String, String> emptyHeaders = ImmutableMap.of();
    Map<String, String> catalogHeaders = ImmutableMap.of(
        "Authorization", "Bearer client-credentials-token:sub=catalog");

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    Answer<OAuthTokenResponse> addOneSecondExpiration = invocation -> {
      OAuthTokenResponse response = (OAuthTokenResponse) invocation.callRealMethod();
      return OAuthTokenResponse.builder()
          .withToken(response.token())
          .withTokenType(response.tokenType())
          .withIssuedTokenType(response.issuedTokenType())
          .addScopes(response.scopes())
          .setExpirationInSeconds(1)
          .build();
    };

    Mockito.doAnswer(addOneSecondExpiration).when(adapter).execute(
        eq(HTTPMethod.POST),
        eq("v1/oauth/tokens"),
        any(),
        any(),
        eq(OAuthTokenResponse.class),
        any(),
        any());

    Map<String, String> contextCredentials = ImmutableMap.of();
    SessionCatalog.SessionContext context = new SessionCatalog.SessionContext(
        UUID.randomUUID().toString(), "user", contextCredentials, ImmutableMap.of());

    RESTCatalog catalog = new RESTCatalog(context, (config) -> adapter);
    catalog.initialize("prod", ImmutableMap.of(CatalogProperties.URI, "ignored", "credential", "catalog:secret"));

    Thread.sleep(3_000); // sleep until after 2 refresh calls

    // call client credentials with no initial auth
    Mockito.verify(adapter).execute(
        eq(HTTPMethod.POST),
        eq("v1/oauth/tokens"),
        any(),
        any(),
        eq(OAuthTokenResponse.class),
        eq(emptyHeaders),
        any());

    // use the client credential token for config
    Mockito.verify(adapter).execute(
        eq(HTTPMethod.GET), eq("v1/config"), any(), any(), eq(ConfigResponse.class), eq(catalogHeaders), any());

    // verify the first token exchange
    Map<String, String> firstRefreshRequest = ImmutableMap.of(
        "grant_type", "urn:ietf:params:oauth:grant-type:token-exchange",
        "subject_token", "client-credentials-token:sub=catalog",
        "subject_token_type", "urn:ietf:params:oauth:token-type:access_token",
        "scope", "catalog"
    );
    Mockito.verify(adapter).execute(
        eq(HTTPMethod.POST),
        eq("v1/oauth/tokens"),
        any(),
        Mockito.argThat(firstRefreshRequest::equals),
        eq(OAuthTokenResponse.class),
        eq(catalogHeaders),
        any());

    // verify that a second exchange occurs
    Map<String, String> secondRefreshRequest = ImmutableMap.of(
        "grant_type", "urn:ietf:params:oauth:grant-type:token-exchange",
        "subject_token", "token-exchange-token:sub=client-credentials-token:sub=catalog",
        "subject_token_type", "urn:ietf:params:oauth:token-type:access_token",
        "scope", "catalog"
    );
    Map<String, String> secondRefreshHeaders = ImmutableMap.of(
        "Authorization", "Bearer token-exchange-token:sub=client-credentials-token:sub=catalog"
    );
    Mockito.verify(adapter).execute(
        eq(HTTPMethod.POST),
        eq("v1/oauth/tokens"),
        any(),
        Mockito.argThat(secondRefreshRequest::equals),
        eq(OAuthTokenResponse.class),
        eq(secondRefreshHeaders),
        any());
  }

  @Test
  public void testCatalogRefreshedTokenIsUsed() throws Exception {
    Map<String, String> emptyHeaders = ImmutableMap.of();
    Map<String, String> catalogHeaders = ImmutableMap.of(
        "Authorization", "Bearer client-credentials-token:sub=catalog");

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    Answer<OAuthTokenResponse> addOneSecondExpiration = invocation -> {
      OAuthTokenResponse response = (OAuthTokenResponse) invocation.callRealMethod();
      return OAuthTokenResponse.builder()
          .withToken(response.token())
          .withTokenType(response.tokenType())
          .withIssuedTokenType(response.issuedTokenType())
          .addScopes(response.scopes())
          .setExpirationInSeconds(1)
          .build();
    };

    Mockito.doAnswer(addOneSecondExpiration).when(adapter).execute(
        eq(HTTPMethod.POST),
        eq("v1/oauth/tokens"),
        any(),
        any(),
        eq(OAuthTokenResponse.class),
        any(),
        any());

    Map<String, String> contextCredentials = ImmutableMap.of();
    SessionCatalog.SessionContext context = new SessionCatalog.SessionContext(
        UUID.randomUUID().toString(), "user", contextCredentials, ImmutableMap.of());

    RESTCatalog catalog = new RESTCatalog(context, (config) -> adapter);
    catalog.initialize("prod", ImmutableMap.of(CatalogProperties.URI, "ignored", "credential", "catalog:secret"));

    Thread.sleep(1_100); // sleep until after 2 refresh calls

    // use the exchanged catalog token
    Assertions.assertFalse(catalog.tableExists(TableIdentifier.of("ns", "table")));

    // call client credentials with no initial auth
    Mockito.verify(adapter).execute(
        eq(HTTPMethod.POST),
        eq("v1/oauth/tokens"),
        any(),
        any(),
        eq(OAuthTokenResponse.class),
        eq(emptyHeaders),
        any());

    // use the client credential token for config
    Mockito.verify(adapter).execute(
        eq(HTTPMethod.GET), eq("v1/config"), any(), any(), eq(ConfigResponse.class), eq(catalogHeaders), any());

    // verify the first token exchange
    Map<String, String> firstRefreshRequest = ImmutableMap.of(
        "grant_type", "urn:ietf:params:oauth:grant-type:token-exchange",
        "subject_token", "client-credentials-token:sub=catalog",
        "subject_token_type", "urn:ietf:params:oauth:token-type:access_token",
        "scope", "catalog"
    );
    Mockito.verify(adapter).execute(
        eq(HTTPMethod.POST),
        eq("v1/oauth/tokens"),
        any(),
        Mockito.argThat(firstRefreshRequest::equals),
        eq(OAuthTokenResponse.class),
        eq(catalogHeaders),
        any());

    // use the refreshed context token for table load
    Map<String, String> refreshedCatalogHeader = ImmutableMap.of(
        "Authorization", "Bearer token-exchange-token:sub=client-credentials-token:sub=catalog"
    );
    Mockito.verify(adapter).execute(
        eq(HTTPMethod.GET),
        eq("v1/namespaces/ns/tables/table"),
        any(),
        any(),
        eq(LoadTableResponse.class),
        eq(refreshedCatalogHeader),
        any());
  }
}
