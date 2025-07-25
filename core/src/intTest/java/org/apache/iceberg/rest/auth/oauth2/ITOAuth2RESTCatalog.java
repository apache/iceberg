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
package org.apache.iceberg.rest.auth.oauth2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nimbusds.oauth2.sdk.GrantType;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.token.AccessToken;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.CatalogTests;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.HTTPHeaders;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTCatalogAdapter;
import org.apache.iceberg.rest.RESTCatalogServlet;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.auth.oauth2.client.OAuth2Client;
import org.apache.iceberg.rest.auth.oauth2.config.ConfigUtil;
import org.apache.iceberg.rest.auth.oauth2.test.ImmutableTestEnvironment;
import org.apache.iceberg.rest.auth.oauth2.test.TestEnvironment;
import org.apache.iceberg.rest.auth.oauth2.test.container.KeycloakContainer;
import org.apache.iceberg.rest.auth.oauth2.test.junit.KeycloakExtension;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link CatalogTests} for {@link RESTCatalog} with {@link OAuth2Manager} and a real Keycloak.
 *
 * <p>In these tests, OAuth2 is configured as follows:
 *
 * <ul>
 *   <li>Catalog session: uses client_credentials as an initial grant, and token exchange for token
 *       refreshes (legacy behavior).
 *   <li>Session context: uses token exchange as the initial grant. The subject token is obtained
 *       off-band with the client_credentials grant. The actor token comes from the parent catalog
 *       session. Token refreshes use the standard refresh_token grant.
 * </ul>
 *
 * The equivalent catalog configuration would be:
 *
 * <pre>{@code
 * rest.auth.type=oauth2
 * rest.auth.oauth2.issuer-url=https://<authorization-server-url>
 * rest.auth.oauth2.grant-type=client_credentials
 * rest.auth.oauth2.client-id=<client-id>
 * rest.auth.oauth2.client-secret=<client-secret>
 * rest.auth.oauth2.scope=catalog
 * }</pre>
 *
 * The equivalent session context configuration would be:
 *
 * <pre>{@code
 * rest.auth.oauth2.issuer-url=https://<authorization-server-url>
 * rest.auth.oauth2.grant-type=urn:ietf:params:oauth:grant-type:token-exchange
 * rest.auth.oauth2.client-id=<client-id>
 * rest.auth.oauth2.client-secret=<client-secret>
 * rest.auth.oauth2.scope=session
 * rest.auth.oauth2.token-exchange.subject-token=<static-subject-token>
 * rest.auth.oauth2.token-exchange.actor-token=::parent::
 * }</pre>
 */
@ExtendWith(KeycloakExtension.class)
public class ITOAuth2RESTCatalog extends CatalogTests<RESTCatalog> {

  @TempDir public static Path tempDir;

  private static InMemoryCatalog backendCatalog;
  private static Server httpServer;
  private static TestEnvironment testEnvironment;

  private RESTCatalog restCatalog;

  @BeforeAll
  static void beforeClass(
      KeycloakContainer keycloakContainer,
      ImmutableTestEnvironment.Builder envBuilder1,
      ImmutableTestEnvironment.Builder envBuilder2)
      throws Exception {
    backendCatalog = new InMemoryCatalog();
    backendCatalog.initialize(
        "in-memory",
        ImmutableMap.of(
            CatalogProperties.WAREHOUSE_LOCATION,
            tempDir.resolve("warehouse").toAbsolutePath().toString()));

    ServletContextHandler servletContext =
        new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    servletContext.addServlet(
        new ServletHolder(
            new RESTCatalogServlet(
                new RESTCatalogAdapter(backendCatalog) {
                  @Override
                  public <T extends RESTResponse> T execute(
                      HTTPRequest request,
                      Class<T> responseType,
                      Consumer<ErrorResponse> errorHandler,
                      Consumer<Map<String, String>> responseHeaders) {
                    // validate the oauth2 token
                    HTTPHeaders.HTTPHeader authorization =
                        request.headers().firstEntry("Authorization").orElseThrow();
                    String authHeader = authorization.value();
                    String token = authHeader.substring("Bearer ".length());
                    keycloakContainer.verifyToken(token);
                    return super.execute(request, responseType, errorHandler, responseHeaders);
                  }
                })),
        "/*");

    httpServer = new Server(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
    httpServer.setHandler(servletContext);
    httpServer.start();

    AccessToken subjectToken;
    try (TestEnvironment env =
            envBuilder1
                .grantType(GrantType.CLIENT_CREDENTIALS)
                .clientId(new ClientID(KeycloakExtension.CLIENT_ID2))
                .clientSecret(new Secret(KeycloakExtension.CLIENT_SECRET2))
                .build();
        OAuth2Client subjectClient = env.newOAuth2Client()) {
      subjectToken = subjectClient.authenticate();
    }

    testEnvironment =
        envBuilder2
            .catalogServerUrl(httpServer.getURI())
            // Catalog session
            .grantType(GrantType.CLIENT_CREDENTIALS)
            .clientId(new ClientID(KeycloakExtension.CLIENT_ID1))
            .clientSecret(new Secret(KeycloakExtension.CLIENT_SECRET1))
            // Contextual session
            .sessionContextGrantType(GrantType.TOKEN_EXCHANGE)
            .sessionContextSubjectTokenString(subjectToken.getValue())
            .sessionContextActorTokenString(ConfigUtil.PARENT_TOKEN)
            .build();
  }

  @AfterAll
  static void afterClass() throws Exception {
    if (testEnvironment != null) {
      testEnvironment.close();
    }

    if (httpServer != null) {
      httpServer.stop();
      httpServer.join();
    }

    if (backendCatalog != null) {
      backendCatalog.close();
    }
  }

  @BeforeEach
  public void before() {
    restCatalog = initCatalog("oauth2-test-catalog", Map.of());
  }

  @AfterEach
  public void after() throws Exception {
    if (restCatalog != null) {
      restCatalog.close();
    }

    if (backendCatalog != null) {
      backendCatalog.close(); // clears the in-memory data structures
    }
  }

  @Override
  @SuppressWarnings("MustBeClosedChecker")
  protected RESTCatalog initCatalog(String catalogName, Map<String, String> additionalProperties) {
    return testEnvironment.newCatalog(additionalProperties);
  }

  @Override
  protected RESTCatalog catalog() {
    return restCatalog;
  }

  @Override
  protected boolean supportsServerSideRetry() {
    return true;
  }

  @Override
  protected boolean supportsNestedNamespaces() {
    return true;
  }

  @Override
  protected boolean requiresNamespaceCreate() {
    return true;
  }

  @Test
  @Override
  @SuppressWarnings("resource")
  public void testLoadTableWithMissingMetadataFile(@TempDir Path ignored) {

    if (requiresNamespaceCreate()) {
      restCatalog.createNamespace(TBL.namespace());
    }

    restCatalog.buildTable(TBL, SCHEMA).create();
    assertThat(restCatalog.tableExists(TBL)).as("Table should exist").isTrue();

    Table table = restCatalog.loadTable(TBL);
    String metadataFileLocation =
        ((HasTableOperations) table).operations().current().metadataFileLocation();
    table.io().deleteFile(metadataFileLocation);

    assertThatThrownBy(() -> restCatalog.loadTable(TBL))
        .isInstanceOf(NotFoundException.class)
        .hasMessageContaining("No in-memory file found for location: " + metadataFileLocation);
  }
}
