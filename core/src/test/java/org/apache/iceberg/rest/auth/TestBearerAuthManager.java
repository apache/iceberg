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
package org.apache.iceberg.rest.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.HTTPHeaders.HTTPHeader;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.HTTPRequest.HTTPMethod;
import org.apache.iceberg.rest.ImmutableHTTPRequest;
import org.apache.iceberg.rest.RESTClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

class TestBearerAuthManager {

  private static final HTTPRequest REQUEST =
      ImmutableHTTPRequest.builder()
          .method(HTTPMethod.GET)
          .baseUri(URI.create("https://localhost"))
          .path("path")
          .build();

  @TempDir private Path tempDir;

  private Path tokenFile;

  @BeforeEach
  void before() throws IOException {
    this.tokenFile = tempDir.resolve("token");
    Files.writeString(tokenFile, "file-token\n");
  }

  @Test
  void tokenFileMissing() {
    Path missing = tempDir.resolve("missing");
    Map<String, String> properties = Map.of(BearerAuthConfig.BEARER_TOKEN_FILE, missing.toString());
    try (AuthManager manager = new BearerAuthManager()) {
      assertThatThrownBy(() -> manager.catalogSession(null, properties))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Invalid token file: %s does not exist or is not a file", missing);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"file:/var/run/token", "https://host/token", "~/token", "$HOME/token"})
  void tokenFileNotALocalPath(String path) {
    // URIs and shell expansions are not interpreted: they resolve to relative paths that don't
    // exist
    Map<String, String> properties = Map.of(BearerAuthConfig.BEARER_TOKEN_FILE, path);
    try (AuthManager manager = new BearerAuthManager()) {
      assertThatThrownBy(() -> manager.catalogSession(null, properties))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage(
              "Invalid token file: %s does not exist or is not a file",
              Paths.get(path).toAbsolutePath().normalize());
    }
  }

  @Test
  void invalidStaticToken() {
    Map<String, String> properties = Map.of(BearerAuthConfig.BEARER_TOKEN, "s3cr3t\r\nX-Foo: bar");
    try (AuthManager manager = new BearerAuthManager()) {
      assertThatThrownBy(() -> manager.catalogSession(null, properties))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Invalid bearer token: contains characters not allowed by RFC 6750");
    }
  }

  @Test
  void staticToken() {
    Map<String, String> properties = Map.of(BearerAuthConfig.BEARER_TOKEN, "static-token");
    try (AuthManager manager = new BearerAuthManager();
        AuthSession session = manager.catalogSession(null, properties)) {
      assertAuthorization(session, "Bearer static-token");
    }
  }

  @Test
  void tokenFile() {
    Map<String, String> properties =
        Map.of(
            BearerAuthConfig.BEARER_TOKEN_FILE,
            tokenFile.toString(),
            BearerAuthConfig.BEARER_TOKEN_FILE_RELOAD_INTERVAL,
            "PT1S");
    try (AuthManager manager = new BearerAuthManager();
        AuthSession session = manager.catalogSession(null, properties)) {
      assertAuthorization(session, "Bearer file-token");
    }
  }

  @Test
  void contextualSessionReturnsParent() {
    Map<String, String> properties = Map.of(BearerAuthConfig.BEARER_TOKEN, "static-token");
    SessionCatalog.SessionContext context =
        new SessionCatalog.SessionContext(
            "id", "user", Map.of(OAuth2Properties.TOKEN, "user-token"), Map.of());
    try (AuthManager manager = new BearerAuthManager();
        AuthSession parent = manager.catalogSession(null, properties)) {
      assertThat(manager.contextualSession(context, parent)).isSameAs(parent);
    }
  }

  @Test
  void tableSessionReturnsParent() {
    Map<String, String> properties = Map.of(BearerAuthConfig.BEARER_TOKEN, "static-token");
    try (AuthManager manager = new BearerAuthManager();
        AuthSession parent = manager.catalogSession(null, properties)) {
      AuthSession tableSession =
          manager.tableSession(
              TableIdentifier.of("ns", "table"),
              Map.of(OAuth2Properties.TOKEN, "vended-token"),
              parent);
      assertThat(tableSession).isSameAs(parent);
    }
  }

  @Test
  void standaloneTableSessionCached() throws IOException {
    Path otherFile = tempDir.resolve("other");
    Files.writeString(otherFile, "other-token");
    Map<String, String> fileProperties =
        Map.of(BearerAuthConfig.BEARER_TOKEN_FILE, tokenFile.toString());
    Map<String, String> sameFileProperties =
        Map.of(
            BearerAuthConfig.BEARER_TOKEN_FILE, tempDir.resolve(".").resolve("token").toString());
    Map<String, String> otherIntervalProperties =
        Map.of(
            BearerAuthConfig.BEARER_TOKEN_FILE,
            tokenFile.toString(),
            BearerAuthConfig.BEARER_TOKEN_FILE_RELOAD_INTERVAL,
            "PT5M");
    Map<String, String> otherFileProperties =
        Map.of(BearerAuthConfig.BEARER_TOKEN_FILE, otherFile.toString());
    Map<String, String> staticProperties = Map.of(BearerAuthConfig.BEARER_TOKEN, "static-token");

    RESTClient restClient = Mockito.mock(RESTClient.class);

    try (AuthManager manager = new BearerAuthManager()) {
      AuthSession fileSession = manager.tableSession(restClient, fileProperties);
      assertAuthorization(fileSession, "Bearer file-token");
      assertThat(manager.tableSession(restClient, fileProperties)).isSameAs(fileSession);
      assertThat(manager.tableSession(restClient, sameFileProperties)).isSameAs(fileSession);
      assertThat(manager.tableSession(restClient, otherIntervalProperties))
          .isNotSameAs(fileSession);

      AuthSession otherFileSession = manager.tableSession(restClient, otherFileProperties);
      assertAuthorization(otherFileSession, "Bearer other-token");
      assertThat(otherFileSession).isNotSameAs(fileSession);

      AuthSession staticSession = manager.tableSession(restClient, staticProperties);
      assertAuthorization(staticSession, "Bearer static-token");
      assertThat(manager.tableSession(restClient, staticProperties)).isSameAs(staticSession);
    }
  }

  @Test
  void standaloneTableSessionCacheClearedOnClose() {
    Map<String, String> properties = Map.of(BearerAuthConfig.BEARER_TOKEN, "static-token");
    RESTClient restClient = Mockito.mock(RESTClient.class);
    try (BearerAuthManager manager = new BearerAuthManager()) {
      AuthSession session = manager.tableSession(restClient, properties);
      assertThat(manager.sessionCache().asMap()).hasSize(1);

      manager.close();
      assertThat(manager.sessionCache().asMap()).isEmpty();
      assertThat(manager.tableSession(restClient, properties)).isNotSameAs(session);
    }
  }

  private static void assertAuthorization(AuthSession session, String expected) {
    assertThat(session.authenticate(REQUEST).headers().entries())
        .singleElement()
        .extracting(HTTPHeader::name, HTTPHeader::value)
        .containsExactly("Authorization", expected);
  }
}
