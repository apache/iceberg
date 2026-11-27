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

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.junit.jupiter.api.Test;

class TestLegacyConfigAdapter {

  private static final String CATALOG_URI = "https://catalog.example.com/api/";

  private final LegacyConfigAdapter adapter = new LegacyConfigAdapter();

  @Test
  void migrateCredentialWithClientId() {
    Map<String, String> migrated =
        adapter.migrate(Map.of(OAuth2Properties.CREDENTIAL, "client1:s3cr3t"));
    assertThat(migrated)
        .containsExactlyInAnyOrderEntriesOf(
            Map.of(
                OAuth2Config.CLIENT_ID, "client1",
                OAuth2Config.CLIENT_SECRET, "s3cr3t",
                OAuth2Config.SCOPE, OAuth2Properties.CATALOG_SCOPE));
  }

  @Test
  void migrateCredentialWithoutClientId() {
    Map<String, String> migrated = adapter.migrate(Map.of(OAuth2Properties.CREDENTIAL, "s3cr3t"));
    assertThat(migrated)
        .containsExactlyInAnyOrderEntriesOf(
            Map.of(
                OAuth2Config.CLIENT_ID, LegacyConfigAdapter.DEFAULT_CLIENT_ID,
                OAuth2Config.CLIENT_SECRET, "s3cr3t",
                OAuth2Config.SCOPE, OAuth2Properties.CATALOG_SCOPE));
  }

  @Test
  void migrateCredentialWithColonInSecret() {
    Map<String, String> migrated =
        adapter.migrate(Map.of(OAuth2Properties.CREDENTIAL, "client1:s3:cr3t"));
    assertThat(migrated)
        .containsExactlyInAnyOrderEntriesOf(
            Map.of(
                OAuth2Config.CLIENT_ID, "client1",
                OAuth2Config.CLIENT_SECRET, "s3:cr3t",
                OAuth2Config.SCOPE, OAuth2Properties.CATALOG_SCOPE));
  }

  @Test
  void migrateCredentialWithExplicitScope() {
    Map<String, String> migrated =
        adapter.migrate(
            Map.of(OAuth2Properties.CREDENTIAL, "client1:s3cr3t", OAuth2Config.SCOPE, "custom"));
    assertThat(migrated).containsEntry(OAuth2Config.SCOPE, "custom");
  }

  @Test
  void migrateOtherLegacyProperties() {
    Map<String, String> migrated =
        adapter.migrate(
            Map.of(
                OAuth2Properties.OAUTH2_SERVER_URI, "https://auth.example.com/token",
                OAuth2Properties.SCOPE, "catalog",
                OAuth2Properties.AUDIENCE, "catalog-api",
                OAuth2Properties.RESOURCE, "urn:example"));
    assertThat(migrated)
        .containsExactlyInAnyOrderEntriesOf(
            Map.of(
                OAuth2Config.TOKEN_ENDPOINT,
                "https://auth.example.com/token",
                OAuth2Config.SCOPE,
                "catalog",
                OAuth2Config.AUDIENCE,
                "catalog-api",
                OAuth2Config.RESOURCE,
                "urn:example"));
  }

  @Test
  void ignoreUnsupportedLegacyProperties() {
    Map<String, String> migrated =
        adapter.migrate(
            Map.of(
                OAuth2Properties.TOKEN,
                "static-token",
                OAuth2Properties.TOKEN_EXPIRES_IN_MS,
                "1000",
                OAuth2Properties.TOKEN_REFRESH_ENABLED,
                "false",
                OAuth2Properties.TOKEN_EXCHANGE_ENABLED,
                "true",
                OAuth2Properties.ACCESS_TOKEN_TYPE,
                "subject-token",
                "unrelated",
                "value"));
    assertThat(migrated).isEmpty();
  }

  @Test
  void newPropertiesTakePrecedence() {
    Map<String, String> migrated =
        adapter.migrate(
            Map.of(
                OAuth2Properties.CREDENTIAL, "client1:s3cr3t",
                OAuth2Properties.SCOPE, "legacy",
                OAuth2Config.CLIENT_ID, "client2",
                OAuth2Config.SCOPE, "new"));
    assertThat(migrated)
        .containsExactlyInAnyOrderEntriesOf(
            Map.of(
                OAuth2Config.CLIENT_ID, "client2",
                OAuth2Config.CLIENT_SECRET, "s3cr3t",
                OAuth2Config.SCOPE, "new"));
  }

  @Test
  void resolveMissingTokenEndpoint() {
    Map<String, String> resolved =
        adapter.resolveTokenEndpoint(Map.of(OAuth2Config.CLIENT_ID, "client1"), CATALOG_URI);
    assertThat(resolved)
        .containsExactlyInAnyOrderEntriesOf(
            Map.of(
                OAuth2Config.CLIENT_ID,
                "client1",
                OAuth2Config.TOKEN_ENDPOINT,
                "https://catalog.example.com/api/v1/oauth/tokens"));
  }

  @Test
  void resolveRelativeTokenEndpoint() {
    Map<String, String> resolved =
        adapter.resolveTokenEndpoint(
            Map.of(OAuth2Config.TOKEN_ENDPOINT, "v1/oauth/tokens"), CATALOG_URI);
    assertThat(resolved)
        .containsExactlyEntriesOf(
            Map.of(OAuth2Config.TOKEN_ENDPOINT, "https://catalog.example.com/api/v1/oauth/tokens"));

    resolved =
        adapter.resolveTokenEndpoint(
            Map.of(OAuth2Config.TOKEN_ENDPOINT, "/v1/oauth/tokens"), CATALOG_URI);
    assertThat(resolved)
        .containsExactlyEntriesOf(
            Map.of(OAuth2Config.TOKEN_ENDPOINT, "https://catalog.example.com/api/v1/oauth/tokens"));
  }

  @Test
  void resolveAbsoluteTokenEndpoint() {
    Map<String, String> properties = Map.of(OAuth2Config.TOKEN_ENDPOINT, "https://auth/token");
    assertThat(adapter.resolveTokenEndpoint(properties, CATALOG_URI)).isSameAs(properties);
  }

  @Test
  void resolveWithIssuerUrl() {
    Map<String, String> properties = Map.of(OAuth2Config.ISSUER_URL, "https://auth/realm");
    assertThat(adapter.resolveTokenEndpoint(properties, CATALOG_URI)).isSameAs(properties);
  }

  @Test
  void resolveRequiresCatalogUri() {
    assertThatThrownBy(() -> adapter.resolveTokenEndpoint(Map.of(), null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid catalog URI: null");
  }

  @Test
  void endToEnd() {
    Map<String, String> properties =
        ImmutableMap.of(
            OAuth2Properties.CREDENTIAL, "client1:s3cr3t",
            OAuth2Properties.SCOPE, "catalog",
            OAuth2Properties.AUDIENCE, "catalog-api");
    OAuth2Config config =
        OAuth2Config.fromProperties(
            adapter.resolveTokenEndpoint(adapter.migrate(properties), CATALOG_URI));
    assertThat(config.clientId()).hasToString("client1");
    assertThat(config.clientSecret().getValue()).isEqualTo("s3cr3t");
    assertThat(config.scope()).hasToString("catalog");
    assertThat(config.tokenEndpoint())
        .hasToString("https://catalog.example.com/api/v1/oauth/tokens");
    assertThat(config.audiences()).containsExactly("catalog-api");
    assertThat(config.extraParams()).isEmpty();
  }
}
