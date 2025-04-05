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
package org.apache.iceberg.gcp.gcs;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.OAuth2CredentialsWithRefresh;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.ErrorHandlers;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthManagers;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.credentials.Credential;
import org.apache.iceberg.rest.responses.LoadCredentialsResponse;

public class OAuth2RefreshCredentialsHandler
    implements OAuth2CredentialsWithRefresh.OAuth2RefreshHandler, AutoCloseable {
  private final Map<String, String> properties;
  private final String credentialsEndpoint;
  // will be used to refresh the OAuth2 token
  private final String catalogEndpoint;
  private volatile HTTPClient client;
  private AuthManager authManager;
  private AuthSession authSession;

  private OAuth2RefreshCredentialsHandler(Map<String, String> properties) {
    Preconditions.checkArgument(
        null != properties.get(GCPProperties.GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT),
        "Invalid credentials endpoint: null");
    Preconditions.checkArgument(
        null != properties.get(CatalogProperties.URI), "Invalid catalog endpoint: null");
    this.credentialsEndpoint =
        properties.get(GCPProperties.GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT);
    this.catalogEndpoint = properties.get(CatalogProperties.URI);
    this.properties = properties;
  }

  @SuppressWarnings("JavaUtilDate") // GCP API uses java.util.Date
  @Override
  public AccessToken refreshAccessToken() {
    LoadCredentialsResponse response =
        httpClient()
            .get(
                credentialsEndpoint,
                null,
                LoadCredentialsResponse.class,
                Map.of(),
                ErrorHandlers.defaultErrorHandler());

    List<Credential> gcsCredentials =
        response.credentials().stream()
            .filter(c -> c.prefix().startsWith("gs"))
            .collect(Collectors.toList());

    Preconditions.checkState(!gcsCredentials.isEmpty(), "Invalid GCS Credentials: empty");
    Preconditions.checkState(
        gcsCredentials.size() == 1,
        "Invalid GCS Credentials: only one GCS credential should exist");

    Credential gcsCredential = gcsCredentials.get(0);
    checkCredential(gcsCredential, GCPProperties.GCS_OAUTH2_TOKEN);
    checkCredential(gcsCredential, GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT);
    String token = gcsCredential.config().get(GCPProperties.GCS_OAUTH2_TOKEN);
    String expiresAt = gcsCredential.config().get(GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT);

    return new AccessToken(token, new Date(Long.parseLong(expiresAt)));
  }

  private void checkCredential(Credential gcsCredential, String gcsOauth2Token) {
    Preconditions.checkState(
        gcsCredential.config().containsKey(gcsOauth2Token),
        "Invalid GCS Credentials: %s not set",
        gcsOauth2Token);
  }

  public static OAuth2RefreshCredentialsHandler create(Map<String, String> properties) {
    return new OAuth2RefreshCredentialsHandler(properties);
  }

  private RESTClient httpClient() {
    if (null == client) {
      synchronized (this) {
        if (null == client) {
          authManager = AuthManagers.loadAuthManager("gcs-credentials-refresh", properties);
          HTTPClient httpClient = HTTPClient.builder(properties).uri(catalogEndpoint).build();
          authSession = authManager.catalogSession(httpClient, properties);
          client = httpClient.withAuthSession(authSession);
        }
      }
    }

    return client;
  }

  @Override
  public void close() {
    CloseableGroup closeableGroup = new CloseableGroup();
    closeableGroup.addCloseable(authSession);
    closeableGroup.addCloseable(authManager);
    closeableGroup.addCloseable(client);
    closeableGroup.setSuppressCloseFailure(true);
    try {
      closeableGroup.close();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close the OAuth2RefreshCredentialsHandler", e);
    }
  }
}
