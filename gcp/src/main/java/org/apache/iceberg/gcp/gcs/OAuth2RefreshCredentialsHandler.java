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
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.ErrorHandlers;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.HTTPHeaders;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.auth.DefaultAuthSession;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.apache.iceberg.rest.credentials.Credential;
import org.apache.iceberg.rest.responses.LoadCredentialsResponse;

public class OAuth2RefreshCredentialsHandler
    implements OAuth2CredentialsWithRefresh.OAuth2RefreshHandler {
  private final Map<String, String> properties;
  private final AuthSession authSession;

  private OAuth2RefreshCredentialsHandler(Map<String, String> properties) {
    Preconditions.checkArgument(
        null != properties.get(GCPProperties.GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT),
        "Invalid credentials endpoint: null");
    this.properties = properties;
    this.authSession =
        DefaultAuthSession.of(
            HTTPHeaders.of(OAuth2Util.authHeaders(properties.get(OAuth2Properties.TOKEN))));
  }

  @SuppressWarnings("JavaUtilDate") // GCP API uses java.util.Date
  @Override
  public AccessToken refreshAccessToken() {
    LoadCredentialsResponse response;
    try (RESTClient client = httpClient()) {
      response =
          client.get(
              properties.get(GCPProperties.GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT),
              null,
              LoadCredentialsResponse.class,
              Map.of(),
              ErrorHandlers.defaultErrorHandler());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

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
    return HTTPClient.builder(properties)
        .uri(properties.get(GCPProperties.GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT))
        .withAuthSession(authSession)
        .build();
  }
}
