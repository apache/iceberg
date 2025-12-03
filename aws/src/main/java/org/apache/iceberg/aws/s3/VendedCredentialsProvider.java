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
package org.apache.iceberg.aws.s3;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.rest.ErrorHandlers;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthManagers;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.credentials.Credential;
import org.apache.iceberg.rest.responses.LoadCredentialsResponse;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.utils.IoUtils;
import software.amazon.awssdk.utils.SdkAutoCloseable;
import software.amazon.awssdk.utils.cache.CachedSupplier;
import software.amazon.awssdk.utils.cache.RefreshResult;

public class VendedCredentialsProvider implements AwsCredentialsProvider, SdkAutoCloseable {
  public static final String URI = "credentials.uri";
  private volatile HTTPClient client;
  private final Map<String, String> properties;
  private final CachedSupplier<AwsCredentials> credentialCache;
  private final String catalogEndpoint;
  private final String credentialsEndpoint;
  private AuthManager authManager;
  private AuthSession authSession;

  private VendedCredentialsProvider(Map<String, String> properties) {
    Preconditions.checkArgument(null != properties, "Invalid properties: null");
    Preconditions.checkArgument(null != properties.get(URI), "Invalid credentials endpoint: null");
    Preconditions.checkArgument(
        null != properties.get(CatalogProperties.URI), "Invalid catalog endpoint: null");
    this.properties = properties;
    this.credentialCache =
        CachedSupplier.builder(() -> credentialFromProperties().orElseGet(this::refreshCredential))
            .cachedValueName(VendedCredentialsProvider.class.getName())
            .build();
    this.catalogEndpoint = properties.get(CatalogProperties.URI);
    this.credentialsEndpoint = properties.get(URI);
  }

  @Override
  public AwsCredentials resolveCredentials() {
    return credentialCache.get();
  }

  @Override
  public void close() {
    IoUtils.closeQuietlyV2(authSession, null);
    IoUtils.closeQuietlyV2(authManager, null);
    IoUtils.closeQuietlyV2(client, null);
    IoUtils.closeQuietlyV2(credentialCache, null);
  }

  public static VendedCredentialsProvider create(Map<String, String> properties) {
    return new VendedCredentialsProvider(properties);
  }

  private RESTClient httpClient() {
    if (null == client) {
      synchronized (this) {
        if (null == client) {
          authManager = AuthManagers.loadAuthManager("s3-credentials-refresh", properties);
          HTTPClient httpClient = HTTPClient.builder(properties).uri(catalogEndpoint).build();
          authSession = authManager.catalogSession(httpClient, properties);
          client = httpClient.withAuthSession(authSession);
        }
      }
    }

    return client;
  }

  private LoadCredentialsResponse fetchCredentials() {
    return httpClient()
        .get(
            credentialsEndpoint,
            null,
            LoadCredentialsResponse.class,
            Map.of(),
            ErrorHandlers.defaultErrorHandler());
  }

  private Optional<RefreshResult<AwsCredentials>> credentialFromProperties() {
    String accessKeyId = properties.get(S3FileIOProperties.ACCESS_KEY_ID);
    String secretAccessKey = properties.get(S3FileIOProperties.SECRET_ACCESS_KEY);
    String sessionToken = properties.get(S3FileIOProperties.SESSION_TOKEN);
    String tokenExpiresAtMillis = properties.get(S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS);
    if (Strings.isNullOrEmpty(accessKeyId)
        || Strings.isNullOrEmpty(secretAccessKey)
        || Strings.isNullOrEmpty(sessionToken)
        || Strings.isNullOrEmpty(tokenExpiresAtMillis)) {
      return Optional.empty();
    }

    Instant expiresAt = Instant.ofEpochMilli(Long.parseLong(tokenExpiresAtMillis));
    Instant prefetchAt = expiresAt.minus(5, ChronoUnit.MINUTES);

    if (Instant.now().isAfter(prefetchAt)) {
      return Optional.empty();
    }

    return Optional.of(
        RefreshResult.builder(
                (AwsCredentials)
                    AwsSessionCredentials.builder()
                        .accessKeyId(accessKeyId)
                        .secretAccessKey(secretAccessKey)
                        .sessionToken(sessionToken)
                        .expirationTime(expiresAt)
                        .build())
            .staleTime(expiresAt)
            .prefetchTime(prefetchAt)
            .build());
  }

  private RefreshResult<AwsCredentials> refreshCredential() {
    LoadCredentialsResponse response = fetchCredentials();

    List<Credential> s3Credentials =
        response.credentials().stream()
            .filter(c -> c.prefix().startsWith("s3"))
            .collect(Collectors.toList());

    Preconditions.checkState(!s3Credentials.isEmpty(), "Invalid S3 Credentials: empty");
    Preconditions.checkState(
        s3Credentials.size() == 1, "Invalid S3 Credentials: only one S3 credential should exist");

    Credential s3Credential = s3Credentials.get(0);
    checkCredential(s3Credential, S3FileIOProperties.ACCESS_KEY_ID);
    checkCredential(s3Credential, S3FileIOProperties.SECRET_ACCESS_KEY);
    checkCredential(s3Credential, S3FileIOProperties.SESSION_TOKEN);
    checkCredential(s3Credential, S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS);

    String accessKeyId = s3Credential.config().get(S3FileIOProperties.ACCESS_KEY_ID);
    String secretAccessKey = s3Credential.config().get(S3FileIOProperties.SECRET_ACCESS_KEY);
    String sessionToken = s3Credential.config().get(S3FileIOProperties.SESSION_TOKEN);
    String tokenExpiresAtMillis =
        s3Credential.config().get(S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS);
    Instant expiresAt = Instant.ofEpochMilli(Long.parseLong(tokenExpiresAtMillis));
    Instant prefetchAt = expiresAt.minus(5, ChronoUnit.MINUTES);

    return RefreshResult.builder(
            (AwsCredentials)
                AwsSessionCredentials.builder()
                    .accessKeyId(accessKeyId)
                    .secretAccessKey(secretAccessKey)
                    .sessionToken(sessionToken)
                    .expirationTime(expiresAt)
                    .build())
        .staleTime(expiresAt)
        .prefetchTime(prefetchAt)
        .build();
  }

  private void checkCredential(Credential credential, String property) {
    Preconditions.checkState(
        credential.config().containsKey(property), "Invalid S3 Credentials: %s not set", property);
  }
}
