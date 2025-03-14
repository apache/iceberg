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
package org.apache.iceberg.azure.adlsv2;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.SimpleTokenCache;
import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.ErrorHandlers;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.HTTPHeaders;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.DefaultAuthSession;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.apache.iceberg.rest.credentials.Credential;
import org.apache.iceberg.rest.responses.LoadCredentialsResponse;
import org.apache.iceberg.util.SerializableMap;
import reactor.core.publisher.Mono;

public class VendedAdlsCredentialProvider implements Serializable, AutoCloseable {

  public static final String URI = "credentials.uri";

  private final SerializableMap<String, String> properties;
  private transient volatile Map<String, SimpleTokenCache> sasCredentialByAccount;
  private transient volatile RESTClient client;

  public VendedAdlsCredentialProvider(Map<String, String> properties) {
    Preconditions.checkArgument(null != properties, "Invalid properties: null");
    Preconditions.checkArgument(null != properties.get(URI), "Invalid URI: null");
    this.properties = SerializableMap.copyOf(properties);
    azureSasCredentialMap = Maps.newHashMap();
  }

  public String credentialForAccount(String storageAccount) {
    Map<String, SimpleTokenCache> tokenCacheForAccountMap = azureSasCredentialMap();
    if (tokenCacheForAccountMap.containsKey(storageAccount)) {
      return tokenFromCache(tokenCacheForAccountMap.get(storageAccount));
    } else {
      SimpleTokenCache tokenCache =
          new SimpleTokenCache(() -> Mono.fromSupplier(() -> sasTokenSupplier(storageAccount)));
      tokenCacheForAccountMap.put(storageAccount, tokenCache);
      return tokenFromCache(tokenCache);
    }
  }

  private String tokenFromCache(SimpleTokenCache simpleTokenCache) {
    return simpleTokenCache.getToken().map(AccessToken::getToken).block();
  }

  private AccessToken sasTokenSupplier(String storageAccount) {
    LoadCredentialsResponse response = fetchCredentials();
    List<Credential> adlsCredentials =
        response.credentials().stream()
            .filter(c -> c.prefix().contains(storageAccount))
            .collect(Collectors.toList());
    Preconditions.checkState(
        !adlsCredentials.isEmpty(),
        String.format("Invalid ADLS Credentials for storage-account %s: empty", storageAccount));
    Preconditions.checkState(
        adlsCredentials.size() == 1,
        "Invalid ADLS Credentials: only one ADLS credential should exist per storage-account");

    Credential adlsCredential = adlsCredentials.get(0);
    checkCredential(adlsCredential, AzureProperties.ADLS_SAS_TOKEN_PREFIX + storageAccount);
    checkCredential(
        adlsCredential, AzureProperties.ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX + storageAccount);

    String sasToken =
        adlsCredential.config().get(AzureProperties.ADLS_SAS_TOKEN_PREFIX + storageAccount);
    Long tokenExpiresAtMillis =
        Long.parseLong(
            adlsCredential
                .config()
                .get(AzureProperties.ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX + storageAccount));

    return new AccessToken(
        updatedSasToken, Instant.ofEpochMilli(tokenExpiresAtMillis).atOffset(ZoneOffset.UTC));
  }

  private Map<String, SimpleTokenCache> sasCredentialByAccount() {
    if (this.azureSasCredentialMap == null) {
      synchronized (this) {
        if (this.azureSasCredentialMap == null) {
          this.azureSasCredentialMap = Maps.newHashMap();
        }
      }
    }
    return this.azureSasCredentialMap;
  }

  private RESTClient httpClient() {
    if (null == client) {
      synchronized (this) {
        if (null == client) {
          DefaultAuthSession authSession =
              DefaultAuthSession.of(
                  HTTPHeaders.of(OAuth2Util.authHeaders(properties.get(OAuth2Properties.TOKEN))));
          client =
              HTTPClient.builder(properties)
                  .uri(properties.get(URI))
                  .withAuthSession(authSession)
                  .build();
        }
      }
    }

    return client;
  }

  private LoadCredentialsResponse fetchCredentials() {
    return httpClient()
        .get(
            properties.get(URI),
            null,
            LoadCredentialsResponse.class,
            Map.of(),
            ErrorHandlers.defaultErrorHandler());
  }

  private void checkCredential(Credential credential, String property) {
    Preconditions.checkState(
        credential.config().containsKey(property),
        "Invalid ADLS Credentials: %s not set",
        property);
  }

  @Override
  public void close() {
    IOUtils.closeQuietly(client);
  }
}
