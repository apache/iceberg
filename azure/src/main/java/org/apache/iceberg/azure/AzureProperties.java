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
package org.apache.iceberg.azure;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeFileSystemClientBuilder;
import java.io.Serializable;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.azure.adlsv2.VendedAdlsCredentialProvider;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SerializableMap;
import reactor.core.publisher.Mono;

public class AzureProperties implements Serializable {
  public static final String ADLS_SAS_TOKEN_PREFIX = "adls.sas-token.";
  public static final String ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX = "adls.sas-token-expires-at-ms.";
  public static final String ADLS_CONNECTION_STRING_PREFIX = "adls.connection-string.";
  public static final String ADLS_READ_BLOCK_SIZE = "adls.read.block-size-bytes";
  public static final String ADLS_WRITE_BLOCK_SIZE = "adls.write.block-size-bytes";
  public static final String ADLS_SHARED_KEY_ACCOUNT_NAME = "adls.auth.shared-key.account.name";
  public static final String ADLS_SHARED_KEY_ACCOUNT_KEY = "adls.auth.shared-key.account.key";
  public static final String ADLS_TOKEN = "adls.token";

  /**
   * When set, the {@link VendedAdlsCredentialProvider} will be used to fetch and refresh vended
   * credentials from this endpoint.
   */
  public static final String ADLS_REFRESH_CREDENTIALS_ENDPOINT =
      "adls.refresh-credentials-endpoint";

  /** Controls whether vended credentials should be refreshed or not. Defaults to true. */
  public static final String ADLS_REFRESH_CREDENTIALS_ENABLED = "adls.refresh-credentials-enabled";

  private Map<String, String> adlsSasTokens = Collections.emptyMap();
  private Map<String, String> adlsConnectionStrings = Collections.emptyMap();
  private Map.Entry<String, String> namedKeyCreds;
  private Integer adlsReadBlockSize;
  private Long adlsWriteBlockSize;
  private String adlsRefreshCredentialsEndpoint;
  private boolean adlsRefreshCredentialsEnabled;
  private String token;
  private Map<String, String> allProperties;

  public AzureProperties() {}

  public AzureProperties(Map<String, String> properties) {
    this.adlsSasTokens = PropertyUtil.propertiesWithPrefix(properties, ADLS_SAS_TOKEN_PREFIX);
    this.adlsConnectionStrings =
        PropertyUtil.propertiesWithPrefix(properties, ADLS_CONNECTION_STRING_PREFIX);

    String sharedKeyAccountName = properties.get(ADLS_SHARED_KEY_ACCOUNT_NAME);
    String sharedKeyAccountKey = properties.get(ADLS_SHARED_KEY_ACCOUNT_KEY);
    if (sharedKeyAccountName != null || sharedKeyAccountKey != null) {
      Preconditions.checkArgument(
          sharedKeyAccountName != null && sharedKeyAccountKey != null,
          "Azure authentication: shared-key requires both %s and %s",
          ADLS_SHARED_KEY_ACCOUNT_NAME,
          ADLS_SHARED_KEY_ACCOUNT_KEY);
      this.namedKeyCreds = Maps.immutableEntry(sharedKeyAccountName, sharedKeyAccountKey);
    }

    if (properties.containsKey(ADLS_READ_BLOCK_SIZE)) {
      this.adlsReadBlockSize = Integer.parseInt(properties.get(ADLS_READ_BLOCK_SIZE));
    }
    if (properties.containsKey(ADLS_WRITE_BLOCK_SIZE)) {
      this.adlsWriteBlockSize = Long.parseLong(properties.get(ADLS_WRITE_BLOCK_SIZE));
    }
    this.adlsRefreshCredentialsEndpoint =
        RESTUtil.resolveEndpoint(
            properties.get(CatalogProperties.URI),
            properties.get(ADLS_REFRESH_CREDENTIALS_ENDPOINT));
    this.adlsRefreshCredentialsEnabled =
        PropertyUtil.propertyAsBoolean(properties, ADLS_REFRESH_CREDENTIALS_ENABLED, true);
    this.token = properties.get(ADLS_TOKEN);
    this.allProperties = SerializableMap.copyOf(properties);
  }

  public Optional<Integer> adlsReadBlockSize() {
    return Optional.ofNullable(adlsReadBlockSize);
  }

  public Optional<Long> adlsWriteBlockSize() {
    return Optional.ofNullable(adlsWriteBlockSize);
  }

  public Optional<VendedAdlsCredentialProvider> vendedAdlsCredentialProvider() {
    if (adlsRefreshCredentialsEnabled && !Strings.isNullOrEmpty(adlsRefreshCredentialsEndpoint)) {
      Map<String, String> credentialProviderProperties = Maps.newHashMap(allProperties);
      credentialProviderProperties.put(
          VendedAdlsCredentialProvider.URI, adlsRefreshCredentialsEndpoint);
      return Optional.of(new VendedAdlsCredentialProvider(credentialProviderProperties));
    } else {
      return Optional.empty();
    }
  }

  /**
   * Applies configuration to the {@link DataLakeFileSystemClientBuilder} to provide the endpoint
   * and credentials required to create an instance of the client.
   *
   * <p>Default credentials are provided via the {@link com.azure.identity.DefaultAzureCredential}.
   *
   * @param account the service account key (e.g. a hostname or storage account key to get values)
   * @param builder the builder instance
   */
  public void applyClientConfiguration(String account, DataLakeFileSystemClientBuilder builder) {
    if (!adlsRefreshCredentialsEnabled || Strings.isNullOrEmpty(adlsRefreshCredentialsEndpoint)) {
      String sasToken = adlsSasTokens.get(account);
      if (sasToken != null && !sasToken.isEmpty()) {
        builder.sasToken(sasToken);
      } else if (namedKeyCreds != null) {
        builder.credential(
            new StorageSharedKeyCredential(namedKeyCreds.getKey(), namedKeyCreds.getValue()));
      } else if (token != null && !token.isEmpty()) {
        // Use TokenCredential with the provided token
        TokenCredential tokenCredential =
            new TokenCredential() {
              @Override
              public Mono<AccessToken> getToken(TokenRequestContext request) {
                // Assume the token is valid for 1 hour from the current time
                return Mono.just(
                    new AccessToken(token, OffsetDateTime.now(ZoneOffset.UTC).plusHours(1)));
              }
            };
        builder.credential(tokenCredential);
      } else {
        builder.credential(new DefaultAzureCredentialBuilder().build());
      }
    }

    // apply connection string last so its parameters take precedence, e.g. SAS token
    String connectionString = adlsConnectionStrings.get(account);
    if (connectionString != null && !connectionString.isEmpty()) {
      builder.endpoint(connectionString);
    } else {
      builder.endpoint("https://" + account);
    }
  }
}
