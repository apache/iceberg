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
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.file.datalake.DataLakePathClientBuilder;
import java.io.Serializable;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;
import reactor.core.publisher.Mono;

public class AzureProperties implements Serializable {
  public static final String ADLS_OAUTH2_TOKEN = "adls.oauth2.token";
  public static final String ADLS_OAUTH2_TOKEN_EXPIRES_AT = "adls.oauth2.token-expires-at";
  public static final String ADLS_READ_BLOCK_SIZE = "adls.read.block-size-bytes";
  public static final String ADLS_WRITE_BLOCK_SIZE = "adls.write.block-size-bytes";

  private String adlsOAuth2Token;
  private OffsetDateTime adlsOAuth2TokenExpiresAt;
  private Integer adlsReadBlockSize;
  private Long adlsWriteBlockSize;

  public AzureProperties() {}

  public AzureProperties(Map<String, String> properties) {
    this.adlsOAuth2Token = properties.get(ADLS_OAUTH2_TOKEN);
    if (properties.containsKey(ADLS_OAUTH2_TOKEN_EXPIRES_AT)) {
      long epochMillis = Long.parseLong(properties.get(ADLS_OAUTH2_TOKEN_EXPIRES_AT));
      this.adlsOAuth2TokenExpiresAt =
          OffsetDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneOffset.UTC);
    }

    if (properties.containsKey(ADLS_READ_BLOCK_SIZE)) {
      this.adlsReadBlockSize = Integer.parseInt(properties.get(ADLS_READ_BLOCK_SIZE));
    }
    if (properties.containsKey(ADLS_WRITE_BLOCK_SIZE)) {
      this.adlsWriteBlockSize = Long.parseLong(properties.get(ADLS_WRITE_BLOCK_SIZE));
    }
  }

  public Optional<Integer> adlsReadBlockSize() {
    return Optional.ofNullable(adlsReadBlockSize);
  }

  public Optional<Long> adlsWriteBlockSize() {
    return Optional.ofNullable(adlsWriteBlockSize);
  }

  public <T extends DataLakePathClientBuilder> void applyCredentialConfiguration(T builder) {
    TokenCredential credential;
    if (adlsOAuth2Token != null && !adlsOAuth2Token.isEmpty() && adlsOAuth2TokenExpiresAt != null) {
      AccessToken accessToken = new AccessToken(adlsOAuth2Token, adlsOAuth2TokenExpiresAt);
      credential = context -> Mono.just(accessToken);
    } else {
      credential = new DefaultAzureCredentialBuilder().build();
    }
    builder.credential(credential);
  }
}
