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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import java.time.OffsetDateTime;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

public class TestAzureTokenCredentialProviders {

  @Test
  public void testDefaultFactory() {
    AzureTokenCredentialProvider provider = AzureTokenCredentialProviders.defaultFactory();
    assertThat(provider).isNotNull();
    assertThat(provider)
        .isInstanceOf(AzureTokenCredentialProviders.DefaultTokenCredentialProvider.class);
  }

  @Test
  public void testFromWithNoProvider() {
    Map<String, String> properties = ImmutableMap.of();
    AzureTokenCredentialProvider provider = AzureTokenCredentialProviders.from(properties);

    assertThat(provider).isNotNull();
    assertThat(provider)
        .isInstanceOf(AzureTokenCredentialProviders.DefaultTokenCredentialProvider.class);
  }

  @Test
  public void testFromWithNullProvider() {
    Map<String, String> properties =
        ImmutableMap.of(AzureProperties.ADLS_TOKEN_CREDENTIAL_PROVIDER, "");
    AzureTokenCredentialProvider provider = AzureTokenCredentialProviders.from(properties);
    assertThat(provider).isNotNull();
    assertThat(provider)
        .isInstanceOf(AzureTokenCredentialProviders.DefaultTokenCredentialProvider.class);
  }

  @Test
  public void testFromWithDefaultProvider() {
    Map<String, String> properties =
        ImmutableMap.of(
            AzureProperties.ADLS_TOKEN_CREDENTIAL_PROVIDER,
            "org.apache.iceberg.azure.AzureTokenCredentialProviders$DefaultTokenCredentialProvider");
    AzureTokenCredentialProvider provider = AzureTokenCredentialProviders.from(properties);
    assertThat(provider).isNotNull();
    assertThat(provider)
        .isInstanceOf(AzureTokenCredentialProviders.DefaultTokenCredentialProvider.class);
  }

  @Test
  public void testFromWithDummyProvider() {
    Map<String, String> properties =
        ImmutableMap.of(
            AzureProperties.ADLS_TOKEN_CREDENTIAL_PROVIDER,
            "org.apache.iceberg.azure.TestAzureTokenCredentialProviders$DummyTokenCredentialProvider");
    AzureTokenCredentialProvider provider = AzureTokenCredentialProviders.from(properties);

    assertThat(provider).isNotNull();
    assertThat(provider).isInstanceOf(DummyTokenCredentialProvider.class);
    assertThat(provider.credential()).isInstanceOf(DummyTokenCredential.class);
  }

  @Test
  public void testFromWithInvalidProvider() {
    Map<String, String> properties =
        ImmutableMap.of(
            AzureProperties.ADLS_TOKEN_CREDENTIAL_PROVIDER,
            "org.apache.iceberg.azure.NonExistentProvider");

    assertThatIllegalArgumentException()
        .isThrownBy(() -> AzureTokenCredentialProviders.from(properties))
        .withMessageContaining(
            "Cannot initialize AzureTokenCredentialProvider, missing no-arg constructor");
  }

  @Test
  public void testFromWithNonImplementingClass() {
    Map<String, String> properties =
        ImmutableMap.of(AzureProperties.ADLS_TOKEN_CREDENTIAL_PROVIDER, "java.lang.String");
    assertThatIllegalArgumentException()
        .isThrownBy(() -> AzureTokenCredentialProviders.from(properties))
        .withMessageContaining("java.lang.String does not implement AzureTokenCredentialProvider");
  }

  @Test
  public void testLoadCredentialProviderWithProperties() {
    Map<String, String> properties =
        ImmutableMap.of(
            AzureProperties.ADLS_TOKEN_CREDENTIAL_PROVIDER,
            "org.apache.iceberg.azure.TestAzureTokenCredentialProviders$DummyTokenCredentialProvider",
            "custom.property",
            "custom.value");

    AzureTokenCredentialProvider provider = AzureTokenCredentialProviders.from(properties);
    assertThat(provider).isInstanceOf(DummyTokenCredentialProvider.class);
    assertThat(provider.credential()).isInstanceOf(DummyTokenCredential.class);
  }

  // Dummy implementation for testing
  static class DummyTokenCredentialProvider implements AzureTokenCredentialProvider {

    @Override
    public TokenCredential credential() {
      return new DummyTokenCredential();
    }

    @Override
    public void initialize(Map<String, String> properties) {}
  }

  // Dummy TokenCredential for testing
  static class DummyTokenCredential implements TokenCredential {
    @Override
    public Mono<AccessToken> getToken(TokenRequestContext request) {
      return Mono.just(new AccessToken("dummy-token", OffsetDateTime.now().plusHours(1)));
    }
  }
}
