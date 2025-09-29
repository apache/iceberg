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

public class TestAdlsTokenCredentialProviders {

  @Test
  public void useDefaultFactory() {
    assertThat(AdlsTokenCredentialProviders.defaultFactory())
        .isNotNull()
        .isInstanceOf(AdlsTokenCredentialProviders.DefaultTokenCredentialProvider.class);
  }

  @Test
  public void emptyPropertiesWithNoProvider() {
    assertThat(AdlsTokenCredentialProviders.from(ImmutableMap.of()))
        .isNotNull()
        .isInstanceOf(AdlsTokenCredentialProviders.DefaultTokenCredentialProvider.class);
  }

  @Test
  public void emptyCredentialProvider() {
    Map<String, String> properties =
        ImmutableMap.of(AzureProperties.ADLS_TOKEN_CREDENTIAL_PROVIDER, "");
    assertThat(AdlsTokenCredentialProviders.from(properties))
        .isNotNull()
        .isInstanceOf(AdlsTokenCredentialProviders.DefaultTokenCredentialProvider.class);
  }

  @Test
  public void defaultProviderAsCredentialProvider() {
    Map<String, String> properties =
        ImmutableMap.of(
            AzureProperties.ADLS_TOKEN_CREDENTIAL_PROVIDER,
            AdlsTokenCredentialProviders.DefaultTokenCredentialProvider.class.getName());
    assertThat(AdlsTokenCredentialProviders.from(properties))
        .isNotNull()
        .isInstanceOf(AdlsTokenCredentialProviders.DefaultTokenCredentialProvider.class);
  }

  @Test
  public void customProviderAsCredentialProvider() {
    Map<String, String> properties =
        ImmutableMap.of(
            AzureProperties.ADLS_TOKEN_CREDENTIAL_PROVIDER,
            TestAdlsTokenCredentialProviders.DummyTokenCredentialProvider.class.getName());
    AdlsTokenCredentialProvider provider = AdlsTokenCredentialProviders.from(properties);

    assertThat(provider).isNotNull().isInstanceOf(DummyTokenCredentialProvider.class);
    assertThat(provider.credential()).isInstanceOf(DummyTokenCredential.class);
  }

  @Test
  public void nonExistentCredentialProvider() {
    Map<String, String> properties =
        ImmutableMap.of(
            AzureProperties.ADLS_TOKEN_CREDENTIAL_PROVIDER,
            "org.apache.iceberg.azure.NonExistentProvider");

    assertThatIllegalArgumentException()
        .isThrownBy(() -> AdlsTokenCredentialProviders.from(properties))
        .withMessageContaining(
            "Cannot initialize AdlsTokenCredentialProvider, missing no-arg constructor");
  }

  @Test
  public void nonImplementingClassAsCredentialProvider() {
    Map<String, String> properties =
        ImmutableMap.of(AzureProperties.ADLS_TOKEN_CREDENTIAL_PROVIDER, "java.lang.String");
    assertThatIllegalArgumentException()
        .isThrownBy(() -> AdlsTokenCredentialProviders.from(properties))
        .withMessageContaining("java.lang.String does not implement AdlsTokenCredentialProvider");
  }

  @Test
  public void loadCredentialProviderWithProperties() {
    Map<String, String> properties =
        ImmutableMap.of(
            AzureProperties.ADLS_TOKEN_CREDENTIAL_PROVIDER,
            TestAdlsTokenCredentialProviders.DummyTokenCredentialProvider.class.getName(),
            AzureProperties.ADLS_TOKEN_PROVIDER_PREFIX + "client-id",
            "clientId",
            AzureProperties.ADLS_TOKEN_PROVIDER_PREFIX + "client-secret",
            "clientSecret",
            "custom.property",
            "custom.value");

    AdlsTokenCredentialProvider provider = AdlsTokenCredentialProviders.from(properties);
    assertThat(provider).isInstanceOf(DummyTokenCredentialProvider.class);
    DummyTokenCredentialProvider credentialProvider = (DummyTokenCredentialProvider) provider;
    assertThat(credentialProvider.properties())
        .containsEntry("client-id", "clientId")
        .containsEntry("client-secret", "clientSecret")
        .doesNotContainKey("custom.property")
        .doesNotContainKey(AzureProperties.ADLS_TOKEN_CREDENTIAL_PROVIDER);
    assertThat(provider.credential()).isInstanceOf(DummyTokenCredential.class);
  }

  static class DummyTokenCredentialProvider implements AdlsTokenCredentialProvider {

    private Map<String, String> properties;

    @Override
    public TokenCredential credential() {
      return new DummyTokenCredential();
    }

    @Override
    public void initialize(Map<String, String> credentialProperties) {
      this.properties = credentialProperties;
    }

    public Map<String, String> properties() {
      return properties;
    }
  }

  static class DummyTokenCredential implements TokenCredential {
    @Override
    public Mono<AccessToken> getToken(TokenRequestContext request) {
      return Mono.just(new AccessToken("dummy-token", OffsetDateTime.now().plusHours(1)));
    }
  }
}
