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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.google.auth.oauth2.GoogleCredentials;
import java.util.Map;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class TestGcsTokenCredentialProviders {

  @Test
  public void useDefaultFactory() {
    assertThat(GcsTokenCredentialProviders.defaultFactory())
        .isNotNull()
        .isInstanceOf(GcsTokenCredentialProviders.DefaultGcsTokenCredentialProvider.class);
  }

  @Test
  public void emptyPropertiesWithNoProvider() {
    assertThat(GcsTokenCredentialProviders.from(ImmutableMap.of()))
        .isNotNull()
        .isInstanceOf(GcsTokenCredentialProviders.DefaultGcsTokenCredentialProvider.class);
  }

  @Test
  public void emptyCredentialProvider() {
    Map<String, String> properties =
        ImmutableMap.of(GCPProperties.GCS_TOKEN_CREDENTIAL_PROVIDER, "");
    assertThat(GcsTokenCredentialProviders.from(properties))
        .isNotNull()
        .isInstanceOf(GcsTokenCredentialProviders.DefaultGcsTokenCredentialProvider.class);
  }

  @Test
  public void defaultProviderAsCredentialProvider() {
    Map<String, String> properties =
        ImmutableMap.of(
            GCPProperties.GCS_TOKEN_CREDENTIAL_PROVIDER,
            GcsTokenCredentialProviders.DefaultGcsTokenCredentialProvider.class.getName());
    assertThat(GcsTokenCredentialProviders.from(properties))
        .isNotNull()
        .isInstanceOf(GcsTokenCredentialProviders.DefaultGcsTokenCredentialProvider.class);
  }

  @Test
  public void customProviderAsCredentialProvider() {
    Map<String, String> properties =
        ImmutableMap.of(
            GCPProperties.GCS_TOKEN_CREDENTIAL_PROVIDER,
            DummyGcsTokenCredentialProvider.class.getName());
    GcsTokenCredentialProvider provider = GcsTokenCredentialProviders.from(properties);

    assertThat(provider).isNotNull().isInstanceOf(DummyGcsTokenCredentialProvider.class);
    assertThat(provider.credential()).isNull();
  }

  @Test
  public void nonExistentCredentialProvider() {
    Map<String, String> properties =
        ImmutableMap.of(
            GCPProperties.GCS_TOKEN_CREDENTIAL_PROVIDER,
            "org.apache.iceberg.gcp.gcs.NonExistentProvider");

    assertThatIllegalArgumentException()
        .isThrownBy(() -> GcsTokenCredentialProviders.from(properties))
        .withMessageContaining(
            "Cannot initialize GcsTokenCredentialProvider, cannot load class or missing no-arg constructor");
  }

  @Test
  public void nonImplementingClassAsCredentialProvider() {
    Map<String, String> properties =
        ImmutableMap.of(GCPProperties.GCS_TOKEN_CREDENTIAL_PROVIDER, "java.lang.String");
    assertThatIllegalArgumentException()
        .isThrownBy(() -> GcsTokenCredentialProviders.from(properties))
        .withMessageContaining("java.lang.String does not implement GcsTokenCredentialProvider");
  }

  @Test
  public void loadCredentialProviderWithProperties() {
    Map<String, String> properties =
        ImmutableMap.of(
            GCPProperties.GCS_TOKEN_CREDENTIAL_PROVIDER,
            DummyGcsTokenCredentialProvider.class.getName(),
            GCPProperties.GCS_TOKEN_PROVIDER_PREFIX + "service-account-key",
            "keyValue",
            "custom.property",
            "custom.value");

    GcsTokenCredentialProvider provider = GcsTokenCredentialProviders.from(properties);
    assertThat(provider).isInstanceOf(DummyGcsTokenCredentialProvider.class);
    DummyGcsTokenCredentialProvider credentialProvider = (DummyGcsTokenCredentialProvider) provider;
    assertThat(credentialProvider.properties())
        .containsEntry("service-account-key", "keyValue")
        .doesNotContainKey("custom.property")
        .doesNotContainKey(GCPProperties.GCS_TOKEN_CREDENTIAL_PROVIDER);
  }

  static class DummyGcsTokenCredentialProvider implements GcsTokenCredentialProvider {

    private Map<String, String> properties;

    @Override
    public GoogleCredentials credential() {
      return null;
    }

    @Override
    public void initialize(Map<String, String> credentialProperties) {
      this.properties = credentialProperties;
    }

    public Map<String, String> properties() {
      return properties;
    }
  }
}
