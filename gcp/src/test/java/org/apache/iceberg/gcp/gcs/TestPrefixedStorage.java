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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Date;
import java.util.Map;
import org.apache.iceberg.EnvironmentContext;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

@SuppressWarnings("resource")
public class TestPrefixedStorage {

  @Test
  public void invalidParameters() {
    assertThatThrownBy(() -> new PrefixedStorage(null, null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid storage prefix: null or empty");

    assertThatThrownBy(() -> new PrefixedStorage("", null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid storage prefix: null or empty");

    assertThatThrownBy(() -> new PrefixedStorage("gs://bucket", null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid properties: null");
  }

  @Test
  public void validParameters() {
    Map<String, String> properties =
        ImmutableMap.of(
            GCPProperties.GCS_PROJECT_ID, "myProject", GCPProperties.GCS_OAUTH2_TOKEN, "token");
    PrefixedStorage storage = new PrefixedStorage("gs://bucket", properties, null);

    assertThat(storage.storage()).isNotNull();
    assertThat(storage.storagePrefix()).isEqualTo("gs://bucket");
    assertThat(storage.gcpProperties().properties()).isEqualTo(properties);
  }

  @Test
  public void userAgentPrefix() {
    Map<String, String> properties =
        ImmutableMap.of(
            GCPProperties.GCS_PROJECT_ID, "myProject",
            GCPProperties.GCS_OAUTH2_TOKEN, "token",
            GCPProperties.GCS_USER_PROJECT, "myUserProject");
    PrefixedStorage storage = new PrefixedStorage("gs://bucket", properties, null);

    assertThat(storage.storage().getOptions().getUserAgent())
        .isEqualTo("gcsfileio/" + EnvironmentContext.get());
  }

  @Test
  public void noAuthPropertiesAreRead() {
    Map<String, String> properties =
        ImmutableMap.of(
            GCPProperties.GCS_PROJECT_ID, "myProject",
            GCPProperties.GCS_NO_AUTH, "true");

    PrefixedStorage storage = new PrefixedStorage("gs://bucket", properties, null);

    assertThat(storage.gcpProperties().noAuth()).isTrue();
    assertThat(storage.storage()).isNotNull();
    assertThat(storage.storage().getOptions().getCredentials().toString())
        .contains("NoCredentials");
  }

  @Test
  public void oauth2TokenPropertiesAreRead() {
    long expiresAt = System.currentTimeMillis() + 3600000;
    Map<String, String> properties =
        ImmutableMap.of(
            GCPProperties.GCS_PROJECT_ID, "myProject",
            GCPProperties.GCS_OAUTH2_TOKEN, "test-token",
            GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT, String.valueOf(expiresAt));

    PrefixedStorage storage = new PrefixedStorage("gs://bucket", properties, null);

    assertThat(storage.gcpProperties().oauth2Token()).contains("test-token");
    assertThat(storage.gcpProperties().oauth2TokenExpiresAt())
        .isPresent()
        .get()
        .extracting(Date::getTime)
        .isEqualTo(expiresAt);
  }

  @Test
  public void impersonationPropertiesAreRead() {
    Map<String, String> properties =
        ImmutableMap.of(
            GCPProperties.GCS_PROJECT_ID, "myProject",
            GCPProperties.GCS_IMPERSONATE_SERVICE_ACCOUNT,
                "test-sa@project.iam.gserviceaccount.com",
            GCPProperties.GCS_IMPERSONATE_DELEGATES, "delegate-sa@project.iam.gserviceaccount.com",
            GCPProperties.GCS_IMPERSONATE_LIFETIME_SECONDS, "1800");

    PrefixedStorage storage = new PrefixedStorage("gs://bucket", properties, null);

    assertThat(storage.gcpProperties().impersonateServiceAccount())
        .contains("test-sa@project.iam.gserviceaccount.com");
    assertThat(storage.gcpProperties().impersonateDelegates())
        .contains("delegate-sa@project.iam.gserviceaccount.com");
    assertThat(storage.gcpProperties().impersonateLifetimeSeconds()).isEqualTo(1800);
  }

  @Test
  public void impersonationPropertiesWithDefaults() {
    Map<String, String> properties =
        ImmutableMap.of(
            GCPProperties.GCS_PROJECT_ID, "myProject",
            GCPProperties.GCS_IMPERSONATE_SERVICE_ACCOUNT,
                "test-sa@project.iam.gserviceaccount.com");

    PrefixedStorage storage = new PrefixedStorage("gs://bucket", properties, null);

    assertThat(storage.gcpProperties().impersonateServiceAccount())
        .contains("test-sa@project.iam.gserviceaccount.com");
    assertThat(storage.gcpProperties().impersonateDelegates()).isEmpty();
    assertThat(storage.gcpProperties().impersonateLifetimeSeconds())
        .isEqualTo(GCPProperties.GCS_IMPERSONATE_LIFETIME_SECONDS_DEFAULT);
  }
}
