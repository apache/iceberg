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

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.gcs.analyticscore.client.GcsClientOptions;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystem;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemOptions;
import com.google.cloud.gcs.analyticscore.client.GcsReadOptions;
import com.google.cloud.gcs.analyticscore.client.GcsWriteOptions;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
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
  public void httpTimeoutsNotSetByDefault() {
    Map<String, String> properties = ImmutableMap.of(GCPProperties.GCS_PROJECT_ID, "myProject");
    PrefixedStorage storage = new PrefixedStorage("gs://bucket", properties, null);

    assertThat(storage.storage().getOptions().getTransportOptions())
        .isInstanceOf(HttpTransportOptions.class);
    HttpTransportOptions transportOptions =
        (HttpTransportOptions) storage.storage().getOptions().getTransportOptions();
    assertThat(transportOptions.getConnectTimeout())
        .isEqualTo(HttpTransportOptions.newBuilder().build().getConnectTimeout());
    assertThat(transportOptions.getReadTimeout())
        .isEqualTo(HttpTransportOptions.newBuilder().build().getReadTimeout());
  }

  @Test
  public void httpTimeoutsAreWired() {
    Map<String, String> properties =
        ImmutableMap.of(
            GCPProperties.GCS_PROJECT_ID, "myProject",
            GCPProperties.GCS_HTTP_CONNECT_TIMEOUT, "5000",
            GCPProperties.GCS_HTTP_READ_TIMEOUT, "10000");
    PrefixedStorage storage = new PrefixedStorage("gs://bucket", properties, null);

    HttpTransportOptions transportOptions =
        (HttpTransportOptions) storage.storage().getOptions().getTransportOptions();
    assertThat(transportOptions.getConnectTimeout()).isEqualTo(5000);
    assertThat(transportOptions.getReadTimeout()).isEqualTo(10000);
  }

  @Test
  public void readTimeoutIsActuallyEnforced() throws IOException {
    // Proves the configured timeout changes real request behavior, not just that the value is
    // stored: the default read timeout is effectively unbounded (java.net.URLConnection blocks
    // forever on read by default), so a server that accepts the connection and then never
    // responds would hang indefinitely without this feature. With gcs.http.read-timeout-ms set,
    // the request must fail quickly instead.
    //
    // The Storage client retries failed requests with backoff by default (observed: ~51s wall
    // time for the unmodified client below, matching gax's default total retry timeout), which
    // would swamp a single request's timeout and make this test both slow and a poor signal.
    // maxAttempts(1) isolates the behavior of one HTTP attempt, which is what the configured
    // timeout actually governs.
    try (ServerSocket serverSocket = new ServerSocket(0)) {
      int port = serverSocket.getLocalPort();
      Thread unresponsiveServer =
          new Thread(
              () -> {
                try {
                  serverSocket.accept();
                  // Accept the TCP connection but never write an HTTP response.
                  Thread.sleep(30_000);
                } catch (Exception e) {
                  // Expected once the client gives up and closes the connection.
                }
              });
      unresponsiveServer.setDaemon(true);
      unresponsiveServer.start();

      Map<String, String> properties =
          ImmutableMap.of(
              GCPProperties.GCS_PROJECT_ID, "myProject",
              GCPProperties.GCS_NO_AUTH, "true",
              GCPProperties.GCS_SERVICE_HOST, "http://localhost:" + port,
              GCPProperties.GCS_HTTP_CONNECT_TIMEOUT, "2000",
              GCPProperties.GCS_HTTP_READ_TIMEOUT, "500");
      PrefixedStorage storage = new PrefixedStorage("gs://bucket", properties, null);
      Storage singleAttemptClient =
          storage.storage().getOptions().toBuilder()
              .setRetrySettings(RetrySettings.newBuilder().setMaxAttempts(1).build())
              .build()
              .getService();

      long start = System.nanoTime();
      // The message confirms this is genuinely a read timeout (SocketTimeoutException), not a
      // connect failure or some other error that happened to also be fast.
      assertThatThrownBy(() -> singleAttemptClient.get(BlobId.of("bucket", "object")))
          .isInstanceOf(StorageException.class)
          .hasMessage("Read timed out")
          .hasCauseInstanceOf(SocketTimeoutException.class);
      long elapsedMs = (System.nanoTime() - start) / 1_000_000;

      // Generous bound well below what an unbounded default read would take (the server never
      // responds), confirming the configured 500ms timeout is what actually triggered failure.
      assertThat(elapsedMs).isLessThan(10_000);
    }
  }

  @Test
  public void impersonationPropertiesAreRead() {
    Map<String, String> properties =
        ImmutableMap.of(
            GCPProperties.GCS_PROJECT_ID, "myProject",
            GCPProperties.GCS_IMPERSONATE_SERVICE_ACCOUNT,
                "test-sa@project.iam.gserviceaccount.com",
            GCPProperties.GCS_IMPERSONATE_DELEGATES, "delegate-sa@project.iam.gserviceaccount.com",
            GCPProperties.GCS_IMPERSONATE_LIFETIME_SECONDS, "1800",
            GCPProperties.GCS_IMPERSONATE_SCOPES, "bigquery,devstorage.read_only");

    GCPProperties gcpProperties = new GCPProperties(properties);

    assertThat(gcpProperties.impersonateServiceAccount())
        .contains("test-sa@project.iam.gserviceaccount.com");
    assertThat(gcpProperties.impersonateDelegates())
        .contains("delegate-sa@project.iam.gserviceaccount.com");
    assertThat(gcpProperties.impersonateLifetimeSeconds()).isEqualTo(1800);
    assertThat(gcpProperties.impersonateScopes())
        .containsExactly(
            "https://www.googleapis.com/auth/bigquery",
            "https://www.googleapis.com/auth/devstorage.read_only");
  }

  @Test
  public void impersonationPropertiesWithDefaults() {
    Map<String, String> properties =
        ImmutableMap.of(
            GCPProperties.GCS_PROJECT_ID, "myProject",
            GCPProperties.GCS_IMPERSONATE_SERVICE_ACCOUNT,
                "test-sa@project.iam.gserviceaccount.com");

    GCPProperties gcpProperties = new GCPProperties(properties);

    assertThat(gcpProperties.impersonateServiceAccount())
        .contains("test-sa@project.iam.gserviceaccount.com");
    assertThat(gcpProperties.impersonateDelegates()).isNull();
    assertThat(gcpProperties.impersonateLifetimeSeconds())
        .isEqualTo(GCPProperties.GCS_IMPERSONATE_LIFETIME_SECONDS_DEFAULT);
  }

  @Test
  public void gcsFileSystemDisabledByDefault() {
    Map<String, String> properties = ImmutableMap.of(GCPProperties.GCS_PROJECT_ID, "myProject");
    PrefixedStorage storage = new PrefixedStorage("gs://bucket", properties, null);

    assertThat(storage.gcsFileSystem()).isNull();
  }

  @Test
  public void gcsFileSystem() {
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put(GCPProperties.GCS_ANALYTICS_CORE_ENABLED, "true")
            .put(GCPProperties.GCS_PROJECT_ID, "myProject")
            .put(GCPProperties.GCS_USER_PROJECT, "userProject")
            .put(GCPProperties.GCS_CLIENT_LIB_TOKEN, "gccl")
            .put(GCPProperties.GCS_SERVICE_HOST, "example.com")
            .put(GCPProperties.GCS_DECRYPTION_KEY, "decryptionKey")
            .put(GCPProperties.GCS_ENCRYPTION_KEY, "encryptionKey")
            .put(GCPProperties.GCS_CHANNEL_READ_CHUNK_SIZE, "1024")
            .build();
    PrefixedStorage storage = new PrefixedStorage("gs://bucket", properties, null);
    GcsFileSystemOptions expectedOptions =
        GcsFileSystemOptions.builder()
            .setGcsClientOptions(
                GcsClientOptions.builder()
                    .setProjectId("myProject")
                    .setClientLibToken("gccl")
                    .setServiceHost("example.com")
                    .setUserAgent("gcsfileio/" + EnvironmentContext.get())
                    .setGcsReadOptions(
                        GcsReadOptions.builder()
                            .setChunkSize(1024)
                            .setDecryptionKey("decryptionKey")
                            .setUserProjectId("userProject")
                            .build())
                    .setGcsWriteOptions(
                        GcsWriteOptions.builder()
                            .setEncryptionKey("encryptionKey")
                            .setUserProject("userProject")
                            .build())
                    .build())
            .build();

    GcsFileSystem fileSystem = (GcsFileSystem) storage.gcsFileSystem();

    assertThat(fileSystem).isNotNull();
    assertThat(fileSystem.getGcsClient()).isNotNull();
    assertThat(fileSystem.getFileSystemOptions()).isEqualTo(expectedOptions);
  }
}
