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
package org.apache.iceberg.io.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

class TestHttpUrlHelper {

  @Test
  void connectionConfigIsNullWhenTimeoutsUnset() {
    assertThat(HttpUrlHelper.configureConnectionConfig(ImmutableMap.of())).isNull();
  }

  @Test
  void connectionConfigAppliesBothTimeouts() {
    long connectionTimeoutMs = 1_000L;
    long socketTimeoutMs = 2_000L;
    Map<String, String> properties =
        ImmutableMap.of(
            HttpUrlHelper.CONNECTION_TIMEOUT_MS, String.valueOf(connectionTimeoutMs),
            HttpUrlHelper.SOCKET_TIMEOUT_MS, String.valueOf(socketTimeoutMs));

    ConnectionConfig connectionConfig = HttpUrlHelper.configureConnectionConfig(properties);
    assertThat(connectionConfig).isNotNull();
    assertThat(connectionConfig.getConnectTimeout().toMilliseconds())
        .isEqualTo(connectionTimeoutMs);
    assertThat(connectionConfig.getSocketTimeout().toMilliseconds()).isEqualTo(socketTimeoutMs);
  }

  @Test
  void connectionConfigAppliesOnlyConnectionTimeout() {
    long connectionTimeoutMs = 1_500L;
    ConnectionConfig defaults = ConnectionConfig.custom().build();

    ConnectionConfig connectionConfig =
        HttpUrlHelper.configureConnectionConfig(
            ImmutableMap.of(
                HttpUrlHelper.CONNECTION_TIMEOUT_MS, String.valueOf(connectionTimeoutMs)));
    assertThat(connectionConfig).isNotNull();
    assertThat(connectionConfig.getConnectTimeout().toMilliseconds())
        .isEqualTo(connectionTimeoutMs);
    assertThat(connectionConfig.getSocketTimeout()).isEqualTo(defaults.getSocketTimeout());
  }

  @Test
  void connectionConfigAppliesOnlySocketTimeout() {
    long socketTimeoutMs = 2_500L;
    ConnectionConfig defaults = ConnectionConfig.custom().build();

    ConnectionConfig connectionConfig =
        HttpUrlHelper.configureConnectionConfig(
            ImmutableMap.of(HttpUrlHelper.SOCKET_TIMEOUT_MS, String.valueOf(socketTimeoutMs)));
    assertThat(connectionConfig).isNotNull();
    assertThat(connectionConfig.getSocketTimeout().toMilliseconds()).isEqualTo(socketTimeoutMs);
    assertThat(connectionConfig.getConnectTimeout()).isEqualTo(defaults.getConnectTimeout());
  }

  @Test
  void requestConfigIsNullWhenAcquisitionTimeoutUnset() {
    assertThat(HttpUrlHelper.configureRequestConfig(ImmutableMap.of())).isNull();
  }

  @Test
  void requestConfigAppliesAcquisitionTimeout() {
    long acquisitionTimeoutMs = 3_000L;

    RequestConfig requestConfig =
        HttpUrlHelper.configureRequestConfig(
            ImmutableMap.of(
                HttpUrlHelper.CONNECTION_ACQUISITION_TIMEOUT_MS,
                String.valueOf(acquisitionTimeoutMs)));
    assertThat(requestConfig).isNotNull();
    assertThat(requestConfig.getConnectionRequestTimeout().toMilliseconds())
        .isEqualTo(acquisitionTimeoutMs);
  }

  @Test
  void connectionManagerAppliesConfiguredPoolSizes() {
    int maxConnections = 7;
    int maxConnectionsPerRoute = 3;
    Map<String, String> properties =
        ImmutableMap.of(
            HttpUrlHelper.MAX_CONNECTIONS, String.valueOf(maxConnections),
            HttpUrlHelper.MAX_CONNECTIONS_PER_ROUTE, String.valueOf(maxConnectionsPerRoute));

    PoolingHttpClientConnectionManager pool = poolingManager(properties);
    assertThat(pool.getMaxTotal()).isEqualTo(maxConnections);
    assertThat(pool.getDefaultMaxPerRoute()).isEqualTo(maxConnectionsPerRoute);
  }

  @Test
  void connectionManagerAppliesOnlyMaxConnections() {
    int maxConnections = 9;
    PoolingHttpClientConnectionManager defaults =
        PoolingHttpClientConnectionManagerBuilder.create().build();

    PoolingHttpClientConnectionManager pool =
        poolingManager(
            ImmutableMap.of(HttpUrlHelper.MAX_CONNECTIONS, String.valueOf(maxConnections)));
    assertThat(pool.getMaxTotal()).isEqualTo(maxConnections);
    assertThat(pool.getDefaultMaxPerRoute()).isEqualTo(defaults.getDefaultMaxPerRoute());
  }

  @Test
  void connectionManagerFallsThroughToApacheDefaultsWhenUnset() {
    PoolingHttpClientConnectionManager defaults =
        PoolingHttpClientConnectionManagerBuilder.create().build();

    PoolingHttpClientConnectionManager pool = poolingManager(ImmutableMap.of());
    assertThat(pool.getMaxTotal()).isEqualTo(defaults.getMaxTotal());
    assertThat(pool.getDefaultMaxPerRoute()).isEqualTo(defaults.getDefaultMaxPerRoute());
  }

  @Test
  void readChunkSizeUsesGenericDefaultWhenUnspecified() {
    assertThat(new HttpUrlHelper(ImmutableMap.of()).readChunkSize())
        .isEqualTo(HttpUrlHelper.READ_CHUNK_SIZE_BYTES_DEFAULT);
  }

  @Test
  void readChunkSizeUsesCallerDefaultWhenPropertyUnset() {
    int callerDefault = 8 * 1024 * 1024;
    assertThat(new HttpUrlHelper(ImmutableMap.of(), callerDefault).readChunkSize())
        .isEqualTo(callerDefault);
  }

  @Test
  void readChunkSizePropertyOverridesCallerDefault() {
    int override = 1024;
    HttpUrlHelper support =
        new HttpUrlHelper(
            ImmutableMap.of(HttpUrlHelper.READ_CHUNK_SIZE_BYTES, String.valueOf(override)),
            8 * 1024 * 1024);
    assertThat(support.readChunkSize()).isEqualTo(override);
  }

  @Test
  void nonPositiveReadChunkSizeIsRejected() {
    assertThatThrownBy(
            () -> new HttpUrlHelper(ImmutableMap.of(HttpUrlHelper.READ_CHUNK_SIZE_BYTES, "0")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(HttpUrlHelper.READ_CHUNK_SIZE_BYTES);
  }

  @Test
  void redactStripsQueryUserInfoAndFragment() {
    assertThat(
            HttpUrlHelper.redact(
                "https://user:pass@bucket.s3.amazonaws.com:443/key/f.parquet?X-Amz-Signature=abc#frag"))
        .isEqualTo("https://bucket.s3.amazonaws.com:443/key/f.parquet");
  }

  @Test
  void redactKeepsSchemeHostAndPathForHttpAndHttps() {
    assertThat(HttpUrlHelper.redact("https://bucket.s3.amazonaws.com/key"))
        .isEqualTo("https://bucket.s3.amazonaws.com/key");
    assertThat(HttpUrlHelper.redact("http://127.0.0.1:9000/bucket/key"))
        .isEqualTo("http://127.0.0.1:9000/bucket/key");
  }

  @Test
  void redactFailsClosedForMalformedOrHostlessUrlsAndNull() {
    assertThat(HttpUrlHelper.redact("not a url")).isEqualTo("<redacted>");
    assertThat(HttpUrlHelper.redact("mailto:someone@example.com")).isEqualTo("<redacted>");
    assertThat(HttpUrlHelper.redact(null)).isEqualTo("null");
  }

  private static PoolingHttpClientConnectionManager poolingManager(Map<String, String> properties) {
    HttpClientConnectionManager connectionManager =
        HttpUrlHelper.configureConnectionManager(properties);
    assertThat(connectionManager).isInstanceOf(PoolingHttpClientConnectionManager.class);
    return (PoolingHttpClientConnectionManager) connectionManager;
  }
}
