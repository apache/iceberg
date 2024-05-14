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
package org.apache.iceberg.aws;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.util.PropertyUtil;
import software.amazon.awssdk.awscore.client.builder.AwsSyncClientBuilder;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;

class ApacheHttpClientConfigurations {
  private Long connectionTimeoutMs;
  private Long socketTimeoutMs;
  private Long acquisitionTimeoutMs;
  private Long connectionMaxIdleTimeMs;
  private Long connectionTimeToLiveMs;
  private Boolean expectContinueEnabled;
  private Integer maxConnections;
  private Boolean tcpKeepAliveEnabled;
  private Boolean useIdleConnectionReaperEnabled;
  private String proxyEndpoint;

  private ApacheHttpClientConfigurations() {}

  public <T extends AwsSyncClientBuilder> void configureHttpClientBuilder(T awsClientBuilder) {
    ApacheHttpClient.Builder apacheHttpClientBuilder = ApacheHttpClient.builder();
    configureApacheHttpClientBuilder(apacheHttpClientBuilder);
    awsClientBuilder.httpClientBuilder(apacheHttpClientBuilder);
  }

  private void initialize(Map<String, String> httpClientProperties) {
    this.connectionTimeoutMs =
        PropertyUtil.propertyAsNullableLong(
            httpClientProperties, HttpClientProperties.APACHE_CONNECTION_TIMEOUT_MS);
    this.socketTimeoutMs =
        PropertyUtil.propertyAsNullableLong(
            httpClientProperties, HttpClientProperties.APACHE_SOCKET_TIMEOUT_MS);
    this.acquisitionTimeoutMs =
        PropertyUtil.propertyAsNullableLong(
            httpClientProperties, HttpClientProperties.APACHE_CONNECTION_ACQUISITION_TIMEOUT_MS);
    this.connectionMaxIdleTimeMs =
        PropertyUtil.propertyAsNullableLong(
            httpClientProperties, HttpClientProperties.APACHE_CONNECTION_MAX_IDLE_TIME_MS);
    this.connectionTimeToLiveMs =
        PropertyUtil.propertyAsNullableLong(
            httpClientProperties, HttpClientProperties.APACHE_CONNECTION_TIME_TO_LIVE_MS);
    this.expectContinueEnabled =
        PropertyUtil.propertyAsNullableBoolean(
            httpClientProperties, HttpClientProperties.APACHE_EXPECT_CONTINUE_ENABLED);
    this.maxConnections =
        PropertyUtil.propertyAsNullableInt(
            httpClientProperties, HttpClientProperties.APACHE_MAX_CONNECTIONS);
    this.tcpKeepAliveEnabled =
        PropertyUtil.propertyAsNullableBoolean(
            httpClientProperties, HttpClientProperties.APACHE_TCP_KEEP_ALIVE_ENABLED);
    this.useIdleConnectionReaperEnabled =
        PropertyUtil.propertyAsNullableBoolean(
            httpClientProperties, HttpClientProperties.APACHE_USE_IDLE_CONNECTION_REAPER_ENABLED);
    this.proxyEndpoint =
        PropertyUtil.propertyAsString(
            httpClientProperties, HttpClientProperties.PROXY_ENDPOINT, null);
  }

  @VisibleForTesting
  void configureApacheHttpClientBuilder(ApacheHttpClient.Builder apacheHttpClientBuilder) {
    if (connectionTimeoutMs != null) {
      apacheHttpClientBuilder.connectionTimeout(Duration.ofMillis(connectionTimeoutMs));
    }
    if (socketTimeoutMs != null) {
      apacheHttpClientBuilder.socketTimeout(Duration.ofMillis(socketTimeoutMs));
    }
    if (acquisitionTimeoutMs != null) {
      apacheHttpClientBuilder.connectionAcquisitionTimeout(Duration.ofMillis(acquisitionTimeoutMs));
    }
    if (connectionMaxIdleTimeMs != null) {
      apacheHttpClientBuilder.connectionMaxIdleTime(Duration.ofMillis(connectionMaxIdleTimeMs));
    }
    if (connectionTimeToLiveMs != null) {
      apacheHttpClientBuilder.connectionTimeToLive(Duration.ofMillis(connectionTimeToLiveMs));
    }
    if (expectContinueEnabled != null) {
      apacheHttpClientBuilder.expectContinueEnabled(expectContinueEnabled);
    }
    if (maxConnections != null) {
      apacheHttpClientBuilder.maxConnections(maxConnections);
    }
    if (tcpKeepAliveEnabled != null) {
      apacheHttpClientBuilder.tcpKeepAlive(tcpKeepAliveEnabled);
    }
    if (useIdleConnectionReaperEnabled != null) {
      apacheHttpClientBuilder.useIdleConnectionReaper(useIdleConnectionReaperEnabled);
    }
    if (proxyEndpoint != null) {
      apacheHttpClientBuilder.proxyConfiguration(
          ProxyConfiguration.builder().endpoint(URI.create(proxyEndpoint)).build());
    }
  }

  public static ApacheHttpClientConfigurations create(Map<String, String> properties) {
    ApacheHttpClientConfigurations configurations = new ApacheHttpClientConfigurations();
    configurations.initialize(properties);
    return configurations;
  }
}
