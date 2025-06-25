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
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.ProxyConfiguration;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;

class NettyHttpAsyncClientConfigurations {
  private Long connectionTimeoutMs;
  private Long socketTimeoutMs;
  private Long acquisitionTimeoutMs;
  private Long connectionMaxIdleTimeMs;
  private Long connectionTimeToLiveMs;
  private Integer maxConnections;
  private Boolean tcpKeepAliveEnabled;
  private Boolean useIdleConnectionReaperEnabled;
  private String proxyEndpoint;

  private NettyHttpAsyncClientConfigurations() {}

  public <T extends S3AsyncClientBuilder> void configureHttpAsyncClientBuilder(
      T s3AsyncClientBuilder) {
    NettyNioAsyncHttpClient.Builder nettyBuilder = NettyNioAsyncHttpClient.builder();
    configureNettyHttpAsyncClientBuilder(nettyBuilder);
    s3AsyncClientBuilder.httpClientBuilder(nettyBuilder);
  }

  private void initialize(Map<String, String> httpAsyncClientProperties) {
    this.connectionTimeoutMs =
        PropertyUtil.propertyAsNullableLong(
            httpAsyncClientProperties, HttpAsyncClientProperties.NETTY_CONNECTION_TIMEOUT_MS);
    this.socketTimeoutMs =
        PropertyUtil.propertyAsNullableLong(
            httpAsyncClientProperties, HttpAsyncClientProperties.NETTY_SOCKET_TIMEOUT_MS);
    this.acquisitionTimeoutMs =
        PropertyUtil.propertyAsNullableLong(
            httpAsyncClientProperties,
            HttpAsyncClientProperties.NETTY_CONNECTION_ACQUISITION_TIMEOUT_MS);
    this.connectionMaxIdleTimeMs =
        PropertyUtil.propertyAsNullableLong(
            httpAsyncClientProperties, HttpAsyncClientProperties.NETTY_CONNECTION_MAX_IDLE_TIME_MS);
    this.connectionTimeToLiveMs =
        PropertyUtil.propertyAsNullableLong(
            httpAsyncClientProperties, HttpAsyncClientProperties.NETTY_CONNECTION_TIME_TO_LIVE_MS);
    this.maxConnections =
        PropertyUtil.propertyAsNullableInt(
            httpAsyncClientProperties, HttpAsyncClientProperties.NETTY_MAX_CONNECTIONS);
    this.tcpKeepAliveEnabled =
        PropertyUtil.propertyAsNullableBoolean(
            httpAsyncClientProperties, HttpAsyncClientProperties.NETTY_TCP_KEEP_ALIVE_ENABLED);
    this.useIdleConnectionReaperEnabled =
        PropertyUtil.propertyAsNullableBoolean(
            httpAsyncClientProperties,
            HttpAsyncClientProperties.NETTY_USE_IDLE_CONNECTION_REAPER_ENABLED);
    this.proxyEndpoint =
        PropertyUtil.propertyAsString(
            httpAsyncClientProperties, HttpAsyncClientProperties.NETTY_PROXY_ENDPOINT, null);
  }

  @VisibleForTesting
  void configureNettyHttpAsyncClientBuilder(NettyNioAsyncHttpClient.Builder nettyBuilder) {
    if (connectionTimeoutMs != null) {
      nettyBuilder.connectionTimeout(Duration.ofMillis(connectionTimeoutMs));
    }
    if (socketTimeoutMs != null) {
      nettyBuilder.readTimeout(Duration.ofMillis(socketTimeoutMs));
      nettyBuilder.writeTimeout(Duration.ofMillis(socketTimeoutMs));
    }
    if (acquisitionTimeoutMs != null) {
      nettyBuilder.connectionAcquisitionTimeout(Duration.ofMillis(acquisitionTimeoutMs));
    }
    if (connectionMaxIdleTimeMs != null) {
      nettyBuilder.connectionMaxIdleTime(Duration.ofMillis(connectionMaxIdleTimeMs));
    }
    if (connectionTimeToLiveMs != null) {
      nettyBuilder.connectionTimeToLive(Duration.ofMillis(connectionTimeToLiveMs));
    }
    if (maxConnections != null) {
      nettyBuilder.maxConcurrency(maxConnections);
    }
    if (tcpKeepAliveEnabled != null) {
      nettyBuilder.tcpKeepAlive(tcpKeepAliveEnabled);
    }
    if (useIdleConnectionReaperEnabled != null) {
      nettyBuilder.useIdleConnectionReaper(useIdleConnectionReaperEnabled);
    }
    if (proxyEndpoint != null) {
      nettyBuilder.proxyConfiguration(
          ProxyConfiguration.builder()
              .host(URI.create(proxyEndpoint).getHost())
              .port(URI.create(proxyEndpoint).getPort())
              .build());
    }
  }

  public static NettyHttpAsyncClientConfigurations create(Map<String, String> properties) {
    NettyHttpAsyncClientConfigurations configurations = new NettyHttpAsyncClientConfigurations();
    configurations.initialize(properties);
    return configurations;
  }
}
