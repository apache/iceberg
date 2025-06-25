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

import java.time.Duration;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.ProxyConfiguration;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;
import software.amazon.awssdk.services.s3.crt.S3CrtHttpConfiguration;

public class TestHttpAsyncClientConfigurations {

  @Test
  public void testNettyOverrideConfigurations() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(HttpAsyncClientProperties.NETTY_CONNECTION_TIMEOUT_MS, "200");
    properties.put(HttpAsyncClientProperties.NETTY_SOCKET_TIMEOUT_MS, "100");
    properties.put(HttpAsyncClientProperties.NETTY_CONNECTION_ACQUISITION_TIMEOUT_MS, "101");
    properties.put(HttpAsyncClientProperties.NETTY_CONNECTION_MAX_IDLE_TIME_MS, "102");
    properties.put(HttpAsyncClientProperties.NETTY_CONNECTION_TIME_TO_LIVE_MS, "103");
    properties.put(HttpAsyncClientProperties.NETTY_MAX_CONNECTIONS, "104");
    properties.put(HttpAsyncClientProperties.NETTY_TCP_KEEP_ALIVE_ENABLED, "true");
    properties.put(HttpAsyncClientProperties.NETTY_USE_IDLE_CONNECTION_REAPER_ENABLED, "false");
    properties.put(HttpAsyncClientProperties.NETTY_PROXY_ENDPOINT, "http://proxy:8080");

    NettyHttpAsyncClientConfigurations nettyConfigurations =
        NettyHttpAsyncClientConfigurations.create(properties);
    NettyNioAsyncHttpClient.Builder nettyBuilder = NettyNioAsyncHttpClient.builder();
    NettyNioAsyncHttpClient.Builder spyNettyBuilder = Mockito.spy(nettyBuilder);

    nettyConfigurations.configureNettyHttpAsyncClientBuilder(spyNettyBuilder);

    Mockito.verify(spyNettyBuilder).connectionTimeout(Duration.ofMillis(200));
    Mockito.verify(spyNettyBuilder).readTimeout(Duration.ofMillis(100));
    Mockito.verify(spyNettyBuilder).writeTimeout(Duration.ofMillis(100));
    Mockito.verify(spyNettyBuilder).connectionAcquisitionTimeout(Duration.ofMillis(101));
    Mockito.verify(spyNettyBuilder).connectionMaxIdleTime(Duration.ofMillis(102));
    Mockito.verify(spyNettyBuilder).connectionTimeToLive(Duration.ofMillis(103));
    Mockito.verify(spyNettyBuilder).maxConcurrency(104);
    Mockito.verify(spyNettyBuilder).tcpKeepAlive(true);
    Mockito.verify(spyNettyBuilder).useIdleConnectionReaper(false);
    Mockito.verify(spyNettyBuilder).proxyConfiguration(Mockito.any(ProxyConfiguration.class));
  }

  @Test
  public void testNettyDefaultConfigurations() {
    NettyHttpAsyncClientConfigurations nettyConfigurations =
        NettyHttpAsyncClientConfigurations.create(Maps.newHashMap());
    NettyNioAsyncHttpClient.Builder nettyBuilder = NettyNioAsyncHttpClient.builder();
    NettyNioAsyncHttpClient.Builder spyNettyBuilder = Mockito.spy(nettyBuilder);

    nettyConfigurations.configureNettyHttpAsyncClientBuilder(spyNettyBuilder);

    Mockito.verify(spyNettyBuilder, Mockito.never()).connectionTimeout(Mockito.any(Duration.class));
    Mockito.verify(spyNettyBuilder, Mockito.never()).readTimeout(Mockito.any(Duration.class));
    Mockito.verify(spyNettyBuilder, Mockito.never()).writeTimeout(Mockito.any(Duration.class));
    Mockito.verify(spyNettyBuilder, Mockito.never())
        .connectionAcquisitionTimeout(Mockito.any(Duration.class));
    Mockito.verify(spyNettyBuilder, Mockito.never())
        .connectionMaxIdleTime(Mockito.any(Duration.class));
    Mockito.verify(spyNettyBuilder, Mockito.never())
        .connectionTimeToLive(Mockito.any(Duration.class));
    Mockito.verify(spyNettyBuilder, Mockito.never()).maxConcurrency(Mockito.anyInt());
    Mockito.verify(spyNettyBuilder, Mockito.never()).tcpKeepAlive(Mockito.anyBoolean());
    Mockito.verify(spyNettyBuilder, Mockito.never()).useIdleConnectionReaper(Mockito.anyBoolean());
    Mockito.verify(spyNettyBuilder, Mockito.never())
        .proxyConfiguration(Mockito.any(ProxyConfiguration.class));
  }

  @Test
  public void testCrtOverrideConfigurations() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(HttpAsyncClientProperties.CRT_CONNECTION_TIMEOUT_MS, "200");
    properties.put(HttpAsyncClientProperties.CRT_MAX_CONCURRENCY, "50");

    CrtHttpAsyncClientConfigurations crtConfigurations =
        CrtHttpAsyncClientConfigurations.create(properties);
    S3CrtAsyncClientBuilder crtBuilder = Mockito.mock(S3CrtAsyncClientBuilder.class);

    crtConfigurations.configureCrtHttpAsyncClientBuilder(crtBuilder);

    Mockito.verify(crtBuilder).httpConfiguration(Mockito.any(S3CrtHttpConfiguration.class));
    Mockito.verify(crtBuilder).maxConcurrency(50);
  }

  @Test
  public void testCrtDefaultConfigurations() {
    CrtHttpAsyncClientConfigurations crtConfigurations =
        CrtHttpAsyncClientConfigurations.create(Maps.newHashMap());
    S3CrtAsyncClientBuilder crtBuilder = Mockito.mock(S3CrtAsyncClientBuilder.class);

    crtConfigurations.configureCrtHttpAsyncClientBuilder(crtBuilder);

    Mockito.verify(crtBuilder, Mockito.never())
        .httpConfiguration(Mockito.any(S3CrtHttpConfiguration.class));
    Mockito.verify(crtBuilder)
        .maxConcurrency(HttpAsyncClientProperties.CRT_MAX_CONCURRENCY_DEFAULT);
  }
}
