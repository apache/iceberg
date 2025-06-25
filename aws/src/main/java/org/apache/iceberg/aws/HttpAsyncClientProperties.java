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

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.util.PropertyUtil;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;

/**
 * Configuration properties for HTTP async clients used by AWS services.
 *
 * <p>The client type (Netty vs CRT) is determined by the {@code s3.crt-enabled} property in
 * S3FileIOProperties. When CRT is enabled, CRT-specific properties are used; when disabled,
 * Netty-specific properties are used.
 */
public class HttpAsyncClientProperties implements Serializable {

  private static final String CLIENT_PREFIX = "http-async-client.";

  /**
   * Used to configure the connection timeout in milliseconds for {@link
   * software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient.Builder}. This flag only works
   * when {@code s3.crt-enabled} is {@code false}
   *
   * <p>For more details, see
   * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/nio/netty/NettyNioAsyncHttpClient.Builder.html
   */
  public static final String NETTY_CONNECTION_TIMEOUT_MS =
      "http-async-client.netty.connection-timeout-ms";

  /**
   * Used to configure the socket timeout in milliseconds for {@link
   * software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient.Builder}. This flag only works
   * when {@code s3.crt-enabled} is {@code false}
   *
   * <p>For more details, see
   * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/nio/netty/NettyNioAsyncHttpClient.Builder.html
   */
  public static final String NETTY_SOCKET_TIMEOUT_MS = "http-async-client.netty.socket-timeout-ms";

  /**
   * Used to configure the connection acquisition timeout in milliseconds for {@link
   * software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient.Builder}. This flag only works
   * when {@code s3.crt-enabled} is {@code false}
   *
   * <p>For more details, see
   * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/nio/netty/NettyNioAsyncHttpClient.Builder.html
   */
  public static final String NETTY_CONNECTION_ACQUISITION_TIMEOUT_MS =
      "http-async-client.netty.connection-acquisition-timeout-ms";

  /**
   * Used to configure the connection max idle time in milliseconds for {@link
   * software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient.Builder}. This flag only works
   * when {@code s3.crt-enabled} is {@code false}
   *
   * <p>For more details, see
   * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/nio/netty/NettyNioAsyncHttpClient.Builder.html
   */
  public static final String NETTY_CONNECTION_MAX_IDLE_TIME_MS =
      "http-async-client.netty.connection-max-idle-time-ms";

  /**
   * Used to configure the connection time to live in milliseconds for {@link
   * software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient.Builder}. This flag only works
   * when {@code s3.crt-enabled} is {@code false}
   *
   * <p>For more details, see
   * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/nio/netty/NettyNioAsyncHttpClient.Builder.html
   */
  public static final String NETTY_CONNECTION_TIME_TO_LIVE_MS =
      "http-async-client.netty.connection-time-to-live-ms";

  /**
   * Used to configure the max connections number for {@link
   * software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient.Builder}. This flag only works
   * when {@code s3.crt-enabled} is {@code false}
   *
   * <p>For more details, see
   * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/nio/netty/NettyNioAsyncHttpClient.Builder.html
   */
  public static final String NETTY_MAX_CONNECTIONS = "http-async-client.netty.max-connections";

  /**
   * Used to configure whether to enable the tcp keep alive setting for {@link
   * software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient.Builder}. This flag only works
   * when {@code s3.crt-enabled} is {@code false}.
   *
   * <p>In default, this is disabled.
   *
   * <p>For more details, see
   * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/nio/netty/NettyNioAsyncHttpClient.Builder.html
   */
  public static final String NETTY_TCP_KEEP_ALIVE_ENABLED =
      "http-async-client.netty.tcp-keep-alive-enabled";

  /**
   * Used to configure whether to use idle connection reaper for {@link
   * software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient.Builder}. This flag only works
   * when {@code s3.crt-enabled} is {@code false}.
   *
   * <p>In default, this is enabled.
   *
   * <p>For more details, see
   * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/nio/netty/NettyNioAsyncHttpClient.Builder.html
   */
  public static final String NETTY_USE_IDLE_CONNECTION_REAPER_ENABLED =
      "http-async-client.netty.use-idle-connection-reaper-enabled";

  /**
   * Used to configure the proxy endpoint for {@link
   * software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient.Builder}. This flag only works
   * when {@code s3.crt-enabled} is {@code false}
   *
   * <p>For more details, see
   * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/nio/netty/NettyNioAsyncHttpClient.Builder.html
   */
  public static final String NETTY_PROXY_ENDPOINT = "http-async-client.netty.proxy-endpoint";

  /**
   * Used to configure the connection timeout in milliseconds for {@link
   * software.amazon.awssdk.services.s3.internal.crt.S3CrtAsyncHttpClient.Builder}. This flag only
   * works when {@code s3.crt-enabled} is {@code true}
   *
   * <p>For more details, see
   * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/s3/internal/crt/S3CrtAsyncHttpClient.Builder.html
   */
  public static final String CRT_CONNECTION_TIMEOUT_MS =
      "http-async-client.crt.connection-timeout-ms";

  /**
   * Used to configure the max concurrency for {@link
   * software.amazon.awssdk.services.s3.internal.crt.S3CrtAsyncHttpClient.Builder}. This flag only
   * works when {@code s3.crt-enabled} is {@code true}
   *
   * <p>For more details, see
   * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/s3/internal/crt/S3CrtAsyncHttpClient.Builder.html
   */
  public static final String CRT_MAX_CONCURRENCY = "http-async-client.crt.max-concurrency";

  /**
   * To fully benefit from the analytics-accelerator-s3 library where this S3 CRT client is used, it
   * is recommended to initialize with higher concurrency.
   *
   * <p>For more details, see: https://github.com/awslabs/analytics-accelerator-s3
   */
  public static final int CRT_MAX_CONCURRENCY_DEFAULT = 500;

  private final Map<String, String> httpAsyncClientProperties;

  public HttpAsyncClientProperties() {
    this.httpAsyncClientProperties = Collections.emptyMap();
  }

  public HttpAsyncClientProperties(Map<String, String> properties) {
    this.httpAsyncClientProperties =
        PropertyUtil.filterProperties(properties, key -> key.startsWith(CLIENT_PREFIX));
  }

  public <T extends S3AsyncClientBuilder> void applyHttpAsyncClientConfigurations(T builder) {
    NettyHttpAsyncClientConfigurations nettyConfigurations =
        loadHttpAsyncClientConfigurations(NettyHttpAsyncClientConfigurations.class.getName());
    nettyConfigurations.configureHttpAsyncClientBuilder(builder);
  }

  public <T extends S3CrtAsyncClientBuilder> void applyHttpAsyncClientConfigurations(T builder) {
    CrtHttpAsyncClientConfigurations crtConfigurations =
        loadHttpAsyncClientConfigurations(CrtHttpAsyncClientConfigurations.class.getName());
    crtConfigurations.configureHttpAsyncClientBuilder(builder);
  }

  private <T> T loadHttpAsyncClientConfigurations(String impl) {
    try {
      Object httpAsyncClientConfigurations =
          DynMethods.builder("create")
              .hiddenImpl(impl, Map.class)
              .buildStaticChecked()
              .invoke(httpAsyncClientProperties);
      return (T) httpAsyncClientConfigurations;
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format("Cannot create %s to generate and configure the http client builder", impl),
          e);
    }
  }
}
