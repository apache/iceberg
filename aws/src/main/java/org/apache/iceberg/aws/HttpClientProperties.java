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
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.util.PropertyUtil;
import software.amazon.awssdk.awscore.client.builder.AwsAsyncClientBuilder;
import software.amazon.awssdk.awscore.client.builder.AwsSyncClientBuilder;

public class HttpClientProperties implements Serializable {

  /**
   * The type of {@link software.amazon.awssdk.http.SdkHttpClient} implementation used by {@link
   * AwsClientFactory} If set, all AWS clients will use this specified HTTP client. If not set,
   * {@link #CLIENT_TYPE_DEFAULT} will be used. For specific types supported, see CLIENT_TYPE_*
   * defined below.
   */
  public static final String CLIENT_TYPE = "http-client.type";

  /**
   * If this is set under {@link #CLIENT_TYPE}, {@link
   * software.amazon.awssdk.http.apache.ApacheHttpClient} will be used as the HTTP Client in {@link
   * AwsClientFactory}
   */
  public static final String CLIENT_TYPE_APACHE = "apache";

  private static final String CLIENT_PREFIX = "http-client.";
  /**
   * If this is set under {@link #CLIENT_TYPE}, {@link
   * software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient} will be used as the HTTP
   * Client in {@link AwsClientFactory}
   */
  public static final String CLIENT_TYPE_URLCONNECTION = "urlconnection";

  public static final String CLIENT_TYPE_DEFAULT = CLIENT_TYPE_APACHE;
  /**
   * Used to configure the connection timeout in milliseconds for {@link
   * software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient.Builder}. This flag only
   * works when {@link #CLIENT_TYPE} is set to {@link #CLIENT_TYPE_URLCONNECTION}
   *
   * <p>For more details, see
   * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/urlconnection/UrlConnectionHttpClient.Builder.html
   */
  public static final String URLCONNECTION_CONNECTION_TIMEOUT_MS =
      "http-client.urlconnection.connection-timeout-ms";
  /**
   * Used to configure the socket timeout in milliseconds for {@link
   * software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient.Builder}. This flag only
   * works when {@link #CLIENT_TYPE} is set to {@link #CLIENT_TYPE_URLCONNECTION}
   *
   * <p>For more details, see
   * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/urlconnection/UrlConnectionHttpClient.Builder.html
   */
  public static final String URLCONNECTION_SOCKET_TIMEOUT_MS =
      "http-client.urlconnection.socket-timeout-ms";
  /**
   * Used to configure the connection timeout in milliseconds for {@link
   * software.amazon.awssdk.http.apache.ApacheHttpClient.Builder}. This flag only works when {@link
   * #CLIENT_TYPE} is set to {@link #CLIENT_TYPE_APACHE}
   *
   * <p>For more details, see
   * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/apache/ApacheHttpClient.Builder.html
   */
  public static final String APACHE_CONNECTION_TIMEOUT_MS =
      "http-client.apache.connection-timeout-ms";
  /**
   * Used to configure the socket timeout in milliseconds for {@link
   * software.amazon.awssdk.http.apache.ApacheHttpClient.Builder}. This flag only works when {@link
   * #CLIENT_TYPE} is set to {@link #CLIENT_TYPE_APACHE}
   *
   * <p>For more details, see
   * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/apache/ApacheHttpClient.Builder.html
   */
  public static final String APACHE_SOCKET_TIMEOUT_MS = "http-client.apache.socket-timeout-ms";
  /**
   * Used to configure the connection acquisition timeout in milliseconds for {@link
   * software.amazon.awssdk.http.apache.ApacheHttpClient.Builder}. This flag only works when {@link
   * #CLIENT_TYPE} is set to {@link #CLIENT_TYPE_APACHE}
   *
   * <p>For more details, see
   * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/apache/ApacheHttpClient.Builder.html
   */
  public static final String APACHE_CONNECTION_ACQUISITION_TIMEOUT_MS =
      "http-client.apache.connection-acquisition-timeout-ms";
  /**
   * Used to configure the connection max idle time in milliseconds for {@link
   * software.amazon.awssdk.http.apache.ApacheHttpClient.Builder}. This flag only works when {@link
   * #CLIENT_TYPE} is set to {@link #CLIENT_TYPE_APACHE}
   *
   * <p>For more details, see
   * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/apache/ApacheHttpClient.Builder.html
   */
  public static final String APACHE_CONNECTION_MAX_IDLE_TIME_MS =
      "http-client.apache.connection-max-idle-time-ms";
  /**
   * Used to configure the connection time to live in milliseconds for {@link
   * software.amazon.awssdk.http.apache.ApacheHttpClient.Builder}. This flag only works when {@link
   * #CLIENT_TYPE} is set to {@link #CLIENT_TYPE_APACHE}
   *
   * <p>For more details, see
   * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/apache/ApacheHttpClient.Builder.html
   */
  public static final String APACHE_CONNECTION_TIME_TO_LIVE_MS =
      "http-client.apache.connection-time-to-live-ms";
  /**
   * Used to configure whether to enable the expect continue setting for {@link
   * software.amazon.awssdk.http.apache.ApacheHttpClient.Builder}. This flag only works when {@link
   * #CLIENT_TYPE} is set to {@link #CLIENT_TYPE_APACHE}
   *
   * <p>In default, this is disabled.
   *
   * <p>For more details, see
   * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/apache/ApacheHttpClient.Builder.html
   */
  public static final String APACHE_EXPECT_CONTINUE_ENABLED =
      "http-client.apache.expect-continue-enabled";
  /**
   * Used to configure the max connections number for {@link
   * software.amazon.awssdk.http.apache.ApacheHttpClient.Builder}. This flag only works when {@link
   * #CLIENT_TYPE} is set to {@link #CLIENT_TYPE_APACHE}
   *
   * <p>For more details, see
   * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/apache/ApacheHttpClient.Builder.html
   */
  public static final String APACHE_MAX_CONNECTIONS = "http-client.apache.max-connections";
  /**
   * Used to configure whether to enable the tcp keep alive setting for {@link
   * software.amazon.awssdk.http.apache.ApacheHttpClient.Builder}. This flag only works when {@link
   * #CLIENT_TYPE} is set to {@link #CLIENT_TYPE_APACHE}.
   *
   * <p>In default, this is disabled.
   *
   * <p>For more details, see
   * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/apache/ApacheHttpClient.Builder.html
   */
  public static final String APACHE_TCP_KEEP_ALIVE_ENABLED =
      "http-client.apache.tcp-keep-alive-enabled";
  /**
   * Used to configure whether to use idle connection reaper for {@link
   * software.amazon.awssdk.http.apache.ApacheHttpClient.Builder}. This flag only works when {@link
   * #CLIENT_TYPE} is set to {@link #CLIENT_TYPE_APACHE}.
   *
   * <p>In default, this is enabled.
   *
   * <p>For more details, see
   * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/apache/ApacheHttpClient.Builder.html
   */
  public static final String APACHE_USE_IDLE_CONNECTION_REAPER_ENABLED =
      "http-client.apache.use-idle-connection-reaper-enabled";

  /**
   * If this is set under {@link #CLIENT_TYPE}, {@link
   * software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient} will be used as the HTTP Client
   */
  public static final String HTTP_CLIENT_TYPE_NETTYNIO = "nettynio";

  /**
   * Used to configure connection maximum idle time {@link
   * software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient.Builder}. This flag only works
   * when {@link #CLIENT_TYPE} is set to {@link #HTTP_CLIENT_TYPE_NETTYNIO}.
   *
   * <p>For more details, see
   * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/nio/netty/NettyNioAsyncHttpClient.Builder.html
   */
  public static final String NETTYNIO_CONNECTION_MAX_IDLE_TIME_MS =
      "http-client.nettynio.connection-max-idle-time-ms";
  /**
   * Used to configure connection acquisition timeout {@link
   * software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient.Builder}. This flag only works
   * when {@link #CLIENT_TYPE} is set to {@link #HTTP_CLIENT_TYPE_NETTYNIO}.
   *
   * <p>For more details, see
   * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/nio/netty/NettyNioAsyncHttpClient.Builder.html
   */
  public static final String NETTYNIO_ACQUISITION_TIMEOUT_MS =
      "http-client.nettynio.connection-acquisition-timeout-ms";
  /**
   * Used to configure connection timeout {@link
   * software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient.Builder}. This flag only works
   * when {@link #CLIENT_TYPE} is set to {@link #HTTP_CLIENT_TYPE_NETTYNIO}.
   *
   * <p>For more details, see
   * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/nio/netty/NettyNioAsyncHttpClient.Builder.html
   */
  public static final String NETTYNIO_CONNECTION_TIMEOUT_MS =
      "http-client.nettynio.connection-timeout-ms";
  /**
   * Used to configure connection time to live {@link
   * software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient.Builder}. This flag only works
   * when {@link #CLIENT_TYPE} is set to {@link #HTTP_CLIENT_TYPE_NETTYNIO}.
   *
   * <p>For more details, see
   * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/nio/netty/NettyNioAsyncHttpClient.Builder.html
   */
  public static final String NETTYNIO_CONNECTION_TIME_TO_LIVE_MS =
      "http-client.nettynio.connection-time-to-live-ms";
  /**
   * Used to configure maximum concurrency {@link
   * software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient.Builder}. This flag only works
   * when {@link #CLIENT_TYPE} is set to {@link #HTTP_CLIENT_TYPE_NETTYNIO}.
   *
   * <p>For more details, see
   * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/nio/netty/NettyNioAsyncHttpClient.Builder.html
   */
  public static final String NETTYNIO_MAX_CONCURRENCY = "http-client.nettynio.max-concurrency";
  /**
   * Used to configure read timeout {@link
   * software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient.Builder}. This flag only works
   * when {@link #CLIENT_TYPE} is set to {@link #HTTP_CLIENT_TYPE_NETTYNIO}.
   *
   * <p>For more details, see
   * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/nio/netty/NettyNioAsyncHttpClient.Builder.html
   */
  public static final String NETTYNIO_READ_TIMEOUT = "http-client.nettynio.read-timeout";
  /**
   * Used to configure maximum pending connection acquires {@link
   * software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient.Builder}. This flag only works
   * when {@link #CLIENT_TYPE} is set to {@link #HTTP_CLIENT_TYPE_NETTYNIO}.
   *
   * <p>For more details, see
   * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/nio/netty/NettyNioAsyncHttpClient.Builder.html
   */
  public static final String NETTYNIO_MAX_PENDING_CONNECTION_ACQUIRES =
      "http-client.nettynio.max-pending-connection-acquires";
  /**
   * Used to configure write timeout {@link
   * software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient.Builder}. This flag only works
   * when {@link #CLIENT_TYPE} is set to {@link #HTTP_CLIENT_TYPE_NETTYNIO}.
   *
   * <p>For more details, see
   * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/nio/netty/NettyNioAsyncHttpClient.Builder.html
   */
  public static final String NETTYNIO_WRITE_TIMEOUT = "http-client.nettynio.write-timeout";

  private String httpClientType;
  private final Map<String, String> httpClientProperties;

  public HttpClientProperties() {
    this.httpClientType = CLIENT_TYPE_DEFAULT;
    this.httpClientProperties = Collections.emptyMap();
  }

  public HttpClientProperties(Map<String, String> properties) {
    this.httpClientType =
        PropertyUtil.propertyAsString(properties, CLIENT_TYPE, CLIENT_TYPE_DEFAULT);
    this.httpClientProperties =
        PropertyUtil.filterProperties(properties, key -> key.startsWith(CLIENT_PREFIX));
  }

  /**
   * Configure the httpClient for a client according to the HttpClientType. The two supported
   * HttpClientTypes are urlconnection and apache
   *
   * <p>Sample usage:
   *
   * <pre>
   *     S3Client.builder().applyMutation(awsProperties::applyHttpClientConfigurations)
   * </pre>
   */
  public <T extends AwsSyncClientBuilder> void applyHttpClientConfigurations(T builder) {
    if (Strings.isNullOrEmpty(httpClientType)) {
      httpClientType = CLIENT_TYPE_DEFAULT;
    }

    switch (httpClientType) {
      case CLIENT_TYPE_URLCONNECTION:
        UrlConnectionHttpClientConfigurations urlConnectionHttpClientConfigurations =
            loadHttpClientConfigurations(UrlConnectionHttpClientConfigurations.class.getName());
        urlConnectionHttpClientConfigurations.configureHttpClientBuilder(builder);
        break;
      case CLIENT_TYPE_APACHE:
        ApacheHttpClientConfigurations apacheHttpClientConfigurations =
            loadHttpClientConfigurations(ApacheHttpClientConfigurations.class.getName());
        apacheHttpClientConfigurations.configureHttpClientBuilder(builder);
        break;
      default:
        throw new IllegalArgumentException("Unrecognized HTTP client type " + httpClientType);
    }
  }

  /**
   * Configure the httpClient for a client according to the HttpClientType. The supported
   * HttpClientTypes are nettynio
   *
   * <p>Sample usage:
   *
   * <pre>
   *     S3Client.builder().applyMutation(awsProperties::applyAsyncHttpClientConfigurations)
   * </pre>
   */
  public <T extends AwsAsyncClientBuilder> void applyAsyncHttpClientConfigurations(T builder) {
    if (Strings.isNullOrEmpty(httpClientType)) {
      httpClientType = HTTP_CLIENT_TYPE_NETTYNIO;
    }
    switch (httpClientType) {
      case HTTP_CLIENT_TYPE_NETTYNIO:
        NettyNioAsyncHttpClientConfigurations nettyNioAsyncHttpClientConfigurations =
            loadHttpClientConfigurations(NettyNioAsyncHttpClientConfigurations.class.getName());
        nettyNioAsyncHttpClientConfigurations.configureHttpClientBuilder(builder);
        break;
      default:
        throw new IllegalArgumentException("Unrecognized HTTP client type " + httpClientType);
    }
  }

  /**
   * Dynamically load the http client builder to avoid runtime deps requirements of both {@link
   * software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient} and {@link
   * software.amazon.awssdk.http.apache.ApacheHttpClient}, since including both will cause error
   * described in <a href="https://github.com/apache/iceberg/issues/6715">issue#6715</a>
   */
  private <T> T loadHttpClientConfigurations(String impl) {
    Object httpClientConfigurations;
    try {
      httpClientConfigurations =
          DynMethods.builder("create")
              .hiddenImpl(impl, Map.class)
              .buildStaticChecked()
              .invoke(httpClientProperties);
      return (T) httpClientConfigurations;
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format("Cannot create %s to generate and configure the http client builder", impl),
          e);
    }
  }
}
