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
package org.apache.iceberg.io;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SerializableMap;

abstract class BaseHttpFile {
  static final String CONNECTION_TIMEOUT_MS = "io.http.connection-timeout-ms";
  static final String SOCKET_TIMEOUT_MS = "io.http.socket-timeout-ms";
  static final String CONNECTION_ACQUISITION_TIMEOUT_MS =
      "io.http.connection-acquisition-timeout-ms";
  static final String MAX_CONNECTIONS = "io.http.max-connections";
  static final String MAX_CONNECTIONS_PER_ROUTE = "io.http.connections-per-route";

  private final CloseableHttpClient client;
  private final SerializableMap<String, String> properties;

  private final String location;
  private final String url;
  private final MetricsContext metrics;

  BaseHttpFile(
      CloseableHttpClient client,
      String location,
      String url,
      Map<String, String> properties,
      MetricsContext metrics) {
    Preconditions.checkNotNull(location, "Invalid location: null");
    Preconditions.checkNotNull(url, "Invalid url: null");
    Preconditions.checkNotNull(metrics, "Invalid metrics context: null");
    this.client = client;
    this.location = location;
    this.url = url;
    this.properties = SerializableMap.copyOf(properties == null ? Map.of() : properties);
    this.metrics = metrics;
  }

  public String location() {
    return location;
  }

  protected String url() {
    return url;
  }

  protected MetricsContext metrics() {
    return metrics;
  }

  protected CloseableHttpClient client() {
    return client;
  }

  protected boolean hasSharedClient() {
    return client != null;
  }

  protected CloseableHttpClient newHttpClient() {
    return newHttpClient(properties);
  }

  enum Status {
    OK,
    PARTIAL_CONTENT,
    RANGE_NOT_SATISFIABLE,
    NOT_FOUND,
    FORBIDDEN,
    RETRYABLE,
    FAILURE
  }

  static Status classifyStatus(int statusCode) {
    return switch (statusCode) {
      case HttpStatus.SC_OK -> Status.OK;
      case HttpStatus.SC_PARTIAL_CONTENT -> Status.PARTIAL_CONTENT;
      case HttpStatus.SC_REQUESTED_RANGE_NOT_SATISFIABLE -> Status.RANGE_NOT_SATISFIABLE;
      case HttpStatus.SC_NOT_FOUND -> Status.NOT_FOUND;
      case HttpStatus.SC_FORBIDDEN -> Status.FORBIDDEN;
      case HttpStatus.SC_TOO_MANY_REQUESTS,
              HttpStatus.SC_REQUEST_TIMEOUT,
              HttpStatus.SC_INTERNAL_SERVER_ERROR,
              HttpStatus.SC_BAD_GATEWAY,
              HttpStatus.SC_SERVICE_UNAVAILABLE,
              HttpStatus.SC_GATEWAY_TIMEOUT ->
          Status.RETRYABLE;
      default -> Status.FAILURE;
    };
  }

  @VisibleForTesting
  static CloseableHttpClient newHttpClient(Map<String, String> properties) {
    HttpClientBuilder clientBuilder =
        HttpClients.custom()
            .useSystemProperties()
            // Pre-signed URLs resolve directly to object bytes (200/206); never follow redirects.
            .disableRedirectHandling()
            .disableAutomaticRetries()
            .setConnectionManager(configureConnectionManager(properties));

    RequestConfig requestConfig = configureRequestConfig(properties);
    if (requestConfig != null) {
      clientBuilder.setDefaultRequestConfig(requestConfig);
    }

    return clientBuilder.build();
  }

  @VisibleForTesting
  static HttpClientConnectionManager configureConnectionManager(Map<String, String> properties) {
    Map<String, String> httpProperties = properties == null ? Map.of() : properties;
    PoolingHttpClientConnectionManagerBuilder connectionManagerBuilder =
        PoolingHttpClientConnectionManagerBuilder.create().useSystemProperties();

    Integer maxConnections = PropertyUtil.propertyAsNullableInt(httpProperties, MAX_CONNECTIONS);
    if (maxConnections != null) {
      connectionManagerBuilder.setMaxConnTotal(maxConnections);
    }

    Integer maxConnectionsPerRoute =
        PropertyUtil.propertyAsNullableInt(httpProperties, MAX_CONNECTIONS_PER_ROUTE);
    if (maxConnectionsPerRoute != null) {
      connectionManagerBuilder.setMaxConnPerRoute(maxConnectionsPerRoute);
    }

    ConnectionConfig connectionConfig = configureConnectionConfig(httpProperties);
    if (connectionConfig != null) {
      connectionManagerBuilder.setDefaultConnectionConfig(connectionConfig);
    }

    return connectionManagerBuilder.build();
  }

  @VisibleForTesting
  static ConnectionConfig configureConnectionConfig(Map<String, String> properties) {
    Map<String, String> httpProperties = properties == null ? Map.of() : properties;
    Long connectionTimeoutMillis =
        PropertyUtil.propertyAsNullableLong(httpProperties, CONNECTION_TIMEOUT_MS);
    Integer socketTimeoutMillis =
        PropertyUtil.propertyAsNullableInt(httpProperties, SOCKET_TIMEOUT_MS);

    if (connectionTimeoutMillis == null && socketTimeoutMillis == null) {
      return null;
    }

    ConnectionConfig.Builder connectionConfigBuilder = ConnectionConfig.custom();
    if (connectionTimeoutMillis != null) {
      connectionConfigBuilder.setConnectTimeout(connectionTimeoutMillis, TimeUnit.MILLISECONDS);
    }

    if (socketTimeoutMillis != null) {
      connectionConfigBuilder.setSocketTimeout(socketTimeoutMillis, TimeUnit.MILLISECONDS);
    }

    return connectionConfigBuilder.build();
  }

  @VisibleForTesting
  static RequestConfig configureRequestConfig(Map<String, String> properties) {
    Map<String, String> httpProperties = properties == null ? Map.of() : properties;
    Long acquisitionTimeoutMillis =
        PropertyUtil.propertyAsNullableLong(httpProperties, CONNECTION_ACQUISITION_TIMEOUT_MS);

    if (acquisitionTimeoutMillis == null) {
      return null;
    }

    return RequestConfig.custom()
        .setConnectionRequestTimeout(acquisitionTimeoutMillis, TimeUnit.MILLISECONDS)
        .build();
  }

  /**
   * Returns {@code url} reduced to {@code scheme://host[:port]/path}, dropping the query, user
   * info, and fragment so a pre-signed URL can be logged without exposing the signature or
   * credentials those components carry.
   */
  static String redact(String url) {
    if (url == null) {
      return "null";
    }

    try {
      URI uri = URI.create(url);
      String scheme = uri.getScheme();
      String host = uri.getHost();
      if (scheme == null || host == null) {
        return "<redacted>";
      }

      StringBuilder sanitized = new StringBuilder(scheme).append("://").append(host);
      if (uri.getPort() != -1) {
        sanitized.append(':').append(uri.getPort());
      }

      if (uri.getRawPath() != null) {
        sanitized.append(uri.getRawPath());
      }

      return sanitized.toString();
    } catch (IllegalArgumentException e) {
      return "<redacted>";
    }
  }
}
