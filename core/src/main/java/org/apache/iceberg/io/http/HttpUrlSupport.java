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

import java.io.Serializable;
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
import org.apache.hc.core5.io.CloseMode;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.PropertyUtil;

/**
 * A helper that a {@link org.apache.iceberg.io.FileIO} can delegate to for reading a file directly
 * over HTTP(S) instead of its normal, credentialed read path.
 *
 * <p>Intended for catalogs that vend a pre-signed object-store URL directly as a file's location
 * (e.g. a scan task's {@code file-path}). The location is used unchanged as the fetch URL, so
 * {@link InputFile#location()} equals the location passed to {@link #newInputFile(String)}.
 *
 * <p>The underlying HTTP client's timeouts and connection pool are configurable through the
 * properties passed to {@link #HttpUrlSupport(Map)}. When a setting is not provided, the client
 * falls back to JVM system properties and then to the Apache HttpClient defaults.
 */
public class HttpUrlSupport implements Serializable {

  static final String CONNECTION_TIMEOUT_MS = "io.http.connection-timeout-ms";
  static final String SOCKET_TIMEOUT_MS = "io.http.socket-timeout-ms";
  static final String CONNECTION_ACQUISITION_TIMEOUT_MS =
      "io.http.connection-acquisition-timeout-ms";
  static final String MAX_CONNECTIONS = "io.http.max-connections";
  static final String MAX_CONNECTIONS_PER_ROUTE = "io.http.connections-per-route";

  private final Map<String, String> properties;

  private transient volatile CloseableHttpClient httpClient;

  public HttpUrlSupport() {
    this(ImmutableMap.of());
  }

  public HttpUrlSupport(Map<String, String> properties) {
    this.properties = properties == null ? ImmutableMap.of() : ImmutableMap.copyOf(properties);
  }

  /** Returns {@code true} if {@code location} is an HTTP(S) URL. */
  public static boolean isHttpUrl(String location) {
    if (location == null) {
      return false;
    }

    try {
      String scheme = URI.create(location).getScheme();
      return "https".equalsIgnoreCase(scheme) || "http".equalsIgnoreCase(scheme);
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  /**
   * Returns an {@link InputFile} that reads {@code location} directly over HTTP(S).
   *
   * @param location an HTTP(S) URL; see {@link #isHttpUrl(String)}
   * @param metrics a metrics context that receives read metrics for the returned file
   */
  public InputFile newInputFile(String location, MetricsContext metrics) {
    return new HTTPInputFile(httpClient(), location, location, metrics);
  }

  /**
   * Returns an {@link InputFile} of the given {@code length} that reads {@code location} directly
   * over HTTP(S).
   *
   * @param location an HTTP(S) URL; see {@link #isHttpUrl(String)}
   * @param length the known content length of the file
   * @param metrics a metrics context that receives read metrics for the returned file
   */
  public InputFile newInputFile(String location, long length, MetricsContext metrics) {
    return new HTTPInputFile(httpClient(), location, location, length, metrics);
  }

  public void close() {
    synchronized (this) {
      if (httpClient != null) {
        httpClient.close(CloseMode.GRACEFUL);
        httpClient = null;
      }
    }
  }

  private CloseableHttpClient httpClient() {
    if (httpClient == null) {
      synchronized (this) {
        if (httpClient == null) {
          HttpClientBuilder clientBuilder =
              HttpClients.custom()
                  .useSystemProperties()
                  .setConnectionManager(configureConnectionManager(properties));

          RequestConfig requestConfig = configureRequestConfig(properties);
          if (requestConfig != null) {
            clientBuilder.setDefaultRequestConfig(requestConfig);
          }

          this.httpClient = clientBuilder.build();
        }
      }
    }

    return httpClient;
  }

  @VisibleForTesting
  static HttpClientConnectionManager configureConnectionManager(Map<String, String> properties) {
    PoolingHttpClientConnectionManagerBuilder connectionManagerBuilder =
        PoolingHttpClientConnectionManagerBuilder.create().useSystemProperties();

    Integer maxConnections = PropertyUtil.propertyAsNullableInt(properties, MAX_CONNECTIONS);
    if (maxConnections != null) {
      connectionManagerBuilder.setMaxConnTotal(maxConnections);
    }

    Integer maxConnectionsPerRoute =
        PropertyUtil.propertyAsNullableInt(properties, MAX_CONNECTIONS_PER_ROUTE);
    if (maxConnectionsPerRoute != null) {
      connectionManagerBuilder.setMaxConnPerRoute(maxConnectionsPerRoute);
    }

    ConnectionConfig connectionConfig = configureConnectionConfig(properties);
    if (connectionConfig != null) {
      connectionManagerBuilder.setDefaultConnectionConfig(connectionConfig);
    }

    return connectionManagerBuilder.build();
  }

  @VisibleForTesting
  static ConnectionConfig configureConnectionConfig(Map<String, String> properties) {
    Long connectionTimeoutMillis =
        PropertyUtil.propertyAsNullableLong(properties, CONNECTION_TIMEOUT_MS);
    Integer socketTimeoutMillis = PropertyUtil.propertyAsNullableInt(properties, SOCKET_TIMEOUT_MS);

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
    Long acquisitionTimeoutMillis =
        PropertyUtil.propertyAsNullableLong(properties, CONNECTION_ACQUISITION_TIMEOUT_MS);

    if (acquisitionTimeoutMillis == null) {
      return null;
    }

    return RequestConfig.custom()
        .setConnectionRequestTimeout(acquisitionTimeoutMillis, TimeUnit.MILLISECONDS)
        .build();
  }
}
