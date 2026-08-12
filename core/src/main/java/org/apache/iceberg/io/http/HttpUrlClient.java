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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SerializableMap;

/**
 * A client that a {@link org.apache.iceberg.io.FileIO} can delegate to for reading a file directly
 * over HTTP(S) instead of its normal, credentialed read path.
 *
 * <p>Intended for catalogs that vend a pre-signed object-store URL directly as a file's location
 * (e.g. a scan task's {@code file-path}). The location is used unchanged as the fetch URL, so
 * {@link InputFile#location()} equals the location passed to {@link #newInputFile(String,
 * MetricsContext)}.
 *
 * <p>The underlying HTTP client's timeouts and connection pool are configurable through the
 * properties passed to {@link #HttpUrlClient(Map)}. When a setting is not provided, the client
 * falls back to JVM system properties and then to the Apache HttpClient defaults.
 *
 * <p>The chunk size used for sequential reads is configurable via {@value #READ_CHUNK_SIZE_BYTES}.
 * When the property is not set, the default passed to {@link #HttpUrlClient(Map, int)} is used,
 * letting a {@link org.apache.iceberg.io.FileIO} supply a value tuned for its backing object store.
 */
public class HttpUrlClient implements Serializable {

  static final String CONNECTION_TIMEOUT_MS = "io.http.connection-timeout-ms";
  static final String SOCKET_TIMEOUT_MS = "io.http.socket-timeout-ms";
  static final String CONNECTION_ACQUISITION_TIMEOUT_MS =
      "io.http.connection-acquisition-timeout-ms";
  static final String MAX_CONNECTIONS = "io.http.max-connections";
  static final String MAX_CONNECTIONS_PER_ROUTE = "io.http.connections-per-route";
  static final String READ_CHUNK_SIZE_BYTES = "io.http.read.chunk-size-bytes";
  static final int READ_CHUNK_SIZE_BYTES_DEFAULT = 4 * 1024 * 1024; // 4 MB

  private final SerializableMap<String, String> properties;
  private final int readChunkSize;

  private transient volatile CloseableHttpClient httpClient;

  public HttpUrlClient() {
    this(Maps.newHashMap());
  }

  public HttpUrlClient(Map<String, String> properties) {
    this(properties, READ_CHUNK_SIZE_BYTES_DEFAULT);
  }

  /**
   * Creates a client whose sequential-read chunk size defaults to {@code defaultReadChunkSize},
   * unless the {@value #READ_CHUNK_SIZE_BYTES} property is set, in which case that value wins.
   *
   * @param properties configuration properties, typically the owning FileIO's properties
   * @param defaultReadChunkSize the read chunk size, in bytes, to use when the property is unset
   */
  public HttpUrlClient(Map<String, String> properties, int defaultReadChunkSize) {
    this.properties = SerializableMap.copyOf(properties == null ? Maps.newHashMap() : properties);
    this.readChunkSize =
        PropertyUtil.propertyAsInt(this.properties, READ_CHUNK_SIZE_BYTES, defaultReadChunkSize);
    Preconditions.checkArgument(
        readChunkSize > 0, "Invalid %s: %s (must be > 0)", READ_CHUNK_SIZE_BYTES, readChunkSize);
  }

  @VisibleForTesting
  int readChunkSize() {
    return readChunkSize;
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
   * Returns {@code url} reduced to {@code scheme://host[:port]/path}, dropping the query, user
   * info, and fragment so a pre-signed URL can be logged without exposing the signature or
   * credentials those components carry. Returns {@code "<redacted>"} when {@code url} cannot be
   * parsed or carries no host, and {@code "null"} for a null input.
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

  /**
   * Returns an {@link InputFile} that reads {@code location} directly over HTTP(S).
   *
   * @param location an HTTP(S) URL; see {@link #isHttpUrl(String)}
   * @param metrics a metrics context that receives read metrics for the returned file
   */
  public InputFile newInputFile(String location, MetricsContext metrics) {
    return new HttpInputFile(httpClient(), location, location, readChunkSize, metrics);
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
    return new HttpInputFile(httpClient(), location, location, length, readChunkSize, metrics);
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
                  // Pre-signed URLs resolve directly to object bytes (200/206); never follow
                  // redirects, so a 3xx cannot bounce a read to an untrusted or internal host.
                  .disableRedirectHandling()
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
