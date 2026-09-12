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

import java.io.IOException;
import java.net.URI;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SerializableMap;

/**
 * An {@link InputFile} backed by an HTTP URL, typically a pre-signed object-store URL that encodes
 * auth in its query parameters.
 *
 * <p>A known content length is returned directly; otherwise it is fetched lazily via a {@code GET
 * Range: bytes=0-0} request, which (unlike HEAD) works with pre-signed GET URLs.
 */
public class HttpInputFile implements InputFile {
  static final String CONNECTION_TIMEOUT_MS = "io.http.connection-timeout-ms";
  static final String SOCKET_TIMEOUT_MS = "io.http.socket-timeout-ms";
  static final String CONNECTION_ACQUISITION_TIMEOUT_MS =
      "io.http.connection-acquisition-timeout-ms";
  static final String MAX_CONNECTIONS = "io.http.max-connections";
  static final String MAX_CONNECTIONS_PER_ROUTE = "io.http.connections-per-route";
  static final String READ_CHUNK_SIZE_BYTES = "io.http.read.chunk-size-bytes";
  static final int READ_CHUNK_SIZE_BYTES_DEFAULT = 8 * 1024 * 1024;
  static final long UNKNOWN_LENGTH = -1L;

  private final SerializableMap<String, String> properties;
  private final CloseableHttpClient client;
  private final String location;
  private final String url;
  private final int chunkSize;
  private final MetricsContext metrics;

  private long length;

  public static InputFile fromLocation(
      String location, Map<String, String> properties, MetricsContext metrics) {
    return new HttpInputFile(location, location, UNKNOWN_LENGTH, properties, metrics);
  }

  public static InputFile fromLocation(
      String location, long length, Map<String, String> properties, MetricsContext metrics) {
    return new HttpInputFile(location, location, length, properties, metrics);
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

  private HttpInputFile(
      String location,
      String url,
      long length,
      Map<String, String> properties,
      MetricsContext metrics) {
    this(null, location, url, length, readChunkSize(properties), properties, metrics);
  }

  HttpInputFile(
      CloseableHttpClient client,
      String location,
      String url,
      int chunkSize,
      MetricsContext metrics) {
    this(client, location, url, UNKNOWN_LENGTH, chunkSize, null, metrics);
  }

  HttpInputFile(
      CloseableHttpClient client,
      String location,
      String url,
      long length,
      int chunkSize,
      MetricsContext metrics) {
    this(client, location, url, length, chunkSize, null, metrics);
  }

  private HttpInputFile(
      CloseableHttpClient client,
      String location,
      String url,
      long length,
      int chunkSize,
      Map<String, String> properties,
      MetricsContext metrics) {
    Preconditions.checkNotNull(location, "Invalid location: null");
    Preconditions.checkNotNull(url, "Invalid url: null");
    Preconditions.checkNotNull(metrics, "Invalid metrics context: null");
    Preconditions.checkArgument(
        chunkSize > 0, "Invalid %s: %s (must be > 0)", READ_CHUNK_SIZE_BYTES, chunkSize);
    this.client = client;
    this.location = location;
    this.url = url;
    this.chunkSize = chunkSize;
    this.length = length;
    this.metrics = metrics;
    this.properties = SerializableMap.copyOf(properties == null ? Map.of() : properties);
  }

  @Override
  public long getLength() {
    if (length == UNKNOWN_LENGTH) {
      long fetchedLength = fetchContentLength();
      if (fetchedLength == UNKNOWN_LENGTH) {
        throw new RuntimeIOException(
            "Cannot determine content length for %s", HttpInputFile.redact(location));
      }

      this.length = fetchedLength;
    }

    return length;
  }

  @Override
  public SeekableInputStream newStream() {
    if (client != null) {
      return new HttpInputStream(client, location, url, chunkSize, metrics);
    }

    return new HttpInputStream(newHttpClient(properties), location, url, chunkSize, metrics, true);
  }

  @Override
  public String location() {
    return location;
  }

  @Override
  public boolean exists() {
    try (CloseableHttpClient httpClient = client != null ? null : newHttpClient(properties)) {
      HttpGet request = new HttpGet(url);
      request.setHeader(HttpHeaders.RANGE, "bytes=0-0");
      CloseableHttpClient requestClient = client != null ? client : httpClient;
      HttpStatusCategory category =
          requestClient.execute(
              request, response -> HttpStatusCategory.classify(response.getCode()));
      return category == HttpStatusCategory.OK || category == HttpStatusCategory.PARTIAL_CONTENT;
    } catch (IOException e) {
      throw new RuntimeIOException(
          e, "Failed to check existence of %s", HttpInputFile.redact(location));
    }
  }

  /**
   * Fetches the content length via {@code GET Range: bytes=0-0}, reading the total from the {@code
   * Content-Range} header. Works with pre-signed GET URLs, unlike a {@code HEAD} request.
   */
  private long fetchContentLength() {
    try (CloseableHttpClient httpClient = client != null ? null : newHttpClient(properties)) {
      HttpGet request = new HttpGet(url);
      request.setHeader(HttpHeaders.RANGE, "bytes=0-0");
      CloseableHttpClient requestClient = client != null ? client : httpClient;

      return requestClient.execute(
          request,
          response -> {
            int statusCode = response.getCode();
            return switch (HttpStatusCategory.classify(statusCode)) {
                // 206 Partial Content: total parsed from "Content-Range: bytes 0-0/TOTAL"
              case PARTIAL_CONTENT -> parseTotalFromPartialContent(response);
                // 200 OK: server returned full content, use Content-Length
              case OK -> parseLengthFrom200(response);
              case NOT_FOUND ->
                  throw new NotFoundException(
                      "Location does not exist: %s", HttpInputFile.redact(location));
              case FORBIDDEN ->
                  throw new ForbiddenException(
                      "Access forbidden for %s", HttpInputFile.redact(location));
              case RANGE_NOT_SATISFIABLE, TRANSIENT, TERMINAL ->
                  throw new IOException(
                      String.format(
                          Locale.ROOT,
                          "Unexpected HTTP %d for %s",
                          statusCode,
                          HttpInputFile.redact(url)));
            };
          });
    } catch (IOException e) {
      throw new RuntimeIOException(
          e, "Failed to fetch content length for %s", HttpInputFile.redact(location));
    }
  }

  @VisibleForTesting
  static int readChunkSize(Map<String, String> properties) {
    Map<String, String> httpProperties = properties == null ? Map.of() : properties;
    int readChunkSize =
        PropertyUtil.propertyAsInt(
            httpProperties, READ_CHUNK_SIZE_BYTES, READ_CHUNK_SIZE_BYTES_DEFAULT);
    Preconditions.checkArgument(
        readChunkSize > 0, "Invalid %s: %s (must be > 0)", READ_CHUNK_SIZE_BYTES, readChunkSize);
    return readChunkSize;
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

  private static long parseTotalFromPartialContent(ClassicHttpResponse response) {
    Header contentRange = response.getFirstHeader("Content-Range");
    if (contentRange != null) {
      long total = parseTotalFromContentRange(contentRange.getValue());
      if (total >= 0) {
        return total;
      }
    }

    return UNKNOWN_LENGTH;
  }

  private static long parseLengthFrom200(ClassicHttpResponse response) {
    long contentLength =
        response.getEntity() != null ? response.getEntity().getContentLength() : UNKNOWN_LENGTH;
    if (contentLength >= 0) {
      return contentLength;
    }

    Header header = response.getFirstHeader("Content-Length");
    if (header != null) {
      try {
        return Long.parseLong(header.getValue());
      } catch (NumberFormatException e) {
        // fall through to UNKNOWN_LENGTH
      }
    }

    return UNKNOWN_LENGTH;
  }

  @VisibleForTesting
  static long parseTotalFromContentRange(String contentRange) {
    int slash = contentRange.lastIndexOf('/');
    if (slash < 0) {
      return UNKNOWN_LENGTH;
    }

    String totalStr = contentRange.substring(slash + 1).trim();
    if ("*".equals(totalStr)) {
      return UNKNOWN_LENGTH;
    }

    try {
      return Long.parseLong(totalStr);
    } catch (NumberFormatException e) {
      return UNKNOWN_LENGTH;
    }
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
