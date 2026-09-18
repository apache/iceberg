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

import java.io.IOException;
import java.net.URI;
import java.util.Locale;
import java.util.Map;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.PropertyUtil;

/**
 * An {@link InputFile} backed by an HTTP URL, typically a pre-signed object-store URL that encodes
 * auth in its query parameters.
 *
 * <p>A known content length is returned directly; otherwise it is fetched lazily via a {@code GET
 * Range: bytes=0-0} request, which (unlike HEAD) works with pre-signed GET URLs.
 */
class HttpInputFile extends BaseHttpFile implements InputFile {
  static final String READ_CHUNK_SIZE_BYTES = "io.http.read.chunk-size-bytes";
  static final int READ_CHUNK_SIZE_BYTES_DEFAULT = 8 * 1024 * 1024;
  static final long UNKNOWN_LENGTH = -1L;

  private final int chunkSize;

  private long length;

  static InputFile fromLocation(
      String location, Map<String, String> properties, MetricsContext metrics) {
    return new HttpInputFile(location, location, UNKNOWN_LENGTH, properties, metrics);
  }

  static InputFile fromLocation(
      String location, long length, Map<String, String> properties, MetricsContext metrics) {
    return new HttpInputFile(location, location, length, properties, metrics);
  }

  /** Returns {@code true} if {@code location} is an HTTP(S) URL. */
  static boolean isHttpUrl(String location) {
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
    super(client, location, url, properties, metrics);
    Preconditions.checkArgument(
        chunkSize > 0, "Invalid %s: %s (must be > 0)", READ_CHUNK_SIZE_BYTES, chunkSize);
    this.chunkSize = chunkSize;
    this.length = length;
  }

  @Override
  public long getLength() {
    if (length == UNKNOWN_LENGTH) {
      long fetchedLength = fetchContentLength();
      if (fetchedLength == UNKNOWN_LENGTH) {
        throw new RuntimeIOException(
            "Cannot determine content length for %s", BaseHttpFile.redact(location()));
      }

      this.length = fetchedLength;
    }

    return length;
  }

  @Override
  public SeekableInputStream newStream() {
    if (hasSharedClient()) {
      return new HttpInputStream(client(), location(), url(), chunkSize, metrics());
    }

    return new HttpInputStream(newHttpClient(), location(), url(), chunkSize, metrics(), true);
  }

  @Override
  public boolean exists() {
    try (CloseableHttpClient httpClient = hasSharedClient() ? null : newHttpClient()) {
      HttpGet request = new HttpGet(url());
      request.setHeader(HttpHeaders.RANGE, "bytes=0-0");
      CloseableHttpClient requestClient = hasSharedClient() ? client() : httpClient;
      return requestClient.execute(
          request,
          response ->
              switch (BaseHttpFile.classifyStatus(response.getCode())) {
                case OK, PARTIAL_CONTENT -> true;
                case RANGE_NOT_SATISFIABLE, NOT_FOUND, FORBIDDEN, RETRYABLE, FAILURE -> false;
              });
    } catch (IOException e) {
      throw new RuntimeIOException(
          e, "Failed to check existence of %s", BaseHttpFile.redact(location()));
    }
  }

  /**
   * Fetches the content length via {@code GET Range: bytes=0-0}, reading the total from the {@code
   * Content-Range} header. Works with pre-signed GET URLs, unlike a {@code HEAD} request.
   */
  private long fetchContentLength() {
    try (CloseableHttpClient httpClient = hasSharedClient() ? null : newHttpClient()) {
      HttpGet request = new HttpGet(url());
      request.setHeader(HttpHeaders.RANGE, "bytes=0-0");
      CloseableHttpClient requestClient = hasSharedClient() ? client() : httpClient;

      return requestClient.execute(
          request,
          response -> {
            int statusCode = response.getCode();
            return switch (BaseHttpFile.classifyStatus(statusCode)) {
              case PARTIAL_CONTENT -> parseTotalFromPartialContent(response);
              case OK -> parseLengthFrom200(response);
              case NOT_FOUND ->
                  throw new NotFoundException(
                      "Location does not exist: %s", BaseHttpFile.redact(location()));
              case FORBIDDEN ->
                  throw new ForbiddenException(
                      "Access forbidden for %s", BaseHttpFile.redact(location()));
              case RANGE_NOT_SATISFIABLE, RETRYABLE, FAILURE ->
                  throw new IOException(
                      String.format(
                          Locale.ROOT,
                          "Unexpected HTTP %d for %s",
                          statusCode,
                          BaseHttpFile.redact(url())));
            };
          });
    } catch (IOException e) {
      throw new RuntimeIOException(
          e, "Failed to fetch content length for %s", BaseHttpFile.redact(location()));
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
}
