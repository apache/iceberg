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
import java.util.Locale;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * An {@link InputFile} backed by an HTTP URL, typically a pre-signed object-store URL that encodes
 * auth in its query parameters.
 *
 * <p>A known content length is returned directly; otherwise it is fetched lazily via a {@code GET
 * Range: bytes=0-0} request, which (unlike HEAD) works with pre-signed GET URLs.
 */
class HTTPInputFile implements InputFile {
  private static final long UNKNOWN_LENGTH = -1L;

  private final CloseableHttpClient client;
  private final String location;
  private final String url;

  private long length;

  HTTPInputFile(CloseableHttpClient client, String location, String url) {
    this(client, location, url, UNKNOWN_LENGTH);
  }

  HTTPInputFile(CloseableHttpClient client, String location, String url, long length) {
    Preconditions.checkNotNull(client, "Invalid HTTP client: null");
    Preconditions.checkNotNull(location, "Invalid location: null");
    Preconditions.checkNotNull(url, "Invalid url: null");
    this.client = client;
    this.location = location;
    this.url = url;
    this.length = length;
  }

  @Override
  public long getLength() {
    if (length == UNKNOWN_LENGTH) {
      this.length = fetchContentLength();
    }

    return length;
  }

  @Override
  public SeekableInputStream newStream() {
    return new HTTPInputStream(client, location, url);
  }

  @Override
  public String location() {
    return location;
  }

  @Override
  public boolean exists() {
    try {
      HttpGet request = new HttpGet(url);
      request.setHeader(HttpHeaders.RANGE, "bytes=0-0");
      int statusCode = client.execute(request, ClassicHttpResponse::getCode);
      return statusCode == HttpStatus.SC_PARTIAL_CONTENT || statusCode == HttpStatus.SC_OK;
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to check existence of %s", location);
    }
  }

  /**
   * Fetches the content length via {@code GET Range: bytes=0-0}, reading the total from the {@code
   * Content-Range} header. Works with pre-signed GET URLs, unlike a {@code HEAD} request.
   */
  private long fetchContentLength() {
    try {
      HttpGet request = new HttpGet(url);
      request.setHeader(HttpHeaders.RANGE, "bytes=0-0");

      return client.execute(
          request,
          response -> {
            int statusCode = response.getCode();

            if (statusCode == HttpStatus.SC_NOT_FOUND) {
              throw new NotFoundException("Location does not exist: %s", location);
            }

            // 206 Partial Content: parse total from "Content-Range: bytes 0-0/TOTAL"
            if (statusCode == HttpStatus.SC_PARTIAL_CONTENT) {
              Header contentRange = response.getFirstHeader("Content-Range");
              if (contentRange != null) {
                long total = parseTotalFromContentRange(contentRange.getValue());
                if (total >= 0) {
                  return total;
                }
              }

              return UNKNOWN_LENGTH;
            }

            // 200 OK: server returned full content, use Content-Length
            if (statusCode == HttpStatus.SC_OK) {
              return parseLengthFrom200(response);
            }

            throw new IOException(
                String.format(Locale.ROOT, "Unexpected HTTP %d for %s", statusCode, url));
          });
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to fetch content length for %s", location);
    }
  }

  /** Extracts content length from a 200 response via entity or {@code Content-Length} header. */
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

  /**
   * Parses the total object size from a {@code Content-Range} header value such as {@code bytes
   * 0-0/12345}.
   */
  private static long parseTotalFromContentRange(String contentRange) {
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
