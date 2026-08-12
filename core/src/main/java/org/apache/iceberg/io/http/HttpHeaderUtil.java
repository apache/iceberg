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

import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.Header;

/**
 * Reads a file's content length from the HTTP response to a {@code GET Range: bytes=0-0} probe.
 *
 * <p>A length that cannot be determined is reported as {@link #UNKNOWN_LENGTH}.
 */
class HttpHeaderUtil {
  static final long UNKNOWN_LENGTH = -1L;

  private HttpHeaderUtil() {}

  /** Reads the total object size from the {@code Content-Range} header of a 206 response. */
  static long parseTotalFromPartialContent(ClassicHttpResponse response) {
    Header contentRange = response.getFirstHeader("Content-Range");
    if (contentRange != null) {
      long total = parseTotalFromContentRange(contentRange.getValue());
      if (total >= 0) {
        return total;
      }
    }

    return UNKNOWN_LENGTH;
  }

  /** Extracts content length from a 200 response via entity or {@code Content-Length} header. */
  static long parseLengthFrom200(ClassicHttpResponse response) {
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
