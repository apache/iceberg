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
import java.io.UncheckedIOException;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.metrics.MetricsContext;

/**
 * An {@link InputFile} addressed by a pre-signed URL. {@link #location()} is the URL itself.
 *
 * <p>The length comes from the caller; {@code 0} means unknown, as in {@code S3InputFile}, and is
 * then read from {@code Content-Range} on a single-byte range GET. HEAD is not an option, as the
 * method is part of the signature and presigned URLs are signed with GET method.
 */
class PreSignedUrlInputFile implements InputFile {

  private final CloseableHttpClient http;
  private final String url;
  private final MetricsContext metrics;
  private Long length;

  PreSignedUrlInputFile(CloseableHttpClient http, String url, long length, MetricsContext metrics) {
    this.http = http;
    this.url = url;
    this.length = length > 0 ? length : null;
    this.metrics = metrics;
  }

  @Override
  public long getLength() {
    if (length == null) {
      this.length = probeLength();
    }

    return length;
  }

  @Override
  public SeekableInputStream newStream() {
    return new PreSignedUrlInputStream(http, url, metrics);
  }

  @Override
  public String location() {
    return url;
  }

  @Override
  public boolean exists() {
    try {
      long probed = probeLength();
      if (length == null) {
        this.length = probed;
      }

      return true;
    } catch (NotFoundException e) {
      return false;
    }
  }

  /** One single-byte range GET; the same request answers both the length and existence. */
  private long probeLength() {
    HttpGet get = new HttpGet(url);
    get.setHeader("Range", "bytes=0-0");
    try {
      ClassicHttpResponse response = http.executeOpen(null, get, null);
      int code = response.getCode();
      if (code != HttpStatus.SC_PARTIAL_CONTENT
          && code != HttpStatus.SC_REQUESTED_RANGE_NOT_SATISFIABLE) {
        throw PreSignedUrlInputStream.failure("Length probe", url, response);
      }

      // Content-Range: bytes 0-0/<length>, or bytes */<length> for an empty object
      Header contentRange = response.getFirstHeader("Content-Range");
      PreSignedUrlInputStream.discard(response);
      if (contentRange == null || !contentRange.getValue().contains("/")) {
        throw new IOException("Length probe of " + url + " returned no Content-Range");
      }

      String value = contentRange.getValue();
      return Long.parseLong(value.substring(value.lastIndexOf('/') + 1).trim());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
