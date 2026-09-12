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
import java.io.InputStream;
import java.util.Locale;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.io.CloseMode;
import org.apache.hc.core5.io.ModalCloseable;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.metrics.Counter;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.metrics.MetricsContext.Unit;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Reads an object through a URL with range requests. A seek beyond the bytes already read reopens
 * the connection at the new offset.
 */
class PreSignedUrlInputStream extends SeekableInputStream {

  private static final int SKIP_SIZE = 1024 * 1024;

  private final CloseableHttpClient http;
  private final String url;
  private final Counter readBytes;
  private final Counter readOperations;

  private ClassicHttpResponse response = null;
  private InputStream stream = null;
  private boolean streamAtEof = false;
  private long pos = 0;
  private long next = 0;
  private boolean closed = false;

  PreSignedUrlInputStream(CloseableHttpClient http, String url, MetricsContext metrics) {
    this.http = http;
    this.url = url;
    this.readBytes = metrics.counter(FileIOMetricsContext.READ_BYTES, Unit.BYTES);
    this.readOperations = metrics.counter(FileIOMetricsContext.READ_OPERATIONS);
  }

  @Override
  public long getPos() {
    return next;
  }

  @Override
  public void seek(long newPos) {
    Preconditions.checkState(!closed, "Cannot seek: already closed");
    Preconditions.checkArgument(newPos >= 0, "Invalid position (negative): %s", newPos);
    this.next = newPos;
  }

  @Override
  public int read() throws IOException {
    Preconditions.checkState(!closed, "Cannot read: already closed");
    positionStream();

    int b = stream.read();
    if (b >= 0) {
      pos += 1;
      next += 1;
      readBytes.increment();
      readOperations.increment();
    } else {
      streamAtEof = true;
    }

    return b;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    Preconditions.checkState(!closed, "Cannot read: already closed");
    if (len == 0) {
      return 0;
    }

    positionStream();

    int n = stream.read(b, off, len);
    if (n > 0) {
      pos += n;
      next += n;
      readBytes.increment(n);
      readOperations.increment();
    } else if (n < 0) {
      streamAtEof = true;
    }

    return n;
  }

  private void positionStream() throws IOException {
    if (stream != null && next == pos) {
      return;
    }

    if (stream != null && next > pos && next - pos <= SKIP_SIZE) {
      long skip = next - pos;
      try {
        while (skip > 0) {
          long skipped = stream.skip(skip);
          if (skipped <= 0) {
            break;
          }
          skip -= skipped;
        }
      } catch (IOException e) {
        skip = -1;
      }

      if (skip == 0) {
        pos = next;
        return;
      }
    }

    closeStream();
    openStream(next);
    pos = next;
  }

  private void openStream(long from) throws IOException {
    HttpGet get = new HttpGet(url);
    get.setHeader("Range", String.format(Locale.ROOT, "bytes=%d-", from));
    ClassicHttpResponse opened = http.executeOpen(null, get, null);

    int code = opened.getCode();
    if (code == HttpStatus.SC_REQUESTED_RANGE_NOT_SATISFIABLE) {
      // at or past the end of the object
      discard(opened);
      this.response = null;
      this.stream = InputStream.nullInputStream();
      return;
    }

    if (code != HttpStatus.SC_OK && code != HttpStatus.SC_PARTIAL_CONTENT) {
      throw failure("Read", url, opened);
    }

    if (from > 0 && code == HttpStatus.SC_OK) {
      // the server ignored the range; reading this as data from byte 0 would be silent corruption
      discard(opened);
      throw new IOException(
          String.format(
              Locale.ROOT, "Read of %s failed: server ignored Range bytes=%d-", url, from));
    }

    this.response = opened;
    this.stream = opened.getEntity().getContent();
    this.streamAtEof = false;
  }

  private void closeStream() throws IOException {
    if (response != null) {
      if (streamAtEof) {
        response.close();
      } else {
        discard(response);
      }

      response = null;
    }

    stream = null;
    streamAtEof = false;
  }

  @Override
  public void close() throws IOException {
    super.close();
    closed = true;
    closeStream();
  }

  /**
   * Closes a response without reading the rest of its body. A graceful close drains the body to
   * keep the connection reusable, which for an open-ended range means downloading the rest of the
   * object.
   */
  static void discard(ClassicHttpResponse response) throws IOException {
    if (response instanceof ModalCloseable) {
      ((ModalCloseable) response).close(CloseMode.IMMEDIATE);
    } else {
      response.close();
    }
  }

  /** The exception for a failed request; 404 is thrown. */
  static IOException failure(String what, String url, ClassicHttpResponse response)
      throws IOException {
    try {
      if (response.getCode() == HttpStatus.SC_NOT_FOUND) {
        throw new NotFoundException("Location does not exist: %s", url);
      }

      return new IOException(
          String.format(
              Locale.ROOT, "%s of %s failed with HTTP %d", what, url, response.getCode()));
    } finally {
      discard(response);
    }
  }
}
