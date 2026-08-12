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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIOMetricsContext;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.metrics.Counter;
import org.apache.iceberg.metrics.DefaultMetricsContext;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.metrics.MetricsContext.Unit;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Exercises {@link HTTPInputFile}/{@link HTTPInputStream} against a real {@link HttpServer} that
 * serves ranges from the request {@code Range} header, giving cloud-free coverage of the HTTP read
 * path, including multi-chunk reads that cross the configured chunk-size boundary.
 */
class TestHTTPInputFile {

  private static final String PATH = "/object";
  private static final int CHUNK_SIZE = 64 * 1024;
  private static final byte[] DATA = randomBytes(CHUNK_SIZE + 1_000, 42L);

  private static HttpServer server;
  private static CloseableHttpClient client;
  private static String url;
  private static String missingUrl;
  private static String forbiddenUrl;
  private static String serverErrorUrl;
  private static String throttledUrl;
  private static String notImplementedUrl;
  private static String redirectUrl;
  private static final AtomicInteger REQUEST_COUNT = new AtomicInteger();

  @BeforeAll
  static void startServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    server.createContext(PATH, TestHTTPInputFile::handle);
    server.createContext("/missing", exchange -> respondStatus(exchange, 404));
    server.createContext("/forbidden", exchange -> respondStatus(exchange, 403));
    server.createContext("/server-error", exchange -> respondStatus(exchange, 500));
    server.createContext("/throttled", exchange -> respondStatus(exchange, 429));
    server.createContext("/not-implemented", exchange -> respondStatus(exchange, 501));
    server.createContext("/redirect", TestHTTPInputFile::respondRedirect);
    server.start();
    // disable the client's built-in retries so tests exercise HTTPInputStream's own retry logic
    client = HttpClients.custom().disableAutomaticRetries().build();
    int port = server.getAddress().getPort();
    url = String.format(Locale.ROOT, "http://127.0.0.1:%d%s", port, PATH);
    missingUrl = String.format(Locale.ROOT, "http://127.0.0.1:%d/missing", port);
    forbiddenUrl = String.format(Locale.ROOT, "http://127.0.0.1:%d/forbidden", port);
    serverErrorUrl = String.format(Locale.ROOT, "http://127.0.0.1:%d/server-error", port);
    throttledUrl = String.format(Locale.ROOT, "http://127.0.0.1:%d/throttled", port);
    notImplementedUrl = String.format(Locale.ROOT, "http://127.0.0.1:%d/not-implemented", port);
    redirectUrl = String.format(Locale.ROOT, "http://127.0.0.1:%d/redirect", port);
  }

  @AfterAll
  static void stopServer() throws IOException {
    client.close();
    server.stop(0);
  }

  @BeforeEach
  void resetRequestCount() {
    REQUEST_COUNT.set(0);
  }

  @Test
  void getLengthFetchesTotalFromContentRange() {
    HTTPInputFile inputFile = httpInputFile(url, MetricsContext.nullMetrics());

    assertThat(inputFile.getLength()).isEqualTo(DATA.length);
    assertThat(REQUEST_COUNT.get()).isEqualTo(1);
  }

  @Test
  void getLengthUsesKnownLengthWithoutRequest() {
    HTTPInputFile inputFile =
        new HTTPInputFile(
            client, "s3://bucket/object", url, 123L, CHUNK_SIZE, MetricsContext.nullMetrics());

    assertThat(inputFile.getLength()).isEqualTo(123L);
    assertThat(REQUEST_COUNT.get()).isZero();
  }

  @Test
  void readFullyReadsExactRange() throws IOException {
    HTTPInputFile inputFile = httpInputFile(url, MetricsContext.nullMetrics());

    byte[] buffer = new byte[2_048];
    try (SeekableInputStream stream = inputFile.newStream()) {
      ((RangeReadable) stream).readFully(1_000, buffer, 0, buffer.length);
    }

    assertThat(buffer).isEqualTo(Arrays.copyOfRange(DATA, 1_000, 1_000 + buffer.length));
  }

  @Test
  void readTailReadsSuffix() throws IOException {
    HTTPInputFile inputFile = httpInputFile(url, MetricsContext.nullMetrics());

    byte[] buffer = new byte[512];
    int read;
    try (SeekableInputStream stream = inputFile.newStream()) {
      read = ((RangeReadable) stream).readTail(buffer, 0, buffer.length);
    }

    assertThat(read).isEqualTo(buffer.length);
    assertThat(buffer)
        .isEqualTo(Arrays.copyOfRange(DATA, DATA.length - buffer.length, DATA.length));
  }

  @Test
  void sequentialReadCrossesChunkBoundary() throws IOException {
    HTTPInputFile inputFile = httpInputFile(url, MetricsContext.nullMetrics());

    byte[] actual = new byte[DATA.length];
    try (SeekableInputStream stream = inputFile.newStream()) {
      IOUtil.readFully(stream, actual, 0, actual.length);
    }

    assertThat(actual).isEqualTo(DATA);
    // reading past a single chunk requires at least two range fetches
    assertThat(REQUEST_COUNT.get()).isGreaterThanOrEqualTo(2);
  }

  @Test
  void sequentialReadWithinSingleChunkFetchesOnce() throws IOException {
    // a chunk larger than the object means the whole file is served by one range fetch
    HTTPInputFile inputFile =
        new HTTPInputFile(
            client, "s3://bucket/object", url, DATA.length + 1_000, MetricsContext.nullMetrics());

    byte[] actual = new byte[DATA.length];
    try (SeekableInputStream stream = inputFile.newStream()) {
      IOUtil.readFully(stream, actual, 0, actual.length);
    }

    assertThat(actual).isEqualTo(DATA);
    assertThat(REQUEST_COUNT.get()).isEqualTo(1);
  }

  @Test
  void sequentialReadTracksReadMetrics() throws IOException {
    CachingMetricsContext metrics = new CachingMetricsContext();
    HTTPInputFile inputFile = httpInputFile(url, metrics);

    byte[] actual = new byte[DATA.length];
    try (SeekableInputStream stream = inputFile.newStream()) {
      IOUtil.readFully(stream, actual, 0, actual.length);
    }

    Counter readBytes = metrics.counter(FileIOMetricsContext.READ_BYTES, Unit.BYTES);
    Counter readOperations = metrics.counter(FileIOMetricsContext.READ_OPERATIONS);
    assertThat(readBytes.value()).isEqualTo(DATA.length);
    assertThat(readOperations.value()).isGreaterThan(0);
  }

  @Test
  void getLengthThrowsNotFoundWhenMissing() {
    HTTPInputFile inputFile = httpInputFile(missingUrl, MetricsContext.nullMetrics());

    assertThatThrownBy(inputFile::getLength)
        .isInstanceOf(NotFoundException.class)
        .hasMessageContaining("Location does not exist");
  }

  @Test
  void getLengthThrowsForbiddenWhenForbidden() {
    HTTPInputFile inputFile = httpInputFile(forbiddenUrl, MetricsContext.nullMetrics());

    assertThatThrownBy(inputFile::getLength)
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("Access forbidden");
  }

  @Test
  void readThrowsForbiddenWithoutRetry() {
    HTTPInputFile inputFile = httpInputFile(forbiddenUrl, MetricsContext.nullMetrics());

    assertThatThrownBy(
            () -> {
              try (SeekableInputStream stream = inputFile.newStream()) {
                ((RangeReadable) stream).readFully(0, new byte[16], 0, 16);
              }
            })
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("Access forbidden");
    assertThat(REQUEST_COUNT.get()).isEqualTo(1);
  }

  @Test
  void readRetriesOnServerErrorThenFails() {
    HTTPInputFile inputFile = httpInputFile(serverErrorUrl, MetricsContext.nullMetrics());

    assertThatThrownBy(
            () -> {
              try (SeekableInputStream stream = inputFile.newStream()) {
                ((RangeReadable) stream).readFully(0, new byte[16], 0, 16);
              }
            })
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Transient HTTP");
    assertThat(REQUEST_COUNT.get()).isGreaterThan(1);
  }

  @Test
  void readRetriesOnThrottlingThenFails() {
    // throttling (429) is transient and must be retried, with backoff, rather than failing outright
    HTTPInputFile inputFile = httpInputFile(throttledUrl, MetricsContext.nullMetrics());

    assertThatThrownBy(
            () -> {
              try (SeekableInputStream stream = inputFile.newStream()) {
                ((RangeReadable) stream).readFully(0, new byte[16], 0, 16);
              }
            })
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Transient HTTP 429");
    assertThat(REQUEST_COUNT.get()).isGreaterThan(1);
  }

  @Test
  void readDoesNotRetryOnNonRetryableServerError() {
    // not every 5xx is transient: 501 Not Implemented is terminal and must not be retried
    HTTPInputFile inputFile = httpInputFile(notImplementedUrl, MetricsContext.nullMetrics());

    assertThatThrownBy(
            () -> {
              try (SeekableInputStream stream = inputFile.newStream()) {
                ((RangeReadable) stream).readFully(0, new byte[16], 0, 16);
              }
            })
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Unexpected HTTP 501");
    assertThat(REQUEST_COUNT.get()).isEqualTo(1);
  }

  @Test
  void readDoesNotFollowRedirect() {
    // HttpUrlHelper builds the client with redirect handling disabled, so a 3xx is surfaced as an
    // error rather than followed to its target (which would defeat a host allow-list).
    HttpUrlHelper support = new HttpUrlHelper();
    try {
      InputFile inputFile = support.newInputFile(redirectUrl, MetricsContext.nullMetrics());
      assertThatThrownBy(
              () -> {
                try (SeekableInputStream stream = inputFile.newStream()) {
                  stream.read();
                }
              })
          .isInstanceOf(IOException.class)
          .hasMessageContaining("Unexpected HTTP 302");
      // only the redirect endpoint is hit; the target is never fetched
      assertThat(REQUEST_COUNT.get()).isEqualTo(1);
    } finally {
      support.close();
    }
  }

  @Test
  void existsReturnsTrueWhenPresent() {
    HTTPInputFile inputFile = httpInputFile(url, MetricsContext.nullMetrics());

    assertThat(inputFile.exists()).isTrue();
  }

  @Test
  void existsReturnsFalseWhenMissing() {
    HTTPInputFile inputFile = httpInputFile(missingUrl, MetricsContext.nullMetrics());

    assertThat(inputFile.exists()).isFalse();
  }

  @Test
  void existsReturnsFalseWhenForbidden() {
    HTTPInputFile inputFile = httpInputFile(forbiddenUrl, MetricsContext.nullMetrics());

    assertThat(inputFile.exists()).isFalse();
  }

  /** A {@link MetricsContext} that returns the same counter instance for a given name. */
  private static class CachingMetricsContext implements MetricsContext {
    private final Map<String, org.apache.iceberg.metrics.Counter> counters =
        Maps.newConcurrentMap();

    @Override
    public org.apache.iceberg.metrics.Counter counter(String name, Unit unit) {
      return counters.computeIfAbsent(
          name, ignored -> new DefaultMetricsContext().counter(name, unit));
    }
  }

  private static void handle(HttpExchange exchange) throws IOException {
    REQUEST_COUNT.incrementAndGet();
    String range = exchange.getRequestHeaders().getFirst("Range");
    long total = DATA.length;

    long start;
    long end;
    if (range == null) {
      start = 0;
      end = total - 1;
    } else {
      String spec = range.substring(range.indexOf('=') + 1);
      int dash = spec.indexOf('-');
      String lower = spec.substring(0, dash);
      String upper = spec.substring(dash + 1);
      if (lower.isEmpty()) {
        // suffix range: bytes=-N
        start = Math.max(0, total - Long.parseLong(upper));
        end = total - 1;
      } else {
        start = Long.parseLong(lower);
        end = upper.isEmpty() ? total - 1 : Long.parseLong(upper);
      }
    }

    if (start >= total) {
      exchange.getResponseHeaders().set("Content-Range", "bytes */" + total);
      exchange.sendResponseHeaders(416, -1);
      exchange.close();
      return;
    }

    end = Math.min(end, total - 1);
    int length = (int) (end - start + 1);
    exchange
        .getResponseHeaders()
        .set("Content-Range", String.format(Locale.ROOT, "bytes %d-%d/%d", start, end, total));
    exchange.getResponseHeaders().set("Accept-Ranges", "bytes");
    exchange.sendResponseHeaders(range == null ? 200 : 206, length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(DATA, (int) start, length);
    }
  }

  private static void respondStatus(HttpExchange exchange, int status) throws IOException {
    REQUEST_COUNT.incrementAndGet();
    exchange.sendResponseHeaders(status, -1);
    exchange.close();
  }

  private static void respondRedirect(HttpExchange exchange) throws IOException {
    REQUEST_COUNT.incrementAndGet();
    exchange.getResponseHeaders().set("Location", PATH);
    exchange.sendResponseHeaders(302, -1);
    exchange.close();
  }

  private static HTTPInputFile httpInputFile(String requestUrl, MetricsContext metrics) {
    return new HTTPInputFile(client, "s3://bucket/object", requestUrl, CHUNK_SIZE, metrics);
  }

  private static byte[] randomBytes(int size, long seed) {
    byte[] bytes = new byte[size];
    new Random(seed).nextBytes(bytes);
    return bytes;
  }
}
