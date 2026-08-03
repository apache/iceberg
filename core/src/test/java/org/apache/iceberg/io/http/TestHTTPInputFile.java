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
import org.apache.iceberg.io.FileIOMetricsContext;
import org.apache.iceberg.io.IOUtil;
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
 * path, including multi-chunk reads that cross the {@link HTTPInputStream#CHUNK_SIZE} boundary.
 */
class TestHTTPInputFile {

  private static final String PATH = "/object";
  private static final byte[] DATA = randomBytes(HTTPInputStream.CHUNK_SIZE + 1_000, 42L);

  private static HttpServer server;
  private static CloseableHttpClient client;
  private static String url;
  private static final AtomicInteger REQUEST_COUNT = new AtomicInteger();

  @BeforeAll
  static void startServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    server.createContext(PATH, TestHTTPInputFile::handle);
    server.start();
    client = HttpClients.createDefault();
    url = String.format(Locale.ROOT, "http://127.0.0.1:%d%s", server.getAddress().getPort(), PATH);
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
    HTTPInputFile inputFile =
        new HTTPInputFile(client, "s3://bucket/object", url, MetricsContext.nullMetrics());

    assertThat(inputFile.getLength()).isEqualTo(DATA.length);
    assertThat(REQUEST_COUNT.get()).isEqualTo(1);
  }

  @Test
  void getLengthUsesKnownLengthWithoutRequest() {
    HTTPInputFile inputFile =
        new HTTPInputFile(client, "s3://bucket/object", url, 123L, MetricsContext.nullMetrics());

    assertThat(inputFile.getLength()).isEqualTo(123L);
    assertThat(REQUEST_COUNT.get()).isZero();
  }

  @Test
  void readFullyReadsExactRange() throws IOException {
    HTTPInputFile inputFile =
        new HTTPInputFile(client, "s3://bucket/object", url, MetricsContext.nullMetrics());

    byte[] buffer = new byte[2_048];
    try (SeekableInputStream stream = inputFile.newStream()) {
      ((RangeReadable) stream).readFully(1_000, buffer, 0, buffer.length);
    }

    assertThat(buffer).isEqualTo(Arrays.copyOfRange(DATA, 1_000, 1_000 + buffer.length));
  }

  @Test
  void readTailReadsSuffix() throws IOException {
    HTTPInputFile inputFile =
        new HTTPInputFile(client, "s3://bucket/object", url, MetricsContext.nullMetrics());

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
    HTTPInputFile inputFile =
        new HTTPInputFile(client, "s3://bucket/object", url, MetricsContext.nullMetrics());

    byte[] actual = new byte[DATA.length];
    try (SeekableInputStream stream = inputFile.newStream()) {
      IOUtil.readFully(stream, actual, 0, actual.length);
    }

    assertThat(actual).isEqualTo(DATA);
    // reading past a single 4 MB chunk requires at least two range fetches
    assertThat(REQUEST_COUNT.get()).isGreaterThanOrEqualTo(2);
  }

  @Test
  void sequentialReadTracksReadMetrics() throws IOException {
    CachingMetricsContext metrics = new CachingMetricsContext();
    HTTPInputFile inputFile = new HTTPInputFile(client, "s3://bucket/object", url, metrics);

    byte[] actual = new byte[DATA.length];
    try (SeekableInputStream stream = inputFile.newStream()) {
      IOUtil.readFully(stream, actual, 0, actual.length);
    }

    Counter readBytes = metrics.counter(FileIOMetricsContext.READ_BYTES, Unit.BYTES);
    Counter readOperations = metrics.counter(FileIOMetricsContext.READ_OPERATIONS);
    assertThat(readBytes.value()).isEqualTo(DATA.length);
    assertThat(readOperations.value()).isGreaterThan(0);
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

  private static byte[] randomBytes(int size, long seed) {
    byte[] bytes = new byte[size];
    new Random(seed).nextBytes(bytes);
    return bytes;
  }
}
