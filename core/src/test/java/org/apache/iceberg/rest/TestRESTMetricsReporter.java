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
package org.apache.iceberg.rest;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.metrics.DefaultMetricsContext;
import org.apache.iceberg.metrics.ImmutableScanReport;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.ScanMetrics;
import org.apache.iceberg.metrics.ScanMetricsResult;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.junit.jupiter.api.Test;

class TestRESTMetricsReporter {

  @Test
  void reportIsNonBlocking() throws InterruptedException {
    CountDownLatch postStarted = new CountDownLatch(1);
    CountDownLatch postRelease = new CountDownLatch(1);

    // A slow client that blocks until explicitly released.
    RESTClient slowClient =
        new NoOpRESTClient() {
          @Override
          public <T extends RESTResponse> T post(
              String path,
              RESTRequest body,
              Class<T> responseType,
              Map<String, String> headers,
              Consumer<ErrorResponse> errorHandler) {
            postStarted.countDown();
            try {
              postRelease.await();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            return null;
          }
        };

    RESTMetricsReporter reporter =
        new RESTMetricsReporter(slowClient, "http://endpoint/metrics", Map::of);

    long start = System.currentTimeMillis();
    reporter.report(buildReport());
    long elapsed = System.currentTimeMillis() - start;

    // report() must return immediately — well before the simulated 5-second POST delay.
    assertThat(elapsed).isLessThan(2_000);

    // The background HTTP call eventually starts.
    assertThat(postStarted.await(5, TimeUnit.SECONDS)).isTrue();
    postRelease.countDown();
  }

  @Test
  void reportSendsHttpPost() throws InterruptedException {
    AtomicInteger postCount = new AtomicInteger(0);
    CountDownLatch postDone = new CountDownLatch(1);

    RESTClient client =
        new NoOpRESTClient() {
          @Override
          public <T extends RESTResponse> T post(
              String path,
              RESTRequest body,
              Class<T> responseType,
              Map<String, String> headers,
              Consumer<ErrorResponse> errorHandler) {
            postCount.incrementAndGet();
            postDone.countDown();
            return null;
          }
        };

    RESTMetricsReporter reporter =
        new RESTMetricsReporter(client, "http://endpoint/metrics", Map::of);
    reporter.report(buildReport());

    assertThat(postDone.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(postCount.get()).isEqualTo(1);
  }

  @Test
  void reportNullIsIgnored() throws InterruptedException {
    AtomicInteger postCount = new AtomicInteger(0);

    RESTClient client =
        new NoOpRESTClient() {
          @Override
          public <T extends RESTResponse> T post(
              String path,
              RESTRequest body,
              Class<T> responseType,
              Map<String, String> headers,
              Consumer<ErrorResponse> errorHandler) {
            postCount.incrementAndGet();
            return null;
          }
        };

    RESTMetricsReporter reporter =
        new RESTMetricsReporter(client, "http://endpoint/metrics", Map::of);
    reporter.report(null);

    // Give a moment to ensure no async call is triggered.
    Thread.sleep(200);
    assertThat(postCount.get()).isEqualTo(0);
  }

  private static MetricsReport buildReport() {
    return ImmutableScanReport.builder()
        .tableName("test-table")
        .snapshotId(1L)
        .filter(Expressions.alwaysTrue())
        .schemaId(1)
        .scanMetrics(ScanMetricsResult.fromScanMetrics(ScanMetrics.noop()))
        .build();
  }

  /** Minimal RESTClient that throws UnsupportedOperationException for every method. */
  private abstract static class NoOpRESTClient implements RESTClient {
    @Override
    public void head(
        String path, Map<String, String> headers, Consumer<ErrorResponse> errorHandler) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T extends RESTResponse> T delete(
        String path,
        Class<T> responseType,
        Map<String, String> headers,
        Consumer<ErrorResponse> errorHandler) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T extends RESTResponse> T get(
        String path,
        Map<String, String> queryParams,
        Class<T> responseType,
        Map<String, String> headers,
        Consumer<ErrorResponse> errorHandler) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T extends RESTResponse> T post(
        String path,
        RESTRequest body,
        Class<T> responseType,
        Map<String, String> headers,
        Consumer<ErrorResponse> errorHandler) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T extends RESTResponse> T postForm(
        String path,
        Map<String, String> formData,
        Class<T> responseType,
        Map<String, String> headers,
        Consumer<ErrorResponse> errorHandler) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {}
  }
}
