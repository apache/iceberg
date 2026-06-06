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

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.iceberg.rest.requests.ReportMetricsRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link MetricsReporter} implementation that reports the {@link MetricsReport} to a REST
 * endpoint. This is the default metrics reporter when using {@link RESTCatalog}.
 */
class RESTMetricsReporter implements MetricsReporter {
  private static final Logger LOG = LoggerFactory.getLogger(RESTMetricsReporter.class);

  // A single daemon thread processes reports asynchronously. The unbounded queue retains all
  // pending reports without dropping; callers return immediately after enqueueing.
  //
  // Note: Tasks.range(1).executeWith(executor).run() must NOT be used here — despite submitting
  // work to the executor, it blocks the calling thread in Tasks.waitFor() until the task
  // completes, making report() synchronous and causing thread pile-up in any caller that wraps
  // report() in its own executor pool.
  private static final ExecutorService METRICS_EXECUTOR =
      new ThreadPoolExecutor(
          1,
          1,
          0L,
          TimeUnit.MILLISECONDS,
          new LinkedBlockingQueue<>(),
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat("rest-metrics-reporter-%d")
              .build());

  private final RESTClient client;
  private final String metricsEndpoint;
  private final Supplier<Map<String, String>> headers;

  RESTMetricsReporter(
      RESTClient client, String metricsEndpoint, Supplier<Map<String, String>> headers) {
    this.client = client;
    this.metricsEndpoint = metricsEndpoint;
    this.headers = headers;
  }

  @Override
  public void report(MetricsReport report) {
    if (null == report) {
      LOG.warn("Received invalid metrics report: null");
      return;
    }

    METRICS_EXECUTOR.execute(
        () -> {
          try {
            client.post(
                metricsEndpoint,
                ReportMetricsRequest.of(report),
                null,
                headers,
                ErrorHandlers.defaultErrorHandler());
          } catch (Exception e) {
            LOG.warn("Failed to report metrics to REST endpoint {}", metricsEndpoint, e);
          }
        });
  }
}
