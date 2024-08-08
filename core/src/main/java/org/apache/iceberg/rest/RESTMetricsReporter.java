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
import java.util.function.Supplier;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.rest.requests.ReportMetricsRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link MetricsReporter} implementation that reports the {@link MetricsReport} to a REST
 * endpoint. This is the default metrics reporter when using {@link RESTCatalog}.
 */
class RESTMetricsReporter implements MetricsReporter {
  private static final Logger LOG = LoggerFactory.getLogger(RESTMetricsReporter.class);

  private final RESTClient client;
  private final String metricsEndpoint;
  private final Supplier<Map<String, String>> headers;
  private final Map<String, String> queryParams;

  RESTMetricsReporter(
      RESTClient client,
      String metricsEndpoint,
      Supplier<Map<String, String>> headers,
      Map<String, String> queryParams) {
    this.client = client;
    this.metricsEndpoint = metricsEndpoint;
    this.headers = headers;
    this.queryParams = queryParams;
  }

  @Override
  public void report(MetricsReport report) {
    if (null == report) {
      LOG.warn("Received invalid metrics report: null");
      return;
    }

    try {
      client.post(
          metricsEndpoint,
          ReportMetricsRequest.of(report),
          queryParams,
          null,
          headers,
          ErrorHandlers.defaultErrorHandler());
    } catch (Exception e) {
      LOG.warn("Failed to report metrics to REST endpoint {}", metricsEndpoint, e);
    }
  }
}
