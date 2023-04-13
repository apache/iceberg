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
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.rest.requests.ReportMetricsRequest;
import org.apache.iceberg.util.SerializableFunction;
import org.apache.iceberg.util.SerializableMap;
import org.apache.iceberg.util.SerializableSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RESTMetricsReporter implements MetricsReporter {
  private static final Logger LOG = LoggerFactory.getLogger(RESTMetricsReporter.class);

  private transient volatile RESTClient client;
  private final String metricsEndpoint;
  private final SerializableSupplier<Map<String, String>> headers;
  private final SerializableFunction<Map<String, String>, RESTClient> clientBuilder;
  private final SerializableMap<String, String> properties;

  RESTMetricsReporter(
      RESTClient client,
      String metricsEndpoint,
      SerializableSupplier<Map<String, String>> headers,
      SerializableFunction<Map<String, String>, RESTClient> clientBuilder,
      SerializableMap<String, String> properties) {
    this.client = client;
    this.metricsEndpoint = metricsEndpoint;
    this.headers = headers;
    this.clientBuilder = clientBuilder;
    this.properties = properties;
  }

  @Override
  public void report(MetricsReport report) {
    if (null == report) {
      LOG.warn("Received invalid metrics report: null");
      return;
    }

    try {
      client()
          .post(
              metricsEndpoint,
              ReportMetricsRequest.of(report),
              null,
              headers,
              ErrorHandlers.defaultErrorHandler());
    } catch (Exception e) {
      LOG.warn("Failed to report metrics to REST endpoint {}", metricsEndpoint, e);
    }
  }

  private RESTClient client() {
    // lazy init the client in case RESTMetricsReporter was deserialized
    if (null == client) {
      synchronized (this) {
        if (null == client) {
          client = clientBuilder.apply(properties);
        }
      }
    }

    return client;
  }

  Object writeReplace() {
    // fetch the latest headers from the AuthSession and carry them over in a separate supplier so
    // that AuthSession doesn't have to be Serializable
    Map<String, String> authHeaders = headers.get();
    return new RESTMetricsReporter(
        client, metricsEndpoint, () -> authHeaders, clientBuilder, properties);
  }
}
