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
package org.apache.iceberg.gcp.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import com.google.cloud.ServiceOptions;
import com.google.cloud.monitoring.v3.MetricServiceClient;
import com.google.cloud.monitoring.v3.MetricServiceClient.ListTimeSeriesPagedResponse;
import com.google.monitoring.v3.ListTimeSeriesRequest;
import com.google.monitoring.v3.ProjectName;
import com.google.monitoring.v3.TimeInterval;
import com.google.monitoring.v3.TimeSeries;
import com.google.protobuf.util.Timestamps;
import java.time.Duration;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.RESTCatalog;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

/**
 * Integration test that exercises {@link CloudMonitoringMetricsReporter} end-to-end against a real
 * Cloud Monitoring project and a configured catalog. Skipped automatically unless the required
 * properties below are provided.
 *
 * <p>Required configuration (system properties or environment variables):
 *
 * <ul>
 *   <li>{@code iceberg.gcp.project} (or default GCP credentials project)
 *   <li>{@code iceberg.gcp.it.catalog-uri} — REST catalog URI
 *   <li>{@code iceberg.gcp.it.warehouse} — warehouse location (e.g. {@code gs://...})
 *   <li>{@code iceberg.gcp.it.namespace} and {@code iceberg.gcp.it.table} — table to scan
 * </ul>
 *
 * Optional: {@code iceberg.gcp.it.catalog-properties} as a comma-separated list of {@code k=v}
 * pairs for catalog-specific properties (e.g. headers, scan-planning-mode).
 */
public class TestCloudMonitoringMetricsReporterIntegration {

  private static final String SYS_PROJECT = "iceberg.gcp.project";
  private static final String SYS_CATALOG_URI = "iceberg.gcp.it.catalog-uri";
  private static final String SYS_WAREHOUSE = "iceberg.gcp.it.warehouse";
  private static final String SYS_NAMESPACE = "iceberg.gcp.it.namespace";
  private static final String SYS_TABLE = "iceberg.gcp.it.table";
  private static final String SYS_CATALOG_PROPS = "iceberg.gcp.it.catalog-properties";

  @Test
  void testMetricsReporting() throws Exception {
    String projectId = System.getProperty(SYS_PROJECT, ServiceOptions.getDefaultProjectId());
    String catalogUri = System.getProperty(SYS_CATALOG_URI);
    String warehouse = System.getProperty(SYS_WAREHOUSE);
    String namespace = System.getProperty(SYS_NAMESPACE);
    String tableName = System.getProperty(SYS_TABLE);
    assumeThat(projectId).as("GCP project ID is not configured").isNotNull();
    assumeThat(catalogUri).as("%s is not set", SYS_CATALOG_URI).isNotNull();
    assumeThat(warehouse).as("%s is not set", SYS_WAREHOUSE).isNotNull();
    assumeThat(namespace).as("%s is not set", SYS_NAMESPACE).isNotNull();
    assumeThat(tableName).as("%s is not set", SYS_TABLE).isNotNull();

    Map<String, String> properties = Maps.newHashMap();
    properties.put(CatalogProperties.URI, catalogUri);
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);
    properties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.gcp.gcs.GCSFileIO");
    properties.put(
        CatalogProperties.METRICS_REPORTER_IMPL, CloudMonitoringMetricsReporter.class.getName());
    properties.put(GCPProperties.GCP_MONITORING_PROJECT_ID, projectId);
    properties.putAll(parseExtraProperties(System.getProperty(SYS_CATALOG_PROPS, "")));

    long startMillis = System.currentTimeMillis();
    String catalogId = "iceberg_cloud_monitoring_it";

    RESTCatalog catalog = new RESTCatalog();
    catalog.initialize(catalogId, properties);

    Table table = catalog.loadTable(TableIdentifier.of(namespace, tableName));
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      tasks.forEach(t -> {});
    }

    String prefix =
        properties.getOrDefault(
            GCPProperties.GCP_MONITORING_METRIC_PREFIX,
            GCPProperties.GCP_MONITORING_METRIC_PREFIX_DEFAULT);
    String filter =
        String.format(
            "metric.type=\"%s/scan_metrics\" AND metric.label.\"metric_name\"=\"scan_operations\"",
            prefix);

    try (MetricServiceClient client = MetricServiceClient.create()) {
      Awaitility.await("scan_operations metric appears in Cloud Monitoring")
          .atMost(Duration.ofMinutes(3))
          .pollInterval(Duration.ofSeconds(5))
          .until(() -> findScanOperations(client, projectId, filter, startMillis));
    }
  }

  private static boolean findScanOperations(
      MetricServiceClient client, String projectId, String filter, long startMillis) {
    TimeInterval interval =
        TimeInterval.newBuilder()
            .setStartTime(Timestamps.fromMillis(startMillis))
            .setEndTime(Timestamps.fromMillis(System.currentTimeMillis()))
            .build();
    ListTimeSeriesRequest request =
        ListTimeSeriesRequest.newBuilder()
            .setName(ProjectName.of(projectId).toString())
            .setFilter(filter)
            .setInterval(interval)
            .build();
    ListTimeSeriesPagedResponse response = client.listTimeSeries(request);
    for (TimeSeries ts : response.iterateAll()) {
      assertThat(ts.getMetric().getLabelsMap()).containsKey("table");
      assertThat(ts.getMetric().getLabelsMap()).containsKey("catalog");
      assertThat(ts.getMetric().getLabelsMap()).doesNotContainKeys("filter", "queried_field_ids");
      return true;
    }
    return false;
  }

  private static Map<String, String> parseExtraProperties(String raw) {
    Map<String, String> out = Maps.newHashMap();
    if (raw == null || raw.isBlank()) {
      return out;
    }
    for (String pair : raw.split(",")) {
      int eq = pair.indexOf('=');
      if (eq > 0) {
        out.put(pair.substring(0, eq).trim(), pair.substring(eq + 1).trim());
      }
    }
    return out;
  }
}
