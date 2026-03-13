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

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.BatchScan;
import org.apache.iceberg.BatchScanAdapter;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.ImmutableTableScanContext;
import org.apache.iceberg.SupportsDistributedScanPlanning;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.credentials.Credential;
import org.apache.iceberg.util.PropertyUtil;

class RESTTable extends BaseTable implements SupportsDistributedScanPlanning {
  private static final String DEFAULT_FILE_IO_IMPL = "org.apache.iceberg.io.ResolvingFileIO";

  private final RESTClient client;
  private final Supplier<Map<String, String>> headers;
  private final MetricsReporter reporter;
  private final ResourcePaths resourcePaths;
  private final TableIdentifier tableIdentifier;
  private final boolean supportsAsync;
  private final boolean supportsCancel;
  private final boolean supportsFetchTasks;
  private final Map<String, String> catalogProperties;
  private final Object hadoopConf;

  RESTTable(
      TableOperations ops,
      String name,
      MetricsReporter reporter,
      RESTClient client,
      Supplier<Map<String, String>> headers,
      TableIdentifier tableIdentifier,
      ResourcePaths resourcePaths,
      boolean supportsAsync,
      boolean supportsCancel,
      boolean supportsFetchTasks,
      Map<String, String> catalogProperties,
      Object hadoopConf) {
    super(ops, name, reporter);
    this.reporter = reporter;
    this.client = client;
    this.headers = headers;
    this.tableIdentifier = tableIdentifier;
    this.resourcePaths = resourcePaths;
    this.supportsAsync = supportsAsync;
    this.supportsCancel = supportsCancel;
    this.supportsFetchTasks = supportsFetchTasks;
    this.catalogProperties = catalogProperties;
    this.hadoopConf = hadoopConf;
  }

  @Override
  public TableScan newScan() {
    BiFunction<List<Credential>, String, FileIO> fileIOFactory =
        (credentials, scanPlanId) -> {
          Map<String, String> props =
              scanPlanId != null
                  ? ImmutableMap.<String, String>builder()
                      .putAll(catalogProperties)
                      .put(RESTCatalogProperties.REST_SCAN_PLAN_ID, scanPlanId)
                      .buildKeepingLast()
                  : catalogProperties;
          List<StorageCredential> storageCredentials =
              credentials.stream()
                  .map(c -> StorageCredential.create(c.prefix(), c.config()))
                  .collect(Collectors.toList());
          return CatalogUtil.loadFileIO(
              catalogProperties.getOrDefault(CatalogProperties.FILE_IO_IMPL, DEFAULT_FILE_IO_IMPL),
              props,
              hadoopConf,
              storageCredentials);
        };

    RESTScanContext scanContext =
        new RESTScanContext(
            resourcePaths,
            tableIdentifier,
            supportsAsync,
            supportsCancel,
            supportsFetchTasks,
            PropertyUtil.propertyAsLong(
                catalogProperties,
                RESTCatalogProperties.REST_SCAN_PLANNING_POLL_TIMEOUT_MS,
                RESTCatalogProperties.REST_SCAN_PLANNING_POLL_TIMEOUT_MS_DEFAULT),
            fileIOFactory);

    return new RESTTableScan(
        this,
        schema(),
        ImmutableTableScanContext.builder().metricsReporter(reporter).build(),
        client,
        headers,
        scanContext);
  }

  @Override
  public BatchScan newBatchScan() {
    return new BatchScanAdapter(newScan());
  }

  @Override
  public boolean allowDistributedPlanning() {
    return false;
  }
}
