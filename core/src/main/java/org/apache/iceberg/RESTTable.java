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
package org.apache.iceberg;

import java.util.Map;
import java.util.function.Supplier;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.rest.ResourcePaths;

public class RESTTable extends BaseTable {
  private final RESTClient client;
  private final String path;
  private final Supplier<Map<String, String>> headers;
  private final boolean tableScanViaRestEnabled;
  private final MetricsReporter reporter;

  private final ResourcePaths resourcePaths;
  private final TableIdentifier tableIdentifier;

  public RESTTable(
      TableOperations ops,
      String name,
      MetricsReporter reporter,
      RESTClient client,
      String path,
      Supplier<Map<String, String>> headers,
      boolean tableScanViaRestEnabled,
      TableIdentifier tableIdentifier,
      ResourcePaths resourcePaths) {
    super(ops, name, reporter);
    this.reporter = reporter;
    this.client = client;
    this.headers = headers;
    this.path = path;
    this.tableScanViaRestEnabled = tableScanViaRestEnabled;
    this.tableIdentifier = tableIdentifier;
    this.resourcePaths = resourcePaths;
  }

  @Override
  public TableScan newScan() {

    if (tableScanViaRestEnabled
        && Boolean.parseBoolean(
            this.properties().get(RESTSessionCatalog.REST_TABLE_SCAN_ENABLED))) {
      return new RESTTableScan(
          this,
          schema(),
          ImmutableTableScanContext.builder().metricsReporter(reporter).build(),
          client,
          path,
          headers,
          operations(),
          tableIdentifier,
          resourcePaths);
    }
    return super.newScan();
  }
}
