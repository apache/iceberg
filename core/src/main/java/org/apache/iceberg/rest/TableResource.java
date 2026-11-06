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
import java.util.Set;
import java.util.function.BiFunction;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.rest.credentials.Credential;

/**
 * Encapsulates table-level resource paths and endpoint support for REST operations. Constructed
 * from catalog-level ResourcePaths and table-specific identifiers.
 */
class TableResource {
  private final ResourcePaths resourcePaths;
  private final TableIdentifier tableIdentifier;
  private final Set<Endpoint> supportedEndpoints;
  private final long pollTimeoutMs;
  private final BiFunction<List<Credential>, String, FileIO> fileIOFactory;

  TableResource(
      ResourcePaths resourcePaths,
      TableIdentifier tableIdentifier,
      Set<Endpoint> supportedEndpoints,
      long pollTimeoutMs,
      BiFunction<List<Credential>, String, FileIO> fileIOFactory) {
    this.resourcePaths = resourcePaths;
    this.tableIdentifier = tableIdentifier;
    this.supportedEndpoints = supportedEndpoints;
    this.pollTimeoutMs = pollTimeoutMs;
    this.fileIOFactory = fileIOFactory;
  }

  String planPath() {
    return resourcePaths.planTableScan(tableIdentifier);
  }

  String planPath(String planId) {
    return resourcePaths.plan(tableIdentifier, planId);
  }

  String fetchPath() {
    return resourcePaths.fetchScanTasks(tableIdentifier);
  }

  boolean supportsAsync() {
    return supportedEndpoints.contains(Endpoint.V1_FETCH_TABLE_SCAN_PLAN);
  }

  boolean supportsCancel() {
    return supportedEndpoints.contains(Endpoint.V1_CANCEL_TABLE_SCAN_PLAN);
  }

  long pollTimeoutMs() {
    return pollTimeoutMs;
  }

  FileIO createFileIO(List<Credential> credentials, String planId) {
    return fileIOFactory.apply(credentials, planId);
  }
}
