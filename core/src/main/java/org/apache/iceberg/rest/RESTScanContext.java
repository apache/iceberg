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
import java.util.function.BiFunction;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.rest.credentials.Credential;

/** Encapsulates REST scan planning context passed from the catalog to the scan. */
class RESTScanContext {
  private final ResourcePaths resourcePaths;
  private final TableIdentifier tableIdentifier;
  private final boolean supportsAsync;
  private final boolean supportsCancel;
  private final boolean supportsFetchTasks;
  private final long pollTimeoutMs;
  private final BiFunction<List<Credential>, String, FileIO> fileIOFactory;

  RESTScanContext(
      ResourcePaths resourcePaths,
      TableIdentifier tableIdentifier,
      boolean supportsAsync,
      boolean supportsCancel,
      boolean supportsFetchTasks,
      long pollTimeoutMs,
      BiFunction<List<Credential>, String, FileIO> fileIOFactory) {
    this.resourcePaths = resourcePaths;
    this.tableIdentifier = tableIdentifier;
    this.supportsAsync = supportsAsync;
    this.supportsCancel = supportsCancel;
    this.supportsFetchTasks = supportsFetchTasks;
    this.pollTimeoutMs = pollTimeoutMs;
    this.fileIOFactory = fileIOFactory;
  }

  String planTableScanPath() {
    return resourcePaths.planTableScan(tableIdentifier);
  }

  String planPath(String planId) {
    return resourcePaths.plan(tableIdentifier, planId);
  }

  String fetchScanTasksPath() {
    return resourcePaths.fetchScanTasks(tableIdentifier);
  }

  boolean supportsAsync() {
    return supportsAsync;
  }

  boolean supportsCancel() {
    return supportsCancel;
  }

  boolean supportsFetchTasks() {
    return supportsFetchTasks;
  }

  long pollTimeoutMs() {
    return pollTimeoutMs;
  }

  FileIO createFileIO(List<Credential> credentials, String planId) {
    return fileIOFactory.apply(credentials, planId);
  }
}
