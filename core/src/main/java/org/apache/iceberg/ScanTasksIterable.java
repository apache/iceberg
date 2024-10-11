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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.rest.ErrorHandlers;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.ResourcePaths;
import org.apache.iceberg.rest.requests.FetchScanTasksRequest;
import org.apache.iceberg.rest.responses.FetchScanTasksResponse;

public class ScanTasksIterable implements CloseableIterable<FileScanTask> {
  private final RESTClient client;
  private final ResourcePaths resourcePaths;
  private final TableIdentifier tableIdentifier;
  private final Supplier<Map<String, String>> headers;
  private final List<String> planTasks;
  private final List<FileScanTask> fileScanTasks;

  public ScanTasksIterable(
      List<String> planTasks,
      List<FileScanTask> fileScanTasks,
      RESTClient client,
      ResourcePaths resourcePaths,
      TableIdentifier tableIdentifier,
      Supplier<Map<String, String>> headers) {
    this.planTasks = planTasks;
    this.fileScanTasks = fileScanTasks;
    this.client = client;
    this.resourcePaths = resourcePaths;
    this.tableIdentifier = tableIdentifier;
    this.headers = headers;
  }

  @Override
  public CloseableIterator<FileScanTask> iterator() {
    return new ScanTasksIterator(
        planTasks, fileScanTasks, client, resourcePaths, tableIdentifier, headers);
  }

  @Override
  public void close() throws IOException {}

  private static class ScanTasksIterator implements CloseableIterator<FileScanTask> {
    private final RESTClient client;
    private final ResourcePaths resourcePaths;
    private final TableIdentifier tableIdentifier;
    private final Supplier<Map<String, String>> headers;
    private List<String> planTasks;
    private List<FileScanTask> fileScanTasks;

    ScanTasksIterator(
        List<String> planTasks,
        List<FileScanTask> fileScanTasks,
        RESTClient client,
        ResourcePaths resourcePaths,
        TableIdentifier tableIdentifier,
        Supplier<Map<String, String>> headers) {
      this.client = client;
      this.resourcePaths = resourcePaths;
      this.tableIdentifier = tableIdentifier;
      this.headers = headers;
      this.planTasks = planTasks != null ? planTasks : new ArrayList<String>();
      this.fileScanTasks = fileScanTasks != null ? fileScanTasks : new ArrayList<FileScanTask>();
    }

    @Override
    public boolean hasNext() {
      if (!fileScanTasks.isEmpty()) {
        // Have file scan tasks so continue to consume
        return true;
      }
      // Out of file scan tasks, so need to now fetch more from each planTask
      // Service can send back more planTasks which acts as pagination
      if (!planTasks.isEmpty()) {
        executeFetchScanTasks(planTasks.remove(0));
        // Make another hasNext() call, as more planTasks and fileScanTasks have been fetched
        return hasNext();
      }
      // we have no file scan tasks left to consume, and planTasks are exhausted
      // so means we are finished
      return false;
    }

    @Override
    public FileScanTask next() {
      return fileScanTasks.remove(0);
    }

    private void executeFetchScanTasks(String planTask) {
      FetchScanTasksRequest fetchScanTasksRequest = new FetchScanTasksRequest(planTask);
      FetchScanTasksResponse response =
          client.post(
              resourcePaths.fetchScanTasks(tableIdentifier),
              fetchScanTasksRequest,
              FetchScanTasksResponse.class,
              headers,
              ErrorHandlers.defaultErrorHandler());
      fileScanTasks.addAll(response.fileScanTasks());
      planTasks.addAll(response.planTasks());
    }

    @Override
    public void close() throws IOException {}
  }
}
