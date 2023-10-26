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
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.ExpressionParser;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.ErrorHandlers;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.ResourcePaths;
import org.apache.iceberg.rest.requests.CreateScanRequest;
import org.apache.iceberg.rest.responses.CreateScanResponse;
import org.apache.iceberg.rest.responses.GetScanTasksResponse;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RESTTableScan extends DataTableScan {

  private static final Logger LOG = LoggerFactory.getLogger(RESTTableScan.class);
  private final RESTClient client;
  private final String path;
  private final Supplier<Map<String, String>> headers;
  private final TableOperations operations;
  private final Table table;
  private final Schema schema;
  private final TableScanContext context;
  private final ResourcePaths resourcePaths;
  private final TableIdentifier tableIdentifier;

  public RESTTableScan(
      Table table,
      Schema schema,
      TableScanContext context,
      RESTClient client,
      String path,
      Supplier<Map<String, String>> headers,
      TableOperations operations,
      TableIdentifier tableIdentifier,
      ResourcePaths resourcePaths) {
    super(table, schema, context);
    this.context = context;
    this.table = table;
    this.schema = schema;
    this.client = client;
    this.headers = headers;
    this.path = path;
    this.operations = operations;
    this.tableIdentifier = tableIdentifier;
    this.resourcePaths = resourcePaths;
  }

  @Override
  public CloseableIterable<FileScanTask> planFiles() {
    String filterString = ExpressionParser.toJson(filter());

    CreateScanRequest request =
        CreateScanRequest.builder().withSelect(columnNames()).withFilter(filterString).build();

    CreateScanResponse createScanResponse =
        client.post(
            resourcePaths.createScan(tableIdentifier),
            request,
            CreateScanResponse.class,
            headers,
            ErrorHandlers.defaultErrorHandler());

    return new RESTScanTasks(createScanResponse.scan(), createScanResponse.shards());
  }

  @Override
  protected TableScan newRefinedScan(
      Table refinedTable, Schema refinedSchema, TableScanContext refinedContext) {
    return new RESTTableScan(
        refinedTable,
        refinedSchema,
        refinedContext,
        client,
        path,
        headers,
        operations,
        tableIdentifier,
        resourcePaths);
  }

  private List<String> columnNames() {
    List<String> columnNames =
        schema.columns().stream().map(Types.NestedField::name).collect(Collectors.toList());
    return columnNames;
  }

  public class RESTScanTasks implements CloseableIterable<FileScanTask> {
    private String scanID;
    private List<String> shardsList;
    private List<FileScanTask> fileScanTaskList;

    private String currentShard;
    private String nextToken;
    private Map<String, String> queryParams;

    public RESTScanTasks(String scanID, List<String> shards) {
      this.scanID = scanID;
      this.shardsList = Lists.newArrayList(shards);
      this.fileScanTaskList = Lists.newArrayList();
      this.currentShard = null;
      this.nextToken = null;
      this.queryParams = Maps.newHashMap();
    }

    @Override
    public CloseableIterator<FileScanTask> iterator() {

      return new CloseableIterator<FileScanTask>() {

        @Override
        public void close() {}

        @Override
        public boolean hasNext() {
          if (fileScanTaskList.isEmpty()) {
            if (shardsList.isEmpty()) {
              // we have no fileScanTasks in our scan tasks list
              // and we have no shards to get more fileScanTasks so done
              return false;
            }
            if (currentShard == null) {
              // get a shard so we can start consuming fileScanTasks
              currentShard = shardsList.remove(0);
            }
            // retrive fileScanTasks for this shard
            getNextFileScanTasks();
          }
          return true;
        }

        @Override
        public FileScanTask next() {
          return fileScanTaskList.remove(0);
        }
      };
    }

    public void getNextFileScanTasks() {
      queryParams.put("shard", currentShard);
      if (nextToken != null) {
        queryParams.put("next", nextToken);
      }
      GetScanTasksResponse getScanTasksResponse =
          client.get(
              resourcePaths.getScanTasks(tableIdentifier, scanID),
              queryParams,
              GetScanTasksResponse.class,
              headers,
              ErrorHandlers.defaultErrorHandler());

      if (!getScanTasksResponse.fileScanTasks().isEmpty()) {
        fileScanTaskList.addAll(getScanTasksResponse.fileScanTasks());
      }

      nextToken = getScanTasksResponse.nextToken();
      if (nextToken == null) {
        // this means that we have consumed all fileScanTasks for this shard
        // so set currentShard to null
        // this allows us to get the next shard in hasNext
        currentShard = null;
      }
    }

    @Override
    public void close() throws IOException {}
  }
}
