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
package org.apache.iceberg.flink.source;

import static org.apache.iceberg.TestBase.FILE_A;
import static org.apache.iceberg.TestBase.SCHEMA;
import static org.apache.iceberg.TestBase.SPEC;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.Endpoint;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.ImmutableHTTPRequest;
import org.apache.iceberg.rest.PlanStatus;
import org.apache.iceberg.rest.RESTCatalogAdapter;
import org.apache.iceberg.rest.RESTCatalogProperties;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.TestBaseWithRESTServer;
import org.apache.iceberg.rest.credentials.ImmutableCredential;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.PlanTableScanResponse;
import org.apache.iceberg.util.ThreadPools;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestRemoteScanPlanning extends TestBaseWithRESTServer {

  @Override
  protected String catalogName() {
    return "flink-with-remote-scan-planning";
  }

  @Override
  protected RESTCatalogAdapter createAdapterForServer() {
    return Mockito.spy(
        new RESTCatalogAdapter(backendCatalog) {
          @SuppressWarnings("unchecked")
          @Override
          public <T extends RESTResponse> T execute(
              HTTPRequest request,
              Class<T> responseType,
              Consumer<ErrorResponse> errorHandler,
              Consumer<Map<String, String>> responseHeaders) {
            Object body = roundTripSerialize(request.body(), "request");
            HTTPRequest req = ImmutableHTTPRequest.builder().from(request).body(body).build();
            T restResponse = super.execute(req, responseType, errorHandler, responseHeaders);
            if (restResponse instanceof ConfigResponse response) {
              restResponse =
                  (T)
                      ConfigResponse.builder()
                          .withEndpoints(
                              ImmutableList.<Endpoint>builder()
                                  .addAll(response.endpoints())
                                  .add(Endpoint.V1_SUBMIT_TABLE_SCAN_PLAN)
                                  .build())
                          .withOverrides(
                              ImmutableMap.of(
                                  RESTCatalogProperties.REST_SCAN_PLANNING_ENABLED, "true"))
                          .build();
            } else if (restResponse instanceof PlanTableScanResponse response
                && PlanStatus.COMPLETED == response.planStatus()) {
              return (T)
                  PlanTableScanResponse.builder()
                      .withPlanStatus(response.planStatus())
                      .withPlanId(response.planId())
                      .withFileScanTasks(response.fileScanTasks())
                      .withSpecsById(response.specsById())
                      .withCredentials(
                          ImmutableList.of(
                              ImmutableCredential.builder()
                                  .prefix("gcs")
                                  .putConfig("gcs.oauth2.token", "dummyToken")
                                  .build()))
                      .build();
            }
            return roundTripSerialize(restResponse, "response");
          }
        });
  }

  @Test
  public void fileIOIsPropagated() {
    restCatalog.createNamespace(NS);
    Table table =
        restCatalog
            .buildTable(TableIdentifier.of(NS, "file_io_propagation"), SCHEMA)
            .withPartitionSpec(SPEC)
            .create();
    table.newAppend().appendFile(FILE_A).commit();

    assertThat(table.io().properties()).doesNotContainKey(RESTCatalogProperties.REST_SCAN_PLAN_ID);

    ScanContext scanContext = ScanContext.builder().project(table.schema()).build();
    List<IcebergSourceSplit> splits =
        FlinkSplitPlanner.planIcebergSourceSplits(table, scanContext, ThreadPools.getWorkerPool());

    assertThat(table.io().properties()).doesNotContainKey(RESTCatalogProperties.REST_SCAN_PLAN_ID);
    assertThat(splits)
        .isNotEmpty()
        .anySatisfy(
            split -> {
              assertThat(split.fileIO()).isNotNull();
              assertThat(split.fileIO().properties())
                  .containsKey(RESTCatalogProperties.REST_SCAN_PLAN_ID);
            });
  }
}
