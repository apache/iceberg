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

import static org.apache.iceberg.TestBase.FILE_A;
import static org.apache.iceberg.TestBase.SCHEMA;
import static org.apache.iceberg.rest.RequestMatcher.matches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.HTTPRequest.HTTPMethod;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.util.JsonUtil;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class TestRESTCatalogFormatV4 extends TestBaseWithRESTServer {
  private static final ResourcePaths RESOURCE_PATHS =
      ResourcePaths.forCatalogProperties(
          ImmutableMap.of(
              RESTCatalogProperties.NAMESPACE_SEPARATOR,
              RESTCatalogAdapter.NAMESPACE_SEPARATOR_URLENCODED_UTF_8));
  private static final TableIdentifier TBL = TableIdentifier.of(NS, "tbl");
  private static final String TBL_LOCATION = "memory://ns/tbl";

  @Override
  protected String catalogName() {
    return "catalog-format-v4";
  }

  @Test
  void catalogSuppliedTableLocationIsUsedAndNotCommitted() {
    restCatalog.createNamespace(NS);
    restCatalog
        .buildTable(TBL, SCHEMA)
        .withLocation(TBL_LOCATION)
        .withProperty(TableProperties.FORMAT_VERSION, "4")
        .create();

    Mockito.doAnswer(
            invocation -> {
              LoadTableResponse response = (LoadTableResponse) invocation.callRealMethod();
              return LoadTableResponse.builder()
                  .withTableMetadata(withoutLocation(response.tableMetadata()))
                  .withTableLocation(response.tableLocation())
                  .build();
            })
        .when(adapterForRESTServer)
        .execute(
            matches(HTTPMethod.GET, RESOURCE_PATHS.tableV2(TBL)),
            eq(LoadTableResponse.class),
            any(),
            any());

    Table loaded = restCatalog.loadTable(TBL);
    assertThat(loaded.location()).isEqualTo(TBL_LOCATION);

    Mockito.clearInvocations(adapterForRESTServer);
    loaded.newAppend().appendFile(FILE_A).commit();

    ArgumentCaptor<HTTPRequest> requests = ArgumentCaptor.forClass(HTTPRequest.class);
    Mockito.verify(adapterForRESTServer, Mockito.atLeastOnce())
        .execute(requests.capture(), any(), any(), any());
    List<MetadataUpdate> updates =
        requests.getAllValues().stream()
            .map(HTTPRequest::body)
            .filter(UpdateTableRequest.class::isInstance)
            .flatMap(body -> ((UpdateTableRequest) body).updates().stream())
            .collect(Collectors.toList());
    assertThat(updates).isNotEmpty().noneMatch(MetadataUpdate.SetLocation.class::isInstance);
    assertThat(backendCatalog.loadTable(TBL).location()).isEqualTo(TBL_LOCATION);
    assertThat(backendCatalog.loadTable(TBL).currentSnapshot()).isNotNull();
  }

  private static TableMetadata withoutLocation(TableMetadata metadata) {
    ObjectNode json =
        (ObjectNode) JsonUtil.parse(TableMetadataParser.toJson(metadata), node -> node);
    json.remove("location");
    return TableMetadataParser.fromJson(metadata.metadataFileLocation(), json);
  }
}
