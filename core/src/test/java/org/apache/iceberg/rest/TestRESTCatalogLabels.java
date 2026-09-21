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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.ImmutableFieldLabel;
import org.apache.iceberg.ImmutableLabels;
import org.apache.iceberg.Labels;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SupportsLabels;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies that catalog-provided labels returned on the load-table response are surfaced on the
 * table loaded through {@link RESTCatalog}.
 */
class TestRESTCatalogLabels {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  private static final Namespace NAMESPACE = Namespace.of("ns");
  private static final TableIdentifier IDENTIFIER = TableIdentifier.of(NAMESPACE, "tbl");

  private static final Labels LABELS =
      ImmutableLabels.builder()
          .objectLabels(ImmutableMap.of("owner", "team-a"))
          .addFields(
              ImmutableFieldLabel.builder()
                  .fieldId(1)
                  .labels(ImmutableMap.of("classification", "pii"))
                  .build())
          .build();

  @TempDir private File warehouse;

  private RESTCatalog restCatalog;

  @BeforeEach
  public void createCatalog() {
    InMemoryCatalog backendCatalog = new InMemoryCatalog();
    backendCatalog.initialize(
        "in-memory",
        ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, warehouse.getAbsolutePath()));

    // stand in for a server that enriches every load-table response with catalog-provided labels
    RESTCatalogAdapter labelInjectingAdapter =
        new RESTCatalogAdapter(backendCatalog) {
          @Override
          public <T extends RESTResponse> T handleRequest(
              Route route,
              Map<String, String> vars,
              HTTPRequest request,
              Class<T> responseType,
              Consumer<Map<String, String>> responseHeaders) {
            T response = super.handleRequest(route, vars, request, responseType, responseHeaders);
            if (response instanceof LoadTableResponse loadTableResponse) {
              return castResponse(
                  responseType,
                  LoadTableResponse.builder()
                      .withTableMetadata(loadTableResponse.tableMetadata())
                      .addAllConfig(loadTableResponse.config())
                      .withLabels(LABELS)
                      .build());
            }

            return response;
          }
        };

    this.restCatalog = new RESTCatalog((config) -> labelInjectingAdapter);
    restCatalog.setConf(new Configuration());
    restCatalog.initialize(
        "prod",
        ImmutableMap.of(
            CatalogProperties.URI,
            "http://localhost",
            CatalogProperties.FILE_IO_IMPL,
            "org.apache.iceberg.inmemory.InMemoryFileIO"));
    restCatalog.createNamespace(NAMESPACE);
  }

  @AfterEach
  public void closeCatalog() throws Exception {
    if (restCatalog != null) {
      restCatalog.close();
    }
  }

  @Test
  void loadTableSurfacesCatalogLabels() {
    restCatalog.createTable(IDENTIFIER, SCHEMA);

    Table table = restCatalog.loadTable(IDENTIFIER);

    assertThat(table).isInstanceOf(SupportsLabels.class);
    Labels labels = ((SupportsLabels) table).labels();
    assertThat(labels.objectLabels()).containsEntry("owner", "team-a");
    assertThat(labels.fields()).hasSize(1);
    assertThat(labels.fields().get(0).fieldId()).isEqualTo(1);
    assertThat(labels.fields().get(0).labels()).containsEntry("classification", "pii");
  }
}
