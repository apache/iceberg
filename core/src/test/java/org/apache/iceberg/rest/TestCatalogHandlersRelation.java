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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.CatalogObjectType;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.rest.RESTCatalogProperties.SnapshotMode;
import org.apache.iceberg.rest.requests.BatchLoadRelationRequestItem;
import org.apache.iceberg.rest.requests.BatchLoadRelationsRequest;
import org.apache.iceberg.rest.responses.BatchLoadRelationResultItem;
import org.apache.iceberg.rest.responses.BatchLoadRelationsResponse;
import org.apache.iceberg.rest.responses.LoadRelationResponse;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.ViewBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestCatalogHandlersRelation {

  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "x", Types.LongType.get()));
  private static final Namespace NS = Namespace.of("ns");
  private static final TableIdentifier TABLE_IDENT = TableIdentifier.of(NS, "tbl");
  private static final TableIdentifier VIEW_IDENT = TableIdentifier.of(NS, "vw");
  private static final TableIdentifier MISSING_IDENT = TableIdentifier.of(NS, "missing");

  @TempDir File tempDir;

  private InMemoryCatalog catalog;

  @BeforeEach
  void before() throws IOException {
    catalog = new InMemoryCatalog();
    catalog.initialize("test", java.util.Map.of("warehouse", tempDir.toURI().toString()));
    catalog.createNamespace(NS);
    catalog.createTable(TABLE_IDENT, SCHEMA);

    ViewBuilder viewBuilder = catalog.buildView(VIEW_IDENT);
    viewBuilder.withSchema(SCHEMA).withDefaultNamespace(NS).withQuery("spark", "SELECT 1").create();
  }

  @Test
  void loadRelationTable() {
    LoadRelationResponse response =
        CatalogHandlers.loadRelation(catalog, catalog, TABLE_IDENT, SnapshotMode.ALL);
    assertThat(response.objectType()).isEqualTo(CatalogObjectType.TABLE);
    assertThat(response.tableResponse()).isNotNull();
    assertThat(response.tableResponse().tableMetadata().schema().columns()).hasSize(1);
    assertThat(response.viewResponse()).isNull();
  }

  @Test
  void loadRelationView() {
    LoadRelationResponse response =
        CatalogHandlers.loadRelation(catalog, catalog, VIEW_IDENT, SnapshotMode.ALL);
    assertThat(response.objectType()).isEqualTo(CatalogObjectType.VIEW);
    assertThat(response.viewResponse()).isNotNull();
    assertThat(response.tableResponse()).isNull();
  }

  @Test
  void loadRelationNotFound() {
    assertThatThrownBy(
            () -> CatalogHandlers.loadRelation(catalog, catalog, MISSING_IDENT, SnapshotMode.ALL))
        .isInstanceOf(NoSuchTableException.class)
        .hasMessageContaining("missing");
  }

  @Test
  void batchLoadRelationsMixed() {
    BatchLoadRelationsRequest request =
        BatchLoadRelationsRequest.builder()
            .addIdentifier(
                BatchLoadRelationRequestItem.builder().withIdentifier(TABLE_IDENT).build())
            .addIdentifier(
                BatchLoadRelationRequestItem.builder().withIdentifier(VIEW_IDENT).build())
            .addIdentifier(
                BatchLoadRelationRequestItem.builder().withIdentifier(MISSING_IDENT).build())
            .build();

    BatchLoadRelationsResponse response =
        CatalogHandlers.batchLoadRelations(catalog, catalog, request);

    assertThat(response.results()).hasSize(3);

    BatchLoadRelationResultItem tableItem = response.results().get(0);
    assertThat(tableItem.identifier()).isEqualTo(TABLE_IDENT);
    assertThat(tableItem.status()).isEqualTo(HttpStatus.SC_OK);
    assertThat(tableItem.result()).isNotNull();
    assertThat(tableItem.result().objectType()).isEqualTo(CatalogObjectType.TABLE);
    assertThat(tableItem.etag()).isNotNull();

    BatchLoadRelationResultItem viewItem = response.results().get(1);
    assertThat(viewItem.identifier()).isEqualTo(VIEW_IDENT);
    assertThat(viewItem.status()).isEqualTo(HttpStatus.SC_OK);
    assertThat(viewItem.result()).isNotNull();
    assertThat(viewItem.result().objectType()).isEqualTo(CatalogObjectType.VIEW);
    assertThat(viewItem.etag()).isNull();

    BatchLoadRelationResultItem missingItem = response.results().get(2);
    assertThat(missingItem.identifier()).isEqualTo(MISSING_IDENT);
    assertThat(missingItem.status()).isEqualTo(HttpStatus.SC_NOT_FOUND);
    assertThat(missingItem.result()).isNull();
  }

  @Test
  void batchLoadRelationsWithSnapshotMode() {
    BatchLoadRelationsRequest request =
        BatchLoadRelationsRequest.builder()
            .addIdentifier(
                BatchLoadRelationRequestItem.builder()
                    .withIdentifier(TABLE_IDENT)
                    .withSnapshots("refs")
                    .build())
            .build();

    BatchLoadRelationsResponse response =
        CatalogHandlers.batchLoadRelations(catalog, catalog, request);

    assertThat(response.results()).hasSize(1);
    assertThat(response.results().get(0).status()).isEqualTo(HttpStatus.SC_OK);
    assertThat(response.results().get(0).result().objectType()).isEqualTo(CatalogObjectType.TABLE);
  }
}
