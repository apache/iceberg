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
import static org.assertj.core.api.Assumptions.assumeThat;

import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.FieldLabels;
import org.apache.iceberg.Labels;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.View;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Verifies that labels injected by the REST server fixture reach the client over the wire.
 *
 * <p>The labels are read from the raw load responses rather than from the loaded table so that this
 * covers the response payload itself, which is what other client implementations parse.
 */
public class TestRESTServerLabels {

  private static final Namespace NAMESPACE = Namespace.of("ns");

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  @RegisterExtension
  private static final RESTServerExtension REST_SERVER_EXTENSION =
      new RESTServerExtension(
          Map.of(
              RESTCatalogServer.REST_PORT,
              RESTServerExtension.FREE_PORT,
              RESTCatalogServer.CATALOG_NAME,
              "labels_backend",
              CatalogProperties.CLIENT_POOL_SIZE,
              "1",
              "include-labels",
              "true"));

  private static RESTCatalog restCatalog;
  private static RESTClient client;
  private static ResourcePaths paths;

  @BeforeAll
  static void beforeAll() {
    // labels here are injected by the local fixture, so there is nothing to assert when the tests
    // are pointed at an external REST server
    assumeThat(REST_SERVER_EXTENSION.client())
        .as("requires the local REST server fixture")
        .isNotNull();

    restCatalog = REST_SERVER_EXTENSION.client();
    restCatalog.createNamespace(NAMESPACE);

    paths = ResourcePaths.forCatalogProperties(ImmutableMap.of());
    client =
        HTTPClient.builder(ImmutableMap.of())
            .uri(
                String.format(
                    "http://localhost:%s/",
                    REST_SERVER_EXTENSION.config().get(RESTCatalogServer.REST_PORT)))
            // the local fixture does not authenticate requests
            .withAuthSession(AuthSession.EMPTY)
            .build();
  }

  @AfterAll
  static void afterAll() throws Exception {
    if (client != null) {
      client.close();
    }
  }

  @Test
  public void tableLabelsAreReturnedOnLoad() {
    TableIdentifier ident = TableIdentifier.of(NAMESPACE, "tbl");
    restCatalog.createTable(ident, SCHEMA);

    LoadTableResponse response =
        client.get(
            paths.table(ident),
            LoadTableResponse.class,
            ImmutableMap.of(),
            ErrorHandlers.tableErrorHandler());

    assertLabels(response.labels());
    // the response must remain usable, so injecting labels cannot drop the metadata
    assertThat(response.tableMetadata().schema().asStruct()).isEqualTo(SCHEMA.asStruct());
  }

  @Test
  public void viewLabelsAreReturnedOnLoad() {
    TableIdentifier ident = TableIdentifier.of(NAMESPACE, "v");
    View view =
        restCatalog
            .buildView(ident)
            .withSchema(SCHEMA)
            .withDefaultNamespace(NAMESPACE)
            .withQuery("spark", "SELECT id, data FROM tbl")
            .create();

    LoadViewResponse response =
        client.get(
            paths.view(ident),
            LoadViewResponse.class,
            ImmutableMap.of(),
            ErrorHandlers.viewErrorHandler());

    assertLabels(response.labels());
    assertThat(response.metadata().schema().asStruct()).isEqualTo(view.schema().asStruct());
  }

  private void assertLabels(Labels labels) {
    assertThat(labels.isEmpty()).isFalse();
    assertThat(labels.objectLabels()).containsEntry("catalog-name", "labels_backend");

    assertThat(labels.fields()).hasSize(1);
    FieldLabels fieldLabels = labels.fields().get(0);
    assertThat(fieldLabels.fieldId()).isEqualTo(1);
    assertThat(fieldLabels.labels()).containsEntry("classification", "public");
  }
}
