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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.function.Consumer;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.functions.IcebergFunction;
import org.apache.iceberg.functions.IcebergFunctions;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadTableResponseParser;
import org.apache.iceberg.rest.restrictions.ReadRestrictions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies that read restrictions attached to any load table response reach the loaded table, not
 * just the ones returned by loadTable. Register and create return a {@link LoadTableResponse} too,
 * so a server that attaches restrictions there must not have them dropped.
 */
public class TestRESTCatalogReadRestrictions {

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()), optional(2, "email", Types.StringType.get()));

  private static final Namespace NAMESPACE = Namespace.of("restrictions");
  private static final TableIdentifier SOURCE = TableIdentifier.of(NAMESPACE, "source");

  private static final ReadRestrictions RESTRICTIONS =
      ReadRestrictions.of(null, ImmutableList.of(IcebergFunctions.maskAlphanum(2)));

  private InMemoryCatalog backend;

  @BeforeEach
  public void createBackend() {
    this.backend = new InMemoryCatalog();
    backend.initialize(
        "backend", ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, "memory://warehouse"));
    backend.createNamespace(NAMESPACE);
  }

  @Test
  public void registerTableCarriesReadRestrictions() throws Exception {
    String metadataLocation = TableUtil.metadataFileLocation(backend.createTable(SOURCE, SCHEMA));
    TableIdentifier target = TableIdentifier.of(NAMESPACE, "registered");

    try (RESTCatalog catalog = restCatalogAttaching(RESTRICTIONS)) {
      assertThat(restrictedFieldIds(catalog.registerTable(target, metadataLocation)))
          .containsExactly(2);
    }
  }

  @Test
  public void createTableCarriesReadRestrictions() throws Exception {
    TableIdentifier target = TableIdentifier.of(NAMESPACE, "created");

    try (RESTCatalog catalog = restCatalogAttaching(RESTRICTIONS)) {
      assertThat(restrictedFieldIds(catalog.buildTable(target, SCHEMA).create()))
          .containsExactly(2);
    }
  }

  private static Iterable<Integer> restrictedFieldIds(Table table) {
    assertThat(TableUtil.readRestrictions(table)).isPresent();
    return TableUtil.readRestrictions(table).get().columnProjections().stream()
        .map(IcebergFunction::fieldId)
        .collect(ImmutableList.toImmutableList());
  }

  /** A REST catalog whose server attaches the given restrictions to every load table response. */
  private RESTCatalog restCatalogAttaching(ReadRestrictions restrictions) {
    RESTClient client =
        new RESTCatalogAdapter(backend) {
          @Override
          public <T extends RESTResponse> T execute(
              HTTPRequest request,
              Class<T> responseType,
              Consumer<ErrorResponse> errorHandler,
              Consumer<Map<String, String>> responseHeaders) {
            T response = super.execute(request, responseType, errorHandler, responseHeaders);
            if (!(response instanceof LoadTableResponse)) {
              return response;
            }

            LoadTableResponse loaded = (LoadTableResponse) response;
            LoadTableResponse withRestrictions =
                LoadTableResponse.builder()
                    .withTableMetadata(loaded.tableMetadata())
                    .addAllConfig(loaded.config())
                    .addAllCredentials(loaded.credentials())
                    .withReadRestrictions(restrictions)
                    .build();

            return castResponse(
                responseType,
                LoadTableResponseParser.fromJson(LoadTableResponseParser.toJson(withRestrictions)));
          }
        };

    RESTCatalog catalog = new RESTCatalog(config -> client);
    catalog.initialize(
        "rest", ImmutableMap.of(CatalogProperties.FILE_IO_IMPL, InMemoryFileIO.class.getName()));
    return catalog;
  }
}
