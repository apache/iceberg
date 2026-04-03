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
package org.apache.iceberg.rest.responses;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.iceberg.catalog.CatalogObjectType;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Test;

class TestBatchLoadRelationsResponseParser {

  @Test
  void nullAndEmptyCheck() {
    assertThatThrownBy(() -> BatchLoadRelationsResponseParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid batch load relations response: null");

    assertThatThrownBy(() -> BatchLoadRelationsResponseParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse batch load relations response from null object");
  }

  @Test
  void roundTripWithTableResult() {
    LoadTableResponse tableResponse =
        LoadTableResponse.builder()
            .withTableMetadata(TestLoadRelationResponseParser.tableMetadata())
            .build();
    LoadRelationResponse result =
        LoadRelationResponse.builder().withTableResponse(tableResponse).build();

    TableIdentifier ident = TableIdentifier.of(Namespace.of("ns"), "table1");
    BatchLoadRelationsResponse response =
        BatchLoadRelationsResponse.builder()
            .addResult(
                BatchLoadRelationResultItem.builder()
                    .withIdentifier(ident)
                    .withStatus(HttpStatus.SC_OK)
                    .withResult(result)
                    .withEtag("metadata-location")
                    .build())
            .build();

    String json = BatchLoadRelationsResponseParser.toJson(response);
    BatchLoadRelationsResponse deserialized = BatchLoadRelationsResponseParser.fromJson(json);

    assertThat(deserialized.results()).hasSize(1);
    BatchLoadRelationResultItem item = deserialized.results().get(0);
    assertThat(item.identifier().name()).isEqualTo("table1");
    assertThat(item.status()).isEqualTo(HttpStatus.SC_OK);
    assertThat(item.result()).isNotNull();
    assertThat(item.result().objectType()).isEqualTo(CatalogObjectType.TABLE);
    assertThat(item.etag()).isEqualTo("metadata-location");
    assertThat(deserialized.unprocessedIdentifiers()).isEmpty();
  }

  @Test
  void roundTripWithViewResult() {
    LoadViewResponse viewResponse =
        ImmutableLoadViewResponse.builder()
            .metadataLocation("view-metadata-location")
            .metadata(TestLoadRelationResponseParser.viewMetadata())
            .build();
    LoadRelationResponse result =
        LoadRelationResponse.builder().withViewResponse(viewResponse).build();

    TableIdentifier ident = TableIdentifier.of(Namespace.of("ns"), "view1");
    BatchLoadRelationsResponse response =
        BatchLoadRelationsResponse.builder()
            .addResult(
                BatchLoadRelationResultItem.builder()
                    .withIdentifier(ident)
                    .withStatus(HttpStatus.SC_OK)
                    .withResult(result)
                    .build())
            .build();

    String json = BatchLoadRelationsResponseParser.toJson(response);
    BatchLoadRelationsResponse deserialized = BatchLoadRelationsResponseParser.fromJson(json);

    assertThat(deserialized.results()).hasSize(1);
    BatchLoadRelationResultItem item = deserialized.results().get(0);
    assertThat(item.status()).isEqualTo(HttpStatus.SC_OK);
    assertThat(item.result().objectType()).isEqualTo(CatalogObjectType.VIEW);
    assertThat(item.etag()).isNull();
  }

  @Test
  void roundTripNotFound() {
    TableIdentifier ident = TableIdentifier.of(Namespace.of("ns"), "missing");
    BatchLoadRelationsResponse response =
        BatchLoadRelationsResponse.builder()
            .addResult(
                BatchLoadRelationResultItem.builder()
                    .withIdentifier(ident)
                    .withStatus(HttpStatus.SC_NOT_FOUND)
                    .build())
            .build();

    String json = BatchLoadRelationsResponseParser.toJson(response);
    BatchLoadRelationsResponse deserialized = BatchLoadRelationsResponseParser.fromJson(json);

    assertThat(deserialized.results()).hasSize(1);
    assertThat(deserialized.results().get(0).status()).isEqualTo(HttpStatus.SC_NOT_FOUND);
    assertThat(deserialized.results().get(0).result()).isNull();
  }

  @Test
  void roundTripNotModified() {
    TableIdentifier ident = TableIdentifier.of(Namespace.of("ns"), "table1");
    BatchLoadRelationsResponse response =
        BatchLoadRelationsResponse.builder()
            .addResult(
                BatchLoadRelationResultItem.builder()
                    .withIdentifier(ident)
                    .withStatus(HttpStatus.SC_NOT_MODIFIED)
                    .withEtag("some-etag")
                    .build())
            .build();

    String json = BatchLoadRelationsResponseParser.toJson(response);
    BatchLoadRelationsResponse deserialized = BatchLoadRelationsResponseParser.fromJson(json);

    assertThat(deserialized.results()).hasSize(1);
    assertThat(deserialized.results().get(0).status()).isEqualTo(HttpStatus.SC_NOT_MODIFIED);
    assertThat(deserialized.results().get(0).result()).isNull();
    assertThat(deserialized.results().get(0).etag()).isEqualTo("some-etag");
  }

  @Test
  void roundTripWithUnprocessedIdentifiers() {
    TableIdentifier ident1 = TableIdentifier.of(Namespace.of("ns"), "table1");
    TableIdentifier unprocessed = TableIdentifier.of(Namespace.of("ns2"), "table2");

    BatchLoadRelationsResponse response =
        BatchLoadRelationsResponse.builder()
            .addResult(
                BatchLoadRelationResultItem.builder()
                    .withIdentifier(ident1)
                    .withStatus(HttpStatus.SC_NOT_FOUND)
                    .build())
            .addUnprocessedIdentifier(unprocessed)
            .build();

    String json = BatchLoadRelationsResponseParser.toJson(response);
    assertThat(json).contains("\"unprocessed-identifiers\":");

    BatchLoadRelationsResponse deserialized = BatchLoadRelationsResponseParser.fromJson(json);
    assertThat(deserialized.results()).hasSize(1);
    assertThat(deserialized.unprocessedIdentifiers()).hasSize(1);
    assertThat(deserialized.unprocessedIdentifiers().get(0).name()).isEqualTo("table2");
  }

  @Test
  void invalidStatusRequiresResult() {
    assertThatThrownBy(
            () ->
                BatchLoadRelationResultItem.builder()
                    .withIdentifier(TableIdentifier.of(Namespace.of("ns"), "t"))
                    .withStatus(HttpStatus.SC_OK)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid result: null when status is " + HttpStatus.SC_OK);
  }

  @Test
  void invalidStatus() {
    assertThatThrownBy(
            () ->
                BatchLoadRelationResultItem.builder()
                    .withIdentifier(TableIdentifier.of(Namespace.of("ns"), "t"))
                    .withStatus(500)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid status: 500");
  }
}
