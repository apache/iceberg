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
package org.apache.iceberg.rest.requests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Test;

class TestBatchLoadRelationsRequestParser {

  @Test
  void nullAndEmptyCheck() {
    assertThatThrownBy(() -> BatchLoadRelationsRequestParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid batch load relations request: null");

    assertThatThrownBy(() -> BatchLoadRelationsRequestParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse batch load relations request from null object");
  }

  @Test
  void roundTripMinimal() {
    BatchLoadRelationsRequest request =
        BatchLoadRelationsRequest.builder()
            .addIdentifier(
                BatchLoadRelationRequestItem.builder()
                    .withIdentifier(TableIdentifier.of(Namespace.of("ns"), "table1"))
                    .build())
            .build();

    String json = BatchLoadRelationsRequestParser.toJson(request);
    assertThat(json).contains("\"identifiers\":");

    BatchLoadRelationsRequest deserialized = BatchLoadRelationsRequestParser.fromJson(json);
    assertThat(deserialized.identifiers()).hasSize(1);
    assertThat(deserialized.identifiers().get(0).identifier().name()).isEqualTo("table1");
    assertThat(deserialized.identifiers().get(0).etag()).isNull();
    assertThat(deserialized.identifiers().get(0).snapshots()).isNull();
  }

  @Test
  void roundTripWithOptionalFields() {
    BatchLoadRelationsRequest request =
        BatchLoadRelationsRequest.builder()
            .addIdentifier(
                BatchLoadRelationRequestItem.builder()
                    .withIdentifier(TableIdentifier.of(Namespace.of("ns"), "table1"))
                    .withEtag("abc123")
                    .withSnapshots("refs")
                    .build())
            .addIdentifier(
                BatchLoadRelationRequestItem.builder()
                    .withIdentifier(TableIdentifier.of(Namespace.of("ns2"), "view1"))
                    .build())
            .build();

    String json = BatchLoadRelationsRequestParser.toJson(request);
    BatchLoadRelationsRequest deserialized = BatchLoadRelationsRequestParser.fromJson(json);

    assertThat(deserialized.identifiers()).hasSize(2);

    BatchLoadRelationRequestItem first = deserialized.identifiers().get(0);
    assertThat(first.identifier().name()).isEqualTo("table1");
    assertThat(first.etag()).isEqualTo("abc123");
    assertThat(first.snapshots()).isEqualTo("refs");

    BatchLoadRelationRequestItem second = deserialized.identifiers().get(1);
    assertThat(second.identifier().name()).isEqualTo("view1");
    assertThat(second.etag()).isNull();
    assertThat(second.snapshots()).isNull();
  }

  @Test
  void invalidEmptyIdentifiers() {
    assertThatThrownBy(() -> BatchLoadRelationsRequest.builder().build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("null or empty");
  }
}
