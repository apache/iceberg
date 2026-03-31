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
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.CatalogObjectType;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.ImmutableViewVersion;
import org.apache.iceberg.view.ViewMetadata;
import org.junit.jupiter.api.Test;

class TestLoadRelationResponseParser {

  @Test
  void nullAndEmptyCheck() {
    assertThatThrownBy(() -> LoadRelationResponseParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid load relation response: null");

    assertThatThrownBy(() -> LoadRelationResponseParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse load relation response from null object");
  }

  @Test
  void roundTripTableBranch() {
    TableMetadata metadata = tableMetadata();
    LoadTableResponse tableResponse =
        LoadTableResponse.builder().withTableMetadata(metadata).build();

    LoadRelationResponse response =
        LoadRelationResponse.builder().withTableResponse(tableResponse).build();

    assertThat(response.objectType()).isEqualTo(CatalogObjectType.TABLE);
    assertThat(response.tableResponse()).isNotNull();
    assertThat(response.viewResponse()).isNull();

    String json = LoadRelationResponseParser.toJson(response);
    assertThat(json).contains("\"object-type\":\"table\"");
    assertThat(json).contains("\"table\":{");

    LoadRelationResponse deserialized = LoadRelationResponseParser.fromJson(json);
    assertThat(deserialized.objectType()).isEqualTo(CatalogObjectType.TABLE);
    assertThat(deserialized.tableResponse()).isNotNull();
    assertThat(deserialized.tableResponse().tableMetadata().location()).isEqualTo("location");
    assertThat(deserialized.viewResponse()).isNull();
  }

  @Test
  void roundTripViewBranch() {
    ViewMetadata metadata = viewMetadata();
    LoadViewResponse viewResponse =
        ImmutableLoadViewResponse.builder()
            .metadataLocation("view-metadata-location")
            .metadata(metadata)
            .build();

    LoadRelationResponse response =
        LoadRelationResponse.builder().withViewResponse(viewResponse).build();

    assertThat(response.objectType()).isEqualTo(CatalogObjectType.VIEW);
    assertThat(response.viewResponse()).isNotNull();
    assertThat(response.tableResponse()).isNull();

    String json = LoadRelationResponseParser.toJson(response);
    assertThat(json).contains("\"object-type\":\"view\"");
    assertThat(json).contains("\"view\":{");

    LoadRelationResponse deserialized = LoadRelationResponseParser.fromJson(json);
    assertThat(deserialized.objectType()).isEqualTo(CatalogObjectType.VIEW);
    assertThat(deserialized.viewResponse()).isNotNull();
    assertThat(deserialized.viewResponse().metadataLocation()).isEqualTo("view-metadata-location");
    assertThat(deserialized.tableResponse()).isNull();
  }

  @Test
  void invalidMissingObjectType() {
    assertThatThrownBy(() -> LoadRelationResponseParser.fromJson("{\"table\":{}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("object-type");
  }

  @Test
  void invalidTableResponseNull() {
    assertThatThrownBy(
            () -> LoadRelationResponse.builder().withObjectType(CatalogObjectType.TABLE).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("null for object-type table");
  }

  @Test
  void invalidViewResponseNull() {
    assertThatThrownBy(
            () -> LoadRelationResponse.builder().withObjectType(CatalogObjectType.VIEW).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("null for object-type view");
  }

  static TableMetadata tableMetadata() {
    return TableMetadata.buildFromEmpty(2)
        .assignUUID("386b9f01-002b-4d8c-b77f-42c3fd3b7c9b")
        .setLocation("location")
        .setCurrentSchema(new Schema(Types.NestedField.required(1, "x", Types.LongType.get())), 1)
        .addPartitionSpec(PartitionSpec.unpartitioned())
        .addSortOrder(SortOrder.unsorted())
        .discardChanges()
        .withMetadataLocation("metadata-location")
        .build();
  }

  static ViewMetadata viewMetadata() {
    return ViewMetadata.buildFrom(
            ViewMetadata.builder()
                .setLocation("view-location")
                .addSchema(new Schema(Types.NestedField.required(1, "y", Types.StringType.get())))
                .addVersion(
                    ImmutableViewVersion.builder()
                        .versionId(1)
                        .schemaId(0)
                        .timestampMillis(23L)
                        .defaultNamespace(org.apache.iceberg.catalog.Namespace.of("ns"))
                        .build())
                .setCurrentVersionId(1)
                .build())
        .setMetadataLocation("view-metadata-location")
        .build();
  }
}
