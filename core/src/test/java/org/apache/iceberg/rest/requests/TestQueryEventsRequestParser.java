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

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.CatalogObject;
import org.apache.iceberg.catalog.CatalogObjectType;
import org.apache.iceberg.catalog.CatalogObjectUuid;
import org.apache.iceberg.rest.events.operations.OperationType;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestQueryEventsRequestParser {

  @Test
  void testToJson() {
    QueryEventsRequest request = ImmutableQueryEventsRequest.builder()
        .pageToken("pt")
        .pageSize(10)
        .afterTimestampMs(123L)
        .operationTypes(List.of(OperationType.CREATE_TABLE, OperationType.DROP_TABLE))
        .catalogObjectsByName(List.of(CatalogObject.of("a", "b"), CatalogObject.of("c")))
        .catalogObjectsById(List.of(new CatalogObjectUuid("uuid1", CatalogObjectType.TABLE)))
        .objectTypes(List.of(CatalogObjectType.TABLE, CatalogObjectType.NAMESPACE))
        .customFilters(Map.of("k1", "v1"))
        .build();

    String expected =
        "{\"page-token\":\"pt\",\"page-size\":10,\"after-timestamp-ms\":123," +
            "\"operation-types\":[\"create-table\",\"drop-table\"]," +
            "\"catalog-objects-by-name\":[\"a.b\",\"c\"]," +
            "\"catalog-objects-by-id\":[{\"uuid\":\"uuid1\",\"type\":\"table\"}]," +
            "\"object-types\":[\"table\",\"namespace\"]," +
            "\"custom-filters\":{\"k1\":\"v1\"}}";

    assertThat(QueryEventsRequestParser.toJson(request)).isEqualTo(expected);
  }

  @Test
  void testToJsonPretty() {
    QueryEventsRequest request = ImmutableQueryEventsRequest.builder()
        .pageToken("pt")
        .pageSize(10)
        .afterTimestampMs(123L)
        .operationTypes(List.of(OperationType.CREATE_TABLE, OperationType.DROP_TABLE))
        .catalogObjectsByName(List.of(CatalogObject.of("a", "b"), CatalogObject.of("c")))
        .catalogObjectsById(List.of(new CatalogObjectUuid("uuid1", CatalogObjectType.TABLE)))
        .objectTypes(List.of(CatalogObjectType.TABLE, CatalogObjectType.NAMESPACE))
        .customFilters(Map.of("k1", "v1"))
        .build();

    String expected = "{\n" +
        "  \"page-token\" : \"pt\",\n" +
        "  \"page-size\" : 10,\n" +
        "  \"after-timestamp-ms\" : 123,\n" +
        "  \"operation-types\" : [ \"create-table\", \"drop-table\" ],\n" +
        "  \"catalog-objects-by-name\" : [ \"a.b\", \"c\" ],\n" +
        "  \"catalog-objects-by-id\" : [ {\n" +
        "    \"uuid\" : \"uuid1\",\n" +
        "    \"type\" : \"table\"\n" +
        "  } ],\n" +
        "  \"object-types\" : [ \"table\", \"namespace\" ],\n" +
        "  \"custom-filters\" : {\n" +
        "    \"k1\" : \"v1\"\n" +
        "  }\n" +
        "}";

    assertThat(QueryEventsRequestParser.toJsonPretty(request)).isEqualTo(expected);
  }

  @Test
  void testToJsonWithNullRequest() {
    Assertions.assertThatThrownBy(() -> QueryEventsRequestParser.toJson(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid query events request: null");
  }

  @Test
  void testToJsonWithNoProperties() {
    QueryEventsRequest request = ImmutableQueryEventsRequest.builder().build();
    String expected = "{}";
    assertThat(QueryEventsRequestParser.toJson(request)).isEqualTo(expected);
  }

  @Test
  void testFromJson() {
    QueryEventsRequest request = ImmutableQueryEventsRequest.builder()
        .pageToken("pt")
        .pageSize(10)
        .afterTimestampMs(123L)
        .operationTypes(List.of(OperationType.CREATE_TABLE, OperationType.DROP_TABLE))
        .catalogObjectsByName(List.of(CatalogObject.of("a", "b"), CatalogObject.of("c")))
        .catalogObjectsById(List.of(new CatalogObjectUuid("uuid1", CatalogObjectType.TABLE)))
        .objectTypes(List.of(CatalogObjectType.TABLE, CatalogObjectType.NAMESPACE))
        .customFilters(Map.of("k1", "v1", "k2", "v2"))
        .build();
    String json =
        "{\"page-token\":\"pt\",\"page-size\":10,\"after-timestamp-ms\":123," +
            "\"operation-types\":[\"create-table\",\"drop-table\"]," +
            "\"catalog-objects-by-name\":[\"a.b\",\"c\"]," +
            "\"catalog-objects-by-id\":[{\"uuid\":\"uuid1\",\"type\":\"table\"}]," +
            "\"object-types\":[\"table\",\"namespace\"]," +
            "\"custom-filters\":{\"k1\":\"v1\",\"k2\":\"v2\"}}";

    assertThat(QueryEventsRequestParser.fromJson(json)).isEqualTo(request);
  }

  @Test
  void testFromJsonWithNullInput() {
    Assertions.assertThatThrownBy(() -> QueryEventsRequestParser.fromJson((JsonNode) null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Cannot parse query events request from null object");
  }

  @Test
  void testFromJsonWithInvalidProperties() {
    String invalidPageToken = "{\"page-token\":123}";
    Assertions.assertThatThrownBy(() -> QueryEventsRequestParser.fromJson(invalidPageToken))
        .isInstanceOf(IllegalArgumentException.class);

    String invalidPageSize = "{\"page-size\":\"x\"}";
    Assertions.assertThatThrownBy(() -> QueryEventsRequestParser.fromJson(invalidPageSize))
        .isInstanceOf(IllegalArgumentException.class);

    String invalidAfter = "{\"after-timestamp-ms\":\"x\"}";
    Assertions.assertThatThrownBy(() -> QueryEventsRequestParser.fromJson(invalidAfter))
        .isInstanceOf(IllegalArgumentException.class);

    String invalidOperationTypes = "{\"operation-types\":{}}";
    Assertions.assertThatThrownBy(() -> QueryEventsRequestParser.fromJson(invalidOperationTypes))
        .isInstanceOf(IllegalArgumentException.class);

    String invalidOperationType = "{\"operation-types\":[{}]}";
    Assertions.assertThatThrownBy(() -> QueryEventsRequestParser.fromJson(invalidOperationType))
        .isInstanceOf(IllegalArgumentException.class);

    String invalidCatalogObjectsByName = "{\"catalog-objects-by-name\":{}}";
    Assertions.assertThatThrownBy(() -> QueryEventsRequestParser.fromJson(invalidCatalogObjectsByName))
        .isInstanceOf(IllegalArgumentException.class);

    String invalidCatalogObjectsById = "{\"catalog-objects-by-id\":{}}";
    Assertions.assertThatThrownBy(() -> QueryEventsRequestParser.fromJson(invalidCatalogObjectsById))
        .isInstanceOf(IllegalArgumentException.class);

    String invalidCatalogObjectUuid = "{\"catalog-objects-by-id\":[{}]}";
    Assertions.assertThatThrownBy(() -> QueryEventsRequestParser.fromJson(invalidCatalogObjectUuid))
        .isInstanceOf(IllegalArgumentException.class);

    String invalidObjectTypes = "{\"object-types\":{}}";
    Assertions.assertThatThrownBy(() -> QueryEventsRequestParser.fromJson(invalidObjectTypes))
        .isInstanceOf(IllegalArgumentException.class);

    String invalidObjectType = "{\"object-types\":[\"\"]}";
    Assertions.assertThatThrownBy(() -> QueryEventsRequestParser.fromJson(invalidObjectType))
        .isInstanceOf(IllegalArgumentException.class);

    String invalidCustomFilters = "{\"custom-filters\":[]}";
    Assertions.assertThatThrownBy(() -> QueryEventsRequestParser.fromJson(invalidCustomFilters))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void testFromJsonWithNoProperties() {
    QueryEventsRequest request = ImmutableQueryEventsRequest.builder().build();
    String expected = "{}";
    assertThat(QueryEventsRequestParser.fromJson(expected)).isEqualTo(request);
  }
}
