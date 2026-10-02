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
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.iceberg.catalog.CatalogObjectIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.RESTSerializers;
import org.apache.iceberg.rest.events.CatalogObjectType;
import org.apache.iceberg.rest.events.OperationType;
import org.junit.jupiter.api.Test;

public class TestQueryEventsRequestParser {
  @Test
  public void nullCheck() {
    assertThatThrownBy(() -> QueryEventsRequestParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid query events request: null");

    assertThatThrownBy(() -> QueryEventsRequestParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse query events request from null object");
  }

  @Test
  public void roundTripEmpty() {
    QueryEventsRequest request = ImmutableQueryEventsRequest.builder().build();
    String json = "{}";

    assertThat(QueryEventsRequestParser.toJson(request)).isEqualTo(json);
    assertThat(QueryEventsRequestParser.fromJson(json)).isEqualTo(request);
  }

  @Test
  public void invalidPageSize() {
    assertThatThrownBy(() -> ImmutableQueryEventsRequest.builder().pageSize(0).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid page-size: 0 (must be >= 1)");

    assertThatThrownBy(() -> QueryEventsRequestParser.fromJson("{\"page-size\": 0}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid page-size: 0 (must be >= 1)");
  }

  @Test
  public void invalidObjectType() {
    assertThatThrownBy(
            () -> QueryEventsRequestParser.fromJson("{\"object-types\": [\"function\"]}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid object type: function");
  }

  @Test
  public void roundTripAllFields() {
    QueryEventsRequest request =
        ImmutableQueryEventsRequest.builder()
            .continuationToken("token-1")
            .pageSize(100)
            .sinceTimestampMs(1700000000000L)
            .operationTypes(
                ImmutableList.of(OperationType.CREATE_TABLE, OperationType.DROP_NAMESPACE))
            .addCatalogObjectsByName(CatalogObjectIdentifier.of("accounting", "tax"))
            .catalogObjectsByUuid(ImmutableList.of("2cc52516-5e73-41f2-b139-545d41a4e151"))
            .objectTypes(ImmutableList.of(CatalogObjectType.TABLE, CatalogObjectType.NAMESPACE))
            .customFilters(ImmutableMap.of("warehouse", "wh1"))
            .build();

    String expectedJson =
        "{\n"
            + "  \"continuation-token\" : \"token-1\",\n"
            + "  \"page-size\" : 100,\n"
            + "  \"since-timestamp-ms\" : 1700000000000,\n"
            + "  \"operation-types\" : [ \"create-table\", \"drop-namespace\" ],\n"
            + "  \"catalog-objects-by-name\" : [ [ \"accounting\", \"tax\" ] ],\n"
            + "  \"catalog-objects-by-uuid\" : [ \"2cc52516-5e73-41f2-b139-545d41a4e151\" ],\n"
            + "  \"object-types\" : [ \"table\", \"namespace\" ],\n"
            + "  \"custom-filters\" : {\n"
            + "    \"warehouse\" : \"wh1\"\n"
            + "  }\n"
            + "}";

    String json = QueryEventsRequestParser.toJson(request, true);
    assertThat(json).isEqualTo(expectedJson);
    assertThat(QueryEventsRequestParser.fromJson(json)).isEqualTo(request);
  }

  @Test
  public void restSerializersRoundTrip() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    RESTSerializers.registerAll(mapper);

    QueryEventsRequest request =
        ImmutableQueryEventsRequest.builder()
            .pageSize(10)
            .operationTypes(ImmutableList.of(OperationType.RENAME_VIEW))
            .build();

    String json = mapper.writeValueAsString(request);
    assertThat(mapper.readValue(json, QueryEventsRequest.class)).isEqualTo(request);
  }
}
