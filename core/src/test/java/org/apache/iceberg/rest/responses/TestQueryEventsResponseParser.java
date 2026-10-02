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
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.RESTSerializers;
import org.apache.iceberg.rest.events.CatalogOperation;
import org.apache.iceberg.rest.events.Event;
import org.apache.iceberg.rest.events.ImmutableCatalogOperation;
import org.apache.iceberg.rest.events.ImmutableEvent;
import org.junit.jupiter.api.Test;

public class TestQueryEventsResponseParser {
  @Test
  public void nullCheck() {
    assertThatThrownBy(() -> QueryEventsResponseParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid query events response: null");

    assertThatThrownBy(() -> QueryEventsResponseParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse query events response from null object");
  }

  @Test
  public void missingFields() {
    assertThatThrownBy(() -> QueryEventsResponseParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: continuation-token");

    assertThatThrownBy(
            () -> QueryEventsResponseParser.fromJson("{\"continuation-token\": \"next\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing list: events");
  }

  @Test
  public void roundTripEmptyEvents() {
    QueryEventsResponse response =
        ImmutableQueryEventsResponse.builder().continuationToken("next").build();

    String expectedJson =
        "{\n" + "  \"continuation-token\" : \"next\",\n" + "  \"events\" : [ ]\n" + "}";

    String json = QueryEventsResponseParser.toJson(response, true);
    assertThat(json).isEqualTo(expectedJson);
    assertThat(QueryEventsResponseParser.fromJson(json)).isEqualTo(response);
  }

  @Test
  public void roundTripWithEvent() {
    CatalogOperation.DropNamespace operation =
        ImmutableCatalogOperation.DropNamespace.builder().namespace(Namespace.of("ns1")).build();
    Event event =
        ImmutableEvent.builder()
            .eventId("evt-1")
            .requestId("req-1")
            .requestEventCount(1)
            .timestampMs(1700000000000L)
            .actor(ImmutableMap.of("id", "user-1"))
            .operation(operation)
            .build();
    QueryEventsResponse response =
        ImmutableQueryEventsResponse.builder().continuationToken("next").addEvents(event).build();

    String expectedJson =
        "{\n"
            + "  \"continuation-token\" : \"next\",\n"
            + "  \"events\" : [ {\n"
            + "    \"event-id\" : \"evt-1\",\n"
            + "    \"request-id\" : \"req-1\",\n"
            + "    \"request-event-count\" : 1,\n"
            + "    \"timestamp-ms\" : 1700000000000,\n"
            + "    \"actor\" : {\n"
            + "      \"id\" : \"user-1\"\n"
            + "    },\n"
            + "    \"operation\" : {\n"
            + "      \"operation-type\" : \"drop-namespace\",\n"
            + "      \"namespace\" : [ \"ns1\" ]\n"
            + "    }\n"
            + "  } ]\n"
            + "}";

    String json = QueryEventsResponseParser.toJson(response, true);
    assertThat(json).isEqualTo(expectedJson);
    assertThat(QueryEventsResponseParser.fromJson(json)).isEqualTo(response);
  }

  @Test
  public void restSerializersRoundTrip() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    RESTSerializers.registerAll(mapper);

    QueryEventsResponse response =
        ImmutableQueryEventsResponse.builder().continuationToken("next").build();

    String json = mapper.writeValueAsString(response);
    assertThat(mapper.readValue(json, QueryEventsResponse.class)).isEqualTo(response);
  }
}
