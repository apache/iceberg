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
package org.apache.iceberg.rest.events;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class TestEventParser {
  @Test
  public void nullCheck() {
    assertThatThrownBy(() -> EventParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid event: null");

    assertThatThrownBy(() -> EventParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse event from null object");
  }

  @Test
  public void missingFields() {
    assertThatThrownBy(() -> EventParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: event-id");
  }

  @Test
  public void roundTripWithoutActor() {
    Event event =
        ImmutableEvent.builder()
            .eventId("evt-1")
            .requestId("req-1")
            .requestEventCount(2)
            .timestampMs(1700000000000L)
            .operation(
                ImmutableCatalogOperation.DropNamespace.builder()
                    .namespace(Namespace.of("ns1"))
                    .build())
            .build();

    String expectedJson =
        "{\n"
            + "  \"event-id\" : \"evt-1\",\n"
            + "  \"request-id\" : \"req-1\",\n"
            + "  \"request-event-count\" : 2,\n"
            + "  \"timestamp-ms\" : 1700000000000,\n"
            + "  \"operation\" : {\n"
            + "    \"operation-type\" : \"drop-namespace\",\n"
            + "    \"namespace\" : [ \"ns1\" ]\n"
            + "  }\n"
            + "}";

    String json = EventParser.toJson(event, true);
    assertThat(json).isEqualTo(expectedJson);
    assertThat(EventParser.fromJson(json)).isEqualTo(event);
  }

  @Test
  public void roundTripWithActor() {
    Event event =
        ImmutableEvent.builder()
            .eventId("evt-1")
            .requestId("req-1")
            .requestEventCount(1)
            .timestampMs(1700000000000L)
            .actor(ImmutableMap.of("id", "user-1", "type", "user"))
            .operation(
                ImmutableCatalogOperation.DropNamespace.builder()
                    .namespace(Namespace.of("ns1"))
                    .build())
            .build();

    String json = EventParser.toJson(event, true);
    assertThat(EventParser.fromJson(json)).isEqualTo(event);
    assertThat(EventParser.fromJson(json).actor()).containsEntry("id", "user-1");
  }
}
