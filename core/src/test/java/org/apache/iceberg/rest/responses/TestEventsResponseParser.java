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

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.events.Event;
import org.apache.iceberg.rest.events.ImmutableEvent;
import org.apache.iceberg.rest.events.operations.ImmutableCreateNamespaceOperation;
import org.apache.iceberg.rest.events.operations.Operation;
import org.apache.iceberg.rest.events.parsers.EventParser;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestEventsResponseParser {

  private static Operation sampleOperation() {
    return ImmutableCreateNamespaceOperation.builder()
        .namespace(Namespace.of("a", "b"))
        .build();
  }

  private static Event sampleEventWithActor() {
    return ImmutableEvent.builder()
        .eventId("e-1")
        .requestId("r-1")
        .eventCount(2)
        .timestampMs(123L)
        .actor("user1")
        .operation(sampleOperation())
        .build();
  }

  @Test
  void testToJson() {
    EventsResponse response = ImmutableEventsResponse.builder()
        .nextPageToken("npt")
        .highestProcessedTimestampMs(5000L)
        .events(List.of(sampleEventWithActor()))
        .build();

    String expected =
        "{\"next-page-token\":\"npt\",\"highest-processed-timestamp-ms\":5000,\"events\":[{" +
            "\"event-id\":\"e-1\",\"request-id\":\"r-1\",\"event-count\":2,\"timestamp-ms\":123,\"actor\":\"user1\",\"operation\":{\"operation-type\":\"create-namespace\",\"namespace\":[\"a\",\"b\"]}}]}";
    assertThat(EventsResponseParser.toJson(response)).isEqualTo(expected);
  }

  @Test
  void testToJsonPretty() {
    EventsResponse response = ImmutableEventsResponse.builder()
        .nextPageToken("npt")
        .highestProcessedTimestampMs(5000L)
        .events(List.of(sampleEventWithActor()))
        .build();

    String expected = "{\n" +
        "  \"next-page-token\" : \"npt\",\n" +
        "  \"highest-processed-timestamp-ms\" : 5000,\n" +
        "  \"events\" : [ {\n" +
        "    \"event-id\" : \"e-1\",\n" +
        "    \"request-id\" : \"r-1\",\n" +
        "    \"event-count\" : 2,\n" +
        "    \"timestamp-ms\" : 123,\n" +
        "    \"actor\" : \"user1\",\n" +
        "    \"operation\" : {\n" +
        "      \"operation-type\" : \"create-namespace\",\n" +
        "      \"namespace\" : [ \"a\", \"b\" ]\n" +
        "    }\n" +
        "  } ]\n" +
        "}";
    assertThat(EventsResponseParser.toJsonPretty(response)).isEqualTo(expected);
  }

  @Test
  void testToJsonWithNullResponse() {
    assertThatThrownBy(() -> EventsResponseParser.toJson(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid events response: null");
  }

  @Test
  void testFromJson() {
    EventsResponse response = ImmutableEventsResponse.builder()
        .nextPageToken("npt")
        .highestProcessedTimestampMs(5000L)
        .events(List.of(sampleEventWithActor()))
        .build();
    String json =
        "{\"next-page-token\":\"npt\",\"highest-processed-timestamp-ms\":5000,\"events\":[{" +
            "\"event-id\":\"e-1\",\"request-id\":\"r-1\",\"event-count\":2,\"timestamp-ms\":123,\"actor\":\"user1\",\"operation\":{\"operation-type\":\"create-namespace\",\"namespace\":[\"a\",\"b\"]}}]}";

    assertThat(EventsResponseParser.fromJson(json)).isEqualTo(response);
  }

  @Test
  void testFromJsonWithNullInput() {
    assertThatThrownBy(() -> EventsResponseParser.fromJson((JsonNode) null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Cannot parse events response from null object");
  }

  @Test
  void testFromJsonWithMissingProperties() {
    String missingHighestProcessedTimestamp =
        "{\"events\":[]}";
    assertThatThrownBy(() -> EventsResponseParser.fromJson(missingHighestProcessedTimestamp))
        .isInstanceOf(IllegalArgumentException.class);

    String missingEvents =
        "{\"highest-processed-timestamp-ms\":1}";
    assertThatThrownBy(() -> EventsResponseParser.fromJson(missingEvents))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void testFromJsonWithInvalidProperties() {
    String invalidNextPageToken =
        "{\"next-page-token\":123,\"highest-processed-timestamp-ms\":1,\"events\":[]}";
    assertThatThrownBy(() -> EventsResponseParser.fromJson(invalidNextPageToken))
        .isInstanceOf(IllegalArgumentException.class);

    String invalidHighestProcessed =
        "{\"next-page-token\":\"npt\",\"highest-processed-timestamp-ms\":\"x\",\"events\":[]}";
    assertThatThrownBy(() -> EventsResponseParser.fromJson(invalidHighestProcessed))
        .isInstanceOf(IllegalArgumentException.class);

    String invalidEvents = "{\"highest-processed-timestamp-ms\":1,\"events\":{}}";
    assertThatThrownBy(() -> EventsResponseParser.fromJson(invalidEvents))
        .isInstanceOf(IllegalArgumentException.class);

    String invalidEventsObject = "{\"highest-processed-timestamp-ms\":1,\"events\":[{}]}";
    assertThatThrownBy(() -> EventsResponseParser.fromJson(invalidEventsObject))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
