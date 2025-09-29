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
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.events.Event;
import org.apache.iceberg.rest.events.ImmutableEvent;
import org.apache.iceberg.rest.events.operations.ImmutableCreateNamespaceOperation;
import org.apache.iceberg.rest.events.operations.Operation;
import org.junit.jupiter.api.Test;

public class TestEventsResponseParser {

  private static Operation sampleOperation() {
    return ImmutableCreateNamespaceOperation.builder().namespace(Namespace.of("a", "b")).build();
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
    EventsResponse response =
        ImmutableEventsResponse.builder()
            .nextPageToken("npt")
            .highestProcessedTimestampMs(5000L)
            .events(List.of(sampleEventWithActor()))
            .build();

    String expected =
        "{\"next-page-token\":\"npt\",\"highest-processed-timestamp-ms\":5000,\"events\":[{"
            + "\"event-id\":\"e-1\",\"request-id\":\"r-1\",\"event-count\":2,\"timestamp-ms\":123,\"actor\":\"user1\",\"operation\":{\"operation-type\":\"create-namespace\",\"namespace\":[\"a\",\"b\"]}}]}";
    assertThat(EventsResponseParser.toJson(response)).isEqualTo(expected);
  }

  @Test
  void testToJsonPretty() {
    EventsResponse response =
        ImmutableEventsResponse.builder()
            .nextPageToken("npt")
            .highestProcessedTimestampMs(5000L)
            .events(List.of(sampleEventWithActor()))
            .build();

    String expected =
        "{\n"
            + "  \"next-page-token\" : \"npt\",\n"
            + "  \"highest-processed-timestamp-ms\" : 5000,\n"
            + "  \"events\" : [ {\n"
            + "    \"event-id\" : \"e-1\",\n"
            + "    \"request-id\" : \"r-1\",\n"
            + "    \"event-count\" : 2,\n"
            + "    \"timestamp-ms\" : 123,\n"
            + "    \"actor\" : \"user1\",\n"
            + "    \"operation\" : {\n"
            + "      \"operation-type\" : \"create-namespace\",\n"
            + "      \"namespace\" : [ \"a\", \"b\" ]\n"
            + "    }\n"
            + "  } ]\n"
            + "}";
    assertThat(EventsResponseParser.toJsonPretty(response)).isEqualTo(expected);
  }

  @Test
  void testToJsonWithNullResponse() {
    assertThatNullPointerException()
        .isThrownBy(() -> EventsResponseParser.toJson(null))
        .withMessage("Invalid events response: null");
  }

  @Test
  void testFromJson() {
    EventsResponse response =
        ImmutableEventsResponse.builder()
            .nextPageToken("npt")
            .highestProcessedTimestampMs(5000L)
            .events(List.of(sampleEventWithActor()))
            .build();
    String json =
        "{\"next-page-token\":\"npt\",\"highest-processed-timestamp-ms\":5000,\"events\":[{"
            + "\"event-id\":\"e-1\",\"request-id\":\"r-1\",\"event-count\":2,\"timestamp-ms\":123,\"actor\":\"user1\",\"operation\":{\"operation-type\":\"create-namespace\",\"namespace\":[\"a\",\"b\"]}}]}";

    assertThat(EventsResponseParser.fromJson(json)).isEqualTo(response);
  }

  @Test
  void testFromJsonWithNullInput() {
    assertThatNullPointerException()
        .isThrownBy(() -> EventsResponseParser.fromJson((JsonNode) null))
        .withMessage("Cannot parse events response from null object");
  }

  @Test
  void testFromJsonWithMissingProperties() {
    String missingHighestProcessedTimestamp = "{\"events\":[]}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> EventsResponseParser.fromJson(missingHighestProcessedTimestamp));

    String missingEvents = "{\"highest-processed-timestamp-ms\":1}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> EventsResponseParser.fromJson(missingEvents));
  }

  @Test
  void testFromJsonWithInvalidProperties() {
    String invalidNextPageToken =
        "{\"next-page-token\":123,\"highest-processed-timestamp-ms\":1,\"events\":[]}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> EventsResponseParser.fromJson(invalidNextPageToken));

    String invalidHighestProcessed =
        "{\"next-page-token\":\"npt\",\"highest-processed-timestamp-ms\":\"x\",\"events\":[]}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> EventsResponseParser.fromJson(invalidHighestProcessed));

    String invalidEvents = "{\"highest-processed-timestamp-ms\":1,\"events\":{}}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> EventsResponseParser.fromJson(invalidEvents));

    String invalidEventsObject = "{\"highest-processed-timestamp-ms\":1,\"events\":[{}]}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> EventsResponseParser.fromJson(invalidEventsObject));
  }
}
