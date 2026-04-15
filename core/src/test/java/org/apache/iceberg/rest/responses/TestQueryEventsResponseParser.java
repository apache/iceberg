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
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.events.CatalogOperation;
import org.apache.iceberg.rest.events.Event;
import org.apache.iceberg.rest.events.ImmutableEvent;
import org.junit.jupiter.api.Test;

public class TestQueryEventsResponseParser {

  private static CatalogOperation sampleOperation() {
    return new CatalogOperation.CreateNamespace(Namespace.of("a", "b"), Map.of());
  }

  private static Event sampleEventWithActor() {
    return ImmutableEvent.builder()
        .eventId("e-1")
        .requestId("r-1")
        .requestEventCount(2)
        .timestampMs(123L)
        .actor(Map.of("id", "user1"))
        .operation(sampleOperation())
        .build();
  }

  @Test
  void testToJson() {
    QueryEventsResponse response =
        ImmutableQueryEventsResponse.builder()
            .continuationToken("npt")
            .highestProcessedTimestampMs(5000L)
            .events(List.of(sampleEventWithActor()))
            .build();

    String expected =
        "{\"continuation-token\":\"npt\",\"highest-processed-timestamp-ms\":5000,\"events\":[{"
            + "\"event-id\":\"e-1\",\"request-id\":\"r-1\",\"request-event-count\":2,\"timestamp-ms\":123,\"actor\":{\"id\":\"user1\"},\"operation\":{\"operation-type\":\"create-namespace\",\"namespace\":[\"a\",\"b\"]}}]}";
    assertThat(QueryEventsResponseParser.toJson(response)).isEqualTo(expected);
  }

  @Test
  void testToJsonWithoutContinuationToken() {
    QueryEventsResponse response =
        ImmutableQueryEventsResponse.builder()
            .highestProcessedTimestampMs(1L)
            .events(List.of())
            .build();
    assertThat(QueryEventsResponseParser.toJson(response))
        .isEqualTo("{\"highest-processed-timestamp-ms\":1,\"events\":[]}");
  }

  @Test
  void testToJsonPretty() {
    QueryEventsResponse response =
        ImmutableQueryEventsResponse.builder()
            .continuationToken("npt")
            .highestProcessedTimestampMs(5000L)
            .events(List.of(sampleEventWithActor()))
            .build();

    String expected =
        "{\n"
            + "  \"continuation-token\" : \"npt\",\n"
            + "  \"highest-processed-timestamp-ms\" : 5000,\n"
            + "  \"events\" : [ {\n"
            + "    \"event-id\" : \"e-1\",\n"
            + "    \"request-id\" : \"r-1\",\n"
            + "    \"request-event-count\" : 2,\n"
            + "    \"timestamp-ms\" : 123,\n"
            + "    \"actor\" : {\n"
            + "      \"id\" : \"user1\"\n"
            + "    },\n"
            + "    \"operation\" : {\n"
            + "      \"operation-type\" : \"create-namespace\",\n"
            + "      \"namespace\" : [ \"a\", \"b\" ]\n"
            + "    }\n"
            + "  } ]\n"
            + "}";
    assertThat(QueryEventsResponseParser.toJson(response, true)).isEqualTo(expected);
  }

  @Test
  void testToJsonWithNullResponse() {
    assertThatNullPointerException()
        .isThrownBy(() -> QueryEventsResponseParser.toJson(null))
        .withMessage("Invalid query events response: null");
  }

  @Test
  void testFromJson() {
    QueryEventsResponse response =
        ImmutableQueryEventsResponse.builder()
            .continuationToken("npt")
            .highestProcessedTimestampMs(5000L)
            .events(List.of(sampleEventWithActor()))
            .build();
    String json =
        "{\"continuation-token\":\"npt\",\"highest-processed-timestamp-ms\":5000,\"events\":[{"
            + "\"event-id\":\"e-1\",\"request-id\":\"r-1\",\"request-event-count\":2,\"timestamp-ms\":123,\"actor\":{\"id\":\"user1\"},\"operation\":{\"operation-type\":\"create-namespace\",\"namespace\":[\"a\",\"b\"]}}]}";

    assertThat(QueryEventsResponseParser.fromJson(json)).isEqualTo(response);
  }

  @Test
  void testFromJsonWithNullInput() {
    assertThatNullPointerException()
        .isThrownBy(() -> QueryEventsResponseParser.fromJson((JsonNode) null))
        .withMessage("Cannot parse query events response from null object");
  }

  @Test
  void testFromJsonWithMissingProperties() {
    QueryEventsResponse missingContinuationTokenExpected =
        ImmutableQueryEventsResponse.builder()
            .highestProcessedTimestampMs(1L)
            .events(List.of())
            .build();
    assertThat(
            QueryEventsResponseParser.fromJson(
                "{\"highest-processed-timestamp-ms\":1,\"events\":[]}"))
        .isEqualTo(missingContinuationTokenExpected);

    String missingHighestProcessedTimestamp = "{\"events\":[]}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> QueryEventsResponseParser.fromJson(missingHighestProcessedTimestamp));

    String missingEvents = "{\"highest-processed-timestamp-ms\":1}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> QueryEventsResponseParser.fromJson(missingEvents));
  }

  @Test
  void testFromJsonWithInvalidProperties() {
    String invalidContinuationToken =
        "{\"continuation-token\":123,\"highest-processed-timestamp-ms\":1,\"events\":[]}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> QueryEventsResponseParser.fromJson(invalidContinuationToken));

    String invalidHighestProcessed =
        "{\"next-page-token\":\"npt\",\"highest-processed-timestamp-ms\":\"x\",\"events\":[]}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> QueryEventsResponseParser.fromJson(invalidHighestProcessed));

    String invalidEvents = "{\"highest-processed-timestamp-ms\":1,\"events\":{}}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> QueryEventsResponseParser.fromJson(invalidEvents));

    String invalidEventsObject = "{\"highest-processed-timestamp-ms\":1,\"events\":[{}]}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> QueryEventsResponseParser.fromJson(invalidEventsObject));
  }

  @Test
  void testRoundTrip() {
    String json =
        "{\"continuation-token\":\"npt\",\"highest-processed-timestamp-ms\":5000,\"events\":[{"
            + "\"event-id\":\"e-1\",\"request-id\":\"r-1\",\"request-event-count\":2,\"timestamp-ms\":123,\"actor\":{\"id\":\"user1\"},\"operation\":{\"operation-type\":\"create-namespace\",\"namespace\":[\"a\",\"b\"]}}]}";
    assertThat(QueryEventsResponseParser.toJson(QueryEventsResponseParser.fromJson(json)))
        .isEqualTo(json);
  }
}
