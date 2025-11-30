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
package org.apache.iceberg.rest.events.parsers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.events.Event;
import org.apache.iceberg.rest.events.ImmutableEvent;
import org.apache.iceberg.rest.events.operations.ImmutableCreateNamespaceOperation;
import org.apache.iceberg.rest.events.operations.Operation;
import org.junit.jupiter.api.Test;

public class TestEventParser {

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

  private static Event sampleEventWithoutActor() {
    return ImmutableEvent.builder()
        .eventId("e-2")
        .requestId("r-2")
        .eventCount(1)
        .timestampMs(999L)
        .operation(sampleOperation())
        .build();
  }

  @Test
  void testToJson() {
    Event event = sampleEventWithActor();
    String expected =
        "{\"event-id\":\"e-1\",\"request-id\":\"r-1\",\"event-count\":2,\"timestamp-ms\":123,\"actor\":\"user1\",\"operation\":{\"operation-type\":\"create-namespace\",\"namespace\":[\"a\",\"b\"]}}";
    assertThat(EventParser.toJson(event)).isEqualTo(expected);
  }

  @Test
  void testToJsonPretty() {
    Event event = sampleEventWithActor();
    String expected =
        "{\n"
            + "  \"event-id\" : \"e-1\",\n"
            + "  \"request-id\" : \"r-1\",\n"
            + "  \"event-count\" : 2,\n"
            + "  \"timestamp-ms\" : 123,\n"
            + "  \"actor\" : \"user1\",\n"
            + "  \"operation\" : {\n"
            + "    \"operation-type\" : \"create-namespace\",\n"
            + "    \"namespace\" : [ \"a\", \"b\" ]\n"
            + "  }\n"
            + "}";
    assertThat(EventParser.toJsonPretty(event)).isEqualTo(expected);
  }

  @Test
  void testToJsonWithNullEvent() {
    assertThatNullPointerException()
        .isThrownBy(() -> EventParser.toJson(null))
        .withMessage("Invalid event: null");
  }

  @Test
  void testToJsonWithoutActor() {
    Event event = sampleEventWithoutActor();
    String expected =
        "{\"event-id\":\"e-2\",\"request-id\":\"r-2\",\"event-count\":1,\"timestamp-ms\":999,\"operation\":{\"operation-type\":\"create-namespace\",\"namespace\":[\"a\",\"b\"]}}";
    assertThat(EventParser.toJson(event)).isEqualTo(expected);
  }

  @Test
  void testFromJson() {
    String json =
        "{\"event-id\":\"e-1\",\"request-id\":\"r-1\",\"event-count\":2,\"timestamp-ms\":123,\"actor\":\"user1\",\"operation\":{\"operation-type\":\"create-namespace\",\"namespace\":[\"a\",\"b\"]}}";
    Event expected = sampleEventWithActor();
    assertThat(EventParser.fromJson(json)).isEqualTo(expected);
  }

  @Test
  void testFromJsonWithNullInput() {
    assertThatNullPointerException()
        .isThrownBy(() -> EventParser.fromJson((JsonNode) null))
        .withMessage("Cannot parse event from null object");
  }

  @Test
  void testFromJsonWithMissingProperties() {
    String missingEventId =
        "{\"request-id\":\"r-1\",\"event-count\":2,\"timestamp-ms\":123,\"operation\":{\"operation-type\":\"create-namespace\",\"namespace\":[\"a\",\"b\"]}}";
    assertThatIllegalArgumentException().isThrownBy(() -> EventParser.fromJson(missingEventId));

    String missingRequestId =
        "{\"event-id\":\"e-1\",\"event-count\":2,\"timestamp-ms\":123,\"operation\":{\"operation-type\":\"create-namespace\",\"namespace\":[\"a\",\"b\"]}}";
    assertThatIllegalArgumentException().isThrownBy(() -> EventParser.fromJson(missingRequestId));

    String missingEventCount =
        "{\"event-id\":\"e-1\",\"request-id\":\"r-1\",\"timestamp-ms\":123,\"operation\":{\"operation-type\":\"create-namespace\",\"namespace\":[\"a\",\"b\"]}}";
    assertThatIllegalArgumentException().isThrownBy(() -> EventParser.fromJson(missingEventCount));

    String missingTimestamp =
        "{\"event-id\":\"e-1\",\"request-id\":\"r-1\",\"event-count\":2,\"operation\":{\"operation-type\":\"create-namespace\",\"namespace\":[\"a\",\"b\"]}}";
    assertThatIllegalArgumentException().isThrownBy(() -> EventParser.fromJson(missingTimestamp));

    String missingOperation =
        "{\"event-id\":\"e-1\",\"request-id\":\"r-1\",\"event-count\":2,\"timestamp-ms\":123}";
    assertThatIllegalArgumentException().isThrownBy(() -> EventParser.fromJson(missingOperation));
  }

  @Test
  void testFromJsonWithInvalidProperties() {
    // event-id present but not a string
    String invalidEventId =
        "{\"event-id\":1,\"request-id\":\"r-1\",\"event-count\":2,\"timestamp-ms\":123,\"operation\":{\"operation-type\":\"create-namespace\",\"namespace\":[\"a\",\"b\"]}}";
    assertThatIllegalArgumentException().isThrownBy(() -> EventParser.fromJson(invalidEventId));

    // request-id present but not a string
    String invalidRequestId =
        "{\"event-id\":\"e-1\",\"request-id\":1,\"event-count\":2,\"timestamp-ms\":123,\"operation\":{\"operation-type\":\"create-namespace\",\"namespace\":[\"a\",\"b\"]}}";
    assertThatIllegalArgumentException().isThrownBy(() -> EventParser.fromJson(invalidRequestId));

    // event-count present but not an integer
    String invalidEventCount =
        "{\"event-id\":\"e-1\",\"request-id\":\"r-1\",\"event-count\":\"two\",\"timestamp-ms\":123,\"operation\":{\"operation-type\":\"create-namespace\",\"namespace\":[\"a\",\"b\"]}}";
    assertThatIllegalArgumentException().isThrownBy(() -> EventParser.fromJson(invalidEventCount));

    // timestamp-ms present but not a long
    String invalidTimestamp =
        "{\"event-id\":\"e-1\",\"request-id\":\"r-1\",\"event-count\":2,\"timestamp-ms\":\"123\",\"operation\":{\"operation-type\":\"create-namespace\",\"namespace\":[\"a\",\"b\"]}}";
    assertThatIllegalArgumentException().isThrownBy(() -> EventParser.fromJson(invalidTimestamp));

    // actor present but not a string
    String invalidActor =
        "{\"event-id\":\"e-1\",\"request-id\":\"r-1\",\"event-count\":2,\"timestamp-ms\":123,\"actor\":123,\"operation\":{\"operation-type\":\"create-namespace\",\"namespace\":[\"a\",\"b\"]}}";
    assertThatIllegalArgumentException().isThrownBy(() -> EventParser.fromJson(invalidActor));
  }
}
