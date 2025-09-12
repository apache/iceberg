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
package org.apache.iceberg.events.rest;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Collections;
import org.apache.iceberg.events.model.EventsResponse;
import org.junit.jupiter.api.Test;

public class TestEventsResponseParser {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void testRoundTripString() throws IOException {
    EventsResponse response = new EventsResponse();
    response.setNextPageToken("token-123");
    response.setHighestProcessedTimestampMs(1720000000000L);
    response.setHighestProcessedSequence(99L);
    response.setEvents(Collections.emptyList());

    String json = EventsResponseParser.toJson(response);
    EventsResponse parsed = EventsResponseParser.fromJson(json);

    assertThat(parsed.getNextPageToken()).isEqualTo("token-123");
    assertThat(parsed.getHighestProcessedTimestampMs()).isEqualTo(1720000000000L);
    assertThat(parsed.getHighestProcessedSequence()).isEqualTo(99L);
    assertThat(parsed.getEvents()).isEmpty();
  }

  @Test
  public void testRoundTripJsonNode() throws IOException {
    EventsResponse response = new EventsResponse();
    response.setNextPageToken("page-xyz");

    String json = EventsResponseParser.toJson(response);
    JsonNode node = MAPPER.readTree(json);

    EventsResponse parsed = EventsResponseParser.fromJson(node);

    assertThat(parsed.getNextPageToken()).isEqualTo("page-xyz");
  }

  @Test
  public void testRoundTripJsonGenerator() throws IOException {
    EventsResponse response = new EventsResponse();
    response.setHighestProcessedSequence(12345L);

    StringWriter writer = new StringWriter();
    try (JsonGenerator gen = new JsonFactory().createGenerator(writer)) {
      EventsResponseParser.toJson(response, gen);
    }

    EventsResponse parsed = EventsResponseParser.fromJson(writer.toString());
    assertThat(parsed.getHighestProcessedSequence()).isEqualTo(12345L);
  }
}
