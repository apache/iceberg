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
package org.apache.iceberg.events.model;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

public class EventsResponseTest {

  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  void testSerialization() throws Exception {
    Event event1 = new Event();
    event1.setEventId("evt-1");
    XXXOperation op1 = new XXXOperation();
    op1.setType(OperationType.APPEND); // or CREATE_TABLE, DELETE, etc.
    event1.setOperation(op1);

    Event event2 = new Event();
    event2.setEventId("evt-2");
    XXXOperation op2 = new XXXOperation();
    op2.setType(OperationType.APPEND); // or CREATE_TABLE, DELETE, etc.
    event1.setOperation(op2);

    EventsResponse response = new EventsResponse();

    response.setHighestProcessedSequence(200L);
    response.setHighestProcessedTimestampMs(1690000001000L);
    response.setNextPageToken("token-123");
    response.setEvents(Arrays.asList(event1, event2));

    String json = mapper.writeValueAsString(response);

    assertNotNull(json);
    assertTrue(json.contains("\"highest-processed-sequence\":200"));
    assertTrue(json.contains("\"highest-processed-timestamp-ms\":1690000001000"));
    assertTrue(json.contains("\"next-page-token\":\"token-123\""));
    assertTrue(json.contains("\"events\""));
  }

  @Test
  void testDeserialization() throws Exception {
    String json =
        "{ \"highest-processed-sequence\": 300, \"highest-processed-timestamp-ms\": 1690000010000, \"next-page-token\": \"token-456\", \"events\": [{\"event-id\": \"evt-3\", \"operation\": {\"type\": \"CREATE_TABLE\"}}]}";

    EventsResponse response = mapper.readValue(json, EventsResponse.class);
    assertEquals(300L, response.getHighestProcessedSequence());
    assertEquals("evt-3", response.getEvents().get(0).getEventId());
    assertEquals(OperationType.CREATE_TABLE, response.getEvents().get(0).getOperation().getType());
  }
}
