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
import org.junit.jupiter.api.Test;

public class EventTest {

  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  void testSerializationAndDeserialization() throws Exception {
    XXXOperation op = new XXXOperation();
    op.setType(OperationType.DELETE);

    Event event = new Event();
    event.setEventId("evt-10");
    event.setOperation(op);

    String json = mapper.writeValueAsString(event);
    Event deserialized = mapper.readValue(json, Event.class);

    assertEquals("evt-10", deserialized.getEventId());
    assertEquals(OperationType.DELETE, deserialized.getOperation().getType());
  }
}
