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
package org.apache.iceberg.events.controller;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.apache.iceberg.events.model.EventsResponse;
import org.apache.iceberg.events.model.GetEventsRequest;
import org.apache.iceberg.events.service.EventsService;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

public class EventsControllerTest {

  @Test
  void testGetEvents() {
    // Arrange
    EventsService eventsService = mock(EventsService.class);
    EventsController controller = new EventsController(eventsService);

    String prefix = "testPrefix"; // sample path variable
    GetEventsRequest request = new GetEventsRequest();
    EventsResponse mockResponse = new EventsResponse();

    when(eventsService.fetchEvents(prefix, request)).thenReturn(mockResponse);

    // Act
    ResponseEntity<?> responseEntity = controller.getEvents(prefix, request);

    // Assert
    assertNotNull(responseEntity);
    assertEquals(mockResponse, responseEntity.getBody());
    verify(eventsService, times(1)).fetchEvents(prefix, request);
  }
}
