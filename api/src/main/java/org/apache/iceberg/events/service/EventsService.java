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
package org.apache.iceberg.events.service;

import org.apache.iceberg.events.model.EventsResponse;
import org.apache.iceberg.events.model.GetEventsRequest;
import org.springframework.stereotype.Service;

@Service
public class EventsService {

  public EventsResponse fetchEvents(String prefix, GetEventsRequest request) {
    // For now, you can mock data for testing
    EventsResponse response = new EventsResponse();

    response.setHighestProcessedSequence(100L);
    response.setHighestProcessedTimestampMs(System.currentTimeMillis());

    // Add mock events or integrate with your Event repository
    // Example:
    // List<Event> events = eventRepository.getEvents(request);
    // response.setEvents(events);

    return response;
  }

  public Object getEvents(GetEventsRequest request) {
    return null;
  }
}
