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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.events.Event;
import org.apache.iceberg.rest.events.parsers.EventParser;
import org.apache.iceberg.util.JsonUtil;

public class EventsResponseParser {
  private static final String NEXT_PAGE_TOKEN = "next-page-token";
  private static final String HIGHEST_PROCESSED_TIMESTAMP_MS = "highest-processed-timestamp-ms";
  private static final String EVENTS = "events";

  private EventsResponseParser() {}

  public static String toJson(EventsResponse eventsResponse) {
    return toJson(eventsResponse, false);
  }

  public static String toJsonPretty(EventsResponse eventsResponse) {
    return toJson(eventsResponse, true);
  }

  private static String toJson(EventsResponse eventsResponse, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(eventsResponse, gen), pretty);
  }

  public static void toJson(EventsResponse eventsResponse, JsonGenerator gen) throws IOException {
    Preconditions.checkNotNull(eventsResponse, "Invalid events response: null");

    gen.writeStartObject();

    if (eventsResponse.nextPageToken() != null)
      gen.writeStringField(NEXT_PAGE_TOKEN, eventsResponse.nextPageToken());

    gen.writeNumberField(
        HIGHEST_PROCESSED_TIMESTAMP_MS, eventsResponse.highestProcessedTimestampMs());

    gen.writeArrayFieldStart(EVENTS);
    for (Event event : eventsResponse.events()) {
      EventParser.toJson(event, gen);
    }
    gen.writeEndArray();

    gen.writeEndObject();
  }

  public static EventsResponse fromJson(String json) {
    return JsonUtil.parse(json, EventsResponseParser::fromJson);
  }

  public static EventsResponse fromJson(JsonNode json) {
    Preconditions.checkNotNull(json, "Cannot parse events response from null object");

    Long highestProcessedTimestampMs = JsonUtil.getLong(HIGHEST_PROCESSED_TIMESTAMP_MS, json);
    List<Event> events = JsonUtil.getObjectList(EVENTS, json, EventParser::fromJson);

    ImmutableEventsResponse.Builder builder =
        ImmutableEventsResponse.builder()
            .highestProcessedTimestampMs(highestProcessedTimestampMs)
            .events(events);

    if (json.has(NEXT_PAGE_TOKEN)) {
      builder.nextPageToken(JsonUtil.getString(NEXT_PAGE_TOKEN, json));
    }

    return builder.build();
  }
}
