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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.events.Event;
import org.apache.iceberg.rest.events.ImmutableEvent;
import org.apache.iceberg.rest.events.operations.Operation;
import org.apache.iceberg.util.JsonUtil;

public class EventParser {
  private static final String EVENT_ID = "event-id";
  private static final String REQUEST_ID = "request-id";
  private static final String EVENT_COUNT = "event-count";
  private static final String TIMESTAMP_MS = "timestamp-ms";
  private static final String ACTOR = "actor";
  private static final String OPERATION = "operation";

  private EventParser() {}

  public static String toJson(Event event) {
    return toJson(event, false);
  }

  public static String toJsonPretty(Event event) {
    return toJson(event, true);
  }

  private static String toJson(Event event, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(event, gen), pretty);
  }

  public static void toJson(Event event, JsonGenerator gen) throws IOException {
    Preconditions.checkNotNull(null != event, "Invalid event: null");

    gen.writeStartObject();

    gen.writeStringField(EVENT_ID, event.eventId());
    gen.writeStringField(REQUEST_ID, event.requestId());
    gen.writeNumberField(EVENT_COUNT, event.eventCount());
    gen.writeNumberField(TIMESTAMP_MS, event.timestampMs());

    if (event.actor() != null) {
      gen.writeStringField(ACTOR, event.actor());
    }

    gen.writeFieldName(OPERATION);
    OperationParser.toJson(event.operation(), gen);

    gen.writeEndObject();
  }

  public static Event fromJson(String json) {
    return JsonUtil.parse(json, EventParser::fromJson);
  }

  public static Event fromJson(JsonNode json) {
    Preconditions.checkNotNull(null != json, "Cannot parse event from null object");

    String eventId = JsonUtil.getString(EVENT_ID, json);
    String requestId = JsonUtil.getString(REQUEST_ID, json);
    int eventCount = JsonUtil.getInt(EVENT_COUNT, json);
    long timestampMs = JsonUtil.getLong(TIMESTAMP_MS, json);
    Operation operation = OperationParser.fromJson(JsonUtil.get(OPERATION, json));

    ImmutableEvent.Builder builder =
        ImmutableEvent.builder()
            .eventId(eventId)
            .requestId(requestId)
            .eventCount(eventCount)
            .timestampMs(timestampMs);

    if (json.has(ACTOR)) {
      builder.actor(JsonUtil.getString(ACTOR, json));
    }

    builder.operation(operation);
    return builder.build();
  }
}
