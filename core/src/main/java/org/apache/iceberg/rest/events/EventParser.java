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
package org.apache.iceberg.rest.events;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class EventParser {
  static final String EVENT_ID = "event-id";
  static final String REQUEST_ID = "request-id";
  static final String REQUEST_EVENT_COUNT = "request-event-count";
  static final String TIMESTAMP_MS = "timestamp-ms";
  static final String ACTOR = "actor";
  static final String OPERATION = "operation";

  private EventParser() {}

  public static String toJson(Event event) {
    return toJson(event, false);
  }

  public static String toJson(Event event, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(event, gen), pretty);
  }

  public static void toJson(Event event, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != event, "Invalid event: null");

    gen.writeStartObject();
    gen.writeStringField(EVENT_ID, event.eventId());
    gen.writeStringField(REQUEST_ID, event.requestId());
    gen.writeNumberField(REQUEST_EVENT_COUNT, event.requestEventCount());
    gen.writeNumberField(TIMESTAMP_MS, event.timestampMs());

    Map<String, String> actor = event.actor();
    if (actor != null) {
      JsonUtil.writeStringMap(ACTOR, actor, gen);
    }

    gen.writeFieldName(OPERATION);
    CatalogOperationParser.toJson(event.operation(), gen);
    gen.writeEndObject();
  }

  public static Event fromJson(String json) {
    return JsonUtil.parse(json, EventParser::fromJson);
  }

  public static Event fromJson(JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse event from null object");
    Preconditions.checkArgument(json.isObject(), "Cannot parse event from non-object: %s", json);

    ImmutableEvent.Builder builder =
        ImmutableEvent.builder()
            .eventId(JsonUtil.getString(EVENT_ID, json))
            .requestId(JsonUtil.getString(REQUEST_ID, json))
            .requestEventCount(JsonUtil.getInt(REQUEST_EVENT_COUNT, json))
            .timestampMs(JsonUtil.getLong(TIMESTAMP_MS, json))
            .operation(CatalogOperationParser.fromJson(JsonUtil.get(OPERATION, json)));

    if (json.hasNonNull(ACTOR)) {
      builder.actor(JsonUtil.getStringMap(ACTOR, json));
    }

    return builder.build();
  }
}
