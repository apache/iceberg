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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.rest.events.Event;
import org.apache.iceberg.rest.events.EventParser;
import org.apache.iceberg.util.JsonUtil;

public class QueryEventsResponseParser {

  private static final String NEXT_PAGE_TOKEN = "next-page-token";
  private static final String HIGHEST_PROCESSED_TIMESTAMP_MS = "highest-processed-timestamp-ms";
  private static final String EVENTS = "events";

  private QueryEventsResponseParser() {}

  public static String toJson(QueryEventsResponse response) {
    return toJson(response, false);
  }

  public static String toJson(QueryEventsResponse response, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(response, gen), pretty);
  }

  public static void toJson(QueryEventsResponse response, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != response, "Invalid query events response: null");

    gen.writeStartObject();

    gen.writeStringField(NEXT_PAGE_TOKEN, response.nextPageToken());
    gen.writeNumberField(HIGHEST_PROCESSED_TIMESTAMP_MS, response.highestProcessedTimestampMs());

    gen.writeArrayFieldStart(EVENTS);
    for (Event event : response.events()) {
      EventParser.toJson(event, gen);
    }
    gen.writeEndArray();

    gen.writeEndObject();
  }

  public static QueryEventsResponse fromJson(String json) {
    return JsonUtil.parse(json, QueryEventsResponseParser::fromJson);
  }

  public static QueryEventsResponse fromJson(JsonNode json) {
    Preconditions.checkArgument(
        null != json, "Cannot parse query events response from null object");
    Preconditions.checkArgument(
        json.isObject(), "Cannot parse query events response from non-object: %s", json);

    ImmutableList.Builder<Event> events = ImmutableList.builder();
    for (JsonNode eventNode : json.get(EVENTS)) {
      events.add(EventParser.fromJson(eventNode));
    }

    return ImmutableQueryEventsResponse.builder()
        .nextPageToken(JsonUtil.getString(NEXT_PAGE_TOKEN, json))
        .highestProcessedTimestampMs(JsonUtil.getLong(HIGHEST_PROCESSED_TIMESTAMP_MS, json))
        .events(events.build())
        .build();
  }
}
