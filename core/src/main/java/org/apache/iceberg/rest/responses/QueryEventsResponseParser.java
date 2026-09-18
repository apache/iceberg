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
import org.apache.iceberg.rest.events.Event;
import org.apache.iceberg.rest.events.EventParser;
import org.apache.iceberg.util.JsonUtil;

public class QueryEventsResponseParser {
  static final String CONTINUATION_TOKEN = "continuation-token";
  static final String EVENTS = "events";

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
    gen.writeStringField(CONTINUATION_TOKEN, response.continuationToken());
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

    return ImmutableQueryEventsResponse.builder()
        .continuationToken(JsonUtil.getString(CONTINUATION_TOKEN, json))
        .events(JsonUtil.getObjectList(EVENTS, json, EventParser::fromJson))
        .build();
  }
}
