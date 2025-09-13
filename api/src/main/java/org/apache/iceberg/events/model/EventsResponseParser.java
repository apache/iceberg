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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.apache.iceberg.events.model.EventsResponse;

public class EventsResponseParser {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static String toJson(EventsResponse response) throws IOException {
    return MAPPER.writeValueAsString(response);
  }

  public static EventsResponse fromJson(String json) throws IOException {
    return MAPPER.readValue(json, EventsResponse.class);
  }

  public static void toJson(EventsResponse response, JsonGenerator gen) throws IOException {
    MAPPER.writeValue(gen, response);
  }

  public static EventsResponse fromJson(JsonNode jsonNode) throws IOException {
    return MAPPER.treeToValue(jsonNode, EventsResponse.class);
  }
}
