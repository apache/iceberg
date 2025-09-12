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

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import org.apache.iceberg.events.model.GetEventsRequest;
import org.junit.jupiter.api.Test;

public class TestGetEventsRequestParser {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void testRoundTripString() throws IOException {
    GetEventsRequest request = new GetEventsRequest();
    request.setNextPageToken("token-xyz");
    request.setPageSize(25);
    request.setAfterTimestampMs(1720000000000L);
    request.setAfterSequence(123L);
    request.setUsers(Arrays.asList("alice", "bob"));

    String json = GetEventsRequestParser.toJson(request);
    GetEventsRequest parsed = GetEventsRequestParser.fromJson(json);

    assertThat(parsed.getNextPageToken()).isEqualTo("token-xyz");
    assertThat(parsed.getPageSize()).isEqualTo(25);
    assertThat(parsed.getAfterTimestampMs()).isEqualTo(1720000000000L);
    assertThat(parsed.getAfterSequence()).isEqualTo(123L);
    assertThat(parsed.getUsers()).containsExactly("alice", "bob");
  }

  @Test
  public void testRoundTripJsonNode() throws IOException {
    GetEventsRequest request = new GetEventsRequest();
    request.setNextPageToken("page-1");

    // write to JsonNode
    String json = GetEventsRequestParser.toJson(request);
    JsonNode node = MAPPER.readTree(json);

    // parse back
    GetEventsRequest parsed = GetEventsRequestParser.fromJson(node);

    assertThat(parsed.getNextPageToken()).isEqualTo("page-1");
  }

  @Test
  public void testRoundTripJsonGenerator() throws IOException {
    GetEventsRequest request = new GetEventsRequest();
    request.setPageSize(10);

    StringWriter writer = new StringWriter();
    try (JsonGenerator gen = new JsonFactory().createGenerator(writer)) {
      GetEventsRequestParser.toJson(request, gen);
    }

    GetEventsRequest parsed = GetEventsRequestParser.fromJson(writer.toString());
    assertThat(parsed.getPageSize()).isEqualTo(10);
  }
}
