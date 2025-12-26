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
package org.apache.iceberg.rest;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.rest.requests.PostEventsRequest;
import org.apache.iceberg.rest.responses.EventsResponse;
import org.junit.Test;

public class TestEventsEndpoint {

  @Test
  public void testPostAndGetEvents() {
    RESTCatalogAdapter adapter = new RESTCatalogAdapter(null) {};

    PostEventsRequest req = ImmutablePostEventsRequest.builder().addAllEvents(Collections.emptyList()).build();

    EventsResponse postResp = adapter.handleRequest(Route.EVENTS_POST, Collections.emptyMap(),
        ImmutableHTTPRequest.builder().method(HTTPRequest.HTTPMethod.POST).path("v1/test/events").build(), EventsResponse.class, headers -> {});

    assertThat(postResp).isNotNull();

    EventsResponse getResp = adapter.handleRequest(Route.EVENTS_GET, Collections.emptyMap(),
        ImmutableHTTPRequest.builder().method(HTTPRequest.HTTPMethod.GET).path("v1/test/events").build(), EventsResponse.class, headers -> {});

    assertThat(getResp).isNotNull();
    assertThat(getResp.events()).isNotNull();
  }
}
