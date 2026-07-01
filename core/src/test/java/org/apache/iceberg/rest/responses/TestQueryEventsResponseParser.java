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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.events.CreateNamespaceOperation;
import org.apache.iceberg.rest.events.DropTableOperation;
import org.apache.iceberg.rest.events.Event;
import org.apache.iceberg.rest.events.ImmutableCreateNamespaceOperation;
import org.apache.iceberg.rest.events.ImmutableDropTableOperation;
import org.apache.iceberg.rest.events.ImmutableEvent;
import org.junit.jupiter.api.Test;

public class TestQueryEventsResponseParser {

  @Test
  public void testRoundTripWithMultipleEvents() {
    Event createNsEvent =
        ImmutableEvent.builder()
            .eventId("evt-001")
            .requestId("req-001")
            .requestEventCount(1)
            .timestampMs(1714000000000L)
            .actor(ImmutableMap.of("user", "alice"))
            .operation(
                ImmutableCreateNamespaceOperation.builder()
                    .namespace(Namespace.of("analytics"))
                    .putProperties("owner", "data-team")
                    .build())
            .build();

    Event dropTableEvent =
        ImmutableEvent.builder()
            .eventId("evt-002")
            .requestId("req-002")
            .requestEventCount(1)
            .timestampMs(1714000001000L)
            .operation(
                ImmutableDropTableOperation.builder()
                    .identifier(TableIdentifier.of(Namespace.of("analytics"), "old_metrics"))
                    .tableUuid("abc-123")
                    .purge(false)
                    .build())
            .build();

    QueryEventsResponse response =
        ImmutableQueryEventsResponse.builder()
            .nextPageToken("next-token-xyz")
            .highestProcessedTimestampMs(1714000001000L)
            .events(ImmutableList.of(createNsEvent, dropTableEvent))
            .build();

    String json = QueryEventsResponseParser.toJson(response);
    QueryEventsResponse parsed = QueryEventsResponseParser.fromJson(json);

    assertThat(parsed.nextPageToken()).isEqualTo("next-token-xyz");
    assertThat(parsed.highestProcessedTimestampMs()).isEqualTo(1714000001000L);
    assertThat(parsed.events()).hasSize(2);

    Event parsedEvent1 = parsed.events().get(0);
    assertThat(parsedEvent1.eventId()).isEqualTo("evt-001");
    assertThat(parsedEvent1.operation()).isInstanceOf(CreateNamespaceOperation.class);

    Event parsedEvent2 = parsed.events().get(1);
    assertThat(parsedEvent2.eventId()).isEqualTo("evt-002");
    assertThat(parsedEvent2.operation()).isInstanceOf(DropTableOperation.class);
    assertThat(((DropTableOperation) parsedEvent2.operation()).purge()).isFalse();
  }

  @Test
  public void testRoundTripEmptyEvents() {
    QueryEventsResponse response =
        ImmutableQueryEventsResponse.builder()
            .nextPageToken("end-token")
            .highestProcessedTimestampMs(1714000000000L)
            .events(ImmutableList.of())
            .build();

    String json = QueryEventsResponseParser.toJson(response);
    QueryEventsResponse parsed = QueryEventsResponseParser.fromJson(json);

    assertThat(parsed.nextPageToken()).isEqualTo("end-token");
    assertThat(parsed.highestProcessedTimestampMs()).isEqualTo(1714000000000L);
    assertThat(parsed.events()).isEmpty();
  }
}
