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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class TestEventParser {

  @Test
  public void testRoundTripCreateNamespaceEvent() {
    Event event =
        ImmutableEvent.builder()
            .eventId("evt-001")
            .requestId("req-001")
            .requestEventCount(1)
            .timestampMs(1714000000000L)
            .actor(ImmutableMap.of("user", "alice"))
            .operation(
                ImmutableCreateNamespaceOperation.builder()
                    .namespace(Namespace.of("accounting", "tax"))
                    .putProperties("owner", "alice")
                    .build())
            .build();

    String json = EventParser.toJson(event);
    Event parsed = EventParser.fromJson(json);

    assertThat(parsed.eventId()).isEqualTo(event.eventId());
    assertThat(parsed.requestId()).isEqualTo(event.requestId());
    assertThat(parsed.requestEventCount()).isEqualTo(event.requestEventCount());
    assertThat(parsed.timestampMs()).isEqualTo(event.timestampMs());
    assertThat(parsed.actor()).isEqualTo(event.actor());
    assertThat(parsed.operation().operationType()).isEqualTo("create-namespace");

    CreateNamespaceOperation op = (CreateNamespaceOperation) parsed.operation();
    assertThat(op.namespace()).isEqualTo(Namespace.of("accounting", "tax"));
    assertThat(op.properties()).containsEntry("owner", "alice");
  }

  @Test
  public void testRoundTripDropNamespaceEvent() {
    Event event =
        ImmutableEvent.builder()
            .eventId("evt-002")
            .requestId("req-002")
            .requestEventCount(1)
            .timestampMs(1714000000000L)
            .operation(
                ImmutableDropNamespaceOperation.builder()
                    .namespace(Namespace.of("old_namespace"))
                    .build())
            .build();

    String json = EventParser.toJson(event);
    Event parsed = EventParser.fromJson(json);

    assertThat(parsed.eventId()).isEqualTo("evt-002");
    assertThat(parsed.actor()).isNull();
    assertThat(parsed.operation().operationType()).isEqualTo("drop-namespace");

    DropNamespaceOperation op = (DropNamespaceOperation) parsed.operation();
    assertThat(op.namespace()).isEqualTo(Namespace.of("old_namespace"));
  }

  @Test
  public void testRoundTripDropTableEvent() {
    Event event =
        ImmutableEvent.builder()
            .eventId("evt-003")
            .requestId("req-003")
            .requestEventCount(2)
            .timestampMs(1714000000000L)
            .actor(ImmutableMap.of("user", "bob", "role", "admin"))
            .operation(
                ImmutableDropTableOperation.builder()
                    .identifier(TableIdentifier.of(Namespace.of("db"), "users"))
                    .tableUuid("123e4567-e89b-12d3-a456-426614174000")
                    .purge(true)
                    .build())
            .build();

    String json = EventParser.toJson(event);
    Event parsed = EventParser.fromJson(json);

    assertThat(parsed.eventId()).isEqualTo("evt-003");
    assertThat(parsed.requestEventCount()).isEqualTo(2);
    assertThat(parsed.operation().operationType()).isEqualTo("drop-table");

    DropTableOperation op = (DropTableOperation) parsed.operation();
    assertThat(op.identifier()).isEqualTo(TableIdentifier.of(Namespace.of("db"), "users"));
    assertThat(op.tableUuid()).isEqualTo("123e4567-e89b-12d3-a456-426614174000");
    assertThat(op.purge()).isTrue();
  }

  @Test
  public void testDropTableWithoutPurge() {
    Event event =
        ImmutableEvent.builder()
            .eventId("evt-004")
            .requestId("req-004")
            .requestEventCount(1)
            .timestampMs(1714000000000L)
            .operation(
                ImmutableDropTableOperation.builder()
                    .identifier(TableIdentifier.of(Namespace.of("db"), "temp"))
                    .tableUuid("abc-def-123")
                    .build())
            .build();

    String json = EventParser.toJson(event);
    Event parsed = EventParser.fromJson(json);

    DropTableOperation op = (DropTableOperation) parsed.operation();
    assertThat(op.purge()).isNull();
  }

  @Test
  public void testRoundTripRenameTableEvent() {
    Event event =
        ImmutableEvent.builder()
            .eventId("evt-005")
            .requestId("req-005")
            .requestEventCount(1)
            .timestampMs(1714000000000L)
            .operation(
                ImmutableRenameTableOperation.builder()
                    .source(TableIdentifier.of(Namespace.of("db"), "old_name"))
                    .destination(TableIdentifier.of(Namespace.of("db"), "new_name"))
                    .tableUuid("uuid-123")
                    .build())
            .build();

    String json = EventParser.toJson(event);
    Event parsed = EventParser.fromJson(json);

    assertThat(parsed.operation().operationType()).isEqualTo("rename-table");
    RenameTableOperation op = (RenameTableOperation) parsed.operation();
    assertThat(op.source()).isEqualTo(TableIdentifier.of(Namespace.of("db"), "old_name"));
    assertThat(op.destination()).isEqualTo(TableIdentifier.of(Namespace.of("db"), "new_name"));
    assertThat(op.tableUuid()).isEqualTo("uuid-123");
  }

  @Test
  public void testRoundTripDropViewEvent() {
    Event event =
        ImmutableEvent.builder()
            .eventId("evt-006")
            .requestId("req-006")
            .requestEventCount(1)
            .timestampMs(1714000000000L)
            .operation(
                ImmutableDropViewOperation.builder()
                    .identifier(TableIdentifier.of(Namespace.of("analytics"), "daily_report"))
                    .viewUuid("view-uuid-456")
                    .build())
            .build();

    String json = EventParser.toJson(event);
    Event parsed = EventParser.fromJson(json);

    assertThat(parsed.operation().operationType()).isEqualTo("drop-view");
    DropViewOperation op = (DropViewOperation) parsed.operation();
    assertThat(op.identifier())
        .isEqualTo(TableIdentifier.of(Namespace.of("analytics"), "daily_report"));
    assertThat(op.viewUuid()).isEqualTo("view-uuid-456");
  }

  @Test
  public void testRoundTripRenameViewEvent() {
    Event event =
        ImmutableEvent.builder()
            .eventId("evt-007")
            .requestId("req-007")
            .requestEventCount(1)
            .timestampMs(1714000000000L)
            .operation(
                ImmutableRenameViewOperation.builder()
                    .source(TableIdentifier.of(Namespace.of("db"), "old_view"))
                    .destination(TableIdentifier.of(Namespace.of("db"), "new_view"))
                    .viewUuid("view-uuid-789")
                    .build())
            .build();

    String json = EventParser.toJson(event);
    Event parsed = EventParser.fromJson(json);

    assertThat(parsed.operation().operationType()).isEqualTo("rename-view");
    RenameViewOperation op = (RenameViewOperation) parsed.operation();
    assertThat(op.source()).isEqualTo(TableIdentifier.of(Namespace.of("db"), "old_view"));
    assertThat(op.destination()).isEqualTo(TableIdentifier.of(Namespace.of("db"), "new_view"));
    assertThat(op.viewUuid()).isEqualTo("view-uuid-789");
  }

  @Test
  public void testRoundTripUpdateNamespacePropertiesEvent() {
    Event event =
        ImmutableEvent.builder()
            .eventId("evt-008")
            .requestId("req-008")
            .requestEventCount(1)
            .timestampMs(1714000000000L)
            .operation(
                ImmutableUpdateNamespacePropertiesOperation.builder()
                    .namespace(Namespace.of("production"))
                    .updated(ImmutableList.of("owner", "team"))
                    .removed(ImmutableList.of("deprecated"))
                    .build())
            .build();

    String json = EventParser.toJson(event);
    Event parsed = EventParser.fromJson(json);

    assertThat(parsed.operation().operationType()).isEqualTo("update-namespace-properties");
    UpdateNamespacePropertiesOperation op = (UpdateNamespacePropertiesOperation) parsed.operation();
    assertThat(op.namespace()).isEqualTo(Namespace.of("production"));
    assertThat(op.updated()).containsExactly("owner", "team");
    assertThat(op.removed()).containsExactly("deprecated");
    assertThat(op.missing()).isEmpty();
  }

  @Test
  public void testRoundTripCustomOperationEvent() {
    Event event =
        ImmutableEvent.builder()
            .eventId("evt-009")
            .requestId("req-009")
            .requestEventCount(1)
            .timestampMs(1714000000000L)
            .operation(
                ImmutableCustomOperation.builder()
                    .customType("x-compact-table")
                    .identifier(TableIdentifier.of(Namespace.of("db"), "events"))
                    .tableUuid("table-uuid-123")
                    .putAdditionalProperties("compaction-strategy", "binpack")
                    .build())
            .build();

    String json = EventParser.toJson(event);
    Event parsed = EventParser.fromJson(json);

    assertThat(parsed.operation().operationType()).isEqualTo("custom");
    CustomOperation op = (CustomOperation) parsed.operation();
    assertThat(op.customType()).isEqualTo("x-compact-table");
    assertThat(op.identifier()).isEqualTo(TableIdentifier.of(Namespace.of("db"), "events"));
    assertThat(op.tableUuid()).isEqualTo("table-uuid-123");
    assertThat(op.namespace()).isNull();
    assertThat(op.viewUuid()).isNull();
    assertThat(op.additionalProperties()).containsEntry("compaction-strategy", "binpack");
  }

  @Test
  public void testForwardCompatibilityUnknownOperationType() {
    // Unknown operation types should not throw — they should be deserialized as CustomOperation
    String json =
        "{\"event-id\":\"evt-010\",\"request-id\":\"req-010\","
            + "\"request-event-count\":1,\"timestamp-ms\":1714000000000,"
            + "\"operation\":{\"operation-type\":\"x-future-operation\"}}";

    Event parsed = EventParser.fromJson(json);
    assertThat(parsed.operation()).isInstanceOf(CustomOperation.class);
    assertThat(parsed.operation().operationType()).isEqualTo("x-future-operation");
  }

  @Test
  public void testCreateNamespaceWithEmptyProperties() {
    Event event =
        ImmutableEvent.builder()
            .eventId("evt-011")
            .requestId("req-011")
            .requestEventCount(1)
            .timestampMs(1714000000000L)
            .operation(
                ImmutableCreateNamespaceOperation.builder()
                    .namespace(Namespace.of("empty_ns"))
                    .build())
            .build();

    String json = EventParser.toJson(event);
    Event parsed = EventParser.fromJson(json);

    CreateNamespaceOperation op = (CreateNamespaceOperation) parsed.operation();
    assertThat(op.properties()).isEmpty();
  }

  @Test
  public void testNullEventThrows() {
    assertThatThrownBy(() -> EventParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid event: null");
  }

  @Test
  public void testNullJsonThrows() {
    assertThatThrownBy(() -> EventParser.fromJson((String) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("argument \"content\" is null");
  }
}
