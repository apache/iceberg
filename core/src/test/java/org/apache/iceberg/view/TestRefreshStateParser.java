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
package org.apache.iceberg.view;

import java.util.Arrays;
import java.util.Collections;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestRefreshStateParser {

  @Test
  public void testRoundTripSourceTableState() {
    SourceTableState tableState =
        new SourceTableState(
            "events",
            Arrays.asList("default"),
            null,
            "d4a10b5c-1e8a-4b72-9d67-3f4a8c9e1b2d",
            6148331192489823102L,
            null);

    RefreshState refreshState =
        new RefreshState(1, Collections.singletonList(tableState), 1573518435000L);

    String json = RefreshStateParser.toJson(refreshState);
    RefreshState parsed = RefreshStateParser.fromJson(json);

    Assertions.assertThat(parsed.viewVersionId()).isEqualTo(1);
    Assertions.assertThat(parsed.refreshStartTimestampMs()).isEqualTo(1573518435000L);
    Assertions.assertThat(parsed.sourceStates()).hasSize(1);

    SourceState source = parsed.sourceStates().get(0);
    Assertions.assertThat(source).isInstanceOf(SourceTableState.class);
    Assertions.assertThat(source.type()).isEqualTo("table");
    Assertions.assertThat(source.name()).isEqualTo("events");
    Assertions.assertThat(source.namespace()).containsExactly("default");
    Assertions.assertThat(source.catalog()).isNull();
    Assertions.assertThat(source.uuid()).isEqualTo("d4a10b5c-1e8a-4b72-9d67-3f4a8c9e1b2d");

    SourceTableState parsedTable = (SourceTableState) source;
    Assertions.assertThat(parsedTable.snapshotId()).isEqualTo(6148331192489823102L);
    Assertions.assertThat(parsedTable.ref()).isNull();
  }

  @Test
  public void testRoundTripSourceViewState() {
    SourceViewState viewState =
        new SourceViewState(
            "daily_summary",
            Arrays.asList("analytics", "views"),
            "other_catalog",
            "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            5);

    RefreshState refreshState =
        new RefreshState(2, Collections.singletonList(viewState), 1573518435000L);

    String json = RefreshStateParser.toJson(refreshState);
    RefreshState parsed = RefreshStateParser.fromJson(json);

    Assertions.assertThat(parsed.sourceStates()).hasSize(1);

    SourceState source = parsed.sourceStates().get(0);
    Assertions.assertThat(source).isInstanceOf(SourceViewState.class);
    Assertions.assertThat(source.type()).isEqualTo("view");
    Assertions.assertThat(source.name()).isEqualTo("daily_summary");
    Assertions.assertThat(source.namespace()).containsExactly("analytics", "views");
    Assertions.assertThat(source.catalog()).isEqualTo("other_catalog");

    SourceViewState parsedView = (SourceViewState) source;
    Assertions.assertThat(parsedView.versionId()).isEqualTo(5);
  }

  @Test
  public void testRoundTripMixedSourceStates() {
    SourceTableState tableState =
        new SourceTableState("events", Arrays.asList("default"), null, "uuid-1", 100L, "main");

    SourceViewState viewState =
        new SourceViewState("event_summary", Arrays.asList("default"), null, "uuid-2", 3);

    RefreshState refreshState =
        new RefreshState(1, Arrays.asList(tableState, viewState), 1573518435000L);

    String json = RefreshStateParser.toJson(refreshState);
    RefreshState parsed = RefreshStateParser.fromJson(json);

    Assertions.assertThat(parsed.sourceStates()).hasSize(2);
    Assertions.assertThat(parsed.sourceStates().get(0)).isInstanceOf(SourceTableState.class);
    Assertions.assertThat(parsed.sourceStates().get(1)).isInstanceOf(SourceViewState.class);

    SourceTableState parsedTable = (SourceTableState) parsed.sourceStates().get(0);
    Assertions.assertThat(parsedTable.ref()).isEqualTo("main");
  }

  @Test
  public void testParseSpecExample() {
    String json =
        "{"
            + "\"view-version-id\":1,"
            + "\"refresh-start-timestamp-ms\":1573518435000,"
            + "\"source-states\":[{"
            + "\"type\":\"table\","
            + "\"namespace\":[\"default\"],"
            + "\"name\":\"events\","
            + "\"uuid\":\"d4a10b5c-1e8a-4b72-9d67-3f4a8c9e1b2d\","
            + "\"snapshot-id\":6148331192489823102"
            + "}]"
            + "}";

    RefreshState parsed = RefreshStateParser.fromJson(json);

    Assertions.assertThat(parsed.viewVersionId()).isEqualTo(1);
    Assertions.assertThat(parsed.refreshStartTimestampMs()).isEqualTo(1573518435000L);
    Assertions.assertThat(parsed.sourceStates()).hasSize(1);

    SourceTableState tableState = (SourceTableState) parsed.sourceStates().get(0);
    Assertions.assertThat(tableState.name()).isEqualTo("events");
    Assertions.assertThat(tableState.namespace()).containsExactly("default");
    Assertions.assertThat(tableState.uuid()).isEqualTo("d4a10b5c-1e8a-4b72-9d67-3f4a8c9e1b2d");
    Assertions.assertThat(tableState.snapshotId()).isEqualTo(6148331192489823102L);
  }

  @Test
  public void testEmptySourceStates() {
    RefreshState refreshState = new RefreshState(1, Collections.emptyList(), 1573518435000L);

    String json = RefreshStateParser.toJson(refreshState);
    RefreshState parsed = RefreshStateParser.fromJson(json);

    Assertions.assertThat(parsed.sourceStates()).isEmpty();
    Assertions.assertThat(parsed.viewVersionId()).isEqualTo(1);
  }

  @Test
  public void testSourceTableStateWithRef() {
    SourceTableState tableState =
        new SourceTableState(
            "events", Arrays.asList("default"), null, "uuid-1", 100L, "audit_branch");

    RefreshState refreshState =
        new RefreshState(1, Collections.singletonList(tableState), 1573518435000L);

    String json = RefreshStateParser.toJson(refreshState);
    Assertions.assertThat(json).contains("\"ref\":\"audit_branch\"");

    RefreshState parsed = RefreshStateParser.fromJson(json);
    SourceTableState parsedTable = (SourceTableState) parsed.sourceStates().get(0);
    Assertions.assertThat(parsedTable.ref()).isEqualTo("audit_branch");
  }

  @Test
  public void testNullJsonThrows() {
    Assertions.assertThatThrownBy(() -> RefreshStateParser.fromJson((String) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse refresh state from null string");
  }

  @Test
  public void testUnknownTypeThrows() {
    String json =
        "{"
            + "\"view-version-id\":1,"
            + "\"refresh-start-timestamp-ms\":1573518435000,"
            + "\"source-states\":[{"
            + "\"type\":\"unknown\","
            + "\"namespace\":[\"default\"],"
            + "\"name\":\"events\","
            + "\"uuid\":\"uuid-1\""
            + "}]"
            + "}";

    Assertions.assertThatThrownBy(() -> RefreshStateParser.fromJson(json))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unknown source state type: unknown");
  }
}
