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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.util.JsonUtil;

public class RefreshStateParser {

  private static final String VIEW_VERSION_ID = "view-version-id";
  private static final String SOURCE_STATES = "source-states";
  private static final String REFRESH_START_TIMESTAMP_MS = "refresh-start-timestamp-ms";

  // Source state common fields
  private static final String TYPE = "type";
  private static final String NAME = "name";
  private static final String NAMESPACE = "namespace";
  private static final String CATALOG = "catalog";
  private static final String UUID = "uuid";

  // Source table state fields
  private static final String SNAPSHOT_ID = "snapshot-id";
  private static final String REF = "ref";

  // Source view state fields
  private static final String VERSION_ID = "version-id";

  private RefreshStateParser() {}

  public static String toJson(RefreshState refreshState) {
    return JsonUtil.generate(gen -> toJson(refreshState, gen), false);
  }

  public static void toJson(RefreshState refreshState, JsonGenerator generator) throws IOException {
    Preconditions.checkArgument(refreshState != null, "Cannot serialize null refresh state");
    generator.writeStartObject();

    generator.writeNumberField(VIEW_VERSION_ID, refreshState.viewVersionId());
    generator.writeNumberField(REFRESH_START_TIMESTAMP_MS, refreshState.refreshStartTimestampMs());

    generator.writeArrayFieldStart(SOURCE_STATES);
    for (SourceState sourceState : refreshState.sourceStates()) {
      writeSourceState(sourceState, generator);
    }
    generator.writeEndArray();

    generator.writeEndObject();
  }

  private static void writeSourceState(SourceState sourceState, JsonGenerator generator)
      throws IOException {
    generator.writeStartObject();
    generator.writeStringField(TYPE, sourceState.type());
    JsonUtil.writeStringArray(NAMESPACE, sourceState.namespace(), generator);
    generator.writeStringField(NAME, sourceState.name());

    if (sourceState.catalog() != null) {
      generator.writeStringField(CATALOG, sourceState.catalog());
    }

    generator.writeStringField(UUID, sourceState.uuid());

    if (sourceState instanceof SourceTableState) {
      SourceTableState tableState = (SourceTableState) sourceState;
      generator.writeNumberField(SNAPSHOT_ID, tableState.snapshotId());
      if (tableState.ref() != null) {
        generator.writeStringField(REF, tableState.ref());
      }
    } else if (sourceState instanceof SourceViewState) {
      SourceViewState viewState = (SourceViewState) sourceState;
      generator.writeNumberField(VERSION_ID, viewState.versionId());
    }

    generator.writeEndObject();
  }

  public static RefreshState fromJson(String json) {
    Preconditions.checkArgument(json != null, "Cannot parse refresh state from null string");
    return JsonUtil.parse(json, RefreshStateParser::fromJson);
  }

  public static RefreshState fromJson(JsonNode node) {
    Preconditions.checkArgument(node != null, "Cannot parse refresh state from null object");
    Preconditions.checkArgument(
        node.isObject(), "Cannot parse refresh state from a non-object: %s", node);

    int viewVersionId = JsonUtil.getInt(VIEW_VERSION_ID, node);
    long refreshStartTimestampMs = JsonUtil.getLong(REFRESH_START_TIMESTAMP_MS, node);

    JsonNode sourceStatesNode = node.get(SOURCE_STATES);
    ImmutableList.Builder<SourceState> sourceStates = ImmutableList.builder();
    if (sourceStatesNode != null && sourceStatesNode.isArray()) {
      for (JsonNode sourceStateNode : sourceStatesNode) {
        sourceStates.add(parseSourceState(sourceStateNode));
      }
    }

    return new RefreshState(viewVersionId, sourceStates.build(), refreshStartTimestampMs);
  }

  private static SourceState parseSourceState(JsonNode node) {
    String type = JsonUtil.getString(TYPE, node);
    String name = JsonUtil.getString(NAME, node);
    List<String> namespace = Arrays.asList(JsonUtil.getStringArray(JsonUtil.get(NAMESPACE, node)));
    String catalog = JsonUtil.getStringOrNull(CATALOG, node);
    String uuid = JsonUtil.getString(UUID, node);

    switch (type) {
      case SourceTableState.TYPE:
        long snapshotId = JsonUtil.getLong(SNAPSHOT_ID, node);
        String ref = JsonUtil.getStringOrNull(REF, node);
        return new SourceTableState(name, namespace, catalog, uuid, snapshotId, ref);

      case SourceViewState.TYPE:
        int versionId = JsonUtil.getInt(VERSION_ID, node);
        return new SourceViewState(name, namespace, catalog, uuid, versionId);

      default:
        throw new IllegalArgumentException("Unknown source state type: " + type);
    }
  }
}
