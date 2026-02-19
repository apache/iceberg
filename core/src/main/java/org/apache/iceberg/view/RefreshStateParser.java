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
import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.util.JsonUtil;

/** Parser for refresh state JSON serialization and deserialization. */
public class RefreshStateParser {

  private static final String VIEW_VERSION_ID = "view-version-id";
  private static final String SOURCE_STATES = "source-states";
  private static final String REFRESH_START_TIMESTAMP_MS = "refresh-start-timestamp-ms";

  // Source state field names
  private static final String TYPE = "type";
  private static final String NAME = "name";
  private static final String NAMESPACE = "namespace";
  private static final String CATALOG = "catalog";

  // TABLE-specific fields
  private static final String UUID = "uuid";
  private static final String SNAPSHOT_ID = "snapshot-id";
  private static final String REF = "ref";

  // VIEW-specific fields
  private static final String VERSION_ID = "version-id";

  // MATERIALIZED_VIEW-specific fields
  private static final String MV_VIEW_UUID = "view-uuid";
  private static final String MV_VIEW_VERSION_ID = "view-version-id";
  private static final String STORAGE_TABLE_UUID = "storage-table-uuid";
  private static final String STORAGE_TABLE_SNAPSHOT_ID = "storage-table-snapshot-id";

  private RefreshStateParser() {}

  public static String toJson(RefreshState refreshState) {
    return toJson(refreshState, false);
  }

  public static String toJson(RefreshState refreshState, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(refreshState, gen), pretty);
  }

  public static void toJson(RefreshState refreshState, JsonGenerator generator) throws IOException {
    Preconditions.checkArgument(refreshState != null, "Cannot serialize null refresh state");

    generator.writeStartObject();

    generator.writeNumberField(VIEW_VERSION_ID, refreshState.viewVersionId());
    generator.writeNumberField(REFRESH_START_TIMESTAMP_MS, refreshState.refreshStartTimestampMs());

    generator.writeArrayFieldStart(SOURCE_STATES);
    for (SourceState sourceState : refreshState.sourceStates()) {
      toJson(sourceState, generator);
    }
    generator.writeEndArray();

    generator.writeEndObject();
  }

  private static void toJson(SourceState sourceState, JsonGenerator generator) throws IOException {
    generator.writeStartObject();

    generator.writeStringField(TYPE, sourceState.type().toJsonValue());
    generator.writeStringField(NAME, sourceState.name());
    JsonUtil.writeStringArray(NAMESPACE, List.of(sourceState.namespace().levels()), generator);

    if (sourceState.catalog() != null) {
      generator.writeStringField(CATALOG, sourceState.catalog());
    }

    // Write type-specific fields using a switch expression
    switch (sourceState.type()) {
      case TABLE:
        generator.writeStringField(UUID, sourceState.uuid());
        generator.writeNumberField(SNAPSHOT_ID, sourceState.snapshotId());
        generator.writeStringField(REF, sourceState.ref());
        break;
      case VIEW:
        generator.writeStringField(UUID, sourceState.uuid());
        generator.writeNumberField(VERSION_ID, sourceState.versionId());
        break;
      case MATERIALIZED_VIEW:
        generator.writeStringField(MV_VIEW_UUID, sourceState.viewUuid());
        generator.writeNumberField(MV_VIEW_VERSION_ID, sourceState.viewVersionId());
        generator.writeStringField(STORAGE_TABLE_UUID, sourceState.storageTableUuid());
        generator.writeNumberField(STORAGE_TABLE_SNAPSHOT_ID, sourceState.storageTableSnapshotId());
        break;
      default:
        throw new IllegalStateException("Unknown source state type: " + sourceState.type());
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

    JsonNode sourceStatesNode = JsonUtil.get(SOURCE_STATES, node);
    Preconditions.checkArgument(
        sourceStatesNode.isArray(),
        "Cannot parse source-states from non-array: %s",
        sourceStatesNode);

    ImmutableList.Builder<SourceState> sourceStates = ImmutableList.builder();
    for (JsonNode sourceStateNode : sourceStatesNode) {
      sourceStates.add(parseSourceState(sourceStateNode));
    }

    return ImmutableRefreshState.builder()
        .viewVersionId(viewVersionId)
        .refreshStartTimestampMs(refreshStartTimestampMs)
        .sourceStates(sourceStates.build())
        .build();
  }

  private static SourceState parseSourceState(JsonNode node) {
    Preconditions.checkArgument(
        node != null && node.isObject(), "Cannot parse source state from non-object: %s", node);

    String typeString = JsonUtil.getString(TYPE, node);
    SourceState.Type type = SourceState.Type.fromString(typeString);

    String name = JsonUtil.getString(NAME, node);
    Namespace namespace = Namespace.of(JsonUtil.getStringArray(JsonUtil.get(NAMESPACE, node)));
    String catalog = JsonUtil.getStringOrNull(CATALOG, node);

    SourceState.Builder builder =
        SourceState.builder(type).name(name).namespace(namespace).catalog(catalog);

    // Parse type-specific fields using a switch expression
    switch (type) {
      case TABLE:
        return parseTableSource(node, builder);
      case VIEW:
        return parseViewSource(node, builder);
      case MATERIALIZED_VIEW:
        return parseMaterializedViewSource(node, builder);
      default:
        throw new IllegalArgumentException("Unknown source type: " + type);
    }
  }

  private static SourceState parseTableSource(JsonNode node, SourceState.Builder builder) {
    String uuid = JsonUtil.getString(UUID, node);
    long snapshotId = JsonUtil.getLong(SNAPSHOT_ID, node);
    String ref = JsonUtil.getStringOrNull(REF, node);

    builder.uuid(uuid).snapshotId(snapshotId);

    if (ref != null) {
      builder.ref(ref);
    }

    return builder.build();
  }

  private static SourceState parseViewSource(JsonNode node, SourceState.Builder builder) {
    String uuid = JsonUtil.getString(UUID, node);
    int versionId = JsonUtil.getInt(VERSION_ID, node);

    return builder.uuid(uuid).versionId(versionId).build();
  }

  private static SourceState parseMaterializedViewSource(
      JsonNode node, SourceState.Builder builder) {
    String viewUuid = JsonUtil.getString(MV_VIEW_UUID, node);
    int viewVersionId = JsonUtil.getInt(MV_VIEW_VERSION_ID, node);
    String storageTableUuid = JsonUtil.getString(STORAGE_TABLE_UUID, node);
    long storageTableSnapshotId = JsonUtil.getLong(STORAGE_TABLE_SNAPSHOT_ID, node);

    return builder
        .viewUuid(viewUuid)
        .viewVersionId(viewVersionId)
        .storageTableUuid(storageTableUuid)
        .storageTableSnapshotId(storageTableSnapshotId)
        .build();
  }
}
