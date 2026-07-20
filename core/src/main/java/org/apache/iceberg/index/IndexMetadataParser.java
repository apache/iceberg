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
package org.apache.iceberg.index;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.JsonUtil;

/**
 * JSON serialization for {@link IndexMetadata}.
 *
 * <p>The format matches the index metadata JSON defined in format/index.md.
 */
public class IndexMetadataParser {

  private static final String FORMAT_VERSION = "format-version";
  private static final String UUID = "uuid";
  private static final String TABLE_UUID = "table-uuid";
  private static final String LOCATION = "location";
  private static final String TYPE = "type";
  private static final String TRANSFORM_FUNCTION = "transform-function";
  private static final String KEY_COLUMN_IDS = "key-column-ids";
  private static final String INCLUDED_COLUMN_IDS = "included-column-ids";
  private static final String PROPERTIES = "properties";
  private static final String CURRENT_SNAPSHOT_ID = "current-snapshot-id";
  private static final String SNAPSHOTS = "snapshots";

  private IndexMetadataParser() {}

  public static String toJson(IndexMetadata metadata) {
    return toJson(metadata, false);
  }

  public static String toJson(IndexMetadata metadata, boolean pretty) {
    try {
      StringWriter writer = new StringWriter();
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
      if (pretty) {
        generator.useDefaultPrettyPrinter();
      }
      toJson(metadata, generator);
      generator.flush();
      return writer.toString();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to serialize index metadata", e);
    }
  }

  static void toJson(IndexMetadata metadata, JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    generator.writeNumberField(FORMAT_VERSION, metadata.formatVersion());
    generator.writeStringField(UUID, metadata.uuid());
    generator.writeStringField(TABLE_UUID, metadata.tableUuid());
    generator.writeStringField(LOCATION, metadata.location());
    generator.writeStringField(TYPE, metadata.type());
    generator.writeStringField(TRANSFORM_FUNCTION, metadata.transformFunction());

    generator.writeArrayFieldStart(KEY_COLUMN_IDS);
    for (int id : metadata.keyColumnIds()) {
      generator.writeNumber(id);
    }
    generator.writeEndArray();

    if (!metadata.includedColumnIds().isEmpty()) {
      generator.writeArrayFieldStart(INCLUDED_COLUMN_IDS);
      for (int id : metadata.includedColumnIds()) {
        generator.writeNumber(id);
      }
      generator.writeEndArray();
    }

    if (!metadata.properties().isEmpty()) {
      JsonUtil.writeStringMap(PROPERTIES, metadata.properties(), generator);
    }

    if (metadata.currentSnapshotId() != null) {
      generator.writeNumberField(CURRENT_SNAPSHOT_ID, metadata.currentSnapshotId());
    }

    generator.writeArrayFieldStart(SNAPSHOTS);
    for (IndexSnapshot snapshot : metadata.snapshots()) {
      IndexSnapshotParser.toJson(snapshot, generator);
    }
    generator.writeEndArray();

    generator.writeEndObject();
  }

  public static IndexMetadata fromJson(String json) {
    try {
      return fromJson(JsonUtil.mapper().readValue(json, JsonNode.class));
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to deserialize index metadata", e);
    }
  }

  static IndexMetadata fromJson(JsonNode node) {
    GenericIndexMetadata.Builder builder = GenericIndexMetadata.builder();
    builder.formatVersion(JsonUtil.getInt(FORMAT_VERSION, node));
    builder.uuid(JsonUtil.getString(UUID, node));
    builder.tableUuid(JsonUtil.getString(TABLE_UUID, node));
    builder.location(JsonUtil.getString(LOCATION, node));
    builder.type(JsonUtil.getString(TYPE, node));
    builder.transformFunction(JsonUtil.getString(TRANSFORM_FUNCTION, node));
    builder.keyColumnIds(JsonUtil.getIntegerList(KEY_COLUMN_IDS, node));

    if (node.has(INCLUDED_COLUMN_IDS)) {
      builder.includedColumnIds(JsonUtil.getIntegerList(INCLUDED_COLUMN_IDS, node));
    }
    if (node.has(PROPERTIES)) {
      builder.properties(JsonUtil.getStringMap(PROPERTIES, node));
    }
    if (node.has(CURRENT_SNAPSHOT_ID) && !node.get(CURRENT_SNAPSHOT_ID).isNull()) {
      builder.currentSnapshotId(JsonUtil.getLong(CURRENT_SNAPSHOT_ID, node));
    }

    if (node.has(SNAPSHOTS)) {
      List<IndexSnapshot> snapshots = Lists.newArrayList();
      Iterator<JsonNode> snapshotNodes = node.get(SNAPSHOTS).elements();
      while (snapshotNodes.hasNext()) {
        snapshots.add(IndexSnapshotParser.fromJson(snapshotNodes.next()));
      }
      // add snapshots individually so builder tracks currentSnapshotId
      for (IndexSnapshot snapshot : snapshots) {
        builder.addSnapshot(snapshot);
      }
      // override currentSnapshotId with what was in JSON (addSnapshot sets it to last added)
      if (node.has(CURRENT_SNAPSHOT_ID) && !node.get(CURRENT_SNAPSHOT_ID).isNull()) {
        builder.currentSnapshotId(JsonUtil.getLong(CURRENT_SNAPSHOT_ID, node));
      } else if (snapshots.isEmpty()) {
        builder.currentSnapshotId(null);
      }
    }

    return builder.build();
  }
}
