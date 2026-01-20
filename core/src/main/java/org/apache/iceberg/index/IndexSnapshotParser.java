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
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.JsonUtil;

class IndexSnapshotParser {

  private static final String TABLE_SNAPSHOT_ID = "table-snapshot-id";
  private static final String INDEX_SNAPSHOT_ID = "index-snapshot-id";
  private static final String VERSION_ID = "version-id";
  private static final String PROPERTIES = "properties";

  private IndexSnapshotParser() {}

  static String toJson(IndexSnapshot snapshot) {
    return JsonUtil.generate(gen -> toJson(snapshot, gen), false);
  }

  static void toJson(IndexSnapshot snapshot, JsonGenerator generator) throws IOException {
    Preconditions.checkArgument(snapshot != null, "Invalid index snapshot: null");
    generator.writeStartObject();
    generator.writeNumberField(TABLE_SNAPSHOT_ID, snapshot.tableSnapshotId());
    generator.writeNumberField(INDEX_SNAPSHOT_ID, snapshot.indexSnapshotId());
    generator.writeNumberField(VERSION_ID, snapshot.versionId());

    if (snapshot.properties() != null && !snapshot.properties().isEmpty()) {
      JsonUtil.writeStringMap(PROPERTIES, snapshot.properties(), generator);
    }

    generator.writeEndObject();
  }

  static IndexSnapshot fromJson(String json) {
    return JsonUtil.parse(json, IndexSnapshotParser::fromJson);
  }

  static IndexSnapshot fromJson(JsonNode node) {
    Preconditions.checkArgument(node != null, "Cannot parse index snapshot from null object");
    Preconditions.checkArgument(
        node.isObject(), "Cannot parse index snapshot from non-object: %s", node);

    long tableSnapshotId = JsonUtil.getLong(TABLE_SNAPSHOT_ID, node);
    long indexSnapshotId = JsonUtil.getLong(INDEX_SNAPSHOT_ID, node);
    int versionId = JsonUtil.getInt(VERSION_ID, node);
    Map<String, String> properties =
        node.has(PROPERTIES) ? JsonUtil.getStringMap(PROPERTIES, node) : ImmutableMap.of();

    return ImmutableIndexSnapshot.builder()
        .tableSnapshotId(tableSnapshotId)
        .indexSnapshotId(indexSnapshotId)
        .versionId(versionId)
        .properties(properties)
        .build();
  }
}
