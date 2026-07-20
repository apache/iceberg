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
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.JsonUtil;

/** JSON serialization for {@link IndexSnapshot}. */
class IndexSnapshotParser {

  private static final String SNAPSHOT_ID = "snapshot-id";
  private static final String SOURCE_TABLE_SNAPSHOT_ID = "source-table-snapshot-id";
  private static final String TIMESTAMP_MS = "timestamp-ms";
  private static final String TRACKING_FILE = "tracking-file";
  private static final String PROPERTIES = "properties";
  private static final String KEY_METADATA = "key-metadata";

  private IndexSnapshotParser() {}

  static void toJson(IndexSnapshot snapshot, JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    generator.writeNumberField(SNAPSHOT_ID, snapshot.snapshotId());
    generator.writeNumberField(SOURCE_TABLE_SNAPSHOT_ID, snapshot.sourceTableSnapshotId());
    generator.writeNumberField(TIMESTAMP_MS, snapshot.timestampMs());
    generator.writeStringField(TRACKING_FILE, snapshot.trackingFile());
    if (!snapshot.properties().isEmpty()) {
      JsonUtil.writeStringMap(PROPERTIES, snapshot.properties(), generator);
    }
    if (snapshot.keyMetadata() != null) {
      generator.writeBinaryField(KEY_METADATA, ByteBuffers.toByteArray(snapshot.keyMetadata()));
    }
    generator.writeEndObject();
  }

  static IndexSnapshot fromJson(JsonNode node) {
    GenericIndexSnapshot.Builder builder = GenericIndexSnapshot.builder();
    builder.snapshotId(JsonUtil.getLong(SNAPSHOT_ID, node));
    builder.sourceTableSnapshotId(JsonUtil.getLong(SOURCE_TABLE_SNAPSHOT_ID, node));
    builder.timestampMs(JsonUtil.getLong(TIMESTAMP_MS, node));
    builder.trackingFile(JsonUtil.getString(TRACKING_FILE, node));
    if (node.has(PROPERTIES)) {
      Map<String, String> props = JsonUtil.getStringMap(PROPERTIES, node);
      builder.properties(props);
    }
    if (node.has(KEY_METADATA)) {
      try {
        builder.keyMetadata(ByteBuffer.wrap(node.get(KEY_METADATA).binaryValue()));
      } catch (IOException e) {
        throw new RuntimeException("Failed to read key-metadata", e);
      }
    }
    return builder.build();
  }
}
