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

class IndexVersionParser {

  private static final String VERSION_ID = "version-id";
  private static final String TIMESTAMP_MS = "timestamp-ms";
  private static final String PROPERTIES = "properties";

  private IndexVersionParser() {}

  static String toJson(IndexVersion version) {
    return JsonUtil.generate(gen -> toJson(version, gen), false);
  }

  static void toJson(IndexVersion version, JsonGenerator generator) throws IOException {
    Preconditions.checkArgument(version != null, "Invalid index version: null");
    generator.writeStartObject();
    generator.writeNumberField(VERSION_ID, version.versionId());
    generator.writeNumberField(TIMESTAMP_MS, version.timestampMillis());

    if (version.properties() != null && !version.properties().isEmpty()) {
      JsonUtil.writeStringMap(PROPERTIES, version.properties(), generator);
    }

    generator.writeEndObject();
  }

  static IndexVersion fromJson(String json) {
    return JsonUtil.parse(json, IndexVersionParser::fromJson);
  }

  static IndexVersion fromJson(JsonNode node) {
    Preconditions.checkArgument(node != null, "Cannot parse index version from null object");
    Preconditions.checkArgument(
        node.isObject(), "Cannot parse index version from non-object: %s", node);

    int versionId = JsonUtil.getInt(VERSION_ID, node);
    long timestampMillis = JsonUtil.getLong(TIMESTAMP_MS, node);
    Map<String, String> properties =
        node.has(PROPERTIES) ? JsonUtil.getStringMap(PROPERTIES, node) : ImmutableMap.of();

    return ImmutableIndexVersion.builder()
        .versionId(versionId)
        .timestampMillis(timestampMillis)
        .properties(properties)
        .build();
  }
}
