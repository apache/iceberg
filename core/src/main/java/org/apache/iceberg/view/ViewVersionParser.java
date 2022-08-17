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
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

class ViewVersionParser {

  private static final String VERSION_ID = "version-id";
  private static final String PARENT_VERSION_ID = "parent-version-id";
  private static final String TIMESTAMP_MS = "timestamp-ms";
  private static final String SUMMARY = "summary";
  private static final String OPERATION = "operation";
  private static final String REPRESENTATIONS = "representations";

  static void toJson(ViewVersion version, JsonGenerator generator)
      throws IOException {
    generator.writeStartObject();

    generator.writeNumberField(VERSION_ID, version.versionId());
    if (version.parentId() != null) {
      generator.writeNumberField(PARENT_VERSION_ID, version.parentId());
    }
    generator.writeNumberField(TIMESTAMP_MS, version.timestampMillis());
    JsonUtil.writeStringMap(SUMMARY, version.summary(), generator);
    JsonUtil.writeObjectList(REPRESENTATIONS, version.representations(), ViewRepresentationParser::toJson, generator);

    generator.writeEndObject();
  }

  static ViewVersion fromJson(JsonNode node) {
    Preconditions.checkArgument(node.isObject(),
        "Cannot parse table version from a non-object: %s", node);

    int versionId = JsonUtil.getInt(VERSION_ID, node);
    Integer parentId = JsonUtil.getIntOrNull(PARENT_VERSION_ID, node);
    long timestamp = JsonUtil.getLong(TIMESTAMP_MS, node);
    Map<String, String> summary = JsonUtil.getStringMap(SUMMARY, node);
    List<ViewRepresentation> representations =
        JsonUtil.getObjectList(REPRESENTATIONS, node, ViewRepresentationParser::fromJson);

    return BaseViewVersion.builder()
        .versionId(versionId)
        .parentId(parentId)
        .timestampMillis(timestamp)
        .summary(summary)
        .representations(representations)
        .build();
  }

  private ViewVersionParser() {
  }
}
