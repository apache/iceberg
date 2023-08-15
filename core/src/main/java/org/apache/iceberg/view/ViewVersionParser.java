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
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.util.JsonUtil;

class ViewVersionParser {

  private static final String VERSION_ID = "version-id";
  private static final String TIMESTAMP_MS = "timestamp-ms";
  private static final String SUMMARY = "summary";
  private static final String REPRESENTATIONS = "representations";
  private static final String SCHEMA_ID = "schema-id";
  private static final String DEFAULT_CATALOG = "default-catalog";
  private static final String DEFAULT_NAMESPACE = "default-namespace";

  private ViewVersionParser() {}

  static void toJson(ViewVersion version, JsonGenerator generator) throws IOException {
    Preconditions.checkArgument(version != null, "Cannot serialize null view version");
    generator.writeStartObject();

    generator.writeNumberField(VERSION_ID, version.versionId());
    generator.writeNumberField(TIMESTAMP_MS, version.timestampMillis());
    generator.writeNumberField(SCHEMA_ID, version.schemaId());
    JsonUtil.writeStringMap(SUMMARY, version.summary(), generator);

    if (version.defaultCatalog() != null) {
      generator.writeStringField(DEFAULT_CATALOG, version.defaultCatalog());
    }

    JsonUtil.writeStringArray(
        DEFAULT_NAMESPACE, Arrays.asList(version.defaultNamespace().levels()), generator);

    generator.writeArrayFieldStart(REPRESENTATIONS);
    for (ViewRepresentation representation : version.representations()) {
      ViewRepresentationParser.toJson(representation, generator);
    }
    generator.writeEndArray();

    generator.writeEndObject();
  }

  static String toJson(ViewVersion version) {
    return JsonUtil.generate(gen -> toJson(version, gen), false);
  }

  static ViewVersion fromJson(String json) {
    Preconditions.checkArgument(json != null, "Cannot parse view version from null string");
    return JsonUtil.parse(json, ViewVersionParser::fromJson);
  }

  static ViewVersion fromJson(JsonNode node) {
    Preconditions.checkArgument(node != null, "Cannot parse view version from null object");
    Preconditions.checkArgument(
        node.isObject(), "Cannot parse view version from a non-object: %s", node);

    int versionId = JsonUtil.getInt(VERSION_ID, node);
    int schemaId = JsonUtil.getInt(SCHEMA_ID, node);
    long timestamp = JsonUtil.getLong(TIMESTAMP_MS, node);
    Map<String, String> summary = JsonUtil.getStringMap(SUMMARY, node);

    JsonNode serializedRepresentations = node.get(REPRESENTATIONS);
    ImmutableList.Builder<ViewRepresentation> representations = ImmutableList.builder();
    for (JsonNode serializedRepresentation : serializedRepresentations) {
      ViewRepresentation representation =
          ViewRepresentationParser.fromJson(serializedRepresentation);
      representations.add(representation);
    }

    String defaultCatalog = JsonUtil.getStringOrNull(DEFAULT_CATALOG, node);

    Namespace defaultNamespace =
        Namespace.of(JsonUtil.getStringArray(JsonUtil.get(DEFAULT_NAMESPACE, node)));

    return ImmutableViewVersion.builder()
        .versionId(versionId)
        .timestampMillis(timestamp)
        .schemaId(schemaId)
        .summary(summary)
        .defaultNamespace(defaultNamespace)
        .defaultCatalog(defaultCatalog)
        .representations(representations.build())
        .build();
  }
}
