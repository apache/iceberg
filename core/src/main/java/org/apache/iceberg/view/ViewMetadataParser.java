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
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.JsonUtil;
import org.apache.iceberg.util.PropertyUtil;

class ViewMetadataParser {

  static final String FORMAT_VERSION = "format-version";
  static final String LOCATION = "location";
  static final String CURRENT_VERSION_ID = "current-version-id";
  static final String VERSIONS = "versions";
  static final String VERSION_LOG = "version-log";
  static final String PROPERTIES = "properties";
  static final String SCHEMAS = "schemas";
  static final String CURRENT_SCHEMA_ID = "current-schema-id";

  public static void overwrite(ViewMetadata metadata, OutputFile outputFile) {
    internalWrite(metadata, outputFile, true);
  }

  public static void write(ViewMetadata metadata, OutputFile outputFile) {
    internalWrite(metadata, outputFile, false);
  }

  public static void toJson(ViewMetadata metadata, JsonGenerator generator) throws IOException {
    generator.writeStartObject();

    generator.writeNumberField(FORMAT_VERSION, metadata.formatVersion());
    generator.writeStringField(LOCATION, metadata.location());

    JsonUtil.writeStringMap(PROPERTIES, metadata.properties(), generator);
    generator.writeNumberField(CURRENT_SCHEMA_ID, metadata.currentSchemaId());
    generator.writeArrayFieldStart(SCHEMAS);
    for (Schema schema : metadata.schemas()) {
      SchemaParser.toJson(schema, generator);
    }

    generator.writeEndArray();

    generator.writeNumberField(CURRENT_VERSION_ID, metadata.currentVersionId());
    generator.writeArrayFieldStart(VERSIONS);
    for (ViewVersion version : metadata.versions()) {
      ViewVersionParser.toJson(version, generator);
    }

    generator.writeEndArray();

    generator.writeArrayFieldStart(VERSION_LOG);
    for (ViewHistoryEntry viewHistoryEntry : metadata.history()) {
      ViewHistoryEntryParser.toJson(viewHistoryEntry, generator);
    }

    generator.writeEndArray();
    generator.writeEndObject();
  }

  static String toJson(ViewMetadata viewMetadata) {
    return JsonUtil.generate(gen -> toJson(viewMetadata, gen), false);
  }

  public static ViewMetadata read(InputFile file) {
    try (InputStream is = file.newStream()) {
      return fromJson(JsonUtil.mapper().readValue(is, JsonNode.class));
    } catch (IOException e) {
      throw new UncheckedIOException(String.format("Failed to read json file: %s", file), e);
    }
  }

  public static ViewMetadata fromJson(JsonNode node) {
    Preconditions.checkArgument(node != null, "Cannot parse view metadata from null json");
    Preconditions.checkArgument(
        node.isObject(), "Cannot parse metadata from a non-object: %s", node);

    int formatVersion = JsonUtil.getInt(FORMAT_VERSION, node);
    Preconditions.checkArgument(
        formatVersion <= ViewMetadata.SUPPORTED_VIEW_FORMAT_VERSION,
        "Cannot read unsupported version %s",
        formatVersion);

    String location = JsonUtil.getString(LOCATION, node);
    int currentVersionId = JsonUtil.getInt(CURRENT_VERSION_ID, node);
    Map<String, String> properties = JsonUtil.getStringMap(PROPERTIES, node);

    JsonNode versionsListNode = JsonUtil.get(VERSIONS, node);
    Preconditions.checkArgument(
        versionsListNode.isArray(), "Cannot parse versions from non-array: %s", versionsListNode);

    List<ViewVersion> versions = Lists.newArrayListWithExpectedSize(versionsListNode.size());
    ViewVersion currentVersion = null;
    for (JsonNode versionNode : versionsListNode) {
      ViewVersion version = ViewVersionParser.fromJson(versionNode);
      if (version.versionId() == currentVersionId) {
        currentVersion = version;
      }

      versions.add(version);
    }

    Preconditions.checkArgument(
        currentVersion != null,
        "Cannot find version with %s=%s from %s",
        CURRENT_VERSION_ID,
        currentVersionId,
        VERSIONS);

    JsonNode versionLogNode = JsonUtil.get(VERSION_LOG, node);
    Preconditions.checkArgument(
        versionLogNode.isArray(), "Cannot parse version-log from non-array: %s", versionLogNode);
    List<ViewHistoryEntry> historyEntries =
        Lists.newArrayListWithExpectedSize(versionLogNode.size());
    Iterator<JsonNode> versionLogIterator = versionLogNode.elements();
    while (versionLogIterator.hasNext()) {
      historyEntries.add(ViewHistoryEntryParser.fromJson(versionLogIterator.next()));
    }

    List<Schema> schemas;
    Integer currentSchemaId;
    Schema currentSchema = null;
    JsonNode schemaArray = node.get(SCHEMAS);

    Preconditions.checkArgument(
        schemaArray.isArray(), "Cannot parse schemas from non-array: %s", schemaArray);
    currentSchemaId = JsonUtil.getInt(CURRENT_SCHEMA_ID, node);

    ImmutableList.Builder<Schema> builder = ImmutableList.builder();
    for (JsonNode schemaNode : schemaArray) {
      Schema schema = SchemaParser.fromJson(schemaNode);
      if (schema.schemaId() == currentSchemaId) {
        currentSchema = schema;
      }

      builder.add(schema);
    }

    Preconditions.checkArgument(
        currentSchema != null,
        "Cannot find schema with %s=%s from %s",
        CURRENT_SCHEMA_ID,
        currentSchemaId,
        SCHEMAS);

    schemas = builder.build();

    int numVersionsToKeep =
        PropertyUtil.propertyAsInt(
            properties,
            ViewProperties.VERSION_HISTORY_SIZE,
            ViewProperties.VERSION_HISTORY_SIZE_DEFAULT);

    Preconditions.checkArgument(
        numVersionsToKeep >= 1, "%s must be positive", ViewProperties.VERSION_HISTORY_SIZE);

    if (versions.size() > numVersionsToKeep) {
      versions = versions.subList(versions.size() - numVersionsToKeep, versions.size());
      historyEntries =
          historyEntries.subList(historyEntries.size() - numVersionsToKeep, historyEntries.size());
    }

    return ImmutableViewMetadata.builder()
        .location(location)
        .currentVersionId(currentVersionId)
        .properties(properties)
        .versions(versions)
        .schemas(schemas)
        .currentSchemaId(currentSchemaId)
        .history(historyEntries)
        .formatVersion(formatVersion)
        .build();
  }

  static ViewMetadata fromJson(String json) {
    Preconditions.checkArgument(json != null, "Cannot parse view metadata from null string");
    return JsonUtil.parse(json, ViewMetadataParser::fromJson);
  }

  private static void internalWrite(
      ViewMetadata metadata, OutputFile outputFile, boolean overwrite) {
    OutputStream stream = overwrite ? outputFile.createOrOverwrite() : outputFile.create();
    try (OutputStreamWriter writer = new OutputStreamWriter(stream, StandardCharsets.UTF_8)) {
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
      generator.useDefaultPrettyPrinter();
      toJson(metadata, generator);
      generator.flush();
    } catch (IOException e) {
      throw new UncheckedIOException(
          String.format("Failed to write json to file: %s", outputFile), e);
    }
  }

  private ViewMetadataParser() {}
}
