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
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.JsonUtil;

public class ViewMetadataParser {

  static final String FORMAT_VERSION = "format-version";
  static final String LOCATION = "location";
  static final String CURRENT_VERSION_ID = "current-version-id";
  static final String VERSIONS = "versions";
  static final String VERSION_LOG = "version-log";
  static final String PROPERTIES = "properties";
  static final String SCHEMAS = "schemas";

  private ViewMetadataParser() {}

  public static String toJson(ViewMetadata metadata) {
    return toJson(metadata, false);
  }

  public static String toJson(ViewMetadata metadata, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(metadata, gen), pretty);
  }

  static void toJson(ViewMetadata metadata, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != metadata, "Invalid view metadata: null");

    gen.writeStartObject();

    gen.writeNumberField(FORMAT_VERSION, metadata.formatVersion());
    gen.writeStringField(LOCATION, metadata.location());
    JsonUtil.writeStringMap(PROPERTIES, metadata.properties(), gen);

    gen.writeArrayFieldStart(SCHEMAS);
    for (Schema schema : metadata.schemas()) {
      SchemaParser.toJson(schema, gen);
    }
    gen.writeEndArray();

    gen.writeNumberField(CURRENT_VERSION_ID, metadata.currentVersionId());
    gen.writeArrayFieldStart(VERSIONS);
    for (ViewVersion version : metadata.versions()) {
      ViewVersionParser.toJson(version, gen);
    }
    gen.writeEndArray();

    gen.writeArrayFieldStart(VERSION_LOG);
    for (ViewHistoryEntry viewHistoryEntry : metadata.history()) {
      ViewHistoryEntryParser.toJson(viewHistoryEntry, gen);
    }
    gen.writeEndArray();

    gen.writeEndObject();
  }

  public static ViewMetadata fromJson(String json) {
    Preconditions.checkArgument(json != null, "Cannot parse view metadata from null string");
    return JsonUtil.parse(json, ViewMetadataParser::fromJson);
  }

  public static ViewMetadata fromJson(JsonNode json) {
    Preconditions.checkArgument(json != null, "Cannot parse view metadata from null object");
    Preconditions.checkArgument(
        json.isObject(), "Cannot parse view metadata from non-object: %s", json);

    int formatVersion = JsonUtil.getInt(FORMAT_VERSION, json);
    String location = JsonUtil.getString(LOCATION, json);
    Map<String, String> properties = JsonUtil.getStringMap(PROPERTIES, json);

    JsonNode schemasNode = JsonUtil.get(SCHEMAS, json);

    Preconditions.checkArgument(
        schemasNode.isArray(), "Cannot parse schemas from non-array: %s", schemasNode);
    List<Schema> schemas = Lists.newArrayListWithExpectedSize(schemasNode.size());

    for (JsonNode schemaNode : schemasNode) {
      schemas.add(SchemaParser.fromJson(schemaNode));
    }

    int currentVersionId = JsonUtil.getInt(CURRENT_VERSION_ID, json);
    JsonNode versionsNode = JsonUtil.get(VERSIONS, json);
    Preconditions.checkArgument(
        versionsNode.isArray(), "Cannot parse versions from non-array: %s", versionsNode);
    List<ViewVersion> versions = Lists.newArrayListWithExpectedSize(versionsNode.size());
    for (JsonNode versionNode : versionsNode) {
      versions.add(ViewVersionParser.fromJson(versionNode));
    }

    JsonNode versionLogNode = JsonUtil.get(VERSION_LOG, json);
    Preconditions.checkArgument(
        versionLogNode.isArray(), "Cannot parse version-log from non-array: %s", versionLogNode);
    List<ViewHistoryEntry> historyEntries =
        Lists.newArrayListWithExpectedSize(versionLogNode.size());
    for (JsonNode vLog : versionLogNode) {
      historyEntries.add(ViewHistoryEntryParser.fromJson(vLog));
    }

    return ImmutableViewMetadata.builder()
        .location(location)
        .currentVersionId(currentVersionId)
        .properties(properties)
        .versions(versions)
        .schemas(schemas)
        .history(historyEntries)
        .formatVersion(formatVersion)
        .build();
  }

  public static void overwrite(ViewMetadata metadata, OutputFile outputFile) {
    internalWrite(metadata, outputFile, true);
  }

  public static void write(ViewMetadata metadata, OutputFile outputFile) {
    internalWrite(metadata, outputFile, false);
  }

  public static ViewMetadata read(InputFile file) {
    try (InputStream is = file.newStream()) {
      return fromJson(JsonUtil.mapper().readValue(is, JsonNode.class));
    } catch (IOException e) {
      throw new UncheckedIOException(String.format("Failed to read json file: %s", file), e);
    }
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
}
