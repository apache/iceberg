/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, ViewVersion 2.0 (the
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
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;
import org.apache.iceberg.util.PropertyUtil;

class ViewMetadataParser {

  // visible for testing
  static final String FORMAT_VERSION = "format-version";
  static final String LOCATION = "location";
  static final String CURRENT_VERSION_ID = "current-version-id";
  static final String VERSIONS = "versions";
  static final String VERSION_LOG = "version-log";
  static final String PROPERTIES = "properties";

  public static void overwrite(ViewMetadata metadata, OutputFile outputFile) {
    internalWrite(metadata, outputFile, true);
  }

  public static void write(ViewMetadata metadata, OutputFile outputFile) {
    internalWrite(metadata, outputFile, false);
  }

  public static void internalWrite(
      ViewMetadata metadata, OutputFile outputFile, boolean overwrite) {
    OutputStream stream = overwrite ? outputFile.createOrOverwrite() : outputFile.create();
    try (OutputStreamWriter writer = new OutputStreamWriter(stream, StandardCharsets.UTF_8)) {
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
      generator.useDefaultPrettyPrinter();
      toJson(metadata, generator);
      generator.flush();
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to write json to file: %s", outputFile);
    }
  }

  public static void toJson(ViewMetadata metadata, JsonGenerator generator) throws IOException {
    generator.writeStartObject();

    generator.writeNumberField(FORMAT_VERSION, metadata.formatVersion());
    generator.writeStringField(LOCATION, metadata.location());

    JsonUtil.writeStringMap(PROPERTIES, metadata.properties(), generator);

    generator.writeNumberField(CURRENT_VERSION_ID, metadata.currentVersionId());
    JsonUtil.writeObjectList(VERSIONS, metadata.versions(), ViewVersionParser::toJson, generator);

    JsonUtil.writeObjectList(VERSION_LOG, metadata.history(), ViewHistoryEntryParser::toJson, generator);

    generator.writeEndObject();
  }

  public static ViewMetadata read(InputFile file) {
    try (InputStream is = file.newStream()) {
      return fromJson(JsonUtil.mapper().readValue(is, JsonNode.class));
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to read file: %s", file);
    }
  }

  public static ViewMetadata fromJson(JsonNode node) {
    Preconditions.checkArgument(node.isObject(),
        "Cannot parse metadata from a non-object: %s", node);

    int formatVersion = JsonUtil.getInt(FORMAT_VERSION, node);
    Preconditions.checkArgument(formatVersion <= ViewMetadata.SUPPORTED_VIEW_FORMAT_VERSION,
        "Cannot read unsupported version %d", formatVersion);

    String location = JsonUtil.getString(LOCATION, node);

    int currentVersionId = JsonUtil.getInt(CURRENT_VERSION_ID, node);

    Map<String, String> properties = JsonUtil.getStringMap(PROPERTIES, node);

    List<ViewVersion> versions = JsonUtil.getObjectList(VERSIONS, node, ViewVersionParser::fromJson);

    List<ViewHistoryEntry> history = JsonUtil.getObjectList(VERSION_LOG, node, ViewHistoryEntryParser::fromJson);

    int numVersionsToKeep = PropertyUtil.propertyAsInt(properties,
        ViewProperties.VERSION_HISTORY_SIZE, ViewProperties.VERSION_HISTORY_SIZE_DEFAULT);

    return ViewMetadata.builder()
        .location(location)
        .currentVersionId(currentVersionId)
        .properties(properties)
        .versions(versions)
        .keepVersions(numVersionsToKeep)
        .history(history)
        .keepHistory(numVersionsToKeep)
        .build();
  }

  private ViewMetadataParser() {
  }
}
