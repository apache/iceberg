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
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.iceberg.TableMetadataParser.Codec;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.JsonUtil;

public class IndexMetadataParser {

  static final String INDEX_UUID = "index-uuid";
  static final String FORMAT_VERSION = "format-version";
  static final String INDEX_TYPE = "index-type";
  static final String INDEX_COLUMN_IDS = "index-column-ids";
  static final String OPTIMIZED_COLUMN_IDS = "optimized-column-ids";
  static final String LOCATION = "location";
  static final String CURRENT_VERSION_ID = "current-version-id";
  static final String VERSIONS = "versions";
  static final String VERSION_LOG = "version-log";
  static final String SNAPSHOTS = "snapshots";

  private IndexMetadataParser() {}

  public static String toJson(IndexMetadata metadata) {
    return toJson(metadata, false);
  }

  public static String toJson(IndexMetadata metadata, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(metadata, gen), pretty);
  }

  public static void toJson(IndexMetadata metadata, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != metadata, "Invalid index metadata: null");

    gen.writeStartObject();

    gen.writeStringField(INDEX_UUID, metadata.uuid());
    gen.writeNumberField(FORMAT_VERSION, metadata.formatVersion());
    gen.writeStringField(INDEX_TYPE, metadata.type().typeName());
    JsonUtil.writeIntegerArray(INDEX_COLUMN_IDS, metadata.indexColumnIds(), gen);
    JsonUtil.writeIntegerArray(OPTIMIZED_COLUMN_IDS, metadata.optimizedColumnIds(), gen);
    gen.writeStringField(LOCATION, metadata.location());

    gen.writeNumberField(CURRENT_VERSION_ID, metadata.currentVersionId());

    gen.writeArrayFieldStart(VERSIONS);
    for (IndexVersion version : metadata.versions()) {
      IndexVersionParser.toJson(version, gen);
    }
    gen.writeEndArray();

    gen.writeArrayFieldStart(VERSION_LOG);
    for (IndexHistoryEntry historyEntry : metadata.history()) {
      IndexHistoryEntryParser.toJson(historyEntry, gen);
    }
    gen.writeEndArray();

    gen.writeArrayFieldStart(SNAPSHOTS);
    for (IndexSnapshot snapshot : metadata.snapshots()) {
      IndexSnapshotParser.toJson(snapshot, gen);
    }
    gen.writeEndArray();

    gen.writeEndObject();
  }

  public static IndexMetadata fromJson(String metadataLocation, String json) {
    return JsonUtil.parse(json, node -> IndexMetadataParser.fromJson(metadataLocation, node));
  }

  public static IndexMetadata fromJson(String json) {
    Preconditions.checkArgument(json != null, "Cannot parse index metadata from null string");
    return JsonUtil.parse(json, IndexMetadataParser::fromJson);
  }

  public static IndexMetadata fromJson(JsonNode json) {
    return fromJson(null, json);
  }

  public static IndexMetadata fromJson(String metadataLocation, JsonNode json) {
    Preconditions.checkArgument(json != null, "Cannot parse index metadata from null object");
    Preconditions.checkArgument(
        json.isObject(), "Cannot parse index metadata from non-object: %s", json);

    String uuid = JsonUtil.getString(INDEX_UUID, json);
    int formatVersion = JsonUtil.getInt(FORMAT_VERSION, json);
    IndexType type = IndexType.fromString(JsonUtil.getString(INDEX_TYPE, json));
    List<Integer> indexColumnIds = JsonUtil.getIntegerList(INDEX_COLUMN_IDS, json);
    List<Integer> optimizedColumnIds = JsonUtil.getIntegerList(OPTIMIZED_COLUMN_IDS, json);
    String location = JsonUtil.getString(LOCATION, json);

    int currentVersionId = JsonUtil.getInt(CURRENT_VERSION_ID, json);

    JsonNode versionsNode = JsonUtil.get(VERSIONS, json);
    Preconditions.checkArgument(
        versionsNode.isArray(), "Cannot parse versions from non-array: %s", versionsNode);
    List<IndexVersion> versions = Lists.newArrayListWithExpectedSize(versionsNode.size());
    for (JsonNode versionNode : versionsNode) {
      versions.add(IndexVersionParser.fromJson(versionNode));
    }

    JsonNode versionLogNode = JsonUtil.get(VERSION_LOG, json);
    Preconditions.checkArgument(
        versionLogNode.isArray(), "Cannot parse version-log from non-array: %s", versionLogNode);
    List<IndexHistoryEntry> historyEntries =
        Lists.newArrayListWithExpectedSize(versionLogNode.size());
    for (JsonNode vLog : versionLogNode) {
      historyEntries.add(IndexHistoryEntryParser.fromJson(vLog));
    }

    JsonNode snapshotsNode = JsonUtil.get(SNAPSHOTS, json);
    Preconditions.checkArgument(
        snapshotsNode.isArray(), "Cannot parse snapshots from non-array: %s", snapshotsNode);
    List<IndexSnapshot> snapshots = Lists.newArrayListWithExpectedSize(snapshotsNode.size());
    for (JsonNode snapshotNode : snapshotsNode) {
      snapshots.add(IndexSnapshotParser.fromJson(snapshotNode));
    }

    return ImmutableIndexMetadata.of(
        uuid,
        formatVersion,
        type,
        indexColumnIds,
        optimizedColumnIds,
        location,
        currentVersionId,
        versions,
        historyEntries,
        snapshots,
        ImmutableList.of(),
        metadataLocation);
  }

  public static void overwrite(IndexMetadata metadata, OutputFile outputFile) {
    internalWrite(metadata, outputFile, true);
  }

  public static void write(IndexMetadata metadata, OutputFile outputFile) {
    internalWrite(metadata, outputFile, false);
  }

  public static IndexMetadata read(FileIO io, String path) {
    return read(io.newInputFile(path));
  }

  public static IndexMetadata read(InputFile file) {
    Codec codec = Codec.fromFileName(file.location());
    try (InputStream is =
        codec == Codec.GZIP ? new GZIPInputStream(file.newStream()) : file.newStream()) {
      return fromJson(file.location(), JsonUtil.mapper().readValue(is, JsonNode.class));
    } catch (IOException e) {
      throw new UncheckedIOException(
          String.format("Failed to read json file: %s", file.location()), e);
    }
  }

  private static void internalWrite(
      IndexMetadata metadata, OutputFile outputFile, boolean overwrite) {
    boolean isGzip = Codec.fromFileName(outputFile.location()) == Codec.GZIP;
    OutputStream stream = overwrite ? outputFile.createOrOverwrite() : outputFile.create();
    try (OutputStreamWriter writer =
        new OutputStreamWriter(
            isGzip ? new GZIPOutputStream(stream) : stream, StandardCharsets.UTF_8)) {
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
      toJson(metadata, generator);
      generator.flush();
    } catch (IOException e) {
      throw new UncheckedIOException(
          String.format("Failed to write json to file: %s", outputFile.location()), e);
    }
  }
}
