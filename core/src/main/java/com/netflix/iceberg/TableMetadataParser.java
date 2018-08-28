/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.iceberg.TableMetadata.SnapshotLogEntry;
import com.netflix.iceberg.exceptions.RuntimeIOException;
import com.netflix.iceberg.io.InputFile;
import com.netflix.iceberg.io.OutputFile;
import com.netflix.iceberg.util.JsonUtil;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.TreeSet;

public class TableMetadataParser {

  private static final String FORMAT_VERSION = "format-version";
  private static final String LOCATION = "location";
  private static final String LAST_UPDATED_MILLIS = "last-updated-ms";
  private static final String LAST_COLUMN_ID = "last-column-id";
  private static final String SCHEMA = "schema";
  private static final String PARTITION_SPEC = "partition-spec";
  private static final String PROPERTIES = "properties";
  private static final String CURRENT_SNAPSHOT_ID = "current-snapshot-id";
  private static final String SNAPSHOTS = "snapshots";
  private static final String SNAPSHOT_ID = "snapshot-id";
  private static final String TIMESTAMP_MS = "timestamp-ms";
  private static final String SNAPSHOT_LOG = "snapshot-log";

  public static String toJson(TableMetadata metadata) {
    StringWriter writer = new StringWriter();
    try {
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
      toJson(metadata, generator);
      generator.flush();
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to write json for: %s", metadata);
    }
    return writer.toString();
  }

  public static void write(TableMetadata metadata, OutputFile outputFile) {
    try (OutputStreamWriter writer = new OutputStreamWriter(outputFile.create())) {
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
      generator.useDefaultPrettyPrinter();
      toJson(metadata, generator);
      generator.flush();
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to write json to file: %s", outputFile);
    }
  }

  private static void toJson(TableMetadata metadata, JsonGenerator generator) throws IOException {
    generator.writeStartObject();

    generator.writeNumberField(FORMAT_VERSION, TableMetadata.TABLE_FORMAT_VERSION);
    generator.writeStringField(LOCATION, metadata.location());
    generator.writeNumberField(LAST_UPDATED_MILLIS, metadata.lastUpdatedMillis());
    generator.writeNumberField(LAST_COLUMN_ID, metadata.lastColumnId());

    generator.writeFieldName(SCHEMA);
    SchemaParser.toJson(metadata.schema(), generator);

    generator.writeFieldName(PARTITION_SPEC);
    PartitionSpecParser.toJson(metadata.spec(), generator);

    generator.writeObjectFieldStart(PROPERTIES);
    for (Map.Entry<String, String> keyValue : metadata.properties().entrySet()) {
      generator.writeStringField(keyValue.getKey(), keyValue.getValue());
    }
    generator.writeEndObject();

    generator.writeNumberField(CURRENT_SNAPSHOT_ID,
        metadata.currentSnapshot() != null ? metadata.currentSnapshot().snapshotId() : -1);

    generator.writeArrayFieldStart(SNAPSHOTS);
    for (Snapshot snapshot : metadata.snapshots()) {
      SnapshotParser.toJson(snapshot, generator);
    }
    generator.writeEndArray();

    generator.writeArrayFieldStart(SNAPSHOT_LOG);
    for (SnapshotLogEntry logEntry : metadata.snapshotLog()) {
      generator.writeStartObject();
      generator.writeNumberField(TIMESTAMP_MS, logEntry.timestampMillis());
      generator.writeNumberField(SNAPSHOT_ID, logEntry.snapshotId());
      generator.writeEndObject();
    }
    generator.writeEndArray();

    generator.writeEndObject();
  }

  public static TableMetadata read(TableOperations ops, InputFile file) {
    try {
      return fromJson(ops, file, JsonUtil.mapper().readValue(file.newStream(), JsonNode.class));
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to read file: %s", file);
    }
  }

  static TableMetadata fromJson(TableOperations ops, InputFile file, JsonNode node) {
    Preconditions.checkArgument(node.isObject(),
        "Cannot parse metadata from a non-object: %s", node);

    int formatVersion = JsonUtil.getInt(FORMAT_VERSION, node);
    Preconditions.checkArgument(formatVersion == TableMetadata.TABLE_FORMAT_VERSION,
        "Cannot read unsupported version %d", formatVersion);

    String location = JsonUtil.getString(LOCATION, node);
    int lastAssignedColumnId = JsonUtil.getInt(LAST_COLUMN_ID, node);
    Schema schema = SchemaParser.fromJson(node.get(SCHEMA));
    PartitionSpec spec = PartitionSpecParser.fromJson(schema, node.get(PARTITION_SPEC));
    Map<String, String> properties = JsonUtil.getStringMap(PROPERTIES, node);
    long currentVersionId = JsonUtil.getLong(CURRENT_SNAPSHOT_ID, node);
    long lastUpdatedMillis = JsonUtil.getLong(LAST_UPDATED_MILLIS, node);

    JsonNode snapshotArray = node.get(SNAPSHOTS);
    Preconditions.checkArgument(snapshotArray.isArray(),
        "Cannot parse snapshots from non-array: %s", snapshotArray);

    List<Snapshot> snapshots = Lists.newArrayListWithExpectedSize(snapshotArray.size());
    Iterator<JsonNode> iterator = snapshotArray.elements();
    while (iterator.hasNext()) {
      snapshots.add(SnapshotParser.fromJson(ops, iterator.next()));
    }

    SortedSet<SnapshotLogEntry> entries =
        Sets.newTreeSet(Comparator.comparingLong(SnapshotLogEntry::timestampMillis));
    if (node.has(SNAPSHOT_LOG)) {
      Iterator<JsonNode> logIterator = node.get(SNAPSHOT_LOG).elements();
      while (logIterator.hasNext()) {
        JsonNode entryNode = logIterator.next();
        entries.add(new SnapshotLogEntry(
            JsonUtil.getLong(TIMESTAMP_MS, entryNode), JsonUtil.getLong(SNAPSHOT_ID, entryNode)));
      }
    }

    return new TableMetadata(ops, file, location,
        lastUpdatedMillis, lastAssignedColumnId, schema, spec, properties, currentVersionId,
        snapshots, ImmutableList.copyOf(entries.iterator()));
  }

}
