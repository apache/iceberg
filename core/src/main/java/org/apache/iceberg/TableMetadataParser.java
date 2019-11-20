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

package org.apache.iceberg;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SortedSet;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.iceberg.TableMetadata.MetadataLogEntry;
import org.apache.iceberg.TableMetadata.SnapshotLogEntry;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.util.JsonUtil;

public class TableMetadataParser {

  public enum Codec {
    NONE(""),
    GZIP(".gz");

    private final String extension;

    Codec(String extension) {
      this.extension = extension;
    }

    public static Codec fromName(String codecName) {
      Preconditions.checkArgument(codecName != null, "Codec name is null");
      return Codec.valueOf(codecName.toUpperCase(Locale.ENGLISH));
    }

    public static Codec fromFileName(String fileName) {
      Preconditions.checkArgument(fileName.contains(".metadata.json"),
          "%s is not a valid metadata file", fileName);
      // we have to be backward-compatible with .metadata.json.gz files
      if (fileName.endsWith(".metadata.json.gz")) {
        return Codec.GZIP;
      }
      String fileNameWithoutSuffix = fileName.substring(0, fileName.lastIndexOf(".metadata.json"));
      if (fileNameWithoutSuffix.endsWith(Codec.GZIP.extension)) {
        return Codec.GZIP;
      } else {
        return Codec.NONE;
      }
    }
  }

  private TableMetadataParser() {}

  // visible for testing
  static final String FORMAT_VERSION = "format-version";
  static final String TABLE_UUID = "table-uuid";
  static final String LOCATION = "location";
  static final String LAST_UPDATED_MILLIS = "last-updated-ms";
  static final String LAST_COLUMN_ID = "last-column-id";
  static final String SCHEMA = "schema";
  static final String PARTITION_SPEC = "partition-spec";
  static final String PARTITION_SPECS = "partition-specs";
  static final String DEFAULT_SPEC_ID = "default-spec-id";
  static final String PROPERTIES = "properties";
  static final String CURRENT_SNAPSHOT_ID = "current-snapshot-id";
  static final String SNAPSHOTS = "snapshots";
  static final String SNAPSHOT_ID = "snapshot-id";
  static final String TIMESTAMP_MS = "timestamp-ms";
  static final String SNAPSHOT_LOG = "snapshot-log";
  static final String METADATA_FILE = "metadata-file";
  static final String METADATA_LOG = "metadata-log";

  public static void overwrite(TableMetadata metadata, OutputFile outputFile) {
    internalWrite(metadata, outputFile, true);
  }

  public static void write(TableMetadata metadata, OutputFile outputFile) {
    internalWrite(metadata, outputFile, false);
  }

  public static void internalWrite(
      TableMetadata metadata, OutputFile outputFile, boolean overwrite) {
    boolean isGzip = Codec.fromFileName(outputFile.location()) == Codec.GZIP;
    OutputStream stream = overwrite ? outputFile.createOrOverwrite() : outputFile.create();
    try (OutputStreamWriter writer = new OutputStreamWriter(isGzip ? new GZIPOutputStream(stream) : stream)) {
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
      generator.useDefaultPrettyPrinter();
      toJson(metadata, generator);
      generator.flush();
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to write json to file: %s", outputFile);
    }
  }

  public static String getFileExtension(String codecName) {
    return getFileExtension(Codec.fromName(codecName));
  }

  public static String getFileExtension(Codec codec) {
    return codec.extension + ".metadata.json";
  }

  public static String getOldFileExtension(Codec codec) {
    // we have to be backward-compatible with .metadata.json.gz files
    return ".metadata.json" + codec.extension;
  }

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

  private static void toJson(TableMetadata metadata, JsonGenerator generator) throws IOException {
    generator.writeStartObject();

    generator.writeNumberField(FORMAT_VERSION, TableMetadata.TABLE_FORMAT_VERSION);
    generator.writeStringField(TABLE_UUID, metadata.uuid());
    generator.writeStringField(LOCATION, metadata.location());
    generator.writeNumberField(LAST_UPDATED_MILLIS, metadata.lastUpdatedMillis());
    generator.writeNumberField(LAST_COLUMN_ID, metadata.lastColumnId());

    generator.writeFieldName(SCHEMA);
    SchemaParser.toJson(metadata.schema(), generator);

    // for older readers, continue writing the default spec as "partition-spec"
    generator.writeFieldName(PARTITION_SPEC);
    PartitionSpecParser.toJsonFields(metadata.spec(), generator);

    // write the default spec ID and spec list
    generator.writeNumberField(DEFAULT_SPEC_ID, metadata.defaultSpecId());
    generator.writeArrayFieldStart(PARTITION_SPECS);
    for (PartitionSpec spec : metadata.specs()) {
      PartitionSpecParser.toJson(spec, generator);
    }
    generator.writeEndArray();

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
    for (HistoryEntry logEntry : metadata.snapshotLog()) {
      generator.writeStartObject();
      generator.writeNumberField(TIMESTAMP_MS, logEntry.timestampMillis());
      generator.writeNumberField(SNAPSHOT_ID, logEntry.snapshotId());
      generator.writeEndObject();
    }
    generator.writeEndArray();

    generator.writeArrayFieldStart(METADATA_LOG);
    for (MetadataLogEntry logEntry : metadata.previousFiles()) {
      generator.writeStartObject();
      generator.writeNumberField(TIMESTAMP_MS, logEntry.timestampMillis());
      generator.writeStringField(METADATA_FILE, logEntry.file());
      generator.writeEndObject();
    }
    generator.writeEndArray();

    generator.writeEndObject();
  }

  public static TableMetadata read(FileIO io, InputFile file) {
    Codec codec = Codec.fromFileName(file.location());
    try (InputStream is = codec == Codec.GZIP ? new GZIPInputStream(file.newStream()) : file.newStream()) {
      return fromJson(io, file, JsonUtil.mapper().readValue(is, JsonNode.class));
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to read file: %s", file);
    }
  }

  static TableMetadata fromJson(FileIO io, InputFile file, JsonNode node) {
    Preconditions.checkArgument(node.isObject(),
        "Cannot parse metadata from a non-object: %s", node);

    int formatVersion = JsonUtil.getInt(FORMAT_VERSION, node);
    Preconditions.checkArgument(formatVersion == TableMetadata.TABLE_FORMAT_VERSION,
        "Cannot read unsupported version %s", formatVersion);

    String uuid = JsonUtil.getStringOrNull(TABLE_UUID, node);
    String location = JsonUtil.getString(LOCATION, node);
    int lastAssignedColumnId = JsonUtil.getInt(LAST_COLUMN_ID, node);
    Schema schema = SchemaParser.fromJson(node.get(SCHEMA));

    JsonNode specArray = node.get(PARTITION_SPECS);
    List<PartitionSpec> specs;
    int defaultSpecId;
    if (specArray != null) {
      Preconditions.checkArgument(specArray.isArray(),
          "Cannot parse partition specs from non-array: %s", specArray);
      // default spec ID is required when the spec array is present
      defaultSpecId = JsonUtil.getInt(DEFAULT_SPEC_ID, node);

      // parse the spec array
      ImmutableList.Builder<PartitionSpec> builder = ImmutableList.builder();
      for (JsonNode spec : specArray) {
        builder.add(PartitionSpecParser.fromJson(schema, spec));
      }
      specs = builder.build();

    } else {
      // partition spec is required for older readers, but is always set to the default if the spec
      // array is set. it is only used to default the spec map is missing, indicating that the
      // table metadata was written by an older writer.
      defaultSpecId = TableMetadata.INITIAL_SPEC_ID;
      specs = ImmutableList.of(PartitionSpecParser.fromJsonFields(
          schema, TableMetadata.INITIAL_SPEC_ID, node.get(PARTITION_SPEC)));
    }

    Map<String, String> properties = JsonUtil.getStringMap(PROPERTIES, node);
    long currentVersionId = JsonUtil.getLong(CURRENT_SNAPSHOT_ID, node);
    long lastUpdatedMillis = JsonUtil.getLong(LAST_UPDATED_MILLIS, node);

    JsonNode snapshotArray = node.get(SNAPSHOTS);
    Preconditions.checkArgument(snapshotArray.isArray(),
        "Cannot parse snapshots from non-array: %s", snapshotArray);

    List<Snapshot> snapshots = Lists.newArrayListWithExpectedSize(snapshotArray.size());
    Iterator<JsonNode> iterator = snapshotArray.elements();
    while (iterator.hasNext()) {
      snapshots.add(SnapshotParser.fromJson(io, iterator.next()));
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

    SortedSet<MetadataLogEntry> metadataEntries =
            Sets.newTreeSet(Comparator.comparingLong(MetadataLogEntry::timestampMillis));
    if (node.has(METADATA_LOG)) {
      Iterator<JsonNode> logIterator = node.get(METADATA_LOG).elements();
      while (logIterator.hasNext()) {
        JsonNode entryNode = logIterator.next();
        metadataEntries.add(new MetadataLogEntry(
                JsonUtil.getLong(TIMESTAMP_MS, entryNode), JsonUtil.getString(METADATA_FILE, entryNode)));
      }
    }

    return new TableMetadata(file, uuid, location,
        lastUpdatedMillis, lastAssignedColumnId, schema, defaultSpecId, specs, properties,
        currentVersionId, snapshots, ImmutableList.copyOf(entries.iterator()),
        ImmutableList.copyOf(metadataEntries.iterator()));
  }
}
