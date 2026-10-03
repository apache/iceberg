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
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnapshotParser {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotParser.class);

  private SnapshotParser() {}

  /** A dummy {@link FileIO} implementation that is only used to retrieve the path */
  private static final DummyFileIO DUMMY_FILE_IO = new DummyFileIO();

  private static final String SEQUENCE_NUMBER = "sequence-number";
  private static final String SNAPSHOT_ID = "snapshot-id";
  private static final String PARENT_SNAPSHOT_ID = "parent-snapshot-id";
  private static final String TIMESTAMP_MS = "timestamp-ms";
  private static final String SUMMARY = "summary";
  private static final String OPERATION = "operation";
  private static final String MANIFESTS = "manifests";
  private static final String MANIFEST_LIST = "manifest-list";
  private static final String SCHEMA_ID = "schema-id";
  private static final String FIRST_ROW_ID = "first-row-id";
  private static final String ADDED_ROWS = "added-rows";
  private static final String KEY_ID = "key-id";
  private static final String SNAPSHOTS = "snapshots";

  static void toJson(Snapshot snapshot, JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    if (snapshot.sequenceNumber() > TableMetadata.INITIAL_SEQUENCE_NUMBER) {
      generator.writeNumberField(SEQUENCE_NUMBER, snapshot.sequenceNumber());
    }
    generator.writeNumberField(SNAPSHOT_ID, snapshot.snapshotId());
    if (snapshot.parentId() != null) {
      generator.writeNumberField(PARENT_SNAPSHOT_ID, snapshot.parentId());
    }
    generator.writeNumberField(TIMESTAMP_MS, snapshot.timestampMillis());

    // if there is an operation, write the summary map
    if (snapshot.operation() != null) {
      generator.writeObjectFieldStart(SUMMARY);
      generator.writeStringField(OPERATION, snapshot.operation());
      if (snapshot.summary() != null) {
        for (Map.Entry<String, String> entry : snapshot.summary().entrySet()) {
          // only write operation once
          if (OPERATION.equals(entry.getKey())) {
            continue;
          }
          generator.writeStringField(entry.getKey(), entry.getValue());
        }
      }
      generator.writeEndObject();
    }

    String manifestList = snapshot.manifestListLocation();
    if (manifestList != null) {
      // write just the location. manifests should not be embedded in JSON along with a list
      generator.writeStringField(MANIFEST_LIST, manifestList);
    } else {
      // embed the manifest list in the JSON, v1 only
      JsonUtil.writeStringArray(
          MANIFESTS,
          Iterables.transform(snapshot.allManifests(DUMMY_FILE_IO), ManifestFile::path),
          generator);
    }

    // schema ID might be null for snapshots written by old writers
    if (snapshot.schemaId() != null) {
      generator.writeNumberField(SCHEMA_ID, snapshot.schemaId());
    }

    if (snapshot.firstRowId() != null) {
      generator.writeNumberField(FIRST_ROW_ID, snapshot.firstRowId());
    }

    if (snapshot.addedRows() != null) {
      generator.writeNumberField(ADDED_ROWS, snapshot.addedRows());
    }

    JsonUtil.writeStringFieldIfPresent(KEY_ID, snapshot.keyId(), generator);

    generator.writeEndObject();
  }

  public static String toJson(Snapshot snapshot) {
    // Use true as default value of pretty for backwards compatibility
    return toJson(snapshot, true);
  }

  public static String toJson(Snapshot snapshot, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(snapshot, gen), pretty);
  }

  static Snapshot fromJson(JsonNode node) {
    Preconditions.checkArgument(
        node.isObject(), "Cannot parse table version from a non-object: %s", node);

    long sequenceNumber = TableMetadata.INITIAL_SEQUENCE_NUMBER;
    if (node.has(SEQUENCE_NUMBER)) {
      sequenceNumber = JsonUtil.getLong(SEQUENCE_NUMBER, node);
    }
    long snapshotId = JsonUtil.getLong(SNAPSHOT_ID, node);
    Long parentId = null;
    if (node.has(PARENT_SNAPSHOT_ID)) {
      parentId = JsonUtil.getLong(PARENT_SNAPSHOT_ID, node);
    }
    long timestamp = JsonUtil.getLong(TIMESTAMP_MS, node);

    Map<String, String> summary = null;
    String operation = null;
    if (node.has(SUMMARY)) {
      JsonNode sNode = node.get(SUMMARY);
      Preconditions.checkArgument(
          sNode != null && !sNode.isNull() && sNode.isObject(),
          "Cannot parse summary from non-object value: %s",
          sNode);

      if (sNode.size() > 0) {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        Iterator<String> fields = sNode.fieldNames();
        while (fields.hasNext()) {
          String field = fields.next();
          if (field.equals(OPERATION)) {
            operation = JsonUtil.getString(OPERATION, sNode);
          } else {
            builder.put(field, JsonUtil.getString(field, sNode));
          }
        }
        summary = builder.build();

        // When the operation is not found, default to overwrite
        // to ensure that we can read the summary without raising an exception
        if (operation == null) {
          LOG.warn(
              "Encountered invalid summary for snapshot {}: the field 'operation' is required but missing, setting 'operation' to overwrite",
              snapshotId);
          operation = DataOperations.OVERWRITE;
        }
      }
    }

    Integer schemaId = JsonUtil.getIntOrNull(SCHEMA_ID, node);

    Long firstRowId = JsonUtil.getLongOrNull(FIRST_ROW_ID, node);
    Long addedRows = JsonUtil.getLongOrNull(ADDED_ROWS, node);

    String keyId = JsonUtil.getStringOrNull(KEY_ID, node);

    if (node.has(MANIFEST_LIST)) {
      // the manifest list is stored in a manifest list file
      String manifestList = JsonUtil.getString(MANIFEST_LIST, node);
      return new BaseSnapshot(
          sequenceNumber,
          snapshotId,
          parentId,
          timestamp,
          operation,
          summary,
          schemaId,
          manifestList,
          firstRowId,
          addedRows,
          keyId);

    } else {
      // fall back to an embedded manifest list. pass in the manifest's InputFile so length can be
      // loaded lazily, if it is needed
      return new BaseSnapshot(
          sequenceNumber,
          snapshotId,
          parentId,
          timestamp,
          operation,
          summary,
          schemaId,
          JsonUtil.getStringList(MANIFESTS, node).toArray(new String[0]));
    }
  }

  public static Snapshot fromJson(String json) {
    return JsonUtil.parse(json, SnapshotParser::fromJson);
  }

  /**
   * Streams snapshots from a table metadata JSON file one at a time.
   *
   * <p>{@link TableMetadataParser} reads the whole document into memory and materializes every
   * entry of the {@code snapshots} array. For tables with very large snapshot histories that can
   * dominate heap usage even when only a few snapshots are needed. This method walks the document
   * with a streaming parser and holds only one {@link Snapshot} in memory at a time.
   *
   * <p>The returned iterable opens a fresh stream on each iteration and must be closed by the
   * caller. It yields an empty iterable if the file has no {@code snapshots} array.
   */
  public static CloseableIterable<Snapshot> fromJson(InputFile file) {
    Preconditions.checkArgument(file != null, "Invalid metadata file: null");
    return new CloseableIterable<Snapshot>() {
      @Override
      public CloseableIterator<Snapshot> iterator() {
        return new SnapshotIterator(file);
      }

      @Override
      public void close() {}
    };
  }

  private static class SnapshotIterator implements CloseableIterator<Snapshot> {
    private final JsonParser parser;
    private Snapshot next;
    private boolean closed;

    private SnapshotIterator(InputFile file) {
      SeekableInputStream input = file.newStream();
      JsonParser jsonParser;
      try {
        jsonParser = JsonUtil.factory().createParser(input);
        jsonParser.setCodec(JsonUtil.mapper());
      } catch (IOException e) {
        try {
          input.close();
        } catch (IOException ignored) {
          // suppress so the original failure is reported
        }
        throw new UncheckedIOException("Failed to open metadata file: " + file.location(), e);
      }

      this.parser = jsonParser;
      if (!seekToSnapshots(parser)) {
        close();
      }
    }

    @Override
    public boolean hasNext() {
      if (closed) {
        return false;
      }

      if (next == null) {
        next = advance();
        if (next == null) {
          close();
          return false;
        }
      }

      return true;
    }

    @Override
    public Snapshot next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      Snapshot result = next;
      this.next = null;
      return result;
    }

    @Override
    public void close() {
      if (!closed) {
        this.closed = true;
        try {
          parser.close();
        } catch (IOException e) {
          throw new UncheckedIOException("Failed to close metadata parser", e);
        }
      }
    }

    private Snapshot advance() {
      try {
        if (parser.nextToken() == JsonToken.START_OBJECT) {
          JsonNode node = parser.readValueAsTree();
          return fromJson(node);
        }

        // END_ARRAY or end of input: no more snapshots
        return null;
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to read snapshot from table metadata", e);
      }
    }

    /** Advances the parser to the start of the {@code snapshots} array, if present. */
    private static boolean seekToSnapshots(JsonParser parser) {
      try {
        if (parser.nextToken() != JsonToken.START_OBJECT) {
          return false;
        }

        while (parser.nextToken() == JsonToken.FIELD_NAME) {
          String field = parser.currentName();
          parser.nextToken();
          if (SNAPSHOTS.equals(field)) {
            return parser.currentToken() == JsonToken.START_ARRAY;
          }

          parser.skipChildren();
        }

        return false;
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to read table metadata", e);
      }
    }
  }

  /**
   * The main purpose of this class is to lazily retrieve the path from a v1 Snapshot that has
   * manifest lists
   */
  private static class DummyFileIO implements FileIO {
    @Override
    public InputFile newInputFile(String path) {
      return new InputFile() {
        @Override
        public long getLength() {
          throw new UnsupportedOperationException();
        }

        @Override
        public SeekableInputStream newStream() {
          throw new UnsupportedOperationException();
        }

        @Override
        public String location() {
          return path;
        }

        @Override
        public boolean exists() {
          return true;
        }
      };
    }

    @Override
    public OutputFile newOutputFile(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void deleteFile(String path) {
      throw new UnsupportedOperationException();
    }
  }
}
