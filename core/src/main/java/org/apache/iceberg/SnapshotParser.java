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
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.util.JsonUtil;

public class SnapshotParser {

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
    }

    Integer schemaId = JsonUtil.getIntOrNull(SCHEMA_ID, node);

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
          manifestList);

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
