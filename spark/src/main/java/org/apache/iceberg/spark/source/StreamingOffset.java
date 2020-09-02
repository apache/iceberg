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

package org.apache.iceberg.spark.source;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;
import org.apache.spark.sql.sources.v2.reader.streaming.Offset;

/**
 * An implementation of Spark Structured Streaming Offset, to track the current processed
 * files of Iceberg table. This StreamingOffset consists of:
 *
 * version: The version of StreamingOffset. The offset was created with a version number used to validate
 * when deserializing from json string.
 * snapshot_id: The snapshot id of StreamingOffset, this is used to record the current processed snapshot of Iceberg
 * table.
 * index: The index of snapshot, this is used to record the processed data file index in this snapshot.
 * scan_all_files: This is used to identify if we should scan all the files, not just incrementally add files in this
 * snapshot.
 * snapshot_fully_processed: This is used to denote whether the current snapshot is fully processed, to avoid revisiting
 * the processed snapshot.
 */
class StreamingOffset extends Offset {
  static final StreamingOffset START_OFFSET = new StreamingOffset(-1L, -1, false, true);

  private static final int CURR_VERSION = 1;
  private static final String VERSION = "version";
  private static final String SNAPSHOT_ID = "snapshot_id";
  private static final String INDEX = "index";
  private static final String SCAN_ALL_FILES = "scan_all_files";
  private static final String SNAPSHOT_FULLY_PROCESSED = "snapshot_fully_processed";

  private final long snapshotId;
  private final int index;
  private final boolean scanAllFiles;
  private final boolean snapshotFullyProcessed;

  StreamingOffset(long snapshotId, int index, boolean scanAllFiles, boolean snapshotFullyProcessed) {
    this.snapshotId = snapshotId;
    this.index = index;
    this.scanAllFiles = scanAllFiles;
    this.snapshotFullyProcessed = snapshotFullyProcessed;
  }

  static StreamingOffset fromJson(String json) {
    Preconditions.checkNotNull(json, "The input JSON string is null");

    try {
      JsonNode node = JsonUtil.mapper().readValue(json, JsonNode.class);
      int version = JsonUtil.getInt(VERSION, node);
      if (version != CURR_VERSION) {
        throw new IOException(String.format("Cannot deserialize a JSON offset from version %d. %d does not match " +
            "this version of Iceberg %d and cannot be used. Please use a compatible version of Iceberg " +
            "to read this offset", version, version, CURR_VERSION));
      }

      long snapshotId = JsonUtil.getLong(SNAPSHOT_ID, node);
      int index = JsonUtil.getInt(INDEX, node);
      boolean shouldScanAllFiles = JsonUtil.getBool(SCAN_ALL_FILES, node);
      boolean snapshotFullyProcessed = JsonUtil.getBool(SNAPSHOT_FULLY_PROCESSED, node);

      return new StreamingOffset(snapshotId, index, shouldScanAllFiles, snapshotFullyProcessed);
    } catch (IOException e) {
      throw new UncheckedIOException(String.format("Failed to parse StreamingOffset from JSON string %s", json), e);
    }
  }

  @Override
  public String json() {
    StringWriter writer = new StringWriter();
    try {
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
      generator.writeStartObject();
      generator.writeNumberField(VERSION, CURR_VERSION);
      generator.writeNumberField(SNAPSHOT_ID, snapshotId);
      generator.writeNumberField(INDEX, index);
      generator.writeBooleanField(SCAN_ALL_FILES, scanAllFiles);
      generator.writeBooleanField(SNAPSHOT_FULLY_PROCESSED, snapshotFullyProcessed);
      generator.writeEndObject();
      generator.flush();

    } catch (IOException e) {
      throw new UncheckedIOException("Failed to write StreamingOffset to json", e);
    }

    return writer.toString();
  }

  long snapshotId() {
    return snapshotId;
  }

  int index() {
    return index;
  }

  boolean shouldScanAllFiles() {
    return scanAllFiles;
  }

  boolean isSnapshotFullyProcessed() {
    return snapshotFullyProcessed;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof StreamingOffset) {
      StreamingOffset offset = (StreamingOffset) obj;
      return offset.snapshotId == snapshotId &&
          offset.index == index &&
          offset.scanAllFiles == scanAllFiles &&
          offset.snapshotFullyProcessed == snapshotFullyProcessed;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(snapshotId, index, scanAllFiles, snapshotFullyProcessed);
  }

  @Override
  public String toString() {
    return String.format("Streaming Offset[%d index (%d) scan_all_files (%b) snapshot_fully_processed (%b)]",
      snapshotId, index, scanAllFiles, snapshotFullyProcessed);
  }
}
