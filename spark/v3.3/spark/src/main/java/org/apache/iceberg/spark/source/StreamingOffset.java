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
import java.io.InputStream;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.Optional;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.spark.sql.connector.read.streaming.Offset;

class StreamingOffset extends Offset {
  static final StreamingOffset START_OFFSET = new StreamingOffset(-1L, -1, false, -1L, -1L);

  private static final int CURR_VERSION = 1;
  private static final String VERSION = "version";
  private static final String SNAPSHOT_ID = "snapshot_id";
  private static final String POSITION = "position";
  private static final String SCAN_ALL_FILES = "scan_all_files";
  private static final String SNAPSHOT_TIMESTAMP_MS = "snapshot_timestamp_ms";
  private static final String SNAPSHOT_TOTAL_ROWS = "snapshot_total_rows";

  private final long snapshotId;
  private final long position;
  private final boolean scanAllFiles;
  private final long snapshotTimestampMillis;
  private final long snapshotTotalRows;

  /**
   * An implementation of Spark Structured Streaming Offset, to track the current processed files of
   * Iceberg table.
   *
   * @param snapshotId The current processed snapshot id.
   * @param position The position of last scanned file in snapshot.
   * @param scanAllFiles whether to scan all files in a snapshot; for example, to read all data when
   *     starting a stream.
   * @param snapshotTimestampMillis The time the snapshot was created
   * @param snapshotTotalRows Total rows in the snapshot
   */
  StreamingOffset(
      long snapshotId,
      long position,
      boolean scanAllFiles,
      long snapshotTimestampMillis,
      long snapshotTotalRows) {
    this.snapshotId = snapshotId;
    this.position = position;
    this.scanAllFiles = scanAllFiles;
    this.snapshotTimestampMillis = snapshotTimestampMillis;
    this.snapshotTotalRows = snapshotTotalRows;
  }

  static StreamingOffset fromJson(String json) {
    Preconditions.checkNotNull(json, "Cannot parse StreamingOffset JSON: null");

    try {
      JsonNode node = JsonUtil.mapper().readValue(json, JsonNode.class);
      return fromJsonNode(node);
    } catch (IOException e) {
      throw new UncheckedIOException(
          String.format("Failed to parse StreamingOffset from JSON string %s", json), e);
    }
  }

  static StreamingOffset fromJson(InputStream inputStream) {
    Preconditions.checkNotNull(inputStream, "Cannot parse StreamingOffset from inputStream: null");

    JsonNode node;
    try {
      node = JsonUtil.mapper().readValue(inputStream, JsonNode.class);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read StreamingOffset from json", e);
    }

    return fromJsonNode(node);
  }

  @Override
  public String json() {
    StringWriter writer = new StringWriter();
    try {
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
      generator.writeStartObject();
      generator.writeNumberField(VERSION, CURR_VERSION);
      generator.writeNumberField(SNAPSHOT_ID, snapshotId);
      generator.writeNumberField(POSITION, position);
      generator.writeBooleanField(SCAN_ALL_FILES, scanAllFiles);
      generator.writeNumberField(SNAPSHOT_TIMESTAMP_MS, snapshotTimestampMillis);
      generator.writeNumberField(SNAPSHOT_TOTAL_ROWS, snapshotTotalRows);
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

  long position() {
    return position;
  }

  boolean shouldScanAllFiles() {
    return scanAllFiles;
  }

  long snapshotTimestampMs() {
    return snapshotTimestampMillis;
  }

  long snapshotTotalRows() {
    return snapshotTotalRows;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof StreamingOffset) {
      StreamingOffset offset = (StreamingOffset) obj;
      return offset.snapshotId == snapshotId
          && offset.position == position
          && offset.scanAllFiles == scanAllFiles
          && offset.snapshotTimestampMillis == snapshotTimestampMillis
          && offset.snapshotTotalRows == snapshotTotalRows;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        snapshotId, position, scanAllFiles, snapshotTimestampMillis, snapshotTotalRows);
  }

  @Override
  public String toString() {
    return String.format(
        "Streaming Offset[%d: position (%d) scan_all_files (%b), snapshot_timestamp_ms (%d), snapshot_total_rows (%d)]",
        snapshotId, position, scanAllFiles, snapshotTimestampMillis, snapshotTotalRows);
  }

  private static StreamingOffset fromJsonNode(JsonNode node) {
    // The version of StreamingOffset. The offset was created with a version number
    // used to validate when deserializing from json string.
    int version = JsonUtil.getInt(VERSION, node);
    Preconditions.checkArgument(
        version == CURR_VERSION,
        "This version of Iceberg source only supports version %s. Version %s is not supported.",
        CURR_VERSION,
        version);

    long snapshotId = JsonUtil.getLong(SNAPSHOT_ID, node);
    int position = JsonUtil.getInt(POSITION, node);
    boolean shouldScanAllFiles = JsonUtil.getBool(SCAN_ALL_FILES, node);
    long snapshotTimestampMs =
        Optional.ofNullable(JsonUtil.getLongOrNull(SNAPSHOT_TIMESTAMP_MS, node)).orElse(-1L);
    long snapshotTotalRows =
        Optional.ofNullable(JsonUtil.getLongOrNull(SNAPSHOT_TOTAL_ROWS, node)).orElse(-1L);

    return new StreamingOffset(
        snapshotId, position, shouldScanAllFiles, snapshotTimestampMs, snapshotTotalRows);
  }

  public static StreamingOffset fromSnapshot(Snapshot snapshot) {
    return new StreamingOffset(
        snapshot.snapshotId(),
        0,
        false,
        snapshot.timestampMillis(),
        PropertyUtil.propertyAsLong(snapshot.summary(), SnapshotSummary.TOTAL_RECORDS_PROP, -1));
  }
}
