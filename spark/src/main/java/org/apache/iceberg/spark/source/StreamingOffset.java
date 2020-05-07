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
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.StringWriter;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.util.JsonUtil;
import org.apache.spark.sql.sources.v2.reader.streaming.Offset;

/**
 * An implementation of Spark Structured Streaming Offset, to track the current processed
 * files of Iceberg table. This StreamingOffset is combined by:
 *
 * version: used to validate the version number of StreamingOffset when deserializing from json
 * string.
 * snapshot_id: used to record the current processed snapshot of Iceberg table.
 * index: used to record the index of scan files in this snapshot.
 * is_starting_snapshot: used to identify if this snapshot is a starting snapshot. For starting snapshot
 * we should scan all the files, not just incrementally add files.
 * last_index_of_snapshot: used to denote whether the current index is the last file of this snapshot,
 * to avoid revist the processed snapshot.
 */
class StreamingOffset extends Offset {
  static final StreamingOffset START_OFFSET = new StreamingOffset(-1L, -1, false, true);

  private static final int CURR_VERSION = 1;
  private static final String VERSION = "version";
  private static final String SNAPSHOT_ID = "snapshot_id";
  private static final String INDEX = "index";
  private static final String IS_STARTING = "is_starting_snapshot";
  private static final String LAST_INDEX_OF_SNAPSHOT = "last_index_of_snapshot";

  private final long snapshotId;
  private final int index;
  private final boolean isStarting;
  private final boolean lastIndexOfSnapshot;

  StreamingOffset(long snapshotId, int index, boolean isStarting, boolean lastIndexOfSnapshot) {
    this.snapshotId = snapshotId;
    this.index = index;
    this.isStarting = isStarting;
    this.lastIndexOfSnapshot = lastIndexOfSnapshot;
  }

  static StreamingOffset fromJson(String json) {
    Preconditions.checkNotNull(json, "The input JSON string is null");

    try {
      JsonNode node = JsonUtil.mapper().readValue(json, JsonNode.class);
      int version = JsonUtil.getInt(VERSION, node);
      if (version != CURR_VERSION) {
        throw new IOException("The version number in JSON string " + version + " is not a valid version number");
      }

      long snapshotId = JsonUtil.getLong(SNAPSHOT_ID, node);
      int index = JsonUtil.getInt(INDEX, node);
      boolean isStarting = JsonUtil.getBool(IS_STARTING, node);
      boolean lastIndexOfSnapshot = JsonUtil.getBool(LAST_INDEX_OF_SNAPSHOT, node);

      return new StreamingOffset(snapshotId, index, isStarting, lastIndexOfSnapshot);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to parse StreamingOffset from JSON string %s", json);
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
      generator.writeBooleanField(IS_STARTING, isStarting);
      generator.writeBooleanField(LAST_INDEX_OF_SNAPSHOT, lastIndexOfSnapshot);
      generator.writeEndObject();
      generator.flush();
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to write StreamingOffset to json");
    }

    return writer.toString();
  }

  long snapshotId() {
    return snapshotId;
  }

  int index() {
    return index;
  }

  boolean isStartingSnapshotId() {
    return isStarting;
  }

  boolean isLastIndexOfSnapshot() {
    return lastIndexOfSnapshot;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof StreamingOffset) {
      StreamingOffset offset = (StreamingOffset) obj;
      return offset.snapshotId == snapshotId &&
          offset.index == index &&
          offset.isStarting == isStarting &&
          offset.lastIndexOfSnapshot == lastIndexOfSnapshot;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(snapshotId, index, isStarting, lastIndexOfSnapshot);
  }

  @Override
  public String toString() {
    return String.format("Streaming Offset[%d index (%d) is_starting_snapshot (%b) is_last_index_of_snapshot (%b)]",
      snapshotId, index, isStarting, lastIndexOfSnapshot);
  }
}
