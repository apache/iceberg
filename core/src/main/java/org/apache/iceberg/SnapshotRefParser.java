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
import java.util.Locale;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class SnapshotRefParser {

  private SnapshotRefParser() {}

  private static final String SNAPSHOT_ID = "snapshot-id";
  private static final String TYPE = "type";
  private static final String MIN_SNAPSHOTS_TO_KEEP = "min-snapshots-to-keep";
  private static final String MAX_SNAPSHOT_AGE_MS = "max-snapshot-age-ms";
  private static final String MAX_REF_AGE_MS = "max-ref-age-ms";

  public static String toJson(SnapshotRef ref) {
    return toJson(ref, false);
  }

  public static String toJson(SnapshotRef ref, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(ref, gen), pretty);
  }

  public static void toJson(SnapshotRef ref, JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    generator.writeNumberField(SNAPSHOT_ID, ref.snapshotId());
    generator.writeStringField(TYPE, ref.type().name().toLowerCase(Locale.ENGLISH));
    JsonUtil.writeIntegerFieldIf(
        ref.minSnapshotsToKeep() != null,
        MIN_SNAPSHOTS_TO_KEEP,
        ref.minSnapshotsToKeep(),
        generator);
    JsonUtil.writeLongFieldIf(
        ref.maxSnapshotAgeMs() != null, MAX_SNAPSHOT_AGE_MS, ref.maxSnapshotAgeMs(), generator);
    JsonUtil.writeLongFieldIf(
        ref.maxRefAgeMs() != null, MAX_REF_AGE_MS, ref.maxRefAgeMs(), generator);
    generator.writeEndObject();
  }

  public static SnapshotRef fromJson(String json) {
    Preconditions.checkArgument(
        json != null && !json.isEmpty(), "Cannot parse snapshot ref from invalid JSON: %s", json);
    return JsonUtil.parse(json, SnapshotRefParser::fromJson);
  }

  public static SnapshotRef fromJson(JsonNode node) {
    Preconditions.checkArgument(
        node.isObject(), "Cannot parse snapshot reference from a non-object: %s", node);
    long snapshotId = JsonUtil.getLong(SNAPSHOT_ID, node);
    SnapshotRefType type = SnapshotRefType.fromString(JsonUtil.getString(TYPE, node));
    Integer minSnapshotsToKeep = JsonUtil.getIntOrNull(MIN_SNAPSHOTS_TO_KEEP, node);
    Long maxSnapshotAgeMs = JsonUtil.getLongOrNull(MAX_SNAPSHOT_AGE_MS, node);
    Long maxRefAgeMs = JsonUtil.getLongOrNull(MAX_REF_AGE_MS, node);
    return SnapshotRef.builderFor(snapshotId, type)
        .minSnapshotsToKeep(minSnapshotsToKeep)
        .maxSnapshotAgeMs(maxSnapshotAgeMs)
        .maxRefAgeMs(maxRefAgeMs)
        .build();
  }
}
