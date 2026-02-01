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
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.JsonUtil;

/** Parser for {@link IndexUpdate} implementations. */
public class IndexUpdateParser {

  private IndexUpdateParser() {}

  private static final String ACTION = "action";

  // action types - visible for testing
  static final String ASSIGN_UUID = "assign-uuid";
  static final String ADD_SNAPSHOT = "add-snapshot";
  static final String REMOVE_SNAPSHOTS = "remove-snapshots";
  static final String SET_CURRENT_VERSION = "set-current-version";
  static final String ADD_VERSION = "add-version";
  static final String SET_LOCATION = "set-location";

  // AssignUUID
  private static final String UUID = "uuid";

  // AddIndexSnapshot
  private static final String SNAPSHOT = "snapshot";

  // RemoveIndexSnapshots
  private static final String SNAPSHOT_IDS = "snapshot-ids";

  // SetIndexCurrentVersion
  private static final String VERSION_ID = "version-id";

  // AddIndexVersion
  private static final String VERSION = "version";

  // SetLocation
  private static final String LOCATION = "location";

  private static final Map<Class<? extends IndexUpdate>, String> ACTIONS =
      ImmutableMap.<Class<? extends IndexUpdate>, String>builder()
          .put(IndexUpdate.AssignUUID.class, ASSIGN_UUID)
          .put(IndexUpdate.AddSnapshot.class, ADD_SNAPSHOT)
          .put(IndexUpdate.RemoveSnapshots.class, REMOVE_SNAPSHOTS)
          .put(IndexUpdate.SetCurrentVersion.class, SET_CURRENT_VERSION)
          .put(IndexUpdate.AddVersion.class, ADD_VERSION)
          .put(IndexUpdate.SetLocation.class, SET_LOCATION)
          .buildOrThrow();

  public static String toJson(IndexUpdate indexUpdate) {
    return toJson(indexUpdate, false);
  }

  public static String toJson(IndexUpdate indexUpdate, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(indexUpdate, gen), pretty);
  }

  public static void toJson(IndexUpdate indexUpdate, JsonGenerator generator) throws IOException {
    String updateAction = ACTIONS.get(indexUpdate.getClass());

    Preconditions.checkArgument(
        updateAction != null,
        "Cannot convert index update to json. Unrecognized index update type: %s",
        indexUpdate.getClass().getName());

    generator.writeStartObject();
    generator.writeStringField(ACTION, updateAction);

    switch (updateAction) {
      case ASSIGN_UUID:
        writeAssignUUID((IndexUpdate.AssignUUID) indexUpdate, generator);
        break;
      case ADD_SNAPSHOT:
        writeAddIndexSnapshot((IndexUpdate.AddSnapshot) indexUpdate, generator);
        break;
      case REMOVE_SNAPSHOTS:
        writeRemoveIndexSnapshots((IndexUpdate.RemoveSnapshots) indexUpdate, generator);
        break;
      case SET_CURRENT_VERSION:
        writeSetIndexCurrentVersion((IndexUpdate.SetCurrentVersion) indexUpdate, generator);
        break;
      case ADD_VERSION:
        writeAddIndexVersion((IndexUpdate.AddVersion) indexUpdate, generator);
        break;
      case SET_LOCATION:
        writeSetLocation((IndexUpdate.SetLocation) indexUpdate, generator);
        break;
      default:
        throw new IllegalArgumentException(
            String.format(
                "Cannot convert index update to json. Unrecognized action: %s", updateAction));
    }

    generator.writeEndObject();
  }

  /**
   * Read an IndexUpdate from a JSON string.
   *
   * @param json a JSON string of an IndexUpdate
   * @return an IndexUpdate object
   */
  public static IndexUpdate fromJson(String json) {
    Preconditions.checkArgument(json != null, "Cannot parse index update from null string");
    return JsonUtil.parse(json, IndexUpdateParser::fromJson);
  }

  public static IndexUpdate fromJson(JsonNode jsonNode) {
    Preconditions.checkArgument(
        jsonNode != null && jsonNode.isObject(),
        "Cannot parse index update from non-object value: %s",
        jsonNode);
    Preconditions.checkArgument(
        jsonNode.hasNonNull(ACTION), "Cannot parse index update. Missing field: action");
    String action = JsonUtil.getString(ACTION, jsonNode).toLowerCase(Locale.ROOT);

    switch (action) {
      case ASSIGN_UUID:
        return readAssignUUID(jsonNode);
      case ADD_SNAPSHOT:
        return readAddIndexSnapshot(jsonNode);
      case REMOVE_SNAPSHOTS:
        return readRemoveIndexSnapshots(jsonNode);
      case SET_CURRENT_VERSION:
        return readSetIndexCurrentVersion(jsonNode);
      case ADD_VERSION:
        return readAddIndexVersion(jsonNode);
      case SET_LOCATION:
        return readSetLocation(jsonNode);
      default:
        throw new UnsupportedOperationException(
            String.format("Cannot convert index update action from json: %s", action));
    }
  }

  private static void writeAssignUUID(IndexUpdate.AssignUUID update, JsonGenerator gen)
      throws IOException {
    gen.writeStringField(UUID, update.uuid());
  }

  private static void writeAddIndexSnapshot(IndexUpdate.AddSnapshot update, JsonGenerator gen)
      throws IOException {
    gen.writeFieldName(SNAPSHOT);
    IndexSnapshotParser.toJson(update.indexSnapshot(), gen);
  }

  private static void writeRemoveIndexSnapshots(
      IndexUpdate.RemoveSnapshots update, JsonGenerator gen) throws IOException {
    JsonUtil.writeLongArray(SNAPSHOT_IDS, update.indexSnapshotIds(), gen);
  }

  private static void writeSetIndexCurrentVersion(
      IndexUpdate.SetCurrentVersion update, JsonGenerator gen) throws IOException {
    gen.writeNumberField(VERSION_ID, update.versionId());
  }

  private static void writeAddIndexVersion(IndexUpdate.AddVersion update, JsonGenerator gen)
      throws IOException {
    gen.writeFieldName(VERSION);
    IndexVersionParser.toJson(update.indexVersion(), gen);
  }

  private static void writeSetLocation(IndexUpdate.SetLocation update, JsonGenerator gen)
      throws IOException {
    gen.writeStringField(LOCATION, update.location());
  }

  private static IndexUpdate readAssignUUID(JsonNode node) {
    String uuid = JsonUtil.getString(UUID, node);
    return new IndexUpdate.AssignUUID(uuid);
  }

  private static IndexUpdate readAddIndexSnapshot(JsonNode node) {
    JsonNode snapshotNode = JsonUtil.get(SNAPSHOT, node);
    IndexSnapshot snapshot = IndexSnapshotParser.fromJson(snapshotNode);
    return new IndexUpdate.AddSnapshot(snapshot);
  }

  private static IndexUpdate readRemoveIndexSnapshots(JsonNode node) {
    Set<Long> snapshotIds = JsonUtil.getLongSetOrNull(SNAPSHOT_IDS, node);
    Preconditions.checkArgument(
        snapshotIds != null,
        "Invalid set of index snapshot ids to remove: must be non-null",
        snapshotIds);
    return new IndexUpdate.RemoveSnapshots(snapshotIds);
  }

  private static IndexUpdate readSetIndexCurrentVersion(JsonNode node) {
    int versionId = JsonUtil.getInt(VERSION_ID, node);
    return new IndexUpdate.SetCurrentVersion(versionId);
  }

  private static IndexUpdate readAddIndexVersion(JsonNode node) {
    JsonNode versionNode = JsonUtil.get(VERSION, node);
    IndexVersion version = IndexVersionParser.fromJson(versionNode);
    return new IndexUpdate.AddVersion(version);
  }

  private static IndexUpdate readSetLocation(JsonNode node) {
    String location = JsonUtil.getString(LOCATION, node);
    return new IndexUpdate.SetLocation(location);
  }
}
