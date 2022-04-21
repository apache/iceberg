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
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.JsonUtil;

public class MetadataUpdateParser {

  private MetadataUpdateParser() {
  }

  private static final String ACTION = "action";

  // action types
  private static final String ASSIGN_UUID = "assign-uuid";
  private static final String UPGRADE_FORMAT_VERSION = "upgrade-format-version";
  private static final String ADD_SCHEMA = "add-schema";
  private static final String SET_CURRENT_SCHEMA = "set-current-schema";
  private static final String ADD_PARTITION_SPEC = "add-spec";
  private static final String SET_DEFAULT_PARTITION_SPEC = "set-default-spec";
  private static final String ADD_SORT_ORDER = "add-sort-order";
  private static final String SET_DEFAULT_SORT_ORDER = "set-default-sort-order";
  private static final String ADD_SNAPSHOT = "add-snapshot";
  private static final String REMOVE_SNAPSHOT = "remove-snapshot";
  private static final String SET_SNAPSHOT_REF = "set-snapshot-ref";
  private static final String SET_PROPERTIES = "set-properties";
  private static final String REMOVE_PROPERTIES = "remove-properties";
  private static final String SET_LOCATION = "set-location";

  // UpgradeFormatVersion
  private static final String FORMAT_VERSION = "format-version";

  private static final Map<Class<? extends MetadataUpdate>, String> ACTIONS = ImmutableMap
      .<Class<? extends MetadataUpdate>, String>builder()
      .put(MetadataUpdate.AssignUUID.class, ASSIGN_UUID)
      .put(MetadataUpdate.UpgradeFormatVersion.class, UPGRADE_FORMAT_VERSION)
      .put(MetadataUpdate.AddSchema.class, ADD_SCHEMA)
      .put(MetadataUpdate.SetCurrentSchema.class, SET_CURRENT_SCHEMA)
      .put(MetadataUpdate.AddPartitionSpec.class, ADD_PARTITION_SPEC)
      .put(MetadataUpdate.SetDefaultPartitionSpec.class, SET_DEFAULT_PARTITION_SPEC)
      .put(MetadataUpdate.AddSortOrder.class, ADD_SORT_ORDER)
      .put(MetadataUpdate.SetDefaultSortOrder.class, SET_DEFAULT_SORT_ORDER)
      .put(MetadataUpdate.AddSnapshot.class, ADD_SNAPSHOT)
      .put(MetadataUpdate.RemoveSnapshot.class, REMOVE_SNAPSHOT)
      .put(MetadataUpdate.SetSnapshotRef.class, SET_SNAPSHOT_REF)
      .put(MetadataUpdate.SetProperties.class, SET_PROPERTIES)
      .put(MetadataUpdate.RemoveProperties.class, REMOVE_PROPERTIES)
      .put(MetadataUpdate.SetLocation.class, SET_LOCATION)
      .build();

  public static String toJson(MetadataUpdate metadataUpdate) {
    return toJson(metadataUpdate, false);
  }

  public static String toJson(MetadataUpdate metadataUpdate, boolean pretty) {
    try {
      StringWriter writer = new StringWriter();
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
      if (pretty) {
        generator.useDefaultPrettyPrinter();
      }
      toJson(metadataUpdate, generator);
      generator.flush();
      return writer.toString();
    } catch (IOException e) {
      throw new UncheckedIOException(String.format("Failed to write metadata update json for: %s", metadataUpdate), e);
    }
  }

  public static void toJson(MetadataUpdate metadataUpdate, JsonGenerator generator) throws IOException {
    String updateAction = ACTIONS.get(metadataUpdate.getClass());

    // Add shared `action` field used to differentiate between implementations of MetadataUpdate
    generator.writeStartObject();
    generator.writeStringField(ACTION, updateAction);

    // Write subclass specific fields
    switch (updateAction) {
      case ASSIGN_UUID:
        throw new UnsupportedOperationException("Not Implemented: MetadataUpdate#toJson for AssignUUID");
      case UPGRADE_FORMAT_VERSION:
        writeUpgradeFormatVersion((MetadataUpdate.UpgradeFormatVersion) metadataUpdate, generator);
        break;
      default:
        throw new IllegalArgumentException(
            String.format("Cannot convert metadata update to json. Unrecognized action: %s", updateAction));
    }

    generator.writeEndObject();
  }

  /**
   * Read MetadataUpdate from a JSON string.
   *
   * @param json a JSON string of a MetadataUpdate
   * @return a MetadataUpdate object
   */
  public static MetadataUpdate fromJson(String json) {
    try {
      return fromJson(JsonUtil.mapper().readValue(json, JsonNode.class));
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read JSON string: " + json, e);
    }
  }

  public static MetadataUpdate fromJson(JsonNode jsonNode) {
    Preconditions.checkArgument(jsonNode != null && jsonNode.isObject(),
        "Cannot parse metadata update from non-object value: %s", jsonNode);
    Preconditions.checkArgument(jsonNode.hasNonNull(ACTION), "Cannot parse metadata update. Missing field: action");
    String action = JsonUtil.getString(ACTION, jsonNode).toLowerCase(Locale.ROOT);

    switch (action) {
      case ASSIGN_UUID:
        throw new UnsupportedOperationException("Not implemented: AssignUUID");
      case UPGRADE_FORMAT_VERSION:
        return readAsUpgradeFormatVersion(jsonNode);
      default:
        throw new UnsupportedOperationException(
            String.format("Cannot convert metadata update action to json: %s", action));
    }
  }

  // Write all fields specific to UpgradeFormatVersion
  private static void writeUpgradeFormatVersion(MetadataUpdate.UpgradeFormatVersion update, JsonGenerator gen)
      throws IOException {
    gen.writeNumberField(FORMAT_VERSION, update.formatVersion());
  }

  private static MetadataUpdate readAsUpgradeFormatVersion(JsonNode node) {
    int formatVersion = JsonUtil.getInt(FORMAT_VERSION, node);
    return new MetadataUpdate.UpgradeFormatVersion(formatVersion);
  }
}

