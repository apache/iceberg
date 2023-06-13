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
package org.apache.iceberg.view;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.PropertyUtil;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Value.Immutable
public interface ViewMetadata extends Serializable {
  Logger LOG = LoggerFactory.getLogger(ViewMetadata.class);
  int SUPPORTED_VIEW_FORMAT_VERSION = 1;

  int formatVersion();

  String location();

  Integer currentSchemaId();

  List<Schema> schemas();

  int currentVersionId();

  List<ViewVersion> versions();

  List<ViewHistoryEntry> history();

  Map<String, String> properties();

  default ViewVersion version(int versionId) {
    return versionsById().get(versionId);
  }

  default ViewVersion currentVersion() {
    return versionsById().get(currentVersionId());
  }

  @Value.Derived
  default Map<Integer, ViewVersion> versionsById() {
    ImmutableMap.Builder<Integer, ViewVersion> builder = ImmutableMap.builder();
    for (ViewVersion version : versions()) {
      builder.put(version.versionId(), version);
    }

    return builder.build();
  }

  @Value.Derived
  default Map<Integer, Schema> schemasById() {
    ImmutableMap.Builder<Integer, Schema> builder = ImmutableMap.builder();
    for (Schema schema : schemas()) {
      builder.put(schema.schemaId(), schema);
    }

    return builder.build();
  }

  default Schema schema() {
    return schemasById().get(currentSchemaId());
  }

  @Value.Check
  default ViewMetadata checkAndNormalize() {
    Preconditions.checkArgument(
        formatVersion() > 0 && formatVersion() <= ViewMetadata.SUPPORTED_VIEW_FORMAT_VERSION,
        "Unsupported format version: %s",
        formatVersion());

    Preconditions.checkArgument(versions().size() > 0, "Invalid view versions: empty");
    Preconditions.checkArgument(history().size() > 0, "Invalid view history: empty");
    Preconditions.checkArgument(schemas().size() > 0, "Invalid schemas: empty");

    Preconditions.checkArgument(
        versionsById().containsKey(currentVersionId()),
        "Cannot find current version %s in view versions: %s",
        currentVersionId(),
        versionsById().keySet());

    Preconditions.checkArgument(
        schemasById().containsKey(currentSchemaId()),
        "Cannot find current schema with id %s in schemas: %s",
        currentSchemaId(),
        schemasById().keySet());

    int versionHistorySizeToKeep =
        PropertyUtil.propertyAsInt(
            properties(),
            ViewProperties.VERSION_HISTORY_SIZE,
            ViewProperties.VERSION_HISTORY_SIZE_DEFAULT);

    if (versionHistorySizeToKeep <= 0) {
      LOG.warn(
          "{} must be positive but was {}",
          ViewProperties.VERSION_HISTORY_SIZE,
          versionHistorySizeToKeep);
    } else if (versions().size() > versionHistorySizeToKeep) {
      List<ViewVersion> versions =
          versions().subList(versions().size() - versionHistorySizeToKeep, versions().size());
      List<ViewHistoryEntry> history =
          history().subList(history().size() - versionHistorySizeToKeep, history().size());
      return ImmutableViewMetadata.builder().from(this).versions(versions).history(history).build();
    }

    return this;
  }
}
