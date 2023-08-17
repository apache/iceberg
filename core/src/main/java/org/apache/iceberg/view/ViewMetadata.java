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
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;
import org.immutables.value.Value;
import org.immutables.value.Value.Style.ImplementationVisibility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("ImmutablesStyle")
@Value.Immutable(builder = false)
@Value.Style(allParameters = true, visibility = ImplementationVisibility.PACKAGE)
public interface ViewMetadata extends Serializable {
  Logger LOG = LoggerFactory.getLogger(ViewMetadata.class);
  int SUPPORTED_VIEW_FORMAT_VERSION = 1;
  int DEFAULT_VIEW_FORMAT_VERSION = 1;

  int formatVersion();

  String location();

  default Integer currentSchemaId() {
    // fail when accessing the current schema if ViewMetadata was created through the
    // ViewMetadataParser with an invalid schema id
    int currentSchemaId = currentVersion().schemaId();
    Preconditions.checkArgument(
        schemasById().containsKey(currentSchemaId),
        "Cannot find current schema with id %s in schemas: %s",
        currentSchemaId,
        schemasById().keySet());

    return currentSchemaId;
  }

  List<Schema> schemas();

  int currentVersionId();

  List<ViewVersion> versions();

  List<ViewHistoryEntry> history();

  Map<String, String> properties();

  List<MetadataUpdate> changes();

  default ViewVersion version(int versionId) {
    return versionsById().get(versionId);
  }

  default ViewVersion currentVersion() {
    // fail when accessing the current version if ViewMetadata was created through the
    // ViewMetadataParser with an invalid view version id
    Preconditions.checkArgument(
        versionsById().containsKey(currentVersionId()),
        "Cannot find current version %s in view versions: %s",
        currentVersionId(),
        versionsById().keySet());

    return versionsById().get(currentVersionId());
  }

  @Value.Derived
  default Map<Integer, ViewVersion> versionsById() {
    return Builder.indexVersions(versions());
  }

  @Value.Derived
  default Map<Integer, Schema> schemasById() {
    return Builder.indexSchemas(schemas());
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

    return this;
  }

  static Builder builder() {
    return new Builder();
  }

  static Builder buildFrom(ViewMetadata base) {
    return new Builder(base);
  }

  class Builder {
    private int formatVersion = DEFAULT_VIEW_FORMAT_VERSION;
    private String location;
    private List<Schema> schemas = Lists.newArrayList();
    private int currentVersionId;
    private int lastAddedVersionId;
    private List<ViewVersion> versions = Lists.newArrayList();
    private List<ViewHistoryEntry> history = Lists.newArrayList();
    private Map<String, String> properties = Maps.newHashMap();
    private List<MetadataUpdate> changes = Lists.newArrayList();

    private Builder() {}

    private Builder(ViewMetadata base) {
      this.formatVersion = base.formatVersion();
      this.location = base.location();
      this.schemas = Lists.newArrayList(base.schemas());
      this.currentVersionId = base.currentVersionId();
      this.versions = Lists.newArrayList(base.versions());
      this.history = Lists.newArrayList(base.history());
      this.properties = Maps.newHashMap(base.properties());
      this.changes = Lists.newArrayList(base.changes());
    }

    public Builder upgradeFormatVersion(int newFormatVersion) {
      Preconditions.checkArgument(
          newFormatVersion >= this.formatVersion,
          "Cannot downgrade v%s view to v%s",
          formatVersion,
          newFormatVersion);

      if (this.formatVersion == newFormatVersion) {
        return this;
      }

      this.formatVersion = newFormatVersion;
      this.changes.add(new MetadataUpdate.UpgradeFormatVersion(newFormatVersion));
      return this;
    }

    public Builder setLocation(String newLocation) {
      Preconditions.checkArgument(null != newLocation, "Invalid location: null");
      if (null != this.location && this.location.equals(newLocation)) {
        return this;
      }

      this.location = newLocation;
      this.changes.add(new MetadataUpdate.SetLocation(newLocation));
      return this;
    }

    public Builder setCurrentVersionId(int newVersionId) {
      if (this.currentVersionId == newVersionId) {
        return this;
      }

      checkCurrentVersionIdIsValid(newVersionId);

      this.currentVersionId = newVersionId;
      this.changes.add(new MetadataUpdate.SetCurrentViewVersion(newVersionId));
      return this;
    }

    //    public Builder setCurrentVersion(ViewVersion version) {
    //      Schema schema = schemasById.get(version.schemaId());
    //      int newSchemaId = addSchemaInternal(schema);
    //      return setCurrentVersionId(addVersionInternal(version), newSchemaId);
    //    }

    public Builder addSchema(Schema schema) {
      // adding schema to changes is handled in build()
      this.schemas.add(schema);
      return this;
    }

    public Builder addVersion(ViewVersion version) {
      this.versions.add(version);
      this.changes.add(new MetadataUpdate.AddViewVersion(version));
      this.history.add(
          ImmutableViewHistoryEntry.builder()
              .timestampMillis(version.timestampMillis())
              .versionId(version.versionId())
              .build());
      return this;
    }

    public Builder setProperties(Map<String, String> updated) {
      if (updated.isEmpty()) {
        return this;
      }

      this.properties.putAll(updated);
      this.changes.add(new MetadataUpdate.SetProperties(updated));
      return this;
    }

    public Builder removeProperties(Set<String> propertiesToRemove) {
      if (propertiesToRemove.isEmpty()) {
        return this;
      }

      propertiesToRemove.forEach(this.properties::remove);
      this.changes.add(new MetadataUpdate.RemoveProperties(propertiesToRemove));
      return this;
    }

    private static Map<Integer, ViewVersion> indexVersions(List<ViewVersion> versionsToIndex) {
      ImmutableMap.Builder<Integer, ViewVersion> builder = ImmutableMap.builder();
      for (ViewVersion version : versionsToIndex) {
        builder.put(version.versionId(), version);
      }

      return builder.build();
    }

    private static Map<Integer, Schema> indexSchemas(List<Schema> schemasToIndex) {
      ImmutableMap.Builder<Integer, Schema> builder = ImmutableMap.builder();
      for (Schema schema : schemasToIndex) {
        builder.put(schema.schemaId(), schema);
      }

      return builder.build();
    }

    public ViewMetadata build() {
      Preconditions.checkArgument(null != location, "Invalid location: null");
      Preconditions.checkArgument(versions.size() > 0, "Invalid view: no versions were added");

      checkCurrentVersionIdIsValid(currentVersionId);

      int highestFieldId =
          Lists.newArrayList(schemas).stream()
              .map(Schema::highestFieldId)
              .max(Integer::compareTo)
              .orElse(0);
      for (Schema schema : schemas) {
        this.changes.add(new MetadataUpdate.AddSchema(schema, highestFieldId));
      }

      int versionHistorySizeToKeep =
          PropertyUtil.propertyAsInt(
              properties,
              ViewProperties.VERSION_HISTORY_SIZE,
              ViewProperties.VERSION_HISTORY_SIZE_DEFAULT);

      Preconditions.checkArgument(
          versionHistorySizeToKeep > 0,
          "%s must be positive but was %s",
          ViewProperties.VERSION_HISTORY_SIZE,
          versionHistorySizeToKeep);

      if (versions.size() > versionHistorySizeToKeep) {
        List<ViewVersion> versionsToKeep =
            versions.subList(versions.size() - versionHistorySizeToKeep, versions.size());
        List<ViewHistoryEntry> historyToKeep =
            history.subList(history.size() - versionHistorySizeToKeep, history.size());
        List<MetadataUpdate> changesToKeep = Lists.newArrayList(changes);
        Set<MetadataUpdate.AddViewVersion> toRemove =
            changesToKeep.stream()
                .filter(update -> update instanceof MetadataUpdate.AddViewVersion)
                .map(update -> (MetadataUpdate.AddViewVersion) update)
                .filter(
                    update ->
                        update.viewVersion().versionId() != currentVersionId
                            && !versionsToKeep.contains(update.viewVersion()))
                .collect(Collectors.toSet());
        changesToKeep.removeAll(toRemove);

        versions = versionsToKeep;
        history = historyToKeep;
        changes = changesToKeep;
      }

      return ImmutableViewMetadata.of(
          formatVersion,
          location,
          schemas,
          currentVersionId,
          versions,
          history,
          properties,
          changes);
    }

    private void checkCurrentVersionIdIsValid(int versionId) {
      Map<Integer, ViewVersion> versionsById = indexVersions(versions);
      Preconditions.checkArgument(
          versionsById.containsKey(versionId),
          "Cannot find current version %s in view versions: %s",
          versionId,
          versionsById.keySet());

      int currentSchemaId = versionsById.get(versionId).schemaId();
      Map<Integer, Schema> schemasById = indexSchemas(schemas);
      Preconditions.checkArgument(
          schemasById.containsKey(currentSchemaId),
          "Cannot find current schema with id %s in schemas: %s",
          currentSchemaId,
          schemasById.keySet());
    }
  }
}
