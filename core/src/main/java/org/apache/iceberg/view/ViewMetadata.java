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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.ValidationException;
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
  default void check() {
    Preconditions.checkArgument(
        formatVersion() > 0 && formatVersion() <= ViewMetadata.SUPPORTED_VIEW_FORMAT_VERSION,
        "Unsupported format version: %s",
        formatVersion());
  }

  static Builder builder() {
    return new Builder();
  }

  static Builder buildFrom(ViewMetadata base) {
    return new Builder(base);
  }

  class Builder {
    private static final int LAST_ADDED = -1;
    private final List<ViewVersion> versions;
    private final List<Schema> schemas;
    private final List<ViewHistoryEntry> history;
    private final Map<String, String> properties;
    private final List<MetadataUpdate> changes;
    private int formatVersion = DEFAULT_VIEW_FORMAT_VERSION;
    private int currentVersionId;
    private String location;

    // internal change tracking
    private Integer lastAddedVersionId = null;

    // indexes
    private final Map<Integer, ViewVersion> versionsById;
    private final Map<Integer, Schema> schemasById;

    private Builder() {
      this.versions = Lists.newArrayList();
      this.versionsById = Maps.newHashMap();
      this.schemas = Lists.newArrayList();
      this.schemasById = Maps.newHashMap();
      this.history = Lists.newArrayList();
      this.properties = Maps.newHashMap();
      this.changes = Lists.newArrayList();
    }

    private Builder(ViewMetadata base) {
      this.versions = Lists.newArrayList(base.versions());
      this.versionsById = Maps.newHashMap(base.versionsById());
      this.schemas = Lists.newArrayList(base.schemas());
      this.schemasById = Maps.newHashMap(base.schemasById());
      this.history = Lists.newArrayList(base.history());
      this.properties = Maps.newHashMap(base.properties());
      this.changes = Lists.newArrayList();
      this.formatVersion = base.formatVersion();
      this.currentVersionId = base.currentVersionId();
      this.location = base.location();
    }

    public Builder upgradeFormatVersion(int newFormatVersion) {
      Preconditions.checkArgument(
          newFormatVersion >= formatVersion,
          "Cannot downgrade v%s view to v%s",
          formatVersion,
          newFormatVersion);

      if (formatVersion == newFormatVersion) {
        return this;
      }

      this.formatVersion = newFormatVersion;
      changes.add(new MetadataUpdate.UpgradeFormatVersion(newFormatVersion));
      return this;
    }

    public Builder setLocation(String newLocation) {
      Preconditions.checkArgument(null != newLocation, "Invalid location: null");
      if (null != location && location.equals(newLocation)) {
        return this;
      }

      this.location = newLocation;
      changes.add(new MetadataUpdate.SetLocation(newLocation));
      return this;
    }

    public Builder setCurrentVersionId(int newVersionId) {
      if (newVersionId == LAST_ADDED) {
        ValidationException.check(
            lastAddedVersionId != null,
            "Cannot set last version id: no current version id has been set");
        return setCurrentVersionId(lastAddedVersionId);
      }

      if (currentVersionId == newVersionId) {
        return this;
      }

      ViewVersion version = versionsById.get(newVersionId);
      Preconditions.checkArgument(
          version != null, "Cannot set current version to unknown version: %s", newVersionId);

      this.currentVersionId = newVersionId;

      if (lastAddedVersionId != null && lastAddedVersionId == newVersionId) {
        changes.add(new MetadataUpdate.SetCurrentViewVersion(LAST_ADDED));
      } else {
        changes.add(new MetadataUpdate.SetCurrentViewVersion(newVersionId));
      }

      return this;
    }

    public Builder setCurrentVersion(ViewVersion version, Schema schema) {
      int newSchemaId = addSchemaInternal(schema);
      ViewVersion newVersion =
          ImmutableViewVersion.builder().from(version).schemaId(newSchemaId).build();
      return setCurrentVersionId(addVersionInternal(newVersion));
    }

    public Builder addVersion(ViewVersion version) {
      addVersionInternal(version);
      return this;
    }

    private int addVersionInternal(ViewVersion version) {
      int newVersionId = reuseOrCreateNewViewVersionId(version);
      if (versionsById.containsKey(newVersionId)) {
        boolean addedInBuilder =
            changes(MetadataUpdate.AddViewVersion.class)
                .anyMatch(added -> added.viewVersion().versionId() == newVersionId);
        this.lastAddedVersionId = addedInBuilder ? newVersionId : null;
        return newVersionId;
      }

      Preconditions.checkArgument(
          schemasById.containsKey(version.schemaId()),
          "Cannot add version with unknown schema: %s",
          version.schemaId());

      ViewVersion newVersion;
      if (newVersionId != version.versionId()) {
        newVersion = ImmutableViewVersion.builder().from(version).versionId(newVersionId).build();
      } else {
        newVersion = version;
      }

      versions.add(newVersion);
      versionsById.put(newVersion.versionId(), newVersion);
      changes.add(new MetadataUpdate.AddViewVersion(newVersion));
      history.add(
          ImmutableViewHistoryEntry.builder()
              .timestampMillis(newVersion.timestampMillis())
              .versionId(newVersion.versionId())
              .build());

      this.lastAddedVersionId = newVersionId;

      return newVersionId;
    }

    private int reuseOrCreateNewViewVersionId(ViewVersion viewVersion) {
      // if the view version already exists, use its id; otherwise use the highest id + 1
      int newVersionId = viewVersion.versionId();
      for (ViewVersion version : versions) {
        if (version.equals(viewVersion)) {
          return version.versionId();
        } else if (version.versionId() >= newVersionId) {
          newVersionId = viewVersion.versionId() + 1;
        }
      }

      return newVersionId;
    }

    public Builder addSchema(Schema schema) {
      addSchemaInternal(schema);
      return this;
    }

    private int addSchemaInternal(Schema schema) {
      int newSchemaId = reuseOrCreateNewSchemaId(schema);
      if (schemasById.containsKey(newSchemaId)) {
        // this schema existed or was already added in the builder
        return newSchemaId;
      }

      Schema newSchema;
      if (newSchemaId != schema.schemaId()) {
        newSchema = new Schema(newSchemaId, schema.columns(), schema.identifierFieldIds());
      } else {
        newSchema = schema;
      }

      int highestFieldId = Math.max(highestFieldId(), newSchema.highestFieldId());
      schemas.add(newSchema);
      schemasById.put(newSchema.schemaId(), newSchema);
      changes.add(new MetadataUpdate.AddSchema(newSchema, highestFieldId));

      return newSchemaId;
    }

    private int highestFieldId() {
      return schemas.stream().map(Schema::highestFieldId).max(Integer::compareTo).orElse(0);
    }

    private int reuseOrCreateNewSchemaId(Schema newSchema) {
      // if the schema already exists, use its id; otherwise use the highest id + 1
      int newSchemaId = newSchema.schemaId();
      for (Schema schema : schemas) {
        if (schema.sameSchema(newSchema)) {
          return schema.schemaId();
        } else if (schema.schemaId() >= newSchemaId) {
          newSchemaId = schema.schemaId() + 1;
        }
      }

      return newSchemaId;
    }

    public Builder setProperties(Map<String, String> updated) {
      if (updated.isEmpty()) {
        return this;
      }

      properties.putAll(updated);
      changes.add(new MetadataUpdate.SetProperties(updated));
      return this;
    }

    public Builder removeProperties(Set<String> propertiesToRemove) {
      if (propertiesToRemove.isEmpty()) {
        return this;
      }

      propertiesToRemove.forEach(properties::remove);
      changes.add(new MetadataUpdate.RemoveProperties(propertiesToRemove));
      return this;
    }

    public ViewMetadata build() {
      Preconditions.checkArgument(null != location, "Invalid location: null");
      Preconditions.checkArgument(versions.size() > 0, "Invalid view: no versions were added");

      int historySize =
          PropertyUtil.propertyAsInt(
              properties,
              ViewProperties.VERSION_HISTORY_SIZE,
              ViewProperties.VERSION_HISTORY_SIZE_DEFAULT);

      Preconditions.checkArgument(
          historySize > 0,
          "%s must be positive but was %s",
          ViewProperties.VERSION_HISTORY_SIZE,
          historySize);

      // expire old versions, but keep at least the versions added in this builder
      int numAddedVersions = (int) changes(MetadataUpdate.AddViewVersion.class).count();
      int numVersionsToKeep = Math.max(numAddedVersions, historySize);

      List<ViewVersion> retainedVersions;
      List<ViewHistoryEntry> retainedHistory;
      if (versions.size() > numVersionsToKeep) {
        retainedVersions = expireVersions(versionsById, numVersionsToKeep);
        Set<Integer> retainedVersionIds =
            retainedVersions.stream().map(ViewVersion::versionId).collect(Collectors.toSet());
        retainedHistory = updateHistory(history, retainedVersionIds);
      } else {
        retainedVersions = versions;
        retainedHistory = history;
      }

      return ImmutableViewMetadata.of(
          formatVersion,
          location,
          schemas,
          currentVersionId,
          retainedVersions,
          retainedHistory,
          properties,
          changes);
    }

    static List<ViewVersion> expireVersions(
        Map<Integer, ViewVersion> versionsById, int numVersionsToKeep) {
      // version ids are assigned sequentially. keep the latest versions by ID.
      List<Integer> ids = Lists.newArrayList(versionsById.keySet());
      ids.sort(Comparator.reverseOrder());

      List<ViewVersion> retainedVersions = Lists.newArrayList();
      for (int idToKeep : ids.subList(0, numVersionsToKeep)) {
        retainedVersions.add(versionsById.get(idToKeep));
      }

      return retainedVersions;
    }

    static List<ViewHistoryEntry> updateHistory(List<ViewHistoryEntry> history, Set<Integer> ids) {
      List<ViewHistoryEntry> retainedHistory = Lists.newArrayList();
      for (ViewHistoryEntry entry : history) {
        if (ids.contains(entry.versionId())) {
          retainedHistory.add(entry);
        } else {
          // clear history past any unknown version
          retainedHistory.clear();
        }
      }

      return retainedHistory;
    }

    private <U extends MetadataUpdate> Stream<U> changes(Class<U> updateClass) {
      return changes.stream().filter(updateClass::isInstance).map(updateClass::cast);
    }
  }
}
