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
    private static final int LAST_ADDED = -1;
    private int formatVersion = DEFAULT_VIEW_FORMAT_VERSION;
    private String location;
    private List<Schema> schemas = Lists.newArrayList();
    private int currentVersionId;
    private Integer lastAddedVersionId;
    private Integer lastAddedSchemaId;
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
      if (newVersionId == -1) {
        ValidationException.check(
            lastAddedVersionId != null,
            "Cannot set last version id: no current version id has been set");
        return setCurrentVersionId(lastAddedVersionId);
      }

      if (this.currentVersionId == newVersionId) {
        return this;
      }

      checkCurrentVersionIdIsValid(newVersionId);

      // by that time, schemas are defined and the highestFieldId can be determined and schema
      // changes can be added
      int highestFieldId = highestFieldId();
      for (Schema schema : schemas) {
        this.changes.add(new MetadataUpdate.AddSchema(schema, highestFieldId));
      }

      this.currentVersionId = newVersionId;

      if (lastAddedVersionId != null && lastAddedVersionId == newVersionId) {
        this.changes.add(new MetadataUpdate.SetCurrentViewVersion(LAST_ADDED));
      } else {
        this.changes.add(new MetadataUpdate.SetCurrentViewVersion(newVersionId));
      }

      return this;
    }

    private int highestFieldId() {
      return schemas.stream().map(Schema::highestFieldId).max(Integer::compareTo).orElse(0);
    }

    public Builder setCurrentVersion(ViewVersion version) {
      Schema schema = indexSchemas(schemas).get(version.schemaId());
      int newSchemaId = addSchemaInternal(schema);
      return setCurrentVersionId(
          addVersionInternal(
              ImmutableViewVersion.builder().from(version).schemaId(newSchemaId).build()));
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

    private int addVersionInternal(ViewVersion version) {
      int newVersionId = reuseOrCreateNewViewVersionId(version);

      if (versions.stream().anyMatch(v -> v.versionId() == newVersionId)) {
        boolean isNewVersion =
            lastAddedVersionId != null
                && changes(MetadataUpdate.AddViewVersion.class)
                    .anyMatch(added -> added.viewVersion().versionId() == newVersionId);
        this.lastAddedVersionId = isNewVersion ? newVersionId : null;
        return newVersionId;
      }

      ViewVersion newVersion;
      if (newVersionId != version.versionId()) {
        newVersion = ImmutableViewVersion.builder().from(version).versionId(newVersionId).build();
      } else {
        newVersion = version;
      }

      this.versions.add(newVersion);
      this.changes.add(new MetadataUpdate.AddViewVersion(newVersion));
      this.history.add(
          ImmutableViewHistoryEntry.builder()
              .timestampMillis(newVersion.timestampMillis())
              .versionId(newVersion.versionId())
              .build());
      this.lastAddedVersionId = newVersionId;

      return newVersionId;
    }

    private int addSchemaInternal(Schema schema) {
      if (schema.schemaId() == -1) {
        ValidationException.check(
            lastAddedSchemaId != null, "Cannot set last added schema: no schema has been added");

        return addSchemaInternal(
            new Schema(lastAddedSchemaId, schema.columns(), schema.identifierFieldIds()));
      }

      int newSchemaId = reuseOrCreateNewSchemaId(schema);
      if (schemas.stream().anyMatch(s -> s.schemaId() == newSchemaId)) {
        boolean isNewSchema =
            lastAddedSchemaId != null
                && changes(MetadataUpdate.AddSchema.class)
                    .anyMatch(added -> added.schema().schemaId() == newSchemaId);
        this.lastAddedSchemaId = isNewSchema ? newSchemaId : null;
        return newSchemaId;
      }

      Schema newSchema;
      if (newSchemaId != schema.schemaId()) {
        newSchema = new Schema(newSchemaId, schema.columns(), schema.identifierFieldIds());
      } else {
        newSchema = schema;
      }

      this.schemas.add(newSchema);
      // AddSchema changes can only be added once all schemas are known, thus this is done in
      // setCurrentVersionId(..)
      this.lastAddedSchemaId = newSchemaId;

      return newSchemaId;
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

    public Builder addSchema(Schema schema) {
      addSchemaInternal(schema);
      return this;
    }

    public Builder addVersion(ViewVersion version) {
      addVersionInternal(version);
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

    private <U extends MetadataUpdate> Stream<U> changes(Class<U> updateClass) {
      return changes.stream().filter(updateClass::isInstance).map(updateClass::cast);
    }
  }
}
