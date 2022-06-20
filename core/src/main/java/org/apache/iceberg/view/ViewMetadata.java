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
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Ordering;

/** Metadata for versioning a view. */
public class ViewMetadata implements Serializable {
  static final int DEFAULT_VIEW_FORMAT_VERSION = 1;
  static final int SUPPORTED_VIEW_FORMAT_VERSION = 1;

  // stored metadata
  private final int formatVersion;
  private final String location;
  private final Map<String, String> properties;
  private final int currentVersionId;
  private final int currentSchemaId;
  private final List<ViewVersion> versions;
  private final Map<Long, ViewVersion> versionsById;
  private final List<ViewHistoryEntry> versionLog;
  private final List<Schema> schemas;
  private final Map<Integer, Schema> schemasById;

  public static Builder builder() {
    return new Builder();
  }

  public static Builder buildFrom(ViewMetadata metadata) {
    return builder()
        .location(metadata.location())
        .properties(metadata.properties())
        .currentVersionId(metadata.currentVersionId())
        .versions(metadata.versions())
        .schemas(metadata.schemas())
        .currentSchemaId(metadata.currentSchemaId())
        .history(metadata.history());
  }

  // Creates a new view version metadata by simply assigning variables
  private ViewMetadata(
      String location,
      Map<String, String> properties,
      int currentVersionId,
      List<ViewVersion> versions,
      List<ViewHistoryEntry> versionLog,
      List<Schema> schemas,
      int currentSchemaId) {
    this.formatVersion = DEFAULT_VIEW_FORMAT_VERSION;
    this.location = location;
    this.properties = properties;
    this.currentVersionId = currentVersionId;
    Preconditions.checkState(versions.size() > 0);
    this.versions = versions;
    checkVersionLog(versionLog);
    this.versionLog = versionLog;
    this.versionsById = indexVersions(versions);
    this.schemas = schemas;
    this.schemasById = indexSchemas(schemas);
    this.currentSchemaId = currentSchemaId;
  }

  public int formatVersion() {
    return formatVersion;
  }

  public String location() {
    return location;
  }

  public Map<String, String> properties() {
    return properties;
  }

  public ViewVersion version(int versionId) {
    return versionsById.get(versionId);
  }

  public ViewVersion currentVersion() {
    return versionsById.get(currentVersionId);
  }

  public int currentVersionId() {
    return currentVersionId;
  }

  public List<ViewVersion> versions() {
    return versions;
  }

  public List<ViewHistoryEntry> history() {
    return versionLog;
  }

  public List<Schema> schemas() {
    return schemas;
  }

  public int currentSchemaId() {
    return currentSchemaId;
  }

  public Schema schema() {
    return schemasById.get(currentSchemaId);
  }

  @Override
  public String toString() {
    return "ViewMetadata{"
        + "formatVersion="
        + formatVersion
        + ", location='"
        + location
        + '\''
        + ", properties="
        + properties
        + ", currentVersionId="
        + currentVersionId
        + ", versions="
        + versions
        + ", versionsById="
        + versionsById
        + ", versionLog="
        + versionLog
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ViewMetadata that = (ViewMetadata) o;

    if (formatVersion != that.formatVersion) {
      return false;
    }
    if (currentVersionId != that.currentVersionId) {
      return false;
    }
    if (!location.equals(that.location)) {
      return false;
    }
    if (!properties.equals(that.properties)) {
      return false;
    }
    if (!versions.equals(that.versions)) {
      return false;
    }
    return versionLog.equals(that.versionLog);
  }

  @Override
  public int hashCode() {
    int result = formatVersion;
    result = 31 * result + location.hashCode();
    result = 31 * result + properties.hashCode();
    result = 31 * result + currentVersionId;
    result = 31 * result + versions.hashCode();
    result = 31 * result + versionLog.hashCode();
    return result;
  }

  private static void checkVersionLog(List<ViewHistoryEntry> versionLog) {
    Preconditions.checkState(versionLog.size() > 0);
    Preconditions.checkState(
        Ordering.from(Comparator.comparing(ViewHistoryEntry::timestampMillis))
            .isOrdered(versionLog),
        "[BUG] Expected sorted version log entries.");
  }

  private static Map<Long, ViewVersion> indexVersions(List<ViewVersion> versions) {
    ImmutableMap.Builder<Long, ViewVersion> builder = ImmutableMap.builder();
    for (ViewVersion version : versions) {
      builder.put(version.versionId(), version);
    }
    return builder.build();
  }

  private static Map<Integer, Schema> indexSchemas(List<Schema> schemas) {
    ImmutableMap.Builder<Integer, Schema> builder = ImmutableMap.builder();
    for (Schema schema : schemas) {
      builder.put(schema.schemaId(), schema);
    }
    return builder.build();
  }

  public ViewMetadata replaceProperties(Map<String, String> newProperties) {
    ValidationException.check(newProperties != null, "Cannot set properties to null");

    return buildFrom(this).properties(newProperties).build();
  }

  public static final class Builder {
    private String location;
    private Map<String, String> properties = Maps.newHashMap();
    private int currentVersionId;
    private List<ViewVersion> versions = Lists.newArrayList();
    private List<ViewHistoryEntry> versionLog = Lists.newArrayList();
    private int currentSchemaId;
    private List<Schema> schemas;

    private Builder() {}

    public Builder location(String value) {
      location = value;
      return this;
    }

    public Builder properties(Map<String, String> value) {
      properties = value;
      return this;
    }

    public Builder currentVersionId(int value) {
      currentVersionId = value;
      return this;
    }

    public Builder versions(List<ViewVersion> value) {
      versions = value;
      return this;
    }

    public Builder history(List<ViewHistoryEntry> value) {
      versionLog = value;
      return this;
    }

    public Builder addVersion(ViewVersion version) {
      versions.add(version);
      versionLog.add(BaseViewHistoryEntry.of(version.timestampMillis(), version.versionId()));
      return this;
    }

    public Builder keepVersions(int numVersionsToKeep) {
      if (versions.size() > numVersionsToKeep) {
        versions = versions.subList(versions.size() - numVersionsToKeep, numVersionsToKeep);
      }
      return this;
    }

    public Builder keepHistory(int numVersionsToKeep) {
      if (versionLog.size() > numVersionsToKeep) {
        versionLog = versionLog.subList(versionLog.size() - numVersionsToKeep, numVersionsToKeep);
      }
      return this;
    }

    public Builder schemas(List<Schema> schemas) {
      this.schemas = schemas;
      return this;
    }

    public Builder currentSchemaId(int currentSchemaId) {
      this.currentSchemaId = currentSchemaId;
      return this;
    }

    public ViewMetadata build() {
      return new ViewMetadata(
          location, properties, currentVersionId, versions, versionLog, schemas, currentSchemaId);
    }
  }
}
