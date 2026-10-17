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

import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/** Immutable implementation of {@link IndexMetadata}. */
public class GenericIndexMetadata implements IndexMetadata {

  private final int formatVersion;
  private final String uuid;
  private final String tableUuid;
  private final String location;
  private final String type;
  private final String transformFunction;
  private final List<Integer> keyColumnIds;
  private final List<Integer> includedColumnIds;
  private final Map<String, String> properties;
  private final Long currentSnapshotId;
  private final List<IndexSnapshot> snapshots;
  private final String metadataFileLocation;

  private GenericIndexMetadata(
      int formatVersion,
      String uuid,
      String tableUuid,
      String location,
      String type,
      String transformFunction,
      List<Integer> keyColumnIds,
      List<Integer> includedColumnIds,
      Map<String, String> properties,
      Long currentSnapshotId,
      List<IndexSnapshot> snapshots,
      String metadataFileLocation) {
    this.formatVersion = formatVersion;
    this.uuid = uuid;
    this.tableUuid = tableUuid;
    this.location = location;
    this.type = type;
    this.transformFunction = transformFunction;
    this.keyColumnIds = ImmutableList.copyOf(keyColumnIds);
    this.includedColumnIds =
        includedColumnIds != null ? ImmutableList.copyOf(includedColumnIds) : ImmutableList.of();
    this.properties = properties != null ? ImmutableMap.copyOf(properties) : ImmutableMap.of();
    this.currentSnapshotId = currentSnapshotId;
    this.snapshots = ImmutableList.copyOf(snapshots);
    this.metadataFileLocation = metadataFileLocation;
  }

  @Override
  public int formatVersion() {
    return formatVersion;
  }

  @Override
  public String uuid() {
    return uuid;
  }

  @Override
  public String tableUuid() {
    return tableUuid;
  }

  @Override
  public String location() {
    return location;
  }

  @Override
  public String type() {
    return type;
  }

  @Override
  public String transformFunction() {
    return transformFunction;
  }

  @Override
  public List<Integer> keyColumnIds() {
    return keyColumnIds;
  }

  @Override
  public List<Integer> includedColumnIds() {
    return includedColumnIds;
  }

  @Override
  public Map<String, String> properties() {
    return properties;
  }

  @Override
  @Nullable
  public Long currentSnapshotId() {
    return currentSnapshotId;
  }

  @Override
  public List<IndexSnapshot> snapshots() {
    return snapshots;
  }

  @Override
  @Nullable
  public String metadataFileLocation() {
    return metadataFileLocation;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder buildFrom(IndexMetadata base) {
    return new Builder(base);
  }

  /** Builder for {@link GenericIndexMetadata}. */
  public static class Builder {
    private int formatVersion = DEFAULT_INDEX_FORMAT_VERSION;
    private String uuid;
    private String tableUuid;
    private String location;
    private String type;
    private String transformFunction;
    private List<Integer> keyColumnIds = ImmutableList.of();
    private List<Integer> includedColumnIds = ImmutableList.of();
    private Map<String, String> properties = ImmutableMap.of();
    private Long currentSnapshotId;
    private final List<IndexSnapshot> snapshots;
    private String metadataFileLocation;

    private Builder() {
      this.uuid = UUID.randomUUID().toString();
      this.snapshots = Lists.newArrayList();
    }

    private Builder(IndexMetadata base) {
      this.formatVersion = base.formatVersion();
      this.uuid = base.uuid();
      this.tableUuid = base.tableUuid();
      this.location = base.location();
      this.type = base.type();
      this.transformFunction = base.transformFunction();
      this.keyColumnIds = ImmutableList.copyOf(base.keyColumnIds());
      this.includedColumnIds = ImmutableList.copyOf(base.includedColumnIds());
      this.properties = ImmutableMap.copyOf(base.properties());
      this.currentSnapshotId = base.currentSnapshotId();
      this.snapshots = Lists.newArrayList(base.snapshots());
    }

    public Builder formatVersion(int version) {
      Preconditions.checkArgument(
          version > 0 && version <= SUPPORTED_INDEX_FORMAT_VERSION,
          "Unsupported format version: %s",
          version);
      this.formatVersion = version;
      return this;
    }

    public Builder uuid(String indexUuid) {
      this.uuid = Preconditions.checkNotNull(indexUuid, "uuid is required");
      return this;
    }

    public Builder tableUuid(String tblUuid) {
      this.tableUuid = Preconditions.checkNotNull(tblUuid, "table-uuid is required");
      return this;
    }

    public Builder location(String loc) {
      this.location = Preconditions.checkNotNull(loc, "location is required");
      return this;
    }

    public Builder type(String indexType) {
      this.type = Preconditions.checkNotNull(indexType, "type is required");
      return this;
    }

    public Builder transformFunction(String transform) {
      this.transformFunction =
          Preconditions.checkNotNull(transform, "transform-function is required");
      return this;
    }

    public Builder keyColumnIds(List<Integer> ids) {
      Preconditions.checkArgument(
          ids != null && !ids.isEmpty(), "key-column-ids must be non-empty");
      this.keyColumnIds = ImmutableList.copyOf(ids);
      return this;
    }

    public Builder includedColumnIds(List<Integer> ids) {
      this.includedColumnIds = ids != null ? ImmutableList.copyOf(ids) : ImmutableList.of();
      return this;
    }

    public Builder properties(Map<String, String> props) {
      this.properties = props != null ? ImmutableMap.copyOf(props) : ImmutableMap.of();
      return this;
    }

    public Builder addSnapshot(IndexSnapshot snapshot) {
      Preconditions.checkNotNull(snapshot, "snapshot is required");
      snapshots.add(snapshot);
      this.currentSnapshotId = snapshot.snapshotId();
      return this;
    }

    public Builder removeSnapshot(long snapshotId) {
      snapshots.removeIf(s -> s.snapshotId() == snapshotId);
      if (Long.valueOf(snapshotId).equals(currentSnapshotId)) {
        this.currentSnapshotId =
            snapshots.isEmpty() ? null : snapshots.get(snapshots.size() - 1).snapshotId();
      }
      return this;
    }

    public Builder currentSnapshotId(Long id) {
      this.currentSnapshotId = id;
      return this;
    }

    public Builder metadataFileLocation(String path) {
      this.metadataFileLocation = path;
      return this;
    }

    public GenericIndexMetadata build() {
      Preconditions.checkArgument(uuid != null, "uuid is required");
      Preconditions.checkArgument(tableUuid != null, "table-uuid is required");
      Preconditions.checkArgument(location != null, "location is required");
      Preconditions.checkArgument(type != null, "type is required");
      Preconditions.checkArgument(transformFunction != null, "transform-function is required");
      Preconditions.checkArgument(!keyColumnIds.isEmpty(), "key-column-ids must be non-empty");
      return new GenericIndexMetadata(
          formatVersion,
          uuid,
          tableUuid,
          location,
          type,
          transformFunction,
          keyColumnIds,
          includedColumnIds,
          properties,
          currentSnapshotId,
          snapshots,
          metadataFileLocation);
    }
  }
}
