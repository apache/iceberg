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

import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

/**
 * Represents the state of a source dependency (table, view, or materialized view) at the time of a
 * materialized view refresh.
 *
 * <p>Source states are stored in the refresh-state metadata of a materialized view's storage table
 * snapshot to track which versions of source data were used to compute the materialized results.
 */
public class SourceState {

  /** The type of source dependency. */
  public enum Type {
    TABLE,
    VIEW,
    MATERIALIZED_VIEW;

    public static Type fromString(String typeString) {
      Preconditions.checkArgument(typeString != null, "Invalid source type: null");
      switch (typeString.toLowerCase(Locale.ROOT)) {
        case "table":
          return TABLE;
        case "view":
          return VIEW;
        case "materialized-view":
          return MATERIALIZED_VIEW;
        default:
          throw new IllegalArgumentException("Unknown source type: " + typeString);
      }
    }

    public String toJsonValue() {
      return name().toLowerCase(Locale.ROOT).replace('_', '-');
    }
  }

  private final Type type;
  private final String name;
  private final Namespace namespace;
  private final String catalog;
  private final Map<String, Object> typeSpecificData;

  private SourceState(
      Type type,
      String name,
      Namespace namespace,
      String catalog,
      Map<String, Object> typeSpecificData) {
    this.type = type;
    this.name = name;
    this.namespace = namespace;
    this.catalog = catalog;
    this.typeSpecificData = ImmutableMap.copyOf(typeSpecificData);
  }

  /** Returns the type of this source state. */
  public Type type() {
    return type;
  }

  public String name() {
    return name;
  }

  public Namespace namespace() {
    return namespace;
  }

  /**
   * Returns the catalog name, or null if the source is in the same catalog as the materialized
   * view.
   */
  @Nullable
  public String catalog() {
    return catalog;
  }

  public String uuid() {
    Preconditions.checkState(
        type == Type.TABLE || type == Type.VIEW,
        "uuid() is only valid for TABLE and VIEW sources, not %s",
        type);
    return (String) typeSpecificData.get("uuid");
  }

  public long snapshotId() {
    Preconditions.checkState(type == Type.TABLE, "snapshotId() is only valid for TABLE sources");
    return (Long) typeSpecificData.get("snapshot-id");
  }

  public String ref() {
    Preconditions.checkState(type == Type.TABLE, "ref() is only valid for TABLE sources");
    Object ref = typeSpecificData.get("ref");
    return ref != null ? (String) ref : "main";
  }

  public int versionId() {
    Preconditions.checkState(type == Type.VIEW, "versionId() is only valid for VIEW sources");
    return (Integer) typeSpecificData.get("version-id");
  }

  public String viewUuid() {
    Preconditions.checkState(
        type == Type.MATERIALIZED_VIEW, "viewUuid() is only valid for MATERIALIZED_VIEW sources");
    return (String) typeSpecificData.get("view-uuid");
  }

  public int viewVersionId() {
    Preconditions.checkState(
        type == Type.MATERIALIZED_VIEW,
        "viewVersionId() is only valid for MATERIALIZED_VIEW sources");
    return (Integer) typeSpecificData.get("view-version-id");
  }

  public String storageTableUuid() {
    Preconditions.checkState(
        type == Type.MATERIALIZED_VIEW,
        "storageTableUuid() is only valid for MATERIALIZED_VIEW sources");
    return (String) typeSpecificData.get("storage-table-uuid");
  }

  public long storageTableSnapshotId() {
    Preconditions.checkState(
        type == Type.MATERIALIZED_VIEW,
        "storageTableSnapshotId() is only valid for MATERIALIZED_VIEW sources");
    return (Long) typeSpecificData.get("storage-table-snapshot-id");
  }

  /** Validates that all required fields are present for this source type. */
  public void validate() {
    Preconditions.checkNotNull(name, "Source name cannot be null");
    Preconditions.checkNotNull(namespace, "Source namespace cannot be null");

    switch (type) {
      case TABLE:
        Preconditions.checkArgument(
            typeSpecificData.containsKey("uuid"), "TABLE source must have uuid");
        Preconditions.checkArgument(
            typeSpecificData.containsKey("snapshot-id"), "TABLE source must have snapshot-id");
        break;
      case VIEW:
        Preconditions.checkArgument(
            typeSpecificData.containsKey("uuid"), "VIEW source must have uuid");
        Preconditions.checkArgument(
            typeSpecificData.containsKey("version-id"), "VIEW source must have version-id");
        break;
      case MATERIALIZED_VIEW:
        Preconditions.checkArgument(
            typeSpecificData.containsKey("view-uuid"),
            "MATERIALIZED_VIEW source must have view-uuid");
        Preconditions.checkArgument(
            typeSpecificData.containsKey("view-version-id"),
            "MATERIALIZED_VIEW source must have view-version-id");
        Preconditions.checkArgument(
            typeSpecificData.containsKey("storage-table-uuid"),
            "MATERIALIZED_VIEW source must have storage-table-uuid");
        Preconditions.checkArgument(
            typeSpecificData.containsKey("storage-table-snapshot-id"),
            "MATERIALIZED_VIEW source must have storage-table-snapshot-id");
        break;
    }
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    SourceState that = (SourceState) other;
    return type == that.type
        && name.equals(that.name)
        && namespace.equals(that.namespace)
        && Objects.equals(catalog, that.catalog)
        && typeSpecificData.equals(that.typeSpecificData);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, name, namespace, catalog, typeSpecificData);
  }

  @Override
  public String toString() {
    return String.format(
        "SourceState{type=%s, name=%s, namespace=%s, catalog=%s, data=%s}",
        type, name, namespace, catalog, typeSpecificData);
  }

  /** Creates a new builder for the specified source type. */
  public static Builder builder(Type type) {
    return new Builder(type);
  }

  /** Builder for creating SourceState instances. */
  public static class Builder {
    private final Type type;
    private String name;
    private Namespace namespace;
    private String catalog;
    private final ImmutableMap.Builder<String, Object> dataBuilder = ImmutableMap.builder();

    private Builder(Type type) {
      Preconditions.checkNotNull(type, "Source type cannot be null");
      this.type = type;
    }

    public Builder name(String sourceName) {
      this.name = sourceName;
      return this;
    }

    public Builder namespace(Namespace sourceNamespace) {
      this.namespace = sourceNamespace;
      return this;
    }

    public Builder catalog(String sourceCatalog) {
      this.catalog = sourceCatalog;
      return this;
    }

    /** Adds a type-specific data field. */
    public Builder put(String key, Object value) {
      if (value != null) {
        dataBuilder.put(key, value);
      }
      return this;
    }

    // Type-specific builder methods for TABLE sources

    public Builder uuid(String uuid) {
      return put("uuid", uuid);
    }

    public Builder snapshotId(long snapshotId) {
      return put("snapshot-id", snapshotId);
    }

    public Builder ref(String ref) {
      return put("ref", ref);
    }

    // Type-specific builder methods for VIEW sources

    public Builder versionId(int versionId) {
      return put("version-id", versionId);
    }

    // Type-specific builder methods for MATERIALIZED_VIEW sources

    public Builder viewUuid(String viewUuid) {
      return put("view-uuid", viewUuid);
    }

    public Builder viewVersionId(int viewVersionId) {
      return put("view-version-id", viewVersionId);
    }

    public Builder storageTableUuid(String storageTableUuid) {
      return put("storage-table-uuid", storageTableUuid);
    }

    public Builder storageTableSnapshotId(long storageTableSnapshotId) {
      return put("storage-table-snapshot-id", storageTableSnapshotId);
    }

    /** Builds and validates the SourceState. */
    public SourceState build() {
      SourceState state = new SourceState(type, name, namespace, catalog, dataBuilder.build());
      state.validate();
      return state;
    }
  }
}
