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
import org.immutables.value.Value;

@Value.Immutable
public abstract class ViewMetadata implements Serializable {
  static final int DEFAULT_VIEW_FORMAT_VERSION = 1;
  static final int SUPPORTED_VIEW_FORMAT_VERSION = 1;

  public static ImmutableViewMetadata.Builder buildFrom(ViewMetadata metadata) {
    return ImmutableViewMetadata.builder()
        .formatVersion(DEFAULT_VIEW_FORMAT_VERSION)
        .location(metadata.location())
        .properties(metadata.properties())
        .currentVersionId(metadata.currentVersionId())
        .versions(metadata.versions())
        .schemas(metadata.schemas())
        .currentSchemaId(metadata.currentSchemaId())
        .history(metadata.history());
  }

  @Value.Check
  void check() {
    Preconditions.checkState(versions().size() > 0, "Expecting non-empty version log");
  }

  public abstract int formatVersion();

  public abstract String location();

  public abstract Map<String, String> properties();

  public abstract int currentVersionId();

  public abstract List<ViewVersion> versions();

  public abstract List<ViewHistoryEntry> history();

  public abstract List<Schema> schemas();

  public abstract Integer currentSchemaId();

  public ViewVersion version(int versionId) {
    return versionsById().get(versionId);
  }

  @Value.Derived
  public ViewVersion currentVersion() {
    return versionsById().get(currentVersionId());
  }

  @Value.Derived
  public Map<Integer, ViewVersion> versionsById() {
    return indexVersions(versions());
  }

  @Value.Derived
  public Map<Integer, Schema> schemasById() {
    return indexSchemas(schemas());
  }

  @Value.Derived
  public Schema schema() {
    return schemasById().get(currentSchemaId());
  }

  private static Map<Integer, ViewVersion> indexVersions(List<ViewVersion> versions) {
    ImmutableMap.Builder<Integer, ViewVersion> builder = ImmutableMap.builder();
    for (ViewVersion version : versions) {
      builder.put(version.versionId(), version);
    }

    return builder.build();
  }

  private static Map<Integer, Schema> indexSchemas(List<Schema> schemas) {
    if (schemas == null) {
      return ImmutableMap.of();
    }

    ImmutableMap.Builder<Integer, Schema> builder = ImmutableMap.builder();
    for (Schema schema : schemas) {
      builder.put(schema.schemaId(), schema);
    }

    return builder.build();
  }
}
