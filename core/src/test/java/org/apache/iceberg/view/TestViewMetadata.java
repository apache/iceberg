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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestViewMetadata {

  @Test
  public void nullAndMissingFields() {
    assertThatThrownBy(() -> ImmutableViewMetadata.builder().build())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot build ViewMetadata, some of required attributes are not set [formatVersion, location, currentVersionId]");

    assertThatThrownBy(() -> ImmutableViewMetadata.builder().formatVersion(1).build())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot build ViewMetadata, some of required attributes are not set [location, currentVersionId]");

    assertThatThrownBy(
            () -> ImmutableViewMetadata.builder().formatVersion(1).location("location").build())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot build ViewMetadata, some of required attributes are not set [currentVersionId]");

    assertThatThrownBy(
            () ->
                ImmutableViewMetadata.builder()
                    .formatVersion(1)
                    .location("location")
                    .currentVersionId(1)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid view versions: empty");
  }

  @Test
  public void unsupportedFormatVersion() {
    assertThatThrownBy(
            () ->
                ImmutableViewMetadata.builder()
                    .formatVersion(23)
                    .location("location")
                    .currentVersionId(1)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unsupported format version: 23");
  }

  @Test
  public void emptyViewVersion() {
    assertThatThrownBy(
            () ->
                ImmutableViewMetadata.builder()
                    .formatVersion(1)
                    .location("location")
                    .currentVersionId(1)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid view versions: empty");
  }

  @Test
  public void emptySchemas() {
    assertThatThrownBy(
            () ->
                ImmutableViewMetadata.builder()
                    .formatVersion(1)
                    .location("location")
                    .currentVersionId(1)
                    .addVersions(
                        ImmutableViewVersion.builder()
                            .schemaId(1)
                            .versionId(1)
                            .timestampMillis(23L)
                            .putSummary("operation", "op")
                            .defaultNamespace(Namespace.of("ns"))
                            .build())
                    .addHistory(
                        ImmutableViewHistoryEntry.builder()
                            .timestampMillis(23L)
                            .versionId(1)
                            .build())
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid schemas: empty");
  }

  @Test
  public void invalidCurrentVersionId() {
    assertThatThrownBy(
            () ->
                ImmutableViewMetadata.builder()
                    .formatVersion(1)
                    .location("location")
                    .currentVersionId(23)
                    .addVersions(
                        ImmutableViewVersion.builder()
                            .schemaId(1)
                            .versionId(1)
                            .timestampMillis(23L)
                            .putSummary("operation", "op")
                            .defaultNamespace(Namespace.of("ns"))
                            .build())
                    .addHistory(
                        ImmutableViewHistoryEntry.builder()
                            .timestampMillis(23L)
                            .versionId(1)
                            .build())
                    .addSchemas(
                        new Schema(1, Types.NestedField.required(1, "x", Types.LongType.get())))
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find current version 23 in view versions: [1]");
  }

  @Test
  public void invalidCurrentSchemaId() {
    assertThatThrownBy(
            () ->
                ImmutableViewMetadata.builder()
                    .formatVersion(1)
                    .location("location")
                    .currentVersionId(1)
                    .addVersions(
                        ImmutableViewVersion.builder()
                            .schemaId(23)
                            .versionId(1)
                            .defaultNamespace(Namespace.of("ns"))
                            .timestampMillis(23L)
                            .putSummary("operation", "op")
                            .build())
                    .addHistory(
                        ImmutableViewHistoryEntry.builder()
                            .timestampMillis(23L)
                            .versionId(1)
                            .build())
                    .addSchemas(
                        new Schema(1, Types.NestedField.required(1, "x", Types.LongType.get())))
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find current schema with id 23 in schemas: [1]");
  }

  @Test
  public void invalidVersionHistorySizeToKeep() {
    ImmutableViewMetadata viewMetadata =
        ImmutableViewMetadata.builder()
            // setting history to < 1 shouldn't do anything and only issue a WARN
            .properties(ImmutableMap.of(ViewProperties.VERSION_HISTORY_SIZE, "0"))
            .formatVersion(1)
            .location("location")
            .currentVersionId(3)
            .addVersions(
                ImmutableViewVersion.builder()
                    .schemaId(1)
                    .versionId(1)
                    .timestampMillis(23L)
                    .putSummary("operation", "a")
                    .defaultNamespace(Namespace.of("ns"))
                    .build())
            .addVersions(
                ImmutableViewVersion.builder()
                    .schemaId(1)
                    .versionId(2)
                    .timestampMillis(24L)
                    .putSummary("operation", "b")
                    .defaultNamespace(Namespace.of("ns"))
                    .build())
            .addVersions(
                ImmutableViewVersion.builder()
                    .schemaId(1)
                    .versionId(3)
                    .timestampMillis(25L)
                    .putSummary("operation", "c")
                    .defaultNamespace(Namespace.of("ns"))
                    .build())
            .addHistory(
                ImmutableViewHistoryEntry.builder().versionId(1).timestampMillis(23L).build())
            .addHistory(
                ImmutableViewHistoryEntry.builder().versionId(2).timestampMillis(24L).build())
            .addSchemas(new Schema(1, Types.NestedField.required(1, "x", Types.LongType.get())))
            .build();

    assertThat(viewMetadata.versions()).hasSize(3);
    assertThat(viewMetadata.history()).hasSize(2);
  }

  @Test
  public void emptyHistory() {
    assertThatThrownBy(
            () ->
                ImmutableViewMetadata.builder()
                    .formatVersion(1)
                    .location("location")
                    .currentVersionId(2)
                    .addVersions(
                        ImmutableViewVersion.builder()
                            .schemaId(1)
                            .versionId(1)
                            .timestampMillis(23L)
                            .putSummary("operation", "op")
                            .defaultNamespace(Namespace.of("ns"))
                            .build())
                    .addVersions(
                        ImmutableViewVersion.builder()
                            .schemaId(1)
                            .versionId(2)
                            .timestampMillis(24L)
                            .putSummary("operation", "op")
                            .defaultNamespace(Namespace.of("ns"))
                            .build())
                    .addSchemas(
                        new Schema(1, Types.NestedField.required(1, "x", Types.LongType.get())))
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid view history: empty");
  }

  @Test
  public void viewHistoryNormalization() {
    ImmutableViewMetadata viewMetadata =
        ImmutableViewMetadata.builder()
            .properties(ImmutableMap.of(ViewProperties.VERSION_HISTORY_SIZE, "1"))
            .formatVersion(1)
            .location("location")
            .currentVersionId(3)
            .addVersions(
                ImmutableViewVersion.builder()
                    .schemaId(1)
                    .versionId(1)
                    .timestampMillis(23L)
                    .putSummary("operation", "a")
                    .defaultNamespace(Namespace.of("ns"))
                    .build())
            .addVersions(
                ImmutableViewVersion.builder()
                    .schemaId(1)
                    .versionId(2)
                    .timestampMillis(24L)
                    .putSummary("operation", "b")
                    .defaultNamespace(Namespace.of("ns"))
                    .build())
            .addVersions(
                ImmutableViewVersion.builder()
                    .schemaId(1)
                    .versionId(3)
                    .timestampMillis(25L)
                    .putSummary("operation", "c")
                    .defaultNamespace(Namespace.of("ns"))
                    .build())
            .addHistory(
                ImmutableViewHistoryEntry.builder().versionId(1).timestampMillis(23L).build())
            .addHistory(
                ImmutableViewHistoryEntry.builder().versionId(2).timestampMillis(24L).build())
            .addSchemas(new Schema(1, Types.NestedField.required(1, "x", Types.LongType.get())))
            .build();

    assertThat(viewMetadata.versions()).hasSize(1);
    assertThat(viewMetadata.history()).hasSize(1);
  }
}
