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

import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestViewMetadataSchemaRetention {

  @Test
  void retainsOnlySchemasReferencedByRetainedVersions() {
    Schema schemaOne = new Schema(0, Types.NestedField.required(1, "x", Types.LongType.get()));
    Schema schemaTwo = new Schema(1, Types.NestedField.required(2, "y", Types.LongType.get()));
    Schema schemaThree = new Schema(2, Types.NestedField.required(3, "z", Types.LongType.get()));

    ViewVersion versionOne = newViewVersion(1, schemaOne.schemaId(), "select x from ns.tbl");
    ViewVersion versionTwo = newViewVersion(2, schemaTwo.schemaId(), "select y from ns.tbl");
    ViewVersion versionThree = newViewVersion(3, schemaThree.schemaId(), "select z from ns.tbl");

    ViewMetadata metadata =
        ViewMetadata.builder()
            .setProperties(ImmutableMap.of(ViewProperties.VERSION_HISTORY_SIZE, "2"))
            .setLocation("location")
            .addSchema(schemaOne)
            .addSchema(schemaTwo)
            .addSchema(schemaThree)
            .addVersion(versionOne)
            .addVersion(versionTwo)
            .addVersion(versionThree)
            .setCurrentVersionId(versionThree.versionId())
            .build();

    ViewMetadata normalized = ViewMetadata.buildFrom(metadata).build();

    assertThat(normalized.versions()).containsExactlyInAnyOrder(versionTwo, versionThree);
    assertThat(normalized.schemas()).containsExactly(schemaTwo, schemaThree);
    assertAllVersionsReferenceKnownSchemas(normalized);
  }

  @Test
  void retainsPreviouslyUnreferencedSchemas() {
    Schema currentSchema = new Schema(0, Types.NestedField.required(1, "x", Types.LongType.get()));
    Schema standaloneSchema =
        new Schema(1, Types.NestedField.required(2, "y", Types.LongType.get()));
    ViewVersion currentVersion =
        newViewVersion(1, currentSchema.schemaId(), "select x from ns.tbl");

    ViewMetadata metadataWithStandaloneSchema =
        ImmutableViewMetadata.of(
            "test-uuid",
            ViewMetadata.DEFAULT_VIEW_FORMAT_VERSION,
            "location",
            ImmutableList.of(currentSchema, standaloneSchema),
            currentVersion.versionId(),
            ImmutableList.of(currentVersion),
            ImmutableList.of(),
            ImmutableMap.of(),
            ImmutableList.of(),
            null);

    ViewMetadata normalized = ViewMetadata.buildFrom(metadataWithStandaloneSchema).build();

    assertThat(normalized.versions()).containsExactly(currentVersion);
    assertThat(normalized.schemas()).containsExactly(currentSchema, standaloneSchema);
    assertAllVersionsReferenceKnownSchemas(normalized);
  }

  @Test
  void retainsStandaloneSchemaAddedInCurrentBuild() {
    Schema currentSchema = new Schema(0, Types.NestedField.required(1, "x", Types.LongType.get()));
    Schema existingStandaloneSchema =
        new Schema(9, Types.NestedField.required(2, "y", Types.LongType.get()));
    Schema newStandaloneSchema =
        new Schema(10, Types.NestedField.required(3, "z", Types.LongType.get()));
    ViewVersion currentVersion =
        newViewVersion(1, currentSchema.schemaId(), "select x from ns.tbl");

    ViewMetadata metadataWithStandaloneSchema =
        ImmutableViewMetadata.of(
            "test-uuid",
            ViewMetadata.DEFAULT_VIEW_FORMAT_VERSION,
            "location",
            ImmutableList.of(currentSchema, existingStandaloneSchema),
            currentVersion.versionId(),
            ImmutableList.of(currentVersion),
            ImmutableList.of(),
            ImmutableMap.of(),
            ImmutableList.of(),
            null);

    ViewMetadata updated =
        ViewMetadata.buildFrom(metadataWithStandaloneSchema).addSchema(newStandaloneSchema).build();

    assertThat(updated.versions()).containsExactly(currentVersion);
    assertThat(updated.schemas())
        .containsExactly(currentSchema, existingStandaloneSchema, newStandaloneSchema);
    assertAllVersionsReferenceKnownSchemas(updated);
  }

  @Test
  void removesExpiredVersionSchemasWhenApplyingRestStyleUpdates() {
    Schema currentSchema = new Schema(0, Types.NestedField.required(1, "x", Types.LongType.get()));
    Schema standaloneSchema =
        new Schema(9, Types.NestedField.required(2, "y", Types.LongType.get()));
    Schema replacementSchema =
        new Schema(10, Types.NestedField.required(3, "z", Types.LongType.get()));
    ViewVersion currentVersion =
        newViewVersion(1, currentSchema.schemaId(), "select x from ns.tbl");

    ViewMetadata baseMetadata =
        ImmutableViewMetadata.of(
            "test-uuid",
            ViewMetadata.DEFAULT_VIEW_FORMAT_VERSION,
            "location",
            ImmutableList.of(currentSchema, standaloneSchema),
            currentVersion.versionId(),
            ImmutableList.of(currentVersion),
            ImmutableList.of(),
            ImmutableMap.of(ViewProperties.VERSION_HISTORY_SIZE, "1"),
            ImmutableList.of(),
            null);

    ViewVersion replacementVersion =
        newViewVersion(2, replacementSchema.schemaId(), "select z from ns.tbl");
    ViewMetadata clientUpdate =
        ViewMetadata.buildFrom(baseMetadata)
            .setCurrentVersion(replacementVersion, replacementSchema)
            .build();

    ViewMetadata.Builder serverBuilder = ViewMetadata.buildFrom(baseMetadata);
    clientUpdate.changes().forEach(update -> update.applyTo(serverBuilder));
    ViewMetadata serverMetadata = serverBuilder.build();

    assertThat(serverMetadata.versions()).containsExactly(clientUpdate.currentVersion());
    assertThat(serverMetadata.schemas()).containsExactly(standaloneSchema, clientUpdate.schema());
    assertAllVersionsReferenceKnownSchemas(serverMetadata);
  }

  @Test
  void retainsSchemaReferencedByRetainedVersion() {
    Schema sharedSchema = new Schema(0, Types.NestedField.required(1, "x", Types.LongType.get()));
    Schema currentSchema = new Schema(1, Types.NestedField.required(2, "y", Types.LongType.get()));

    ViewVersion versionOne = newViewVersion(1, sharedSchema.schemaId(), "select x from ns.tbl");
    ViewVersion versionTwo = newViewVersion(2, sharedSchema.schemaId(), "select x + 1 from ns.tbl");
    ViewVersion versionThree = newViewVersion(3, currentSchema.schemaId(), "select y from ns.tbl");

    ViewMetadata metadata =
        ViewMetadata.builder()
            .setProperties(ImmutableMap.of(ViewProperties.VERSION_HISTORY_SIZE, "2"))
            .setLocation("location")
            .addSchema(sharedSchema)
            .addSchema(currentSchema)
            .addVersion(versionOne)
            .addVersion(versionTwo)
            .addVersion(versionThree)
            .setCurrentVersionId(versionThree.versionId())
            .build();

    ViewMetadata normalized = ViewMetadata.buildFrom(metadata).build();

    assertThat(normalized.versions()).containsExactlyInAnyOrder(versionTwo, versionThree);
    assertThat(normalized.schemas()).containsExactly(sharedSchema, currentSchema);
    assertAllVersionsReferenceKnownSchemas(normalized);
  }

  @Test
  void retainsSchemaOfCurrentVersionWhenOlderVersionIsCurrent() {
    Schema currentSchema = new Schema(0, Types.NestedField.required(1, "x", Types.LongType.get()));
    Schema schemaTwo = new Schema(1, Types.NestedField.required(2, "y", Types.LongType.get()));
    Schema schemaThree = new Schema(2, Types.NestedField.required(3, "z", Types.LongType.get()));

    ViewVersion currentVersion =
        newViewVersion(1, currentSchema.schemaId(), "select x from ns.tbl");
    ViewVersion versionTwo = newViewVersion(2, schemaTwo.schemaId(), "select y from ns.tbl");
    ViewVersion versionThree = newViewVersion(3, schemaThree.schemaId(), "select z from ns.tbl");

    ViewMetadata metadata =
        ViewMetadata.builder()
            .setProperties(ImmutableMap.of(ViewProperties.VERSION_HISTORY_SIZE, "1"))
            .setLocation("location")
            .addSchema(currentSchema)
            .addSchema(schemaTwo)
            .addSchema(schemaThree)
            .addVersion(currentVersion)
            .addVersion(versionTwo)
            .addVersion(versionThree)
            .setCurrentVersionId(currentVersion.versionId())
            .build();

    ViewMetadata normalized = ViewMetadata.buildFrom(metadata).build();

    assertThat(normalized.versions()).containsExactly(currentVersion);
    assertThat(normalized.schemas()).containsExactly(currentSchema);
    assertThat(normalized.schema()).isEqualTo(currentSchema);
    assertAllVersionsReferenceKnownSchemas(normalized);
  }

  @Test
  void retainsSchemasForVersionsAddedInCurrentBuild() {
    Schema schemaOne = new Schema(0, Types.NestedField.required(1, "x", Types.LongType.get()));
    Schema schemaTwo = new Schema(1, Types.NestedField.required(2, "y", Types.LongType.get()));
    Schema schemaThree = new Schema(2, Types.NestedField.required(3, "z", Types.LongType.get()));

    ViewVersion versionOne = newViewVersion(1, schemaOne.schemaId(), "select x from ns.tbl");
    ViewVersion versionTwo = newViewVersion(2, schemaTwo.schemaId(), "select y from ns.tbl");
    ViewVersion versionThree = newViewVersion(3, schemaThree.schemaId(), "select z from ns.tbl");

    ViewMetadata metadata =
        ViewMetadata.builder()
            .setProperties(ImmutableMap.of(ViewProperties.VERSION_HISTORY_SIZE, "1"))
            .setLocation("location")
            .addSchema(schemaOne)
            .addVersion(versionOne)
            .setCurrentVersionId(versionOne.versionId())
            .build();

    ViewMetadata updated =
        ViewMetadata.buildFrom(metadata)
            .addSchema(schemaTwo)
            .addVersion(versionTwo)
            .addSchema(schemaThree)
            .addVersion(versionThree)
            .setCurrentVersionId(versionThree.versionId())
            .build();

    assertThat(updated.versions()).containsExactlyInAnyOrder(versionTwo, versionThree);
    assertThat(updated.schemas()).containsExactly(schemaTwo, schemaThree);
    assertAllVersionsReferenceKnownSchemas(updated);
  }

  private void assertAllVersionsReferenceKnownSchemas(ViewMetadata metadata) {
    List<Integer> schemaIds =
        metadata.schemas().stream().map(Schema::schemaId).collect(Collectors.toList());
    assertThat(metadata.versions()).allMatch(version -> schemaIds.contains(version.schemaId()));
  }

  private ViewVersion newViewVersion(int id, int schemaId, String sql) {
    return ImmutableViewVersion.builder()
        .versionId(id)
        .schemaId(schemaId)
        .timestampMillis(id * 1000L)
        .defaultCatalog("prod")
        .defaultNamespace(Namespace.of("default"))
        .addRepresentations(
            ImmutableSQLViewRepresentation.builder().dialect("spark").sql(sql).build())
        .build();
  }
}
