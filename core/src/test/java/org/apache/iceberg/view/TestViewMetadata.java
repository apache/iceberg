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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

public class TestViewMetadata {

  private ViewVersion newViewVersion(int id, String sql) {
    return newViewVersion(id, 0, sql);
  }

  private ViewVersion newViewVersion(int id, int schemaId, String sql) {
    return ImmutableViewVersion.builder()
        .versionId(id)
        .timestampMillis(System.currentTimeMillis())
        .defaultCatalog("prod")
        .defaultNamespace(Namespace.of("default"))
        .putSummary("user", "some-user")
        .addRepresentations(
            ImmutableSQLViewRepresentation.builder().dialect("spark").sql(sql).build())
        .schemaId(schemaId)
        .build();
  }

  @Test
  public void testExpiration() {
    // purposely use versions and timestamps that do not match to check that version ID is used
    ViewVersion v1 = newViewVersion(1, "select 1 as count");
    ViewVersion v3 = newViewVersion(3, "select count from t1");
    ViewVersion v2 = newViewVersion(2, "select count(1) as count from t2");
    Map<Integer, ViewVersion> versionsById = ImmutableMap.of(1, v1, 2, v2, 3, v3);

    List<ViewVersion> retainedVersions = ViewMetadata.Builder.expireVersions(versionsById, 2);
    assertThat(retainedVersions).hasSameElementsAs(ImmutableList.of(v2, v3));
  }

  @Test
  public void testUpdateHistory() {
    ViewVersion v1 = newViewVersion(1, "select 1 as count");
    ViewVersion v2 = newViewVersion(2, "select count(1) as count from t2");
    ViewVersion v3 = newViewVersion(3, "select count from t1");

    Set<Integer> versionsById = ImmutableSet.of(2, 3);

    List<ViewHistoryEntry> history =
        ImmutableList.of(
            ImmutableViewHistoryEntry.builder()
                .versionId(v1.versionId())
                .timestampMillis(v1.timestampMillis())
                .build(),
            ImmutableViewHistoryEntry.builder()
                .versionId(v2.versionId())
                .timestampMillis(v2.timestampMillis())
                .build(),
            ImmutableViewHistoryEntry.builder()
                .versionId(v3.versionId())
                .timestampMillis(v3.timestampMillis())
                .build());

    List<ViewHistoryEntry> retainedHistory =
        ViewMetadata.Builder.updateHistory(history, versionsById);
    assertThat(retainedHistory).hasSameElementsAs(history.subList(1, 3));
  }

  @Test
  public void nullAndMissingFields() {
    assertThatThrownBy(() -> ViewMetadata.builder().build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid location: null");

    assertThatThrownBy(() -> ViewMetadata.builder().setLocation("location").build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid view: no versions were added");

    assertThatThrownBy(
            () -> ViewMetadata.builder().setLocation("location").setCurrentVersionId(1).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot set current version to unknown version: 1");

    assertThatThrownBy(() -> ViewMetadata.builder().assignUUID(null).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot set uuid to null");
  }

  @Test
  public void unsupportedFormatVersion() {
    assertThatThrownBy(
            () ->
                ViewMetadata.builder()
                    .upgradeFormatVersion(23)
                    .setLocation("location")
                    .addSchema(new Schema(Types.NestedField.required(1, "x", Types.LongType.get())))
                    .addVersion(
                        ImmutableViewVersion.builder()
                            .schemaId(0)
                            .versionId(1)
                            .timestampMillis(23L)
                            .defaultNamespace(Namespace.of("ns"))
                            .build())
                    .setCurrentVersionId(1)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unsupported format version: 23");

    assertThatThrownBy(
            () ->
                ViewMetadata.builder()
                    .upgradeFormatVersion(0)
                    .setLocation("location")
                    .setCurrentVersionId(1)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot downgrade v1 view to v0");
  }

  @Test
  public void emptyViewVersion() {
    assertThatThrownBy(
            () -> ViewMetadata.builder().setLocation("location").setCurrentVersionId(1).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot set current version to unknown version: 1");
  }

  @Test
  public void emptySchemas() {
    assertThatThrownBy(
            () ->
                ViewMetadata.builder()
                    .setLocation("location")
                    .addVersion(
                        ImmutableViewVersion.builder()
                            .schemaId(1)
                            .versionId(1)
                            .timestampMillis(23L)
                            .defaultNamespace(Namespace.of("ns"))
                            .build())
                    .setCurrentVersionId(1)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot add version with unknown schema: 1");
  }

  @Test
  public void invalidCurrentVersionId() {
    assertThatThrownBy(
            () ->
                ViewMetadata.builder()
                    .setLocation("location")
                    .addSchema(new Schema(Types.NestedField.required(1, "x", Types.LongType.get())))
                    .addVersion(
                        ImmutableViewVersion.builder()
                            .schemaId(0)
                            .versionId(1)
                            .timestampMillis(23L)
                            .defaultNamespace(Namespace.of("ns"))
                            .build())
                    .setCurrentVersionId(23)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot set current version to unknown version: 23");
  }

  @Test
  public void invalidCurrentSchemaId() {
    assertThatThrownBy(
            () ->
                ViewMetadata.builder()
                    .setLocation("location")
                    .addSchema(
                        new Schema(1, Types.NestedField.required(1, "x", Types.LongType.get())))
                    .addVersion(
                        ImmutableViewVersion.builder()
                            .schemaId(23)
                            .versionId(1)
                            .defaultNamespace(Namespace.of("ns"))
                            .timestampMillis(23L)
                            .build())
                    .setCurrentVersionId(1)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot add version with unknown schema: 23");
  }

  @Test
  public void invalidVersionHistorySizeToKeep() {
    assertThatThrownBy(
            () ->
                ViewMetadata.builder()
                    .setProperties(ImmutableMap.of(ViewProperties.VERSION_HISTORY_SIZE, "0"))
                    .setLocation("location")
                    .addSchema(
                        new Schema(1, Types.NestedField.required(1, "x", Types.LongType.get())))
                    .addVersion(newViewVersion(1, "select * from ns.tbl"))
                    .addVersion(newViewVersion(2, "select count(*) from ns.tbl"))
                    .addVersion(newViewVersion(3, "select count(*) as count from ns.tbl"))
                    .setCurrentVersionId(3)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("version.history.num-entries must be positive but was 0");
  }

  @Test
  public void viewHistoryNormalization() {
    Map<String, String> properties = ImmutableMap.of(ViewProperties.VERSION_HISTORY_SIZE, "2");
    ViewVersion viewVersionOne = newViewVersion(1, "select * from ns.tbl");
    ViewVersion viewVersionTwo = newViewVersion(2, "select count(*) from ns.tbl");
    ViewVersion viewVersionThree = newViewVersion(3, "select count(*) as count from ns.tbl");

    ViewMetadata originalViewMetadata =
        ViewMetadata.builder()
            .setProperties(properties)
            .setLocation("location")
            .addSchema(new Schema(Types.NestedField.required(1, "x", Types.LongType.get())))
            .addVersion(viewVersionOne)
            .addVersion(viewVersionTwo)
            .addVersion(viewVersionThree)
            .setCurrentVersionId(3)
            .build();

    // the first build will not expire versions that were added in the builder
    assertThat(originalViewMetadata.versions()).hasSize(3);
    assertThat(originalViewMetadata.history()).hasSize(3);

    // rebuild the metadata to expire older versions
    ViewMetadata viewMetadata = ViewMetadata.buildFrom(originalViewMetadata).build();
    assertThat(viewMetadata.versions()).hasSize(2);
    assertThat(viewMetadata.history()).hasSize(2);

    // make sure that metadata changes reflect the current state after the history was adjusted,
    // meaning that the first view version shouldn't be included
    List<MetadataUpdate> changes = originalViewMetadata.changes();
    assertThat(changes).hasSize(7);
    assertThat(changes)
        .element(0)
        .isInstanceOf(MetadataUpdate.SetProperties.class)
        .asInstanceOf(InstanceOfAssertFactories.type(MetadataUpdate.SetProperties.class))
        .extracting(MetadataUpdate.SetProperties::updated)
        .isEqualTo(properties);

    assertThat(changes)
        .element(1)
        .isInstanceOf(MetadataUpdate.SetLocation.class)
        .asInstanceOf(InstanceOfAssertFactories.type(MetadataUpdate.SetLocation.class))
        .extracting(MetadataUpdate.SetLocation::location)
        .isEqualTo("location");

    assertThat(changes)
        .element(2)
        .isInstanceOf(MetadataUpdate.AddSchema.class)
        .asInstanceOf(InstanceOfAssertFactories.type(MetadataUpdate.AddSchema.class))
        .extracting(MetadataUpdate.AddSchema::schema)
        .extracting(Schema::schemaId)
        .isEqualTo(0);

    assertThat(changes)
        .element(3)
        .isInstanceOf(MetadataUpdate.AddViewVersion.class)
        .asInstanceOf(InstanceOfAssertFactories.type(MetadataUpdate.AddViewVersion.class))
        .extracting(MetadataUpdate.AddViewVersion::viewVersion)
        .isEqualTo(viewVersionOne);

    assertThat(changes)
        .element(4)
        .isInstanceOf(MetadataUpdate.AddViewVersion.class)
        .asInstanceOf(InstanceOfAssertFactories.type(MetadataUpdate.AddViewVersion.class))
        .extracting(MetadataUpdate.AddViewVersion::viewVersion)
        .isEqualTo(viewVersionTwo);

    assertThat(changes)
        .element(5)
        .isInstanceOf(MetadataUpdate.AddViewVersion.class)
        .asInstanceOf(InstanceOfAssertFactories.type(MetadataUpdate.AddViewVersion.class))
        .extracting(MetadataUpdate.AddViewVersion::viewVersion)
        .isEqualTo(viewVersionThree);

    assertThat(changes)
        .element(6)
        .isInstanceOf(MetadataUpdate.SetCurrentViewVersion.class)
        .asInstanceOf(InstanceOfAssertFactories.type(MetadataUpdate.SetCurrentViewVersion.class))
        .extracting(MetadataUpdate.SetCurrentViewVersion::versionId)
        .isEqualTo(-1);
  }

  @Test
  public void viewMetadataAndMetadataChanges() {
    Map<String, String> properties = ImmutableMap.of("key1", "prop1", "key2", "prop2");
    Schema schemaOne = new Schema(0, Types.NestedField.required(1, "x", Types.LongType.get()));
    Schema schemaTwo = new Schema(1, Types.NestedField.required(1, "y", Types.LongType.get()));
    ViewVersion viewVersionOne = newViewVersion(1, 0, "select * from ns.tbl");
    ViewVersion viewVersionTwo = newViewVersion(2, 0, "select count(*) from ns.tbl");
    ViewVersion viewVersionThree = newViewVersion(3, 1, "select count(*) as count from ns.tbl");

    String uuid = "fa6506c3-7681-40c8-86dc-e36561f83385";
    ViewMetadata viewMetadata =
        ViewMetadata.builder()
            .assignUUID(uuid)
            .setLocation("custom-location")
            .setProperties(properties)
            .addSchema(schemaOne)
            .addSchema(schemaTwo)
            .addVersion(viewVersionOne)
            .addVersion(viewVersionTwo)
            .addVersion(viewVersionThree)
            .setCurrentVersionId(3)
            .build();

    assertThat(viewMetadata.versions())
        .hasSize(3)
        .containsExactly(viewVersionOne, viewVersionTwo, viewVersionThree);
    assertThat(viewMetadata.history()).hasSize(3);
    assertThat(viewMetadata.currentVersionId()).isEqualTo(3);
    assertThat(viewMetadata.currentVersion()).isEqualTo(viewVersionThree);
    assertThat(viewMetadata.formatVersion()).isEqualTo(ViewMetadata.DEFAULT_VIEW_FORMAT_VERSION);
    assertThat(viewMetadata.schemas().stream().map(Schema::asStruct))
        .hasSize(2)
        .containsExactly(schemaOne.asStruct(), schemaTwo.asStruct());
    assertThat(viewMetadata.schema().asStruct()).isEqualTo(schemaTwo.asStruct());
    assertThat(viewMetadata.currentSchemaId()).isEqualTo(schemaTwo.schemaId());
    assertThat(viewMetadata.location()).isEqualTo("custom-location");
    assertThat(viewMetadata.properties()).isEqualTo(properties);

    List<MetadataUpdate> changes = viewMetadata.changes();
    assertThat(changes).hasSize(9);
    assertThat(changes)
        .element(0)
        .isInstanceOf(MetadataUpdate.AssignUUID.class)
        .asInstanceOf(InstanceOfAssertFactories.type(MetadataUpdate.AssignUUID.class))
        .extracting(MetadataUpdate.AssignUUID::uuid)
        .isEqualTo(uuid);

    assertThat(changes)
        .element(1)
        .isInstanceOf(MetadataUpdate.SetLocation.class)
        .asInstanceOf(InstanceOfAssertFactories.type(MetadataUpdate.SetLocation.class))
        .extracting(MetadataUpdate.SetLocation::location)
        .isEqualTo("custom-location");

    assertThat(changes)
        .element(2)
        .isInstanceOf(MetadataUpdate.SetProperties.class)
        .asInstanceOf(InstanceOfAssertFactories.type(MetadataUpdate.SetProperties.class))
        .extracting(MetadataUpdate.SetProperties::updated)
        .isEqualTo(properties);

    assertThat(changes)
        .element(3)
        .isInstanceOf(MetadataUpdate.AddSchema.class)
        .asInstanceOf(InstanceOfAssertFactories.type(MetadataUpdate.AddSchema.class))
        .extracting(MetadataUpdate.AddSchema::schema)
        .extracting(Schema::schemaId)
        .isEqualTo(0);

    assertThat(changes)
        .element(4)
        .isInstanceOf(MetadataUpdate.AddSchema.class)
        .asInstanceOf(InstanceOfAssertFactories.type(MetadataUpdate.AddSchema.class))
        .extracting(MetadataUpdate.AddSchema::schema)
        .extracting(Schema::schemaId)
        .isEqualTo(1);

    assertThat(changes)
        .element(5)
        .isInstanceOf(MetadataUpdate.AddViewVersion.class)
        .asInstanceOf(InstanceOfAssertFactories.type(MetadataUpdate.AddViewVersion.class))
        .extracting(MetadataUpdate.AddViewVersion::viewVersion)
        .isEqualTo(viewVersionOne);

    assertThat(changes)
        .element(6)
        .isInstanceOf(MetadataUpdate.AddViewVersion.class)
        .asInstanceOf(InstanceOfAssertFactories.type(MetadataUpdate.AddViewVersion.class))
        .extracting(MetadataUpdate.AddViewVersion::viewVersion)
        .isEqualTo(viewVersionTwo);

    assertThat(changes)
        .element(7)
        .isInstanceOf(MetadataUpdate.AddViewVersion.class)
        .asInstanceOf(InstanceOfAssertFactories.type(MetadataUpdate.AddViewVersion.class))
        .extracting(MetadataUpdate.AddViewVersion::viewVersion)
        .isEqualTo(viewVersionThree);

    assertThat(changes)
        .element(8)
        .isInstanceOf(MetadataUpdate.SetCurrentViewVersion.class)
        .asInstanceOf(InstanceOfAssertFactories.type(MetadataUpdate.SetCurrentViewVersion.class))
        .extracting(MetadataUpdate.SetCurrentViewVersion::versionId)
        .isEqualTo(-1);
  }

  @Test
  public void uuidAssignment() {
    String uuid = "fa6506c3-7681-40c8-86dc-e36561f83385";
    ViewMetadata viewMetadata =
        ViewMetadata.builder()
            .assignUUID(uuid)
            .setLocation("custom-location")
            .addSchema(new Schema(Types.NestedField.required(1, "x", Types.LongType.get())))
            .addVersion(
                ImmutableViewVersion.builder()
                    .schemaId(0)
                    .versionId(1)
                    .timestampMillis(23L)
                    .defaultNamespace(Namespace.of("ns"))
                    .build())
            .setCurrentVersionId(1)
            .build();

    assertThat(viewMetadata.uuid()).isEqualTo(uuid);

    // uuid should be carried over
    ViewMetadata updated = ViewMetadata.buildFrom(viewMetadata).build();
    assertThat(updated.uuid()).isEqualTo(uuid);
    assertThat(updated.changes()).isEmpty();

    // assigning the same uuid shouldn't fail and shouldn't cause any changes
    updated = ViewMetadata.buildFrom(viewMetadata).assignUUID(uuid).build();
    assertThat(updated.uuid()).isEqualTo(uuid);
    assertThat(updated.changes()).isEmpty();

    // can't reassign view uuid
    assertThatThrownBy(
            () ->
                ViewMetadata.buildFrom(viewMetadata)
                    .assignUUID(UUID.randomUUID().toString())
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot reassign uuid");
  }

  @Test
  public void viewMetadataWithMetadataLocation() {
    Schema schema = new Schema(Types.NestedField.required(1, "x", Types.LongType.get()));
    ViewVersion viewVersion =
        ImmutableViewVersion.builder()
            .schemaId(schema.schemaId())
            .versionId(1)
            .timestampMillis(23L)
            .defaultNamespace(Namespace.of("ns"))
            .build();

    assertThatThrownBy(
            () ->
                ViewMetadata.builder()
                    .setLocation("custom-location")
                    .setMetadataLocation("metadata-location")
                    .addSchema(schema)
                    .addVersion(viewVersion)
                    .setCurrentVersionId(1)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot create view metadata with a metadata location and changes");

    // setting metadata location without changes is ok
    ViewMetadata viewMetadata =
        ViewMetadata.buildFrom(
                ViewMetadata.builder()
                    .setLocation("custom-location")
                    .addSchema(schema)
                    .addVersion(viewVersion)
                    .setCurrentVersionId(1)
                    .build())
            .setMetadataLocation("metadata-location")
            .build();
    assertThat(viewMetadata.metadataFileLocation()).isEqualTo("metadata-location");
  }

  @Test
  public void viewVersionIDReassignment() {
    // all view versions have the same ID
    ViewVersion viewVersionOne = newViewVersion(1, "select * from ns.tbl");
    ViewVersion viewVersionTwo = newViewVersion(1, "select count(*) from ns.tbl");
    ViewVersion viewVersionThree = newViewVersion(1, "select count(*) as count from ns.tbl");

    ViewMetadata viewMetadata =
        ViewMetadata.builder()
            .setLocation("custom-location")
            .addSchema(new Schema(Types.NestedField.required(1, "x", Types.LongType.get())))
            .addVersion(viewVersionOne)
            .addVersion(viewVersionTwo)
            .addVersion(viewVersionThree)
            .setCurrentVersionId(3)
            .build();

    assertThat(viewMetadata.currentVersion())
        .isEqualTo(ImmutableViewVersion.builder().from(viewVersionThree).versionId(3).build());

    // IDs of the view versions should be re-assigned
    assertThat(viewMetadata.versions())
        .hasSize(3)
        .containsExactly(
            viewVersionOne,
            ImmutableViewVersion.builder().from(viewVersionTwo).versionId(2).build(),
            ImmutableViewVersion.builder().from(viewVersionThree).versionId(3).build());
  }

  @Test
  public void viewVersionDeduplication() {
    // all view versions have the same ID
    // additionally, there are duplicate view versions that only differ in their creation timestamp
    ViewVersion viewVersionOne = newViewVersion(1, "select * from ns.tbl");
    ViewVersion viewVersionTwo = newViewVersion(1, "select count(*) from ns.tbl");
    ViewVersion viewVersionThree = newViewVersion(1, "select count(*) as count from ns.tbl");
    ViewVersion viewVersionOneUpdated =
        ImmutableViewVersion.builder().from(viewVersionOne).timestampMillis(1000).build();
    ViewVersion viewVersionTwoUpdated =
        ImmutableViewVersion.builder().from(viewVersionTwo).timestampMillis(100).build();
    ViewVersion viewVersionThreeUpdated =
        ImmutableViewVersion.builder().from(viewVersionThree).timestampMillis(10).build();

    ViewMetadata viewMetadata =
        ViewMetadata.builder()
            .setLocation("custom-location")
            .addSchema(new Schema(Types.NestedField.required(1, "x", Types.LongType.get())))
            .addVersion(viewVersionOne)
            .addVersion(viewVersionTwo)
            .addVersion(viewVersionThree)
            .addVersion(viewVersionOneUpdated)
            .addVersion(viewVersionTwoUpdated)
            .addVersion(viewVersionThreeUpdated)
            .setCurrentVersionId(3)
            .build();

    assertThat(viewMetadata.currentVersion())
        .isEqualTo(ImmutableViewVersion.builder().from(viewVersionThree).versionId(3).build());

    // IDs of the view versions should be re-assigned and view versions should be de-duplicated
    assertThat(viewMetadata.versions())
        .hasSize(3)
        .containsExactly(
            viewVersionOne,
            ImmutableViewVersion.builder().from(viewVersionTwo).versionId(2).build(),
            ImmutableViewVersion.builder().from(viewVersionThree).versionId(3).build());
  }

  @Test
  public void viewVersionDeduplicationWithCustomSummary() {
    // all view versions have the same ID
    // additionally, there are duplicate view versions that only differ in the summary
    ViewVersion viewVersionOne = newViewVersion(1, "select * from ns.tbl");
    ViewVersion viewVersionTwo = newViewVersion(1, "select count(*) from ns.tbl");
    ViewVersion viewVersionOneUpdated =
        ImmutableViewVersion.builder()
            .from(viewVersionOne)
            .timestampMillis(1000)
            .summary(ImmutableMap.of("user", "some-user"))
            .build();
    ViewVersion viewVersionTwoUpdated =
        ImmutableViewVersion.builder()
            .from(viewVersionTwo)
            .summary(ImmutableMap.of("user", "some-user", "key", "val"))
            .build();

    ViewMetadata viewMetadata =
        ViewMetadata.builder()
            .setLocation("custom-location")
            .addSchema(new Schema(Types.NestedField.required(1, "x", Types.LongType.get())))
            .addVersion(viewVersionOne)
            .addVersion(viewVersionTwo)
            .addVersion(viewVersionOneUpdated)
            .addVersion(viewVersionTwoUpdated)
            .setCurrentVersionId(3)
            .build();

    assertThat(viewMetadata.currentVersion())
        .isEqualTo(ImmutableViewVersion.builder().from(viewVersionTwoUpdated).versionId(3).build());

    // IDs of the view versions should be re-assigned and view versions should be de-duplicated
    assertThat(viewMetadata.versions())
        .hasSize(3)
        .containsExactly(
            viewVersionOne,
            ImmutableViewVersion.builder().from(viewVersionTwo).versionId(2).build(),
            ImmutableViewVersion.builder().from(viewVersionTwoUpdated).versionId(3).build());
  }

  @Test
  public void schemaIDReassignment() {
    Schema schemaOne = new Schema(5, Types.NestedField.required(1, "x", Types.LongType.get()));
    Schema schemaTwo = new Schema(7, Types.NestedField.required(1, "y", Types.LongType.get()));
    Schema schemaThree = new Schema(9, Types.NestedField.required(1, "z", Types.LongType.get()));

    ViewVersion viewVersion = newViewVersion(1, "select * from ns.tbl");
    ViewMetadata viewMetadata =
        ViewMetadata.builder()
            .setLocation("custom-location")
            .addSchema(schemaOne)
            .addSchema(schemaTwo)
            .addSchema(schemaThree)
            .setCurrentVersion(viewVersion, schemaThree)
            .build();

    // schema ID should be re-assigned
    assertThat(viewMetadata.versions())
        .hasSize(1)
        .containsExactly(ImmutableViewVersion.builder().from(viewVersion).schemaId(2).build());

    // all schema IDs should be re-assigned and start at 0
    assertThat(viewMetadata.schemas().stream().map(Schema::asStruct))
        .hasSize(3)
        .containsExactly(schemaOne.asStruct(), schemaTwo.asStruct(), schemaThree.asStruct());
    assertThat(viewMetadata.schemasById().keySet()).containsExactly(0, 1, 2);
  }

  @Test
  public void schemaDeduplication() {
    Schema schemaOne = new Schema(5, Types.NestedField.required(1, "x", Types.LongType.get()));
    Schema schemaTwo = new Schema(7, Types.NestedField.required(1, "y", Types.LongType.get()));
    Schema schemaThree = new Schema(9, Types.NestedField.required(1, "z", Types.LongType.get()));
    Schema schemaOneUpdated =
        new Schema(6, Types.NestedField.required(1, "x", Types.LongType.get()));
    Schema schemaTwoUpdated =
        new Schema(8, Types.NestedField.required(1, "y", Types.LongType.get()));
    Schema schemaThreeUpdated =
        new Schema(10, Types.NestedField.required(1, "z", Types.LongType.get()));

    ViewVersion viewVersion = newViewVersion(1, "select * from ns.tbl");
    ViewMetadata viewMetadata =
        ViewMetadata.builder()
            .setLocation("custom-location")
            .addSchema(schemaOne)
            .addSchema(schemaTwo)
            .addSchema(schemaThree)
            .addSchema(schemaOneUpdated)
            .addSchema(schemaTwoUpdated)
            .addSchema(schemaThreeUpdated)
            .setCurrentVersion(viewVersion, schemaThree)
            .build();

    // schema ID should be re-assigned
    assertThat(viewMetadata.versions())
        .hasSize(1)
        .containsExactly(ImmutableViewVersion.builder().from(viewVersion).schemaId(2).build());

    // all schema IDs should be re-assigned and start at 0 and be de-duplicated
    assertThat(viewMetadata.schemas().stream().map(Schema::asStruct))
        .hasSize(3)
        .containsExactly(schemaOne.asStruct(), schemaTwo.asStruct(), schemaThree.asStruct());
    assertThat(viewMetadata.schemasById().keySet()).containsExactly(0, 1, 2);
  }

  @Test
  public void viewVersionAndSchemaIDReassignment() {
    Schema schemaOne = new Schema(5, Types.NestedField.required(1, "x", Types.LongType.get()));
    Schema schemaTwo = new Schema(7, Types.NestedField.required(1, "y", Types.LongType.get()));
    Schema schemaThree = new Schema(9, Types.NestedField.required(1, "z", Types.LongType.get()));

    // all view versions have the same ID
    ViewVersion viewVersionOne = newViewVersion(1, 5, "select * from ns.tbl");
    ViewVersion viewVersionTwo = newViewVersion(1, 7, "select count(*) from ns.tbl");
    ViewVersion viewVersionThree = newViewVersion(1, 9, "select count(*) as count from ns.tbl");

    ViewMetadata viewMetadata =
        ViewMetadata.builder()
            .setLocation("custom-location")
            .addSchema(schemaOne)
            .addSchema(schemaTwo)
            .addSchema(schemaThree)
            .setCurrentVersion(viewVersionOne, schemaOne)
            .setCurrentVersion(viewVersionTwo, schemaTwo)
            .setCurrentVersion(viewVersionThree, schemaThree)
            .build();

    assertThat(viewMetadata.currentVersion())
        .isEqualTo(
            ImmutableViewVersion.builder().from(viewVersionThree).versionId(3).schemaId(2).build());

    // IDs of the schemas and view versions should be re-assigned
    assertThat(viewMetadata.versions())
        .hasSize(3)
        .containsExactly(
            ImmutableViewVersion.builder().from(viewVersionOne).versionId(1).schemaId(0).build(),
            ImmutableViewVersion.builder().from(viewVersionTwo).versionId(2).schemaId(1).build(),
            ImmutableViewVersion.builder().from(viewVersionThree).versionId(3).schemaId(2).build());

    assertThat(viewMetadata.schemas().stream().map(Schema::asStruct))
        .hasSize(3)
        .containsExactly(schemaOne.asStruct(), schemaTwo.asStruct(), schemaThree.asStruct());
    assertThat(viewMetadata.schemasById().keySet()).containsExactly(0, 1, 2);
  }

  @Test
  public void viewVersionAndSchemaDeduplication() {
    Schema schemaOne = new Schema(5, Types.NestedField.required(1, "x", Types.LongType.get()));
    Schema schemaTwo = new Schema(7, Types.NestedField.required(1, "y", Types.LongType.get()));
    Schema schemaThree = new Schema(9, Types.NestedField.required(1, "z", Types.LongType.get()));

    // all view versions have the same ID
    // additionally, there are duplicate view versions and schemas
    ViewVersion viewVersionOne = newViewVersion(1, 5, "select * from ns.tbl");
    ViewVersion viewVersionTwo = newViewVersion(1, 7, "select count(*) from ns.tbl");
    ViewVersion viewVersionThree = newViewVersion(1, 9, "select count(*) as count from ns.tbl");

    // all view versions have the same ID
    ViewMetadata viewMetadata =
        ViewMetadata.builder()
            .setLocation("custom-location")
            .addSchema(schemaOne)
            .addSchema(schemaTwo)
            .addSchema(schemaThree)
            .setCurrentVersion(viewVersionOne, schemaOne)
            .setCurrentVersion(viewVersionTwo, schemaTwo)
            .setCurrentVersion(viewVersionThree, schemaThree)
            .setCurrentVersion(viewVersionThree, schemaThree)
            .setCurrentVersion(viewVersionTwo, schemaTwo)
            .setCurrentVersion(viewVersionOne, schemaOne)
            .build();

    assertThat(viewMetadata.currentVersion())
        .isEqualTo(
            ImmutableViewVersion.builder().from(viewVersionOne).versionId(1).schemaId(0).build());

    // IDs of schemas and view versions should be re-assigned and both should be de-duplicated
    assertThat(viewMetadata.versions())
        .hasSize(3)
        .containsExactly(
            ImmutableViewVersion.builder().from(viewVersionOne).versionId(1).schemaId(0).build(),
            ImmutableViewVersion.builder().from(viewVersionTwo).versionId(2).schemaId(1).build(),
            ImmutableViewVersion.builder().from(viewVersionThree).versionId(3).schemaId(2).build());

    assertThat(viewMetadata.schemas().stream().map(Schema::asStruct))
        .hasSize(3)
        .containsExactly(schemaOne.asStruct(), schemaTwo.asStruct(), schemaThree.asStruct());
    assertThat(viewMetadata.schemasById().keySet()).containsExactly(0, 1, 2);
  }

  @Test
  public void viewMetadataWithMultipleSQLForSameDialect() {
    assertThatThrownBy(
            () ->
                ViewMetadata.builder()
                    .setLocation("custom-location")
                    .addSchema(new Schema(Types.NestedField.required(1, "x", Types.LongType.get())))
                    .addVersion(
                        ImmutableViewVersion.builder()
                            .schemaId(0)
                            .versionId(1)
                            .timestampMillis(23L)
                            .defaultNamespace(Namespace.of("ns"))
                            .addRepresentations(
                                ImmutableSQLViewRepresentation.builder()
                                    .dialect("spark")
                                    .sql("select * from ns.tbl")
                                    .build())
                            .addRepresentations(
                                ImmutableSQLViewRepresentation.builder()
                                    .dialect("spark")
                                    .sql("select * from ns.tbl2")
                                    .build())
                            .build())
                    .setCurrentVersionId(1)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid view version: Cannot add multiple queries for dialect spark");
  }
}
