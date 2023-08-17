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
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

public class TestViewMetadata {

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
        .hasMessage("Cannot find current version 1 in view versions: []");
  }

  @Test
  public void unsupportedFormatVersion() {
    assertThatThrownBy(
            () ->
                ViewMetadata.builder()
                    .upgradeFormatVersion(23)
                    .setLocation("location")
                    .addVersion(
                        ImmutableViewVersion.builder()
                            .schemaId(1)
                            .versionId(1)
                            .timestampMillis(23L)
                            .putSummary("operation", "op")
                            .defaultNamespace(Namespace.of("ns"))
                            .build())
                    .addSchema(
                        new Schema(1, Types.NestedField.required(1, "x", Types.LongType.get())))
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
        .hasMessage("Cannot find current version 1 in view versions: []");
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
                            .putSummary("operation", "op")
                            .defaultNamespace(Namespace.of("ns"))
                            .build())
                    .setCurrentVersionId(1)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find current schema with id 1 in schemas: []");
  }

  @Test
  public void invalidCurrentVersionId() {
    assertThatThrownBy(
            () ->
                ViewMetadata.builder()
                    .setLocation("location")
                    .addVersion(
                        ImmutableViewVersion.builder()
                            .schemaId(1)
                            .versionId(1)
                            .timestampMillis(23L)
                            .putSummary("operation", "op")
                            .defaultNamespace(Namespace.of("ns"))
                            .build())
                    .addSchema(
                        new Schema(1, Types.NestedField.required(1, "x", Types.LongType.get())))
                    .setCurrentVersionId(23)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find current version 23 in view versions: [1]");
  }

  @Test
  public void invalidCurrentSchemaId() {
    assertThatThrownBy(
            () ->
                ViewMetadata.builder()
                    .setLocation("location")
                    .addVersion(
                        ImmutableViewVersion.builder()
                            .schemaId(23)
                            .versionId(1)
                            .defaultNamespace(Namespace.of("ns"))
                            .timestampMillis(23L)
                            .putSummary("operation", "op")
                            .build())
                    .addSchema(
                        new Schema(1, Types.NestedField.required(1, "x", Types.LongType.get())))
                    .setCurrentVersionId(1)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find current schema with id 23 in schemas: [1]");
  }

  @Test
  public void invalidVersionHistorySizeToKeep() {
    assertThatThrownBy(
            () ->
                ViewMetadata.builder()
                    .setProperties(ImmutableMap.of(ViewProperties.VERSION_HISTORY_SIZE, "0"))
                    .setLocation("location")
                    .addVersion(
                        ImmutableViewVersion.builder()
                            .schemaId(1)
                            .versionId(1)
                            .timestampMillis(23L)
                            .putSummary("operation", "a")
                            .defaultNamespace(Namespace.of("ns"))
                            .build())
                    .addVersion(
                        ImmutableViewVersion.builder()
                            .schemaId(1)
                            .versionId(2)
                            .timestampMillis(24L)
                            .putSummary("operation", "b")
                            .defaultNamespace(Namespace.of("ns"))
                            .build())
                    .addVersion(
                        ImmutableViewVersion.builder()
                            .schemaId(1)
                            .versionId(3)
                            .timestampMillis(25L)
                            .putSummary("operation", "c")
                            .defaultNamespace(Namespace.of("ns"))
                            .build())
                    .addSchema(
                        new Schema(1, Types.NestedField.required(1, "x", Types.LongType.get())))
                    .setCurrentVersionId(3)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("version.history.num-entries must be positive but was 0");
  }

  @Test
  public void viewHistoryNormalization() {
    Map<String, String> properties = ImmutableMap.of(ViewProperties.VERSION_HISTORY_SIZE, "2");
    ViewVersion viewVersionOne =
        ImmutableViewVersion.builder()
            .schemaId(1)
            .versionId(1)
            .timestampMillis(23L)
            .putSummary("operation", "a")
            .defaultNamespace(Namespace.of("ns"))
            .build();
    ViewVersion viewVersionTwo =
        ImmutableViewVersion.builder()
            .schemaId(1)
            .versionId(2)
            .timestampMillis(24L)
            .putSummary("operation", "b")
            .defaultNamespace(Namespace.of("ns"))
            .build();
    ViewVersion viewVersionThree =
        ImmutableViewVersion.builder()
            .schemaId(1)
            .versionId(3)
            .timestampMillis(25L)
            .putSummary("operation", "c")
            .defaultNamespace(Namespace.of("ns"))
            .build();

    ViewMetadata viewMetadata =
        ViewMetadata.builder()
            .setProperties(properties)
            .setLocation("location")
            .addVersion(viewVersionOne)
            .addVersion(viewVersionTwo)
            .addVersion(viewVersionThree)
            .addSchema(new Schema(1, Types.NestedField.required(1, "x", Types.LongType.get())))
            .setCurrentVersionId(3)
            .build();

    assertThat(viewMetadata.versions()).hasSize(2);
    assertThat(viewMetadata.history()).hasSize(2);

    // make sure that metadata changes reflect the current state after the history was adjusted,
    // meaning that the first view version shouldn't be included
    List<MetadataUpdate> changes = viewMetadata.changes();
    assertThat(changes).hasSize(6);
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
        .isInstanceOf(MetadataUpdate.AddViewVersion.class)
        .asInstanceOf(InstanceOfAssertFactories.type(MetadataUpdate.AddViewVersion.class))
        .extracting(MetadataUpdate.AddViewVersion::viewVersion)
        .isEqualTo(viewVersionTwo);

    assertThat(changes)
        .element(3)
        .isInstanceOf(MetadataUpdate.AddViewVersion.class)
        .asInstanceOf(InstanceOfAssertFactories.type(MetadataUpdate.AddViewVersion.class))
        .extracting(MetadataUpdate.AddViewVersion::viewVersion)
        .isEqualTo(viewVersionThree);

    assertThat(changes)
        .element(4)
        .isInstanceOf(MetadataUpdate.AddSchema.class)
        .asInstanceOf(InstanceOfAssertFactories.type(MetadataUpdate.AddSchema.class))
        .extracting(MetadataUpdate.AddSchema::schema)
        .extracting(Schema::schemaId)
        .isEqualTo(1);

    assertThat(changes)
        .element(5)
        .isInstanceOf(MetadataUpdate.SetCurrentViewVersion.class)
        .asInstanceOf(InstanceOfAssertFactories.type(MetadataUpdate.SetCurrentViewVersion.class))
        .extracting(MetadataUpdate.SetCurrentViewVersion::versionId)
        .isEqualTo(-1);
  }

  @Test
  public void viewMetadataAndMetadataChanges() {
    Map<String, String> properties = ImmutableMap.of("key1", "prop1", "key2", "prop2");
    Schema schemaOne = new Schema(1, Types.NestedField.required(1, "x", Types.LongType.get()));
    Schema schemaTwo = new Schema(2, Types.NestedField.required(1, "y", Types.LongType.get()));
    ViewVersion viewVersionOne =
        ImmutableViewVersion.builder()
            .schemaId(schemaOne.schemaId())
            .versionId(1)
            .timestampMillis(23L)
            .putSummary("operation", "a")
            .defaultNamespace(Namespace.of("ns"))
            .build();
    ViewVersion viewVersionTwo =
        ImmutableViewVersion.builder()
            .schemaId(schemaOne.schemaId())
            .versionId(2)
            .timestampMillis(24L)
            .putSummary("operation", "b")
            .defaultNamespace(Namespace.of("ns"))
            .build();
    ViewVersion viewVersionThree =
        ImmutableViewVersion.builder()
            .schemaId(schemaTwo.schemaId())
            .versionId(3)
            .timestampMillis(25L)
            .putSummary("operation", "c")
            .defaultNamespace(Namespace.of("ns"))
            .build();

    ViewMetadata viewMetadata =
        ViewMetadata.builder()
            .setLocation("custom-location")
            .setProperties(properties)
            .addVersion(viewVersionOne)
            .addVersion(viewVersionTwo)
            .addVersion(viewVersionThree)
            .addSchema(schemaOne)
            .addSchema(schemaTwo)
            .setCurrentVersionId(3)
            .build();

    assertThat(viewMetadata.versions())
        .hasSize(3)
        .containsExactly(viewVersionOne, viewVersionTwo, viewVersionThree);
    assertThat(viewMetadata.history()).hasSize(3);
    assertThat(viewMetadata.currentVersionId()).isEqualTo(3);
    assertThat(viewMetadata.currentVersion()).isEqualTo(viewVersionThree);
    assertThat(viewMetadata.formatVersion()).isEqualTo(ViewMetadata.DEFAULT_VIEW_FORMAT_VERSION);
    assertThat(viewMetadata.schemas()).hasSize(2).containsExactly(schemaOne, schemaTwo);
    assertThat(viewMetadata.schema().asStruct()).isEqualTo(schemaTwo.asStruct());
    assertThat(viewMetadata.currentSchemaId()).isEqualTo(schemaTwo.schemaId());
    assertThat(viewMetadata.location()).isEqualTo("custom-location");
    assertThat(viewMetadata.properties()).isEqualTo(properties);

    List<MetadataUpdate> changes = viewMetadata.changes();
    assertThat(changes).hasSize(8);
    assertThat(changes)
        .element(0)
        .isInstanceOf(MetadataUpdate.SetLocation.class)
        .asInstanceOf(InstanceOfAssertFactories.type(MetadataUpdate.SetLocation.class))
        .extracting(MetadataUpdate.SetLocation::location)
        .isEqualTo("custom-location");

    assertThat(changes)
        .element(1)
        .isInstanceOf(MetadataUpdate.SetProperties.class)
        .asInstanceOf(InstanceOfAssertFactories.type(MetadataUpdate.SetProperties.class))
        .extracting(MetadataUpdate.SetProperties::updated)
        .isEqualTo(properties);

    assertThat(changes)
        .element(2)
        .isInstanceOf(MetadataUpdate.AddViewVersion.class)
        .asInstanceOf(InstanceOfAssertFactories.type(MetadataUpdate.AddViewVersion.class))
        .extracting(MetadataUpdate.AddViewVersion::viewVersion)
        .isEqualTo(viewVersionOne);

    assertThat(changes)
        .element(3)
        .isInstanceOf(MetadataUpdate.AddViewVersion.class)
        .asInstanceOf(InstanceOfAssertFactories.type(MetadataUpdate.AddViewVersion.class))
        .extracting(MetadataUpdate.AddViewVersion::viewVersion)
        .isEqualTo(viewVersionTwo);

    assertThat(changes)
        .element(4)
        .isInstanceOf(MetadataUpdate.AddViewVersion.class)
        .asInstanceOf(InstanceOfAssertFactories.type(MetadataUpdate.AddViewVersion.class))
        .extracting(MetadataUpdate.AddViewVersion::viewVersion)
        .isEqualTo(viewVersionThree);

    assertThat(changes)
        .element(5)
        .isInstanceOf(MetadataUpdate.AddSchema.class)
        .asInstanceOf(InstanceOfAssertFactories.type(MetadataUpdate.AddSchema.class))
        .extracting(MetadataUpdate.AddSchema::schema)
        .extracting(Schema::schemaId)
        .isEqualTo(1);

    assertThat(changes)
        .element(6)
        .isInstanceOf(MetadataUpdate.AddSchema.class)
        .asInstanceOf(InstanceOfAssertFactories.type(MetadataUpdate.AddSchema.class))
        .extracting(MetadataUpdate.AddSchema::schema)
        .extracting(Schema::schemaId)
        .isEqualTo(2);

    assertThat(changes)
        .element(7)
        .isInstanceOf(MetadataUpdate.SetCurrentViewVersion.class)
        .asInstanceOf(InstanceOfAssertFactories.type(MetadataUpdate.SetCurrentViewVersion.class))
        .extracting(MetadataUpdate.SetCurrentViewVersion::versionId)
        .isEqualTo(-1);
  }
}
