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
package org.apache.iceberg;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.ImmutableViewVersion;
import org.apache.iceberg.view.ViewMetadata;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestUpdateRequirements {
  private final TableMetadata metadata = mock(TableMetadata.class);
  private final TableMetadata updated = mock(TableMetadata.class);
  private final ViewMetadata viewMetadata = mock(ViewMetadata.class);
  private final ViewMetadata updatedViewMetadata = mock(ViewMetadata.class);

  @BeforeEach
  public void before() {
    String uuid = UUID.randomUUID().toString();
    when(metadata.uuid()).thenReturn(uuid);
    when(updated.uuid()).thenReturn(uuid);
    when(viewMetadata.uuid()).thenReturn(uuid);
    when(updatedViewMetadata.uuid()).thenReturn(uuid);
  }

  @Test
  public void nullCheck() {
    assertThatThrownBy(() -> UpdateRequirements.forCreateTable(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid metadata updates: null");

    assertThatThrownBy(() -> UpdateRequirements.forUpdateTable(null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid table metadata: null");

    assertThatThrownBy(() -> UpdateRequirements.forUpdateTable(metadata, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid metadata updates: null");

    assertThatThrownBy(() -> UpdateRequirements.forReplaceTable(null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid table metadata: null");

    assertThatThrownBy(() -> UpdateRequirements.forReplaceTable(metadata, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid metadata updates: null");

    assertThatThrownBy(() -> UpdateRequirements.forReplaceView(null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid view metadata: null");

    assertThatThrownBy(() -> UpdateRequirements.forReplaceView(viewMetadata, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid metadata updates: null");
  }

  @Test
  public void emptyUpdatesForCreateTable() {
    assertThat(UpdateRequirements.forCreateTable(ImmutableList.of()))
        .hasSize(1)
        .hasOnlyElementsOfType(UpdateRequirement.AssertTableDoesNotExist.class);
  }

  @Test
  public void emptyUpdatesForUpdateAndReplaceTable() {
    assertThat(UpdateRequirements.forReplaceTable(metadata, ImmutableList.of()))
        .hasSize(1)
        .hasOnlyElementsOfType(UpdateRequirement.AssertTableUUID.class);

    assertThat(UpdateRequirements.forUpdateTable(metadata, ImmutableList.of()))
        .hasSize(1)
        .hasOnlyElementsOfType(UpdateRequirement.AssertTableUUID.class);
  }

  @Test
  public void emptyUpdatesForReplaceView() {
    assertThat(UpdateRequirements.forReplaceView(viewMetadata, ImmutableList.of()))
        .hasSize(1)
        .hasOnlyElementsOfType(UpdateRequirement.AssertViewUUID.class);
  }

  @Test
  public void tableAlreadyExists() {
    List<UpdateRequirement> requirements = UpdateRequirements.forCreateTable(ImmutableList.of());

    assertThatThrownBy(() -> requirements.forEach(req -> req.validate(metadata)))
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Requirement failed: table already exists");
  }

  @Test
  public void assignUUID() {
    List<UpdateRequirement> requirements =
        UpdateRequirements.forUpdateTable(
            metadata,
            ImmutableList.of(
                new MetadataUpdate.AssignUUID(metadata.uuid()),
                new MetadataUpdate.AssignUUID(UUID.randomUUID().toString()),
                new MetadataUpdate.AssignUUID(UUID.randomUUID().toString())));
    requirements.forEach(req -> req.validate(metadata));

    assertThat(requirements)
        .hasSize(1)
        .hasOnlyElementsOfType(UpdateRequirement.AssertTableUUID.class);

    assertTableUUID(requirements);
  }

  @Test
  public void assignUUIDFailure() {
    List<UpdateRequirement> requirements =
        UpdateRequirements.forUpdateTable(
            metadata, ImmutableList.of(new MetadataUpdate.AssignUUID(metadata.uuid())));

    when(updated.uuid()).thenReturn(UUID.randomUUID().toString());
    assertThatThrownBy(() -> requirements.forEach(req -> req.validate(updated)))
        .isInstanceOf(CommitFailedException.class)
        .hasMessage(
            String.format(
                "Requirement failed: UUID does not match: expected %s != %s",
                updated.uuid(), metadata.uuid()));
  }

  @Test
  public void assignUUIDToView() {
    List<UpdateRequirement> requirements =
        UpdateRequirements.forReplaceView(
            viewMetadata,
            ImmutableList.of(
                new MetadataUpdate.AssignUUID(viewMetadata.uuid()),
                new MetadataUpdate.AssignUUID(UUID.randomUUID().toString()),
                new MetadataUpdate.AssignUUID(UUID.randomUUID().toString())));
    requirements.forEach(req -> req.validate(viewMetadata));

    assertThat(requirements)
        .hasSize(1)
        .hasOnlyElementsOfType(UpdateRequirement.AssertViewUUID.class);

    assertViewUUID(requirements);
  }

  @Test
  public void assignUUIDToViewFailure() {
    List<UpdateRequirement> requirements =
        UpdateRequirements.forReplaceView(
            viewMetadata, ImmutableList.of(new MetadataUpdate.AssignUUID(viewMetadata.uuid())));

    when(updatedViewMetadata.uuid()).thenReturn(UUID.randomUUID().toString());
    assertThatThrownBy(() -> requirements.forEach(req -> req.validate(updatedViewMetadata)))
        .isInstanceOf(CommitFailedException.class)
        .hasMessage(
            String.format(
                "Requirement failed: view UUID does not match: expected %s != %s",
                updatedViewMetadata.uuid(), viewMetadata.uuid()));
  }

  @ParameterizedTest
  @ValueSource(ints = {2, 3})
  public void upgradeFormatVersion(int formatVersion) {
    List<UpdateRequirement> requirements =
        UpdateRequirements.forUpdateTable(
            metadata, ImmutableList.of(new MetadataUpdate.UpgradeFormatVersion(formatVersion)));
    requirements.forEach(req -> req.validate(metadata));

    assertThat(requirements)
        .hasSize(1)
        .hasOnlyElementsOfType(UpdateRequirement.AssertTableUUID.class);

    assertTableUUID(requirements);
  }

  @ParameterizedTest
  @ValueSource(ints = {2, 3})
  public void upgradeFormatVersionForView(int formatVersion) {
    List<UpdateRequirement> requirements =
        UpdateRequirements.forReplaceView(
            viewMetadata, ImmutableList.of(new MetadataUpdate.UpgradeFormatVersion(formatVersion)));
    requirements.forEach(req -> req.validate(viewMetadata));

    assertThat(requirements)
        .hasSize(1)
        .hasOnlyElementsOfType(UpdateRequirement.AssertViewUUID.class);

    assertViewUUID(requirements);
  }

  @Test
  public void upgradeFormatVersionForViewV3() {
    List<UpdateRequirement> requirements =
        UpdateRequirements.forReplaceView(
            viewMetadata, ImmutableList.of(new MetadataUpdate.UpgradeFormatVersion(3)));
    requirements.forEach(req -> req.validate(viewMetadata));

    assertThat(requirements)
        .hasSize(1)
        .hasOnlyElementsOfType(UpdateRequirement.AssertViewUUID.class);

    assertViewUUID(requirements);
  }

  @Test
  public void addSchema() {
    int lastColumnId = 1;
    when(metadata.lastColumnId()).thenReturn(lastColumnId);
    List<UpdateRequirement> requirements =
        UpdateRequirements.forUpdateTable(
            metadata,
            ImmutableList.of(
                new MetadataUpdate.AddSchema(new Schema(), lastColumnId),
                new MetadataUpdate.AddSchema(new Schema(), lastColumnId + 1),
                new MetadataUpdate.AddSchema(new Schema(), lastColumnId + 2)));
    requirements.forEach(req -> req.validate(metadata));

    assertThat(requirements)
        .hasSize(2)
        .hasOnlyElementsOfTypes(
            UpdateRequirement.AssertTableUUID.class,
            UpdateRequirement.AssertLastAssignedFieldId.class);

    assertTableUUID(requirements);

    assertThat(requirements)
        .element(1)
        .asInstanceOf(
            InstanceOfAssertFactories.type(UpdateRequirement.AssertLastAssignedFieldId.class))
        .extracting(UpdateRequirement.AssertLastAssignedFieldId::lastAssignedFieldId)
        .isEqualTo(lastColumnId);
  }

  @Test
  public void addSchemaFailure() {
    when(metadata.lastColumnId()).thenReturn(2);
    when(updated.lastColumnId()).thenReturn(3);

    List<UpdateRequirement> requirements =
        UpdateRequirements.forUpdateTable(
            metadata,
            ImmutableList.of(
                new MetadataUpdate.AddSchema(new Schema(), 1),
                new MetadataUpdate.AddSchema(new Schema(), 2),
                new MetadataUpdate.AddSchema(new Schema(), 3)));

    assertThatThrownBy(() -> requirements.forEach(req -> req.validate(updated)))
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Requirement failed: last assigned field id changed: expected id 2 != 3");
  }

  @Test
  public void addSchemaForView() {
    int lastColumnId = 1;
    List<UpdateRequirement> requirements =
        UpdateRequirements.forReplaceView(
            viewMetadata,
            ImmutableList.of(
                new MetadataUpdate.AddSchema(new Schema(), lastColumnId),
                new MetadataUpdate.AddSchema(new Schema(), lastColumnId + 1),
                new MetadataUpdate.AddSchema(new Schema(), lastColumnId + 2)));
    requirements.forEach(req -> req.validate(viewMetadata));

    assertThat(requirements)
        .hasSize(1)
        .hasOnlyElementsOfTypes(UpdateRequirement.AssertViewUUID.class);

    assertViewUUID(requirements);
  }

  @Test
  public void setCurrentSchema() {
    int schemaId = 3;
    when(metadata.currentSchemaId()).thenReturn(schemaId);
    List<UpdateRequirement> requirements =
        UpdateRequirements.forUpdateTable(
            metadata,
            ImmutableList.of(
                new MetadataUpdate.SetCurrentSchema(schemaId),
                new MetadataUpdate.SetCurrentSchema(schemaId + 1),
                new MetadataUpdate.SetCurrentSchema(schemaId + 2)));
    requirements.forEach(req -> req.validate(metadata));

    assertThat(requirements)
        .hasSize(2)
        .hasOnlyElementsOfTypes(
            UpdateRequirement.AssertTableUUID.class, UpdateRequirement.AssertCurrentSchemaID.class);

    assertTableUUID(requirements);

    assertThat(requirements)
        .element(1)
        .asInstanceOf(InstanceOfAssertFactories.type(UpdateRequirement.AssertCurrentSchemaID.class))
        .extracting(UpdateRequirement.AssertCurrentSchemaID::schemaId)
        .isEqualTo(schemaId);
  }

  @Test
  public void setCurrentSchemaFailure() {
    int schemaId = 3;
    when(metadata.currentSchemaId()).thenReturn(schemaId);
    when(updated.currentSchemaId()).thenReturn(schemaId + 1);

    List<UpdateRequirement> requirements =
        UpdateRequirements.forUpdateTable(
            metadata,
            ImmutableList.of(
                new MetadataUpdate.SetCurrentSchema(schemaId),
                new MetadataUpdate.SetCurrentSchema(schemaId + 1),
                new MetadataUpdate.SetCurrentSchema(schemaId + 2)));

    assertThatThrownBy(() -> requirements.forEach(req -> req.validate(updated)))
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Requirement failed: current schema changed: expected id 3 != 4");
  }

  @Test
  public void addPartitionSpec() {
    int lastAssignedPartitionId = 3;
    when(metadata.lastAssignedPartitionId()).thenReturn(lastAssignedPartitionId);
    Schema schema = new Schema(required(3, "id", Types.IntegerType.get()));
    List<UpdateRequirement> requirements =
        UpdateRequirements.forUpdateTable(
            metadata,
            ImmutableList.of(
                new MetadataUpdate.AddPartitionSpec(
                    PartitionSpec.builderFor(schema).withSpecId(lastAssignedPartitionId).build()),
                new MetadataUpdate.AddPartitionSpec(
                    PartitionSpec.builderFor(schema)
                        .withSpecId(lastAssignedPartitionId + 1)
                        .build()),
                new MetadataUpdate.AddPartitionSpec(
                    PartitionSpec.builderFor(schema)
                        .withSpecId(lastAssignedPartitionId + 2)
                        .build())));
    requirements.forEach(req -> req.validate(metadata));

    assertThat(requirements)
        .hasSize(2)
        .hasOnlyElementsOfTypes(
            UpdateRequirement.AssertTableUUID.class,
            UpdateRequirement.AssertLastAssignedPartitionId.class);

    assertTableUUID(requirements);

    assertThat(requirements)
        .element(1)
        .asInstanceOf(
            InstanceOfAssertFactories.type(UpdateRequirement.AssertLastAssignedPartitionId.class))
        .extracting(UpdateRequirement.AssertLastAssignedPartitionId::lastAssignedPartitionId)
        .isEqualTo(lastAssignedPartitionId);
  }

  @Test
  public void addPartitionSpecFailure() {
    when(metadata.lastAssignedPartitionId()).thenReturn(3);
    when(updated.lastAssignedPartitionId()).thenReturn(4);

    List<UpdateRequirement> requirements =
        UpdateRequirements.forUpdateTable(
            metadata,
            ImmutableList.of(new MetadataUpdate.AddPartitionSpec(PartitionSpec.unpartitioned())));

    assertThatThrownBy(() -> requirements.forEach(req -> req.validate(updated)))
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Requirement failed: last assigned partition id changed: expected id 3 != 4");
  }

  @Test
  public void setDefaultPartitionSpec() {
    int specId = 3;
    when(metadata.defaultSpecId()).thenReturn(specId);
    List<UpdateRequirement> requirements =
        UpdateRequirements.forUpdateTable(
            metadata,
            ImmutableList.of(
                new MetadataUpdate.SetDefaultPartitionSpec(specId),
                new MetadataUpdate.SetDefaultPartitionSpec(specId + 1),
                new MetadataUpdate.SetDefaultPartitionSpec(specId + 2)));
    requirements.forEach(req -> req.validate(metadata));

    assertThat(requirements)
        .hasSize(2)
        .hasOnlyElementsOfTypes(
            UpdateRequirement.AssertTableUUID.class, UpdateRequirement.AssertDefaultSpecID.class);

    assertTableUUID(requirements);

    assertThat(requirements)
        .element(1)
        .asInstanceOf(InstanceOfAssertFactories.type(UpdateRequirement.AssertDefaultSpecID.class))
        .extracting(UpdateRequirement.AssertDefaultSpecID::specId)
        .isEqualTo(specId);
  }

  @Test
  public void setDefaultPartitionSpecFailure() {
    int specId = PartitionSpec.unpartitioned().specId();
    when(updated.defaultSpecId()).thenReturn(specId + 1);

    List<UpdateRequirement> requirements =
        UpdateRequirements.forUpdateTable(
            metadata,
            ImmutableList.of(
                new MetadataUpdate.SetDefaultPartitionSpec(specId),
                new MetadataUpdate.SetDefaultPartitionSpec(specId + 1),
                new MetadataUpdate.SetDefaultPartitionSpec(specId + 2)));

    assertThatThrownBy(() -> requirements.forEach(req -> req.validate(updated)))
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Requirement failed: default partition spec changed: expected id 0 != 1");
  }

  @Test
  public void addSortOrder() {
    List<UpdateRequirement> requirements =
        UpdateRequirements.forUpdateTable(
            metadata, ImmutableList.of(new MetadataUpdate.AddSortOrder(SortOrder.unsorted())));
    requirements.forEach(req -> req.validate(metadata));

    assertThat(requirements)
        .hasSize(1)
        .hasOnlyElementsOfTypes(UpdateRequirement.AssertTableUUID.class);

    assertTableUUID(requirements);
  }

  @Test
  public void setDefaultSortOrder() {
    int sortOrderId = 3;
    when(metadata.defaultSortOrderId()).thenReturn(sortOrderId);
    List<UpdateRequirement> requirements =
        UpdateRequirements.forUpdateTable(
            metadata,
            ImmutableList.of(
                new MetadataUpdate.SetDefaultSortOrder(sortOrderId),
                new MetadataUpdate.SetDefaultSortOrder(sortOrderId + 1),
                new MetadataUpdate.SetDefaultSortOrder(sortOrderId + 2)));
    requirements.forEach(req -> req.validate(metadata));

    assertThat(requirements)
        .hasSize(2)
        .hasOnlyElementsOfTypes(
            UpdateRequirement.AssertTableUUID.class,
            UpdateRequirement.AssertDefaultSortOrderID.class);

    assertTableUUID(requirements);

    assertThat(requirements)
        .element(1)
        .asInstanceOf(
            InstanceOfAssertFactories.type(UpdateRequirement.AssertDefaultSortOrderID.class))
        .extracting(UpdateRequirement.AssertDefaultSortOrderID::sortOrderId)
        .isEqualTo(sortOrderId);
  }

  @Test
  public void setDefaultSortOrderFailure() {
    int sortOrderId = SortOrder.unsorted().orderId();
    when(updated.defaultSortOrderId()).thenReturn(sortOrderId + 1);
    List<UpdateRequirement> requirements =
        UpdateRequirements.forUpdateTable(
            metadata, ImmutableList.of(new MetadataUpdate.SetDefaultSortOrder(sortOrderId)));

    assertThatThrownBy(() -> requirements.forEach(req -> req.validate(updated)))
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Requirement failed: default sort order changed: expected id 0 != 1");
  }

  @Test
  public void setAndRemoveStatistics() {
    List<UpdateRequirement> requirements =
        UpdateRequirements.forUpdateTable(
            metadata,
            ImmutableList.of(new MetadataUpdate.SetStatistics(0L, mock(StatisticsFile.class))));
    requirements.forEach(req -> req.validate(metadata));

    assertThat(requirements)
        .hasSize(1)
        .hasOnlyElementsOfTypes(UpdateRequirement.AssertTableUUID.class);

    assertTableUUID(requirements);

    requirements =
        UpdateRequirements.forUpdateTable(
            metadata, ImmutableList.of(new MetadataUpdate.RemoveStatistics(0L)));
    requirements.forEach(req -> req.validate(metadata));

    assertThat(requirements)
        .hasSize(1)
        .hasOnlyElementsOfTypes(UpdateRequirement.AssertTableUUID.class);

    assertTableUUID(requirements);
  }

  @Test
  public void addAndRemoveSnapshot() {
    List<UpdateRequirement> requirements =
        UpdateRequirements.forUpdateTable(
            metadata, ImmutableList.of(new MetadataUpdate.AddSnapshot(mock(Snapshot.class))));
    requirements.forEach(req -> req.validate(metadata));

    assertThat(requirements)
        .hasSize(1)
        .hasOnlyElementsOfTypes(UpdateRequirement.AssertTableUUID.class);

    assertTableUUID(requirements);

    requirements =
        UpdateRequirements.forUpdateTable(
            metadata, ImmutableList.of(new MetadataUpdate.RemoveSnapshot(0L)));

    assertThat(requirements)
        .hasSize(1)
        .hasOnlyElementsOfTypes(UpdateRequirement.AssertTableUUID.class);

    assertTableUUID(requirements);
  }

  @Test
  public void setAndRemoveSnapshotRef() {
    long snapshotId = 14L;
    String refName = "branch";
    SnapshotRef snapshotRef = mock(SnapshotRef.class);
    when(snapshotRef.snapshotId()).thenReturn(snapshotId);
    when(metadata.ref(refName)).thenReturn(snapshotRef);

    List<UpdateRequirement> requirements =
        UpdateRequirements.forUpdateTable(
            metadata,
            ImmutableList.of(
                new MetadataUpdate.SetSnapshotRef(
                    refName, snapshotId, SnapshotRefType.BRANCH, 0, 0L, 0L),
                new MetadataUpdate.SetSnapshotRef(
                    refName, snapshotId + 1, SnapshotRefType.BRANCH, 0, 0L, 0L),
                new MetadataUpdate.SetSnapshotRef(
                    refName, snapshotId + 2, SnapshotRefType.BRANCH, 0, 0L, 0L)));
    requirements.forEach(req -> req.validate(metadata));

    assertThat(requirements)
        .hasSize(2)
        .hasOnlyElementsOfTypes(
            UpdateRequirement.AssertTableUUID.class, UpdateRequirement.AssertRefSnapshotID.class);

    assertTableUUID(requirements);

    UpdateRequirement.AssertRefSnapshotID assertRefSnapshotID =
        (UpdateRequirement.AssertRefSnapshotID) requirements.get(1);
    assertThat(assertRefSnapshotID.snapshotId()).isEqualTo(snapshotId);
    assertThat(assertRefSnapshotID.refName()).isEqualTo(refName);

    requirements =
        UpdateRequirements.forUpdateTable(
            metadata, ImmutableList.of(new MetadataUpdate.RemoveSnapshot(0L)));
    requirements.forEach(req -> req.validate(metadata));

    assertThat(requirements)
        .hasSize(1)
        .hasOnlyElementsOfTypes(UpdateRequirement.AssertTableUUID.class);

    assertTableUUID(requirements);
  }

  @Test
  public void setSnapshotRefFailure() {
    long snapshotId = 14L;
    String refName = "random_branch";
    SnapshotRef snapshotRef = mock(SnapshotRef.class);
    when(snapshotRef.isBranch()).thenReturn(true);
    when(snapshotRef.snapshotId()).thenReturn(snapshotId);

    ImmutableList<MetadataUpdate> metadataUpdates =
        ImmutableList.of(
            new MetadataUpdate.SetSnapshotRef(
                refName, snapshotId, SnapshotRefType.BRANCH, 0, 0L, 0L));

    when(metadata.ref(refName)).thenReturn(null);
    when(updated.ref(refName)).thenReturn(snapshotRef);
    assertThatThrownBy(
            () ->
                UpdateRequirements.forUpdateTable(metadata, metadataUpdates)
                    .forEach(req -> req.validate(updated)))
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Requirement failed: branch random_branch was created concurrently");

    when(metadata.ref(refName)).thenReturn(snapshotRef);
    when(updated.ref(refName)).thenReturn(null);
    assertThatThrownBy(
            () ->
                UpdateRequirements.forUpdateTable(metadata, metadataUpdates)
                    .forEach(req -> req.validate(updated)))
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Requirement failed: branch or tag random_branch is missing, expected 14");

    SnapshotRef snapshotRefUpdated = mock(SnapshotRef.class);
    when(snapshotRefUpdated.isBranch()).thenReturn(true);
    when(snapshotRefUpdated.snapshotId()).thenReturn(snapshotId + 1);
    when(updated.ref(refName)).thenReturn(snapshotRefUpdated);
    assertThatThrownBy(
            () ->
                UpdateRequirements.forUpdateTable(metadata, metadataUpdates)
                    .forEach(req -> req.validate(updated)))
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Requirement failed: branch random_branch has changed: expected id 14 != 15");
  }

  @Test
  public void setAndRemoveProperties() {
    List<UpdateRequirement> requirements =
        UpdateRequirements.forUpdateTable(
            metadata,
            ImmutableList.of(new MetadataUpdate.SetProperties(ImmutableMap.of("test", "test"))));
    requirements.forEach(req -> req.validate(metadata));

    assertThat(requirements)
        .hasSize(1)
        .hasOnlyElementsOfTypes(UpdateRequirement.AssertTableUUID.class);

    assertTableUUID(requirements);

    requirements =
        UpdateRequirements.forUpdateTable(
            metadata,
            ImmutableList.of(new MetadataUpdate.RemoveProperties(Sets.newHashSet("test"))));
    requirements.forEach(req -> req.validate(metadata));

    assertThat(requirements)
        .hasSize(1)
        .hasOnlyElementsOfTypes(UpdateRequirement.AssertTableUUID.class);

    assertTableUUID(requirements);
  }

  @Test
  public void setAndRemovePropertiesForView() {
    List<UpdateRequirement> requirements =
        UpdateRequirements.forReplaceView(
            viewMetadata,
            ImmutableList.of(new MetadataUpdate.SetProperties(ImmutableMap.of("test", "test"))));
    requirements.forEach(req -> req.validate(viewMetadata));

    assertThat(requirements)
        .hasSize(1)
        .hasOnlyElementsOfTypes(UpdateRequirement.AssertViewUUID.class);

    assertViewUUID(requirements);

    requirements =
        UpdateRequirements.forReplaceView(
            viewMetadata,
            ImmutableList.of(new MetadataUpdate.RemoveProperties(Sets.newHashSet("test"))));
    requirements.forEach(req -> req.validate(viewMetadata));

    assertThat(requirements)
        .hasSize(1)
        .hasOnlyElementsOfTypes(UpdateRequirement.AssertViewUUID.class);

    assertViewUUID(requirements);
  }

  @Test
  public void setLocation() {
    List<UpdateRequirement> requirements =
        UpdateRequirements.forUpdateTable(
            metadata, ImmutableList.of(new MetadataUpdate.SetLocation("location")));
    requirements.forEach(req -> req.validate(metadata));

    assertThat(requirements)
        .hasSize(1)
        .hasOnlyElementsOfTypes(UpdateRequirement.AssertTableUUID.class);

    assertTableUUID(requirements);
  }

  @Test
  public void setLocationForView() {
    List<UpdateRequirement> requirements =
        UpdateRequirements.forReplaceView(
            viewMetadata, ImmutableList.of(new MetadataUpdate.SetLocation("location")));
    requirements.forEach(req -> req.validate(viewMetadata));

    assertThat(requirements)
        .hasSize(1)
        .hasOnlyElementsOfTypes(UpdateRequirement.AssertViewUUID.class);

    assertViewUUID(requirements);
  }

  @Test
  public void addViewVersion() {
    List<UpdateRequirement> requirements =
        UpdateRequirements.forReplaceView(
            viewMetadata,
            ImmutableList.of(
                new MetadataUpdate.AddViewVersion(
                    ImmutableViewVersion.builder()
                        .versionId(1)
                        .schemaId(1)
                        .timestampMillis(System.currentTimeMillis())
                        .defaultNamespace(Namespace.of("ns"))
                        .build()),
                new MetadataUpdate.AddViewVersion(
                    ImmutableViewVersion.builder()
                        .versionId(2)
                        .schemaId(1)
                        .timestampMillis(System.currentTimeMillis())
                        .defaultNamespace(Namespace.of("ns"))
                        .build()),
                new MetadataUpdate.AddViewVersion(
                    ImmutableViewVersion.builder()
                        .versionId(3)
                        .schemaId(1)
                        .timestampMillis(System.currentTimeMillis())
                        .defaultNamespace(Namespace.of("ns"))
                        .build())));
    requirements.forEach(req -> req.validate(viewMetadata));

    assertThat(requirements)
        .hasSize(1)
        .hasOnlyElementsOfTypes(UpdateRequirement.AssertViewUUID.class);

    assertViewUUID(requirements);
  }

  @Test
  public void setCurrentViewVersion() {
    List<UpdateRequirement> requirements =
        UpdateRequirements.forReplaceView(
            viewMetadata,
            ImmutableList.of(
                new MetadataUpdate.AddViewVersion(
                    ImmutableViewVersion.builder()
                        .versionId(3)
                        .schemaId(1)
                        .timestampMillis(System.currentTimeMillis())
                        .defaultNamespace(Namespace.of("ns"))
                        .build()),
                new MetadataUpdate.AddViewVersion(
                    ImmutableViewVersion.builder()
                        .versionId(2)
                        .schemaId(1)
                        .timestampMillis(System.currentTimeMillis())
                        .defaultNamespace(Namespace.of("ns"))
                        .build()),
                new MetadataUpdate.AddViewVersion(
                    ImmutableViewVersion.builder()
                        .versionId(1)
                        .schemaId(1)
                        .timestampMillis(System.currentTimeMillis())
                        .defaultNamespace(Namespace.of("ns"))
                        .build()),
                new MetadataUpdate.SetCurrentViewVersion(2)));
    requirements.forEach(req -> req.validate(viewMetadata));

    assertThat(requirements)
        .hasSize(1)
        .hasOnlyElementsOfTypes(UpdateRequirement.AssertViewUUID.class);

    assertViewUUID(requirements);
  }

  private void assertTableUUID(List<UpdateRequirement> requirements) {
    assertThat(requirements)
        .element(0)
        .asInstanceOf(InstanceOfAssertFactories.type(UpdateRequirement.AssertTableUUID.class))
        .extracting(UpdateRequirement.AssertTableUUID::uuid)
        .isEqualTo(metadata.uuid());
  }

  private void assertViewUUID(List<UpdateRequirement> requirements) {
    assertThat(requirements)
        .element(0)
        .asInstanceOf(InstanceOfAssertFactories.type(UpdateRequirement.AssertViewUUID.class))
        .extracting(UpdateRequirement.AssertViewUUID::uuid)
        .isEqualTo(viewMetadata.uuid());
  }
}
