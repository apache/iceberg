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

import java.io.Serializable;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewVersion;

/** Represents a change to table or view metadata. */
public interface MetadataUpdate extends Serializable {
  default void applyTo(TableMetadata.Builder metadataBuilder) {
    throw new UnsupportedOperationException(
        String.format("Cannot apply update %s to a table", this.getClass().getSimpleName()));
  }

  default void applyTo(ViewMetadata.Builder viewMetadataBuilder) {
    throw new UnsupportedOperationException(
        String.format("Cannot apply update %s to a view", this.getClass().getSimpleName()));
  }

  class AssignUUID implements MetadataUpdate {
    private final String uuid;

    public AssignUUID(String uuid) {
      this.uuid = uuid;
    }

    public String uuid() {
      return uuid;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.assignUUID(uuid);
    }

    @Override
    public void applyTo(ViewMetadata.Builder metadataBuilder) {
      metadataBuilder.assignUUID(uuid);
    }
  }

  class UpgradeFormatVersion implements MetadataUpdate {
    private final int formatVersion;

    public UpgradeFormatVersion(int formatVersion) {
      this.formatVersion = formatVersion;
    }

    public int formatVersion() {
      return formatVersion;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.upgradeFormatVersion(formatVersion);
    }

    @Override
    public void applyTo(ViewMetadata.Builder viewMetadataBuilder) {
      viewMetadataBuilder.upgradeFormatVersion(formatVersion);
    }
  }

  class AddSchema implements MetadataUpdate {
    private final Schema schema;
    private final int lastColumnId;

    public AddSchema(Schema schema, int lastColumnId) {
      this.schema = schema;
      this.lastColumnId = lastColumnId;
    }

    public Schema schema() {
      return schema;
    }

    public int lastColumnId() {
      return lastColumnId;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.addSchema(schema, lastColumnId);
    }

    @Override
    public void applyTo(ViewMetadata.Builder viewMetadataBuilder) {
      viewMetadataBuilder.addSchema(schema);
    }
  }

  class SetCurrentSchema implements MetadataUpdate {
    private final int schemaId;

    public SetCurrentSchema(int schemaId) {
      this.schemaId = schemaId;
    }

    public int schemaId() {
      return schemaId;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.setCurrentSchema(schemaId);
    }
  }

  class AddPartitionSpec implements MetadataUpdate {
    private final UnboundPartitionSpec spec;

    public AddPartitionSpec(PartitionSpec spec) {
      this(spec.toUnbound());
    }

    public AddPartitionSpec(UnboundPartitionSpec spec) {
      this.spec = spec;
    }

    public UnboundPartitionSpec spec() {
      return spec;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.addPartitionSpec(spec);
    }
  }

  class SetDefaultPartitionSpec implements MetadataUpdate {
    private final int specId;

    public SetDefaultPartitionSpec(int specId) {
      this.specId = specId;
    }

    public int specId() {
      return specId;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.setDefaultPartitionSpec(specId);
    }
  }

  class AddSortOrder implements MetadataUpdate {
    private final UnboundSortOrder sortOrder;

    public AddSortOrder(SortOrder sortOrder) {
      this(sortOrder.toUnbound());
    }

    public AddSortOrder(UnboundSortOrder sortOrder) {
      this.sortOrder = sortOrder;
    }

    public UnboundSortOrder sortOrder() {
      return sortOrder;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.addSortOrder(sortOrder);
    }
  }

  class SetDefaultSortOrder implements MetadataUpdate {
    private final int sortOrderId;

    public SetDefaultSortOrder(int sortOrderId) {
      this.sortOrderId = sortOrderId;
    }

    public int sortOrderId() {
      return sortOrderId;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.setDefaultSortOrder(sortOrderId);
    }
  }

  class SetStatistics implements MetadataUpdate {
    private final long snapshotId;
    private final StatisticsFile statisticsFile;

    public SetStatistics(long snapshotId, StatisticsFile statisticsFile) {
      this.snapshotId = snapshotId;
      this.statisticsFile = statisticsFile;
    }

    public long snapshotId() {
      return snapshotId;
    }

    public StatisticsFile statisticsFile() {
      return statisticsFile;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.setStatistics(snapshotId, statisticsFile);
    }
  }

  class RemoveStatistics implements MetadataUpdate {
    private final long snapshotId;

    public RemoveStatistics(long snapshotId) {
      this.snapshotId = snapshotId;
    }

    public long snapshotId() {
      return snapshotId;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.removeStatistics(snapshotId);
    }
  }

  class AddSnapshot implements MetadataUpdate {
    private final Snapshot snapshot;

    public AddSnapshot(Snapshot snapshot) {
      this.snapshot = snapshot;
    }

    public Snapshot snapshot() {
      return snapshot;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.addSnapshot(snapshot);
    }
  }

  class RemoveSnapshot implements MetadataUpdate {
    private final long snapshotId;

    public RemoveSnapshot(long snapshotId) {
      this.snapshotId = snapshotId;
    }

    public long snapshotId() {
      return snapshotId;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.removeSnapshots(ImmutableSet.of(snapshotId));
    }
  }

  class RemoveSnapshotRef implements MetadataUpdate {
    private final String refName;

    public RemoveSnapshotRef(String refName) {
      this.refName = refName;
    }

    public String name() {
      return refName;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.removeRef(refName);
    }
  }

  class SetSnapshotRef implements MetadataUpdate {
    private final String refName;
    private final Long snapshotId;
    private final SnapshotRefType type;
    private Integer minSnapshotsToKeep;
    private Long maxSnapshotAgeMs;
    private Long maxRefAgeMs;

    public SetSnapshotRef(
        String refName,
        Long snapshotId,
        SnapshotRefType type,
        Integer minSnapshotsToKeep,
        Long maxSnapshotAgeMs,
        Long maxRefAgeMs) {
      this.refName = refName;
      this.snapshotId = snapshotId;
      this.type = type;
      this.minSnapshotsToKeep = minSnapshotsToKeep;
      this.maxSnapshotAgeMs = maxSnapshotAgeMs;
      this.maxRefAgeMs = maxRefAgeMs;
    }

    public String name() {
      return refName;
    }

    public String type() {
      return type.name().toLowerCase(Locale.ROOT);
    }

    public long snapshotId() {
      return snapshotId;
    }

    public Integer minSnapshotsToKeep() {
      return minSnapshotsToKeep;
    }

    public Long maxSnapshotAgeMs() {
      return maxSnapshotAgeMs;
    }

    public Long maxRefAgeMs() {
      return maxRefAgeMs;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      SnapshotRef ref =
          SnapshotRef.builderFor(snapshotId, type)
              .minSnapshotsToKeep(minSnapshotsToKeep)
              .maxSnapshotAgeMs(maxSnapshotAgeMs)
              .maxRefAgeMs(maxRefAgeMs)
              .build();
      metadataBuilder.setRef(refName, ref);
    }
  }

  class SetProperties implements MetadataUpdate {
    private final Map<String, String> updated;

    public SetProperties(Map<String, String> updated) {
      this.updated = updated;
    }

    public Map<String, String> updated() {
      return updated;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.setProperties(updated);
    }

    @Override
    public void applyTo(ViewMetadata.Builder viewMetadataBuilder) {
      viewMetadataBuilder.setProperties(updated);
    }
  }

  class RemoveProperties implements MetadataUpdate {
    private final Set<String> removed;

    public RemoveProperties(Set<String> removed) {
      this.removed = removed;
    }

    public Set<String> removed() {
      return removed;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.removeProperties(removed);
    }

    @Override
    public void applyTo(ViewMetadata.Builder viewMetadataBuilder) {
      viewMetadataBuilder.removeProperties(removed);
    }
  }

  class SetLocation implements MetadataUpdate {
    private final String location;

    public SetLocation(String location) {
      this.location = location;
    }

    public String location() {
      return location;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.setLocation(location);
    }

    @Override
    public void applyTo(ViewMetadata.Builder viewMetadataBuilder) {
      viewMetadataBuilder.setLocation(location);
    }
  }

  class AddViewVersion implements MetadataUpdate {
    private final ViewVersion viewVersion;

    public AddViewVersion(ViewVersion viewVersion) {
      this.viewVersion = viewVersion;
    }

    public ViewVersion viewVersion() {
      return viewVersion;
    }

    @Override
    public void applyTo(ViewMetadata.Builder viewMetadataBuilder) {
      viewMetadataBuilder.addVersion(viewVersion);
    }
  }

  class SetCurrentViewVersion implements MetadataUpdate {
    private final int versionId;

    public SetCurrentViewVersion(int versionId) {
      this.versionId = versionId;
    }

    public int versionId() {
      return versionId;
    }

    @Override
    public void applyTo(ViewMetadata.Builder viewMetadataBuilder) {
      viewMetadataBuilder.setCurrentVersionId(versionId);
    }
  }

  class AppendFilesUpdate implements MetadataUpdate {
    private final List<String> addedManifests;

    public AppendFilesUpdate(List<String> addedManifests) {
      this.addedManifests = addedManifests;
    }

    public List<String> getAddedManifests() {
      return addedManifests;
    }

    @Override
    public void applyTo(TableMetadata.Builder tableMetadataBuilder) {
      tableMetadataBuilder.appendFiles(addedManifests);
    }
  }

  class DeleteFilesUpdate implements MetadataUpdate {
    private List<String> deletedManifests;
    private Expression deleteExpression;
    private boolean caseSensitive;

    public DeleteFilesUpdate() {}

    public void setDeletedManifests(List<String> deletedManifests) {
      this.deletedManifests = deletedManifests;
    }

    public List<String> getDeletedManifests() {
      return deletedManifests;
    }

    public void setDeleteExpression(Expression expression) {
      this.deleteExpression = expression;
    }

    public Expression getDeleteExpression() {
      return deleteExpression;
    }

    public void setCaseSensitive(boolean caseSensitive) {
      this.caseSensitive = caseSensitive;
    }

    public boolean isCaseSensitive() {
      return caseSensitive;
    }

    @Override
    public void applyTo(TableMetadata.Builder tableMetadataBuilder) {
      tableMetadataBuilder.deleteFiles(deletedManifests, deleteExpression, caseSensitive);
    }
  }

  class OverwriteFilesUpdate implements MetadataUpdate {
    private List<String> addedManifests;
    private List<String> deletedManifests;
    private Expression overwriteByRowFilterExpression;
    private Expression conflictExpression;
    private boolean caseSensitive;

    public OverwriteFilesUpdate() {}

    public void setAddedManifests(List<String> addedManifests) {
      this.addedManifests = addedManifests;
    }

    public List<String> getAddedManifests() {
      return addedManifests;
    }

    public void setDeletedManifests(List<String> deletedManifests) {
      this.deletedManifests = deletedManifests;
    }

    public List<String> getDeletedManifests() {
      return deletedManifests;
    }

    public void setOverwriteByRowFilterExpression(Expression overwriteByRowFilterExpression) {
      this.overwriteByRowFilterExpression = overwriteByRowFilterExpression;
    }

    public Expression getOverwriteByRowFilterExpression() {
      return overwriteByRowFilterExpression;
    }

    public void setConflictExpression(Expression conflictExpression) {
      this.conflictExpression = conflictExpression;
    }

    public Expression getConflictExpression() {
      return conflictExpression;
    }

    public void setCaseSensitive(boolean caseSensitive) {
      this.caseSensitive = caseSensitive;
    }

    public boolean isCaseSensitive() {
      return caseSensitive;
    }

    @Override
    public void applyTo(TableMetadata.Builder tableMetadataBuilder) {
      tableMetadataBuilder.overwriteFiles(
          addedManifests,
          deletedManifests,
          overwriteByRowFilterExpression,
          conflictExpression,
          caseSensitive);
    }
  }
}
