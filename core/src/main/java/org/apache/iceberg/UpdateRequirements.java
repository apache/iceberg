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

import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.view.ViewMetadata;

public class UpdateRequirements {

  private UpdateRequirements() {}

  public static List<UpdateRequirement> forCreateTable(List<MetadataUpdate> metadataUpdates) {
    Preconditions.checkArgument(null != metadataUpdates, "Invalid metadata updates: null");
    Builder builder = new Builder((TableMetadata) null, false);
    builder.require(new UpdateRequirement.AssertTableDoesNotExist());
    metadataUpdates.forEach(builder::update);
    return builder.build();
  }

  public static List<UpdateRequirement> forReplaceTable(
      TableMetadata base, List<MetadataUpdate> metadataUpdates) {
    Preconditions.checkArgument(null != base, "Invalid table metadata: null");
    Preconditions.checkArgument(null != metadataUpdates, "Invalid metadata updates: null");
    Builder builder = new Builder(base, true);
    builder.require(new UpdateRequirement.AssertTableUUID(base.uuid()));
    metadataUpdates.forEach(builder::update);
    return builder.build();
  }

  public static List<UpdateRequirement> forUpdateTable(
      TableMetadata base, List<MetadataUpdate> metadataUpdates) {
    Preconditions.checkArgument(null != base, "Invalid table metadata: null");
    Preconditions.checkArgument(null != metadataUpdates, "Invalid metadata updates: null");
    Builder builder = new Builder(base, false);
    builder.require(new UpdateRequirement.AssertTableUUID(base.uuid()));
    metadataUpdates.forEach(builder::update);
    return builder.build();
  }

  public static List<UpdateRequirement> forReplaceView(
      ViewMetadata base, List<MetadataUpdate> metadataUpdates) {
    Preconditions.checkArgument(null != base, "Invalid view metadata: null");
    Preconditions.checkArgument(null != metadataUpdates, "Invalid metadata updates: null");
    Builder builder = new Builder(base, false);
    builder.require(new UpdateRequirement.AssertViewUUID(base.uuid()));
    metadataUpdates.forEach(builder::update);
    return builder.build();
  }

  private static class Builder {
    private final TableMetadata baseTable;
    private final ViewMetadata baseView;
    private final ImmutableList.Builder<UpdateRequirement> requirements = ImmutableList.builder();
    private final Set<String> changedRefs = Sets.newHashSet();
    private final boolean isReplace;
    private boolean addedSchema = false;
    private boolean setSchemaId = false;
    private boolean addedSpec = false;
    private boolean setSpecId = false;
    private boolean setOrderId = false;

    private Builder(TableMetadata baseTable, boolean isReplace) {
      this.baseTable = baseTable;
      this.baseView = null;
      this.isReplace = isReplace;
    }

    private Builder(ViewMetadata baseView, boolean isReplace) {
      this.baseTable = null;
      this.baseView = baseView;
      this.isReplace = isReplace;
    }

    private Builder require(UpdateRequirement requirement) {
      Preconditions.checkArgument(requirement != null, "Invalid requirement: null");
      requirements.add(requirement);
      return this;
    }

    private Builder update(MetadataUpdate update) {
      Preconditions.checkArgument(update != null, "Invalid update: null");

      // add requirements based on the change
      if (update instanceof MetadataUpdate.SetSnapshotRef) {
        update((MetadataUpdate.SetSnapshotRef) update);
      } else if (update instanceof MetadataUpdate.AddSchema) {
        update((MetadataUpdate.AddSchema) update);
      } else if (update instanceof MetadataUpdate.SetCurrentSchema) {
        update((MetadataUpdate.SetCurrentSchema) update);
      } else if (update instanceof MetadataUpdate.AddPartitionSpec) {
        update((MetadataUpdate.AddPartitionSpec) update);
      } else if (update instanceof MetadataUpdate.SetDefaultPartitionSpec) {
        update((MetadataUpdate.SetDefaultPartitionSpec) update);
      } else if (update instanceof MetadataUpdate.SetDefaultSortOrder) {
        update((MetadataUpdate.SetDefaultSortOrder) update);
      } else if (update instanceof MetadataUpdate.RemovePartitionSpecs) {
        update((MetadataUpdate.RemovePartitionSpecs) update);
      } else if (update instanceof MetadataUpdate.RemoveSchemas) {
        update((MetadataUpdate.RemoveSchemas) update);
      }
      // the above handles requirement for table updates
      // the below handles requirement for view updates
      else if (update instanceof MetadataUpdate.AddViewVersion) {
        update((MetadataUpdate.AddViewVersion) update);
      } else if (update instanceof MetadataUpdate.SetCurrentViewVersion) {
        update((MetadataUpdate.SetCurrentViewVersion) update);
      }

      return this;
    }

    private void update(MetadataUpdate.SetSnapshotRef setRef) {
      // require that the ref is unchanged from the base
      String name = setRef.name();
      // add returns true the first time the ref name is added
      boolean added = changedRefs.add(name);
      if (added && baseTable != null && !isReplace) {
        SnapshotRef baseRef = baseTable.ref(name);
        // require that the ref does not exist (null) or is the same as the base snapshot
        require(
            new UpdateRequirement.AssertRefSnapshotID(
                name, baseRef != null ? baseRef.snapshotId() : null));
      }
    }

    private void update(MetadataUpdate.AddSchema unused) {
      if (!addedSchema) {
        if (baseTable != null) {
          require(new UpdateRequirement.AssertLastAssignedFieldId(baseTable.lastColumnId()));
        }
        this.addedSchema = true;
      }
    }

    private void update(MetadataUpdate.SetCurrentSchema unused) {
      requireCurrentSchemaNotChanged();
    }

    private void update(MetadataUpdate.AddPartitionSpec unused) {
      if (!addedSpec) {
        if (baseTable != null) {
          require(
              new UpdateRequirement.AssertLastAssignedPartitionId(baseTable.lastAssignedPartitionId()));
        }
        this.addedSpec = true;
      }
    }

    private void update(MetadataUpdate.SetDefaultPartitionSpec unused) {
      requireDefaultPartitionSpecNotChanged();
    }

    private void update(MetadataUpdate.SetDefaultSortOrder unused) {
      if (!setOrderId) {
        if (baseTable != null && !isReplace) {
          // require that the default write order has not changed
          require(new UpdateRequirement.AssertDefaultSortOrderID(baseTable.defaultSortOrderId()));
        }
        this.setOrderId = true;
      }
    }

    private void update(MetadataUpdate.RemovePartitionSpecs unused) {
      requireDefaultPartitionSpecNotChanged();

      // require that no branches have changed, so that old specs won't be written.
      requireNoBranchesChanged();
    }

    private void update(MetadataUpdate.RemoveSchemas unused) {
      requireCurrentSchemaNotChanged();

      // require that no branches have changed, so that old schemas won't be written.
      requireNoBranchesChanged();
    }

    private void update(MetadataUpdate.AddViewVersion unused) {
      Preconditions.checkArgument(baseView != null, "Base view metadata is required");

      baseView.versionsById().keySet().stream().max(Integer::compareTo)
          .ifPresent(viewVersion ->
              require(new UpdateRequirement.AssertLastAssignedViewVersionID(viewVersion)));
    }

    private void update(MetadataUpdate.SetCurrentViewVersion unused) {
      Preconditions.checkArgument(baseView != null, "Base view metadata is required");

      require(new UpdateRequirement.AssertCurrentViewVersionID(baseView.currentVersionId()));
    }

    private void requireDefaultPartitionSpecNotChanged() {
      if (!setSpecId) {
        if (baseTable != null && !isReplace) {
          require(new UpdateRequirement.AssertDefaultSpecID(baseTable.defaultSpecId()));
        }
        this.setSpecId = true;
      }
    }

    private void requireCurrentSchemaNotChanged() {
      if (!setSchemaId) {
        if (baseTable != null && !isReplace) {
          require(new UpdateRequirement.AssertCurrentSchemaID(baseTable.currentSchemaId()));
        }
        this.setSchemaId = true;
      }
    }

    private void requireNoBranchesChanged() {
      if (baseTable != null && !isReplace) {
        baseTable.refs()
            .forEach(
                (name, ref) -> {
                  if (ref.isBranch() && !name.equals(SnapshotRef.MAIN_BRANCH)) {
                    require(new UpdateRequirement.AssertRefSnapshotID(name, ref.snapshotId()));
                  }
                });
      }
    }

    private List<UpdateRequirement> build() {
      return requirements.build();
    }
  }
}
