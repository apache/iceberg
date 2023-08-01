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
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

public class UpdateRequirements {

  private UpdateRequirements() {}

  public static List<UpdateRequirement> forCreateTable(List<MetadataUpdate> metadataUpdates) {
    Preconditions.checkArgument(null != metadataUpdates, "Invalid metadata updates: null");
    Builder builder = new Builder(null, false);
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

  private static class Builder {
    private final TableMetadata base;
    private final ImmutableList.Builder<UpdateRequirement> requirements = ImmutableList.builder();
    private final Set<String> changedRefs = Sets.newHashSet();
    private final boolean isReplace;
    private boolean addedSchema = false;
    private boolean setSchemaId = false;
    private boolean addedSpec = false;
    private boolean setSpecId = false;
    private boolean setOrderId = false;

    private Builder(TableMetadata base, boolean isReplace) {
      this.base = base;
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
      }

      return this;
    }

    private void update(MetadataUpdate.SetSnapshotRef setRef) {
      // require that the ref is unchanged from the base
      String name = setRef.name();
      // add returns true the first time the ref name is added
      boolean added = changedRefs.add(name);
      if (added && base != null && !isReplace) {
        SnapshotRef baseRef = base.ref(name);
        // require that the ref does not exist (null) or is the same as the base snapshot
        require(
            new UpdateRequirement.AssertRefSnapshotID(
                name, baseRef != null ? baseRef.snapshotId() : null));
      }
    }

    private void update(MetadataUpdate.AddSchema update) {
      if (!addedSchema) {
        if (base != null) {
          require(new UpdateRequirement.AssertLastAssignedFieldId(base.lastColumnId()));
        }
        this.addedSchema = true;
      }
    }

    private void update(MetadataUpdate.SetCurrentSchema update) {
      if (!setSchemaId) {
        if (base != null && !isReplace) {
          // require that the current schema has not changed
          require(new UpdateRequirement.AssertCurrentSchemaID(base.currentSchemaId()));
        }
        this.setSchemaId = true;
      }
    }

    private void update(MetadataUpdate.AddPartitionSpec update) {
      if (!addedSpec) {
        if (base != null) {
          require(
              new UpdateRequirement.AssertLastAssignedPartitionId(base.lastAssignedPartitionId()));
        }
        this.addedSpec = true;
      }
    }

    private void update(MetadataUpdate.SetDefaultPartitionSpec update) {
      if (!setSpecId) {
        if (base != null && !isReplace) {
          // require that the default spec has not changed
          require(new UpdateRequirement.AssertDefaultSpecID(base.defaultSpecId()));
        }
        this.setSpecId = true;
      }
    }

    private void update(MetadataUpdate.SetDefaultSortOrder update) {
      if (!setOrderId) {
        if (base != null && !isReplace) {
          // require that the default write order has not changed
          require(new UpdateRequirement.AssertDefaultSortOrderID(base.defaultSortOrderId()));
        }
        this.setOrderId = true;
      }
    }

    private List<UpdateRequirement> build() {
      return requirements.build();
    }
  }
}
