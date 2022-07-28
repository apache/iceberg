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
package org.apache.iceberg.rest.requests;

import java.util.List;
import java.util.Set;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.rest.RESTRequest;

public class UpdateTableRequest implements RESTRequest {

  private List<UpdateRequirement> requirements;
  private List<MetadataUpdate> updates;

  public UpdateTableRequest() {
    // needed for Jackson deserialization
  }

  public UpdateTableRequest(List<UpdateRequirement> requirements, List<MetadataUpdate> updates) {
    this.requirements = requirements;
    this.updates = updates;
  }

  @Override
  public void validate() {}

  public List<UpdateRequirement> requirements() {
    return requirements != null ? requirements : ImmutableList.of();
  }

  public List<MetadataUpdate> updates() {
    return updates != null ? updates : ImmutableList.of();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("requirements", requirements)
        .add("updates", updates)
        .toString();
  }

  public static Builder builderForCreate() {
    return new Builder(null, false).requireCreate();
  }

  public static Builder builderForReplace(TableMetadata base) {
    Preconditions.checkNotNull(base, "Cannot create a builder from table metadata: null");
    return new Builder(base, true).requireTableUUID(base.uuid());
  }

  public static Builder builderFor(TableMetadata base) {
    Preconditions.checkNotNull(base, "Cannot create a builder from table metadata: null");
    return new Builder(base, false).requireTableUUID(base.uuid());
  }

  public static class Builder {
    private final TableMetadata base;
    private final ImmutableList.Builder<UpdateRequirement> requirements = ImmutableList.builder();
    private final List<MetadataUpdate> updates = Lists.newArrayList();
    private final Set<String> changedRefs = Sets.newHashSet();
    private final boolean isReplace;
    private boolean addedSchema = false;
    private boolean setSchemaId = false;
    private boolean addedSpec = false;
    private boolean setSpecId = false;
    private boolean setOrderId = false;

    public Builder(TableMetadata base, boolean isReplace) {
      this.base = base;
      this.isReplace = isReplace;
    }

    private Builder require(UpdateRequirement requirement) {
      Preconditions.checkArgument(requirement != null, "Invalid requirement: null");
      requirements.add(requirement);
      return this;
    }

    private Builder requireCreate() {
      return require(new UpdateRequirement.AssertTableDoesNotExist());
    }

    private Builder requireTableUUID(String uuid) {
      Preconditions.checkArgument(uuid != null, "Invalid required UUID: null");
      return require(new UpdateRequirement.AssertTableUUID(uuid));
    }

    private Builder requireRefSnapshotId(String ref, Long snapshotId) {
      return require(new UpdateRequirement.AssertRefSnapshotID(ref, snapshotId));
    }

    private Builder requireLastAssignedFieldId(int fieldId) {
      return require(new UpdateRequirement.AssertLastAssignedFieldId(fieldId));
    }

    private Builder requireCurrentSchemaId(int schemaId) {
      return require(new UpdateRequirement.AssertCurrentSchemaID(schemaId));
    }

    private Builder requireLastAssignedPartitionId(int partitionId) {
      return require(new UpdateRequirement.AssertLastAssignedPartitionId(partitionId));
    }

    private Builder requireDefaultSpecId(int specId) {
      return require(new UpdateRequirement.AssertDefaultSpecID(specId));
    }

    private Builder requireDefaultSortOrderId(int orderId) {
      return require(new UpdateRequirement.AssertDefaultSortOrderID(orderId));
    }

    public Builder update(MetadataUpdate update) {
      Preconditions.checkArgument(update != null, "Invalid update: null");
      updates.add(update);

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
        requireRefSnapshotId(name, baseRef != null ? baseRef.snapshotId() : null);
      }
    }

    private void update(MetadataUpdate.AddSchema update) {
      if (!addedSchema) {
        if (base != null) {
          requireLastAssignedFieldId(base.lastColumnId());
        }
        this.addedSchema = true;
      }
    }

    private void update(MetadataUpdate.SetCurrentSchema update) {
      if (!setSchemaId) {
        if (base != null && !isReplace) {
          // require that the current schema has not changed
          requireCurrentSchemaId(base.currentSchemaId());
        }
        this.setSchemaId = true;
      }
    }

    private void update(MetadataUpdate.AddPartitionSpec update) {
      if (!addedSpec) {
        if (base != null) {
          requireLastAssignedPartitionId(base.lastAssignedPartitionId());
        }
        this.addedSpec = true;
      }
    }

    private void update(MetadataUpdate.SetDefaultPartitionSpec update) {
      if (!setSpecId) {
        if (base != null && !isReplace) {
          // require that the default spec has not changed
          requireDefaultSpecId(base.defaultSpecId());
        }
        this.setSpecId = true;
      }
    }

    private void update(MetadataUpdate.SetDefaultSortOrder update) {
      if (!setOrderId) {
        if (base != null && !isReplace) {
          // require that the default write order has not changed
          requireDefaultSortOrderId(base.defaultSortOrderId());
        }
        this.setOrderId = true;
      }
    }

    public UpdateTableRequest build() {
      return new UpdateTableRequest(requirements.build(), ImmutableList.copyOf(updates));
    }
  }

  public interface UpdateRequirement {
    void validate(TableMetadata base);

    class AssertTableDoesNotExist implements UpdateRequirement {
      AssertTableDoesNotExist() {}

      @Override
      public void validate(TableMetadata base) {
        if (base != null) {
          throw new CommitFailedException("Requirement failed: table already exists");
        }
      }
    }

    class AssertTableUUID implements UpdateRequirement {
      private final String uuid;

      AssertTableUUID(String uuid) {
        this.uuid = uuid;
      }

      public String uuid() {
        return uuid;
      }

      @Override
      public void validate(TableMetadata base) {
        if (!uuid.equalsIgnoreCase(base.uuid())) {
          throw new CommitFailedException(
              "Requirement failed: UUID does not match: expected %s != %s", base.uuid(), uuid);
        }
      }
    }

    class AssertRefSnapshotID implements UpdateRequirement {
      private final String name;
      private final Long snapshotId;

      AssertRefSnapshotID(String name, Long snapshotId) {
        this.name = name;
        this.snapshotId = snapshotId;
      }

      public String refName() {
        return name;
      }

      public Long snapshotId() {
        return snapshotId;
      }

      @Override
      public void validate(TableMetadata base) {
        SnapshotRef ref = base.ref(name);
        if (ref != null) {
          String type = ref.isBranch() ? "branch" : "tag";
          if (snapshotId == null) {
            // a null snapshot ID means the ref should not exist already
            throw new CommitFailedException(
                "Requirement failed: %s %s was created concurrently", type, name);
          } else if (snapshotId != ref.snapshotId()) {
            throw new CommitFailedException(
                "Requirement failed: %s %s has changed: expected id %s != %s",
                type, name, snapshotId, ref.snapshotId());
          }
        } else if (snapshotId != null) {
          throw new CommitFailedException(
              "Requirement failed: branch or tag %s is missing, expected %s", name, snapshotId);
        }
      }
    }

    class AssertLastAssignedFieldId implements UpdateRequirement {
      private final int lastAssignedFieldId;

      public AssertLastAssignedFieldId(int lastAssignedFieldId) {
        this.lastAssignedFieldId = lastAssignedFieldId;
      }

      public int lastAssignedFieldId() {
        return lastAssignedFieldId;
      }

      @Override
      public void validate(TableMetadata base) {
        if (base != null && base.lastColumnId() != lastAssignedFieldId) {
          throw new CommitFailedException(
              "Requirement failed: last assigned field id changed: expected id %s != %s",
              lastAssignedFieldId, base.lastColumnId());
        }
      }
    }

    class AssertCurrentSchemaID implements UpdateRequirement {
      private final int schemaId;

      AssertCurrentSchemaID(int schemaId) {
        this.schemaId = schemaId;
      }

      public int schemaId() {
        return schemaId;
      }

      @Override
      public void validate(TableMetadata base) {
        if (schemaId != base.currentSchemaId()) {
          throw new CommitFailedException(
              "Requirement failed: current schema changed: expected id %s != %s",
              schemaId, base.currentSchemaId());
        }
      }
    }

    class AssertLastAssignedPartitionId implements UpdateRequirement {
      private final int lastAssignedPartitionId;

      public AssertLastAssignedPartitionId(int lastAssignedPartitionId) {
        this.lastAssignedPartitionId = lastAssignedPartitionId;
      }

      public int lastAssignedPartitionId() {
        return lastAssignedPartitionId;
      }

      @Override
      public void validate(TableMetadata base) {
        if (base != null && base.lastAssignedPartitionId() != lastAssignedPartitionId) {
          throw new CommitFailedException(
              "Requirement failed: last assigned partition id changed: expected id %s != %s",
              lastAssignedPartitionId, base.lastAssignedPartitionId());
        }
      }
    }

    class AssertDefaultSpecID implements UpdateRequirement {
      private final int specId;

      AssertDefaultSpecID(int specId) {
        this.specId = specId;
      }

      public int specId() {
        return specId;
      }

      @Override
      public void validate(TableMetadata base) {
        if (specId != base.defaultSpecId()) {
          throw new CommitFailedException(
              "Requirement failed: default partition spec changed: expected id %s != %s",
              specId, base.defaultSpecId());
        }
      }
    }

    class AssertDefaultSortOrderID implements UpdateRequirement {
      private final int sortOrderId;

      AssertDefaultSortOrderID(int sortOrderId) {
        this.sortOrderId = sortOrderId;
      }

      public int sortOrderId() {
        return sortOrderId;
      }

      @Override
      public void validate(TableMetadata base) {
        if (sortOrderId != base.defaultSortOrderId()) {
          throw new CommitFailedException(
              "Requirement failed: default sort order changed: expected id %s != %s",
              sortOrderId, base.defaultSortOrderId());
        }
      }
    }
  }
}
