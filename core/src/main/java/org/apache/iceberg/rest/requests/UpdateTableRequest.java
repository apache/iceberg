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
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.compress.utils.Sets;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

public class UpdateTableRequest {

  private List<UpdateRequirement> requirements;
  private List<MetadataUpdate> updates;

  public UpdateTableRequest() {
    // needed for Jackson deserialization
  }

  public UpdateTableRequest(List<UpdateRequirement> requirements, List<MetadataUpdate> updates) {
    this.requirements = requirements;
    this.updates = updates;
  }

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

  public static Builder builderFor(TableMetadata base) {
    return new Builder(base);
  }

  public static class Builder {
    private final TableMetadata base;
    private final ImmutableList.Builder<UpdateRequirement> requirements = ImmutableList.builder();
    private final List<MetadataUpdate> updates = Lists.newArrayList();
    private final Set<String> changedRefs = Sets.newHashSet();
    private boolean changedSchema = false;
    private boolean changedSpec = false;
    private boolean changedOrder = false;

    public Builder(TableMetadata base) {
      this.base = base;
      requireTableUUID(base.uuid());
    }


    private Builder require(UpdateRequirement requirement) {
      Preconditions.checkArgument(requirement != null, "Invalid requirement: null");
      requirements.add(requirement);
      return this;
    }

    private Builder requireTableUUID(String uuid) {
      Preconditions.checkArgument(uuid != null, "Invalid required UUID: null");
      return require(new UpdateRequirement.AssertTableUUID(uuid));
    }

    private Builder requireRefSnapshotId(String ref, Long snapshotId) {
      return require(new UpdateRequirement.AssertRefSnapshotID(ref, snapshotId));
    }

    private Builder requireCurrentSchemaId(int schemaId) {
      return require(new UpdateRequirement.AssertCurrentSchemaID(schemaId));
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
        // require that the ref is unchanged from the base
        MetadataUpdate.SetSnapshotRef setRef = (MetadataUpdate.SetSnapshotRef) update;
        String name = setRef.name();
        // add returns true the first time the ref name is added
        if (changedRefs.add(name)) {
          SnapshotRef baseRef = base.ref(name);
          // require that the ref does not exist (null) or is the same as the base snapshot
          requireRefSnapshotId(name, baseRef != null ? baseRef.snapshotId() : null);
        }

      } else if (update instanceof MetadataUpdate.SetCurrentSchema) {
        if (!changedSchema) {
          // require that the current schema has not changed
          requireCurrentSchemaId(base.currentSchemaId());
          changedSchema = true;
        }

      } else if (update instanceof MetadataUpdate.SetDefaultPartitionSpec) {
        if (!changedSpec) {
          // require that the default spec has not changed
          requireDefaultSpecId(base.defaultSpecId());
          changedSpec = true;
        }

      } else if (update instanceof MetadataUpdate.SetDefaultSortOrder) {
        if (!changedOrder) {
          // require that the default write order has not changed
          requireDefaultSortOrderId(base.defaultSortOrderId());
          changedOrder = true;
        }
      }

      return this;
    }

    public UpdateTableRequest build() {
      return new UpdateTableRequest(requirements.build(), ImmutableList.copyOf(updates));
    }
  }

  public interface UpdateRequirement {
    void validate(TableMetadata base);

    class AssertTableUUID implements UpdateRequirement {
      private final String uuid;

      private AssertTableUUID(String uuid) {
        this.uuid = uuid;
      }

      public String uuid() {
        return uuid;
      }

      @Override
      public void validate(TableMetadata base) {
        if (!uuid.equalsIgnoreCase(base.uuid())) {
          throw new CommitFailedException("Requirement failed: UUID does not match (%s != %s)", base.uuid(), uuid);
        }
      }
    }

    class AssertRefSnapshotID implements UpdateRequirement {
      private final String name;
      private final Long snapshotId;

      private AssertRefSnapshotID(String name, Long snapshotId) {
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
            throw new CommitFailedException("Requirement failed: %s %s was created concurrently", type, name);
          } else if (snapshotId != ref.snapshotId()) {
            throw new CommitFailedException(
                "Requirement failed: %s %s has changed (%s != %s)", type, name, snapshotId, ref.snapshotId());
          }
        } else if (snapshotId != null) {
          throw new CommitFailedException(
              "Requirement failed: branch or tag %s is missing, expected %s", name, snapshotId);
        }
      }
    }

    class AssertCurrentSchemaID implements UpdateRequirement {
      private final int schemaId;

      private AssertCurrentSchemaID(int schemaId) {
        this.schemaId = schemaId;
      }

      public int schemaId() {
        return schemaId;
      }

      @Override
      public void validate(TableMetadata base) {
        if (schemaId != base.currentSchemaId()) {
          throw new CommitFailedException(
              "Requirement failed: current schema changed (id %s != %s)", schemaId, base.currentSchemaId());
        }
      }
    }

    class AssertDefaultSpecID implements UpdateRequirement {
      private final int specId;

      private AssertDefaultSpecID(int specId) {
        this.specId = specId;
      }

      public int specId() {
        return specId;
      }

      @Override
      public void validate(TableMetadata base) {
        if (specId != base.defaultSpecId()) {
          throw new CommitFailedException(
              "Requirement failed: default partition spec changed (id %s != %s)", specId, base.defaultSpecId());
        }
      }
    }

    class AssertDefaultSortOrderID implements UpdateRequirement {
      private final int sortOrderId;

      private AssertDefaultSortOrderID(int sortOrderId) {
        this.sortOrderId = sortOrderId;
      }

      public int sortOrderId() {
        return sortOrderId;
      }

      @Override
      public void validate(TableMetadata base) {
        if (sortOrderId != base.defaultSortOrderId()) {
          throw new CommitFailedException(
              "Requirement failed: default sort order changed (id %s != %s)", sortOrderId, base.defaultSortOrderId());
        }
      }
    }
  }
}
