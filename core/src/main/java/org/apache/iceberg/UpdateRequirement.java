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

import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.view.ViewMetadata;

/** Represents a requirement for a {@link MetadataUpdate} */
public interface UpdateRequirement {
  default void validate(TableMetadata base) {
    throw new ValidationException(
        "Cannot validate %s against a table", this.getClass().getSimpleName());
  }

  default void validate(ViewMetadata base) {
    throw new ValidationException(
        "Cannot validate %s against a view", this.getClass().getSimpleName());
  }

  class AssertTableDoesNotExist implements UpdateRequirement {
    public AssertTableDoesNotExist() {}

    @Override
    public void validate(TableMetadata base) {
      if (base != null) {
        throw new CommitFailedException("Requirement failed: table already exists");
      }
    }
  }

  class AssertTableUUID implements UpdateRequirement {
    private final String uuid;

    public AssertTableUUID(String uuid) {
      Preconditions.checkArgument(uuid != null, "Invalid required UUID: null");
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

  class AssertViewUUID implements UpdateRequirement {
    private final String uuid;

    public AssertViewUUID(String uuid) {
      Preconditions.checkArgument(uuid != null, "Invalid required UUID: null");
      this.uuid = uuid;
    }

    public String uuid() {
      return uuid;
    }

    @Override
    public void validate(ViewMetadata base) {
      if (!uuid.equalsIgnoreCase(base.uuid())) {
        throw new CommitFailedException(
            "Requirement failed: view UUID does not match: expected %s != %s", base.uuid(), uuid);
      }
    }
  }

  class AssertRefSnapshotID implements UpdateRequirement {
    private final String name;
    private final Long snapshotId;

    public AssertRefSnapshotID(String name, Long snapshotId) {
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

    public AssertCurrentSchemaID(int schemaId) {
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

    public AssertDefaultSpecID(int specId) {
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

    public AssertDefaultSortOrderID(int sortOrderId) {
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
