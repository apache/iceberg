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
import java.util.Map;
import java.util.Set;

/**
 * Represents a change to table metadata.
 */
public interface MetadataUpdate extends Serializable {
  class AssignUUID implements MetadataUpdate {
    private final String uuid;

    public AssignUUID(String uuid) {
      this.uuid = uuid;
    }

    public String uuid() {
      return uuid;
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
  }

  class SetCurrentSchema implements MetadataUpdate {
    private final int schemaId;

    public SetCurrentSchema(int schemaId) {
      this.schemaId = schemaId;
    }

    public int schemaId() {
      return schemaId;
    }
  }

  class AddPartitionSpec implements MetadataUpdate {
    private final PartitionSpec spec;

    public AddPartitionSpec(PartitionSpec spec) {
      this.spec = spec;
    }

    public PartitionSpec spec() {
      return spec;
    }
  }

  class SetDefaultPartitionSpec implements MetadataUpdate {
    private final int specId;

    public SetDefaultPartitionSpec(int schemaId) {
      this.specId = schemaId;
    }

    public int specId() {
      return specId;
    }
  }

  class AddSortOrder implements MetadataUpdate {
    private final SortOrder sortOrder;

    public AddSortOrder(SortOrder sortOrder) {
      this.sortOrder = sortOrder;
    }

    public SortOrder spec() {
      return sortOrder;
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
  }

  class AddSnapshot implements MetadataUpdate {
    private final Snapshot snapshot;

    public AddSnapshot(Snapshot snapshot) {
      this.snapshot = snapshot;
    }

    public Snapshot snapshot() {
      return snapshot;
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
  }

  class SetCurrentSnapshot implements MetadataUpdate {
    private final Long snapshotId;

    public SetCurrentSnapshot(Long snapshotId) {
      this.snapshotId = snapshotId;
    }

    public Long snapshotId() {
      return snapshotId;
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
  }

  class RemoveProperties implements MetadataUpdate {
    private final Set<String> removed;

    public RemoveProperties(Set<String> removed) {
      this.removed = removed;
    }

    public Set<String> removed() {
      return removed;
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
  }
}
