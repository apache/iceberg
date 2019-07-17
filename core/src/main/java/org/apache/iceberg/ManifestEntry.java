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

import com.google.common.base.MoreObjects;
import java.util.Collection;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.StructType;

import static org.apache.iceberg.types.Types.NestedField.required;

class ManifestEntry implements IndexedRecord, SpecificData.SchemaConstructable {
  enum Status {
    EXISTING(0),
    ADDED(1),
    DELETED(2);

    private final int id;

    Status(int id) {
      this.id = id;
    }

    public int id() {
      return id;
    }
  }

  private final org.apache.avro.Schema schema;
  private Status status = Status.EXISTING;
  private long snapshotId = 0L;
  private DataFile file = null;

  ManifestEntry(org.apache.avro.Schema schema) {
    this.schema = schema;
  }

  ManifestEntry(StructType partitionType) {
    this.schema = AvroSchemaUtil.convert(getSchema(partitionType), "manifest_entry");
  }

  private ManifestEntry(ManifestEntry toCopy, boolean fullCopy) {
    this.schema = toCopy.schema;
    this.status = toCopy.status;
    this.snapshotId = toCopy.snapshotId;
    if (fullCopy) {
      this.file = toCopy.file().copy();
    } else {
      this.file = toCopy.file().copyWithoutStats();
    }
  }

  ManifestEntry wrapExisting(long newSnapshotId, DataFile newFile) {
    this.status = Status.EXISTING;
    this.snapshotId = newSnapshotId;
    this.file = newFile;
    return this;
  }

  ManifestEntry wrapAppend(long newSnapshotId, DataFile newFile) {
    this.status = Status.ADDED;
    this.snapshotId = newSnapshotId;
    this.file = newFile;
    return this;
  }

  ManifestEntry wrapDelete(long newSnapshotId, DataFile newFile) {
    this.status = Status.DELETED;
    this.snapshotId = newSnapshotId;
    this.file = newFile;
    return this;
  }

  /**
   * @return the status of the file, whether EXISTING, ADDED, or DELETED
   */
  public Status status() {
    return status;
  }

  /**
   * @return id of the snapshot in which the file was added to the table
   */
  public long snapshotId() {
    return snapshotId;
  }

  /**
   * @return a file
   */
  public DataFile file() {
    return file;
  }

  public ManifestEntry copy() {
    return new ManifestEntry(this, true /* full copy */);
  }

  public ManifestEntry copyWithoutStats() {
    return new ManifestEntry(this, false /* drop stats */);
  }

  @Override
  public void put(int i, Object v) {
    switch (i) {
      case 0:
        this.status = Status.values()[(Integer) v];
        return;
      case 1:
        this.snapshotId = (Long) v;
        return;
      case 2:
        this.file = (DataFile) v;
        return;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public Object get(int i) {
    switch (i) {
      case 0:
        return status.id();
      case 1:
        return snapshotId;
      case 2:
        return file;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }

  @Override
  public org.apache.avro.Schema getSchema() {
    return schema;
  }

  static Schema getSchema(StructType partitionType) {
    return wrapFileSchema(DataFile.getType(partitionType));
  }

  static Schema projectSchema(StructType partitionType, Collection<String> columns) {
    return wrapFileSchema(
        new Schema(DataFile.getType(partitionType).fields()).select(columns).asStruct());
  }

  static Schema wrapFileSchema(StructType fileStruct) {
    // ids for top-level columns are assigned from 1000
    return new Schema(
        required(0, "status", IntegerType.get()),
        required(1, "snapshot_id", LongType.get()),
        required(2, "data_file", fileStruct));
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("status", status)
        .add("snapshot_id", snapshotId)
        .add("file", file)
        .toString();
  }
}
