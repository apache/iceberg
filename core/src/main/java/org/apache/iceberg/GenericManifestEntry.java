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
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.types.Types;

class GenericManifestEntry implements ManifestEntry, IndexedRecord, SpecificData.SchemaConstructable, StructLike {
  private final org.apache.avro.Schema schema;
  private final V1Metadata.IndexedDataFile fileWrapper;
  private Status status = Status.EXISTING;
  private Long snapshotId = null;
  private Long sequenceNumber = null;
  private DataFile file = null;

  GenericManifestEntry(org.apache.avro.Schema schema) {
    this.schema = schema;
    this.fileWrapper = null; // do not use the file wrapper to read
  }

  GenericManifestEntry(Types.StructType partitionType) {
    this.schema = AvroSchemaUtil.convert(V1Metadata.entrySchema(partitionType), "manifest_entry");
    this.fileWrapper = new V1Metadata.IndexedDataFile(schema.getField("data_file").schema());
  }

  private GenericManifestEntry(GenericManifestEntry toCopy, boolean fullCopy) {
    this.schema = toCopy.schema;
    this.fileWrapper = new V1Metadata.IndexedDataFile(schema.getField("data_file").schema());
    this.status = toCopy.status;
    this.snapshotId = toCopy.snapshotId;
    if (fullCopy) {
      this.file = toCopy.file().copy();
    } else {
      this.file = toCopy.file().copyWithoutStats();
    }
  }

  ManifestEntry wrapExisting(Long newSnapshotId, Long newSequenceNumber, DataFile newFile) {
    this.status = Status.EXISTING;
    this.snapshotId = newSnapshotId;
    this.sequenceNumber = newSequenceNumber;
    this.file = newFile;
    return this;
  }

  ManifestEntry wrapAppend(Long newSnapshotId, DataFile newFile) {
    this.status = Status.ADDED;
    this.snapshotId = newSnapshotId;
    this.sequenceNumber = null;
    this.file = newFile;
    return this;
  }

  ManifestEntry wrapDelete(Long newSnapshotId, DataFile newFile) {
    this.status = Status.DELETED;
    this.snapshotId = newSnapshotId;
    this.sequenceNumber = null;
    this.file = newFile;
    return this;
  }

  /**
   * @return the status of the file, whether EXISTING, ADDED, or DELETED
   */
  @Override
  public Status status() {
    return status;
  }

  /**
   * @return id of the snapshot in which the file was added to the table
   */
  @Override
  public Long snapshotId() {
    return snapshotId;
  }

  @Override
  public Long sequenceNumber() {
    return sequenceNumber;
  }

  /**
   * @return a file
   */
  @Override
  public DataFile file() {
    return file;
  }

  @Override
  public ManifestEntry copy() {
    return new GenericManifestEntry(this, true /* full copy */);
  }

  @Override
  public ManifestEntry copyWithoutStats() {
    return new GenericManifestEntry(this, false /* drop stats */);
  }

  @Override
  public void setSnapshotId(long newSnapshotId) {
    this.snapshotId = newSnapshotId;
  }

  @Override
  public void setSequenceNumber(long newSequenceNumber) {
    this.sequenceNumber = newSequenceNumber;
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
        this.sequenceNumber = (Long) v;
        return;
      case 3:
        this.file = (DataFile) v;
        return;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public <T> void set(int pos, T value) {
    put(pos, value);
  }

  @Override
  public Object get(int i) {
    switch (i) {
      case 0:
        return status.id();
      case 1:
        return snapshotId;
      case 2:
        return sequenceNumber;
      case 3:
        if (fileWrapper == null || file instanceof GenericDataFile) {
          return file;
        } else {
          return fileWrapper.wrap(file);
        }
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }

  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    return javaClass.cast(get(pos));
  }

  @Override
  public org.apache.avro.Schema getSchema() {
    return schema;
  }

  @Override
  public int size() {
    return 4;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("status", status)
        .add("snapshot_id", snapshotId)
        .add("sequence_number", sequenceNumber)
        .add("file", file)
        .toString();
  }
}
