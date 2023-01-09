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

import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.types.Types;

class GenericManifestEntry<F extends ContentFile<F>>
    implements ManifestEntry<F>, IndexedRecord, SpecificData.SchemaConstructable, StructLike {
  private final org.apache.avro.Schema schema;
  private Status status = Status.EXISTING;
  private Long snapshotId = null;
  private Long dataSequenceNumber = null;
  private Long fileSequenceNumber = null;
  private F file = null;

  GenericManifestEntry(org.apache.avro.Schema schema) {
    this.schema = schema;
  }

  GenericManifestEntry(Types.StructType partitionType) {
    this.schema = AvroSchemaUtil.convert(V1Metadata.entrySchema(partitionType), "manifest_entry");
  }

  private GenericManifestEntry(GenericManifestEntry<F> toCopy, boolean fullCopy) {
    this.schema = toCopy.schema;
    this.status = toCopy.status;
    this.snapshotId = toCopy.snapshotId;
    this.dataSequenceNumber = toCopy.dataSequenceNumber;
    this.fileSequenceNumber = toCopy.fileSequenceNumber;
    this.file = toCopy.file().copy(fullCopy);
  }

  ManifestEntry<F> wrapExisting(ManifestEntry<F> entry) {
    return wrapExisting(
        entry.snapshotId(), entry.dataSequenceNumber(), entry.fileSequenceNumber(), entry.file());
  }

  ManifestEntry<F> wrapExisting(
      Long newSnapshotId, Long newDataSequenceNumber, Long newFileSequenceNumber, F newFile) {
    this.status = Status.EXISTING;
    this.snapshotId = newSnapshotId;
    this.dataSequenceNumber = newDataSequenceNumber;
    this.fileSequenceNumber = newFileSequenceNumber;
    this.file = newFile;
    return this;
  }

  ManifestEntry<F> wrapAppend(Long newSnapshotId, F newFile) {
    return wrapAppend(newSnapshotId, null, newFile);
  }

  ManifestEntry<F> wrapAppend(Long newSnapshotId, Long newDataSequenceNumber, F newFile) {
    this.status = Status.ADDED;
    this.snapshotId = newSnapshotId;
    this.dataSequenceNumber = newDataSequenceNumber;
    this.fileSequenceNumber = null;
    this.file = newFile;
    return this;
  }

  ManifestEntry<F> wrapDelete(Long newSnapshotId, ManifestEntry<F> entry) {
    return wrapDelete(
        newSnapshotId, entry.dataSequenceNumber(), entry.fileSequenceNumber(), entry.file());
  }

  ManifestEntry<F> wrapDelete(
      Long newSnapshotId, Long newDataSequenceNumber, Long newFileSequenceNumber, F newFile) {
    this.status = Status.DELETED;
    this.snapshotId = newSnapshotId;
    this.dataSequenceNumber = newDataSequenceNumber;
    this.fileSequenceNumber = newFileSequenceNumber;
    this.file = newFile;
    return this;
  }

  /** @return the status of the file, whether EXISTING, ADDED, or DELETED */
  @Override
  public Status status() {
    return status;
  }

  /** @return id of the snapshot in which the file was added to the table */
  @Override
  public Long snapshotId() {
    return snapshotId;
  }

  @Override
  public Long dataSequenceNumber() {
    return dataSequenceNumber;
  }

  @Override
  public Long fileSequenceNumber() {
    return fileSequenceNumber;
  }

  /** @return a file */
  @Override
  public F file() {
    return file;
  }

  @Override
  public ManifestEntry<F> copy() {
    return new GenericManifestEntry<>(this, true /* full copy */);
  }

  @Override
  public ManifestEntry<F> copyWithoutStats() {
    return new GenericManifestEntry<>(this, false /* drop stats */);
  }

  @Override
  public void setSnapshotId(long newSnapshotId) {
    this.snapshotId = newSnapshotId;
  }

  @Override
  public void setDataSequenceNumber(long newDataSequenceNumber) {
    this.dataSequenceNumber = newDataSequenceNumber;
  }

  @Override
  public void setFileSequenceNumber(long newFileSequenceNumber) {
    this.fileSequenceNumber = newFileSequenceNumber;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void put(int i, Object v) {
    switch (i) {
      case 0:
        this.status = Status.values()[(Integer) v];
        return;
      case 1:
        this.snapshotId = (Long) v;
        return;
      case 2:
        this.dataSequenceNumber = (Long) v;
        return;
      case 3:
        this.fileSequenceNumber = (Long) v;
        return;
      case 4:
        this.file = (F) v;
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
        return dataSequenceNumber;
      case 3:
        return fileSequenceNumber;
      case 4:
        return file;
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
    return 5;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("status", status)
        .add("snapshot_id", snapshotId)
        .add("data_sequence_number", dataSequenceNumber)
        .add("file_sequence_number", fileSequenceNumber)
        .add("file", file)
        .toString();
  }
}
