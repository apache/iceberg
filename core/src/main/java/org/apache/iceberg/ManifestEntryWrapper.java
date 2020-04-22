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

/**
 * Wrapper used to replace ManifestEntry values when writing entries to a manifest.
 */
class ManifestEntryWrapper implements ManifestEntry {
  private DataFile wrapped = null;
  private Status status = null;
  private Long snapshotId = null;
  private Long sequenceNumber = null;

  ManifestEntry wrapExisting(Long newSnapshotId, Long newSequenceNumber, DataFile newFile) {
    this.status = Status.EXISTING;
    this.snapshotId = newSnapshotId;
    this.sequenceNumber = newSequenceNumber;
    this.wrapped = newFile;
    return this;
  }

  ManifestEntry wrapAppend(Long newSnapshotId, DataFile newFile) {
    this.status = Status.ADDED;
    this.snapshotId = newSnapshotId;
    this.sequenceNumber = null;
    this.wrapped = newFile;
    return this;
  }

  ManifestEntry wrapDelete(Long newSnapshotId, Long newSequenceNumber, DataFile newFile) {
    this.status = Status.DELETED;
    this.snapshotId = newSnapshotId;
    this.sequenceNumber = newSequenceNumber;
    this.wrapped = newFile;
    return this;
  }

  @Override
  public Status status() {
    return status;
  }

  @Override
  public Long snapshotId() {
    return snapshotId;
  }

  @Override
  public Long sequenceNumber() {
    return sequenceNumber;
  }

  @Override
  public DataFile file() {
    return wrapped;
  }

  @Override
  public void setSnapshotId(long snapshotId) {
    this.snapshotId = snapshotId;
  }

  @Override
  public void setSequenceNumber(long sequenceNumber) {
    this.sequenceNumber = sequenceNumber;
  }

  @Override
  public ManifestEntry copy() {
    throw new UnsupportedOperationException("Cannot copy a ManifestEntryWrapper");
  }

  @Override
  public ManifestEntry copyWithoutStats() {
    throw new UnsupportedOperationException("Cannot copy a ManifestEntryWrapper");
  }
}
