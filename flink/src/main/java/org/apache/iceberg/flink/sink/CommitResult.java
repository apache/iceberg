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

package org.apache.iceberg.flink.sink;

import java.io.Serializable;
import java.util.Collection;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.WriteResult;

class CommitResult implements Serializable {

  private final long snapshotId;
  private final long sequenceNumber;
  private final int specId;
  private final StructLike partition;
  private final WriteResult writeResult;

  private CommitResult(long snapshotId,
                       long sequenceNumber,
                       int specId,
                       StructLike partition,
                       WriteResult writeResult) {
    this.snapshotId = snapshotId;
    this.sequenceNumber = sequenceNumber;
    this.specId = specId;
    this.partition = partition;
    this.writeResult = writeResult;
  }

  long snapshotId() {
    return snapshotId;
  }

  long sequenceNumber() {
    return sequenceNumber;
  }

  public int specId() {
    return specId;
  }

  StructLike partition() {
    return partition;
  }

  WriteResult writeResult() {
    return writeResult;
  }

  static Builder builder(long snapshotId, long sequenceNumber) {
    return new Builder(snapshotId, sequenceNumber);
  }

  static class Builder {

    private final long snapshotId;
    private final long sequenceNumber;
    private int specId;
    private StructLike partition;
    private final WriteResult.Builder writeResult;

    private Builder(long snapshotId, long sequenceNumber) {
      this.snapshotId = snapshotId;
      this.sequenceNumber = sequenceNumber;
      this.specId = 0;
      this.partition = null;
      this.writeResult = WriteResult.builder();
    }

    Builder partition(int newSpecId, StructLike newPartition) {
      this.specId = newSpecId;
      this.partition = newPartition;
      return this;
    }

    Builder addDataFile(Collection<DataFile> files) {
      writeResult.addDataFiles(files);
      return this;
    }

    Builder addDeleteFile(Collection<DeleteFile> files) {
      writeResult.addDeleteFiles(files);
      return this;
    }

    Builder addReferencedDataFile(Collection<CharSequence> files) {
      writeResult.addReferencedDataFiles(files);
      return this;
    }

    CommitResult build() {
      return new CommitResult(snapshotId, sequenceNumber, specId, partition, writeResult.build());
    }
  }
}
