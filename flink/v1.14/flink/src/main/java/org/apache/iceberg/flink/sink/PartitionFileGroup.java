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
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

class PartitionFileGroup implements Serializable {

  private final long sequenceNumber;
  private final long snapshotId;
  private final StructLike partition;
  private final DataFile[] dataFiles;
  private final DeleteFile[] deleteFiles;

  private PartitionFileGroup(long sequenceNumber, long snapshotId, StructLike partition,
                             List<DataFile> dataFiles, List<DeleteFile> deleteFiles) {
    this.sequenceNumber = sequenceNumber;
    this.snapshotId = snapshotId;
    this.partition = partition;
    this.dataFiles = dataFiles.toArray(new DataFile[0]);
    this.deleteFiles = deleteFiles.toArray(new DeleteFile[0]);
  }

  long sequenceNumber() {
    return sequenceNumber;
  }

  long snapshotId() {
    return snapshotId;
  }

  StructLike partition() {
    return partition;
  }

  DataFile[] dataFiles() {
    return dataFiles;
  }

  DeleteFile[] deleteFiles() {
    return deleteFiles;
  }

  static Builder builder(long sequenceNumber, long snapshotId, StructLike partition) {
    return new Builder(sequenceNumber, snapshotId, partition);
  }

  static class Builder {

    private final long sequenceNumber;
    private final long snapshotId;
    private final StructLike partition;
    private final List<DataFile> dataFiles;
    private final List<DeleteFile> deleteFiles;

    private Builder(long sequenceNumber, long snapshotId, StructLike partition) {
      this.sequenceNumber = sequenceNumber;
      this.snapshotId = snapshotId;
      this.partition = partition;
      this.dataFiles = Lists.newArrayList();
      this.deleteFiles = Lists.newArrayList();
    }

    Builder addDataFile(DataFile... files) {
      Collections.addAll(dataFiles, files);
      return this;
    }

    Builder addDataFile(Collection<DataFile> files) {
      dataFiles.addAll(files);
      return this;
    }

    Builder addDeleteFile(DeleteFile... files) {
      Collections.addAll(deleteFiles, files);
      return this;
    }

    Builder addDeleteFile(Collection<DeleteFile> files) {
      deleteFiles.addAll(files);
      return this;
    }

    PartitionFileGroup build() {
      return new PartitionFileGroup(sequenceNumber, snapshotId, partition, dataFiles, deleteFiles);
    }
  }
}
