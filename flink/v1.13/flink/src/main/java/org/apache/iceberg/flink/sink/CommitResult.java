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
import org.apache.iceberg.io.WriteResult;

class CommitResult implements Serializable {

  private final long sequenceNumber;
  private final long snapshotId;
  private final WriteResult writeResult;

  private CommitResult(long snapshotId,
                       long sequenceNumber,
                       WriteResult writeResult) {
    this.snapshotId = snapshotId;
    this.sequenceNumber = sequenceNumber;
    this.writeResult = writeResult;
  }

  long sequenceNumber() {
    return sequenceNumber;
  }

  long snapshotId() {
    return snapshotId;
  }

  WriteResult writeResult() {
    return writeResult;
  }

  static Builder builder(long sequenceNumber, long snapshotId) {
    return new Builder(sequenceNumber, snapshotId);
  }

  static class Builder {

    private final long sequenceNumber;
    private final long snapshotId;
    private final WriteResult.Builder writeResult;

    private Builder(long sequenceNumber, long snapshotId) {
      this.sequenceNumber = sequenceNumber;
      this.snapshotId = snapshotId;
      this.writeResult = WriteResult.builder();
    }

    Builder add(WriteResult result) {
      this.writeResult.add(result);
      return this;
    }

    Builder addAll(Iterable<WriteResult> results) {
      this.writeResult.addAll(results);
      return this;
    }

    CommitResult build() {
      return new CommitResult(snapshotId, sequenceNumber, writeResult.build());
    }
  }
}
