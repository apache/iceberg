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
package org.apache.iceberg.flink.sink.dynamic;

import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.iceberg.BaseReplacePartitions;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;

class FlinkReplacePartitions extends BaseReplacePartitions
    implements FlinkSnapshotValidator<FlinkReplacePartitions> {
  private final Consumer<Snapshot> snapshotValidator;

  private Long startingSnapshotId = null; // check all versions by default

  FlinkReplacePartitions(
      String tableName, TableOperations ops, Consumer<Snapshot> snapshotValidator) {
    super(tableName, ops);
    this.snapshotValidator = snapshotValidator;
  }

  @Override
  public void validateSnapshot(Snapshot snapshot) {
    snapshotValidator.accept(snapshot);
  }

  @Nullable
  @Override
  public Long startingSnapshotId() {
    return startingSnapshotId;
  }

  @Override
  public void validate(TableMetadata base, Snapshot parent) {
    super.validate(base, parent);
    validateSnapshots(base, parent);
  }

  FlinkReplacePartitions validateFromSnapshot(@Nullable Snapshot snapshot) {
    if (snapshot != null) {
      super.validateFromSnapshot(snapshot.snapshotId());
      startingSnapshotId = snapshot.snapshotId();
    }

    return this;
  }
}
