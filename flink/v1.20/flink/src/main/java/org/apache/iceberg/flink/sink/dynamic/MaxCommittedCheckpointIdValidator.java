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

import java.util.Map;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;

class MaxCommittedCheckpointIdValidator implements Consumer<Snapshot> {
  private static final String FLINK_JOB_ID = "flink.job-id";
  private static final String OPERATOR_ID = "flink.operator-id";
  private static final String MAX_COMMITTED_CHECKPOINT_ID = "flink.max-committed-checkpoint-id";
  private static final long INITIAL_CHECKPOINT_ID = -1L;

  private final long stagedCheckpointId;
  private final String flinkJobId;
  private final String flinkOperatorId;

  MaxCommittedCheckpointIdValidator(
      long stagedCheckpointId, String flinkJobId, String flinkOperatorId) {
    this.stagedCheckpointId = stagedCheckpointId;
    this.flinkJobId = flinkJobId;
    this.flinkOperatorId = flinkOperatorId;
  }

  @Override
  public void accept(Snapshot snapshot) {
    @Nullable
    Long checkpointId = extractCommittedCheckpointId(snapshot, flinkJobId, flinkOperatorId);
    if (checkpointId == null) {
      return;
    }

    ValidationException.check(
        checkpointId < stagedCheckpointId,
        "The new parent snapshot '%s' has '%s': '%s' >= '%s' of the currently staged committable."
            + "\nThis can happen, for example, when using the REST catalog: if the previous commit request failed"
            + " in the Flink client but succeeded on the server after the Flink job decided to retry it with the new request."
            + "\nFlink should retry this exception, and the committer should skip the duplicate request during the next retry.",
        snapshot.snapshotId(),
        MAX_COMMITTED_CHECKPOINT_ID,
        checkpointId,
        stagedCheckpointId);
  }

  /** TODO: Reuse {@link org.apache.iceberg.flink.sink.SinkUtil#getMaxCommittedCheckpointId} * */
  static long getMaxCommittedCheckpointId(
      Table table, String flinkJobId, String operatorId, String branch) {
    Snapshot snapshot = table.snapshot(branch);

    while (snapshot != null) {
      @Nullable
      Long committedCheckpointId = extractCommittedCheckpointId(snapshot, flinkJobId, operatorId);
      if (committedCheckpointId != null) {
        return committedCheckpointId;
      }

      Long parentSnapshotId = snapshot.parentId();
      snapshot = parentSnapshotId != null ? table.snapshot(parentSnapshotId) : null;
    }

    return INITIAL_CHECKPOINT_ID;
  }

  @Nullable
  static Long extractCommittedCheckpointId(
      Snapshot snapshot, String flinkJobId, String operatorId) {
    Map<String, String> summary = snapshot.summary();
    String snapshotFlinkJobId = summary.get(FLINK_JOB_ID);
    String snapshotOperatorId = summary.get(OPERATOR_ID);
    if (flinkJobId.equals(snapshotFlinkJobId)
        && (snapshotOperatorId == null || snapshotOperatorId.equals(operatorId))) {
      String value = summary.get(MAX_COMMITTED_CHECKPOINT_ID);
      if (value != null) {
        return Long.parseLong(value);
      }
    }

    return null;
  }

  static void setFlinkProperties(
      SnapshotUpdate<?> operation, long checkpointId, String flinkJobId, String operatorId) {
    operation.set(MAX_COMMITTED_CHECKPOINT_ID, Long.toString(checkpointId));
    operation.set(FLINK_JOB_ID, flinkJobId);
    operation.set(OPERATOR_ID, operatorId);
  }
}
